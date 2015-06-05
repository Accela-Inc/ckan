import datetime

from pylons import config
from sqlalchemy import Table, select, func, and_

import ckan.plugins as p
import ckan.model as model
import logging

cache_enabled = p.toolkit.asbool(config.get('ckanext.stats.cache_enabled', 'False'))
log = logging.getLogger(__name__)

if cache_enabled:
    from pylons import cache
    our_cache = cache.get_cache('stats', type='dbm')

DATE_FORMAT = '%Y-%m-%d'

def table(name):
    return Table(name, model.meta.metadata, autoload=True)

def datetime2date(datetime_):
    return datetime.date(datetime_.year, datetime_.month, datetime_.day)


class Stats(object):
    @classmethod
    def top_rated_packages(cls, limit=10):
        # NB Not using sqlalchemy as sqla 0.4 doesn't work using both group_by
        # and apply_avg
        package = table('package')
        rating = table('rating')
        sql = select([package.c.id, func.avg(rating.c.rating), func.count(rating.c.rating)], from_obj=[package.join(rating)]).\
              group_by(package.c.id).\
              where(and_(package.c.state=='active')).\
              order_by(func.avg(rating.c.rating).desc(), func.count(rating.c.rating).desc()).\
              limit(limit)
        res_ids = model.Session.execute(sql).fetchall()
        res_pkgs = [(model.Session.query(model.Package).get(unicode(pkg_id)), avg, num) for pkg_id, avg, num in res_ids]
        return res_pkgs

    @classmethod
    def most_edited_packages(cls, limit=10):
        package_revision = table('package_revision')
        s = select([package_revision.c.id, func.count(package_revision.c.revision_id)]).\
            group_by(package_revision.c.id).\
            where(and_(package_revision.c.state=='active', package_revision.c.private == 'f')).\
            order_by(func.count(package_revision.c.revision_id).desc()).\
            limit(limit)
        res_ids = model.Session.execute(s).fetchall()
        res_pkgs = [(model.Session.query(model.Package).get(unicode(pkg_id)), val) for pkg_id, val in res_ids]
        return res_pkgs

    @classmethod
    def largest_groups(cls, limit=10):

        #member = table('member')
        #group = table('group')
        #s = select([member.c.group_id, func.count(member.c.table_id)], from_obj=[member.join(group)]).\
            #group_by(member.c.group_id).\
            #where(and_(member.c.group_id!=None, member.c.table_name=='package', member.c.state=='active', group.c.is_organization=='f', group.c.state=='active')).\
            #order_by(func.count(member.c.table_id).desc()).\
            #limit(limit)

        s = '''
	SELECT a.group_id AS group_id, count(a.table_id) AS val FROM (
        select member.group_id, member.table_id from "group" g
        JOIN member on g.id = member.group_id
        WHERE member.table_name = 'package' AND g.state = 'active' AND g.is_organization = 'f' AND member.state = 'active') as a
        JOIN package ON a.table_id = package.id
        WHERE package.state = 'active' AND package.private = 'f'
        GROUP BY a.group_id
        ORDER BY val DESC
        LIMIT 10
        '''
        res_ids = model.Session.execute(s).fetchall()
        res_groups = [(model.Session.query(model.Group).get(unicode(group_id)), val) for group_id, val in res_ids]
        return res_groups

    @classmethod
    def top_tags(cls, limit=10):
        #assert returned_tag_info in ('name', 'id', 'object')
        #tag = table('tag')
        #package_tag = table('package_tag')
        #TODO filter out tags with state=deleted
        #if returned_tag_info == 'name':
            #from_obj = [package_tag.join(tag)]
            #tag_column = tag.c.name
        #else:
            #from_obj = None
            #tag_column = package_tag.c.tag_id
        #s = select([tag_column, func.count(package_tag.c.package_id)],
                    #from_obj=from_obj)
        #s = s.group_by(tag_column).\
            #where(and_(package_tag.c.state=='active')).\
            #order_by(func.count(package_tag.c.package_id).desc()).\
            #limit(limit)
        #res_col = model.Session.execute(s).fetchall()
        #if returned_tag_info in ('id', 'name'):
            #return res_col
        #elif returned_tag_info == 'object':
            #res_tags = [(model.Session.query(model.Tag).get(unicode(tag_id)), val) for tag_id, val in res_col]
            #return res_tags

        s = '''
        SELECT package_tag.tag_id, count(package_tag.tag_id) AS val FROM tag
        JOIN package_tag on tag.id = package_tag.tag_id
        JOIN package on package.id = package_tag.package_id
        WHERE package.state = 'active' AND package_tag.state = 'active' AND package.license_id != 'notspecified'
        GROUP BY package_tag.tag_id
        ORDER BY val DESC
        LIMIT 10
        '''
        res_col = model.Session.execute(s).fetchall()
        res_tags = [(model.Session.query(model.Tag).get(unicode(tag_id)), val) for tag_id, val in res_col]
        return res_tags

    @classmethod
    def top_package_owners(cls, limit=10):
        package_role = table('package_role')
        user_object_role = table('user_object_role')
        s = select([user_object_role.c.user_id, func.count(user_object_role.c.role)], from_obj=[user_object_role.join(package_role)]).\
            where(user_object_role.c.role==model.authz.Role.ADMIN).\
            where(user_object_role.c.user_id!=None).\
            group_by(user_object_role.c.user_id).\
            order_by(func.count(user_object_role.c.role).desc()).\
            limit(limit)
        res_ids = model.Session.execute(s).fetchall()
        res_users = [(model.Session.query(model.User).get(unicode(user_id)), val) for user_id, val in res_ids]
        return res_users

    #*************** edited *****************
    @classmethod
    def user_visit_by_day(cls, limit=10):
        s = ''' select count(a.user_key), a.adate
                from (select DISTINCT user_key, to_char(access_timestamp, 'YYYY-MM-dd') adate
                        from tracking_raw
                        order by to_char(access_timestamp, 'YYYY-MM-dd') desc
                      ) a
                group by a.adate order by a.adate desc limit 10; '''
        user_visit = model.Session.execute(s).fetchall()
        #print user_visit
        return user_visit

    @classmethod
    def user_visit_by_week(cls, limit=10):
        s = ''' select count(a.user_key),  date(to_timestamp( (extract(epoch from now()) - (a.week * 3600 * 24 * 7)) ))
                    from (
                        select DISTINCT user_key, round(((extract(epoch from now()) - extract(epoch from access_timestamp)) / 3600 / 24 / 7)) week
                        from tracking_raw
                        order by week asc
                    ) a
                group by a.week order by a.week asc limit 10; '''
        user_visit = model.Session.execute(s).fetchall()
        #print user_visit
        return user_visit

    @classmethod
    def user_visit_by_month(cls, limit=10):
        s = ''' select count(a.user_key), to_date(a.adate, 'YYYY-MM-DD')
                    from (
                        select DISTINCT user_key, to_char(access_timestamp, 'YYYY-MM') adate
                        from tracking_raw
                        order by to_char(access_timestamp, 'YYYY-MM') desc
                    ) a
                group by a.adate order by a.adate desc limit 10; '''
        user_visit = model.Session.execute(s).fetchall()
        #print user_visit
        return user_visit

    @classmethod
    def dataset_visit_count(cls, order_by='total', limit=10):
        s = ''' select t3.package_id, t4.title, COALESCE(t3.day_sum, 0) d_sum, COALESCE(t3.week_sum, 0) w_sum, COALESCE(t3.month_sum, 0) m_sum, COALESCE(t3.total_sum, 0) t_sum from
                (select t2.package_id, t2.day_sum, t2.week_sum, month_count.month_sum, t2.total_sum from
                (select t1.package_id, t1.day_sum, week_count.week_sum, t1.total_sum from
                (select total_count.package_id, total_count.total_sum, day_count.day_sum from
                        (select package_id, sum(count) total_sum from tracking_summary where package_id!='~~not~found~~' GROUP BY package_id) total_count LEFT JOIN
                        (select package_id, sum(count) day_sum from tracking_summary where package_id!='~~not~found~~' and age(tracking_date) <= INTERVAL '1 day' GROUP BY package_id) day_count
                    on total_count.package_id=day_count.package_id
                ) t1 LEFT JOIN
                (select package_id, sum(count) week_sum from tracking_summary where package_id!='~~not~found~~' and age(tracking_date) <= INTERVAL '1 week' GROUP BY package_id) week_count
                on t1.package_id=week_count.package_id) t2 LEFT JOIN
                (select package_id, sum(count) month_sum from tracking_summary where package_id!='~~not~found~~' and age(tracking_date) <= INTERVAL '1 month' GROUP BY package_id) month_count
                on t2.package_id=month_count.package_id
                ) t3 LEFT JOIN
                (select package.id, package.title from package) t4
                on t3.package_id=t4.id order by COALESCE(t3.order_by, 0) desc LIMIT 10;'''
        if order_by == 'total':
            s=s.replace('order_by', 'total_sum')
        if order_by == 'month':
            s=s.replace('order_by', 'month_sum')
        if order_by == 'week':
            s=s.replace('order_by', 'week_sum')
        if order_by == 'day':
            s=s.replace('order_by', 'day_sum')

        res_dataset_raws = model.Session.execute(s).fetchall()
        #print res_dataset_raws
        return res_dataset_raws

    def resource_visit_count(cls, order_by='total', limit=10):
        s = ''' select t3.resource_id, t4.name, COALESCE(t3.day_sum, 0), COALESCE(t3.week_sum, 0), COALESCE(t3.month_sum, 0), COALESCE(t3.total_sum, 0) from
                (select t2.resource_id, t2.total_sum, t2.day_sum, t2.week_sum, t_month.month_sum from
                (select t1.resource_id, t1.total_sum, t1.day_sum, t_week.week_sum from
                (select t_total.resource_id, t_total.total_sum, t_day.day_sum from
                (select substring(substring(url from '/resource/[^/]*'), 11) resource_id, sum(count) total_sum
                from tracking_summary
                where position('/dataset/' in url) > 0 and position('/resource/' in url) > 0
                group by resource_id) t_total
                LEFT JOIN
                (select substring(substring(url from '/resource/[^/]*'), 11) resource_id, sum(count) day_sum
                from tracking_summary
                where position('/dataset/' in url) > 0 and position('/resource/' in url) > 0 and age(tracking_date) <= INTERVAL '1 day'
                group by resource_id) t_day
                on t_total.resource_id=t_day.resource_id) t1
                LEFT JOIN
                (select substring(substring(url from '/resource/[^/]*'), 11) resource_id, sum(count) week_sum
                from tracking_summary
                where position('/dataset/' in url) > 0 and position('/resource/' in url) > 0 and age(tracking_date) <= INTERVAL '1 week'
                group by resource_id) t_week
                on t_week.resource_id=t1.resource_id) t2
                LEFT JOIN
                (select substring(substring(url from '/resource/[^/]*'), 11) resource_id, sum(count) month_sum
                from tracking_summary
                where position('/dataset/' in url) > 0 and position('/resource/' in url) > 0 and age(tracking_date) <= INTERVAL '1 month'
                group by resource_id) t_month
                on t2.resource_id=t_month.resource_id) t3
                LEFT JOIN
                (select id, name from resource) t4
                on t3.resource_id=t4.id
                order by COALESCE(t3.order_by, 0) desc LIMIT 10;'''
        if order_by == 'total':
            s=s.replace('order_by', 'total_sum')
        if order_by == 'month':
            s=s.replace('order_by', 'month_sum')
        if order_by == 'week':
            s=s.replace('order_by', 'week_sum')
        if order_by == 'day':
            s=s.replace('order_by', 'day_sum')
        res_resource_raws = model.Session.execute(s).fetchall()
        #print res_dataset_raws
        return res_resource_raws


class RevisionStats(object):
    @classmethod
    def package_addition_rate(cls, weeks_ago=0):
        week_commenced = cls.get_date_weeks_ago(weeks_ago)
        return cls.get_objects_in_a_week(week_commenced,
                                          type_='package_addition_rate')

    @classmethod
    def package_revision_rate(cls, weeks_ago=0):
        week_commenced = cls.get_date_weeks_ago(weeks_ago)
        return cls.get_objects_in_a_week(week_commenced,
                                          type_='package_revision_rate')

    @classmethod
    def get_date_weeks_ago(cls, weeks_ago):
        '''
        @param weeks_ago: specify how many weeks ago to give count for
                          (0 = this week so far)
        '''
        date_ = datetime.date.today()
        return date_ - datetime.timedelta(days=
                             datetime.date.weekday(date_) + 7 * weeks_ago)

    @classmethod
    def get_week_dates(cls, weeks_ago):
        '''
        @param weeks_ago: specify how many weeks ago to give count for
                          (0 = this week so far)
        '''
        today = datetime.date.today()
        date_from = datetime.datetime(today.year, today.month, today.day) -\
                    datetime.timedelta(days=datetime.date.weekday(today) + \
                                       7 * weeks_ago)
        date_to = date_from + datetime.timedelta(days=7)
        return (date_from, date_to)

    @classmethod
    def get_date_week_started(cls, date_):
        assert isinstance(date_, datetime.date)
        if isinstance(date_, datetime.datetime):
            date_ = datetime2date(date_)
        return date_ - datetime.timedelta(days=datetime.date.weekday(date_))

    @classmethod
    def get_package_revisions(cls):
        '''
        @return: Returns list of revisions and date of them, in
                 format: [(id, date), ...]
        '''
        package_revision = table('package_revision')
        revision = table('revision')
        s = select([package_revision.c.id, revision.c.timestamp], from_obj=[package_revision.join(revision)]).\
            where(and_(package_revision.c.state == 'active', package_revision.c.private == 'f')).\
            order_by(revision.c.timestamp)
        res = model.Session.execute(s).fetchall() # [(id, datetime), ...]
        return res

    @classmethod
    def get_new_packages(cls):
        '''
        @return: Returns list of new pkgs and date when they were created, in
                 format: [(id, date_ordinal), ...]
        '''
        def new_packages():
            # Can't filter by time in select because 'min' function has to
            # be 'for all time' else you get first revision in the time period.
            package_revision = table('package_revision')
            revision = table('revision')
            s = select([package_revision.c.id, func.min(revision.c.timestamp)], from_obj=[package_revision.join(revision)]).\
                where(and_(package_revision.c.state == 'active', revision.c.state == 'active', package_revision.c.private=='f')).\
                group_by(package_revision.c.id).\
                order_by(func.min(revision.c.timestamp))

            res = model.Session.execute(s).fetchall() # [(id, datetime), ...]
            res_pickleable = []
            for pkg_id, created_datetime in res:
                res_pickleable.append((pkg_id, created_datetime.toordinal()))
            return res_pickleable
        if cache_enabled:
            week_commences = cls.get_date_week_started(datetime.date.today())
            key = 'all_new_packages_%s' + week_commences.strftime(DATE_FORMAT)
            new_packages = our_cache.get_value(key=key,
                                               createfunc=new_packages)
        else:
            new_packages = new_packages()
        return new_packages

    @classmethod
    def get_total_packages(cls):
        '''
        @return: Returns list of new pkgs and date when they were created, in
                 format: [(id, date_ordinal), ...]
        '''
        def new_packages():
            # Can't filter by time in select because 'min' function has to
            # be 'for all time' else you get first revision in the time period.
            package = table('package')
            revision = table('revision')
            s = select([package.c.id, func.min(revision.c.timestamp)], from_obj=[package.join(revision)]).\
                where(and_(package.c.state == 'active', revision.c.state == 'active', package.c.private=='f')).\
                group_by(package.c.id).\
                order_by(func.min(revision.c.timestamp))

            res = model.Session.execute(s).fetchall() # [(id, datetime), ...]
            res_pickleable = []
            for pkg_id, created_datetime in res:
                res_pickleable.append((pkg_id, created_datetime.toordinal()))
            return res_pickleable
        if cache_enabled:
            week_commences = cls.get_date_week_started(datetime.date.today())
            key = 'all_total_packages_%s' + week_commences.strftime(DATE_FORMAT)
            new_packages = our_cache.get_value(key=key,
                                               createfunc=new_packages)
        else:
            new_packages = new_packages()
        return new_packages

    @classmethod
    def get_deleted_packages(cls):
        '''
        @return: Returns list of deleted pkgs and date when they were deleted, in
                 format: [(id, date_ordinal), ...]
        '''
        def deleted_packages():
            # Can't filter by time in select because 'min' function has to
            # be 'for all time' else you get first revision in the time period.
            package_revision = table('package_revision')
            revision = table('revision')

            s = select([package_revision.c.id, func.min(revision.c.timestamp)], from_obj=[package_revision.join(revision)]).\
                where(and_(package_revision.c.state==model.State.DELETED, package_revision.c.private=='f')).\
                group_by(package_revision.c.id).\
                order_by(func.min(revision.c.timestamp))
            res = model.Session.execute(s).fetchall() # [(id, datetime), ...]
            res_pickleable = []
            for pkg_id, deleted_datetime in res:
                res_pickleable.append((pkg_id, deleted_datetime.toordinal()))
            return res_pickleable
        if cache_enabled:
            week_commences = cls.get_date_week_started(datetime.date.today())
            key = 'all_deleted_packages_%s' + week_commences.strftime(DATE_FORMAT)
            deleted_packages = our_cache.get_value(key=key,
                                                   createfunc=deleted_packages)
        else:
            deleted_packages = deleted_packages()
        return deleted_packages

    @classmethod
    def get_num_packages_by_week(cls):
        def num_packages():
            new_packages_by_week = cls.get_by_week('total_packages')
            deleted_packages_by_week = cls.get_by_week('deleted_packages')
            first_date = (min(datetime.datetime.strptime(new_packages_by_week[0][0], DATE_FORMAT),
                              datetime.datetime.strptime(deleted_packages_by_week[0][0], DATE_FORMAT))).date()
            cls._cumulative_num_pkgs = 0
            new_pkgs = []
            deleted_pkgs = []
            def build_weekly_stats(week_commences, new_pkg_ids, deleted_pkg_ids):
                num_pkgs = len(new_pkg_ids)
                #new_pkgs.extend([model.Session.query(model.Package).get(id).name for id in new_pkg_ids])
                #deleted_pkgs.extend([model.Session.query(model.Package).get(id).name for id in deleted_pkg_ids])
                cls._cumulative_num_pkgs += num_pkgs
                return (week_commences.strftime(DATE_FORMAT),
                        num_pkgs, cls._cumulative_num_pkgs)
            week_ends = first_date
            today = datetime.date.today()
            new_package_week_index = 0
            deleted_package_week_index = 0
            weekly_numbers = [] # [(week_commences, num_packages, cumulative_num_pkgs])]
            while week_ends <= today:
                week_commences = week_ends
                week_ends = week_commences + datetime.timedelta(days=7)
                if datetime.datetime.strptime(new_packages_by_week[new_package_week_index][0], DATE_FORMAT).date() == week_commences:
                    new_pkg_ids = new_packages_by_week[new_package_week_index][1]
                    new_package_week_index += 1
                else:
                    new_pkg_ids = []
                if datetime.datetime.strptime(deleted_packages_by_week[deleted_package_week_index][0], DATE_FORMAT).date() == week_commences:
                    deleted_pkg_ids = deleted_packages_by_week[deleted_package_week_index][1]
                    deleted_package_week_index += 1
                else:
                    deleted_pkg_ids = []
                weekly_numbers.append(build_weekly_stats(week_commences, new_pkg_ids, deleted_pkg_ids))
            # just check we got to the end of each count
            assert new_package_week_index == len(new_packages_by_week)
            assert deleted_package_week_index == len(deleted_packages_by_week)
            return weekly_numbers
        if cache_enabled:
            week_commences = cls.get_date_week_started(datetime.date.today())
            key = 'number_packages_%s' + week_commences.strftime(DATE_FORMAT)
            num_packages = our_cache.get_value(key=key,
                                               createfunc=num_packages)
        else:
            num_packages = num_packages()
        return num_packages

    @classmethod
    def get_by_week(cls, object_type):
        cls._object_type = object_type
        def objects_by_week():
            if cls._object_type == 'new_packages':
                objects = cls.get_new_packages()
                def get_date(object_date):
                    return datetime.date.fromordinal(object_date)
            elif cls._object_type == 'total_packages':
                objects = cls.get_total_packages()
                def get_date(object_date):
                    return datetime.date.fromordinal(object_date)
            elif cls._object_type == 'deleted_packages':
                objects = cls.get_deleted_packages()
                def get_date(object_date):
                    return datetime.date.fromordinal(object_date)
            elif cls._object_type == 'package_revisions':
                objects = cls.get_package_revisions()
                def get_date(object_date):
                    return datetime2date(object_date)
            else:
                raise NotImplementedError()
            first_date = get_date(objects[0][1]) if objects else datetime.date.today()
            week_commences = cls.get_date_week_started(first_date)
            week_ends = week_commences + datetime.timedelta(days=7)
            week_index = 0
            weekly_pkg_ids = [] # [(week_commences, [pkg_id1, pkg_id2, ...])]
            pkg_id_stack = []
            cls._cumulative_num_pkgs = 0
            def build_weekly_stats(week_commences, pkg_ids):
                num_pkgs = len(pkg_ids)
                cls._cumulative_num_pkgs += num_pkgs
                return (week_commences.strftime(DATE_FORMAT),
                        pkg_ids, num_pkgs, cls._cumulative_num_pkgs)
            for pkg_id, date_field in objects:
                date_ = get_date(date_field)
                if date_ >= week_ends:
                    weekly_pkg_ids.append(build_weekly_stats(week_commences, pkg_id_stack))
                    pkg_id_stack = []
                    week_commences = week_ends
                    week_ends = week_commences + datetime.timedelta(days=7)
                pkg_id_stack.append(pkg_id)
            weekly_pkg_ids.append(build_weekly_stats(week_commences, pkg_id_stack))
            today = datetime.date.today()
            while week_ends <= today:
                week_commences = week_ends
                week_ends = week_commences + datetime.timedelta(days=7)
                weekly_pkg_ids.append(build_weekly_stats(week_commences, []))
            return weekly_pkg_ids
        if cache_enabled:
            week_commences = cls.get_date_week_started(datetime.date.today())
            key = '%s_by_week_%s' % (cls._object_type, week_commences.strftime(DATE_FORMAT))
            objects_by_week_ = our_cache.get_value(key=key,
                                    createfunc=objects_by_week)
        else:
            objects_by_week_ = objects_by_week()
        return objects_by_week_

    @classmethod
    def get_objects_in_a_week(cls, date_week_commences,
                                 type_='new-package-rate'):
        '''
        @param type: Specifies what to return about the specified week:
                     "package_addition_rate" number of new packages
                     "package_revision_rate" number of package revisions
                     "new_packages" a list of the packages created
                     in a tuple with the date.
                     "deleted_packages" a list of the packages deleted
                     in a tuple with the date.
        @param dates: date range of interest - a tuple:
                     (start_date, end_date)
        '''
        assert isinstance(date_week_commences, datetime.date)
        if type_ in ('package_addition_rate', 'new_packages'):
            object_type = 'new_packages'
        elif type_ == 'deleted_packages':
            object_type = 'deleted_packages'
        elif type_ == 'package_revision_rate':
            object_type = 'package_revisions'
        else:
            raise NotImplementedError()
        objects_by_week = cls.get_by_week(object_type)
        date_wc_str = date_week_commences.strftime(DATE_FORMAT)
        object_ids = None
        for objects_in_a_week in objects_by_week:
            if objects_in_a_week[0] == date_wc_str:
                object_ids = objects_in_a_week[1]
                break
        if object_ids is None:
            raise TypeError('Week specified is outside range')
        assert isinstance(object_ids, list)
        if type_ in ('package_revision_rate', 'package_addition_rate'):
            return len(object_ids)
        elif type_ in ('new_packages', 'deleted_packages'):
            return [ model.Session.query(model.Package).get(pkg_id) \
                     for pkg_id in object_ids ]

