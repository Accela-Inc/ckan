import ckan.plugins as p
from ckan.lib.base import BaseController, config
import stats as stats_lib
import ckan.lib.helpers as h

class StatsController(BaseController):

    def index(self):
        c = p.toolkit.c
        stats = stats_lib.Stats()
        rev_stats = stats_lib.RevisionStats()
        c.top_rated_packages = stats.top_rated_packages()
        c.most_edited_packages = stats.most_edited_packages()
        c.largest_groups = stats.largest_groups()
        c.top_tags = stats.top_tags()
        c.top_package_owners = stats.top_package_owners()
        c.new_packages_by_week = rev_stats.get_by_week('new_packages')
        c.deleted_packages_by_week = rev_stats.get_by_week('deleted_packages')
        c.num_packages_by_week = rev_stats.get_num_packages_by_week()
        c.package_revisions_by_week = rev_stats.get_by_week('package_revisions')

        #******************** edited *********************
        #dataset visit
        c.dataset_visit_by_total = stats.dataset_visit_count('total')
        c.dataset_visit_by_month = stats.dataset_visit_count('month')
        c.dataset_visit_by_week = stats.dataset_visit_count('week')
        c.dataset_visit_by_day = stats.dataset_visit_count('day')
        #resource visit
        c.resource_visit_by_total = stats.resource_visit_count('total')
        c.resource_visit_by_month = stats.resource_visit_count('month')
        c.resource_visit_by_week = stats.resource_visit_count('week')
        c.resource_visit_by_day = stats.resource_visit_count('day')
        #user visit
        c.user_visit_by_day = stats.user_visit_by_day()
        c.user_visit_by_week = stats.user_visit_by_week()
        c.user_visit_by_month = stats.user_visit_by_month()
        
        c.user_visit_by_day_json = []
        c.user_visit_by_week_json = []
        c.user_visit_by_month_json = []
        
        for visit_count, date in c.user_visit_by_day:
            c.user_visit_by_day_json.append({'date': h.date_str_to_datetime(date), 'visit_count': visit_count})
        for visit_count, date in c.user_visit_by_week:
            c.user_visit_by_week_json.append({'date': date, 'visit_count': visit_count})
        for visit_count, date in c.user_visit_by_month:
            c.user_visit_by_month_json.append({'date': date, 'visit_count': visit_count})
        #******************** edited *********************
        
        # Used in the legacy CKAN templates.
        c.packages_by_week = []

        # Used in new CKAN templates gives more control to the templates for formatting.
        c.raw_packages_by_week = []
        for week_date, num_packages, cumulative_num_packages in c.num_packages_by_week:
            c.packages_by_week.append('[new Date(%s), %s]' % (week_date.replace('-', ','), cumulative_num_packages))
            c.raw_packages_by_week.append({'date': h.date_str_to_datetime(week_date), 'total_packages': cumulative_num_packages})

        c.all_package_revisions = []
        c.raw_all_package_revisions = []
        for week_date, revs, num_revisions, cumulative_num_revisions in c.package_revisions_by_week:
            c.all_package_revisions.append('[new Date(%s), %s]' % (week_date.replace('-', ','), num_revisions))
            c.raw_all_package_revisions.append({'date': h.date_str_to_datetime(week_date), 'total_revisions': num_revisions})

        c.new_datasets = []
        c.raw_new_datasets = []
        for week_date, pkgs, num_packages, cumulative_num_packages in c.new_packages_by_week:
            c.new_datasets.append('[new Date(%s), %s]' % (week_date.replace('-', ','), num_packages))
            c.raw_new_datasets.append({'date': h.date_str_to_datetime(week_date), 'new_packages': num_packages})

        return p.toolkit.render('ckanext/stats/index.html')

    def leaderboard(self, id=None):
        c = p.toolkit.c
        c.solr_core_url = config.get('ckanext.stats.solr_core_url',
                'http://solr.okfn.org/solr/ckan')
        return p.toolkit.render('ckanext/stats/leaderboard.html')

