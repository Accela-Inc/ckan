import StringIO
import unicodecsv as csv

import pylons 

import ckan.plugins as p
import ckan.lib.base as base
import ckan.model as model

from ckan.common import request


class DatastoreController(base.BaseController):
    def dump(self, resource_id):
        context = {
            'model': model,
            'session': model.Session,
            'user': p.toolkit.c.user
        }
        
        # CivicData customization block starts
        try:
            from pylons import config
            data_dict = {
                'resource_id': resource_id,
                'limit': request.GET.get('limit', config.get('ckan.limit', '100000')),
                'offset': request.GET.get('offset', 0),
                'sort': request.GET.get('sort', '_id ASC')
            }
        except Exception, ex:
            # This is original data_dict for ckan-2.4.0
            data_dict = {
            'resource_id': resource_id,
            'limit': request.GET.get('limit', 100000),
            'offset': request.GET.get('offset', 0)
        }    
        # CivicData customization block ends
        
        action = p.toolkit.get_action('datastore_search')
        try:
            result = action(context, data_dict)
        except p.toolkit.ObjectNotFound:
            base.abort(404, p.toolkit._('DataStore resource not found'))

        pylons.response.headers['Content-Type'] = 'text/csv'
        pylons.response.headers['Content-disposition'] = \
            'attachment; filename="{name}.csv"'.format(name=resource_id)
        f = StringIO.StringIO()
        wr = csv.writer(f, encoding='utf-8')

        header = [x['id'] for x in result['fields']]
        wr.writerow(header)

        for record in result['records']:
            wr.writerow([record[column] for column in header])
        return f.getvalue()
    
    # CivicData customization block starts
    def json(self, resource_id):
        try:
            from pylons import config
            from ckan.common import c
            import urllib2
            import logging
            log = logging.getLogger(__name__)
            
            f = StringIO.StringIO()
            sql = 'SELECT%20*%20from%20%22{0}%22%20LIMIT%20{1}'.format(resource_id, config.get('ckan.limit', '100000'))
            url = config.get('ckan.site_url', '') + '/api/3/action/datastore_search_sql?apikey='+ c.userobj.apikey +'&sql=' + sql
            log.debug(url)
            result = urllib2.urlopen(url)
            while True:
                s = result.read(1024 * 32)
                if len(s) == 0:
                    break
                f.write(s)
            pylons.response.headers['Content-Type'] = 'text/json'
            pylons.response.headers['Content-disposition'] = \
              'attachment; filename="{name}.json"'.format(name=resource_id)
            f.flush()
            return f.getvalue()
        except p.toolkit.ObjectNotFound:
            base.abort(404, p.toolkit._('DataStore resource not found'))

        return None
    # CivicData customization block ends
