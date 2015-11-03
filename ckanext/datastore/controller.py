import StringIO
import unicodecsv as csv

import pylons 

import ckan.plugins as p
import ckan.lib.base as base
import ckan.model as model

from ckan.common import request

from sqlalchemy import create_engine
import gzip
import os.path
import time
import shutil

from ckan.common import request,c

log = logging.getLogger(__name__)

class DatastoreController(base.BaseController):
    def dump(self, resource_id):
        fname = '{fpath}/{name}.csv'.format(fpath=pylons.config['ckan.datastore.cache_location'],name=resource_id)

        # will cache things for an hour
        if not (os.path.isfile(fname) and ((time.time() - os.path.getmtime(fname)) < 3600)):
            # todo- how to get table metadata without querying db like this:
            context = {
                'model': model,
                'session': model.Session,
                'user': p.toolkit.c.user
            }
            data_dict = {
                'resource_id': resource_id,
                'limit': 0
            }

            action = p.toolkit.get_action('datastore_search')
            _table_meta = p.toolkit.get_action('datastore_search')(context, data_dict)

            _column_names = []
            for i in _table_meta["fields"]:
                _column_names.append('"{field_name}"'.format(field_name=i["id"]))

            eng = create_engine(pylons.config['ckan.datastore.write_url'])
            cxn = eng.raw_connection()
            dbcopy_f = StringIO.StringIO()
            f = open(fname,'w')

            try:
                cur = cxn.cursor()
                _copy_sql = 'COPY "{table_name}" ({cols}) TO STDOUT WITH CSV HEADER'.format(table_name=resource_id,cols=','.join(_column_names))

                cur.copy_expert(_copy_sql, dbcopy_f)
                dbcopy_f.seek(0)
                shutil.copyfileobj(dbcopy_f, f)
            finally:
                cxn.close()
                dbcopy_f.close()
                f.close()

        try:
            user_filename = fname.rsplit('/',1)[-1]
            file_size = os.path.getsize(fname)

            headers = [('Content-Disposition', 'attachment; filename=\"' + user_filename + '\"'),
                   ('Content-Type', 'text/plain'),
                   ('Content-Length', str(file_size))]

            from paste.fileapp import FileApp
            fapp = FileApp(fname, headers=headers)

            return fapp(request.environ, self.start_response)

        except p.toolkit.ObjectNotFound:
            base.abort(404, p.toolkit._('DataStore resource not found'))

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
            base.abort(404, p.toolkit._('From CivicData Customization- DataStore resource not found'))

        return None
    # CivicData customization block ends
