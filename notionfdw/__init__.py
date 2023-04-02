import json
from datetime import datetime
from itertools import cycle

from . import ForeignDataWrapper, TableDefinition, ColumnDefinition
import requests

from .compat import unicode_
from .utils import log_to_postgres


class NotionDataWrapper(ForeignDataWrapper):

    def __init__(self, options, columns):
        super(NotionDataWrapper, self).__init__(options, columns)
        self.columns = columns
        self.tx_hook = options.get('tx_hook', False)
        self.test_type = options.get('test_type', None)
        self.api_key = options.get('api_key')
        self.headers = {
            "Authorization": "Bearer %s" % self.api_key,
            "accept": "application/json",
            "Notion-Version": "2022-06-28",
            "content-type": "application/json"
        }
        self.base_url = 'https://api.notion.com'
        self._row_id_column = options.get('row_id_column',
                                          list(self.columns.keys())[0])
        self.database_id = options.get('database_id')
        self.test_subtype = options.get('test_subtype', None)

    def execute(self, quals, columns):
        url = "%s/v1/databases/%s/query" % (self.base_url, self.database_id)

        payload = {"page_size": 100}
        response = requests.post(url, json=payload, headers=self.headers)
        response = response.json()

        for resp in response['results']:
            line = {}
            for column_name in self.columns:
                id = resp['properties']['id']['title'][0]['plain_text']
                name = resp['properties']['name']['rich_text'][0]['plain_text']
                if column_name == 'id':
                    line[column_name] = id
                else:
                    line[column_name] = name
            yield line

    def insert(self, values):
        data = {
            "parent": {"type": "database_id", "database_id": "597d7f1692f34c68a87d9d5860392ef0"},
            "properties": {"id": {"id": "title",
                                  "title": [{"annotations": {"bold": False,
                                                             "code": False,
                                                             "color": "default",
                                                             "italic": False,
                                                             "strikethrough": False,
                                                             "underline": False},
                                             "href": None,
                                             "plain_text": values.get("id"),
                                             "text": {"content": values.get("id"),
                                                      "link": None},
                                             "type": "text"}],
                                  "type": "title"},
                           "name": {"id": "%40dMw",
                                    "rich_text": [{"annotations": {"bold": False,
                                                                   "code": False,
                                                                   "color": "default",
                                                                   "italic": False,
                                                                   "strikethrough": False,
                                                                   "underline": False},
                                                   "href": None,
                                                   "plain_text": values.get("name"),
                                                   "text": {"content": values.get("name"),
                                                            "link": None},
                                                   "type": "text"}],
                                    "type": "rich_text"}
                           }}

        for key in self.columns:
            values[key] = "INSERTED: %s" % values.get(key, None)

        url = "%s/v1/pages" % self.base_url
        requests.post(url=url, headers=self.headers, data=json.dumps(data))
        return values

    def _as_generator(self, quals, columns):
        random_thing = cycle([1, 2, 3])
        for index in range(20):
            if self.test_type == 'sequence':
                line = []
                for column_name in self.columns:
                    if self.test_subtype == '1null' and len(line) == 0:
                        line.append(None)
                    else:
                        line.append('%s %s %s' % (column_name,
                                                  next(random_thing), index))
            else:
                line = {}
                for column_name, column in self.columns.items():
                    if self.test_type == 'list':
                        line[column_name] = [
                            column_name, next(random_thing),
                            index, '%s,"%s"' % (column_name, index),
                            '{some value, \\" \' 2}']
                    elif self.test_type == 'dict':
                        line[column_name] = {
                            "column_name": column_name,
                            "repeater": next(random_thing),
                            "index": index,
                            "maybe_hstore": "a => b"}
                    elif self.test_type == 'date':
                        line[column_name] = datetime(2011, (index % 12) + 1,
                                                     next(random_thing), 14,
                                                     30, 25)
                    elif self.test_type == 'int':
                        line[column_name] = index
                    elif self.test_type == 'encoding':
                        line[column_name] = (b'\xc3\xa9\xc3\xa0\xc2\xa4'
                                             .decode('utf-8'))
                    elif self.test_type == 'nested_list':
                        line[column_name] = [
                            [column_name, column_name],
                            [next(random_thing), '{some value, \\" 2}'],
                            [index, '%s,"%s"' % (column_name, index)]]
                    elif self.test_type == 'float':
                        line[column_name] = 1. / float(next(random_thing))
                    else:
                        line[column_name] = '%s %s %s' % (column_name,
                                                          next(random_thing),
                                                          index)
            yield line

    def get_rel_size(self, quals, columns):
        if self.test_type == 'planner':
            return (10000000, len(columns) * 10)
        return (20, len(columns) * 10)

    def get_path_keys(self):
        if self.test_type == 'planner':
            return [(('notion',), 1)]
        return []

    def can_sort(self, sortkeys):
        return sortkeys

    def update(self, rowid, newvalues):
        if self.test_type == 'nowrite':
            super(NotionDataWrapper, self).update(rowid, newvalues)
        log_to_postgres("UPDATING: %s with %s" % (
            rowid, sorted(newvalues.items())))
        if self.test_type == 'returning':
            for key in newvalues:
                newvalues[key] = "UPDATED: %s" % newvalues[key]
            return newvalues

    def delete(self, rowid):
        if self.test_type == 'nowrite':
            super(NotionDataWrapper, self).delete(rowid)
        log_to_postgres("DELETING: %s" % rowid)

    @property
    def rowid_column(self):
        return self._row_id_column

    def begin(self, serializable):
        if self.tx_hook:
            log_to_postgres('BEGIN')

    def sub_begin(self, level):
        if self.tx_hook:
            log_to_postgres('SUBBEGIN')

    def sub_rollback(self, level):
        if self.tx_hook:
            log_to_postgres('SUBROLLBACK')

    def sub_commit(self, level):
        if self.tx_hook:
            log_to_postgres('SUBCOMMIT')

    def commit(self):
        if self.tx_hook:
            log_to_postgres('COMMIT')

    def pre_commit(self):
        if self.tx_hook:
            log_to_postgres('PRECOMMIT')

    def rollback(self):
        if self.tx_hook:
            log_to_postgres('ROLLBACK')

    @classmethod
    def import_schema(self, schema, srv_options, options, restriction_type,
                      restricts):
        log_to_postgres("IMPORT %s FROM srv %s OPTIONS %s RESTRICTION: %s %s" %
                        (schema, srv_options, options, restriction_type,
                         restricts))
        tables = set([unicode_("imported_table_1"),
                      unicode_("imported_table_2"),
                      unicode_("imported_table_3")])
        if restriction_type == 'limit':
            tables = tables.intersection(set(restricts))
        elif restriction_type == 'except':
            tables = tables - set(restricts)
        rv = []
        for tname in sorted(list(tables)):
            table = TableDefinition(tname)
            nb_col = options.get('nb_col', 3)
            for col in range(nb_col):
                table.columns.append(
                    ColumnDefinition("col%s" % col,
                                     type_name="text",
                                     options={"option1": "value1"}))
            rv.append(table)
        return rv