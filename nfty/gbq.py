import traceback
import google.api_core.exceptions
from google.cloud import bigquery
import os
import datetime
import arrow

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = f"{os.getcwd()}/keys/gbq.json"
from bq_iterate import BqQueryRowIterator, batchify_iterator
import logging

logger = logging.getLogger('sdb_app')
job_config = bigquery.QueryJobConfig()
job_config.use_legacy_sql = False


class GOOGLE:
    def __init__(self, table=None):
        self.primary_keys = 1,3
        self.client = bigquery.Client()
        self.result = None
        self.description = None
        self.project = 'Viva'
        self.dataset = ''
        self.table = ''
        self.tablename = ''
        self.columns = []
        self.column_types = []
        self.set_schema()
        if table is not None:
            self._table(table)

    def _table(self, tablename):
        self.tablename = tablename
        self.table = self.client.get_table(f'{self.schema}.{tablename}')
        self.columns = [field.name for field in self.table.schema]
        self.column_types = [field.field_type for field in self.table.schema]
        return self

    def set_schema(self, tablename=None, project=None, dataset=None):
        self.project = project or self.project
        self.dataset = dataset or self.dataset
        self.schema = f'{self.project}.{self.dataset}'
        if tablename:
            self._table(tablename)
        return self

    def query(self, q, job_config=None):
        if not job_config:
            x = self.client.query(q)
            self.result = x.result()
        else:
            return self.client.query(q, job_config=job_config)
        return self

    def batch(self, q, slice=200000, size=200000):
        rows = BqQueryRowIterator(query=q, batch_size=size)
        return batchify_iterator(rows, batch_slice=slice)

    def to_list(self):
        def filter(line):
            x = []
            for _ in list(line):
                if isinstance(_, (datetime.datetime)):
                    _ = arrow.get(_).format('YYYY-MM-DD HH:mm:ss')
                if isinstance(_, (datetime.date)):
                    _ = arrow.get(_).format('YYYY-MM-DD')
                x.append(_ or '')
            return x
        return list(map(filter, self.result))

    def to_dict(self):
        return [dict(_) for _ in self.result]

    def get(self):
        if self.tablename in qry:
            self.result = self.client.query(qry[self.tablename]).result()
        else:
            self.result = self.client.query(qry['get'].format(f'{self.table}')).result()
        return self

    def tables(self):
        for t in  self.client.list_tables(self.schema):
            yield t.table_id

    def get_table(self, tablename):
        t = self.client.get_table(f'{self.schema}.{tablename}')
        return {'schema':t.schema, 'description':t.description, 'num_rows':t.num_rows}

    def add_column(self, column_name, typ="STRING"):
        original_schema = self.table.schema
        new_schema = original_schema[:]  # Creates a copy of the schema.
        new_schema.append(bigquery.SchemaField(column_name, typ))
        self.table.schema = new_schema
        self.table = self.client.update_table(self.table, ["schema"])
        if len(self.table.schema) == len(original_schema) + 1 == len(new_schema):
            return True
        else:
            return False

    def set_default_column(self, column_name, default):
        return self.query(f'ALTER TABLE `{self.table}` ALTER COLUMN {column_name} SET DEFAULT {default};')

    def truncate(self):
        if self.table:
            return self.client.query(f'''TRUNCATE TABLE {self.table}; ''').result()

    def delete_duplicates(self, col='created'):
        q = ''' CREATE OR REPLACE table {} as ( SELECT * FROM {} WHERE {} = (SELECT MAX({}) FROM {}) )'''
        self.client.query(q.format(self.table, self.table, col, col, self.table)).result()

    def delete_doctors(self):
        q = ''' DELETE FROM {} WHERE CONCAT(clinic_id, clinic_user_id, created) in (SELECT CONCAT(clinic_id, clinic_user_id, min(created)) FROM {} GROUP BY clinic_id, clinic_user_id HAVING count(*) > 1) '''
        self.client.query(q.format(self.table, self.table)).result()

    def insert(self, rows, JSON=False):
        try:
            if not JSON:
                e = self.client.insert_rows(self.table, rows)
            else:
                e = self.client.insert_rows_json(self.table, rows)
            return e
        except google.api_core.exceptions.NotFound:
            traceback.print_exc()


def curve_load():
    import os
    from google.cloud.bigquery import dbapi
    from google.cloud import bigquery
    import pandas as pd
    import re

    crentials_path = r"C:\Users\AdamRichmond\OneDrive - Specialty Dental Brands\Data Team\2022\Alteryx_GBQ Connection.json"
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = crentials_path
    client = bigquery.Client()
    conn = dbapi.Connection(client)
    curr = conn.cursor()
    curr.execute( "DROP Table specialty-dental-brands.business_intelligence.CurveAPIV2" )

    df = pd.read_csv(r"C:\Production\Data_API\data\ledger_entries.csv",
                     dtype={"tenantName": "string", "account": "string", "providerId": "string", "insurancePayer": "string", "postedOn": "string", "insuranceCode": "string", "description": "string", "transactionType": "string",
                            "adjustmentType": "string", "adjustmentCategory": "string"})

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV, skip_leading_rows=1)

    df['insurancePayer'] = df['insurancePayer'].fillna("")
    a = "{'name': '"

    result = re.search("{'name': '(.*)", a)
    logger.info(result.group(1))

    df['insurancePayer'] = df['insurancePayer'].replace(
        {"{'name':": result.group(1)}, regex=True)

    df['insurancePayer'] = df['insurancePayer'].map(
        lambda x: x.lstrip("\t ''").rstrip("'}"))

    df['insurancePayer'] = df['insurancePayer'].str.replace(r'\t', '')

    client.load_table_from_dataframe(
        df, 'specialty-dental-brands.business_intelligence.CurveAPIV2', job_config=job_config
    )


if __name__=='__main__':
    from pprint import pprint
    g = GOOGLE('Normalized_Doctor_Names')

    # g.set_schema(tablename='Normalized_Locations_Table')
    # g.get()
    # logger.info(g.columns)
    # # g.set_default_column('created', 'CURRENT_DATETIME()')
    # g.delete_duplicates()
    # # logger.info(x)
    # # g.insert('derek_test', [[3, 'Data', arrow.now().format('YYYY-MM-DD') ],[4,'DATA2', arrow.now().format('YYYY-MM-DD')]])
    # logger.info(g.to_list())