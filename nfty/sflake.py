#https://docs.snowflake.com/developer-guide/python-connector/python-connector
import traceback
from snowflake.connector import connect, DictCursor
import arrow
from nfty.queries.viva import qry
from streamlit import cache_resource
import nfty.db as db

report_dict = {
    "Charts": {'name': 'charts', 'resize': False, 'icon':'bar-chart'},
    "Employee Metrics": {'name': 'employee_metrics', 'resize': True, 'icon':'table'},
    "Patient Metrics": {'name': 'patient_metrics', 'resize': True, 'icon':'table'},
    "Patient Visits": {'name': 'patients_seen', 'resize': False, 'icon':'table'},
    "New Patients": {'name': 'new_patients', 'resize': False, 'icon':'table'},
    "Employees": {'name': 'employees', 'resize': True, 'icon':'table'},
    "Collections": {'name': 'collections', 'resize': False, 'icon':'table'},
    "Acuity": {'name': 'acuity', 'resize': False, 'icon':'table'},
    "PDN Payroll": {'name': 'payroll', 'resize': False, 'icon':'table'},
    'Target VS Staff Hours PDN':{'name': 'rollup', 'resize': True, 'icon':'table'},
    "OT Percentage": {'name': 'ot_percent', 'resize': False, 'icon':'table'},
    "Primary/Secondary Payor": {'name': 'payors', 'resize': True, 'icon':'table'},
    "GPM" : {'name': 'gpm', 'resize': True, 'icon':'table'},
    "Upload Report" : {'name': 'File', 'resize': True, 'icon':'upload'}
}


d_cols = {
    'YEAR': 'SET',
    'MONTH': 'SET',
    'YEAR_LAST': 'SET',
    'PERCENT_AUTH_USED':'SET',
    'WEEK_END': 'DATE',
    'DAY': 'DATE',
    'CLIENT_ID': 'DISTINCT',
    'USER_ID': 'DISTINCT',
    'EMPL_ID': 'DISTINCT',
    'CG_ID': 'DISTINCT',
    'CLIENTID': 'DISTINCT',
    'USERID': 'DISTINCT',
    'CGID': 'DISTINCT',
    'ESTATUS': ['Active','Applicant']
}

facility_names = ('All', 'Dallas', 'Austin', 'Ft Worth', 'Viva', 'Pediatric', 'Contracts', 'Richardson')

class API:
    def __init__(self, where):
        self.conn = connect(
            user='daas_reader@vivapeds.com',
            password='S8cuNRnbWJ',
            account='kantime-kt_viva',
            warehouse='VIVA_WH',
            database='KANTIME_PROD_DB'
        )
        self.schema = 'HH_REPORT_DS.'
        self.where = where

    @cache_resource
    def create_connection(self):
        return connect(
            user='daas_reader@vivapeds.com',
            password='S8cuNRnbWJ',
            account='kantime-kt_viva',
            warehouse='VIVA_WH',
            database='KANTIME_PROD_DB'
        )

    def table_info(self, table):
        cur = self.conn.cursor()
        try:
            return [(x[0], x[1]) for x in cur.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table}'")]
        except Exception as e:
            traceback.print_exc()
        finally:
            cur.close()

    def get(self, table):
        if '.' not in table:
            table = self.schema + table
        cur = self.conn.cursor(DictCursor)
        return [x for x in cur.execute(f'select * from {table}')]

    def execute_query(self, query, params=None):
        with self.conn.cursor() as cur:
            cur.execute(query, params)
            return cur.fetchall()

    def fetchwhere(self, qry, where):
        cur = self.conn.cursor(DictCursor)
        return cur.execute(qry, (where,))

    def fetchall(self, qry):
        cur = self.conn.cursor(DictCursor)
        return cur.execute(qry)

    def fetchone(self, qry):
        cur = self.conn.cursor(DictCursor)
        return cur.execute(qry)[0]

    def charts(self):
        q = qry.get('nurse_hours')
        nonnurses = q.format('''AND s.S_ACTUAL_END < DATEADD(DAY, -1, DATE_TRUNC('MONTH', CURRENT_DATE)) AND c.CG_DISCIPLINENAME NOT IN ('RN','LVN') ''').replace(", DATEADD('day', 6 - IFF(DAYOFWEEK(s.SCHEDULE_DATE) = 7, 0, DAYOFWEEK(s.SCHEDULE_DATE)), s.SCHEDULE_DATE)", '').replace(' AS WEEK_END', '')
        nurses = q.format('''AND s.S_ACTUAL_END < DATEADD(DAY, -1, DATE_TRUNC('MONTH', CURRENT_DATE)) AND c.CG_DISCIPLINENAME IN ('RN','LVN') ''').replace(", DATEADD('day', 6 - IFF(DAYOFWEEK(s.SCHEDULE_DATE) = 7, 0, DAYOFWEEK(s.SCHEDULE_DATE)), s.SCHEDULE_DATE)", '').replace(' AS WEEK_END', '')
        acuity = qry.get('acuity')
        return self.fetchall(nurses), self.fetchall(nonnurses), self.fetchall(acuity)

    def report(self, reportname):
        q = qry.get(reportname)
        if reportname == 'charts':
            q = q.format('''AND c.CG_DISCIPLINENAME IN ('RN','LVN') ''')
        if reportname == 'rollup':
            rows = db.fetchreturn(qry['rollup'], 'ROLLUP')
            output = []
            for k, v in rows.items():
                v.update({'WEEK_END': arrow.get(k).datetime})
                output.append(v)
            return output
        if not q and not self.where:
            return self.fetchall(qry['patients_seen'])
        if self.where:
            if self.where == 'All':
                return self.fetchall(q)
            else:
                return self.fetchwhere(q+' WHERE h.AGENCY_BRANCH_NAME = %s', where=self.where)
        if reportname == 'gpm':
            return self.fetchall(q.format(arrow.now().floor('month').format('YYYY-MM-DD'), arrow.now().format('YYYY-MM-DD')))
        return self.fetchall(q)

def create_month_year_index():
    index_dict = {}
    # Start from current month minus 2 years
    arrow_obj = arrow.now().floor('month').shift(years=-2)

    for i in range(0, 25):  # The range starts from 1 and goes up to 24
        # Save current date in dictionary with format 'MMM YY' and then shift to next month
        index_dict[i+1] = arrow_obj.format('MMM YY').upper()
        arrow_obj = arrow_obj.shift(months=1)

    return index_dict

def upload_file(name, df):
    return name

if __name__ == '__main__':
    from pprint import pprint
    x = create_month_year_index()
    pprint(x)
    print(list(x.values()))
    # pprint(s.table_info('CLIENTCENSUSBYDATE_SVW'))
