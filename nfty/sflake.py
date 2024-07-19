#https://docs.snowflake.com/developer-guide/python-connector/python-connector
import traceback
from pprint import pprint
import snowflake.connector
from snowflake.connector import connect, DictCursor



qry = {
    'patients_seen' : '''
SELECT YEAR(s.SCHEDULE_DATE) as YEAR, MONTH(S.SCHEDULE_DATE) AS MONTH,
    CAST(DATEADD(DAY, -1 - EXTRACT(DAYOFWEEK FROM s.SCHEDULE_DATE), DATE_TRUNC('DAY', s.SCHEDULE_DATE)) as VARCHAR) AS WEEK_OF,
    c.CG_EMPLOYEEID AS USERID, c.CG_FIRSTNAME AS FIRSTNAME, c.CG_LASTNAME AS LASTNAME, c.CG_DISCIPLINENAME as DISCIPLINE,
    COUNT(DISTINCT(s.s_CLIENT_ID)) as PATIENTS
FROM KANTIME_PROD_DB.HH_REPORT_DS.CAREGIVERMASTER_SVW as c
INNER JOIN KANTIME_PROD_DB.HH_REPORT_DS.SCHEDULEMASTER_SVW as s ON c.CG_EMPLOYEEID = s.S_CAREGIVER_ID
WHERE s.S_ACTUAL_END IS NOT NULL
-- AND s.SCHEDULE_DATE >= DATEADD('month', -12, CURRENT_DATE)
--AND (s.IS_SCHEDULE_BILLED = true or s.IS_SCHEDULE_PAID = true)
GROUP BY c.CG_EMPLOYEEID, c.CG_FIRSTNAME, c.CG_LASTNAME, c.CG_DISCIPLINENAME, CAST(DATEADD(DAY, -1 - EXTRACT(DAYOFWEEK FROM s.SCHEDULE_DATE), DATE_TRUNC('DAY', s.SCHEDULE_DATE)) as VARCHAR), YEAR(s.SCHEDULE_DATE), MONTH(S.SCHEDULE_DATE)
ORDER BY 3 DESC;''',

    'employee_metrics' : ''' 
SELECT YEAR(s.SCHEDULE_DATE) as YEAR, MONTH(S.SCHEDULE_DATE) AS MONTH,
    CAST(DATEADD(DAY, -1 - EXTRACT(DAYOFWEEK FROM s.SCHEDULE_DATE), DATE_TRUNC('DAY', s.SCHEDULE_DATE)) as VARCHAR) AS WEEK_OF,
    c.CG_EMPLOYEEID AS USERID, c.CG_FIRSTNAME AS FIRSTNAME, c.CG_LASTNAME AS LASTNAME, c.CG_DISCIPLINENAME as DISCIPLINE,
    SUM(s.S_ACTUAL_HOURS) as HOURS,
    ROUND(SUM(IFF(s.IS_SCHEDULE_BILLED, s.S_ACTUAL_HOURS * S.S_BILL_RATE, 0)),2) AS BILLED,
    ROUND(SUM(IFF(NOT s.IS_SCHEDULE_BILLED, s.S_ACTUAL_HOURS * S.S_BILL_RATE, 0)),2) AS UN_BILLED,
    ROUND(SUM(s.S_ACTUAL_HOURS * S.S_BILL_RATE),2) AS TOTAL,
    ROUND(SUM(S.S_OT_PAYROLLAMOUNT)) as OT,
    ROUND(SUM(S.S_OT_HOURS)) as OT_HOURS
FROM KANTIME_PROD_DB.HH_REPORT_DS.SCHEDULEMASTER_SVW AS s
JOIN KANTIME_PROD_DB.HH_REPORT_DS.CAREGIVERMASTER_SVW as c ON s.S_CAREGIVER_ID = c.CG_EMPLOYEEID
WHERE s.S_ACTUAL_END IS NOT NULL
AND s.S_WEEKSTART >= DATEADD('month', -24, CURRENT_DATE())
GROUP BY c.CG_EMPLOYEEID, c.CG_FIRSTNAME, c.CG_LASTNAME, c.CG_DISCIPLINENAME, CAST(DATEADD(DAY, -1 - EXTRACT(DAYOFWEEK FROM s.SCHEDULE_DATE), DATE_TRUNC('DAY', s.SCHEDULE_DATE)) as VARCHAR), YEAR(s.SCHEDULE_DATE), MONTH(S.SCHEDULE_DATE)
ORDER BY 3 DESC;''',

    'new_patients': '''
SELECT YEAR(s.SCHEDULE_DATE) as YEAR, MONTH(S.SCHEDULE_DATE) AS MONTH,
    CAST(DATEADD(DAY, -1 - EXTRACT(DAYOFWEEK FROM s.SCHEDULE_DATE), DATE_TRUNC('DAY', s.SCHEDULE_DATE)) as VARCHAR) AS WEEK_OF,
    c.CG_EMPLOYEEID AS USERID, c.CG_FIRSTNAME AS FIRSTNAME, c.CG_LASTNAME AS LASTNAME, c.CG_DISCIPLINENAME as DISCIPLINE,
    COUNT(DISTINCT(s.S_CLIENT_ID)) as PATIENTS
FROM KANTIME_PROD_DB.HH_REPORT_DS.SCHEDULEMASTER_SVW AS s
JOIN KANTIME_PROD_DB.HH_REPORT_DS.CAREGIVERMASTER_SVW as c ON s.S_CAREGIVER_ID = c.CG_EMPLOYEEID
JOIN KANTIME_PROD_DB.HH_REPORT_DS.CLIENTMASTER_SVW as u ON u.CLIENT_ID = s.S_CLIENT_ID
WHERE  s.S_ACTUAL_END IS NOT NULL
AND s.SCHEDULE_DATE >= DATEADD('month', -24, CURRENT_DATE)
AND WEEK(u.CLIENT_SOC_DATE) = WEEK(s.SCHEDULE_DATE)
GROUP BY c.CG_EMPLOYEEID, c.CG_FIRSTNAME, c.CG_LASTNAME, c.CG_DISCIPLINENAME, CAST(DATEADD(DAY, -1 - EXTRACT(DAYOFWEEK FROM s.SCHEDULE_DATE), DATE_TRUNC('DAY', s.SCHEDULE_DATE)) as VARCHAR), YEAR(s.SCHEDULE_DATE), MONTH(S.SCHEDULE_DATE)
ORDER BY 3 DESC;''',

    'collections': '''
SELECT YEAR(CLAIM_PAID_DATE) as YEAR, MONTH(CLAIM_PAID_DATE) AS MONTH,
       DATEADD(DAY, 0 - EXTRACT(DAYOFWEEK FROM CLAIM_PAID_DATE), DATE_TRUNC('DAY', CLAIM_PAID_DATE)) as WEEK_OF,
       ROUND(SUM(CLAIM_PAIDAMOUNT),2) as CLAIM_PAIDAMOUNT,
       ROUND(SUM(CLAIM_BALANCE),2) as BALANCE
FROM KANTIME_PROD_DB.HH_REPORT_DS.CLAIMSMASTER_SVW as c
WHERE DATEADD(DAY, 0 - EXTRACT(DAYOFWEEK FROM CLAIM_PAID_DATE), DATE_TRUNC('DAY', CLAIM_PAID_DATE)) >= DATEADD('month', -24, CURRENT_DATE())
GROUP BY YEAR(CLAIM_PAID_DATE), MONTH(CLAIM_PAID_DATE), DATEADD(DAY, 0 - EXTRACT(DAYOFWEEK FROM CLAIM_PAID_DATE), DATE_TRUNC('DAY', CLAIM_PAID_DATE)), DATEADD(DAY, 1 - EXTRACT(DAYOFWEEK FROM CLAIM_PAID_DATE), DATE_TRUNC('DAY', CLAIM_PAID_DATE))
ORDER BY 3 DESC;'''
}

report_dict = {
    "Patient Metrics": 'patients_seen',
    "Employee Metrics": 'employee_metrics',
    "New Patients": 'new_patients',
    "Collections": 'collections'
}

class API:
    def __init__(self):
        self.conn = connect(
            user='daas_reader@vivapeds.com',
            password='S8cuNRnbWJ',
            account='kantime-kt_viva',
            warehouse='VIVA_WH',
            database='KANTIME_PROD_DB'
        )
        self.schema = 'HH_REPORT_DS.'

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

    def fetchall(self, qry):
        cur = self.conn.cursor(DictCursor)
        return cur.execute(qry)

    def fetchone(self, qry):
        cur = self.conn.cursor(DictCursor)
        return cur.execute(qry)[0]

    def report(self, reportname):
        if reportname not in qry:
            return self.fetchall(qry['patients_seen'])
        return self.fetchall(qry[reportname])

if __name__ == '__main__':
    s = API()
    for row in s.get('CLIENTCENSUSBYDATE_SVW'):
        print(row)
    # pprint(s.table_info('CLIENTCENSUSBYDATE_SVW'))
