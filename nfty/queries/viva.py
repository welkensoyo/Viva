qry = {
    'patients_seen' : '''
SELECT YEAR(s.SCHEDULE_DATE) as YEAR, MONTH(S.SCHEDULE_DATE) AS MONTH, TO_CHAR(s.SCHEDULE_DATE,'MON') AS MONTH_ABBR,
    h.AGENCY_BRANCH_NAME, c.CG_EMPLOYEEID AS USERID, c.CG_FIRSTNAME AS FIRSTNAME, c.CG_LASTNAME AS LASTNAME, c.CG_DISCIPLINENAME as DISCIPLINE,
    CASE 
        WHEN TRIM(sc.SEVICE_CODE) IN ('PDN Mid Tech - LVN (BIPAP)', 'PDN RN HITech', 'PDN RN HiTech', 'PDN RN HiTECH', 'PDN Hi Tech - RN','PDN Mid Tech - RN (BIPAP)','PDN SHIFT RN', 'PDN Shift RN', 'PDN Shift - RN','PDN Shift LVN', 'PDN Shift LVN ', 'PDN Shift - LVN', 'PDN LVN HiTech', 'PDN LVN HiTECH', 'PDN Hi-Tech - LVN') THEN 'NURSE'
        WHEN TRIM(sc.SEVICE_CODE) in ('ST Eval','ST EVAL SOC','ST Feeding Eval','ST Feeding Visit','ST Re-Eval','ST Visit','PT Assistant','PT Eval','PT Eval high-complexity', 'PT Eval moderate-complexity','PT Eval low-complexity','PT Re-Eval','PT Supervision','PT Tele Visit','PT Visit','OT Eval','OT Eval low-complexity','OT Eval moderate-complexity','OT Eval high-complexity','OT Re-Eval','OT Visit') THEN 'THERAPY'
        ELSE 'NON BILLABLE'
    END AS SERVICE_FILTER,
    CASE 
        WHEN TRIM(sc.SEVICE_CODE) IN ('PDN RN HITech', 'PDN RN HiTech', 'PDN RN HiTECH', 'PDN Hi Tech - RN') THEN 'PDN RN HI TECH'
        WHEN TRIM(sc.SEVICE_CODE) = 'PDN Mid Tech - RN (BIPAP)' THEN 'PDN MID TECH - RN (BIPAP)'
        WHEN TRIM(sc.SEVICE_CODE) IN ('PDN SHIFT RN', 'PDN Shift RN', 'PDN Shift - RN') THEN 'PDN SHIFT RN'
        WHEN TRIM(sc.SEVICE_CODE) IN ('PDN Shift LVN', 'PDN Shift LVN ', 'PDN Shift - LVN') THEN 'PDN SHIFT LVN'
        WHEN TRIM(sc.SEVICE_CODE) IN ('PDN LVN HiTech', 'PDN LVN HiTECH', 'PDN Hi-Tech - LVN') THEN 'PDN LVN HI TECH'
        WHEN TRIM(sc.SEVICE_CODE) = 'PDN Mid Tech - LVN (BIPAP)' THEN 'PDN MID TECH - LVN (BIPAP)'
        WHEN TRIM(sc.SEVICE_CODE) in ('OT Eval','OT Eval low-complexity','OT Eval moderate-complexity','OT Eval high-complexity','OT Re-Eval','OT Visit') THEN 'OT'
        WHEN TRIM(sc.SEVICE_CODE) in ('PT Assistant','PT Eval','PT Eval high-complexity', 'PT Eval moderate-complexity','PT Eval low-complexity','PT Re-Eval','PT Supervision','PT Tele Visit','PT Visit') THEN 'PT'
        WHEN TRIM(sc.SEVICE_CODE) in ('ST Eval','ST EVAL SOC','ST Feeding Eval','ST Feeding Visit','ST Re-Eval','ST Visit') THEN 'ST'
        ELSE 'NON BILLABLE'
    END AS SERVICE_CODE,
    COUNT(DISTINCT(s.s_CLIENT_ID)) as PATIENTS
FROM KANTIME_PROD_DB.HH_REPORT_DS.CAREGIVERMASTER_SVW as c
JOIN KANTIME_PROD_DB.HH_REPORT_DS.SCHEDULEMASTER_SVW as s ON c.CG_EMPLOYEEID = s.S_CAREGIVER_ID
JOIN KANTIME_PROD_DB.HH_REPORT_DS.SERVICECODESMASTER_SVW as sc on sc.SERVICE_ID = s.S_SERVICE_CODE_ID
JOIN KANTIME_PROD_DB.HH_REPORT_DS.HOMEHEALTHAGENCIESBRANCHLIST_SVW as h ON c.CG_PAYROLL_BRANCH_ID = h.AGENCY_BRANCH_ID
WHERE s.S_ACTUAL_END IS NOT NULL
-- AND s.SCHEDULE_DATE >= DATEADD('month', -12, CURRENT_DATE)
--AND (s.IS_SCHEDULE_BILLED = true or s.IS_SCHEDULE_PAID = true)
GROUP BY c.CG_EMPLOYEEID, c.CG_FIRSTNAME, c.CG_LASTNAME, c.CG_DISCIPLINENAME, TO_CHAR(s.SCHEDULE_DATE,'MON'), YEAR(s.SCHEDULE_DATE), MONTH(S.SCHEDULE_DATE), sc.SEVICE_CODE, h.AGENCY_BRANCH_NAME
ORDER BY 3 DESC;''',
'patient_metrics' : '''
    SELECT YEAR(s.SCHEDULE_DATE) as YEAR, MONTH(S.SCHEDULE_DATE) AS MONTH, TO_CHAR(s.SCHEDULE_DATE,'MON') AS MONTH_ABBR,
    DATEADD('day', 6 - IFF(DAYOFWEEK(s.SCHEDULE_DATE) = 7, 0, DAYOFWEEK(s.SCHEDULE_DATE)), s.SCHEDULE_DATE) AS WEEK_END, 
    s.SCHEDULE_DATE,
    h.AGENCY_BRANCH_NAME, u.CLIENT_ID, u.CLIENT_FIRST_NAME as FIRST_NAME, u.CLIENT_LAST_NAME as LAST_NAME,
    c.CG_EMPLOYEEID AS CG_ID, c.CG_FIRSTNAME AS CG_FIRST_NAME, c.CG_LASTNAME AS CG_LAST_NAME,
    c.CG_DISCIPLINENAME as DISCIPLINE,
    s.SERVICE_TYPE,
    CASE 
        WHEN TRIM(sc.SEVICE_CODE) IN ('PDN Mid Tech - LVN (BIPAP)', 'PDN RN HITech', 'PDN RN HiTech', 'PDN RN HiTECH', 'PDN Hi Tech - RN','PDN Mid Tech - RN (BIPAP)','PDN SHIFT RN', 'PDN Shift RN', 'PDN Shift - RN','PDN Shift LVN', 'PDN Shift LVN ', 'PDN Shift - LVN', 'PDN LVN HiTech', 'PDN LVN HiTECH', 'PDN Hi-Tech - LVN') THEN 'NURSE'
        WHEN TRIM(sc.SEVICE_CODE) in ('ST Eval','ST EVAL SOC','ST Feeding Eval','ST Feeding Visit','ST Re-Eval','ST Visit','PT Assistant','PT Eval','PT Eval high-complexity', 'PT Eval moderate-complexity','PT Eval low-complexity','PT Re-Eval','PT Supervision','PT Tele Visit','PT Visit','OT Eval','OT Eval low-complexity','OT Eval moderate-complexity','OT Eval high-complexity','OT Re-Eval','OT Visit') THEN 'THERAPY'
        ELSE 'NON BILLABLE'
    END AS SERVICE_FILTER,
    CASE 
        WHEN TRIM(sc.SEVICE_CODE) IN ('PDN RN HITech', 'PDN RN HiTech', 'PDN RN HiTECH', 'PDN Hi Tech - RN') THEN 'PDN RN HI TECH'
        WHEN TRIM(sc.SEVICE_CODE) = 'PDN Mid Tech - RN (BIPAP)' THEN 'PDN MID TECH - RN (BIPAP)'
        WHEN TRIM(sc.SEVICE_CODE) IN ('PDN SHIFT RN', 'PDN Shift RN', 'PDN Shift - RN') THEN 'PDN SHIFT RN'
        WHEN TRIM(sc.SEVICE_CODE) IN ('PDN Shift LVN', 'PDN Shift LVN ', 'PDN Shift - LVN') THEN 'PDN SHIFT LVN'
        WHEN TRIM(sc.SEVICE_CODE) IN ('PDN LVN HiTech', 'PDN LVN HiTECH', 'PDN Hi-Tech - LVN') THEN 'PDN LVN HI TECH'
        WHEN TRIM(sc.SEVICE_CODE) = 'PDN Mid Tech - LVN (BIPAP)' THEN 'PDN MID TECH - LVN (BIPAP)'
        WHEN TRIM(sc.SEVICE_CODE) in ('OT Eval','OT Eval low-complexity','OT Eval moderate-complexity','OT Eval high-complexity','OT Re-Eval','OT Visit') THEN 'OT'
        WHEN TRIM(sc.SEVICE_CODE) in ('PT Assistant','PT Eval','PT Eval high-complexity', 'PT Eval moderate-complexity','PT Eval low-complexity','PT Re-Eval','PT Supervision','PT Tele Visit','PT Visit') THEN 'PT'
        WHEN TRIM(sc.SEVICE_CODE) in ('ST Eval','ST EVAL SOC','ST Feeding Eval','ST Feeding Visit','ST Re-Eval','ST Visit') THEN 'ST'
        ELSE 'NON BILLABLE'
    END AS SERVICE_CODE,
    sc.SEVICE_CODE as RAW_SERVICE_CODE,
    SUM(s.S_UNITS) as UNITS,
    SUM(ROUND(s.S_ACTUAL_HOURS / 0.25, 0) * 0.25) as ACTUAL_HOURS,
    ROUND(SUM(ROUND(s.S_ACTUAL_HOURS / 0.25, 0) * 0.25 * 100) / NULLIF(a.AUTH_WEEKLY_LIMIT / 4, 0), 2) as PERCENT_AUTH_USED,
    ROUND(SUM(s.S_CONTRACTUAL_AMOUNT),2) AS WEEKLY_GROSS_REVENUE,
    ROUND(SUM(IFF(s.IS_SCHEDULE_BILLED, s.S_BILLED_AMOUNT, 0)),2) AS BILLED,
    ROUND(SUM(IFF(NOT s.IS_SCHEDULE_BILLED, s.S_UNITS * S.S_BILL_RATE, 0)),2) AS UN_BILLED,
    ROUND(SUM(s.S_UNITS * S.S_BILL_RATE),2) AS TOTAL,
    ROUND(SUM(s.S_OT_PAYROLLAMOUNT),2) as OT_PAYROLL_AMOUNT,
    ROUND(SUM(s.S_OT_AMOUNT),2) as OT_AMOUNT,
    ROUND(SUM(ROUND(s.S_OT_HOURS / 0.25, 0) * 0.25),2) as OT_HOURS,
    NULLIF(a.AUTH_WEEKLY_LIMIT, 0) / 4 as AUTH_WEEKLY_LIMIT,
    a.AUTH_UNUSEDUNITS / 4 as AUTH_UNUSED_HOURS,
    a.AUTH_TOTALUNITS / 4 as AUTH_TOTAL
FROM KANTIME_PROD_DB.HH_REPORT_DS.SCHEDULEMASTER_SVW AS s
JOIN KANTIME_PROD_DB.HH_REPORT_DS.SERVICECODESMASTER_SVW as sc on sc.SERVICE_ID = s.S_SERVICE_CODE_ID
JOIN KANTIME_PROD_DB.HH_REPORT_DS.CAREGIVERMASTER_SVW as c ON s.S_CAREGIVER_ID = c.CG_EMPLOYEEID
JOIN KANTIME_PROD_DB.HH_REPORT_DS.CLIENTMASTER_SVW as u ON u.CLIENT_ID = s.S_CLIENT_ID
JOIN KANTIME_PROD_DB.HH_REPORT_DS.HOMEHEALTHAGENCIESBRANCHLIST_SVW as h ON u.AGENCY_BRANCH_ID = h.AGENCY_BRANCH_ID
JOIN KANTIME_PROD_DB.HH_REPORT_DS.AUTHORIZATIONMASTER_SVW as a ON a.AUTH_ID = s.S_AUTHORIZATION_ID
WHERE s.S_ACTUAL_END IS NOT NULL
AND s.S_WEEKSTART >= DATEADD('month', -24, CURRENT_DATE())
GROUP BY c.CG_EMPLOYEEID, c.CG_FIRSTNAME, c.CG_LASTNAME, c.CG_DISCIPLINENAME,h.AGENCY_BRANCH_NAME,
DATEADD('day', 6 - IFF(DAYOFWEEK(s.SCHEDULE_DATE) = 7, 0, DAYOFWEEK(s.SCHEDULE_DATE)), s.SCHEDULE_DATE),
YEAR(s.SCHEDULE_DATE), MONTH(S.SCHEDULE_DATE), TO_CHAR(s.SCHEDULE_DATE,'MON'), u.CLIENT_ID, u.CLIENT_FIRST_NAME, u.CLIENT_LAST_NAME,
sc.SERVICE_TYPE, sc.SEVICE_CODE,     a.AUTH_WEEKLY_LIMIT,a.AUTH_MONTHLY_LIMIT,a.AUTH_UNUSEDUNITS,a.AUTH_TOTALUNITS
ORDER BY 4 DESC;''',
    'employee_metrics' : ''' 
SELECT YEAR(s.SCHEDULE_DATE) as YEAR, MONTH(S.SCHEDULE_DATE) AS MONTH, TO_CHAR(s.SCHEDULE_DATE,'MON') AS MONTH_ABBR,
    DATEADD('day', 6 - IFF(DAYOFWEEK(s.SCHEDULE_DATE) = 7, 0, DAYOFWEEK(s.SCHEDULE_DATE)), s.SCHEDULE_DATE) AS WEEK_END,
    s.SCHEDULE_DATE,
    h.AGENCY_BRANCH_NAME, c.CG_EMPLOYEEID AS USERID, c.CG_FIRSTNAME AS FIRSTNAME, c.CG_LASTNAME AS LASTNAME, c.CG_DISCIPLINENAME as DISCIPLINE,
    SUM(s.S_UNITS) as UNITS,
    SUM(ROUND(s.S_ACTUAL_HOURS / 0.25, 0) * 0.25) as APPROVED_HOURS,
    sc.SERVICE_TYPE,
    s.S_SCHEDULE_STATUS as SCHEDULE_STATUS,
    CASE 
        WHEN TRIM(sc.SEVICE_CODE) IN ('PDN Mid Tech - LVN (BIPAP)', 'PDN RN HITech', 'PDN RN HiTech', 'PDN RN HiTECH', 'PDN Hi Tech - RN','PDN Mid Tech - RN (BIPAP)','PDN SHIFT RN', 'PDN Shift RN', 'PDN Shift - RN','PDN Shift LVN', 'PDN Shift LVN ', 'PDN Shift - LVN', 'PDN LVN HiTech', 'PDN LVN HiTECH', 'PDN Hi-Tech - LVN') THEN 'NURSE'
        WHEN TRIM(sc.SEVICE_CODE) in ('ST Eval','ST EVAL SOC','ST Feeding Eval','ST Feeding Visit','ST Re-Eval','ST Visit','PT Assistant','PT Eval','PT Eval high-complexity', 'PT Eval moderate-complexity','PT Eval low-complexity','PT Re-Eval','PT Supervision','PT Tele Visit','PT Visit','OT Eval','OT Eval low-complexity','OT Eval moderate-complexity','OT Eval high-complexity','OT Re-Eval','OT Visit') THEN 'THERAPY'
        ELSE 'NON BILLABLE'
    END AS SERVICE_FILTER,
    CASE 
        WHEN TRIM(sc.SEVICE_CODE) IN ('PDN RN HITech', 'PDN RN HiTech', 'PDN RN HiTECH', 'PDN Hi Tech - RN') THEN 'PDN RN HI TECH'
        WHEN TRIM(sc.SEVICE_CODE) = 'PDN Mid Tech - RN (BIPAP)' THEN 'PDN MID TECH - RN (BIPAP)'
        WHEN TRIM(sc.SEVICE_CODE) IN ('PDN SHIFT RN', 'PDN Shift RN', 'PDN Shift - RN') THEN 'PDN SHIFT RN'
        WHEN TRIM(sc.SEVICE_CODE) IN ('PDN Shift LVN', 'PDN Shift LVN ', 'PDN Shift - LVN') THEN 'PDN SHIFT LVN'
        WHEN TRIM(sc.SEVICE_CODE) IN ('PDN LVN HiTech', 'PDN LVN HiTECH', 'PDN Hi-Tech - LVN') THEN 'PDN LVN HI TECH'
        WHEN TRIM(sc.SEVICE_CODE) = 'PDN Mid Tech - LVN (BIPAP)' THEN 'PDN MID TECH - LVN (BIPAP)'
        WHEN TRIM(sc.SEVICE_CODE) in ('OT Eval','OT Eval low-complexity','OT Eval moderate-complexity','OT Eval high-complexity','OT Re-Eval','OT Visit') THEN 'OT'
        WHEN TRIM(sc.SEVICE_CODE) in ('PT Assistant','PT Eval','PT Eval high-complexity', 'PT Eval moderate-complexity','PT Eval low-complexity','PT Re-Eval','PT Supervision','PT Tele Visit','PT Visit') THEN 'PT'
        WHEN TRIM(sc.SEVICE_CODE) in ('ST Eval','ST EVAL SOC','ST Feeding Eval','ST Feeding Visit','ST Re-Eval','ST Visit') THEN 'ST'
        ELSE 'NON BILLABLE'
    END AS SERVICE_CODE,
    sc.SEVICE_CODE as RAW_SERVICE_CODE,
    ROUND(SUM(IFF(s.IS_SCHEDULE_BILLED, s.S_BILLED_AMOUNT, 0)),2) AS BILLED,
    ROUND(SUM(IFF(NOT s.IS_SCHEDULE_BILLED, s.S_UNITS * S.S_BILL_RATE, 0)),2) AS UN_BILLED,
    ROUND(SUM(s.S_UNITS * S.S_BILL_RATE),2) AS TOTAL,
    ROUND(SUM(S.S_OT_PAYROLLAMOUNT)) as OT,
    ROUND(SUM(S.S_OT_HOURS)) as OT_HOURS
FROM KANTIME_PROD_DB.HH_REPORT_DS.SCHEDULEMASTER_SVW AS s
JOIN KANTIME_PROD_DB.HH_REPORT_DS.SERVICECODESMASTER_SVW as sc on sc.SERVICE_ID = s.S_SERVICE_CODE_ID
JOIN KANTIME_PROD_DB.HH_REPORT_DS.CAREGIVERMASTER_SVW as c ON s.S_CAREGIVER_ID = c.CG_EMPLOYEEID
JOIN KANTIME_PROD_DB.HH_REPORT_DS.HOMEHEALTHAGENCIESBRANCHLIST_SVW as h ON c.CG_PAYROLL_BRANCH_ID = h.AGENCY_BRANCH_ID
WHERE s.S_ACTUAL_END IS NOT NULL
AND s.S_WEEKSTART >= DATEADD('month', -24, CURRENT_DATE())
GROUP BY c.CG_EMPLOYEEID, c.CG_FIRSTNAME, c.CG_LASTNAME, c.CG_DISCIPLINENAME,
DATEADD('day', 6 - IFF(DAYOFWEEK(s.SCHEDULE_DATE) = 7, 0, DAYOFWEEK(s.SCHEDULE_DATE)), s.SCHEDULE_DATE),
YEAR(s.SCHEDULE_DATE), MONTH(S.SCHEDULE_DATE), TO_CHAR(s.SCHEDULE_DATE,'MON'), h.AGENCY_BRANCH_NAME,
    sc.SERVICE_TYPE, sc.SEVICE_CODE, s.S_SCHEDULE_STATUS
ORDER BY 4 DESC;''',

    'new_patients': '''
SELECT YEAR(s.SCHEDULE_DATE) as YEAR, MONTH(S.SCHEDULE_DATE) AS MONTH, TO_CHAR(s.SCHEDULE_DATE,'MON') AS MONTH_ABBR,
    --CAST(DATEADD(DAY, -1 - EXTRACT(DAYOFWEEK FROM s.SCHEDULE_DATE), DATE_TRUNC('DAY', s.SCHEDULE_DATE)) as VARCHAR) AS WEEK_OF,
    h.AGENCY_BRANCH_NAME, c.CG_EMPLOYEEID AS USERID, c.CG_FIRSTNAME AS FIRSTNAME, c.CG_LASTNAME AS LASTNAME, c.CG_DISCIPLINENAME as DISCIPLINE,
    CASE 
        WHEN TRIM(sc.SEVICE_CODE) IN ('PDN Mid Tech - LVN (BIPAP)', 'PDN RN HITech', 'PDN RN HiTech', 'PDN RN HiTECH', 'PDN Hi Tech - RN','PDN Mid Tech - RN (BIPAP)','PDN SHIFT RN', 'PDN Shift RN', 'PDN Shift - RN','PDN Shift LVN', 'PDN Shift LVN ', 'PDN Shift - LVN', 'PDN LVN HiTech', 'PDN LVN HiTECH', 'PDN Hi-Tech - LVN') THEN 'NURSE'
        WHEN TRIM(sc.SEVICE_CODE) in ('ST Eval','ST EVAL SOC','ST Feeding Eval','ST Feeding Visit','ST Re-Eval','ST Visit','PT Assistant','PT Eval','PT Eval high-complexity', 'PT Eval moderate-complexity','PT Eval low-complexity','PT Re-Eval','PT Supervision','PT Tele Visit','PT Visit','OT Eval','OT Eval low-complexity','OT Eval moderate-complexity','OT Eval high-complexity','OT Re-Eval','OT Visit') THEN 'THERAPY'
        ELSE 'NON BILLABLE'
    END AS SERVICE_FILTER,
    CASE 
        WHEN TRIM(sc.SEVICE_CODE) IN ('PDN RN HITech', 'PDN RN HiTech', 'PDN RN HiTECH', 'PDN Hi Tech - RN') THEN 'PDN RN HI TECH'
        WHEN TRIM(sc.SEVICE_CODE) = 'PDN Mid Tech - RN (BIPAP)' THEN 'PDN MID TECH - RN (BIPAP)'
        WHEN TRIM(sc.SEVICE_CODE) IN ('PDN SHIFT RN', 'PDN Shift RN', 'PDN Shift - RN') THEN 'PDN SHIFT RN'
        WHEN TRIM(sc.SEVICE_CODE) IN ('PDN Shift LVN', 'PDN Shift LVN ', 'PDN Shift - LVN') THEN 'PDN SHIFT LVN'
        WHEN TRIM(sc.SEVICE_CODE) IN ('PDN LVN HiTech', 'PDN LVN HiTECH', 'PDN Hi-Tech - LVN') THEN 'PDN LVN HI TECH'
        WHEN TRIM(sc.SEVICE_CODE) = 'PDN Mid Tech - LVN (BIPAP)' THEN 'PDN MID TECH - LVN (BIPAP)'
        WHEN TRIM(sc.SEVICE_CODE) in ('OT Eval','OT Eval low-complexity','OT Eval moderate-complexity','OT Eval high-complexity','OT Re-Eval','OT Visit') THEN 'OT'
        WHEN TRIM(sc.SEVICE_CODE) in ('PT Assistant','PT Eval','PT Eval high-complexity', 'PT Eval moderate-complexity','PT Eval low-complexity','PT Re-Eval','PT Supervision','PT Tele Visit','PT Visit') THEN 'PT'
        WHEN TRIM(sc.SEVICE_CODE) in ('ST Eval','ST EVAL SOC','ST Feeding Eval','ST Feeding Visit','ST Re-Eval','ST Visit') THEN 'ST'
        ELSE 'NON BILLABLE'
    END AS SERVICE_CODE,
    COUNT(DISTINCT(s.S_CLIENT_ID)) as PATIENTS
FROM KANTIME_PROD_DB.HH_REPORT_DS.SCHEDULEMASTER_SVW AS s
JOIN KANTIME_PROD_DB.HH_REPORT_DS.SERVICECODESMASTER_SVW as sc ON sc.SERVICE_ID = s.S_SERVICE_CODE_ID
JOIN KANTIME_PROD_DB.HH_REPORT_DS.CAREGIVERMASTER_SVW as c ON s.S_CAREGIVER_ID = c.CG_EMPLOYEEID
JOIN KANTIME_PROD_DB.HH_REPORT_DS.CLIENTMASTER_SVW as u ON u.CLIENT_ID = s.S_CLIENT_ID
JOIN KANTIME_PROD_DB.HH_REPORT_DS.HOMEHEALTHAGENCIESBRANCHLIST_SVW as h ON c.CG_PAYROLL_BRANCH_ID = h.AGENCY_BRANCH_ID
WHERE  s.S_ACTUAL_END IS NOT NULL
AND s.SCHEDULE_DATE >= DATEADD('month', -24, CURRENT_DATE)
AND WEEK(u.CLIENT_SOC_DATE) = WEEK(s.SCHEDULE_DATE)
GROUP BY c.CG_EMPLOYEEID, c.CG_FIRSTNAME, c.CG_LASTNAME, c.CG_DISCIPLINENAME, sc.SEVICE_CODE, h.AGENCY_BRANCH_NAME,
--CAST(DATEADD(DAY, -1 - EXTRACT(DAYOFWEEK FROM s.SCHEDULE_DATE), DATE_TRUNC('DAY', s.SCHEDULE_DATE)) as VARCHAR), 
YEAR(s.SCHEDULE_DATE), MONTH(S.SCHEDULE_DATE), TO_CHAR(s.SCHEDULE_DATE,'MON')
ORDER BY 3 DESC;''',

    'collections': '''
SELECT YEAR(c.CLAIM_PAID_DATE) as YEAR, MONTH(c.CLAIM_PAID_DATE) AS MONTH, TO_CHAR(CLAIM_PAID_DATE,'MON') AS MONTH_ABBR, h.AGENCY_BRANCH_NAME,
       ROUND(SUM(c.CLAIM_PAIDAMOUNT),2) as CLAIM_PAIDAMOUNT,
       ROUND(SUM(c.CLAIM_BALANCE),2) as BALANCE
FROM KANTIME_PROD_DB.HH_REPORT_DS.CLAIMSMASTER_SVW as c
JOIN KANTIME_PROD_DB.HH_REPORT_DS.HOMEHEALTHAGENCIESBRANCHLIST_SVW as h ON c.AGENCY_BRANCH_ID = h.AGENCY_BRANCH_ID
WHERE c.CLAIM_PAID_DATE >= DATEADD('year', -1, DATE_TRUNC('year', CURRENT_DATE))
AND CLAIM_IS_CANCELLED = false
AND (CLAIM_IS_FULLY_PAID=true or CLAIM_IS_PARTIALLY_PAID=true)
GROUP BY YEAR(CLAIM_PAID_DATE), MONTH(CLAIM_PAID_DATE),TO_CHAR(CLAIM_PAID_DATE,'MON'), h.AGENCY_BRANCH_NAME
ORDER BY 1 DESC, 2 DESC''',

    'nurse_hours': '''
    SELECT
    -- Formatted date string 'month-year'
    MONTH(s.SCHEDULE_DATE) as MONTH,
    TO_CHAR(SCHEDULE_DATE,'MON') AS MONTH_ABBR
    , DATEADD('day', 6 - IFF(DAYOFWEEK(s.SCHEDULE_DATE) = 7, 0, DAYOFWEEK(s.SCHEDULE_DATE)), s.SCHEDULE_DATE) AS WEEK_END,
    -- c.CG_DISCIPLINENAME as DISCIPLINE,
    -- Sum values for the current year
    SUM(IFF(YEAR(s.SCHEDULE_DATE) = YEAR(CURRENT_DATE), s.S_ACTUAL_HOURS, NULL)) as HOURS_THIS_YEAR,
    ROUND(SUM(IFF(YEAR(s.SCHEDULE_DATE) = YEAR(CURRENT_DATE) AND s.IS_SCHEDULE_BILLED, s.S_BILLED_AMOUNT, NULL)),2) AS BILLED_THIS_YEAR,
    ROUND(SUM(IFF(YEAR(s.SCHEDULE_DATE) = YEAR(CURRENT_DATE) AND NOT s.IS_SCHEDULE_BILLED, s.S_UNITS * S.S_BILL_RATE, NULL)),2) AS UN_BILLED_THIS_YEAR,
    ROUND(SUM(IFF(YEAR(s.SCHEDULE_DATE) = YEAR(CURRENT_DATE), s.S_UNITS * S.S_BILL_RATE, NULL)),2) AS TOTAL_THIS_YEAR,
    ROUND(SUM(IFF(YEAR(s.SCHEDULE_DATE) = YEAR(CURRENT_DATE), S.S_OT_PAYROLLAMOUNT, NULL))) as OT_THIS_YEAR,
    ROUND(SUM(IFF(YEAR(s.SCHEDULE_DATE) = YEAR(CURRENT_DATE), S.S_OT_HOURS, NULL))) as OT_HOURS_THIS_YEAR,
    ROUND((SUM(IFF(YEAR(s.SCHEDULE_DATE) = YEAR(CURRENT_DATE), s.S_OT_HOURS, NULL)) / NULLIF(SUM(IFF(YEAR(s.SCHEDULE_DATE) = YEAR(CURRENT_DATE) AND s.IS_SCHEDULE_BILLED, s.S_ACTUAL_HOURS, NULL)), 0)) * 100, 2) AS "OT_%_THIS_YEAR",

    -- Sum values for the last year
    SUM(IFF(YEAR(s.SCHEDULE_DATE) = YEAR(CURRENT_DATE)-1, s.S_ACTUAL_HOURS, NULL)) as HOURS_LAST_YEAR,
    ROUND(SUM(IFF(YEAR(s.SCHEDULE_DATE) = YEAR(CURRENT_DATE)-1 AND s.IS_SCHEDULE_BILLED, s.S_BILLED_AMOUNT, NULL)),2) AS BILLED_LAST_YEAR,
    ROUND(SUM(IFF(YEAR(s.SCHEDULE_DATE) = YEAR(CURRENT_DATE)-1 AND NOT s.IS_SCHEDULE_BILLED, s.S_UNITS * s.S_BILL_RATE, NULL)),2) AS UN_BILLED_LAST_YEAR,
    ROUND(SUM(IFF(YEAR(s.SCHEDULE_DATE) = YEAR(CURRENT_DATE)-1, s.S_UNITS * S.S_BILL_RATE, NULL)),2) AS TOTAL_LAST_YEAR,
    ROUND(SUM(IFF(YEAR(s.SCHEDULE_DATE) = YEAR(CURRENT_DATE)-1, S.S_OT_PAYROLLAMOUNT, NULL))) as OT_LAST_YEAR,
    ROUND(SUM(IFF(YEAR(s.SCHEDULE_DATE) = YEAR(CURRENT_DATE)-1, S.S_OT_HOURS, NULL))) as OT_HOURS_LAST_YEAR,
    ROUND((SUM(IFF(YEAR(s.SCHEDULE_DATE) = YEAR(CURRENT_DATE)-1, s.S_OT_HOURS, NULL)) / NULLIF(SUM(IFF(YEAR(s.SCHEDULE_DATE) = YEAR(CURRENT_DATE)-1 AND s.IS_SCHEDULE_BILLED, s.S_ACTUAL_HOURS, NULL)), 0)) * 100, 2) AS "OT_%_LAST_YEAR"

FROM KANTIME_PROD_DB.HH_REPORT_DS.SCHEDULEMASTER_SVW AS s
JOIN KANTIME_PROD_DB.HH_REPORT_DS.SERVICECODESMASTER_SVW as sc ON sc.SERVICE_ID = s.S_SERVICE_CODE_ID
JOIN KANTIME_PROD_DB.HH_REPORT_DS.CAREGIVERMASTER_SVW as c
    ON s.S_CAREGIVER_ID = c.CG_EMPLOYEEID
WHERE
sc.SEVICE_CODE IN ('PDN RN HITech','PDN Mid Tech - RN (BIPAP)','PDN SHIFT RN','PDN Shift RN','PDN Shift LVN','PDN LVN HiTech','PDN Mid Tech - LVN (BIPAP)','PDN Shift LVN','PDN RN HiTech','PDN LVN HiTECH','PDN Shift - LVN','PDN RN HiTECH','PDN Hi-Tech - LVN','PDN Shift - RN','PDN Hi Tech - RN') 
AND s.S_ACTUAL_END IS NOT NULL
AND s.S_ACTUAL_END >= DATEADD('year', -2, DATE_TRUNC('year', CURRENT_DATE))
{}
-- GROUP BY Discipline and 'month-year' of the current and previous years
GROUP BY  MONTH(s.SCHEDULE_DATE), TO_CHAR(SCHEDULE_DATE,'MON'), DATEADD('day', 6 - IFF(DAYOFWEEK(s.SCHEDULE_DATE) = 7, 0, DAYOFWEEK(s.SCHEDULE_DATE)), s.SCHEDULE_DATE) --c.CG_DISCIPLINENAME,
ORDER BY 1 DESC;''',
    "acuity": '''SELECT a.YEAR, b.YEAR_LAST, a.MONTH, UPPER(a.MONTH_ABBR) as MONTH_ABBR, a.LOW_HOURS, a.HIGH_HOURS, a.HOURS, a.HIGH_HOURS/a.HOURS * 100 as "HIGH_ACUITY_%",
b.HIGH_HOURS/b.HOURS * 100 AS "PRIOR_YEAR_%", b.LOW_HOURS AS PRIOR_YEAR_LOW_HOURS, b.HIGH_HOURS AS PRIOR_YEAR_HIGH_HOURS, b.HOURS AS PRIOR_YEAR_HOURS
FROM (SELECT
    YEAR(s.SCHEDULE_DATE) as YEAR,
    MONTH(S.SCHEDULE_DATE) AS MONTH,
    TO_CHAR(s.SCHEDULE_DATE,'MON YY') AS MONTH_ABBR,
    SUM(CASE WHEN UPPER(sc.SEVICE_CODE) LIKE '%MID TECH%' OR UPPER(sc.SEVICE_CODE) LIKE '%HITECH' OR UPPER(sc.SEVICE_CODE) LIKE '%HI TECH' THEN s.S_ACTUAL_HOURS ELSE 0 END) as HIGH_HOURS,
    SUM(CASE WHEN UPPER(sc.SEVICE_CODE) NOT LIKE '%MID TECH%' OR UPPER(sc.SEVICE_CODE) NOT LIKE '%HITECH' OR UPPER(sc.SEVICE_CODE) NOT LIKE '%HI TECH' THEN s.S_ACTUAL_HOURS ELSE 0 END) as LOW_HOURS,
    SUM(s.S_ACTUAL_HOURS) as HOURS
FROM KANTIME_PROD_DB.HH_REPORT_DS.SCHEDULEMASTER_SVW as s
JOIN KANTIME_PROD_DB.HH_REPORT_DS.SERVICECODESMASTER_SVW as sc ON sc.SERVICE_ID = s.S_SERVICE_CODE_ID
WHERE s.S_ACTUAL_END IS NOT NULL
AND s.S_ACTUAL_END >= DATEADD('year', -1, CURRENT_DATE)
GROUP BY YEAR(s.SCHEDULE_DATE), MONTH(S.SCHEDULE_DATE), TO_CHAR(s.SCHEDULE_DATE,'MON YY')) as a
LEFT JOIN
(SELECT
    YEAR(s.SCHEDULE_DATE) as YEAR_LAST,
    MONTH(S.SCHEDULE_DATE) AS MONTH,
    TO_CHAR(s.SCHEDULE_DATE,'MON YY') AS MONTH_ABBR,
    SUM(CASE WHEN UPPER(sc.SEVICE_CODE) LIKE '%MID TECH%' OR UPPER(sc.SEVICE_CODE) LIKE '%HITECH' OR UPPER(sc.SEVICE_CODE) LIKE '%HI TECH' THEN s.S_ACTUAL_HOURS ELSE 0 END) as HIGH_HOURS,
    SUM(CASE WHEN UPPER(sc.SEVICE_CODE) NOT LIKE '%MID TECH%' OR UPPER(sc.SEVICE_CODE) NOT LIKE '%HITECH' OR UPPER(sc.SEVICE_CODE) NOT LIKE '%HI TECH' THEN s.S_ACTUAL_HOURS ELSE 0 END) as LOW_HOURS,
    SUM(s.S_ACTUAL_HOURS) as HOURS
FROM KANTIME_PROD_DB.HH_REPORT_DS.SCHEDULEMASTER_SVW as s
JOIN KANTIME_PROD_DB.HH_REPORT_DS.SERVICECODESMASTER_SVW as sc ON sc.SERVICE_ID = s.S_SERVICE_CODE_ID
WHERE s.S_ACTUAL_END IS NOT NULL
AND s.SCHEDULE_DATE >= DATE_TRUNC('MONTH', DATEADD('year', -2, CURRENT_DATE))
AND s.SCHEDULE_DATE < DATEADD('year', -1, LAST_DAY(CURRENT_DATE))
GROUP BY YEAR(s.SCHEDULE_DATE), MONTH(S.SCHEDULE_DATE), TO_CHAR(s.SCHEDULE_DATE,'MON YY')) as b ON a.MONTH = b.MONTH AND a.YEAR = b.YEAR_LAST+1
ORDER BY a.YEAR, a.MONTH''',
    'payroll' : '''
SELECT YEAR(s.SCHEDULE_DATE) as YEAR, MONTH(S.SCHEDULE_DATE) AS MONTH, TO_CHAR(s.SCHEDULE_DATE,'MON') AS MONTH_ABBR,
    DATEADD('day', 6 - IFF(DAYOFWEEK(s.SCHEDULE_DATE) = 7, 0, DAYOFWEEK(s.SCHEDULE_DATE)), s.SCHEDULE_DATE) AS WEEK_END,
    h.AGENCY_BRANCH_NAME, c.CG_EMPLOYEEID AS USERID, c.CG_FIRSTNAME AS FIRSTNAME, c.CG_LASTNAME AS LASTNAME, c.CG_DISCIPLINENAME as DISCIPLINE,
    sc.SERVICE_TYPE,
    CASE 
        WHEN TRIM(sc.SEVICE_CODE) IN ('PDN Mid Tech - LVN (BIPAP)', 'PDN RN HITech', 'PDN RN HiTech', 'PDN RN HiTECH', 'PDN Hi Tech - RN','PDN Mid Tech - RN (BIPAP)','PDN SHIFT RN', 'PDN Shift RN', 'PDN Shift - RN','PDN Shift LVN', 'PDN Shift LVN ', 'PDN Shift - LVN', 'PDN LVN HiTech', 'PDN LVN HiTECH', 'PDN Hi-Tech - LVN') THEN 'NURSE'
        WHEN TRIM(sc.SEVICE_CODE) in ('ST Eval','ST EVAL SOC','ST Feeding Eval','ST Feeding Visit','ST Re-Eval','ST Visit','PT Assistant','PT Eval','PT Eval high-complexity', 'PT Eval moderate-complexity','PT Eval low-complexity','PT Re-Eval','PT Supervision','PT Tele Visit','PT Visit','OT Eval','OT Eval low-complexity','OT Eval moderate-complexity','OT Eval high-complexity','OT Re-Eval','OT Visit') THEN 'THERAPY'
        ELSE 'NON BILLABLE'
    END AS SERVICE_FILTER,
    CASE 
        WHEN TRIM(sc.SEVICE_CODE) IN ('PDN RN HITech', 'PDN RN HiTech', 'PDN RN HiTECH', 'PDN Hi Tech - RN') THEN 'PDN RN HI TECH'
        WHEN TRIM(sc.SEVICE_CODE) = 'PDN Mid Tech - RN (BIPAP)' THEN 'PDN MID TECH - RN (BIPAP)'
        WHEN TRIM(sc.SEVICE_CODE) IN ('PDN SHIFT RN', 'PDN Shift RN', 'PDN Shift - RN') THEN 'PDN SHIFT RN'
        WHEN TRIM(sc.SEVICE_CODE) IN ('PDN Shift LVN', 'PDN Shift LVN ', 'PDN Shift - LVN') THEN 'PDN SHIFT LVN'
        WHEN TRIM(sc.SEVICE_CODE) IN ('PDN LVN HiTech', 'PDN LVN HiTECH', 'PDN Hi-Tech - LVN') THEN 'PDN LVN HI TECH'
        WHEN TRIM(sc.SEVICE_CODE) = 'PDN Mid Tech - LVN (BIPAP)' THEN 'PDN MID TECH - LVN (BIPAP)'
        WHEN TRIM(sc.SEVICE_CODE) in ('OT Eval','OT Eval low-complexity','OT Eval moderate-complexity','OT Eval high-complexity','OT Re-Eval','OT Visit') THEN 'OT'
        WHEN TRIM(sc.SEVICE_CODE) in ('PT Assistant','PT Eval','PT Eval high-complexity', 'PT Eval moderate-complexity','PT Eval low-complexity','PT Re-Eval','PT Supervision','PT Tele Visit','PT Visit') THEN 'PT'
        WHEN TRIM(sc.SEVICE_CODE) in ('ST Eval','ST EVAL SOC','ST Feeding Eval','ST Feeding Visit','ST Re-Eval','ST Visit') THEN 'ST'
        ELSE 'NON BILLABLE'
    END AS SERVICE_CODE,
        ROUND(SUM(s.S_REGULARAMOUNT + s.S_OT_AMOUNT + s.S_BONUS_AMOUNT),2) AS PAYROLL
FROM KANTIME_PROD_DB.HH_REPORT_DS.SCHEDULEMASTER_SVW AS s
JOIN KANTIME_PROD_DB.HH_REPORT_DS.SERVICECODESMASTER_SVW as sc on sc.SERVICE_ID = s.S_SERVICE_CODE_ID
JOIN KANTIME_PROD_DB.HH_REPORT_DS.CAREGIVERMASTER_SVW as c ON s.S_CAREGIVER_ID = c.CG_EMPLOYEEID
JOIN KANTIME_PROD_DB.HH_REPORT_DS.HOMEHEALTHAGENCIESBRANCHLIST_SVW as h ON c.CG_PAYROLL_BRANCH_ID = h.AGENCY_BRANCH_ID
WHERE s.S_ACTUAL_END IS NOT NULL
AND s.S_WEEKSTART >= DATEADD('month', -24, CURRENT_DATE())
GROUP BY c.CG_EMPLOYEEID, c.CG_FIRSTNAME, c.CG_LASTNAME, c.CG_DISCIPLINENAME,
DATEADD('day', 6 - IFF(DAYOFWEEK(s.SCHEDULE_DATE) = 7, 0, DAYOFWEEK(s.SCHEDULE_DATE)), s.SCHEDULE_DATE),
YEAR(s.SCHEDULE_DATE), MONTH(S.SCHEDULE_DATE), TO_CHAR(s.SCHEDULE_DATE,'MON'),sc.SERVICE_TYPE, sc.SEVICE_CODE,h.AGENCY_BRANCH_NAME
ORDER BY 4 DESC;
    ''',
    'ot_percent' : '''SELECT YEAR(s.SCHEDULE_DATE) as YEAR, MONTH(S.SCHEDULE_DATE) AS MONTH, TO_CHAR(s.SCHEDULE_DATE,'MON') AS MONTH_ABBR,
    h.AGENCY_BRANCH_NAME,
    SUM(s.S_ACTUAL_HOURS) as HOURS,
    SUM(s.S_OT_HOURS) as OT_HOURS,
    ROUND(SUM(s.S_OT_HOURS) / NULLIF(SUM(s.S_ACTUAL_HOURS), 0) * 100, 1) as PERCENT_OT

FROM KANTIME_PROD_DB.HH_REPORT_DS.SCHEDULEMASTER_SVW AS s
JOIN KANTIME_PROD_DB.HH_REPORT_DS.SERVICECODESMASTER_SVW as sc on sc.SERVICE_ID = s.S_SERVICE_CODE_ID
JOIN KANTIME_PROD_DB.HH_REPORT_DS.CAREGIVERMASTER_SVW as c ON s.S_CAREGIVER_ID = c.CG_EMPLOYEEID
JOIN KANTIME_PROD_DB.HH_REPORT_DS.HOMEHEALTHAGENCIESBRANCHLIST_SVW as h ON c.CG_PAYROLL_BRANCH_ID = h.AGENCY_BRANCH_ID
WHERE s.S_ACTUAL_END IS NOT NULL
AND s.S_WEEKSTART >= DATEADD('month', -24, CURRENT_DATE())
AND sc.SEVICE_CODE IN ('PDN RN HITech','PDN Mid Tech - RN (BIPAP)','PDN SHIFT RN','PDN Shift RN','PDN Shift LVN','PDN LVN HiTech','PDN Mid Tech - LVN (BIPAP)','PDN Shift LVN','PDN RN HiTech','PDN LVN HiTECH','PDN Shift - LVN','PDN RN HiTECH','PDN Hi-Tech - LVN','PDN Shift - RN','PDN Hi Tech - RN')
GROUP BY
YEAR(s.SCHEDULE_DATE), MONTH(S.SCHEDULE_DATE), TO_CHAR(s.SCHEDULE_DATE,'MON'), h.AGENCY_BRANCH_NAME
ORDER BY 4 DESC;''',
    'payors': '''
SELECT
    C.CLIENT_PATIENT_ID,
    C.CLIENT_FIRST_NAME,
    C.CLIENT_LAST_NAME,
    h.AGENCY_BRANCH_NAME,
    T.*
FROM
    (
        select
            P.PS_NAME Prm_Payer,
            P_S.PS_NAME Sec_Payer,
            A.*
        from
            (
                Select
                    Sec_Claim.ClientID,
                    prm_claim.Prm_InvoiceNo,
                    prm_Claim.Prm_ClaimID,
                    prm_Claim.Prm_CLAIM_ICN,
                    prm_Claim.Prm_Claim_Amount,
                    prm_Claim.Prm_ExpectedAmount,
                    prm_Claim.Prm_VisitDate,
                    prm_Claim.Prm_Claim_PaidAmount,
                    prm_Claim.prm_payerID,
                    prm_Claim.Prm_isMerged,
                    prm_Claim.Prm_IsParent,
                    prm_Claim.Prm_VisitID,
                    prm_Claim.Prm_ChildVisitID,
                    prm_Claim.Prm_LineItemAmount,
                    prm_Claim.Prm_LineItemPaidAmount,
                    prm_Claim.Prm_LineItemID,
                    prm_Claim.prm_ParentLineItemID,
                    prm_Claim.Prm_Visitserviceid,
                    prm_Claim.Prm_Balance,
                    sec_claim.sec_InvoiceNo,
                    sec_Claim.sec_ClaimID,
                    Sec_Claim.sec_CLAIM_ICN,
                    sec_Claim.Sec_Claim_Amount,
                    sec_Claim.sec_ExpectedAmount,
                    sec_Claim.sec_VisitDate,
                    sec_Claim.Sec_Claim_PaidAmount,
                    sec_Claim.sec_payerID,
                    sec_Claim.sec_isMerged,
                    sec_Claim.sec_IsParent,
                    sec_Claim.sec_VisitID,
                    sec_Claim.sec_ChildVisitID,
                    sec_Claim.sec_LineItemAmount,
                    sec_Claim.sec_LineItemPaidAmount,
                    sec_Claim.sec_LineItemID,
                    sec_Claim.sec_ParentLineItemID,
                    sec_Claim.sec_Visitserviceid,
                    sec_Claim.sec_Balance,
                    IsSecondary,
                    IsTransferCopay
                from
                    (
                        select
                            CM.CLIENT_ID ClientID,
                            CM.INVOICE_NUMBER as Sec_InvoiceNo,
                            CM.CLAIM_PARENT_CLAIMID as Prm_ClaimID,
                            CM.CLAIM_ICN as Prm_Claim_ICN,
                            CM.PAYERSOURCE_ID Sec_PayerID,
                            CM.CLAIM_ID as Sec_ClaimID,
                            CM.CLAIM_ICN as Sec_Claim_ICN,
                            CM.CLAIM_TOTALAMOUNT as Sec_Claim_Amount,
                            CM.CLAIM_CONTRACTUAL_AMOUNT as Sec_ExpectedAmount,
                            IFNULL(CM.CLAIM_PAIDAMOUNT, 0) as Sec_Claim_PaidAmount,
                            Cm.CLAIM_BALANCE as Sec_Balance,
                            CDM.CD_SCHEDULE_DATE Sec_VisitDate,
                            IFNULL(CDM.CD_SCHEDULE_ID, 0) as Sec_VisitID,
                            CDM.CD_CLAIM_DETAIL_ID Sec_LineItemID,
                            IFNULL(CDM.CD_CHILD_SCHEDULE_ID, 0) as Sec_ChildVisitID,
                            CDM.CD_START_TIME Sec_VisitStartTime,
                            IFNULL(CDM.CD_UNITS, 0) Sec_Unit,
                            CDM.CD_END_TIME Sec_VisitEndTime,
                            CDM.CD_BILLED_AMOUNT Sec_LineItemAmount,
                            IFNULL(CDM.CD_PAIDAMOUNT, 0) Sec_LineItemPaidAmount,
                            IFNULL(CDM.CD_IS_MERGED, false) as Sec_isMerged,
                            IFNULL(CDM.CD_MERGEDCLAIM_DETAIL_ID, 0) sec_ParentLineItemID,
                            case
                                when IFNULL(CDM.CD_IS_MERGED, false) = false THEN true
                                ELSE CASE
                                    when IFNULL(CDM.CD_IS_MERGED, false) = true
                                    and IFNULL(CDM.CD_MERGEDCLAIM_DETAIL_ID, 0) = 0 THEN true
                                    ELSE false
                                END
                            END as Sec_IsParent,
                            CDM.CD_SERVICE_CODE_ID as Sec_Visitserviceid,
                            IFNULL(CM.CLAIM_IS_SECONDARY, false) IsSecondary,
                            IFNULL(CM.CLAIM_IS_TRANSFERRED_COPAY, false) IsTransferCopay
                        from
                            KANTIME_PROD_DB.HH_REPORT_DS.CLAIMSMASTER_SVW CM
                            JOIN KANTIME_PROD_DB.HH_REPORT_DS.CLAIMDETAILS_SVW CDM on CM.CLAIM_ID = CDM.CD_CLAIM_ID
                        where
                            CM.CLAIM_STATUS != 'Deleted'
                            and (
                                (CM.CLAIM_IS_SECONDARY = true)
                                or CM.CLAIM_IS_TRANSFERRED_COPAY = true
                            ) -- and ((ISNULL(CDM.CD_IS_MERGED,0)=0) OR ((ISNULL(CDM.CD_IS_MERGED,0)=1) and ISNULL(CDM.CD_MERGEDCLAIM_DETAIL_ID,0)=0))
                    ) as Sec_Claim
                    JOIN (
                        select
                            CM.INVOICE_NUMBER as Prm_InvoiceNo,
                            CM.CLAIM_ID as Prm_ClaimID,
                            CM.CLAIM_ICN as Prm_Claim_ICN,
                            CM.PAYERSOURCE_ID Prm_PayerID,
                            IFNULL(CM.CLAIM_TOTALAMOUNT, 0) as Prm_Claim_Amount,
                            CM.CLAIM_CONTRACTUAL_AMOUNT as Prm_ExpectedAmount,
                            IFNULL(CM.CLAIM_PAIDAMOUNT, 0) as Prm_Claim_PaidAmount,
                            Cm.CLAIM_BALANCE as Prm_Balance,
                            CDM.CD_SCHEDULE_DATE Prm_VisitDate,
                            IFNULL(CDM.CD_SCHEDULE_ID, 0) as Prm_VisitID,
                            CDM.CD_CLAIM_DETAIL_ID Prm_LineItemID,
                            IFNULL(CDM.CD_CHILD_SCHEDULE_ID, 0) as Prm_ChildVisitID,
                            CDM.CD_START_TIME Prm_VisitStartTime,
                            IFNULL(CDM.CD_UNITS, 0) Prm_Unit,
                            CDM.CD_END_TIME Prm_VisitEndTime,
                            CDM.CD_BILLED_AMOUNT Prm_LineItemAmount,
                            IFNULL(CDM.CD_PAIDAMOUNT, 0) Prm_LineItemPaidAmount,
                            IFNULL(CDM.CD_IS_MERGED, false) as Prm_isMerged,
                            IFNULL(CDM.CD_MERGEDCLAIM_DETAIL_ID, 0) prm_ParentLineItemID,
                            case
                                when IFNULL(CDM.CD_IS_MERGED, false) = false THEN true
                                ELSE CASE
                                    when IFNULL(CDM.CD_IS_MERGED, false) = true
                                    and IFNULL(CDM.CD_MERGEDCLAIM_DETAIL_ID, 0) = 0 THEN true
                                    ELSE false
                                END
                            END as Prm_IsParent,
                            CDM.CD_SERVICE_CODE_ID as Prm_Visitserviceid
                        from
                            KANTIME_PROD_DB.HH_REPORT_DS.CLAIMSMASTER_SVW SECCLM
                            inner join KANTIME_PROD_DB.HH_REPORT_DS.CLAIMSMASTER_SVW CM ON SECCLM.CLAIM_PARENT_CLAIMID = CM.CLAIM_ID
                            inner join KANTIME_PROD_DB.HH_REPORT_DS.CLAIMDETAILS_SVW CDM on CM.CLAIM_ID = CDM.CD_CLAIM_ID
                        where
                            CM.CLAIM_STATUS != 'Deleted'
                            and SECCLM.CLAIM_STATUS != 'Deleted'
                            and (
                                (SECCLM.CLAIM_IS_SECONDARY = true)
                                or SECCLM.CLAIM_IS_TRANSFERRED_COPAY = true
                            ) -- and ((ISNULL(CDM.isMerged,0)=0) OR ((ISNULL(CDM.isMerged,0)=1) and ISNULL(CDM.MergedClaimDetailID,0)=0))
                    ) as prm_Claim on Sec_Claim.Prm_ClaimID = prm_Claim.Prm_ClaimID
                    and (
                        (
                            (Sec_Claim.Sec_VisitID = prm_Claim.Prm_VisitID)
                            AND (
                                Sec_Claim.Sec_ChildVisitID = prm_Claim.Prm_ChildVisitID
                            )
                            AND (
                                (
                                    Sec_isMerged = false
                                    and Prm_isMerged = false
                                )
                                OR (
                                    Sec_isMerged = true
                                    and Sec_IsParent = false
                                    and Prm_isMerged = true
                                    and Prm_IsParent = false
                                )
                            )
                        )
                        OR (
                            Sec_isMerged = true
                            and Sec_IsParent = true
                            and Prm_IsParent = true
                            and Prm_isMerged = true
                            and (prm_Claim.Prm_VisitDate = Sec_Claim.Sec_VisitDate)
                            and (prm_Claim.Prm_Unit = Sec_Claim.Sec_Unit)
                        )
                    )
                    --where Sec_IsParent=true
            ) A
            left join KANTIME_PROD_DB.HH_REPORT_DS.PAYMENTSOURCEMASTER_SVW P on A.Prm_PayerID = P.PS_PAYERSOURCEID
            left join KANTIME_PROD_DB.HH_REPORT_DS.PAYMENTSOURCEMASTER_SVW P_S on A.Sec_PayerID = P_S.PS_PAYERSOURCEID
    ) T
    JOIN KANTIME_PROD_DB.HH_REPORT_DS.CLIENTMASTER_SVW C on T.ClientID = C.client_id
    JOIN KANTIME_PROD_DB.HH_REPORT_DS.HOMEHEALTHAGENCIESBRANCHLIST_SVW as h ON C.AGENCY_BRANCH_ID = h.AGENCY_BRANCH_ID
    ''',
    'gpm' : '''
with cte1 (
        LOCATION,
        CLIENT_ID,
        HHA_BRANCH_ID,
        CLIENT_FIRST_NAME,
        CLIENT_LAST_NAME,
        PAYER_SOURCE_ID,
        PATIENT_ID,
        SCHEDULE_STATUS,
        SCHEDULE_DATE,
        DISCIPLINE,
        SERVICE_NAME,
        REGULAR_HOURS,
        REGULAR_PAY_RATE,
        REGULAR_PAYROLL_AMOUNT,
        OT_HOURS,
        OT_PAYROLL_RATE,
        OT_PAYROLL_AMOUNT,
        PAYABLE_HOURS,
        PAYROLL_AMOUNT,
        EXPECTED_AMOUNT,
        PROFIT,
        PROFIT_PERCENT
    ) as (
        select
            B.AGENCY_BRANCH_NAME,
            SM.S_CLIENT_ID,
            SM.S_HHA_BRANCH_ID,
            CM.CLIENT_FIRST_NAME,
            CM.CLIENT_LAST_NAME,
            SM.S_PAYER_SOURCE_ID,
            CM.CLIENT_PATIENT_ID,
            SM.S_SCHEDULE_STATUS,
            SCHEDULE_DATE,
            SC.SERVICE_TYPE,
            SC.SERVICE_NAME,
            SM.S_REGULAR_HOURS,
            SM.S_PAY_RATE,
            SM.S_REGULARAMOUNT,
            SM.S_OT_HOURS,
            SM.S_OT_PAYROLL_RATE,
            SM.S_OT_PAYROLLAMOUNT,
            COALESCE(
                NULLIF(
                    CASE
                        WHEN IFNULL(SM.S_PAYBLE_HOURS, '') = '' THEN 0.00
                        ELSE ROUND(
                            (
                                SUBSTR(SM.S_PAYBLE_HOURS, 1, 2) + (SUBSTR(SM.S_PAYBLE_HOURS, 4, 2) / 60)
                            ),
                            2
                        )
                    END,
                    0.00
                ),
                SM.S_ACTUAL_HOURS,
                SM.S_PLANNED_HOURS,
                0
            ),
            CASE
                WHEN IFNULL(SM.IS_SCHEDULE_PAID, FALSE) THEN SM.S_PAYROLLED_AMOUNT
                ELSE SM.S_COST
            END,
            SM.S_CONTRACTUAL_AMOUNT,
            SM.S_PROFIT_CONTRACTUAL,
            Sm.S_PROFITPERCENTAGE_EXPECTED
        from
            KANTIME_PROD_DB.HH_REPORT_DS.CLIENTMASTER_SVW CM
            inner join KANTIME_PROD_DB.HH_REPORT_DS.SCHEDULEMASTER_SVW SM on CM.CLIENT_ID = SM.S_CLIENT_ID
            inner join KANTIME_PROD_DB.HH_REPORT_DS.SERVICECODESMASTER_SVW SC on SM.S_SERVICE_CODE_ID = SC.SERVICE_ID
            left join KANTIME_PROD_DB.HH_REPORT_DS.HOMEHEALTHAGENCIESBRANCHLIST_SVW B on SM.S_HHA_BRANCH_ID = B.AGENCY_BRANCH_ID
        where
            SM.SCHEDULE_DATE between '{}'
            and '{}'
            and SM.IS_MISCVISIT = false
            and SM.IS_CHILD_VISIT = false
            and SM.S_SCHEDULE_STATUS in (
                'In_Progress',
                'Planned',
                'Approved',
                'Completed'
            ) --order by SM.SCHEDULE_DATE
    ),
    cte2 as (
        select
            P.PS_NAME as PAYER_NAME,
            cte1.LOCATION,
            cte1.CLIENT_ID,
            cte1.HHA_BRANCH_ID,
            cte1.CLIENT_FIRST_NAME,
            cte1.CLIENT_LAST_NAME,
            cte1.PAYER_SOURCE_ID,
            cte1.PATIENT_ID,
            cte1.SCHEDULE_STATUS,
            cte1.SCHEDULE_DATE,
            cte1.DISCIPLINE,
            cte1.SERVICE_NAME,
            CASE
                WHEN IFNULL(cte1.REGULAR_HOURS, 0.00) = 0.00 THEN IFNULL(cte1.PAYABLE_HOURS, 0.00) - IFNULL(cte1.OT_HOURS, 0.00)
                ELSE IFNULL(cte1.REGULAR_HOURS, 0.00)
            END as REGULAR_HOURS,
            cte1.REGULAR_PAY_RATE,
            CASE
                WHEN IFNULL(cte1.REGULAR_PAYROLL_AMOUNT, 0.00) = 0.00 THEN IFNULL(cte1.PAYROLL_AMOUNT, 0.00) - IFNULL(cte1.OT_PAYROLL_AMOUNT, 0.00)
                ELSE IFNULL(cte1.REGULAR_PAYROLL_AMOUNT, 0.00)
            END as REGULAR_PAYROLL_AMOUNT,
            cte1.OT_HOURS,
            cte1.OT_PAYROLL_RATE,
            cte1.OT_PAYROLL_AMOUNT,
            cte1.PAYABLE_HOURS,
            cte1.PAYROLL_AMOUNT,
            cte1.EXPECTED_AMOUNT,
            CASE
                WHEN IFNULL(cte1.PROFIT, 0.00) = 0.00 THEN IFNULL(cte1.EXPECTED_AMOUNT, 0.00) - IFNULL(cte1.PAYROLL_AMOUNT, 0.00)
                ELSE IFNULL(cte1.PROFIT, 0.00)
            END as PROFIT,
            CASE
                WHEN IFNULL(cte1.PROFIT_PERCENT, 0.00) = 0.00 THEN CASE
                    WHEN IFNULL(cte1.EXPECTED_AMOUNT, 0.00) = 0.00 THEN 0.00
                    ELSE ROUND(
                        (
                            (
                                (
                                    IFNULL(cte1.EXPECTED_AMOUNT, 0.00) - IFNULL(cte1.PAYROLL_AMOUNT, 0.00)
                                ) / IFNULL(cte1.EXPECTED_AMOUNT, 0.00)
                            ) * 100
                        ),
                        2
                    )
                END
                ELSE IFNULL(cte1.PROFIT_PERCENT, 0.00)
            END as PROFIT_PERCENT
        from
            cte1
            inner join KANTIME_PROD_DB.HH_REPORT_DS.PAYMENTSOURCEMASTER_SVW P on cte1.PAYER_SOURCE_ID = P.PS_PAYERSOURCEID
    )
select * from cte2 order by SCHEDULE_DATE''',
    "employees":'''
SELECT
    h.AGENCY_BRANCH_NAME as PAYROLL_BRANCH,
    c.CG_EMPLOYEEID as EMPL_ID,
    c.CG_FIRSTNAME as FIRST_NAME,
    c.CG_LASTNAME AS LAST_NAME,
    c.CG_STATUS as ESTATUS,
    c.CG_HIREDDATE AS HIRE_DATE,
    c.CG_MAILID AS EMAIL,
    c.CG_PHONE AS PHONE,
    c.CG_MOBILE AS MOBILE,
    c.CG_DISCIPLINENAME AS DISCIPLINE,
    c.CG_EMPLOYMENT_TYPE AS EMPLOYMENT_TYPE,
    c.CG_LOCATIONS as LOCATIONS,
    c.CG_LANGUAGE as LANGUAGE
FROM KANTIME_PROD_DB.HH_REPORT_DS.CAREGIVERMASTER_SVW AS c
JOIN KANTIME_PROD_DB.HH_REPORT_DS.HOMEHEALTHAGENCIESBRANCHLIST_SVW as h ON h.AGENCY_BRANCH_ID = c.CG_PAYROLL_BRANCH_ID
ORDER BY c.CG_HIREDDATE DESC NULLS LAST'''
}


