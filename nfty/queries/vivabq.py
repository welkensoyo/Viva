qry = {
    'patients_seen' : '''
SELECT /*+ NO_MERGE */
    EXTRACT(YEAR FROM s.schedule_date) as year,
    EXTRACT(MONTH FROM s.schedule_date) AS month,
    FORMAT_DATE('%b', s.schedule_date) AS month_abbr,
    h.agency_branch_name,
    c.cg_employeeid AS userid,
    CONCAT(c.cg_firstname, c.cg_lastname) AS `CLINICIAN NAME`,
    c.cg_disciplinename as discipline,
    CASE
        WHEN TRIM(sc.sevice_code) IN ('PDN Mid Tech - LVN (BIPAP)', 'PDN RN HITech', 'PDN RN HiTech', 'PDN RN HiTECH', 'PDN Hi Tech - RN','PDN Mid Tech - RN (BIPAP)','PDN SHIFT RN', 'PDN Shift RN', 'PDN Shift - RN','PDN Shift LVN', 'PDN Shift LVN ', 'PDN Shift - LVN', 'PDN LVN HiTech', 'PDN LVN HiTECH', 'PDN Hi-Tech - LVN') THEN 'NURSE'
        WHEN TRIM(sc.sevice_code) in ('STFEED','STEval','PTTELE','OT Eval low','PT Eval moderate','PTReEval','PT Eval noderate','PTA','PTDVN','OT Eval moderate','OTReEval','OTDVN','ST Eval moderate','STReEval','STFEEDDVN','STDVN','ST Eval','ST EVAL SOC','ST Feeding Eval','ST Feeding Visit','ST Re-Eval','ST Visit','PT Assistant','PT Eval','PT Eval high-complexity', 'PT Eval moderate-complexity','PT Eval low-complexity','PT Re-Eval','PT Supervision','PT Tele Visit','PT Visit','OT Eval','OT Eval low-complexity','OT Eval moderate-complexity','OT Eval high-complexity','OT Re-Eval','OT Visit') THEN 'THERAPY'
        ELSE 'NON BILLABLE'
    END AS service_filter,
    CASE
        WHEN TRIM(sc.sevice_code) IN ('PDN RN HITech', 'PDN RN HiTech', 'PDN RN HiTECH', 'PDN Hi Tech - RN') THEN 'PDN RN HI TECH'
        WHEN TRIM(sc.sevice_code) = 'PDN Mid Tech - RN (BIPAP)' THEN 'PDN MID TECH - RN (BIPAP)'
        WHEN TRIM(sc.sevice_code) IN ('PDN SHIFT RN', 'PDN Shift RN', 'PDN Shift - RN') THEN 'PDN SHIFT RN'
        WHEN TRIM(sc.sevice_code) IN ('PDN Shift LVN', 'PDN Shift LVN ', 'PDN Shift - LVN') THEN 'PDN SHIFT LVN'
        WHEN TRIM(sc.sevice_code) IN ('PDN LVN HiTech', 'PDN LVN HiTECH', 'PDN Hi-Tech - LVN') THEN 'PDN LVN HI TECH'
        WHEN TRIM(sc.sevice_code) = 'PDN Mid Tech - LVN (BIPAP)' THEN 'PDN MID TECH - LVN (BIPAP)'
        WHEN TRIM(sc.sevice_code) in ('OT Eval low','OT Eval moderate','OTReEval','OTDVN','OT Eval','OT Eval low-complexity','OT Eval moderate-complexity','OT Eval high-complexity','OT Re-Eval','OT Visit') THEN 'OT'
        WHEN TRIM(sc.sevice_code) in ('PTTELE','PT Eval moderate','PTReEval','PT Eval noderate','PTA','PTDVN','PT Assistant','PT Eval','PT Eval high-complexity', 'PT Eval moderate-complexity','PT Eval low-complexity','PT Re-Eval','PT Supervision','PT Tele Visit','PT Visit') THEN 'PT'
        WHEN TRIM(sc.sevice_code) in ('STFEED','STEval','ST Eval moderate','STReEval','STFEEDDVN','STDVN','ST Eval','ST EVAL SOC','ST Feeding Eval','ST Feeding Visit','ST Re-Eval','ST Visit') THEN 'ST'
        ELSE 'NON BILLABLE'
    END AS service_code,
    COUNT(DISTINCT(s.s_client_id)) as patients
FROM `prod-daas-viva`.`kt_analytics_hh_viva`.daas_dw_caregivermaster_vw as c
JOIN `prod-daas-viva`.`kt_analytics_hh_viva`.daas_dw_schedulemaster_vw as s ON c.cg_employeeid = s.s_caregiver_id
JOIN `prod-daas-viva`.`kt_analytics_hh_viva`.daas_dw_servicecodesmaster_vw as sc on sc.service_id = s.s_service_code_id
JOIN `prod-daas-viva`.`kt_analytics_hh_viva`.daas_dw_homehealthagenciesbranchlist_vw as h ON c.cg_payroll_branch_id = h.agency_branch_id
WHERE s.s_actual_end IS NOT NULL
AND DATE(s.schedule_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 5 YEAR)
-- AND s.schedule_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH)
--AND (s.is_schedule_billed = true or s.is_schedule_paid = true)
GROUP BY c.cg_employeeid, CONCAT(c.cg_firstname, c.cg_lastname), c.cg_disciplinename, FORMAT_DATE('%b', s.schedule_date),
    EXTRACT(YEAR FROM s.schedule_date), EXTRACT(MONTH FROM s.schedule_date), sc.sevice_code, h.agency_branch_name
ORDER BY 3 DESC;''',

'patient_metrics' : '''
WITH weekly_summary AS (
    SELECT
        s.s_client_id,
        s.s_caregiver_id,
        DATE_ADD(s.schedule_date, INTERVAL (6 - IF(EXTRACT(DAYOFWEEK FROM s.schedule_date) = 1, 0, EXTRACT(DAYOFWEEK FROM s.schedule_date) - 1)) DAY) AS week_end,
        SUM(s.s_units) AS weekly_units
    FROM
        `prod-daas-viva`.`kt_analytics_hh_viva`.daas_dw_schedulemaster_vw AS s
    WHERE DATE(s.schedule_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 YEAR)
    GROUP BY
        s.s_client_id,
        s.s_caregiver_id,
        DATE_ADD(s.schedule_date, INTERVAL (6 - IF(EXTRACT(DAYOFWEEK FROM s.schedule_date) = 1, 0, EXTRACT(DAYOFWEEK FROM s.schedule_date) - 1)) DAY)
),
rolling_60_day_summary AS (
    SELECT
        ws1.s_client_id,
        ws1.s_caregiver_id,
        ws1.week_end,
        (
            SELECT SUM(ws2.weekly_units)
            FROM weekly_summary ws2
            WHERE
                ws2.s_client_id = ws1.s_client_id
                AND ws2.s_caregiver_id = ws1.s_caregiver_id
                AND DATE(ws2.week_end) >= DATE(DATE_SUB(ws1.week_end, INTERVAL 60 DAY)) -- Last 60 days relative to ws1.week_end
                AND ws2.week_end <= ws1.week_end
        ) AS rolling_60_day_units
    FROM
        weekly_summary ws1
)

SELECT EXTRACT(YEAR FROM s.schedule_date) as year, EXTRACT(MONTH FROM s.schedule_date) AS month, FORMAT_DATE('%b', s.schedule_date) AS month_abbr,
    DATE_ADD(s.schedule_date, INTERVAL (6 - IF(EXTRACT(DAYOFWEEK FROM s.schedule_date) = 1, 0, EXTRACT(DAYOFWEEK FROM s.schedule_date) - 1)) DAY) AS week_end,
    s.schedule_date,
    h.agency_branch_name, u.client_id, CONCAT(u.client_first_name, u.client_last_name) as client_name,
    c.cg_employeeid AS cg_id, CONCAT(c.cg_firstname, c.cg_lastname) AS clinician_name,
    c.cg_disciplinename as discipline,
     CASE
        WHEN TRIM(sc.sevice_code) IN ('PDN Mid Tech - LVN (BIPAP)', 'PDN RN HITech', 'PDN RN HiTech', 'PDN RN HiTECH', 'PDN Hi Tech - RN','PDN Mid Tech - RN (BIPAP)','PDN SHIFT RN', 'PDN Shift RN', 'PDN Shift - RN','PDN Shift LVN', 'PDN Shift LVN ', 'PDN Shift - LVN', 'PDN LVN HiTech', 'PDN LVN HiTECH', 'PDN Hi-Tech - LVN') THEN 'NURSE'
        WHEN TRIM(sc.sevice_code) in ('STFEED','STEval','PTTELE','OT Eval low','PT Eval moderate','PTReEval','PT Eval noderate','PTA','PTDVN','OT Eval moderate','OTReEval','OTDVN','ST Eval moderate','STReEval','STFEEDDVN','STDVN','ST Eval','ST EVAL SOC','ST Feeding Eval','ST Feeding Visit','ST Re-Eval','ST Visit','PT Assistant','PT Eval','PT Eval high-complexity', 'PT Eval moderate-complexity','PT Eval low-complexity','PT Re-Eval','PT Supervision','PT Tele Visit','PT Visit','OT Eval','OT Eval low-complexity','OT Eval moderate-complexity','OT Eval high-complexity','OT Re-Eval','OT Visit') THEN 'THERAPY'
        ELSE 'NON BILLABLE'
    END AS service_filter,
    CASE
        WHEN TRIM(sc.sevice_code) IN ('PDN RN HITech', 'PDN RN HiTech', 'PDN RN HiTECH', 'PDN Hi Tech - RN') THEN 'PDN RN HI TECH'
        WHEN TRIM(sc.sevice_code) = 'PDN Mid Tech - RN (BIPAP)' THEN 'PDN MID TECH - RN (BIPAP)'
        WHEN TRIM(sc.sevice_code) IN ('PDN SHIFT RN', 'PDN Shift RN', 'PDN Shift - RN') THEN 'PDN SHIFT RN'
        WHEN TRIM(sc.sevice_code) IN ('PDN Shift LVN', 'PDN Shift LVN ', 'PDN Shift - LVN') THEN 'PDN SHIFT LVN'
        WHEN TRIM(sc.sevice_code) IN ('PDN LVN HiTech', 'PDN LVN HiTECH', 'PDN Hi-Tech - LVN') THEN 'PDN LVN HI TECH'
        WHEN TRIM(sc.sevice_code) = 'PDN Mid Tech - LVN (BIPAP)' THEN 'PDN MID TECH - LVN (BIPAP)'
        WHEN TRIM(sc.sevice_code) in ('OT Eval low','OT Eval moderate','OTReEval','OTDVN','OT Eval','OT Eval low-complexity','OT Eval moderate-complexity','OT Eval high-complexity','OT Re-Eval','OT Visit') THEN 'OT'
        WHEN TRIM(sc.sevice_code) in ('PTTELE','PT Eval moderate','PTReEval','PT Eval noderate','PTA','PTDVN','PT Assistant','PT Eval','PT Eval high-complexity', 'PT Eval moderate-complexity','PT Eval low-complexity','PT Re-Eval','PT Supervision','PT Tele Visit','PT Visit') THEN 'PT'
        WHEN TRIM(sc.sevice_code) in ('STFEED','STEval','ST Eval moderate','STReEval','STFEEDDVN','STDVN','ST Eval','ST EVAL SOC','ST Feeding Eval','ST Feeding Visit','ST Re-Eval','ST Visit') THEN 'ST'
        ELSE 'NON BILLABLE'
    END AS service_code,
    sc.sevice_code as raw_service_code,
    s.s_schedule_status,
    SUM(s.s_units) as units,
    SUM(ROUND(s.s_actual_hours / 0.25) * 0.25) as actual_hours,
    ROUND(SUM(ROUND(s.s_actual_hours / 0.25) * 0.25 * 100) / NULLIF(a.auth_weekly_limit / 4, 0), 2) as percent_auth_used,
    ROUND(SUM(s.s_contractual_amount), 2) AS weekly_gross_revenue,
    ROUND(SUM(IF(s.is_schedule_billed, s.s_billed_amount, 0)), 2) AS billed,
    ROUND(SUM(IF(NOT s.is_schedule_billed, s.s_units * s.s_bill_rate, 0)), 2) AS un_billed,
    ROUND(SUM(s.s_units * s.s_bill_rate), 2) AS total,
    ROUND(SUM(s.s_ot_payrollamount), 2) as ot_payroll_amount,
    ROUND(SUM(s.s_ot_amount), 2) as ot_amount,
    ROUND(SUM(ROUND(s.s_ot_hours / 0.25) * 0.25), 2) as ot_hours,
    NULLIF(a.auth_weekly_limit, 0) as auth_weekly_limit,
    a.auth_unusedunits as auth_unused_units,
    a.auth_totalunits as auth_total_units,
    w.rolling_60_day_units as last_60d_hours
FROM `prod-daas-viva`.`kt_analytics_hh_viva`.daas_dw_schedulemaster_vw AS s
JOIN `prod-daas-viva`.`kt_analytics_hh_viva`.daas_dw_servicecodesmaster_vw as sc on sc.service_id = s.s_service_code_id
JOIN `prod-daas-viva`.`kt_analytics_hh_viva`.daas_dw_caregivermaster_vw as c ON s.s_caregiver_id = c.cg_employeeid
JOIN `prod-daas-viva`.`kt_analytics_hh_viva`.daas_dw_clientmaster_vw as u ON u.client_id = s.s_client_id
JOIN `prod-daas-viva`.`kt_analytics_hh_viva`.daas_dw_homehealthagenciesbranchlist_vw as h ON u.agency_branch_id = h.agency_branch_id
JOIN `prod-daas-viva`.`kt_analytics_hh_viva`.daas_dw_authorizationmaster_vw as a ON a.auth_id = s.s_authorization_id
LEFT JOIN rolling_60_day_summary as w ON w.s_caregiver_id = s.s_caregiver_id AND w.s_client_id = u.client_id AND DATE_ADD(s.schedule_date, INTERVAL (6 - IF(EXTRACT(DAYOFWEEK FROM s.schedule_date) = 1, 0, EXTRACT(DAYOFWEEK FROM s.schedule_date) - 1)) DAY) = w.week_end
WHERE s.s_actual_end IS NOT NULL
AND DATE(s.s_weekstart) >= DATE_SUB(CURRENT_DATE(), INTERVAL 24 MONTH)
AND DATE(s.schedule_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 YEAR)
AND DATE(u.CLIENT_LASTAPPROVED_VISITDATE) >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 YEAR)
AND DATE(a.AUTH_START_DATE) >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 YEAR)
GROUP BY c.cg_employeeid, CONCAT(c.cg_firstname, c.cg_lastname), c.cg_disciplinename, h.agency_branch_name, s.s_schedule_status,
DATE_ADD(s.schedule_date, INTERVAL (6 - IF(EXTRACT(DAYOFWEEK FROM s.schedule_date) = 1, 0, EXTRACT(DAYOFWEEK FROM s.schedule_date) - 1)) DAY), s.schedule_date,
EXTRACT(YEAR FROM s.schedule_date), EXTRACT(MONTH FROM s.schedule_date), FORMAT_DATE('%b', s.schedule_date), u.client_id, u.client_first_name, u.client_last_name,
sc.service_type, sc.sevice_code, a.auth_weekly_limit, a.auth_monthly_limit, a.auth_unusedunits, a.auth_totalunits, w.rolling_60_day_units
ORDER BY week_end DESC''',

    'employee_metrics' : ''' 
SELECT EXTRACT(YEAR FROM s.schedule_date) as year,
       EXTRACT(MONTH FROM s.schedule_date) as month,
       FORMAT_DATE('%b', s.schedule_date) AS month_abbr,
       DATE_ADD(s.schedule_date, INTERVAL (6 - IF(EXTRACT(DAYOFWEEK FROM s.schedule_date) = 1, 7, EXTRACT(DAYOFWEEK FROM s.schedule_date) - 1)) DAY) AS week_end,
       s.schedule_date,
       h.agency_branch_name,
       c.cg_employeeid AS userid,
       CONCAT(c.cg_firstname, c.cg_lastname) AS `clinician name`,
       c.cg_disciplinename as discipline,
       SUM(s.s_units) as units,
       SUM(ROUND(s.s_actual_hours / 0.25) * 0.25) as approved_hours,
       sc.service_type,
       s.s_schedule_status as schedule_status,
       CASE
           WHEN TRIM(sc.sevice_code) IN ('PDN Mid Tech - LVN (BIPAP)', 'PDN RN HITech', 'PDN RN HiTech', 'PDN RN HiTECH', 'PDN Hi Tech - RN','PDN Mid Tech - RN (BIPAP)','PDN SHIFT RN', 'PDN Shift RN', 'PDN Shift - RN','PDN Shift LVN', 'PDN Shift LVN ', 'PDN Shift - LVN', 'PDN LVN HiTech', 'PDN LVN HiTECH', 'PDN Hi-Tech - LVN') THEN 'NURSE'
           WHEN TRIM(sc.sevice_code) in ('STFEED','STEval','PTTELE','OT Eval low','PT Eval moderate','PTReEval','PT Eval noderate','PTA','PTDVN','OT Eval moderate','OTReEval','OTDVN','ST Eval moderate','STReEval','STFEEDDVN','STDVN','ST Eval','ST EVAL SOC','ST Feeding Eval','ST Feeding Visit','ST Re-Eval','ST Visit','PT Assistant','PT Eval','PT Eval high-complexity', 'PT Eval moderate-complexity','PT Eval low-complexity','PT Re-Eval','PT Supervision','PT Tele Visit','PT Visit','OT Eval','OT Eval low-complexity','OT Eval moderate-complexity','OT Eval high-complexity','OT Re-Eval','OT Visit') THEN 'THERAPY'
           ELSE 'NON BILLABLE'
       END AS service_filter,
       CASE
           WHEN TRIM(sc.sevice_code) IN ('PDN RN HITech', 'PDN RN HiTech', 'PDN RN HiTECH', 'PDN Hi Tech - RN') THEN 'PDN RN HI TECH'
           WHEN TRIM(sc.sevice_code) = 'PDN Mid Tech - RN (BIPAP)' THEN 'PDN MID TECH - RN (BIPAP)'
           WHEN TRIM(sc.sevice_code) IN ('PDN SHIFT RN', 'PDN Shift RN', 'PDN Shift - RN') THEN 'PDN SHIFT RN'
           WHEN TRIM(sc.sevice_code) IN ('PDN Shift LVN', 'PDN Shift LVN ', 'PDN Shift - LVN') THEN 'PDN SHIFT LVN'
           WHEN TRIM(sc.sevice_code) IN ('PDN LVN HiTech', 'PDN LVN HiTECH', 'PDN Hi-Tech - LVN') THEN 'PDN LVN HI TECH'
           WHEN TRIM(sc.sevice_code) = 'PDN Mid Tech - LVN (BIPAP)' THEN 'PDN MID TECH - LVN (BIPAP)'
           WHEN TRIM(sc.sevice_code) in ('OT Eval low','OT Eval moderate','OTReEval','OTDVN','OT Eval','OT Eval low-complexity','OT Eval moderate-complexity','OT Eval high-complexity','OT Re-Eval','OT Visit') THEN 'OT'
           WHEN TRIM(sc.sevice_code) in ('PTTELE','PT Eval moderate','PTReEval','PT Eval noderate','PTA','PTDVN','PT Assistant','PT Eval','PT Eval high-complexity', 'PT Eval moderate-complexity','PT Eval low-complexity','PT Re-Eval','PT Supervision','PT Tele Visit','PT Visit') THEN 'PT'
           WHEN TRIM(sc.sevice_code) in ('STFEED','STEval','ST Eval moderate','STReEval','STFEEDDVN','STDVN','ST Eval','ST EVAL SOC','ST Feeding Eval','ST Feeding Visit','ST Re-Eval','ST Visit') THEN 'ST'
           ELSE 'NON BILLABLE'
       END AS service_code,
       sc.sevice_code as raw_service_code,
       ROUND(SUM(IF(s.is_schedule_billed, s.s_billed_amount, 0)), 2) AS billed,
       ROUND(SUM(IF(NOT s.is_schedule_billed, s.s_units * s.s_bill_rate, 0)), 2) AS un_billed,
       ROUND(SUM(s.s_units * s.s_bill_rate), 2) AS total,
       ROUND(SUM(s.s_ot_payrollamount)) as ot,
       ROUND(SUM(s.s_ot_hours)) as ot_hours
FROM `prod-daas-viva`.`kt_analytics_hh_viva`.daas_dw_schedulemaster_vw AS s
JOIN `prod-daas-viva`.`kt_analytics_hh_viva`.daas_dw_servicecodesmaster_vw as sc on sc.service_id = s.s_service_code_id
JOIN `prod-daas-viva`.`kt_analytics_hh_viva`.daas_dw_caregivermaster_vw as c ON s.s_caregiver_id = c.cg_employeeid
JOIN `prod-daas-viva`.`kt_analytics_hh_viva`.daas_dw_homehealthagenciesbranchlist_vw as h ON c.cg_payroll_branch_id = h.agency_branch_id
WHERE (s.s_actual_end IS NOT NULL OR s.s_schedule_status = 'MissedVisit')
  AND DATE(s.schedule_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 YEAR)
GROUP BY
  EXTRACT(YEAR FROM s.schedule_date),
  EXTRACT(MONTH FROM s.schedule_date),
  FORMAT_DATE('%b', s.schedule_date),
  DATE_ADD(s.schedule_date, INTERVAL (6 - IF(EXTRACT(DAYOFWEEK FROM s.schedule_date) = 1, 7, EXTRACT(DAYOFWEEK FROM s.schedule_date) - 1)) DAY),
  s.schedule_date,
  h.agency_branch_name,
  c.cg_employeeid,
  CONCAT(c.cg_firstname, c.cg_lastname),
  c.cg_disciplinename,
  sc.service_type,
  s.s_schedule_status,
  CASE
      WHEN TRIM(sc.sevice_code) IN ('PDN Mid Tech - LVN (BIPAP)', 'PDN RN HITech', 'PDN RN HiTech', 'PDN RN HiTECH', 'PDN Hi Tech - RN','PDN Mid Tech - RN (BIPAP)','PDN SHIFT RN', 'PDN Shift RN', 'PDN Shift - RN','PDN Shift LVN', 'PDN Shift LVN ', 'PDN Shift - LVN', 'PDN LVN HiTech', 'PDN LVN HiTECH', 'PDN Hi-Tech - LVN') THEN 'NURSE'
      WHEN TRIM(sc.sevice_code) in ('STFEED','STEval','PTTELE','OT Eval low','PT Eval moderate','PTReEval','PT Eval noderate','PTA','PTDVN','OT Eval moderate','OTReEval','OTDVN','ST Eval moderate','STReEval','STFEEDDVN','STDVN','ST Eval','ST EVAL SOC','ST Feeding Eval','ST Feeding Visit','ST Re-Eval','ST Visit','PT Assistant','PT Eval','PT Eval high-complexity', 'PT Eval moderate-complexity','PT Eval low-complexity','PT Re-Eval','PT Supervision','PT Tele Visit','PT Visit','OT Eval','OT Eval low-complexity','OT Eval moderate-complexity','OT Eval high-complexity','OT Re-Eval','OT Visit') THEN 'THERAPY'
      ELSE 'NON BILLABLE'
  END,
  CASE
      WHEN TRIM(sc.sevice_code) IN ('PDN RN HITech', 'PDN RN HiTech', 'PDN RN HiTECH', 'PDN Hi Tech - RN') THEN 'PDN RN HI TECH'
      WHEN TRIM(sc.sevice_code) = 'PDN Mid Tech - RN (BIPAP)' THEN 'PDN MID TECH - RN (BIPAP)'
      WHEN TRIM(sc.sevice_code) IN ('PDN SHIFT RN', 'PDN Shift RN', 'PDN Shift - RN') THEN 'PDN SHIFT RN'
      WHEN TRIM(sc.sevice_code) IN ('PDN Shift LVN', 'PDN Shift LVN ', 'PDN Shift - LVN') THEN 'PDN SHIFT LVN'
      WHEN TRIM(sc.sevice_code) IN ('PDN LVN HiTech', 'PDN LVN HiTECH', 'PDN Hi-Tech - LVN') THEN 'PDN LVN HI TECH'
      WHEN TRIM(sc.sevice_code) = 'PDN Mid Tech - LVN (BIPAP)' THEN 'PDN MID TECH - LVN (BIPAP)'
      WHEN TRIM(sc.sevice_code) in ('OT Eval low','OT Eval moderate','OTReEval','OTDVN','OT Eval','OT Eval low-complexity','OT Eval moderate-complexity','OT Eval high-complexity','OT Re-Eval','OT Visit') THEN 'OT'
      WHEN TRIM(sc.sevice_code) in ('PTTELE','PT Eval moderate','PTReEval','PT Eval noderate','PTA','PTDVN','PT Assistant','PT Eval','PT Eval high-complexity', 'PT Eval moderate-complexity','PT Eval low-complexity','PT Re-Eval','PT Supervision','PT Tele Visit','PT Visit') THEN 'PT'
      WHEN TRIM(sc.sevice_code) in ('STFEED','STEval','ST Eval moderate','STReEval','STFEEDDVN','STDVN','ST Eval','ST EVAL SOC','ST Feeding Eval','ST Feeding Visit','ST Re-Eval','ST Visit') THEN 'ST'
      ELSE 'NON BILLABLE'
  END,
  sc.sevice_code
  ORDER BY 4
''',

    'new_patients': '''
SELECT EXTRACT(YEAR FROM s.schedule_date) as year,
       EXTRACT(MONTH FROM s.schedule_date) AS month,
       FORMAT_DATE('%b', s.schedule_date) AS month_abbr,
    --CAST(DATE_SUB(DATE_TRUNC(s.schedule_date, DAY), INTERVAL (1 + EXTRACT(DAYOFWEEK FROM s.schedule_date)) DAY) as STRING) AS WEEK_OF,
    h.agency_branch_name,
    c.cg_employeeid AS userid,
    CONCAT(c.cg_firstname, c.cg_lastname) AS `clinician name`,
    c.cg_disciplinename as discipline,
    CASE
        WHEN TRIM(sc.sevice_code) IN ('PDN Mid Tech - LVN (BIPAP)', 'PDN RN HITech', 'PDN RN HiTech', 'PDN RN HiTECH', 'PDN Hi Tech - RN','PDN Mid Tech - RN (BIPAP)','PDN SHIFT RN', 'PDN Shift RN', 'PDN Shift - RN','PDN Shift LVN', 'PDN Shift LVN ', 'PDN Shift - LVN', 'PDN LVN HiTech', 'PDN LVN HiTECH', 'PDN Hi-Tech - LVN') THEN 'NURSE'
        WHEN TRIM(sc.sevice_code) in ('STFEED','STEval','PTTELE','OT Eval low','PT Eval moderate','PTReEval','PT Eval noderate','PTA','PTDVN','OT Eval moderate','OTReEval','OTDVN','ST Eval moderate','STReEval','STFEEDDVN','STDVN','ST Eval','ST EVAL SOC','ST Feeding Eval','ST Feeding Visit','ST Re-Eval','ST Visit','PT Assistant','PT Eval','PT Eval high-complexity', 'PT Eval moderate-complexity','PT Eval low-complexity','PT Re-Eval','PT Supervision','PT Tele Visit','PT Visit','OT Eval','OT Eval low-complexity','OT Eval moderate-complexity','OT Eval high-complexity','OT Re-Eval','OT Visit') THEN 'THERAPY'
        ELSE 'NON BILLABLE'
    END AS service_filter,
    CASE
        WHEN TRIM(sc.sevice_code) IN ('PDN RN HITech', 'PDN RN HiTech', 'PDN RN HiTECH', 'PDN Hi Tech - RN') THEN 'PDN RN HI TECH'
        WHEN TRIM(sc.sevice_code) = 'PDN Mid Tech - RN (BIPAP)' THEN 'PDN MID TECH - RN (BIPAP)'
        WHEN TRIM(sc.sevice_code) IN ('PDN SHIFT RN', 'PDN Shift RN', 'PDN Shift - RN') THEN 'PDN SHIFT RN'
        WHEN TRIM(sc.sevice_code) IN ('PDN Shift LVN', 'PDN Shift LVN ', 'PDN Shift - LVN') THEN 'PDN SHIFT LVN'
        WHEN TRIM(sc.sevice_code) IN ('PDN LVN HiTech', 'PDN LVN HiTECH', 'PDN Hi-Tech - LVN') THEN 'PDN LVN HI TECH'
        WHEN TRIM(sc.sevice_code) = 'PDN Mid Tech - LVN (BIPAP)' THEN 'PDN MID TECH - LVN (BIPAP)'
        WHEN TRIM(sc.sevice_code) in ('OT Eval low','OT Eval moderate','OTReEval','OTDVN','OT Eval','OT Eval low-complexity','OT Eval moderate-complexity','OT Eval high-complexity','OT Re-Eval','OT Visit') THEN 'OT'
        WHEN TRIM(sc.sevice_code) in ('PTTELE','PT Eval moderate','PTReEval','PT Eval noderate','PTA','PTDVN','PT Assistant','PT Eval','PT Eval high-complexity', 'PT Eval moderate-complexity','PT Eval low-complexity','PT Re-Eval','PT Supervision','PT Tele Visit','PT Visit') THEN 'PT'
        WHEN TRIM(sc.sevice_code) in ('STFEED','STEval','ST Eval moderate','STReEval','STFEEDDVN','STDVN','ST Eval','ST EVAL SOC','ST Feeding Eval','ST Feeding Visit','ST Re-Eval','ST Visit') THEN 'ST'
        ELSE 'NON BILLABLE'
    END AS service_code,
    COUNT(DISTINCT(s.s_client_id)) as patients
FROM `prod-daas-viva`.`kt_analytics_hh_viva`.daas_dw_schedulemaster_vw AS s
JOIN `prod-daas-viva`.`kt_analytics_hh_viva`.daas_dw_servicecodesmaster_vw as sc ON sc.service_id = s.s_service_code_id
JOIN `prod-daas-viva`.`kt_analytics_hh_viva`.daas_dw_caregivermaster_vw as c ON s.s_caregiver_id = c.cg_employeeid
JOIN `prod-daas-viva`.`kt_analytics_hh_viva`.daas_dw_clientmaster_vw as u ON u.client_id = s.s_client_id
JOIN `prod-daas-viva`.`kt_analytics_hh_viva`.daas_dw_homehealthagenciesbranchlist_vw as h ON c.cg_payroll_branch_id = h.agency_branch_id
WHERE s.s_actual_end IS NOT NULL
  AND DATE(s.schedule_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 24 MONTH)
  AND EXTRACT(WEEK FROM u.client_soc_date) = EXTRACT(WEEK FROM s.schedule_date)
GROUP BY
  EXTRACT(YEAR FROM s.schedule_date),
  EXTRACT(MONTH FROM s.schedule_date),
  FORMAT_DATE('%b', s.schedule_date),
  h.agency_branch_name,
  c.cg_employeeid,
  CONCAT(c.cg_firstname, c.cg_lastname),
  c.cg_disciplinename,
  CASE
      WHEN TRIM(sc.sevice_code) IN ('PDN Mid Tech - LVN (BIPAP)', 'PDN RN HITech', 'PDN RN HiTech', 'PDN RN HiTECH', 'PDN Hi Tech - RN','PDN Mid Tech - RN (BIPAP)','PDN SHIFT RN', 'PDN Shift RN', 'PDN Shift - RN','PDN Shift LVN', 'PDN Shift LVN ', 'PDN Shift - LVN', 'PDN LVN HiTech', 'PDN LVN HiTECH', 'PDN Hi-Tech - LVN') THEN 'NURSE'
      WHEN TRIM(sc.sevice_code) in ('STFEED','STEval','PTTELE','OT Eval low','PT Eval moderate','PTReEval','PT Eval noderate','PTA','PTDVN','OT Eval moderate','OTReEval','OTDVN','ST Eval moderate','STReEval','STFEEDDVN','STDVN','ST Eval','ST EVAL SOC','ST Feeding Eval','ST Feeding Visit','ST Re-Eval','ST Visit','PT Assistant','PT Eval','PT Eval high-complexity', 'PT Eval moderate-complexity','PT Eval low-complexity','PT Re-Eval','PT Supervision','PT Tele Visit','PT Visit','OT Eval','OT Eval low-complexity','OT Eval moderate-complexity','OT Eval high-complexity','OT Re-Eval','OT Visit') THEN 'THERAPY'
      ELSE 'NON BILLABLE'
  END,
  CASE
      WHEN TRIM(sc.sevice_code) IN ('PDN RN HITech', 'PDN RN HiTech', 'PDN RN HiTECH', 'PDN Hi Tech - RN') THEN 'PDN RN HI TECH'
      WHEN TRIM(sc.sevice_code) = 'PDN Mid Tech - RN (BIPAP)' THEN 'PDN MID TECH - RN (BIPAP)'
      WHEN TRIM(sc.sevice_code) IN ('PDN SHIFT RN', 'PDN Shift RN', 'PDN Shift - RN') THEN 'PDN SHIFT RN'
      WHEN TRIM(sc.sevice_code) IN ('PDN Shift LVN', 'PDN Shift LVN ', 'PDN Shift - LVN') THEN 'PDN SHIFT LVN'
      WHEN TRIM(sc.sevice_code) IN ('PDN LVN HiTech', 'PDN LVN HiTECH', 'PDN Hi-Tech - LVN') THEN 'PDN LVN HI TECH'
      WHEN TRIM(sc.sevice_code) = 'PDN Mid Tech - LVN (BIPAP)' THEN 'PDN MID TECH - LVN (BIPAP)'
      WHEN TRIM(sc.sevice_code) in ('OT Eval low','OT Eval moderate','OTReEval','OTDVN','OT Eval','OT Eval low-complexity','OT Eval moderate-complexity','OT Eval high-complexity','OT Re-Eval','OT Visit') THEN 'OT'
      WHEN TRIM(sc.sevice_code) in ('PTTELE','PT Eval moderate','PTReEval','PT Eval noderate','PTA','PTDVN','PT Assistant','PT Eval','PT Eval high-complexity', 'PT Eval moderate-complexity','PT Eval low-complexity','PT Re-Eval','PT Supervision','PT Tele Visit','PT Visit') THEN 'PT'
      WHEN TRIM(sc.sevice_code) in ('STFEED','STEval','ST Eval moderate','STReEval','STFEEDDVN','STDVN','ST Eval','ST EVAL SOC','ST Feeding Eval','ST Feeding Visit','ST Re-Eval','ST Visit') THEN 'ST'
      ELSE 'NON BILLABLE'
  END
ORDER BY month_abbr DESC''',

    'collections': '''
SELECT 
  EXTRACT(YEAR FROM c.claim_paid_date) as year, 
  EXTRACT(MONTH FROM c.claim_paid_date) AS month, 
  FORMAT_DATE('%b', c.claim_paid_date) AS month_abbr, 
  h.agency_branch_name,
  ROUND(SUM(c.claim_paidamount), 2) as claim_paidamount,
  ROUND(SUM(c.claim_balance), 2) as balance
FROM `prod-daas-viva`.`kt_analytics_hh_viva`.daas_dw_claimsmaster_vw as c
JOIN `prod-daas-viva`.`kt_analytics_hh_viva`.daas_dw_homehealthagenciesbranchlist_vw as h 
  ON c.agency_branch_id = h.agency_branch_id
WHERE 
  DATE(c.claim_paid_date) >= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), YEAR), INTERVAL 1 YEAR)
  AND DATE(c.claim_paid_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 5 YEAR)
  AND claim_is_cancelled = false
  AND (claim_is_fully_paid = true OR claim_is_partially_paid = true)
GROUP BY 
  EXTRACT(YEAR FROM c.claim_paid_date), 
  EXTRACT(MONTH FROM c.claim_paid_date),
  FORMAT_DATE('%b', c.claim_paid_date), 
  h.agency_branch_name
ORDER BY year DESC, month DESC''',

    'nurse_hours': '''
    SELECT
    -- Formatted date string 'month-year'
    EXTRACT(MONTH FROM s.schedule_date) as month,
    FORMAT_DATE('%b', s.schedule_date) AS month_abbr,
    DATE_ADD(s.schedule_date, INTERVAL (6 - IF(EXTRACT(DAYOFWEEK FROM s.schedule_date) = 1, 0, EXTRACT(DAYOFWEEK FROM s.schedule_date) - 1)) DAY) AS week_end,
    -- c.cg_disciplinename as discipline,
    -- Sum values for the current year
    SUM(IF(EXTRACT(YEAR FROM s.schedule_date) = EXTRACT(YEAR FROM LAST_DAY(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH))), s.s_actual_hours, NULL)) as hours_this_year,
    ROUND(SUM(IF(EXTRACT(YEAR FROM s.schedule_date) = EXTRACT(YEAR FROM LAST_DAY(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH))) AND s.is_schedule_billed, s.s_billed_amount, NULL)),2) AS billed_this_year,
    ROUND(SUM(IF(EXTRACT(YEAR FROM s.schedule_date) = EXTRACT(YEAR FROM LAST_DAY(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH))) AND NOT s.is_schedule_billed, s.s_units * s.s_bill_rate, NULL)),2) AS un_billed_this_year,
    ROUND(SUM(IF(EXTRACT(YEAR FROM s.schedule_date) = EXTRACT(YEAR FROM LAST_DAY(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH))), s.s_units * s.s_bill_rate, NULL)),2) AS total_this_year,
    ROUND(SUM(IF(EXTRACT(YEAR FROM s.schedule_date) = EXTRACT(YEAR FROM LAST_DAY(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH))), s.s_ot_payrollamount, NULL))) as ot_this_year,
    ROUND(SUM(IF(EXTRACT(YEAR FROM s.schedule_date) = EXTRACT(YEAR FROM LAST_DAY(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH))), s.s_ot_hours, NULL))) as ot_hours_this_year,
    ROUND((SUM(IF(EXTRACT(YEAR FROM s.schedule_date) = EXTRACT(YEAR FROM LAST_DAY(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH))), s.s_ot_hours, NULL)) / NULLIF(SUM(IF(EXTRACT(YEAR FROM s.schedule_date) = EXTRACT(YEAR FROM LAST_DAY(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH))) AND s.is_schedule_billed, s.s_actual_hours, NULL)), 0)) * 100, 2) AS `OT_%_THIS_YEAR`,

    -- Sum values for the last year
    SUM(IF(EXTRACT(YEAR FROM s.schedule_date) = EXTRACT(YEAR FROM LAST_DAY(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)))-1, s.s_actual_hours, NULL)) as hours_last_year,
    ROUND(SUM(IF(EXTRACT(YEAR FROM s.schedule_date) = EXTRACT(YEAR FROM LAST_DAY(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)))-1 AND s.is_schedule_billed, s.s_billed_amount, NULL)),2) AS billed_last_year,
    ROUND(SUM(IF(EXTRACT(YEAR FROM s.schedule_date) = EXTRACT(YEAR FROM LAST_DAY(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)))-1 AND NOT s.is_schedule_billed, s.s_units * s.s_bill_rate, NULL)),2) AS un_billed_last_year,
    ROUND(SUM(IF(EXTRACT(YEAR FROM s.schedule_date) = EXTRACT(YEAR FROM LAST_DAY(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)))-1, s.s_units * s.s_bill_rate, NULL)),2) AS total_last_year,
    ROUND(SUM(IF(EXTRACT(YEAR FROM s.schedule_date) = EXTRACT(YEAR FROM LAST_DAY(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)))-1, s.s_ot_payrollamount, NULL))) as ot_last_year,
    ROUND(SUM(IF(EXTRACT(YEAR FROM s.schedule_date) = EXTRACT(YEAR FROM LAST_DAY(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)))-1, s.s_ot_hours, NULL))) as ot_hours_last_year,
    ROUND((SUM(IF(EXTRACT(YEAR FROM s.schedule_date) = EXTRACT(YEAR FROM LAST_DAY(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)))-1, s.s_ot_hours, NULL)) / NULLIF(SUM(IF(EXTRACT(YEAR FROM s.schedule_date) = EXTRACT(YEAR FROM LAST_DAY(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)))-1 AND s.is_schedule_billed, s.s_actual_hours, NULL)), 0)) * 100, 2) AS `OT_%_LAST_YEAR`

FROM `prod-daas-viva`.`kt_analytics_hh_viva`.daas_dw_schedulemaster_vw AS s
JOIN `prod-daas-viva`.`kt_analytics_hh_viva`.daas_dw_servicecodesmaster_vw as sc ON sc.service_id = s.s_service_code_id
JOIN `prod-daas-viva`.`kt_analytics_hh_viva`.daas_dw_caregivermaster_vw as c
    ON s.s_caregiver_id = c.cg_employeeid
WHERE
TRIM(sc.sevice_code) IN ('PDN RN HITech','PDN Mid Tech - RN (BIPAP)','PDN SHIFT RN','PDN Shift RN','PDN Shift LVN','PDN LVN HiTech','PDN Mid Tech - LVN (BIPAP)','PDN Shift LVN','PDN RN HiTech','PDN LVN HiTECH','PDN Shift - LVN','PDN RN HiTECH','PDN Hi-Tech - LVN','PDN Shift - RN','PDN Hi Tech - RN')
AND s.s_actual_end IS NOT NULL
AND DATE(s.s_actual_end) >= DATE_SUB(DATE_TRUNC(DATE_SUB(LAST_DAY(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)), INTERVAL 1 MONTH), YEAR), INTERVAL 2 YEAR)
AND DATE(s.schedule_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 5 YEAR)
{}
-- GROUP BY Discipline and 'month-year' of the current and previous years
GROUP BY EXTRACT(MONTH FROM s.schedule_date), FORMAT_DATE('%b', s.schedule_date),
DATE_ADD(s.schedule_date, INTERVAL (6 - IF(EXTRACT(DAYOFWEEK FROM s.schedule_date) = 1, 0, EXTRACT(DAYOFWEEK FROM s.schedule_date) - 1)) DAY) --c.cg_disciplinename,
ORDER BY month DESC;''',

    "acuity": '''SELECT a.year, b.year_last, a.month, UPPER(a.month_abbr) as month_abbr, a.low_hours, a.high_hours, a.hours, a.high_hours/a.hours * 100 as `HIGH_ACUITY_%`,
b.high_hours/b.hours * 100 AS `PRIOR_YEAR_%`, b.low_hours AS prior_year_low_hours, b.high_hours AS prior_year_high_hours, b.hours AS prior_year_hours
FROM (SELECT
    EXTRACT(YEAR FROM s.schedule_date) as year,
    EXTRACT(MONTH FROM s.schedule_date) AS month,
    FORMAT_DATE('%b %y', s.schedule_date) AS month_abbr,
    SUM(CASE WHEN UPPER(sc.sevice_code) LIKE '%MID TECH%' OR UPPER(sc.sevice_code) LIKE '%HITECH' OR UPPER(sc.sevice_code) LIKE '%HI TECH' THEN s.s_actual_hours ELSE 0 END) as high_hours,
    SUM(CASE WHEN UPPER(sc.sevice_code) NOT LIKE '%MID TECH%' OR UPPER(sc.sevice_code) NOT LIKE '%HITECH' OR UPPER(sc.sevice_code) NOT LIKE '%HI TECH' THEN s.s_actual_hours ELSE 0 END) as low_hours,
    SUM(s.s_actual_hours) as hours
FROM `prod-daas-viva`.`kt_analytics_hh_viva`.daas_dw_schedulemaster_vw as s
JOIN `prod-daas-viva`.`kt_analytics_hh_viva`.daas_dw_servicecodesmaster_vw as sc ON sc.service_id = s.s_service_code_id
WHERE s.s_actual_end IS NOT NULL
AND DATE(s.s_actual_end) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)
AND DATE(s.schedule_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 YEAR)
GROUP BY EXTRACT(YEAR FROM s.schedule_date), EXTRACT(MONTH FROM s.schedule_date), FORMAT_DATE('%b %y', s.schedule_date)) as a
LEFT JOIN
(SELECT
    EXTRACT(YEAR FROM s.schedule_date) as year_last,
    EXTRACT(MONTH FROM s.schedule_date) AS month,
    FORMAT_DATE('%b %y', s.schedule_date) AS month_abbr,
    SUM(CASE WHEN UPPER(sc.sevice_code) LIKE '%MID TECH%' OR UPPER(sc.sevice_code) LIKE '%HITECH' OR UPPER(sc.sevice_code) LIKE '%HI TECH' THEN s.s_actual_hours ELSE 0 END) as high_hours,
    SUM(CASE WHEN UPPER(sc.sevice_code) NOT LIKE '%MID TECH%' OR UPPER(sc.sevice_code) NOT LIKE '%HITECH' OR UPPER(sc.sevice_code) NOT LIKE '%HI TECH' THEN s.s_actual_hours ELSE 0 END) as low_hours,
    SUM(s.s_actual_hours) as hours
FROM `prod-daas-viva`.`kt_analytics_hh_viva`.daas_dw_schedulemaster_vw as s
JOIN `prod-daas-viva`.`kt_analytics_hh_viva`.daas_dw_servicecodesmaster_vw as sc ON sc.service_id = s.s_service_code_id
WHERE s.s_actual_end IS NOT NULL
AND DATE(s.schedule_date) >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 2 YEAR), MONTH)
AND DATE(s.schedule_date) < DATE_SUB(LAST_DAY(CURRENT_DATE()), INTERVAL 1 YEAR)
GROUP BY EXTRACT(YEAR FROM s.schedule_date), EXTRACT(MONTH FROM s.schedule_date), FORMAT_DATE('%b %y', s.schedule_date)) as b ON a.month = b.month AND a.year = b.year_last+1
ORDER BY a.year, a.month''',

    'payroll' : '''
SELECT
    EXTRACT(YEAR FROM s.schedule_date) as year,
    EXTRACT(MONTH FROM s.schedule_date) AS month,
    FORMAT_DATE('%b', s.schedule_date) AS month_abbr,
    DATE_ADD(s.schedule_date, INTERVAL (6 - IF(EXTRACT(DAYOFWEEK FROM s.schedule_date) = 1, 0, EXTRACT(DAYOFWEEK FROM s.schedule_date) - 1)) DAY) AS week_end,
    h.agency_branch_name,
    c.cg_employeeid AS userid,
    CONCAT(c.cg_firstname, c.cg_lastname) AS `CLINICIAN NAME`,
    c.cg_disciplinename as discipline,
    sc.service_type,
    CASE
        WHEN TRIM(sc.sevice_code) IN ('PDN Mid Tech - LVN (BIPAP)', 'PDN RN HITech', 'PDN RN HiTech', 'PDN RN HiTECH', 'PDN Hi Tech - RN','PDN Mid Tech - RN (BIPAP)','PDN SHIFT RN', 'PDN Shift RN', 'PDN Shift - RN','PDN Shift LVN', 'PDN Shift LVN ', 'PDN Shift - LVN', 'PDN LVN HiTech', 'PDN LVN HiTECH', 'PDN Hi-Tech - LVN') THEN 'NURSE'
        WHEN TRIM(sc.sevice_code) in ('STFEED','STEval','PTTELE','OT Eval low','PT Eval moderate','PTReEval','PT Eval noderate','PTA','PTDVN','OT Eval moderate','OTReEval','OTDVN','ST Eval moderate','STReEval','STFEEDDVN','STDVN','ST Eval','ST EVAL SOC','ST Feeding Eval','ST Feeding Visit','ST Re-Eval','ST Visit','PT Assistant','PT Eval','PT Eval high-complexity', 'PT Eval moderate-complexity','PT Eval low-complexity','PT Re-Eval','PT Supervision','PT Tele Visit','PT Visit','OT Eval','OT Eval low-complexity','OT Eval moderate-complexity','OT Eval high-complexity','OT Re-Eval','OT Visit') THEN 'THERAPY'
        ELSE 'NON BILLABLE'
    END AS service_filter,
    CASE
        WHEN TRIM(sc.sevice_code) IN ('PDN RN HITech', 'PDN RN HiTech', 'PDN RN HiTECH', 'PDN Hi Tech - RN') THEN 'PDN RN HI TECH'
        WHEN TRIM(sc.sevice_code) = 'PDN Mid Tech - RN (BIPAP)' THEN 'PDN MID TECH - RN (BIPAP)'
        WHEN TRIM(sc.sevice_code) IN ('PDN SHIFT RN', 'PDN Shift RN', 'PDN Shift - RN') THEN 'PDN SHIFT RN'
        WHEN TRIM(sc.sevice_code) IN ('PDN Shift LVN', 'PDN Shift LVN ', 'PDN Shift - LVN') THEN 'PDN SHIFT LVN'
        WHEN TRIM(sc.sevice_code) IN ('PDN LVN HiTech', 'PDN LVN HiTECH', 'PDN Hi-Tech - LVN') THEN 'PDN LVN HI TECH'
        WHEN TRIM(sc.sevice_code) = 'PDN Mid Tech - LVN (BIPAP)' THEN 'PDN MID TECH - LVN (BIPAP)'
        WHEN TRIM(sc.sevice_code) in ('OT Eval low','OT Eval moderate','OTReEval','OTDVN','OT Eval','OT Eval low-complexity','OT Eval moderate-complexity','OT Eval high-complexity','OT Re-Eval','OT Visit') THEN 'OT'
        WHEN TRIM(sc.sevice_code) in ('PTTELE','PT Eval moderate','PTReEval','PT Eval noderate','PTA','PTDVN','PT Assistant','PT Eval','PT Eval high-complexity', 'PT Eval moderate-complexity','PT Eval low-complexity','PT Re-Eval','PT Supervision','PT Tele Visit','PT Visit') THEN 'PT'
        WHEN TRIM(sc.sevice_code) in ('STFEED','STEval','ST Eval moderate','STReEval','STFEEDDVN','STDVN','ST Eval','ST EVAL SOC','ST Feeding Eval','ST Feeding Visit','ST Re-Eval','ST Visit') THEN 'ST'
        ELSE 'NON BILLABLE'
    END AS service_code,
    ROUND(SUM(s.s_regularamount + s.s_ot_amount + s.s_bonus_amount),2) AS payroll
FROM `prod-daas-viva`.`kt_analytics_hh_viva`.daas_dw_schedulemaster_vw AS s
JOIN `prod-daas-viva`.`kt_analytics_hh_viva`.daas_dw_servicecodesmaster_vw as sc ON sc.service_id = s.s_service_code_id
JOIN `prod-daas-viva`.`kt_analytics_hh_viva`.daas_dw_caregivermaster_vw as c ON s.s_caregiver_id = c.cg_employeeid
JOIN `prod-daas-viva`.`kt_analytics_hh_viva`.daas_dw_homehealthagenciesbranchlist_vw as h ON c.cg_payroll_branch_id = h.agency_branch_id
WHERE s.s_actual_end IS NOT NULL
AND DATE(s.s_weekstart) >= DATE_SUB(CURRENT_DATE(), INTERVAL 24 MONTH)
AND DATE(s.schedule_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 5 YEAR)
GROUP BY c.cg_employeeid, CONCAT(c.cg_firstname, c.cg_lastname), c.cg_disciplinename,
DATE_ADD(s.schedule_date, INTERVAL (6 - IF(EXTRACT(DAYOFWEEK FROM s.schedule_date) = 1, 0, EXTRACT(DAYOFWEEK FROM s.schedule_date) - 1)) DAY),
EXTRACT(YEAR FROM s.schedule_date), EXTRACT(MONTH FROM s.schedule_date), FORMAT_DATE('%b', s.schedule_date), sc.service_type, sc.sevice_code, h.agency_branch_name
ORDER BY week_end DESC
    ''',

    'ot_percent' : '''SELECT
    EXTRACT(YEAR FROM s.schedule_date) as year,
    EXTRACT(MONTH FROM s.schedule_date) AS month,
    FORMAT_DATE('%b', s.schedule_date) AS month_abbr,
    h.agency_branch_name,
    SUM(s.s_actual_hours) as hours,
    SUM(s.s_ot_hours) as ot_hours,
    ROUND(SUM(s.s_ot_hours) / NULLIF(SUM(s.s_actual_hours), 0) * 100, 1) as percent_ot

FROM `prod-daas-viva`.`kt_analytics_hh_viva`.daas_dw_schedulemaster_vw AS s
JOIN `prod-daas-viva`.`kt_analytics_hh_viva`.daas_dw_servicecodesmaster_vw as sc ON sc.service_id = s.s_service_code_id
JOIN `prod-daas-viva`.`kt_analytics_hh_viva`.daas_dw_caregivermaster_vw as c ON s.s_caregiver_id = c.cg_employeeid
JOIN `prod-daas-viva`.`kt_analytics_hh_viva`.daas_dw_homehealthagenciesbranchlist_vw as h ON c.cg_payroll_branch_id = h.agency_branch_id
WHERE s.s_actual_end IS NOT NULL
AND DATE(s.s_weekstart) >= DATE_SUB(CURRENT_DATE(), INTERVAL 24 MONTH)
AND DATE(s.schedule_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 5 YEAR)
AND sc.sevice_code IN ('PDN RN HITech','PDN Mid Tech - RN (BIPAP)','PDN SHIFT RN','PDN Shift RN','PDN Shift LVN','PDN LVN HiTech','PDN Mid Tech - LVN (BIPAP)','PDN Shift LVN','PDN RN HiTech','PDN LVN HiTECH','PDN Shift - LVN','PDN RN HiTECH','PDN Hi-Tech - LVN','PDN Shift - RN','PDN Hi Tech - RN')
GROUP BY
EXTRACT(YEAR FROM s.schedule_date), EXTRACT(MONTH FROM s.schedule_date), FORMAT_DATE('%b', s.schedule_date), h.agency_branch_name
ORDER BY agency_branch_name DESC''',

    'weekly_hours': '''SELECT
    DATE_ADD(s.schedule_date, INTERVAL (6 - IF(EXTRACT(DAYOFWEEK FROM s.schedule_date) = 1, 0, EXTRACT(DAYOFWEEK FROM s.schedule_date) - 1)) DAY) AS week_end,
    h.agency_branch_name,
    SUM(ROUND(s.s_actual_hours / 0.25, 0) * 0.25) as actual_hours
FROM `prod-daas-viva`.`kt_analytics_hh_viva`.daas_dw_schedulemaster_vw AS s
JOIN `prod-daas-viva`.`kt_analytics_hh_viva`.daas_dw_servicecodesmaster_vw as sc ON sc.service_id = s.s_service_code_id
JOIN `prod-daas-viva`.`kt_analytics_hh_viva`.daas_dw_caregivermaster_vw as c ON s.s_caregiver_id = c.cg_employeeid
JOIN `prod-daas-viva`.`kt_analytics_hh_viva`.daas_dw_clientmaster_vw as u ON u.client_id = s.s_client_id
JOIN `prod-daas-viva`.`kt_analytics_hh_viva`.daas_dw_homehealthagenciesbranchlist_vw as h ON u.agency_branch_id = h.agency_branch_id
WHERE s.s_actual_end IS NOT NULL
AND DATE(s.s_weekstart) >= PARSE_DATE('%m/%d/%Y', '10/1/2024')
AND DATE(s.schedule_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 5 YEAR)
AND TRIM(sc.sevice_code) IN ('PDN Mid Tech - LVN (BIPAP)', 'PDN RN HITech', 'PDN RN HiTech', 'PDN RN HiTECH', 'PDN Hi Tech - RN','PDN Mid Tech - RN (BIPAP)','PDN SHIFT RN', 'PDN Shift RN', 'PDN Shift - RN','PDN Shift LVN', 'PDN Shift LVN ', 'PDN Shift - LVN', 'PDN LVN HiTech', 'PDN LVN HiTECH', 'PDN Hi-Tech - LVN')
GROUP BY
DATE_ADD(s.schedule_date, INTERVAL (6 - IF(EXTRACT(DAYOFWEEK FROM s.schedule_date) = 1, 0, EXTRACT(DAYOFWEEK FROM s.schedule_date) - 1)) DAY), h.agency_branch_name''',

    'payors': '''
SELECT
    C.CLIENT_PATIENT_ID,
    C.CLIENT_FIRST_NAME,
    C.CLIENT_LAST_NAME,
    h.AGENCY_BRANCH_NAME,
    T.*
FROM
    (
        SELECT
            P.PS_NAME Prm_Payer,
            P_S.PS_NAME Sec_Payer,
            A.*
        FROM
            (
                SELECT
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
                FROM
                    (
                        SELECT
                            CM.CLIENT_ID ClientID,
                            CM.INVOICE_NUMBER AS Sec_InvoiceNo,
                            CM.CLAIM_PARENT_CLAIMID AS Prm_ClaimID,
                            CM.CLAIM_ICN AS Prm_Claim_ICN,
                            CM.PAYERSOURCE_ID Sec_PayerID,
                            CM.CLAIM_ID AS Sec_ClaimID,
                            CM.CLAIM_ICN AS Sec_Claim_ICN,
                            CM.CLAIM_TOTALAMOUNT AS Sec_Claim_Amount,
                            CM.CLAIM_CONTRACTUAL_AMOUNT AS Sec_ExpectedAmount,
                            IFNULL(CM.CLAIM_PAIDAMOUNT, 0) AS Sec_Claim_PaidAmount,
                            CM.CLAIM_BALANCE AS Sec_Balance,
                            CDM.CD_SCHEDULE_DATE Sec_VisitDate,
                            IFNULL(CDM.CD_SCHEDULE_ID, 0) AS Sec_VisitID,
                            CDM.CD_CLAIM_DETAIL_ID Sec_LineItemID,
                            IFNULL(CDM.CD_CHILD_SCHEDULE_ID, 0) AS Sec_ChildVisitID,
                            CDM.CD_START_TIME Sec_VisitStartTime,
                            IFNULL(CDM.CD_UNITS, 0) Sec_Unit,
                            CDM.CD_END_TIME Sec_VisitEndTime,
                            CDM.CD_BILLED_AMOUNT Sec_LineItemAmount,
                            IFNULL(CDM.CD_PAIDAMOUNT, 0) Sec_LineItemPaidAmount,
                            IFNULL(CDM.CD_IS_MERGED, FALSE) AS Sec_isMerged,
                            IFNULL(CDM.CD_MERGEDCLAIM_DETAIL_ID, 0) sec_ParentLineItemID,
                            CASE
                                WHEN IFNULL(CDM.CD_IS_MERGED, FALSE) = FALSE THEN TRUE
                                ELSE CASE
                                    WHEN IFNULL(CDM.CD_IS_MERGED, FALSE) = TRUE
                                    AND IFNULL(CDM.CD_MERGEDCLAIM_DETAIL_ID, 0) = 0 THEN TRUE
                                    ELSE FALSE
                                END
                            END AS Sec_IsParent,
                            CDM.CD_SERVICE_CODE_ID AS Sec_Visitserviceid,
                            IFNULL(CM.CLAIM_IS_SECONDARY, FALSE) IsSecondary,
                            IFNULL(CM.CLAIM_IS_TRANSFERRED_COPAY, FALSE) IsTransferCopay
                        FROM
                            `prod-daas-viva`.`kt_analytics_hh_viva`.daas_dw_claimsmaster_vw CM
                            JOIN `prod-daas-viva`.`kt_analytics_hh_viva`.daas_dw_claimdetails_vw CDM ON CM.CLAIM_ID = CDM.CD_CLAIM_ID
                        WHERE
                            CM.CLAIM_STATUS != 'Deleted'
                            AND DATE(CM.CLAIM_START_DATE) >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 YEAR)
                            AND (
                                (CM.CLAIM_IS_SECONDARY = TRUE)
                                OR CM.CLAIM_IS_TRANSFERRED_COPAY = TRUE
                            )
                    ) AS Sec_Claim
                    JOIN (
                        SELECT
                            CM.INVOICE_NUMBER AS Prm_InvoiceNo,
                            CM.CLAIM_ID AS Prm_ClaimID,
                            CM.CLAIM_ICN AS Prm_Claim_ICN,
                            CM.PAYERSOURCE_ID Prm_PayerID,
                            IFNULL(CM.CLAIM_TOTALAMOUNT, 0) AS Prm_Claim_Amount,
                            CM.CLAIM_CONTRACTUAL_AMOUNT AS Prm_ExpectedAmount,
                            IFNULL(CM.CLAIM_PAIDAMOUNT, 0) AS Prm_Claim_PaidAmount,
                            CM.CLAIM_BALANCE AS Prm_Balance,
                            CDM.CD_SCHEDULE_DATE Prm_VisitDate,
                            IFNULL(CDM.CD_SCHEDULE_ID, 0) AS Prm_VisitID,
                            CDM.CD_CLAIM_DETAIL_ID Prm_LineItemID,
                            IFNULL(CDM.CD_CHILD_SCHEDULE_ID, 0) AS Prm_ChildVisitID,
                            CDM.CD_START_TIME Prm_VisitStartTime,
                            IFNULL(CDM.CD_UNITS, 0) Prm_Unit,
                            CDM.CD_END_TIME Prm_VisitEndTime,
                            CDM.CD_BILLED_AMOUNT Prm_LineItemAmount,
                            IFNULL(CDM.CD_PAIDAMOUNT, 0) Prm_LineItemPaidAmount,
                            IFNULL(CDM.CD_IS_MERGED, FALSE) AS Prm_isMerged,
                            IFNULL(CDM.CD_MERGEDCLAIM_DETAIL_ID, 0) prm_ParentLineItemID,
                            CASE
                                WHEN IFNULL(CDM.CD_IS_MERGED, FALSE) = FALSE THEN TRUE
                                ELSE CASE
                                    WHEN IFNULL(CDM.CD_IS_MERGED, FALSE) = TRUE
                                    AND IFNULL(CDM.CD_MERGEDCLAIM_DETAIL_ID, 0) = 0 THEN TRUE
                                    ELSE FALSE
                                END
                            END AS Prm_IsParent,
                            CDM.CD_SERVICE_CODE_ID AS Prm_Visitserviceid
                        FROM
                            `prod-daas-viva`.`kt_analytics_hh_viva`.daas_dw_claimsmaster_vw SECCLM
                            INNER JOIN `prod-daas-viva`.`kt_analytics_hh_viva`.daas_dw_claimsmaster_vw CM ON SECCLM.CLAIM_PARENT_CLAIMID = CM.CLAIM_ID
                            INNER JOIN `prod-daas-viva`.`kt_analytics_hh_viva`.daas_dw_claimdetails_vw CDM ON CM.CLAIM_ID = CDM.CD_CLAIM_ID
                        WHERE
                            CM.CLAIM_STATUS != 'Deleted'
                            AND SECCLM.CLAIM_STATUS != 'Deleted'
                            AND DATE(CM.CLAIM_START_DATE) >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 YEAR)
                            AND (
                                (SECCLM.CLAIM_IS_SECONDARY = TRUE)
                                OR SECCLM.CLAIM_IS_TRANSFERRED_COPAY = TRUE
                            )
                    ) AS prm_Claim ON Sec_Claim.Prm_ClaimID = prm_Claim.Prm_ClaimID
                    AND (
                        (
                            (Sec_Claim.Sec_VisitID = prm_Claim.Prm_VisitID)
                            AND (
                                Sec_Claim.Sec_ChildVisitID = prm_Claim.Prm_ChildVisitID
                            )
                            AND (
                                (
                                    Sec_isMerged = FALSE
                                    AND Prm_isMerged = FALSE
                                )
                                OR (
                                    Sec_isMerged = TRUE
                                    AND Sec_IsParent = FALSE
                                    AND Prm_isMerged = TRUE
                                    AND Prm_IsParent = FALSE
                                )
                            )
                        )
                        OR (
                            Sec_isMerged = TRUE
                            AND Sec_IsParent = TRUE
                            AND Prm_IsParent = TRUE
                            AND Prm_isMerged = TRUE
                            AND (prm_Claim.Prm_VisitDate = Sec_Claim.Sec_VisitDate)
                            AND (prm_Claim.Prm_Unit = Sec_Claim.Sec_Unit)
                        )
                    )
            ) A
            LEFT JOIN `prod-daas-viva`.`kt_analytics_hh_viva`.daas_dw_paymentsourcemaster_vw P ON A.Prm_PayerID = P.PS_PAYERSOURCEID
            LEFT JOIN `prod-daas-viva`.`kt_analytics_hh_viva`.daas_dw_paymentsourcemaster_vw P_S ON A.Sec_PayerID = P_S.PS_PAYERSOURCEID
    ) T
    JOIN `prod-daas-viva`.`kt_analytics_hh_viva`.daas_dw_clientmaster_vw C ON T.ClientID = C.client_id
    JOIN `prod-daas-viva`.`kt_analytics_hh_viva`.daas_dw_homehealthagenciesbranchlist_vw AS h ON C.AGENCY_BRANCH_ID = h.AGENCY_BRANCH_ID
    ''',

    "gpm": ''' 
   WITH cte1 AS (
    SELECT
        b.agency_branch_name AS location,
        sm.s_client_id AS client_id,
        sm.s_hha_branch_id AS hha_branch_id,
        cm.client_first_name AS client_first_name,
        cm.client_last_name AS client_last_name,
        sm.s_payer_source_id AS payer_source_id,
        cm.client_patient_id AS patient_id,
        sm.s_schedule_status AS schedule_status,
        schedule_date AS schedule_date,
        DATE_ADD(schedule_date, INTERVAL (6 - IF(EXTRACT(DAYOFWEEK FROM schedule_date) = 1, 0, EXTRACT(DAYOFWEEK FROM schedule_date) - 1)) DAY) AS `wk_end`,
        sc.service_type AS discipline,
        sc.service_name AS service_name,
        sm.s_regular_hours AS regular_hours,
        sm.s_pay_rate AS regular_pay_rate,
        sm.s_regularamount AS regular_payroll_amount,
        sm.s_ot_hours AS ot_hours,
        sm.s_ot_payroll_rate AS ot_payroll_rate,
        sm.s_ot_payrollamount AS ot_payroll_amount,
        COALESCE(
            NULLIF(
                CASE
                    WHEN IFNULL(sm.s_payble_hours, '') = '' THEN 0.00
                    ELSE ROUND(
                        (
                            CAST(SUBSTR(sm.s_payble_hours, 1, 2) AS FLOAT64) + (CAST(SUBSTR(sm.s_payble_hours, 4, 2) AS FLOAT64) / 60)
                        ),
                        2
                    )
                END,
                0.00
            ),
            sm.s_actual_hours,
            sm.s_planned_hours,
            0
        ) AS payable_hours,
        CASE
            WHEN IFNULL(sm.is_schedule_paid, FALSE) THEN sm.s_payrolled_amount
            ELSE sm.s_cost
        END AS payroll_amount,
        sm.s_contractual_amount AS expected_amount,
        sm.s_profit_contractual AS profit,
        sm.s_profitpercentage_expected AS profit_percent,
        sm.s_term_payer_amount AS term_payer_amount,
        sm.s_is_terminalbilled AS is_terminalbilled,
        sm.s_sec_payer_contrate AS sec_payer_contrate
    FROM `prod-daas-viva`.`kt_analytics_hh_viva`.daas_dw_clientmaster_vw cm
    INNER JOIN `prod-daas-viva`.`kt_analytics_hh_viva`.daas_dw_schedulemaster_vw sm ON cm.client_id = sm.s_client_id
    INNER JOIN `prod-daas-viva`.`kt_analytics_hh_viva`.daas_dw_servicecodesmaster_vw sc ON sm.s_service_code_id = sc.service_id
    LEFT JOIN `prod-daas-viva`.`kt_analytics_hh_viva`.daas_dw_homehealthagenciesbranchlist_vw b ON sm.s_hha_branch_id = b.agency_branch_id
    WHERE
        DATE(sm.schedule_date) BETWEEN PARSE_DATE('%m/%d/%Y', '1/1/2024') AND CURRENT_DATE()
        AND sm.is_miscvisit = FALSE
        AND sm.is_child_visit = FALSE
        AND sm.s_schedule_status IN (
            'In_Progress',
            'Planned',
            'Approved',
            'Completed'
        ) --order by SM.SCHEDULE_DATE
),
cte2 AS (
    SELECT
        p.ps_name AS payer_name,
        cte1.location,
        cte1.client_id,
        cte1.hha_branch_id,
        cte1.client_first_name,
        cte1.client_last_name,
        cte1.payer_source_id,
        cte1.patient_id,
        cte1.schedule_status,
        cte1.schedule_date,
        DATE_ADD(cte1.schedule_date, INTERVAL (6 - IF(EXTRACT(DAYOFWEEK FROM cte1.schedule_date) = 1, 0, EXTRACT(DAYOFWEEK FROM cte1.schedule_date) - 1)) DAY) AS `wk_end`,
        cte1.discipline,
        cte1.service_name,
        CASE
            WHEN IFNULL(cte1.regular_hours, 0.00) = 0.00 THEN IFNULL(cte1.payable_hours, 0.00) - IFNULL(cte1.ot_hours, 0.00)
            ELSE IFNULL(cte1.regular_hours, 0.00)
        END AS regular_hours,
        cte1.regular_pay_rate,
        CASE
            WHEN IFNULL(cte1.regular_payroll_amount, 0.00) = 0.00 THEN IFNULL(cte1.payroll_amount, 0.00) - IFNULL(cte1.ot_payroll_amount, 0.00)
            ELSE IFNULL(cte1.regular_payroll_amount, 0.00)
        END AS regular_payroll_amount,
        cte1.ot_hours,
        cte1.ot_payroll_rate,
        cte1.ot_payroll_amount,
        cte1.payable_hours,
        cte1.payroll_amount,
        cte1.expected_amount,
        CASE
            WHEN IFNULL(cte1.profit, 0.00) = 0.00 THEN IFNULL(cte1.expected_amount, 0.00) - IFNULL(cte1.payroll_amount, 0.00)
            ELSE IFNULL(cte1.profit, 0.00)
        END AS profit,
        CASE
            WHEN IFNULL(cte1.profit_percent, 0.00) = 0.00 THEN CASE
                WHEN IFNULL(cte1.expected_amount, 0.00) = 0.00 THEN 0.00
                ELSE ROUND(
                    (
                        (
                            (
                                IFNULL(cte1.expected_amount, 0.00) - IFNULL(cte1.payroll_amount, 0.00)
                            ) / IFNULL(cte1.expected_amount, 0.00)
                        ) * 100
                    ),
                    2
                )
            END
            ELSE IFNULL(cte1.profit_percent, 0.00)
        END AS profit_percent,
        cte1.term_payer_amount,
        cte1.is_terminalbilled,
        cte1.sec_payer_contrate
    FROM
        cte1
        INNER JOIN `prod-daas-viva`.`kt_analytics_hh_viva`.daas_dw_paymentsourcemaster_vw p ON cte1.payer_source_id = p.ps_payersourceid
)
select
    *
from
    cte2
order by
    schedule_date ''',

    'gpm_old' : '''
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
            `prod-daas-viva`.`kt_analytics_hh_viva`.daas_dw_CLIENTMASTER_vw CM
            inner join `prod-daas-viva`.`kt_analytics_hh_viva`.daas_dw_SCHEDULEMASTER_vw SM on CM.CLIENT_ID = SM.S_CLIENT_ID
            inner join `prod-daas-viva`.`kt_analytics_hh_viva`.daas_dw_SERVICECODESMASTER_vw SC on SM.S_SERVICE_CODE_ID = SC.SERVICE_ID
            left join `prod-daas-viva`.`kt_analytics_hh_viva`.daas_dw_HOMEHEALTHAGENCIESBRANCHLIST_vw B on SM.S_HHA_BRANCH_ID = B.AGENCY_BRANCH_ID
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
            inner join `prod-daas-viva`.`kt_analytics_hh_viva`.daas_dw_PAYMENTSOURCEMASTER_vw P on cte1.PAYER_SOURCE_ID = P.PS_PAYERSOURCEID
    )
select * from cte2 order by SCHEDULE_DATE''',

    "employees":'''
SELECT
    h.agency_branch_name as payroll_branch,
    c.cg_employeeid as empl_id,
    CONCAT(c.cg_firstname, c.cg_lastname) as "CLINICIAN NAME",
    c.cg_status as estatus,
    c.cg_hireddate AS hire_date,
    SUM(IFF(YEAR(s.schedule_date) >= YEAR(c.cg_hireddate) AND s.s_actual_end IS NOT NULL, s.s_actual_hours, NULL)) as hours,
    CASE WHEN c.cg_rehired_date = '1900-01-01 00:00:00.000000000' THEN NULL ELSE c.cg_rehired_date END as rehired_date,
    MIN(s.schedule_date) AS first_work_date,
    c.cg_termination_date as termination_date,
    c.cg_mailid AS email,
    c.cg_phone AS phone,
    c.cg_mobile AS mobile,
    c.cg_disciplinename AS discipline,
    c.cg_employment_type AS employment_type,
    c.cg_locations as locations,
    c.cg_language as language
FROM `prod-daas-viva`.`kt_analytics_hh_viva`.daas_dw_caregivermaster_vw AS c
JOIN `prod-daas-viva`.`kt_analytics_hh_viva`.daas_dw_homehealthagenciesbranchlist_vw as h ON h.agency_branch_id = c.cg_payroll_branch_id
LEFT JOIN `prod-daas-viva`.`kt_analytics_hh_viva`.daas_dw_schedulemaster_vw AS s ON c.cg_employeeid = s.s_caregiver_id AND s.s_units > 0
GROUP BY h.agency_branch_name,c.cg_employeeid,CONCAT(c.cg_firstname, c.cg_lastname),c.cg_status,c.cg_hireddate,c.cg_rehired_date,c.cg_termination_date,c.cg_mailid,c.cg_phone,c.cg_mobile,
c.cg_disciplinename,c.cg_employment_type,c.cg_locations,c.cg_language
ORDER BY c.cg_hireddate DESC NULLS LAST
''',
    "missed_visits":'''SELECT
    EXTRACT(YEAR FROM s.schedule_date) as year,
    EXTRACT(MONTH FROM s.schedule_date) AS month,
    FORMAT_DATE('%b', s.schedule_date) AS month_abbr,
    DATE_ADD(s.schedule_date, INTERVAL (6 - IF(EXTRACT(DAYOFWEEK FROM s.schedule_date) = 1, 7, EXTRACT(DAYOFWEEK FROM s.schedule_date) - 1)) DAY) AS week_end,
    s.schedule_date,
    h.agency_branch_name,
    u.client_id,
    CONCAT(u.client_first_name, u.client_last_name) as `CLIENT NAME`,
    c.cg_employeeid AS cg_id,
    CONCAT(c.cg_firstname, c.cg_lastname) AS `CLINICIAN NAME`,
    c.cg_disciplinename as discipline,
    CASE
        WHEN TRIM(sc.sevice_code) IN ('PDN Mid Tech - LVN (BIPAP)', 'PDN RN HITech', 'PDN RN HiTech', 'PDN RN HiTECH', 'PDN Hi Tech - RN',
         'PDN Mid Tech - RN (BIPAP)','PDN SHIFT RN', 'PDN Shift RN', 'PDN Shift - RN','PDN Shift LVN', 'PDN Shift LVN ', 'PDN Shift - LVN',
         'PDN LVN HiTech', 'PDN LVN HiTECH', 'PDN Hi-Tech - LVN') THEN 'NURSE'
        WHEN TRIM(sc.sevice_code) in ('STFEED','STEval','PTTELE','OT Eval low','PT Eval moderate','PTReEval','PT Eval noderate','PTA','PTDVN',
         'OT Eval moderate','OTReEval','OTDVN','ST Eval moderate','STReEval','STFEEDDVN','STDVN','ST Eval','ST EVAL SOC',
         'ST Feeding Eval','ST Feeding Visit','ST Re-Eval','ST Visit','PT Assistant','PT Eval','PT Eval high-complexity',
         'PT Eval moderate-complexity','PT Eval low-complexity','PT Re-Eval','PT Supervision','PT Tele Visit','PT Visit','OT Eval',
         'OT Eval low-complexity','OT Eval moderate-complexity','OT Eval high-complexity','OT Re-Eval','OT Visit') THEN 'THERAPY'
        ELSE 'NON BILLABLE'
    END AS service_filter,
    CASE
        WHEN TRIM(sc.sevice_code) IN ('PDN RN HITech', 'PDN RN HiTech', 'PDN RN HiTECH', 'PDN Hi Tech - RN') THEN 'PDN RN HI TECH'
        WHEN TRIM(sc.sevice_code) = 'PDN Mid Tech - RN (BIPAP)' THEN 'PDN MID TECH - RN (BIPAP)'
        WHEN TRIM(sc.sevice_code) IN ('PDN SHIFT RN', 'PDN Shift RN', 'PDN Shift - RN') THEN 'PDN SHIFT RN'
        WHEN TRIM(sc.sevice_code) IN ('PDN Shift LVN', 'PDN Shift LVN ', 'PDN Shift - LVN') THEN 'PDN SHIFT LVN'
        WHEN TRIM(sc.sevice_code) IN ('PDN LVN HiTech', 'PDN LVN HiTECH', 'PDN Hi-Tech - LVN') THEN 'PDN LVN HI TECH'
        WHEN TRIM(sc.sevice_code) = 'PDN Mid Tech - LVN (BIPAP)' THEN 'PDN MID TECH - LVN (BIPAP)'
        WHEN TRIM(sc.sevice_code) in ('OT Eval low','OT Eval moderate','OTReEval','OTDVN','OT Eval','OT Eval low-complexity',
         'OT Eval moderate-complexity','OT Eval high-complexity','OT Re-Eval','OT Visit') THEN 'OT'
        WHEN TRIM(sc.sevice_code) in ('PTTELE','PT Eval moderate','PTReEval','PT Eval noderate','PTA','PTDVN','PT Assistant','PT Eval',
         'PT Eval high-complexity', 'PT Eval moderate-complexity','PT Eval low-complexity','PT Re-Eval','PT Supervision',
         'PT Tele Visit','PT Visit') THEN 'PT'
        WHEN TRIM(sc.sevice_code) in ('STFEED','STEval','ST Eval moderate','STReEval','STFEEDDVN','STDVN','ST Eval','ST EVAL SOC',
         'ST Feeding Eval','ST Feeding Visit','ST Re-Eval','ST Visit') THEN 'ST'
        ELSE 'NON BILLABLE'
    END AS service_code,
    sc.sevice_code as raw_service_code,
    s.s_planned_hours,
    s.s_schedule_status,
    s.s_missedvisit_reasontype,
    s.s_missedvisit_note

FROM `prod-daas-viva.kt_analytics_hh_viva.daas_dw_schedulemaster_vw` AS s
JOIN `prod-daas-viva.kt_analytics_hh_viva.daas_dw_servicecodesmaster_vw` as sc
    on sc.service_id = s.s_service_code_id
JOIN `prod-daas-viva.kt_analytics_hh_viva.daas_dw_caregivermaster_vw` as c
    ON s.s_caregiver_id = c.cg_employeeid
JOIN `prod-daas-viva.kt_analytics_hh_viva.daas_dw_clientmaster_vw` as u
    ON u.client_id = s.s_client_id
JOIN `prod-daas-viva.kt_analytics_hh_viva.daas_dw_homehealthagenciesbranchlist_vw` as h
    ON u.agency_branch_id = h.agency_branch_id
WHERE
    TRIM(sc.sevice_code) IN ('STFEED','STEval','PTTELE','OT Eval low','PT Eval moderate','PTReEval','PT Eval noderate','PTA','PTDVN',
         'OT Eval moderate','OTReEval','OTDVN','ST Eval moderate','STReEval','STFEEDDVN')

    -- And optionally, if there is a specific status for missed appointments, include that:
    AND s.S_SCHEDULE_STATUS = 'MissedVisit'
    AND DATE(s.S_WEEKSTART) >= DATE_ADD(CURRENT_DATE(), INTERVAL -24 MONTH)
    AND DATE(s.SCHEDULE_DATE) >= DATE_ADD(CURRENT_DATE(), INTERVAL -25 MONTH)
GROUP BY
    EXTRACT(YEAR FROM s.schedule_date),
    EXTRACT(MONTH FROM s.schedule_date),
    FORMAT_DATE('%b', s.schedule_date) ,
    DATE_ADD(s.schedule_date, INTERVAL (6 - IF(EXTRACT(DAYOFWEEK FROM s.schedule_date) = 1, 7, EXTRACT(DAYOFWEEK FROM s.schedule_date) - 1)) DAY),
    s.schedule_date,
    h.agency_branch_name,
    u.client_id,
    CONCAT(u.client_first_name, u.client_last_name),
    c.cg_employeeid,
    CONCAT(c.cg_firstname, c.cg_lastname),
    c.cg_disciplinename,
    sc.SERVICE_TYPE,
    sc.SEVICE_CODE,
    s.s_planned_hours,
    s.s_schedule_status,
    s.s_missedvisit_reasontype,
    s.s_missedvisit_note
ORDER BY WEEK_END DESC''',
    "rollup":''' SELECT meta FROM objects.cache WHERE id = %s '''
}


