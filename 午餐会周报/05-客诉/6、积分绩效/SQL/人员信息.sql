WITH employee_data AS (
    SELECT DISTINCT 
        id,  
        employee_code,
        employee_name,
        cost_center,
        emp_sub_name, 
        mobile,
        email,
        pos_title_name,
        pos_title_id,
        orgunit_code,
        pos_position_name
    FROM csx_ods.csx_ods_csx_basic_data_erp_employee_df
    WHERE sdt = '20251015' 
        AND employee_status = '3'	
        AND card_type = '0'
        AND (company_name LIKE '%彩食鲜%' OR emp_sub_name LIKE '%企业购%')
),
org_hierarchy AS (
    SELECT 
        a.org_unit_code as org_code,
        a.org_unit_name,
        e1.parent_code as parent_org_code,
        e1.org_unit_name as parent_org_name,
        e2.org_unit_name as parent_org_name_3,
        e3.org_unit_name as parent_org_name_4,
        e4.org_unit_name as parent_org_name_5,
        CASE 
            WHEN e4.org_unit_name = '永辉彩食鲜发展有限公司' THEN CONCAT(e4.org_unit_name, '_', e3.org_unit_name, '_', e2.org_unit_name, '_', e1.org_unit_name, '_', a.org_unit_name)
            WHEN e3.org_unit_name = '永辉彩食鲜发展有限公司' THEN CONCAT(e3.org_unit_name, '_', e2.org_unit_name, '_', e1.org_unit_name, '_', a.org_unit_name)
            WHEN e2.org_unit_name = '永辉彩食鲜发展有限公司' THEN CONCAT(e2.org_unit_name, '_', e1.org_unit_name, '_', a.org_unit_name)
            WHEN e1.org_unit_name = '永辉彩食鲜发展有限公司' THEN CONCAT(e1.org_unit_name, '_', a.org_unit_name)
            ELSE CONCAT('永辉彩食鲜发展有限公司', '_', a.org_unit_name)
        END as org_name
    FROM csx_ods.csx_ods_csx_basic_data_erp_organization_df a
    LEFT JOIN csx_ods.csx_ods_csx_basic_data_erp_organization_df e1 
        ON a.parent_code = e1.org_unit_code AND e1.sdt = '20251015' AND e1.status = 1
    LEFT JOIN csx_ods.csx_ods_csx_basic_data_erp_organization_df e2 
        ON e1.parent_code = e2.org_unit_code AND e2.sdt = '20251015' AND e2.status = 1
    LEFT JOIN csx_ods.csx_ods_csx_basic_data_erp_organization_df e3 
        ON e2.parent_code = e3.org_unit_code AND e3.sdt = '20251015' AND e3.status = 1
    LEFT JOIN csx_ods.csx_ods_csx_basic_data_erp_organization_df e4 
        ON e3.parent_code = e4.org_unit_code AND e4.sdt = '20251015' AND e4.status = 1
    WHERE a.sdt = '20251015' AND a.status = 1
)
SELECT 
    erp.id,  -- 明确指定使用erp表的id
    employee_name,
    employee_code as job_no,
    CASE 
        WHEN (emp_sub_name LIKE '永辉彩食鲜%' 
              OR org_name NOT LIKE '永辉彩食鲜发展有限公司_大区%' 
              OR emp_sub_name LIKE '彩食鲜共享%' 
              OR emp_sub_name LIKE '%永辉企业购招投标2300店%' 
              OR emp_sub_name LIKE '彩食鲜-富平%') 
            THEN SUBSTRING_INDEX(SUBSTRING_INDEX(org_name, '_', 2), '_', -1)
        WHEN emp_sub_name LIKE '%大区%' AND emp_sub_name REGEXP '[A-Z]' 
            THEN SUBSTR(emp_sub_name, 1, CHAR_LENGTH(emp_sub_name) - 2)
        WHEN emp_sub_name REGEXP '[\'\'(]'   
            THEN SUBSTRING(emp_sub_name, 1, CHAR_LENGTH(emp_sub_name) - 6)
        WHEN emp_sub_name REGEXP '[2-9]' 
            THEN REPLACE(SUBSTRING(emp_sub_name, 1, CHAR_LENGTH(emp_sub_name) - 4), '-', '')
        ELSE REPLACE(emp_sub_name, '-', '') 
    END as org,
    
    CASE 
        WHEN (emp_sub_name LIKE '永辉彩食鲜%' 
              OR org_name NOT LIKE '永辉彩食鲜发展有限公司_大区%'  
              OR emp_sub_name LIKE '彩食鲜共享%' 
              OR emp_sub_name LIKE '%永辉企业购招投标2300店%' 
              OR emp_sub_name LIKE '彩食鲜-富平%') 
            THEN SUBSTRING_INDEX(SUBSTRING_INDEX(org_name, '_', 2), '_', -1)
        WHEN SUBSTRING_INDEX(
                REPLACE(REPLACE(org_name, '永辉彩食鲜发展有限公司_大区_', ''), '省区_', ''), 
                '_', 3) REGEXP('部|人')
            THEN SUBSTRING_INDEX(
                REPLACE(REPLACE(org_name, '永辉彩食鲜发展有限公司_大区_', ''), '省区_', ''), 
                '_', 2) 
        ELSE SUBSTRING_INDEX(
                REPLACE(REPLACE(org_name, '永辉彩食鲜发展有限公司_大区_', ''), '省区_', ''), 
                '_', 3)
    END as org_short,
    
    CASE 
        WHEN parent_org_name REGEXP '^[0-Z]' THEN parent_org_name_3 
        ELSE parent_org_name 
    END as title_name,      
    
    pos_position_name as position_name,
    orgunit_code,
    parent_org_name,
    org_name,
    emp_sub_name
FROM employee_data erp 
LEFT JOIN org_hierarchy org ON org.org_code = erp.orgunit_code
WHERE org_code IS NOT NULL 
    AND orgunit_code != '15046965'
    AND pos_title_name != '残疾人岗';