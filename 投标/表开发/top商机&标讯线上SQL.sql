-- ******************************************************************** 
-- @功能描述：
-- @创建者： 彭承华 
-- @创建者日期：2025-03-07 20:30:08 
-- @修改者日期：
-- @修改人：
-- @修改内容：
-- ******************************************************************** 
 
  -- 商机基础信息
  drop table  csx_analyse_tmp.csx_analyse_tmp_crm_business_info;
  create table csx_analyse_tmp.csx_analyse_tmp_crm_business_info as 
  SELECT
    a.performance_region_code,
    a.performance_region_name,
    a.performance_province_code,
    a.performance_province_name,
    a.performance_city_code,
    a.performance_city_name,
    a.owner_city_code,
    a.owner_city_name,
    a.customer_code,
    a.customer_name,
    a.first_category_code,
    a.first_category_name,
    a.second_category_code,
    a.second_category_name,
    a.third_category_code,
    a.third_category_name,
    h.new_classify_name,
    a.business_number,
    a.owner_user_name,
    a.business_attribute_name,
    a.business_stage,
    a.estimate_contract_amount,
    a.contract_cycle_int,
    a.company_code,
    m.company_name,
    a.expect_execute_time,
    a.expect_sign_time,
    a.first_business_sign_time,
    a.business_sign_time,
    a.first_sign_time,
    a.create_time,
    a.status,
    business_attribute_code,
    CASE
      WHEN a.business_stage = 1 THEN '10%'
      WHEN a.business_stage = 2 THEN '25%'
      WHEN a.business_stage = 3 THEN '50%'
      WHEN a.business_stage = 4 THEN '75%'
      WHEN a.business_stage = 5 THEN '100%'
      ELSE ''
    END AS business_stage_name,
    IF(
      TO_DATE(a.first_business_sign_time) = TO_DATE(a.business_sign_time)
      OR a.first_business_sign_time IS NULL,
      '新客',
      '老客'
    ) AS is_new_flag,
    ROW_NUMBER() OVER ( PARTITION BY a.customer_code, a.business_attribute_code  ORDER BY  a.create_time desc  ) AS rn,
    LAG(a.business_number, 1) OVER (
      PARTITION BY a.customer_code, a.business_attribute_code
      ORDER BY a.create_time DESC
    ) AS prev_business_number,
    LAG(a.estimate_contract_amount, 1) OVER (
      PARTITION BY a.customer_code, a.business_attribute_code
      ORDER BY a.create_time DESC
    ) AS prev_business_amount
  FROM
    csx_dim.csx_dim_crm_business_info a
    LEFT JOIN (
      SELECT
        company_code,
        company_name
      FROM
        csx_dim.csx_dim_basic_company
      WHERE
        sdt =  regexp_replace(TO_DATE(date_add(current_timestamp(),-1)),'-','')
    ) m ON a.company_code = m.company_code
    left join csx_analyse.csx_analyse_fr_new_customer_classify_mf h on a.second_category_code = h.second_category_code
  WHERE
    a.sdt = 'current'
    -- AND a.status = 1

;
-- select * from   csx_analyse_tmp.csx_analyse_tmp_crm_top_bid_info_new where bid_no= 'BXSX20250225055925'
drop table csx_analyse_tmp.csx_analyse_tmp_crm_top_bid_info_new;
create table csx_analyse_tmp.csx_analyse_tmp_crm_top_bid_info_new as 
    SELECT 
        a.sdt,
        a.id,
        a.bid_no,
        a.performance_region_code,
        a.performance_region_name,
        a.performance_province_code,
        a.performance_province_name,
        a.performance_city_code,
        a.performance_city_name,    
        a.project_customer_province_name,
        a.project_customer_city_name,
        a.project_customer_city,
        regexp_replace(bid_name, '\\s+', '') AS bid_name,
        regexp_replace(bid_number, '\\s+', '') AS bid_number, 
        regexp_replace(bid_customer_name, '\\s+', '') AS bid_customer_name,
        a.business_number,
        a.first_category_code,
        a.first_category_name,
        a.second_category_code,
        a.second_category_name,
        a.new_classify_name,
        a.business_attribute_code,
        a.business_attribute_name,
        a.all_win_bid_amount,
        a.all_win_bid_max_amount,
        a.all_bid_in_amount,
        a.all_bid_amount,
        a.all_win_bid_count,
        a.sub_win_bid_amount,
        a.bid_sub_in_count,
        a.bid_sub_count,
        a.supply_deadline,
        a.sub_bid_name,
        a.sub_bid_amount,
        a.sub_bid_date,
        a.sub_bid_company,
        a.sub_bid_company_name,
        a.early_work,
        COALESCE(IF(a.city_service_flag=1, '前置仓', a.early_work_name), '') AS early_work_name,
        a.sales_user_name,
        a.history_attribute_name,
        a.city_service_flag,
        a.sub_bid_status,
        a.sub_bid_status_name,
        CONCAT(a.sub_bid_name, '：', CAST(a.sub_win_bid_count AS STRING), '家') AS sub_bid_name_note,
        IF(COALESCE(a.history_attribute_name, 0)=0, '新客', '老客') AS is_new_cust_flag,
        regexp_replace(a.sub_bid_result, '\\s+', '') AS sub_bid_result,
        SUM(a.sub_win_bid_amount) OVER (PARTITION BY a.bid_no) AS total_bid_sub_amount,
        sub_bid_rn ,
        b.business_stage_name,
        b.owner_user_name,
        b.estimate_contract_amount,
        b.expect_sign_time,
        b.expect_execute_time,
        b.contract_cycle_int,
        business_sign_time,
        ROW_NUMBER() OVER (PARTITION BY a.bid_no,a.bid_sub_id ORDER BY b.create_time desc  ) AS  business_rn,
        prev_business_number,
        prev_business_amount,
        a.bid_sub_id
    FROM 
         csx_analyse.csx_analyse_crm_bid_info_new a 
    left join 
        csx_analyse_tmp.csx_analyse_tmp_crm_business_info b on a.business_number=b.business_number and a.business_attribute_code=b.business_attribute_code
    left join 
    ( select 
        bid_no,
        id as bid_id,
        bid_sub_id,
        sub_bid_status,
        ROW_NUMBER() OVER (PARTITION BY a.bid_no ORDER BY a.sub_bid_date) AS sub_bid_rn 
    FROM 
          csx_analyse.csx_analyse_crm_bid_info_new  a 
        where 
            a.sdt = '${sdt_yes}'
            and a.sub_bid_status not in ('2','4','7')
        group by bid_no,
        id   ,
        bid_sub_id,
        sub_bid_status,
        sub_bid_date
        ) c on a.bid_no=c.bid_no and a.bid_sub_id=c.bid_sub_id and a.id=c.bid_id
     WHERE 
        a.sdt = '${sdt_yes}'
        AND a.sub_bid_status != '7'
        AND ((a.all_bid_amount >= 1000   AND a.business_attribute_code = '1')
            or (a.all_bid_amount >= 100   AND a.business_attribute_code in ( '2','5'))
            )
        -- and a.bid_no='BXHB20250225058061'
    ;
    
    
 -- 标讯结果表
drop table csx_analyse_tmp.csx_analyse_tmp_crm_top_bid_result;
create table csx_analyse_tmp.csx_analyse_tmp_crm_top_bid_result as
WITH  
tmp_main_bid_info AS (
    SELECT 
        sdt,
        a.id,
        a.bid_no,
        -- 主包信息字段聚合
        MAX(a.performance_region_code) AS performance_region_code,
        MAX(a.performance_region_name) AS performance_region_name,
        MAX(a.performance_province_code) AS performance_province_code,
        MAX(a.performance_province_name) AS performance_province_name,
        MAX(a.performance_city_code) AS performance_city_code,
        MAX(a.performance_city_name) AS performance_city_name, 
        MAX(a.project_customer_province_name) AS project_customer_province_name,
        MAX(a.project_customer_city) AS project_customer_city,
        MAX(a.project_customer_city_name) AS project_customer_city_name,   
        MAX(regexp_replace(a.bid_name, '\\s+', '')) AS bid_name,
        MAX(regexp_replace(a.bid_number, '\\s+', '')) AS bid_number, 
        MAX(regexp_replace(a.bid_customer_name, '\\s+', '')) AS bid_customer_name,
        MAX(a.first_category_code) AS first_category_code,
        MAX(a.first_category_name) AS first_category_name,
        MAX(a.second_category_code) AS second_category_code,
        MAX(a.second_category_name) AS second_category_name,
        MAX(a.new_classify_name) AS new_classify_name,
        MAX(a.business_attribute_code) AS business_attribute_code,
        MAX(a.business_attribute_name) AS business_attribute_name,
        MAX(a.all_win_bid_amount) AS all_win_bid_amount,
        MAX(a.all_win_bid_max_amount) AS all_win_bid_max_amount,
        MAX(a.all_bid_in_amount) AS all_bid_in_amount,
        MAX(a.all_bid_amount) AS all_bid_amount,
        MAX(a.all_win_bid_count) AS all_win_bid_count,
        MAX(a.bid_sub_count) AS bid_sub_count,
        MAX(bid_sub_in_count) as bid_sub_in_count,
        max(supply_deadline) supply_deadline,
        CONCAT_WS(',', COLLECT_SET(a.sub_bid_name_note)) AS sub_bid_name_note,
        -- 状态列表
        CONCAT_WS(',', COLLECT_SET(a.sub_bid_status_name)) AS status_list,
        MAX(a.total_bid_sub_amount) AS total_bid_sub_amount,
        CONCAT_WS(',', COLLECT_SET(a.business_number)) AS list_business_number,
        MIN(a.sub_bid_date) AS min_sub_bid_date,
        MAX(is_new_cust_flag) is_new_cust_flag,
        MAX(early_work_name) early_work_name,
        -- 替换 BOOL_AND/BOOL_OR 逻辑
        -- 判断是否全部分包为停止跟进 (sub_bid_status IN ('2','4'))
        MIN(CASE WHEN a.sub_bid_status IN ('2','4') THEN 1 ELSE 0 END) = 1 AS is_all_stop,
        -- 判断是否存在有效分包 (sub_bid_status NOT IN ('2','4','7'))
        MAX(CASE WHEN a.sub_bid_status NOT IN ('2','4','7') THEN 1 ELSE 0 END) = 1 AS has_active_sub,
        -- 判断是否全部分包为暂停 (sub_bid_status = '7')
        MIN(CASE WHEN a.sub_bid_status = '7' THEN 1 ELSE 0 END) = 1 AS is_all_pause,
        -- 状态优先级
        MAX(
            CASE a.sub_bid_status 
                WHEN '9' THEN '中标' 
                WHEN '8' THEN '未公示'
                WHEN '12' THEN '废标'
                WHEN '10' THEN '未中标'
                WHEN '11' THEN '流标'
                WHEN '13' THEN '放弃中标'
                ELSE '未公示'
            END
        ) AS status_priority,
        SUM(estimate_contract_amount) as estimate_contract_amount,
        max(case when nvl(a.business_number,'')='' then 1 else 0 end )=1 as is_incr
    FROM 
        csx_analyse_tmp.csx_analyse_tmp_crm_top_bid_info_new a 
    GROUP BY 
        a.id, a.bid_no,
        sdt
)
SELECT 
    a.id,
    if(a.business_attribute_code in('2','5') ,'2','1') as level_id,
    a.bid_no,
    a.performance_region_code,
    a.performance_region_name,
    a.performance_province_code,
    a.performance_province_name,
    a.performance_city_code,
    a.performance_city_name, 
    a.project_customer_city,
    a.project_customer_city_name, 
    CASE 
        -- 逻辑4：全部分包为项目暂停
        WHEN a.is_all_pause THEN '项目暂停（取消）'
        -- 逻辑1：全部分包为停止跟进
        WHEN a.is_all_stop THEN '停止跟进'
        -- 逻辑2：存在有效分包且投标未到期
        WHEN a.has_active_sub AND a.min_sub_bid_date > CURRENT_DATE() THEN '待投标'
        -- 逻辑3：按状态优先级判定
        ELSE a.status_priority
    END AS bid_status_name, 
    business_stage_name, 
    a.bid_name,
    a.list_business_number,
    -- a.bid_number,
    a.bid_customer_name,
    -- a.business_attribute_code,
    a.business_attribute_name,
    a.all_bid_amount,
    a.all_bid_in_amount,
    a.bid_sub_count,
    sub_bid_name_note as sub_bid_name_count,
    a.all_win_bid_max_amount,
    a.all_win_bid_amount,
    a.all_win_bid_count,
    bid_sub_in_count,
    supply_deadline,
    a.estimate_contract_amount,
    expect_sign_time,
    contract_cycle_int,
    a.min_sub_bid_date as  bid_date,
    expect_execute_time,
    c.sub_bid_company as company_code,
    c.sub_bid_company_name as company_name,
    is_new_cust_flag,
    case when is_incr then '增量'
        when coalesce(a.estimate_contract_amount,0)> coalesce(prev_business_amount,0) then '增量'
        when size(split(',',list_business_number))>1 then '增量'
        else '存量'
    end  AS is_incr,
    early_work_name,    
    owner_user_name as sales_user_name,
    '' as follow_up_progress ,
    current_timestamp() sys_update_time,
    c.sub_bid_company,
    c.sub_bid_company_name,
    sdt as s_sdt,
    a.total_bid_sub_amount,     -- 分包中标金额
    a.first_category_code,
    a.first_category_name,
    a.second_category_code,
    a.second_category_name,
    a.new_classify_name,
    sdt
FROM 
    tmp_main_bid_info a
LEFT JOIN (
    SELECT distinct bid_no, 
        sub_bid_company_name ,
        sub_bid_company,
        business_stage_name,
        owner_user_name,
        expect_sign_time,
        expect_execute_time,
        contract_cycle_int,
        prev_business_number,
        prev_business_amount
    FROM csx_analyse_tmp.csx_analyse_tmp_crm_top_bid_info_new 
    WHERE sub_bid_rn = 1
     and business_rn=1
    --  and bid_no='BXSX20250225055925'
) c ON a.bid_no = c.bid_no
WHERE 
1=1
-- and a.list_business_number='SJ23071700053'
;

-- 商机TOP
drop table csx_analyse_tmp.csx_analyse_tmp_top_business_info;
CREATE TABLE csx_analyse_tmp.csx_analyse_tmp_top_business_info AS
WITH filtered_business AS
(
  -- 第一步：提前过滤基础数据，减少后续处理的数据量
  SELECT
    a.performance_region_code,
    a.performance_region_name,
    a.performance_province_code,
    a.performance_province_name,
    a.performance_city_code,
    a.performance_city_name,
    a.owner_city_code,
    a.owner_city_name,
    a.customer_code,
    a.customer_name,
    a.first_category_code,
    a.first_category_name,
    a.second_category_code,
    a.second_category_name,
    a.third_category_code,
    a.third_category_name,
    a.new_classify_name,
    a.business_number,
    a.owner_user_name,
    a.business_attribute_name,
    a.business_stage,
    a.estimate_contract_amount,
    a.contract_cycle_int,
    a.company_code,
    a.company_name,
    a.expect_execute_time,
    a.expect_sign_time,
    a.first_business_sign_time,
    a.business_sign_time,
    a.first_sign_time,
    a.create_time,
    a.status,
    business_stage_name,
    is_new_flag,
    business_attribute_code,
    rn
  FROM
    csx_analyse_tmp.csx_analyse_tmp_crm_business_info  a 
  WHERE
    a.business_stage != 5
    and status=1
    AND (
      (
        a.estimate_contract_amount >= 1000
        AND a.business_attribute_code = 1
      )
      OR (
        a.estimate_contract_amount >= 100
        AND a.business_attribute_code IN ('2', '5')
      )
    )
),
customer_last_estimate AS (
  -- 第二步：预计算客户历史签约金额
  SELECT
    customer_code,
    prev_business_number,
    business_attribute_code,
    estimate_contract_amount,
    rn
  FROM
    csx_analyse_tmp.csx_analyse_tmp_crm_business_info 
),
active_business AS (
  -- 第三步：标记已关联标讯的业务
  SELECT
    DISTINCT business_number,
    business_attribute_code
  FROM
    csx_analyse_tmp.csx_analyse_tmp_crm_top_bid_info_new
  WHERE
    business_number IS NOT NULL
)
SELECT
  f.performance_region_code,
  f.performance_region_name,
  f.performance_province_code,
  f.performance_province_name,
  f.performance_city_code,
  f.performance_city_name,
  f.owner_city_code,
  f.owner_city_name,
  f.customer_code,
  f.customer_name,
  f.first_category_code,
  f.first_category_name,
  f.second_category_code,
  f.second_category_name,
  f.third_category_code,
  f.third_category_name,
  f.new_classify_name,
  f.business_number,
  f.owner_user_name,
  f.business_attribute_name,
  f.business_stage,
  f.business_stage_name,
  f.estimate_contract_amount,
  f.contract_cycle_int,
  f.company_code,
  f.company_name,
  f.expect_execute_time,
  f.expect_sign_time,
  f.first_business_sign_time,
  f.business_sign_time,
  f.first_sign_time,
  f.create_time,
  f.status,
  f.is_new_flag,
  f.rn,
  l.estimate_contract_amount AS last_estimate_contract_amt,
  CASE
    WHEN f.is_new_flag = '新客' THEN '增量'
    WHEN f.estimate_contract_amount >= COALESCE(l.estimate_contract_amount, 0) THEN '增量'
    ELSE '存量'
  END AS is_incr,
  f.business_attribute_code
FROM
  filtered_business f
  LEFT JOIN customer_last_estimate l ON f.customer_code = l.customer_code and f.business_number=l.prev_business_number
WHERE
  NOT EXISTS (
    SELECT
      1
    FROM
      active_business a
    WHERE
      a.business_number = f.business_number and a.business_attribute_code=f.business_attribute_code
  );

-- 商机结果表
    
drop table csx_analyse_tmp.csx_analyse_tmp_crm_top_business_result;
create table csx_analyse_tmp.csx_analyse_tmp_crm_top_business_result as
SELECT
  coalesce(b.id,'') as id,
  if(f.business_attribute_code in('2','5') ,'2','1') as level_id,
  coalesce(b.bid_no,'') as bid_no,
  f.performance_region_code,
  f.performance_region_name,
  f.performance_province_code,
  f.performance_province_name,
  f.performance_city_code,
  f.performance_city_name,
  owner_city_code as project_customer_city,
  owner_city_name as project_customer_city_name,
  CASE 
        -- 逻辑4：全部分包为项目暂停
        WHEN b.sub_bid_status='7' THEN '项目暂停（取消）'
        -- 逻辑1：全部分包为停止跟进
        WHEN b.sub_bid_status in ('2','4') THEN '停止跟进'
        -- 逻辑2：存在有效分包且投标未到期
        WHEN b.sub_bid_status not in ('2','4','7') AND b.sub_bid_date > CURRENT_DATE() THEN '待投标'
        -- 逻辑3：按状态优先级判定
        when b.sub_bid_date <= CURRENT_DATE() 
        THEN CASE  sub_bid_status 
        WHEN '9' THEN '中标'  -- 中标
        WHEN '8' THEN '未公示'  -- 未公示 
        WHEN '12' THEN '废标' -- 废标
        WHEN '10' THEN '未中标' -- 未中标
        WHEN '11' THEN '流标' -- 流标
        WHEN '13' THEN '放弃中标' -- 放弃中标
        ELSE '未公示' end 
    END AS bid_status_name,
  business_stage_name, 
  coalesce(b.bid_name,'') bid_name,
  f.business_number,
  f.customer_name as bid_customer_name,
  f.business_attribute_name,
  b.all_bid_amount,
  b.all_bid_in_amount,
  b.bid_sub_count,
  b.sub_bid_name_note as sub_bid_name_count,
  b.all_win_bid_max_amount,
  b.all_win_bid_amount,
  b.all_win_bid_count,
  b.bid_sub_in_count,
  b.supply_deadline,
  f.estimate_contract_amount,
  f.expect_sign_time,
  f.contract_cycle_int,
  b.sub_bid_date as  bid_date,
  f.expect_execute_time,
  f.company_code,
  f.company_name,
  f.is_new_flag as is_new_cust_flag,
  f.is_incr,
  b.early_work_name,
  owner_user_name as sales_user_name,
  '' as follow_up_progress ,
  current_timestamp() sys_update_time,  
  if(length(coalesce(company_code,''))!=0,company_code,sub_bid_company) as  sub_bid_company,
  coalesce(company_name,sub_bid_company_name) as  sub_bid_company_name, 
  '${sdt_yes}' as s_sdt, 
  sub_win_bid_amount as total_bid_sub_amount,     -- 分包中标金额 
  f.first_category_code,
  f.first_category_name,
  f.second_category_code,
  f.second_category_name,
  f.third_category_code,
  f.third_category_name,
  f.new_classify_name,
  business_sign_time,
  '${sdt_yes}' sdt
from
  csx_analyse_tmp.csx_analyse_tmp_top_business_info f
  left join (
    select id,
      bid_no,
      bid_name,
      a.business_attribute_name,
      a.all_win_bid_amount,
      a.all_win_bid_max_amount,
      a.all_bid_in_amount,
      a.all_bid_amount,
      a.all_win_bid_count,
      a.sub_win_bid_amount,
      a.bid_sub_in_count,
      a.bid_sub_count,
      a.supply_deadline,
      a.sub_bid_name,
      a.sub_bid_amount,
      a.sub_bid_date,
      a.sub_bid_company,
      a.sub_bid_company_name,
      a.early_work,
      COALESCE(
        IF(a.city_service_flag = 1, '前置仓', a.early_work_name),
        ''
      ) AS early_work_name,
      a.sales_user_name,
      a.history_attribute_name,
      a.city_service_flag,
      a.sub_bid_status,
      a.sub_bid_status_name,
      CONCAT(
        a.sub_bid_name,
        '：',
        CAST(a.sub_win_bid_count AS STRING),
        '家'
      ) AS sub_bid_name_note,
      business_number
    from
      csx_analyse.csx_analyse_crm_bid_info_new a
    where
      sdt = '${sdt_yes}'
  ) b on f.business_number = b.business_number
;


INSERT overwrite table csx_analyse.csx_analyse_fr_bid_business_million_detail_di partition(sdt)
SELECT 
    cast(a.id as string) id ,
    level_id,
    a.bid_no,
    a.performance_region_code,
    a.performance_region_name,
    a.performance_province_code,
    a.performance_province_name,
    a.performance_city_code,
    a.performance_city_name, 
    a.project_customer_city,
    a.project_customer_city_name, 
    bid_status_name, 
    business_stage_name, 
    a.bid_name,
    a.list_business_number,
    '' as  `bid_name_alias` ,    -- '投标项目别名',
    '' as  `bid_number` ,  --   '投标编码',
    '' as  `bid_name_number` , --  '投标名称+编码',
    a.bid_customer_name,
    -- a.business_attribute_code,
    a.business_attribute_name,
    a.all_bid_amount,
    a.all_bid_in_amount,
    a.bid_sub_count,
    sub_bid_name_count,
    a.all_win_bid_max_amount,
    a.all_win_bid_amount,
    a.all_win_bid_count,
    bid_sub_in_count,
    supply_deadline,
    cast(a.estimate_contract_amount as decimal(26,0)) estimate_contract_amount,
    expect_sign_time,
    contract_cycle_int,
    bid_date,
    expect_execute_time,
    a.sub_bid_company as company_code,
    a.sub_bid_company_name as company_name,
    is_new_cust_flag,
    is_incr,
    early_work_name,    
    sales_user_name,
    '' as follow_up_progress ,
    current_timestamp() sys_update_time,
    sub_bid_company,
    sub_bid_company_name,
    sdt as s_sdt,
    a.total_bid_sub_amount,     -- 分包中标金额
    a.first_category_code,
    a.first_category_name,
    a.second_category_code,
    a.second_category_name,
    a.new_classify_name,
    sdt
FROM csx_analyse_tmp.csx_analyse_tmp_crm_top_bid_result a 
union all 

SELECT 
    a.id,
    level_id,
    a.bid_no,
    a.performance_region_code,
    a.performance_region_name,
    a.performance_province_code,
    a.performance_province_name,
    a.performance_city_code,
    a.performance_city_name, 
    a.project_customer_city,
    a.project_customer_city_name, 
    case when coalesce(bid_no,'')='' then '待投标'
        when datediff(to_date(business_sign_time), to_date(bid_date))>180 then '不投标'
    else bid_status_name
    end 
        bid_status_name, 
    business_stage_name, 
    a.bid_name,
    a.business_number as list_business_number,
    '' as `bid_name_alias` ,    -- '投标项目别名',
    '' as `bid_number` ,  --   '投标编码',
    '' as `bid_name_number` , --  '投标名称+编码',
    -- a.bid_number,
    a.bid_customer_name,
    -- a.business_attribute_code,
    a.business_attribute_name,
    a.all_bid_amount,
    a.all_bid_in_amount,
    a.bid_sub_count,
    sub_bid_name_count,
    a.all_win_bid_max_amount,
    a.all_win_bid_amount,
    a.all_win_bid_count,
    bid_sub_in_count,
    supply_deadline,
     cast(a.estimate_contract_amount as decimal(26,0)) estimate_contract_amount,
    expect_sign_time,
    contract_cycle_int,
    bid_date,
    expect_execute_time,
    company_code,
    company_name,
    is_new_cust_flag,
    is_incr,
    case when coalesce(bid_no,'')='' then '销售先行' 
        when datediff(to_date(business_sign_time), to_date(bid_date))>180 then '销售先行'
        else early_work_name
    end early_work_name,    
    sales_user_name,
    '' as follow_up_progress ,
    current_timestamp() sys_update_time,
    sub_bid_company,
    sub_bid_company_name,
    sdt as s_sdt,
     cast(a.total_bid_sub_amount as decimal(26,0)) total_bid_sub_amount,     -- 分包中标金额
    a.first_category_code,
    a.first_category_name,
    a.second_category_code,
    a.second_category_name,
    a.new_classify_name,
    sdt
FROM csx_analyse_tmp.csx_analyse_tmp_crm_top_business_result a


;



CREATE TABLE `bid_info` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键ID',
  `bid_name` varchar(100) NOT NULL DEFAULT '' COMMENT '项目名称',
  `bid_number` varchar(50) NOT NULL DEFAULT '' COMMENT '项目编号',
  `bid_customer_name` varchar(50) NOT NULL DEFAULT '' COMMENT '项目客户名称',
  `sales_user_id` bigint(20) NOT NULL DEFAULT '0' COMMENT '销售Id',
  `sales_user_name` varchar(25) NOT NULL DEFAULT '' COMMENT '销售名称',
  `bid_user_id` bigint(20) NOT NULL DEFAULT '0' COMMENT '投标负责人Id',
  `bid_user_name` varchar(25) NOT NULL DEFAULT '' COMMENT '投标负责人名称',
  `sales_province` varchar(45) NOT NULL DEFAULT '' COMMENT '销售省份',
  `sales_city` varchar(45) NOT NULL DEFAULT '' COMMENT '销售城市',
  `project_customer_province` varchar(45) NOT NULL DEFAULT '' COMMENT '客户省份',
  `project_customer_city` varchar(45) NOT NULL DEFAULT '' COMMENT '客户城市',
  `first_category_code` varchar(11) NOT NULL DEFAULT '' COMMENT '一级客户分类',
  `second_category_code` varchar(11) NOT NULL DEFAULT '' COMMENT '二级客户分类',
  `third_category_code` varchar(11) NOT NULL DEFAULT '' COMMENT '三级客户分类',
  `category` varchar(200) NOT NULL DEFAULT '' COMMENT '品类',
  `contact_person` varchar(45) NOT NULL DEFAULT '' COMMENT '客户联系人名称',
  `contact_phone` varchar(18) NOT NULL DEFAULT '' COMMENT '客户联系人电话',
  `bid_agent_name` varchar(50) NOT NULL DEFAULT '' COMMENT '代理机构名称',
  `agent_person` varchar(45) NOT NULL DEFAULT '' COMMENT '代理联系人名称',
  `agent_phone` varchar(18) NOT NULL DEFAULT '' COMMENT '代理联系人电话',
  `supply_deadline` int(11) NOT NULL DEFAULT '-1' COMMENT '服务期限（月）',
  `get_bid_date` varchar(25) NOT NULL DEFAULT '' COMMENT '获得标讯日期',
  `early_work` tinyint(4) NOT NULL DEFAULT '-1' COMMENT '前期工作 1.盲投 2.销售先行 3.销售后行',
  `history_win_remark` varchar(100) NOT NULL DEFAULT '' COMMENT '历史中标情况',
  `horizontal_score` varchar(10) NOT NULL DEFAULT '' COMMENT '横向打分分值',
  `objective_subtract_score` varchar(100) NOT NULL DEFAULT '' COMMENT '客观分扣分分值',
  `base_price_type` tinyint(4) NOT NULL DEFAULT '-1' COMMENT '基准价方式 1：低价 2：均价 3：其他',
  `use_attribute` tinyint(4) NOT NULL DEFAULT '-1' COMMENT '历史合作类型 0：未合作 1：日配 2：福利 3：大宗贸易 4：M端 5：BBC',
  `all_win_bid_amount` varchar(20) NOT NULL DEFAULT '' COMMENT '最大中标金额（万元）',
  `all_win_bid_max_amount` varchar(20) NOT NULL DEFAULT '' COMMENT '最大可中标金额（万元）',
  `all_bid_in_amount` varchar(20) NOT NULL DEFAULT '' COMMENT '投标总金额（万元）',
  `all_bid_amount` decimal(26,2) NOT NULL DEFAULT '0.00' COMMENT '标的总金额（万元）',
  `all_win_bid_count` int(11) NOT NULL DEFAULT '-1' COMMENT '可中标分包数',
  `bid_sub_in_count` int(11) NOT NULL DEFAULT '-1' COMMENT '投标分包数',
  `bid_sub_count` int(11) NOT NULL DEFAULT '-1' COMMENT '分包数量',
  `create_user_id` bigint(20) NOT NULL DEFAULT '0' COMMENT '创建人Id',
  `attribute` varchar(20) NOT NULL DEFAULT '' COMMENT '业务类型,分割 1：日配 2：福利 3：大宗贸易 4：M端 5：BBC 6：内购',
  `price_contrast` tinyint(4) NOT NULL DEFAULT '-1' COMMENT '报价对标口径 1市场 2超市 3线上商城 4网站 5其他',
  `subjective_gap_score` varchar(10) NOT NULL DEFAULT '' COMMENT '主观分最大分差',
  `subjective_score` varchar(10) NOT NULL DEFAULT '' COMMENT '主观分',
  `objective_score` varchar(10) NOT NULL DEFAULT '' COMMENT '客观分',
  `price_score` varchar(10) NOT NULL DEFAULT '' COMMENT '价格分',
  `city_service_flag` tinyint(4) NOT NULL DEFAULT '-1' COMMENT '是否前置仓 0否 1是',
  `bid_stage` tinyint(4) NOT NULL DEFAULT '1' COMMENT '标讯阶段 1投标前 2投标中 3投标后',
  `bid_no` varchar(50) NOT NULL DEFAULT '' COMMENT '标讯号',
  `bid_link` varchar(500) NOT NULL DEFAULT '' COMMENT '标讯链接',
  `bid_customer_info` varchar(500) NOT NULL DEFAULT '' COMMENT '客户及项目信息',
  `cost_profit` varchar(500) NOT NULL DEFAULT '' COMMENT '成本及毛利核算',
  `bid_segment` varchar(500) NOT NULL DEFAULT '' COMMENT '招投标环节',
  `compete_situation` varchar(500) NOT NULL DEFAULT '' COMMENT '竞争对手情况',
  `other_price` varchar(5000) NOT NULL DEFAULT '' COMMENT '开标各方报价',
  `bid_result` varchar(500) NOT NULL DEFAULT '' COMMENT '项目结果',
  `settlement_rule` varchar(500) NOT NULL DEFAULT '' COMMENT '结算规则',
  `price_rule` varchar(500) NOT NULL DEFAULT '' COMMENT '报价规则',
  `bid_files` json DEFAULT NULL COMMENT '招标文件',
  `contract_files` json DEFAULT NULL COMMENT '合同文件',
  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `create_by` varchar(45) NOT NULL DEFAULT 'sys' COMMENT '创建人',
  `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  `update_by` varchar(45) NOT NULL DEFAULT 'sys' COMMENT '更新人',
  `bid_customer_type` tinyint(4) NOT NULL DEFAULT '-1' COMMENT '作废-客户类型 1新客户 2老客户',
  `bid_name_alias` varchar(100) NOT NULL DEFAULT '' COMMENT '作废-项目名称别名',
  `win_bid_amount` varchar(20) NOT NULL DEFAULT '' COMMENT '作废-中标金额（万元）',
  `bid_result_remark` varchar(50) NOT NULL DEFAULT '' COMMENT '作废-项目结果备注',
  `bid_result_analysis` varchar(500) NOT NULL DEFAULT '' COMMENT '作废-结果分析',
  `up_rate` varchar(20) NOT NULL DEFAULT '' COMMENT '作废-上浮或折扣率',
  `down_rate` varchar(20) NOT NULL DEFAULT '' COMMENT '作废-下浮或折扣率',
  `win_bid_date` varchar(25) NOT NULL DEFAULT '' COMMENT '作废-公示结果、中标日期',
  `bid_company` varchar(50) NOT NULL DEFAULT '' COMMENT '作废-投标主体',
  `bid_goods` varchar(500) NOT NULL DEFAULT '' COMMENT '作废-标的物',
  `history_attribute` varchar(20) NOT NULL DEFAULT '' COMMENT '作废-历史合作类型,分割 1：日配客户 2：福利客户 5：BBC',
  `cooperation_type` tinyint(4) NOT NULL DEFAULT '0' COMMENT '作废-合作形式 1.自营 2.城市服务商',
  `bid_source` tinyint(4) NOT NULL DEFAULT '0' COMMENT '作废-投标来源 1.每日推送 2.销售提供 3.每日推送&销售提供',
  `bid_date` varchar(25) NOT NULL DEFAULT '' COMMENT '作废-投标日期',
  `enroll_date_end` varchar(25) NOT NULL DEFAULT '' COMMENT '作废-报名截止日期',
  `bid_amount_max` varchar(20) NOT NULL DEFAULT '' COMMENT '作废-项目最大中标金额（万元）',
  `win_bid_count` int(11) NOT NULL DEFAULT '0' COMMENT '作废-中标家数',
  `bid_package_max` int(11) NOT NULL DEFAULT '0' COMMENT '作废-项目最大中标包数',
  `bid_send_package` int(11) NOT NULL DEFAULT '0' COMMENT '作废-投标包数',
  `bid_package` int(11) NOT NULL DEFAULT '0' COMMENT '作废-项目包数',
  `bid_amount` varchar(20) NOT NULL DEFAULT '' COMMENT '作废-标的金额（万元）',
  `business_attribute` tinyint(4) NOT NULL DEFAULT '0' COMMENT '作废-业务类型 1：日配客户 2：福利客户 3：大宗贸易 4：M端 5：BBC 6：内购',
  `notice_type` tinyint(4) NOT NULL DEFAULT '0' COMMENT '作废-公告类型 1招标公告 2意向公告',
  `guid_bid_user_name` varchar(25) NOT NULL DEFAULT '' COMMENT '作废-投标指导人名称',
  `guid_bid_user_id` bigint(20) NOT NULL DEFAULT '0' COMMENT '作废-投标指导人Id',
  `guid_sales_user_name` varchar(25) NOT NULL DEFAULT '' COMMENT '作废-销售指导人名称',
  `guid_sales_user_id` bigint(20) NOT NULL DEFAULT '0' COMMENT '作废-销售指导人Id',
  `approval_status` tinyint(4) NOT NULL DEFAULT '0' COMMENT '作废-审批状态 1：审批中 2：审批完成 3：审批拒绝',
  `approval_type` tinyint(4) NOT NULL DEFAULT '0' COMMENT '作废-审批类型 1：关联商机 2：确认报名 3：主动弃标',
  `bid_ascription` tinyint(4) NOT NULL DEFAULT '1' COMMENT '作废-标讯归属 0.删除 1.标讯池 2.历史标讯 3.标讯池&历史标讯 4.弃标标讯',
  `bid_change_reason` varchar(100) NOT NULL DEFAULT '' COMMENT '作废-变更原因',
  `bid_status` tinyint(4) NOT NULL DEFAULT '1' COMMENT '作废-标讯状态 1.未报名 2.已报名 3.投标中 4.未公示 5.中标 6.未中标 7.流标（投标中） 8.流标（投标后） 9.弃标（投标前） 10.弃标（投标中） 11.弃标（投标后） 12.项目取消（投标前） 13.项目取消（投标中） 14.项目取消（投标后）',
  `business_number` varchar(20) NOT NULL DEFAULT '' COMMENT '作废-商机编号',
  `bid_name_number` varchar(150) NOT NULL DEFAULT '' COMMENT '作废-项目名称加编号',
  `shipper_name` varchar(30) NOT NULL DEFAULT '永辉彩食鲜' COMMENT '租户简称',
  `shipper_code` varchar(20) NOT NULL DEFAULT 'YHCSX' COMMENT '租户编码',
  PRIMARY KEY (`id`) USING BTREE,
  KEY `idx_name_province` (`bid_name`,`sales_province`) USING BTREE,
  KEY `idx_user_province` (`sales_user_id`,`bid_user_id`,`create_user_id`,`sales_province`) USING BTREE,
  KEY `idx_bid_no` (`bid_no`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=81034 DEFAULT CHARSET=utf8mb4 COMMENT='投标信息';