
-- 履约数据
 with tmp_business_info as (SELECT
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
    a.business_number,
    a.owner_user_number,
    a.owner_user_name,
    a.owner_user_position,
    a.business_attribute_name,
    a.business_stage,
    a.estimate_contract_amount,
    a.contract_cycle_int,
    a.company_code,
    a.expect_execute_time,
    a.expect_sign_time,
    a.first_business_sign_time,
    a.business_sign_time,
    a.first_sign_time,
    a.create_time,
    a.status,
    business_attribute_code,
    contract_end_date,
    contract_begin_date,
    CASE
      WHEN a.business_stage = 1 THEN '10%'
      WHEN a.business_stage = 2 THEN '25%'
      WHEN a.business_stage = 3 THEN '50%'
      WHEN a.business_stage = 4 THEN '75%'
      WHEN a.business_stage = 5 AND contract_type =1 THEN '试配(75%)'
      WHEN a.business_stage = 5 and contract_type =2 THEN '100%'
      ELSE ''
    END AS business_stage_name,     
    a.business_source_type,               --  '商机来源 1投标 2非投标 3续签',
    a.business_source_second_type,           --  '商机二级来源 1正式投标 2跟标 3未挂网 4拜访 5内邀',
    a.welfare_type_code ,                      --  '福利类型 1福利单 2福利小店'
    a.business_type_name,
    b.no
  FROM
     csx_dim.csx_dim_crm_business_info_hi a
    join csx_analyse_tmp.business_number b on a.business_number=b.business_number
  WHERE
    a.sdt = 'current'
    and business_stage=5
    -- 增加审指状态，belong_approval_status = 2 已审批，belong_center_flow_id 或者无审批节点
    AND (belong_approval_status = 2 or belong_center_flow_id =-1)
),   
tmp_customer_info as 
(select   no,
    customer_code,
    min(business_sign_time) business_sign_time
from tmp_business_info
    group by  no,
    customer_code
    ),
tmp_sale_detail as 
(select business_type_name,
    business_attribute_desc,
    a.customer_code,
    b.no,
    min(sdt) min_sdt,
    sum(sale_amt) sale_amt 
     
from    csx_dws.csx_dws_sale_detail_di a 
join tmp_customer_info b on a.customer_code=b.customer_code 
left join 
(select   no,
    min(regexp_replace(to_date(business_sign_time),'-','')) business_sign_time
from tmp_business_info
    group by  no
    ) c on b.no=c.no
where sdt>= c.business_sign_time
group by  business_type_name,
    a.customer_code,
    business_attribute_desc,
    b.no
)
select * from 
 tmp_sale_detail
 ;




-- 履约数据
 with tmp_business_info as (SELECT
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
    a.business_number,
    a.owner_user_number,
    a.owner_user_name,
    a.owner_user_position,
    a.business_attribute_name,
    a.business_stage,
    a.estimate_contract_amount,
    a.contract_cycle_int,
    a.company_code,
    a.expect_execute_time,
    a.expect_sign_time,
    a.first_business_sign_time,
    a.business_sign_time,
    a.first_sign_time,
    a.create_time,
    a.status,
    business_attribute_code,
    contract_end_date,
    contract_begin_date,
    CASE
      WHEN a.business_stage = 1 THEN '10%'
      WHEN a.business_stage = 2 THEN '25%'
      WHEN a.business_stage = 3 THEN '50%'
      WHEN a.business_stage = 4 THEN '75%'
      WHEN a.business_stage = 5 AND contract_type =1 THEN '试配(75%)'
      WHEN a.business_stage = 5 and contract_type =2 THEN '100%'
      ELSE ''
    END AS business_stage_name,     
    a.business_source_type,               --  '商机来源 1投标 2非投标 3续签',
    a.business_source_second_type,           --  '商机二级来源 1正式投标 2跟标 3未挂网 4拜访 5内邀',
    a.welfare_type_code ,                      --  '福利类型 1福利单 2福利小店'
    a.business_type_name,
    b.no
  FROM
     csx_dim.csx_dim_crm_business_info_hi a
    join csx_analyse_tmp.business_number b on a.business_number=b.business_number
  WHERE
    a.sdt = 'current'
    and business_stage=5
    -- 增加审指状态，belong_approval_status = 2 已审批，belong_center_flow_id 或者无审批节点
    AND (belong_approval_status = 2 or belong_center_flow_id =-1)
),   
tmp_customer_info as 
(select   no,
    -- business_number,
    concat_ws('、',collect_set(customer_code))  as list_costomer,
    min(regexp_replace(to_date(business_sign_time),'-','')) business_sign_time,
    sum(if(business_stage=5,estimate_contract_amount,0)) estimate_contract_amount
from tmp_business_info
    group by  no 
    )
select * from tmp_customer_info
 ;
