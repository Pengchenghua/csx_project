-- 投标日报统计 20250724
-- drop table csx_analyse_tmp.csx_analyse_tmp_fr_bid_daily_report_di;
create table csx_analyse_tmp.csx_analyse_tmp_fr_bid_daily_report_di as 
with tmp_business_info as (
  SELECT
    a.customer_code,
    a.customer_name,
    a.business_number,
    a.business_stage,
    a.business_attribute_name,
    cast(a.estimate_contract_amount as decimal(26,1)) estimate_contract_amount,
    owner_user_number,
    owner_user_name,
    create_time,
    contract_end_date,
    CASE
      WHEN a.business_stage = 1 THEN '10%'
      WHEN a.business_stage = 2 THEN '25%'
      WHEN a.business_stage = 3 THEN '50%'
      WHEN a.business_stage = 4 THEN '75%'
      WHEN a.business_stage = 5 THEN '100%'
      ELSE ''
    END AS business_stage_name,
     ROW_NUMBER() OVER ( PARTITION BY a.customer_code, a.business_attribute_code  ORDER BY  a.create_time desc  ) AS rn,
    LAG(a.business_number, 1) OVER (
      PARTITION BY a.customer_code, a.business_attribute_code
      ORDER BY a.create_time DESC
    ) AS prev_business_number,
    LAG(a.estimate_contract_amount, 1) OVER (
      PARTITION BY a.customer_code, a.business_attribute_code
      ORDER BY a.create_time DESC
    ) AS prev_business_amount,
    status,
    business_sign_time,
    first_business_sign_time
  FROM
     csx_dim.csx_dim_crm_business_info_hi a
  WHERE
    a.sdt = 'current'
),
tmp_business_contract as 
(select customer_code,
        prev_business_number ,
        contract_end_date
from tmp_business_info
 where  business_stage_name in ('100%') 
    and status=1
 )
 ,
 tmp_business_info_01 as (
  SELECT
    a.customer_code,
    a.customer_name,
    a.business_number,
    a.business_stage,
    a.business_attribute_name,
    estimate_contract_amount,
    owner_user_number,
    owner_user_name,
    create_time,
    a.contract_end_date,
    a.business_stage_name,
    a.prev_business_number,
    a.prev_business_amount,
     CASE WHEN a.business_attribute_name IN ('BBC','福利') THEN '新客'
      WHEN a.business_attribute_name NOT IN ('BBC','福利') AND  (TO_DATE(a.first_business_sign_time) = TO_DATE(a.business_sign_time)  OR a.first_business_sign_time IS NULL ) THEN  '新客'
      WHEN to_date(create_time)>= to_date(b.contract_end_date) then '新客'  
      ELSE '老客' END  AS is_new_flag,
    status
  FROM
     tmp_business_info a
  LEFT JOIN 
     tmp_business_contract b on a.business_number=b.prev_business_number 
)
-- select * from tmp_business_info_01 where  business_number in ('SJ24060600039','SJ24060600044','SJ24060300046','SJ24060300048','SJ24060300049')

,

csx_analyse_tmp_bid_new as 
(   select 
       a.bid_customer_name,
       a.bid_no,
       a.business_attribute_name,
       case when c.bid_customer_name is null then '新客' else '老客' end as customer_type
FROM 
    csx_analyse.csx_analyse_crm_bid_info_new a 
left join 
    csx_analyse.csx_analyse_crm_bid_info_new c
    on a.bid_no != c.bid_no 
    and a.bid_customer_name = c.bid_customer_name 
    and a.business_attribute_name = c.business_attribute_name
    and c.early_sub_bid_date < a.early_sub_bid_date
where 
    a.sdt = '${sdt_yes}'
    and a.business_attribute_name = '日配'
    and a.sub_bid_status_name in ('中标','入围')
group by  
    a.bid_customer_name,
    a.bid_no,
    a.business_attribute_name,
    c.bid_customer_name 
) ,
tmp_bid_info as 
(select
  a.sdt,
  a.bid_no,
  
  to_date(early_sub_bid_date) early_sub_bid_date ,  -- 项目标讯日期
  performance_province_name,
  performance_city_name,
  concat_ws('-',project_customer_province_name, project_customer_city_name) project_customer_city_name,
  a.bid_name,
  a.bid_number,
  sales_user_name,
  bid_user_name,
  concat_ws(',',collect_set(a.bid_customer_name)) bid_customer_name,
  concat_ws(',',collect_set(a.second_category_name)) second_category_name,
  concat_ws(',',collect_set(cast(supply_deadline as string ) ))   supply_deadline,            -- 服务期限
  max(all_bid_amount ) as all_bid_amount,       -- 标的总金额（万）
  max(all_bid_in_amount ) as all_bid_in_amount, -- 投标总金额（万）
  max(all_win_bid_max_amount ) as all_win_bid_max_amount,       -- 最大可中金额（万）
  max(bid_sub_count ) as bid_sub_count,             -- 分包数量
  max(bid_sub_in_count ) as bid_sub_in_count,       -- 投标分包数
  max(all_win_bid_count ) as all_win_bid_count,     -- 可中标分包数
  concat_ws(',',collect_set(a.business_attribute_name ))   attribute_name,
  max(city_service_flag ) city_service_flag,        -- 是否分仓
  concat_ws(',',collect_set(sub_bid_company))   sub_bid_company,
  sum(sub_bid_amount   ) as sub_bid_amount,
  sum(sub_win_bid_count) as sub_win_bid_count,
  concat_ws(',',collect_set(a.sub_bid_status_name )) sub_bid_status_name,
  min(sub_win_bid_date) sub_win_bid_date , -- 公示结果日期
  sum(sub_win_bid_amount) as sub_win_bid_amount, -- 分包中标金额 
  concat_ws(',',collect_set(a.sub_bid_result )) sub_bid_result 
from
     csx_analyse.csx_analyse_crm_bid_info_new a

 where a.sdt = '${sdt_yes}'
 group by a.sdt,
  a.bid_no,
  to_date(early_sub_bid_date)  ,  -- 项目标讯日期
  performance_province_name,
  performance_city_name,
  concat_ws('-',project_customer_province_name, project_customer_city_name) ,
  a.bid_name,
  a.bid_number,
  sales_user_name,
  bid_user_name
),
tmp_bid_business_info as 
(select 
    a.bid_no,
    a.business_number,
    customer_name,
    owner_user_name,
    estimate_contract_amount,
    business_attribute_name,
    business_stage_name,
    is_new_flag
from 
 (select
  a.bid_no,
  a.business_number
from
      csx_analyse.csx_analyse_crm_bid_info_new a
 where a.sdt = '${sdt_yes}'
 group by 
  a.bid_no,
  business_number
  ) a 
  left join 
  (select business_number,
            customer_name,
            owner_user_name,
            estimate_contract_amount,
            business_attribute_name,
            business_stage_name,
            is_new_flag
    from tmp_business_info_01 
  )b on coalesce(a.business_number,'')=b.business_number
)
-- INSERT overwrite table csx_analyse_tmp.csx_analyse_tmp_tomysql_report_bid_info 
select
  a.sdt,
  a.bid_no,
  early_sub_bid_date ,  -- 项目标讯日期
  performance_province_name,
  performance_city_name,
  project_customer_city_name,
  sales_user_name,
  bid_user_name,
  a.bid_name,
  a.bid_number,
  a.bid_customer_name,
  a.second_category_name,
  a.supply_deadline,            -- 服务期限
  a.all_bid_amount  as all_bid_amount,       -- 标的总金额（万）
  a.all_bid_in_amount  as all_bid_in_amount, -- 投标总金额（万）
  a.all_win_bid_max_amount  as all_win_bid_max_amount,       -- 最大可中金额（万）
  a.bid_sub_count  as bid_sub_count,             -- 分包数量
  a.bid_sub_in_count  as bid_sub_in_count,       -- 投标分包数
  a.all_win_bid_count  as all_win_bid_count,     -- 可中标分包数
  concat_ws('/',cast(all_win_bid_count as string), cast(bid_sub_in_count as string), cast(bid_sub_count as string)) bid_count,        --最大可中标包数/投标包数/分包数量
  a.attribute_name,
  if(city_service_flag=1,'是','否' ) city_service_flag,        -- 是否分仓
  a.sub_bid_company  sub_bid_company,
  a.sub_bid_amount  as sub_bid_amount,
  a.sub_win_bid_count as sub_win_bid_count,
  a.sub_bid_status_name sub_bid_status_name,
  a.sub_win_bid_date sub_win_bid_date , -- 公示结果日期
  a.sub_win_bid_amount as sub_win_bid_amount, -- 分包中标金额 
  a.sub_bid_result sub_bid_result,
--   a.business_number)) != '', 1, 0) is_link_business,
  b.business_number ,
  b.customer_name as business_customer_name,
  b.estimate_contract_amount ,
  b.business_stage_name business_stage_name,
  if(b.is_new_flag=1 or c.bid_no is null  ,'新客','老客' ) is_new_flag,
  owner_user_name owner_user_name,
  b.business_attribute_name business_attribute_name
from
     tmp_bid_info a
  left join (
  select bid_no,
        concat_ws('、',collect_set(business_number)) business_number,
        concat_ws('、',collect_set(customer_name)) customer_name,
        concat_ws('、',collect_set(owner_user_name)) owner_user_name,
        concat_ws('、',collect_set(business_attribute_name)) business_attribute_name,
        sum(cast(estimate_contract_amount as decimal(26,1))) estimate_contract_amount,
        min(if(is_new_flag='新客',1,0)) as is_new_flag,
        max(business_stage_name) business_stage_name
  from 
            tmp_bid_business_info
  group by bid_no
  ) b on a.bid_no = b.bid_no
  left join 
    csx_analyse_tmp_bid_new  c on a.bid_no =c.bid_no 
 


CREATE   TABLE `csx_analyse_tmp`.`csx_analyse_tmp_fr_bid_daily_report_di`(
  `sdt` string comment '日期分区 ', 
  `bid_no` string comment '标讯号', 
  `early_sub_bid_date` string comment '投标日期', 
  `performance_province_name` string comment '业绩省区名称', 
  `performance_city_name` string comment '业绩城市名称', 
  `project_customer_city_name` string comment '投标客户城市名称', 
  `sales_user_name` string  comment '业绩销售人员名称', 
  `bid_user_name` string comment '投标销售人员名称', 
  `bid_name` string comment '投标名称', 
  `bid_number` string comment '投标编号', 
  `bid_customer_name` string comment '客户名称', 
  `second_category_name` string comment '二级分类名称', 
  `supply_deadline` string comment '服务期限', 
  `all_bid_amount` string comment '标的总金额（万）', 
  `all_bid_in_amount` string comment '投标总金额（万）', 
  `all_win_bid_max_amount` string comment '最大可中金额（万）', 
  `bid_sub_count` string comment '分包数量', 
  `bid_sub_in_count` string comment '投标分包数', 
  `all_win_bid_count` string comment '可中标分包数', 
  `bid_count` string comment '最大可中标包数/投标包数/分包数量', 
  `attribute_name` string comment '属性名称', 
  `city_service_flag` string comment '是否分仓', 
  `sub_bid_company` string comment '分包公司', 
  `sub_bid_amount` string comment '分包金额', 
  `sub_win_bid_count` string comment '分包中标数量', 
  `sub_bid_status_name` string comment '分包状态', 
  `sub_win_bid_date` string comment '分包中标日期', 
  `sub_win_bid_amount` string comment '分包中标金额', 
  `sub_bid_result` string comment '分包结果', 
  `business_number` string comment '商机号', 
  `business_customer_name` string comment '商机客户名称', 
  `estimate_contract_amount` string comment '预计合同金额', 
  `business_stage_name` string comment '商机阶段', 
  `is_new_flag` string comment '是否新客户', 
  `owner_user_name` string comment '商机归属人', 
  `business_attribute_name` string comment '商机属性',
  `update_time` timestamp comment '更新时间'
) comment '标讯统计日报表'
STORED AS parquet
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'


;

-- 同步MYSQL创建表
CREATE   TABLE csx_data_analysis_prd.report_csx_analyse_tmp_fr_bid_daily_report_di
(
  `id` bigint   auto_increment  comment 'id',
  `sdt` varchar(64) comment '日期分区 ', 
  `bid_no` varchar(64) comment '标讯号', 
  `early_sub_bid_date` varchar(64) comment '投标日期', 
  `performance_province_name` varchar(64) comment '业绩省区名称', 
  `performance_city_name` varchar(64) comment '业绩城市名称', 
  `project_customer_city_name` varchar(64) comment '投标客户城市名称', 
  `sales_user_name` varchar(64) comment '业绩销售人员名称', 
  `bid_user_name` varchar(64) comment '投标销售人员名称', 
  `bid_name` varchar(255) comment '投标名称', 
  `bid_number` varchar(128) comment '投标编号', 
  `bid_customer_name` varchar(255) comment '客户名称', 
  `second_category_name` varchar(128) comment '二级分类名称', 
  `supply_deadline` varchar(64) comment '服务期限', 
  `all_bid_amount` varchar(64) comment '标的总金额（万）', 
  `all_bid_in_amount` varchar(64) comment '投标总金额（万）', 
  `all_win_bid_max_amount` varchar(64) comment '最大可中金额（万）', 
  `bid_sub_count` varchar(64) comment '分包数量', 
  `bid_sub_in_count` varchar(64) comment '投标分包数', 
  `all_win_bid_count` varchar(64) comment '可中标分包数', 
  `bid_count` varchar(64) comment '最大可中标包数/投标包数/分包数量', 
  `attribute_name` varchar(64) comment '属性名称', 
  `city_service_flag` varchar(64) comment '是否分仓', 
  `sub_bid_company` varchar(64) comment '分包公司', 
  `sub_bid_amount` varchar(64) comment '分包金额', 
  `sub_win_bid_count` varchar(64) comment '分包中标数量', 
  `sub_bid_status_name` varchar(64) comment '分包状态', 
  `sub_win_bid_date` varchar(64) comment '分包中标日期', 
  `sub_win_bid_amount` varchar(64) comment '分包中标金额', 
  `sub_bid_result` varchar(255) comment '分包结果', 
  `business_number` varchar(64) comment '商机号', 
  `business_customer_name` varchar(255) comment '商机客户名称', 
  `estimate_contract_amount` varchar(64) comment '预计合同金额', 
  `business_stage_name` varchar(64) comment '商机阶段', 
  `is_new_flag` varchar(64) comment '是否新客户', 
  `owner_user_name` varchar(64) comment '商机归属人', 
  `business_attribute_name` varchar(64) comment '商机属性',
  `update_time` timestamp comment '更新时间',
  primary key(id),
  unique key uk_sdt_bid_no(sdt, performance_province_name,bid_status_name)
) comment= '标讯统计日报表'
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;