-- drop table  csx_analyse_tmp.csx_analyse_tmp_bid_info ; 
create TABLE csx_analyse_tmp.csx_analyse_tmp_bid_info as 
with tmp_bid_00 as (
select a.id,
    bid_no,
    coalesce(b.performance_region_code,b2.performance_region_code, '') as performance_region_code,
    coalesce(b.performance_region_name,b2.performance_region_name, '') as performance_region_name,
    coalesce(b.performance_province_code,b2.performance_province_code, '') as performance_province_code,
    coalesce(b.performance_province_name,b2.performance_province_name, '') as performance_province_name,
    coalesce(b.performance_city_code, '') as performance_city_code,
    coalesce(b.performance_city_name, '') as performance_city_name,	
    regexp_replace(bid_name, '\\s+', '') bid_name,
    regexp_replace(bid_name_alias, '\\s+', '') bid_name_alias,
    regexp_replace(bid_number, '\\s+', '') bid_number, 
    regexp_replace(bid_name_number, '\\s+', '') bid_name_number,
    regexp_replace(bid_customer_name, '\\s+', '') bid_customer_name,
    business_number,
    sales_province,
    d.province,
    project_customer_province,
    coalesce(if(a.project_customer_province in ('16','33','34','35','36'), '100000', a.project_customer_city), '') as customer_city_code,
    coalesce(e.customer_city_name, '') as customer_city_name	,
    first_category_code,
    second_category_code	,
    use_attribute business_attribute,
    coalesce(g.name,'') business_attribute_name,
    all_win_bid_amount,                 -- 最大中标金额
    all_win_bid_max_amount	,          --最大可中标金额
    all_bid_in_amount,                  -- 投标总金额对应中标金额
    all_bid_amount,         -- 标的总金额
    all_win_bid_count,      -- 可中标分包数
    bid_sub_in_count,       -- 投标分包数
    bid_sub_count,          -- 分包数量
    supply_deadline,
    -- enroll_date_end,
    bid_date,
    bid_company,
    company_name,
    early_work,
    coalesce(if(city_service_flag=1,'Q',n2.early_work_name),'') early_work_name,
    sales_user_name,
    use_attribute,
    city_service_flag
from     csx_ods.csx_ods_csx_crm_prod_bid_info_df a 
left join
	( -- 获取客户属性
		select code, name from csx_ods.csx_ods_csx_crm_prod_sys_dict_df
		where sdt = '${sdt_yes}' and parent_code = 'customer_attr'
		and shipper_code='YHCSX'
	) g on cast(a.attribute as string) = g.code  
left join
	( -- 获取省区
		select province_code,
		    province
		 from csx_ods.csx_ods_csx_crm_prod_sys_province_df
		where sdt = '${sdt_yes}'
		and shipper_code='YHCSX'
	) d on a.sales_province = d.province_code
left join
	( -- 获取数据中心定义业绩归属省区与城市信息
		select distinct
			city_code,
			province_id,
			performance_region_code,
			performance_region_name,
			performance_province_code,
			performance_province_name,
			performance_city_code,
			performance_city_name
		from csx_dim.csx_dim_sales_area_belong_mapping
		where sdt = 'current'
		and province_id not in('38')

	) b on a.sales_province = b.province_id 
			and if(a.sales_province in ('16','33','34','35','36'), '100000', a.sales_city) = b.city_code
left join
	( -- 获取数据中心定义业绩归属省区与城市信息
		select distinct
			-- city_code,
			province_id,
			performance_region_code,
			performance_region_name,
			performance_province_code,
			performance_province_name
			-- performance_city_code,
			-- performance_city_name
		from csx_dim.csx_dim_sales_area_belong_mapping
		where sdt = 'current'
		and province_id not in('2','10','20') -- 江苏 上海 province_id=20 performance_province_code：广东省20 广东广州906
		and city_code !='330400'
		union all
		select 2 as province_id, '1' as performance_region_code, '华东大区' as performance_region_name, '2' as performance_province_code, '上海' as performance_province_name
		union all
		select 10 as province_id, '1' as performance_region_code, '华东大区' as performance_region_name, '10' as performance_province_code, '上海' as performance_province_name
		union all
		select 20 as province_id, '2' as performance_region_code, '华南大区' as performance_region_name, '20' as performance_province_code, '广东深圳' as performance_province_name
	) b2 on a.sales_province = b2.province_id 
	left join 
	(
		select company_code,company_name
		from csx_dim.csx_dim_basic_company
		where sdt='${sdt_yes}'
	)m on a.bid_company = m.company_code 
	left join 
	( -- 前期工作 early_work
	select code,name as early_work_name
	from csx_ods.csx_ods_csx_crm_prod_sys_dict_df
	where sdt='${sdt_yes}'
	and status='0'
	and parent_code='early_work'
	)n2 on a.early_work = n2.code
	left join
	( -- 获取客户城市，直辖市可以取到区
		select region_code, region_name as customer_city_name
		from csx_ods.csx_ods_csx_crm_prod_sys_region_df
		where sdt = '${sdt_yes}' and (region_level in (0, 2) or parent_code in ('110100','120100','310100','500100'))
		and shipper_code='YHCSX'
	) e on if(a.project_customer_province in ('16','33','34','35','36'), '100000', a.project_customer_city) = e.region_code   
where sdt='${sdt_yes}'
and bid_date<>''
    and all_win_bid_max_amount>=1000
    and attribute=1
)
-- select * from tmp_bid_00 where id='109890'
,
tmp_sub_bid as (
select
  a.id as sub_id,
  a.bid_id,
  regexp_replace(a.sub_bid_name, '\\s+', '')  sub_bid_name,
  a.sub_bid_company,
  a.sub_bid_date,
  a.sub_bid_amount,
  b.business_number,
  sub_win_bid_count,
  sub_win_bid_amount,
  sub_bid_result,
  sub_bid_status,
  bid_sub_status_name,
  concat(sub_bid_name,'：',cast(sub_win_bid_count as string ),'家') sub_bid_name_note
from
          csx_ods.csx_ods_csx_crm_prod_bid_sub_info_df a
  left join (
    select
      bid_id,
      bid_sub_id,
      business_number
    from
      csx_ods.csx_ods_csx_crm_prod_bid_business_relation_df
    where
      status = 1
  ) b on a.id = b.bid_sub_id
  and a.bid_id = b.bid_id
  left join 
	( -- 标讯状态码表  bid_status
	-- 使用旧码表
	select code,
	name as bid_sub_status_name
	from csx_ods.csx_ods_csx_crm_prod_sys_dict_df
	where
-- 	sdt='20250223'
	sdt='${sdt_yes}'
	and status='0'
	and parent_code='bid_status'
	and shipper_code='YHCSX'
	)n1 on a.sub_bid_status = n1.code 
    -- where sub_bid_status!='7' -- 过滤项目取消
  ),
  tmp_business_info as 
  (select
  performance_region_code,
  performance_region_name,
  performance_province_code,
  performance_province_name,
  performance_city_code,
  performance_city_name,
  owner_city_code,
  owner_city_name,
  customer_code,
  customer_name,
  business_number,
  owner_user_name,
  business_attribute_name,
  business_stage,
  case when a.business_stage=2 then '25%'
    when a.business_stage=3 then '50%'
    when a.business_stage=4 then '75%'
    when a.business_stage=5 then '100%'
    else ''
    end business_stage_name,
  estimate_contract_amount,
  contract_cycle_int,
  company_code,
  expect_execute_time,
  first_business_sign_time,
  business_sign_time,
  first_sign_time,
  create_time,
  expect_sign_time,
  if(to_date(first_business_sign_time)=to_date(business_sign_time) or first_business_sign_time is null ,'新客','老客') as is_new_flag,
  row_number()over(partition by customer_code,business_attribute_code order by create_time asc ) as rn
from
     csx_dim.csx_dim_crm_business_info a 
where
  sdt = 'current'
--   and status=1
    and business_attribute_code=1
 )
  select a.id,
    bid_no,
    a.performance_region_code,
    a.performance_region_name,
    a.performance_province_code,
    a.performance_province_name,
    a.performance_city_code,
    a.performance_city_name,	
    regexp_replace(bid_name, '\\s+', '') bid_name,
    regexp_replace(bid_name_alias, '\\s+', '') bid_name_alias,
    regexp_replace(bid_number, '\\s+', '') bid_number, 
    regexp_replace(bid_name_number, '\\s+', '') bid_name_number,
    regexp_replace(bid_customer_name, '\\s+', '') bid_customer_name,
    a.sales_province,
    a.province,
    -- a.sales_city,
    a.project_customer_province,
    a.customer_city_code	,
    a.customer_city_name,
    a.first_category_code,
    a.second_category_code	,
    a.business_attribute,
    a.business_attribute_name,
    all_win_bid_amount,                 -- 最大中标金额
    all_win_bid_max_amount	,          --最大可中标金额
    all_bid_in_amount,                  -- 投标总金额对应中标金额
    all_bid_amount,         -- 标的总金额
    all_win_bid_count,      -- 可中标分包数
    bid_sub_in_count,       -- 投标分包数
    bid_sub_count,          -- 分包数量
    a.supply_deadline,
    -- a.enroll_date_end,
    a.bid_date,
    a.bid_company,
    a.company_name,
    a.early_work,
    a.early_work_name,
    a.sales_user_name,
    b.sub_id,
    b.bid_id,
    regexp_replace(b.sub_bid_name, '\\s+', '')  sub_bid_name,
    b.sub_bid_company,
    b.sub_bid_date,
    b.sub_bid_amount,
    b.business_number,
    sub_bid_status,
    bid_sub_status_name,
    sub_bid_name_note,
    case when sub_bid_status ='2' or sub_bid_status='4' then '停止跟进'
        when (sub_bid_status !='2' or sub_bid_status !='4' or sub_bid_status!='7') and bid_date  > to_date(current_date()) then '待投标'
        when (sub_bid_status !='2' or sub_bid_status !='4' or sub_bid_status!='7') and bid_date  <= to_date(current_date()) then
                case when sub_bid_status='9' then '中标'
                    when sub_bid_status='8' then '未公示'
                    when sub_bid_status='12' then '废标'
                    when sub_bid_status='10' then '未中标'
                    when sub_bid_status='11' then '流标'
                    when sub_bid_status='13' then '放弃中标'
                    when sub_bid_status='7' then '项目暂停' 
                    else '未公示'
                    end 
        end bid_status_name,
    use_attribute,
    city_service_flag,
    if( coalesce(use_attribute,0)=0 ,'新客','老客') is_new_cust_flag,
    regexp_replace(b.sub_bid_result, '\\s+', '') sub_bid_result ,
    -- owner_city_code,
    -- owner_city_name,
    case when d.business_number is null then '增量' 
        when  coalesce(estimate_contract_amount,0)>=coalesce(last_estimate_amount,0) then '增量'
        else '存量'
        end is_incr,
        estimate_contract_amount,
        last_estimate_amount,
        contract_cycle_int,
        owner_user_name,
         business_stage_name,
        expect_sign_time,
        expect_execute_time,
        rn
  from tmp_bid_00 a 
  left join 
    tmp_sub_bid b on a.id=b.bid_id
  left join 
   (select c.business_number,
        c.rn,
        c.estimate_contract_amount,
        c.customer_code,
        c.contract_cycle_int,
        c.owner_user_name,
        c1.estimate_contract_amount as last_estimate_amount,
        business_stage_name,
        expect_sign_time,
        expect_execute_time
    from  tmp_business_info c
        left join 
        (select customer_code,
            business_number,
            rn,
            estimate_contract_amount
    from  tmp_business_info) c1 on c.customer_code=c1.customer_code and c.rn+1=c1.rn
    ) d 
   on b.business_number= d.business_number
 where sub_bid_status!='7'
  
  
  ;
  
  
  

  -- drop table csx_analyse_tmp.csx_analyse_tmp_business_info;
create table csx_analyse_tmp.csx_analyse_tmp_business_info as 
with tmp_business_info as ( 
select
  performance_region_code,
  performance_region_name,
  performance_province_code,
  performance_province_name,
  performance_city_code,
  performance_city_name,
  owner_city_code,
  owner_city_name,
  customer_code,
  customer_name,
  business_number,
  owner_user_name,
  business_attribute_name,
  business_stage,
  estimate_contract_amount,
  contract_cycle_int,
  a.company_code,
  company_name,
  expect_execute_time,
  expect_sign_time,
  first_business_sign_time,
  business_sign_time,
  first_sign_time,
  create_time,
  status,
  if(to_date(first_business_sign_time)=to_date(business_sign_time) or first_business_sign_time is null ,'新客','老客') as is_new_flag,
  row_number()over(partition by customer_code,business_attribute_code order by create_time asc ) as rn
from
           csx_dim.csx_dim_crm_business_info a 
         left join 
	(
		select company_code,company_name
		from csx_dim.csx_dim_basic_company
		where sdt='${sdt_yes}'
	)m on a.company_code = m.company_code 
where
  sdt = 'current'
--   and status=1
  and business_attribute_code=1
--   and estimate_contract_amount>=1000
--   and business_stage!=5
)
select a. performance_region_code,
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
  a.owner_user_name,
  a.business_attribute_name,
  a.business_stage,
  case when a.business_stage=2 then '25%'
    when a.business_stage=3 then '50%'
    when a.business_stage=4 then '75%'
    when a.business_stage=5 then '100%'
    else ''
    end business_stage_name,
  a.estimate_contract_amount,
  a.contract_cycle_int,
  a.company_code,
  a.company_name,
  a.expect_execute_time,
  expect_sign_time,
  a.first_business_sign_time,
  a.business_sign_time,
  a.first_sign_time,
  a.create_time,
  a.status,
  a.is_new_flag,
  a.rn,
  b.estimate_contract_amount as last_estimate_contract_amt,
  case when is_new_flag='新客' then '增量'
     when  cast(a.estimate_contract_amount as decimal(26,0))>=coalesce(cast(b.estimate_contract_amount as decimal(26,0)),0 ) then '增量' 
        else '存量'
    end as is_incr,
    c.is_flag
from 
(
select * from tmp_business_info 
where
    estimate_contract_amount>=1000 
    and business_stage!=5
    and status=1
)a 
left join 
(
select customer_code,
    estimate_contract_amount,
    rn
from tmp_business_info 
where
    rn=2
 ) b on a.customer_code=b.customer_code
 left join 
 (select business_number,
    1 is_flag
 from csx_analyse_tmp.csx_analyse_tmp_bid_info 
    where business_number is not null 
    group by business_number
 )c on a.business_number=c.business_number
 where c.is_flag is null 

;
 
 -- drop table  csx_analyse_tmp.csx_analyse_tmp_bid_business_info
create table csx_analyse_tmp.csx_analyse_tmp_bid_business_info as 

 select   
    bid_no ,
    a.performance_region_code,
    a.performance_region_name,
    a.performance_province_code,
    a.performance_province_name,
    a.performance_city_code,
    a.performance_city_name,
    a.customer_city_code	,
    a.customer_city_name,
    CONCAT_WS(',', COLLECT_SET(CAST(bid_status_name AS STRING))) bid_status_name,
    business_stage_name,
    regexp_replace(bid_name, '\\s+', '') bid_name,
    CONCAT_WS(',', COLLECT_SET(CAST(business_number AS STRING))) group_business_number ,
    regexp_replace(bid_name_alias, '\\s+', '') bid_name_alias,
    regexp_replace(bid_number, '\\s+', '') bid_number, 
    regexp_replace(bid_name_number, '\\s+', '') bid_name_number,
    regexp_replace(bid_customer_name, '\\s+', '') bid_customer_name,
    a.business_attribute_name,
    all_bid_amount,         -- 标的总金额
    all_bid_in_amount,                  -- 投标总金额对应中标金额
    concat(all_win_bid_count,'/',bid_sub_count)  bid_sub_count, -- 可中标分包数/分包数量
    CONCAT_WS(',', COLLECT_SET(if(sub_bid_status not in ('2','4','7'),regexp_replace(sub_bid_name_note, '\\s+', '') ,'') )) sub_bid_name_count,
    all_win_bid_max_amount	,          --最大可中标金额
    all_win_bid_amount,                 -- 最大中标金额
    cast(all_win_bid_count as string) all_win_bid_count,      -- 可中标分包数
    cast(bid_sub_in_count as string)  bid_sub_in_count,       -- 投标分包数
    cast(supply_deadline as string) supply_deadline,
    sum(cast(estimate_contract_amount as decimal(26,2))) as estimate_contract_amount,
    CONCAT_WS(',', COLLECT_SET(CAST(expect_sign_time AS STRING))) expect_sign_time,
    CONCAT_WS(',', COLLECT_SET(CAST(contract_cycle_int AS STRING))) contract_cycle_int,
    a.bid_date,
    CONCAT_WS(',', COLLECT_SET(CAST(expect_execute_time AS STRING))) expect_execute_time,
    bid_company company_code,
    a.company_name,
    is_new_cust_flag ,
    CONCAT_WS(',', COLLECT_SET(CAST(is_incr AS STRING)))  is_incr,
    a.early_work_name,
    if(length(CONCAT_WS(',', COLLECT_SET(CAST(owner_user_name AS STRING))))=0 ,CONCAT_WS(',', COLLECT_SET(CAST(a.sales_user_name AS STRING))),CONCAT_WS(',', COLLECT_SET(CAST(owner_user_name AS STRING)))  ) sales_user_name
  from  csx_analyse_tmp.csx_analyse_tmp_bid_info a
  group by a.bid_no,
    a.performance_region_code,
    a.performance_region_name,
    a.performance_province_code,
    a.performance_province_name,
    a.performance_city_code,
    a.performance_city_name,	
    regexp_replace(bid_name, '\\s+', '') ,
    regexp_replace(bid_name_alias, '\\s+', '') ,
    regexp_replace(bid_number, '\\s+', '') , 
    regexp_replace(bid_name_number, '\\s+', '') ,
    regexp_replace(bid_customer_name, '\\s+', '') ,
    -- a.sales_province,
    -- a.province,
    -- a.sales_city,
    a.project_customer_province,
    business_stage_name,
    a.customer_city_code,
    a.customer_city_name	,
    a.first_category_code,
    a.second_category_code	,
    a.business_attribute,
    a.business_attribute_name,
    concat(all_win_bid_count,'/',bid_sub_count),
    all_win_bid_amount,                 -- 最大中标金额
    all_win_bid_max_amount	,          --最大可中标金额
    all_bid_in_amount,                  -- 投标总金额对应中标金额
    all_bid_amount,         -- 标的总金额
    all_win_bid_count,      -- 可中标分包数
    bid_sub_in_count,       -- 投标分包数
    bid_sub_count,          -- 分包数量,
    a.supply_deadline,
    -- a.enroll_date_end,
    a.bid_date,
    a.bid_company,
    a.company_name,
    a.early_work_name,
    a.sales_user_name,
    is_new_cust_flag,
    a.customer_city_code	,
    a.customer_city_name
union all 
  select
    a.business_number id,
    a.performance_region_code,
    a.performance_region_name,
    a.performance_province_code,
    a.performance_province_name,
    a.performance_city_code,
    a.performance_city_name,
    a.owner_city_code customer_city_code	,
    a.owner_city_name customer_city_name,
    '' bid_status_name,
    business_stage_name,
    ''bid_name,
    business_number group_business_number ,
    '' bid_name_alias,
    '' bid_number, 
    '' bid_name_number,
    customer_name bid_customer_name,
    a.business_attribute_name,
    '' all_bid_amount,         -- 标的总金额
    '' all_bid_in_amount,                  -- 投标总金额对应中标金额
    ''  bid_sub_count, -- 可中标分包数/分包数量
    '' sub_bid_name_count,
    '' all_win_bid_max_amount	,          --最大可中标金额
    '' all_win_bid_amount,                 -- 最大中标金额
    '' all_win_bid_count,      -- 可中标分包数
    '' bid_sub_in_count,       -- 投标分包数
    '' supply_deadline,
    estimate_contract_amount,
    expect_sign_time,
    cast(contract_cycle_int as string)  contract_cycle_int,
    
    ''bid_date,
    expect_execute_time,
    
    a.company_code,
    company_name,
    is_new_flag as is_new_cust_flag,
    is_incr,
    '' early_work_name,
    owner_user_name sales_user_name
  from csx_analyse_tmp.csx_analyse_tmp_business_info a 
  ;
  
  
  select * from csx_analyse_tmp.csx_analyse_tmp_bid_business_info
  
  

create table csx_analyse.csx_analyse_fr_bid_business_info_di(  
level_id string comment '层级id:1日配2福利BBC',
bid_id	string	comment  'id',
bid_no string comment '投标订单号',
performance_region_code	string	comment  '大区编码',
performance_region_name	string	comment  '大区',
performance_province_code	string	comment  '业绩省区编码',
performance_province_name	string	comment  '业绩省区',
performance_city_code	string	comment  '业绩城市',
performance_city_name	string	comment  '业绩城市',
customer_city_code	string	comment  '客户城市',
customer_city_name	string	comment  '客户城市名称',
bid_status_name	string	comment  '投标状态',
business_stage_name	string	comment  '商机阶段',
bid_name	string	comment  '投标名称',
group_business_number	string	comment  '商机号',
bid_name_alias	string	comment  '投标项目别名',
bid_number	string	comment  '投标编码',
bid_name_number	string	comment  '投标名称+编码',
bid_customer_name	string	comment  '客户名称',
business_attribute_name	string	comment  '业务类型',
all_bid_amount	string	comment  '标的总金额',
all_bid_in_amount	string	comment  '投标总金额',
bid_sub_count	string	comment  '可中标分包数/分包数量',
sub_bid_name_count	string	comment  '中标家数',
all_win_bid_max_amount	string	comment  '最大可中金额（标讯）',
all_win_bid_amount	string	comment  '最大中金额',
all_win_bid_count	string	comment  '可中标分包数',
bid_sub_in_count	string	comment  '投标分包数',
supply_deadline	string	comment  '服务期限(月)',
estimate_contract_amount	string	comment  '预计合同签约金额（商机）',
expect_sign_time	string	comment  '预计签约日期',
contract_cycle_int	string	comment  '合同周期（月）',
bid_date	string	comment  '投标日期',
expect_execute_time	string	comment  '预计履约时间',
company_code	string	comment  '公司编码',
company_name	string	comment  '公司名称',
is_new_cust_flag	string	comment  '新/老客',
is_incr	string	comment  '存/增量',
early_work_name	string	comment  '是否销售先行',
sales_user_name	string	comment  '跟进人',
follow_up_progress string comment '跟进进展',
sys_update_time TIMESTAMP comment 'sys数据同步时间',
s_sdt string comment '日期'
) comment '投标-商机信息数据'
partitioned by (sdt string comment '日分区')
STORED AS parquet;


create table csx_analyse.csx_analyse_fr_bid_business_info_fl_di(  
bid_id	string	comment  'id',
bid_no string comment '投标订单号',
performance_region_code	string	comment  '大区编码',
performance_region_name	string	comment  '大区',
performance_province_code	string	comment  '业绩省区编码',
performance_province_name	string	comment  '业绩省区',
performance_city_code	string	comment  '业绩城市',
performance_city_name	string	comment  '业绩城市',
customer_city_code	string	comment  '客户城市',
customer_city_name	string	comment  '客户城市名称',
bid_status_name	string	comment  '投标状态',
business_stage_name	string	comment  '商机阶段',
bid_name	string	comment  '投标名称',
group_business_number	string	comment  '商机号',
bid_name_alias	string	comment  '投标项目别名',
bid_number	string	comment  '投标编码',
bid_name_number	string	comment  '投标名称+编码',
bid_customer_name	string	comment  '客户名称',
business_attribute_name	string	comment  '业务类型',
all_bid_amount	string	comment  '标的总金额',
all_bid_in_amount	string	comment  '投标总金额',
bid_sub_count	string	comment  '可中标分包数/分包数量',
sub_bid_name_count	string	comment  '中标家数',
all_win_bid_max_amount	string	comment  '最大可中金额（标讯）',
all_win_bid_amount	string	comment  '最大中金额',
all_win_bid_count	string	comment  '可中标分包数',
bid_sub_in_count	string	comment  '投标分包数',
supply_deadline	string	comment  '服务期限(月)',
estimate_contract_amount	string	comment  '预计合同签约金额（商机）',
expect_sign_time	string	comment  '预计签约日期',
contract_cycle_int	string	comment  '合同周期（月）',
bid_date	string	comment  '投标日期',
expect_execute_time	string	comment  '预计履约时间',
company_code	string	comment  '公司编码',
company_name	string	comment  '公司名称',
is_new_cust_flag	string	comment  '新/老客',
is_incr	string	comment  '存/增量',
early_work_name	string	comment  '是否销售先行',
sales_user_name	string	comment  '跟进人',
sys_update_time TIMESTAMP comment 'sys数据同步时间',
s_sdt string comment '日期'
) comment '投标-千万以上商机信息数据'
partitioned by (sdt string comment '日分区')
STORED AS parquet;



create table csx_analyse.csx_analyse_csx_crm_prod_sys_dict_df
(
id	int	comment 'ID',
code	string	comment '字典编码',
name	string	comment '字典名称',
extra	string	comment '额外信息',
status	int	comment '状态：0-正常、1-删除',
sort_no	int	comment '排序序号',
parent_code	string	comment'父级编码',
create_time	timestamp	comment'创建时间',
update_time	timestamp	comment'修改时间',
shipper_name	string	comment'租户简称',
shipper_code	string	comment'租户编码'
) comment '投标信息码表'
partitioned by ( sdt	string	comment日期分区{\"FORMAT\":\"yyyymmdd\"})
