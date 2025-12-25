-- 1-6月客户履约与合同情况 20250722
select 
	a.performance_region_name,
	a.performance_province_name,
	a.performance_city_name,
	b.dev_source_name,
	a.customer_code,
	b.customer_name,
	b.second_category_name,     --  二级客户分类名称
	d.first_business_sale_date,
	max(a.require_delivery_date) as last_business_sale_date,
	-- d.last_business_sale_date,
    c.contract_number as contract_number_z,  -- 合同编号
    -- c.contract_type,  -- 合同类型(1临时合同 2正式合同)
	-- case 
	-- when c.contract_type=1 then '临时合同'
	-- when c.contract_type=2 then '正式合同' 
	-- end as contract_type,
    -- c.contract_must,  -- 是否需签订合同 0否 1是
    c.contract_begin_date as contract_begin_date_z,  -- 合同起始日期
    c.contract_end_date as contract_end_date_z,  -- 合同终止日期
	
    c2.contract_number as contract_number_l,  -- 合同编号
    c2.contract_begin_date as contract_begin_date_l,  -- 合同起始日期
    c2.contract_end_date as contract_end_date_l,  -- 合同终止日期

	f.fixed_begin_end, -- 固定额度起止时间 
	f.temp_begin_end, -- 临时额度起止时间	
	a.business_type_name,
	-- 断约状态 0.待发起 1.审批中 2.已断约 3.已拒绝 4.已取消
	-- case e.status
	-- when 0 then '待发起'
	-- when 1 then '审批中'
	-- when 2 then '已断约'
	-- when 3 then '已拒绝' end as status,
	
	e.work_no,  --  `销售员工号`,
	e.sales_name,  --  `销售员`,
	e.rp_service_user_work_no,  -- `日配_服务管家工号`,
	e.rp_service_user_name,  -- `日配_服务管家`,	
	sum(case when a.smonth between '202501' and '202506' then a.sale_amt end)/10000 as sale_amt,
	sum(case when a.smonth between '202501' and '202506' then a.profit end)/10000 as profit,
	sum(case when a.smonth between '202501' and '202506' then a.profit end) /abs(sum(case when a.smonth between '202501' and '202506' then a.sale_amt end)) as profit_rate,
	
	sum(case when a.smonth='202501' then a.sale_amt end)/10000 as sale_amt_1,
	sum(case when a.smonth='202501' then a.profit end)/abs(sum(case when a.smonth='202501' then a.sale_amt end)) as profit_rate_1,	
	
	sum(case when a.smonth='202502' then a.sale_amt end)/10000 as sale_amt_2,
	sum(case when a.smonth='202502' then a.profit end)/abs(sum(case when a.smonth='202502' then a.sale_amt end)) as profit_rate_2,		
	sum(case when a.smonth='202503' then a.sale_amt end)/10000 as sale_amt_3,
	sum(case when a.smonth='202503' then a.profit end)/abs(sum(case when a.smonth='202503' then a.sale_amt end)) as profit_rate_3,	
	sum(case when a.smonth='202504' then a.sale_amt end)/10000 as sale_amt_4,
	sum(case when a.smonth='202504' then a.profit end)/abs(sum(case when a.smonth='202504' then a.sale_amt end)) as profit_rate_4,	
	sum(case when a.smonth='202505' then a.sale_amt end)/10000 as sale_amt_5,
	sum(case when a.smonth='202505' then a.profit end)/abs(sum(case when a.smonth='202506' then a.sale_amt end)) as profit_rate_5,	
	sum(case when a.smonth='202506' then a.sale_amt end)/10000 as sale_amt_6,
	sum(case when a.smonth='202506' then a.profit end)/abs(sum(case when a.smonth='202506' then a.sale_amt end)) as profit_rate_6	
from 
	(select *,substr(require_delivery_date,1,6) as smonth
	from csx_dws.csx_dws_sale_detail_di  
	where sdt>='20240101'
	and require_delivery_date>='20250101'
	-- and require_delivery_date<='20250630'
	-- and performance_province_name not like '平台%'
	and business_type_code in(1)
	and shipper_code='YHCSX'	
	) a 
-- 	-- 直送类型 详细履约模式的码表
-- 	left join 
-- 	(
-- 	select `code`,name,extra
-- 	from csx_dim.csx_dim_basic_topic_dict_df
-- 	where parent_code = 'direct_delivery_type'
-- 	)a2 on cast(a.direct_delivery_type as string)=a2.`code`
join 
(
select dev_source_name,
	performance_region_name,     --  销售大区名称(业绩划分)
	performance_province_name,     --  销售归属省区名称
	performance_city_name,     --  城市组名称(业绩划分)
	-- channel_code,
	channel_name,
	-- bloc_code,     --  集团编码
	bloc_name,     --  集团名称
	-- customer_id,
	customer_code,
	customer_name,     --  客户名称
	-- first_category_code,     --  一级客户分类编码
	first_category_name,     --  一级客户分类名称
	-- second_category_code,     --  二级客户分类编码
	second_category_name,     --  二级客户分类名称
	-- third_category_code,     --  三级客户分类编码
	third_category_name     --  三级客户分类名称
from csx_dim.csx_dim_crm_customer_info
where sdt='current'
and shipper_code='YHCSX'
-- and customer_type_code=4
-- and (second_category_name='部队'
-- or (second_category_name in('教育','医疗卫生') and (customer_name like'%部队%院%' or customer_name like'%部队%学%'))
-- or (second_category_name in('教育','医疗卫生') and (customer_name like'%军%院%' or customer_name like'%军%学%'))
-- )
)b on a.customer_code=b.customer_code
-- 最近一个日配商机-正式合同
left join
(
select *
from 
	(
select 
	substr(business_sign_time,1,7) month, 
    customer_code,
	customer_name,
	business_number,
	performance_region_code,
	performance_region_name,
	performance_province_code,
	performance_province_name,
	performance_city_code,
	performance_city_name,
	first_category_code,
	first_category_name,
	second_category_code,
	second_category_name,
	third_category_code,
	third_category_name,
	business_attribute_code,
	business_attribute_name,
	estimate_contract_amount,
	to_date(first_sign_time) first_sign_date,
	case when substr(first_sign_time,1,7) = substr(business_sign_time,1,7) then '新签约客户' else '老签约客户' end as new_or_old_customer_mark,
	regexp_replace(to_date(business_sign_time),'-','') business_sign_date,
	to_date(first_business_sign_time) first_business_sign_date,
	contract_number,  -- 合同编号
	contract_type,  -- 合同类型(1临时合同 2正式合同)
	contract_must,  -- 是否需签订合同 0否 1是
	contract_begin_date,  -- 合同起始日期
	contract_end_date,  -- 合同终止日期
	regexp_replace(to_date(contract_begin_date),'-','') as contract_begin_sdt,
	row_number() over(partition by customer_code order by contract_end_date desc)	as num
from csx_dim.csx_dim_crm_business_info
where sdt='current' 
    and business_stage = 5 
	and status='1'
    and business_attribute_code in ('1')
	and shipper_code='YHCSX'
	and contract_type=2 -- 正式合同
	)a
where a.num=1
)c on a.customer_code=c.customer_code
-- 最近一个日配商机-临时合同
left join
(
select *
from 
	(
select 
	substr(business_sign_time,1,7) month, 
    customer_code,
	customer_name,
	business_number,
	performance_region_code,
	performance_region_name,
	performance_province_code,
	performance_province_name,
	performance_city_code,
	performance_city_name,
	first_category_code,
	first_category_name,
	second_category_code,
	second_category_name,
	third_category_code,
	third_category_name,
	business_attribute_code,
	business_attribute_name,
	estimate_contract_amount,
	to_date(first_sign_time) first_sign_date,
	case when substr(first_sign_time,1,7) = substr(business_sign_time,1,7) then '新签约客户' else '老签约客户' end as new_or_old_customer_mark,
	regexp_replace(to_date(business_sign_time),'-','') business_sign_date,
	to_date(first_business_sign_time) first_business_sign_date,
	contract_number,  -- 合同编号
	contract_type,  -- 合同类型(1临时合同 2正式合同)
	contract_must,  -- 是否需签订合同 0否 1是
	contract_begin_date,  -- 合同起始日期
	contract_end_date,  -- 合同终止日期
	regexp_replace(to_date(contract_begin_date),'-','') as contract_begin_sdt,
	row_number() over(partition by customer_code order by contract_end_date desc)	as num
from csx_dim.csx_dim_crm_business_info
where sdt='current' 
    and business_stage = 5 
	and status='1'
    and business_attribute_code in ('1')
	and shipper_code='YHCSX'
	and contract_type=1 -- 临时合同
	)a
where a.num=1
)c2 on a.customer_code=c2.customer_code
left join  
(
select customer_code,business_type_code,
	first_business_sale_date,last_business_sale_date
from csx_dws.csx_dws_crm_customer_business_active_di
where sdt = 'current'
-- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.bbc 7.大宗一部 8.大宗二部 9.商超)
and business_type_code in('1')
)d on a.customer_code=d.customer_code
-- 客户信控额度明细
left join  
(
  select *
  from 
  (
  select *,
  concat(to_date(fixed_begin_time),'~',to_date(fixed_end_time)) as fixed_begin_end, -- 固定额度起止时间 
  concat(to_date(temp_begin_time),'~',to_date(temp_end_time)) as temp_begin_end, -- 临时额度起止时间
  row_number() over(partition by customer_code order by if(fixed_end_time>temp_end_time,fixed_end_time,temp_end_time) desc)	as num 
  from csx_dim.csx_dim_crm_customer_company_details
  where sdt='current'
  and shipper_code='YHCSX'
  and status=1  -- 状态 0.无效 1.有效
  and business_attribute_code=1
  and (credit_limit>0 or temp_credit_limit>0)
  )a where num=1
)f on a.customer_code=f.customer_code
-- 最近下单日期
-- left join
-- (
--   select
--     customer_code,
--     max(customer_name) as customer_name,
--     max(require_delivery_date) as require_delivery_date
--   from csx_dwd.csx_dwd_csms_yszx_order_detail_di
--   where sdt >= '20250101'
--     AND order_status not in ('CANCELLED', 'CREATED', 'PAID', 'CONFIRMED')
--     and shipper_code = 'YHCSX'
-- 	and order_business_type='NORMAL'
-- 	and substr(order_code,1,1)<>'R'
-- group by customer_code
-- )g on a.customer_code=g.customer_code
-- 关联销售和管家
left join 
(
select *
from csx_analyse.csx_analyse_report_crm_customer_sale_service_manager_info_df
where sdt=regexp_replace(date_sub(current_date,1),'-','')
)e on a.customer_code=e.customer_no
-- 是否断约
-- left join 
-- (
-- select customer_code,max(status) as status  
-- from csx_dim.csx_dim_crm_terminate_customer 
-- where sdt='current' 
-- and is_valid=1 
-- and business_attribute_code like '%1%' 
-- and status not in (4)
-- group by customer_code
-- )e on a.customer_code=e.customer_code
group by 
	a.performance_region_name,
	a.performance_province_name,
	a.performance_city_name,
	b.dev_source_name,
	a.customer_code,
	b.customer_name,
	d.first_business_sale_date,
	d.last_business_sale_date,
	b.second_category_name,     --  二级客户分类名称
    c.contract_number,  -- 合同编号
    c.contract_begin_date,  -- 合同起始日期
    c.contract_end_date,  -- 合同终止日期
    c2.contract_number,  -- 合同编号
    c2.contract_begin_date,  -- 合同起始日期
    c2.contract_end_date,  -- 合同终止日期
	f.fixed_begin_end, -- 固定额度起止时间 
	f.temp_begin_end, -- 临时额度起止时间	
	a.business_type_name,
	e.work_no,  --  `销售员工号`,
	e.sales_name,  --  `销售员`,
	e.rp_service_user_work_no,  -- `日配_服务管家工号`,
	e.rp_service_user_name	
;