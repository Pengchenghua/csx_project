-- --------------------------
-- 近90天已断约客户数据
drop table if exists csx_analyse_tmp.csx_analyse_tmp_break_cus_ky; 
create table if not exists csx_analyse_tmp.csx_analyse_tmp_break_cus_ky as 
select 
	a.performance_region_name,
	a.performance_province_name,
	a.performance_city_name,
	a.customer_code,
	b.customer_name,
	a.business_attribute_name,

	b.first_category_name,
	b.second_category_name,
	b.third_category_name,

	(case when a.status=0 then '待发起' 
    	  when a.status=1 then '审批中' 
    	  when a.status=2 then '已断约' 
    	  when a.status=3 then '已拒绝' 
    	  when a.status=4 then '已取消' end) as status,
	a.terminate_time,
	a.reason,

	c.sales_user_number,
    c.sales_user_name,
    b.customer_address_full,
    b.contact_person,
    b.contact_phone,

    d.last_sdt,
    d.sale_amt,
    d.profitlv  
from 
	(select 
		a1.* 
	from 
		(select 
			*,
			row_number()over(partition by customer_code,business_attribute_name order by terminate_time) as pm 
		from csx_dim.csx_dim_crm_terminate_customer 
		where sdt='current' 
		and shipper_code='YHCSX' 
		and performance_region_name in ('华东大区') 
		and status not in (3,4) 
		and to_date(terminate_time)>=date_add('${yes_date}',-90) 
		) a1 
	where a1.pm=1 
	) a 
	left join 
	(select 
		* 
	from csx_dim.csx_dim_crm_customer_info 
	where sdt='current' 
	) b 
	on a.customer_code=b.customer_code 
	left join 
	(select 
		c1.* 
	from 
		(select 
		    *,
		    row_number()over(partition by customer_code order by sdt desc) as pm 
		from csx_dim.csx_dim_crm_customer_info 
		where sales_user_name is not null 
		and length(sales_user_name)>0 
		) c1 
	where c1.pm=1 
	) c 
	on a.customer_code=c.customer_code 
	left join 
	(select 
		customer_code,
		sum(sale_amt)/10000 as sale_amt,
		sum(profit)/abs(sum(sale_amt)) as profitlv,
		max(case when order_channel_code not in (4,5,6) and refund_order_flag<>1 then sdt end) as last_sdt 
	from csx_dws.csx_dws_sale_detail_di 
	where sdt>='20190101' 
	group by 
		customer_code
	) d 
	on a.customer_code=d.customer_code 
;



-- --------------------------
-- 未来60天预断约客户数据
drop table if exists csx_analyse_tmp.csx_analyse_tmp_for_break_cus_ky; 
create table if not exists csx_analyse_tmp.csx_analyse_tmp_for_break_cus_ky as 
select 
	c.performance_region_name,
	c.performance_province_name,
	c.performance_city_name,
	a.customer_code,
	c.customer_name,
	c.sales_user_number,
	c.sales_user_name,
	c.first_category_name,
	c.second_category_name,
	c.third_category_name,
	a.business_attribute_name,
	a.contract_number,
	a.contract_type,-- 正式合同or临时合同
	a.contract_cycle,-- 合同周期
	a.estimate_contract_amount,-- 合同签约金额
	a.contract_begin_date,
	a.contract_end_date,
	b.fixed_begin_date,
	b.fixed_end_date,
	c.cooperation_mode_name,-- 合作模式
	c.dev_source_name,-- 开发来源
	c.customer_address_full -- 客户详细地址
from 
	(
	select 
		*
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
			contract_cycle,-- 合同周期
			(case when contract_type=1 then '临时合同' 
				  when contract_type=2 then '正式合同' 
			end) as contract_type,  -- 合同类型(1临时合同 2正式合同)
			contract_must,  -- 是否需签订合同 0否 1是
			to_date(contract_begin_date) as contract_begin_date,  -- 合同起始日期
			to_date(contract_end_date) as contract_end_date,  -- 合同终止日期
			estimate_contract_amount,
			regexp_replace(to_date(contract_begin_date),'-','') as contract_begin_sdt,
			row_number() over(partition by customer_code,business_attribute_code order by contract_end_date desc)	as num
		from csx_dim.csx_dim_crm_business_info
		where sdt='current' 
		    and business_stage = 5 
			and status='1'
			and shipper_code='YHCSX'
		)a
	where a.num=1
	) a 
	-- 客户信控额度明细
	left join  
	(
	  select *
	  from 
			(
			  select *,
			  to_date(fixed_begin_time) as fixed_begin_date,
			  to_date(fixed_end_time) as fixed_end_date,
			  concat(to_date(fixed_begin_time),'~',to_date(fixed_end_time)) as fixed_begin_end, -- 固定额度起止时间 
			  concat(to_date(temp_begin_time),'~',to_date(temp_end_time)) as temp_begin_end, -- 临时额度起止时间
			  row_number() over(partition by customer_code,business_attribute_code order by if(fixed_end_time>temp_end_time,fixed_end_time,temp_end_time) desc)	as num 
			  from csx_dim.csx_dim_crm_customer_company_details
			  where sdt='current'
			  and shipper_code='YHCSX'
			  and status=1  -- 状态 0.无效 1.有效
			  and business_attribute_code=1
			  and (credit_limit>0 or temp_credit_limit>0)
			)a 
		where num=1
	) b  
	on a.customer_code=b.customer_code and a.business_attribute_code=b.business_attribute_code 
	left join 
	(select 
		* 
	from csx_dim.csx_dim_crm_customer_info 
	where sdt='current' 
	and customer_type_code=4 
	) c 
	on a.customer_code=c.customer_code 
where c.customer_code is not null 
-- 近2个月断约客户
and 
(
	(a.contract_type='正式合同' and a.contract_end_date>=date_add('${yes_date}',1) and a.contract_end_date<=date_add('${yes_date}',60))
	or 
	(a.contract_type='临时合同' and b.fixed_end_date>=date_add('${yes_date}',1) and b.fixed_end_date<=date_add('${yes_date}',60))
) 
and c.performance_region_name='华东大区'
;


insert overwrite table csx_analyse.csx_analyse_hd_break_cus_wf 
select 
	'已断约' as type,
	performance_region_name,
	performance_province_name,
	performance_city_name,
	customer_code,
	customer_name,
	sales_user_number,
    sales_user_name,
	business_attribute_name,

	first_category_name,
	second_category_name,
	third_category_name,

	status,
	terminate_time,
	reason,
	
    
    contact_person,
    contact_phone,

    last_sdt,
    sale_amt,
    profitlv,

    '' as contract_number,
	'' as contract_type,-- 正式合同or临时合同
	'' as contract_cycle,-- 合同周期
	0 as estimate_contract_amount,-- 合同签约金额
	'' as contract_begin_date,
	'' as contract_end_date,
	'' as fixed_begin_date,
	'' as fixed_end_date,
	'' as cooperation_mode_name,-- 合作模式
	'' as dev_source_name,-- 开发来源
      
    customer_address_full   
from csx_analyse_tmp.csx_analyse_tmp_break_cus_ky 
union all 
select 
	'预断约' as type,
	performance_region_name,
	performance_province_name,
	performance_city_name,
	customer_code,
	customer_name,
	sales_user_number,
	sales_user_name,
	business_attribute_name,

	first_category_name,
	second_category_name,
	third_category_name,

	'' as status,
	'' as terminate_time,
	'' as reason,
	   
    '' as contact_person,
    '' as contact_phone,

    '' as last_sdt,
    0 as sale_amt,
    0 as profitlv,
	
	contract_number,
	contract_type,-- 正式合同or临时合同
	contract_cycle,-- 合同周期
	cast(estimate_contract_amount as decimal(20,6)) as estimate_contract_amount,-- 合同签约金额
	contract_begin_date,
	contract_end_date,
	fixed_begin_date,
	fixed_end_date,
	cooperation_mode_name,-- 合作模式
	dev_source_name,-- 开发来源

	customer_address_full -- 客户详细地址 
from csx_analyse_tmp.csx_analyse_tmp_for_break_cus_ky 



select 
	performance_region_name as `大区`,
	performance_province_name as `省区`,
	performance_city_name as `城市`,
	customer_code as `客户编码`,
	customer_name as `客户名称`,
	business_attribute_name as `商机属性`,

	first_category_name as `一级客户分类`,
	second_category_name as `二级客户分类`,
	third_category_name as `三级客户分类`,
	sales_user_number as `销售工号`,
    sales_user_name as `销售姓名`,
    
    contract_number as `合同号`,
	contract_type as `合同类型`,-- 正式合同or临时合同
	contract_cycle as `合同周期`,-- 合同周期
	estimate_contract_amount as `合同签约金额（万）`,-- 合同签约金额
	contract_begin_date as `合同起始日期`,
	contract_end_date as `合同结束日期`,
	fixed_begin_date as `固定信控起始日期`,
	fixed_end_date as `固定信控结束日期`,
	cooperation_mode_name as `合作模式`,-- 合作模式
	dev_source_name as `开发来源`,-- 开发来源 
	regexp_replace(customer_address_full, '\n|\t|\r|\,|\"|\\\\n|\\s', '') as `客户详细地址` 
from csx_analyse.csx_analyse_hd_break_cus_wf 
where  type='预断约' 
order by performance_region_name,performance_province_name,performance_city_name,terminate_time