drop table if exists csx_analyse_tmp.csx_analyse_tmp_final_table_ky; 
create table if not exists csx_analyse_tmp.csx_analyse_tmp_final_table_ky as 
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
		and to_date(terminate_time)>='2023-01-01'
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
    regexp_replace(customer_address_full, '\n|\t|\r|\,|\"|\\\\n|\\s', '') as `客户详细地址`,
    contact_person as `客户对接人`,
    contact_phone as `对接人联系电话`,

	status as `断约状态`,
	terminate_time as `断约时间`,
	reason as `断约原因`,

    last_sdt as `最后签收日期`,
    sale_amt as `销售额（万）`,
    profitlv as `毛利率` 
from csx_analyse_tmp.csx_analyse_tmp_final_table_ky 