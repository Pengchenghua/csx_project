-- 临时表
drop table if exists csx_analyse_tmp.csx_analyse_tmp_dc_customer_list;
create table csx_analyse_tmp.csx_analyse_tmp_dc_customer_list
as
select
	'全国过机直送仓' as cust_flag,
	c.performance_province_name,
	a.inventory_dc_code,
	a.customer_code,
	c.customer_name
from
	(
	select
		customer_code,inventory_dc_code
	from
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt>='20230726' and sdt<='20230825'
		and channel_code in('1','7','9')
		and inventory_dc_code in ('W0AX','W0BD','W0BQ','W0K4','W0T0','W0Z7','WB26','WB38','WB44','WB53','WB54','WB57','WB66','WC02')
	group by 
		customer_code,inventory_dc_code
	) a 
	left join
		(
		select 
			customer_code,customer_name,first_category_code,first_category_name,second_category_code,second_category_name,performance_province_name,performance_city_name
		from 
			csx_dim.csx_dim_crm_customer_info
		where 
			sdt = 'current'
		)c on c.customer_code=a.customer_code
		
union all

select
	'项目供应商仓' as cust_flag,
	c.performance_province_name,
	a.inventory_dc_code,
	a.customer_code,
	c.customer_name
from
	(
	select
		customer_code,inventory_dc_code
	from
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt>='20230726' and sdt<='20230825'
		and channel_code in('1','7','9')
		and inventory_dc_code in ('W0AA','W0BI','W0BN','W0BO','W0G7','W0H6','W0H7','W0H8','W0H9','W0J3','W0P4','W0P7','W0Q0','W0Q7','W0T2','W0Z2','WB05','WB34','WB52','WB60','WB77','WB88')
	group by 
		customer_code,inventory_dc_code
	) a 
	left join
		(
		select 
			customer_code,customer_name,first_category_code,first_category_name,second_category_code,second_category_name,performance_province_name,performance_city_name
		from 
			csx_dim.csx_dim_crm_customer_info
		where 
			sdt = 'current'
		)c on c.customer_code=a.customer_code
;
select * from csx_analyse_tmp.csx_analyse_tmp_dc_customer_list;