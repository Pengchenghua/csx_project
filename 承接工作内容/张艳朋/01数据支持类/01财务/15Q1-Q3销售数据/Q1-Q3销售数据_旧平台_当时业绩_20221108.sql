
insert overwrite directory '/tmp/zhangyanpeng/20221108_01' row format delimited fields terminated by '\t'
	
select
	a.customer_no,
	b.customer_name,
	b.first_category_name,
	b.second_category_name,
	b.third_category_name,
	a.region_name,
	a.province_name,
	a.city_group_name,
	a.logistics_mode_name,
	a.order_channel_name,
	c.quarter_of_year,
	a.channel_name,
	a.business_type_name,
	sum(a.sales_value) as sales_value,
	sum(a.excluding_tax_sales) as excluding_tax_sales,
	sum(a.profit) as profit,
	sum(a.excluding_tax_profit) as excluding_tax_profit
from
	(
	select
		sdt,customer_no,logistics_mode_name,sales_type,region_name,province_name,city_group_name,
		case when sales_type='qyg' then 'B端系统'
			when sales_type='sc' then '商超系统'
			when sales_type='bbc'  then 'BBC小程序'
			when sales_type='fanli'  then '调价返利'
			when sales_type='sapqyg'  then 'SAP销售' 
			when sales_type='sapgc'  then 'SAP工厂端销售到门店' 
			when sales_type='sapwl'  then 'sap调拨单' 
		end as order_channel_name,
		channel_name,business_type_name,	
		sales_value,excluding_tax_sales,profit,excluding_tax_profit
	from
		-- csx_dws.csx_dws_sale_detail_di
		csx_dw.dws_sale_r_d_detail
	where 
		sdt>='20210101' and sdt<='20221031'
		and channel_code in('1','7','9')
	) a 
	left join
		(
		select
			customer_no,customer_name,first_category_name,second_category_name,third_category_name
		from
			-- csx_dim.csx_dim_crm_customer_info
			csx_dw.dws_crm_w_a_customer 
		where
			sdt='current'
		) b on b.customer_no=a.customer_no
	left join
		(
		select
			calday,quarter_of_year
		from
			-- csx_dim.csx_dim_basic_date
			csx_dw.dws_basic_w_a_date
		) c on c.calday=a.sdt
group by 
	a.customer_no,
	b.customer_name,
	b.first_category_name,
	b.second_category_name,
	b.third_category_name,
	a.region_name,
	a.province_name,
	a.city_group_name,
	a.logistics_mode_name,
	a.order_channel_name,
	c.quarter_of_year,
	a.channel_name,
	a.business_type_name
