insert overwrite directory '/tmp/zhangyanpeng/20210623_customer_sale' row format delimited fields terminated by '\t' 
select 
	'20210101~20210622' as period,
	a.province_name,
	a.city_group_name,
	a.dc_code, 
	a.customer_no,
	coalesce(b.customer_name,c.shop_name) as c_name,
	a.business_type_name,
	sum(sales_value) sales_value,
	sum(profit) profit,
	sum(front_profit) front_profit
from 
	csx_dw.dws_sale_r_d_detail a
	left join 
		(
		select
			customer_no,customer_name 
		from  
			csx_dw.dws_crm_w_a_customer 
		where 
			sdt='current'
		) b on a.customer_no=b.customer_no
	left join 
		(
		select
			shop_id,shop_name 
		from  
			csx_dw.dws_basic_w_a_csx_shop_m
		where 
			sdt='current'
		) c on a.customer_no=concat('S',c.shop_id)	
where 
	a.sdt >='20210101' and a.sdt <= '20210622'
	-- and a.channel_code in ('1','7','9')
group by
	a.province_name,
	a.city_group_name,
	a.dc_code, 
	a.customer_no,
	coalesce(b.customer_name,c.shop_name),
	a.business_type_name