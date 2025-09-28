-- ====================================================================================================================================================
-- 4月日配业绩

insert overwrite directory '/tmp/zhangyanpeng/20210521_linshi_5' row format delimited fields terminated by '\t' 

select
	a.customer_no,
	a.sales_value,
	a.profit,
	a.profit_rate,
	b.first_order_date,
	b.last_order_date,
	month_between()
from
	(
	select
		customer_no,
		sum(sales_value) as sales_value,
		sum(profit) as profit,
		sum(profit)/abs(sum(sales_value)) as profit_rate
	from
		(
		select 
			customer_no,sales_value,profit
		from 
			csx_dw.sale_item_m 
		where 
			sdt>='20180101' 
			and sdt<'20190101' 
			and sales_type in('qyg','sapqyg','sapgc','sc','bbc','gc','anhui') 
		union all 
		select 
			customer_no,sales_value,profit
		from 
			csx_dw.dws_sale_r_d_detail
		where 
			sdt>='20190101' 
			and sdt<='20210520'
		) a
	group by 
		customer_no
	) as a 
	left join
		(
		select
			customer_no,first_order_date,last_order_date
		from
			csx_dw.dws_crm_w_a_customer_active
		where
			sdt='current'
		) as b on b.customer_no=a.customer_no




	