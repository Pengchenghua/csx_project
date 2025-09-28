select 
	b.cus_type,
	a.q_type,
	a.business_type_name,
	sum(sales_value) as sales_value,
	count(distinct a.customer_no) as customer_cnt
from 
	(
	select 
		customer_no,
		business_type_name,
		case when sdt between '20210101' and '20210331' then 'Q1'
			when sdt between '20210401' and '20210630' then 'Q2'
			when sdt between '20210701' and '20210930' then 'Q3'
		else '其他' end as q_type,
		sales_value,
		profit
	from 
		csx_dw.dws_sale_r_d_detail
	where 
		sdt>='20210101' 
		and sdt<='20210930'
		and (order_no not in ('OC200529000043','OC200529000044','OC200529000045','OC200529000046',
			  'OC20111000000021','OC20111000000022','OC20111000000023','OC20111000000024','OC20111000000025') or order_no is null)
		and channel_code in('1','7','9')
		and ((business_type_code='1' and dc_code not in('W0K4')) or business_type_code in ('2','6'))  --剔除日配业绩中的WOK4
	)a 
	join
		(
		select 
			customer_no,first_order_date,
			case when first_order_date<='20191231' then '19年及以前的老客'
				when first_order_date>='20200101' and first_order_date<='20201231' then '20年新客'
				when first_order_date>='20210101' and first_order_date<='20210930' then '21年新客'
			else '其他' end as cus_type
		from 
			csx_dw.dws_crm_w_a_customer_active
		where 
			sdt='current'
			and first_order_date <= '20210930'
		)b on a.customer_no=b.customer_no 
group by 
	b.cus_type,
	a.q_type,a.business_type_name
;