--================================================================================================
	
select
	a.sales_region_name,
	a.sales_province_name,
	a.business_type_name,
	count(distinct a.customer_no) as cnt1,
	sum(b.sales_value)as sales_value
from
	(
	select 
		customer_no,sales_region_name,sales_province_name,city_group_name,business_type_name
	from 
		csx_dw.dws_crm_w_a_customer
	where 
		sdt='20211007'
		and customer_no !=''
		and channel_code in ('1','7','9')
	) a 
	left join
		(
		select
			customer_no,
			sum(sales_value) as sales_value
		from 
			csx_dw.dws_sale_r_d_detail
		where 
			sdt between '20210701' and '20210930'
			and channel_code in ('1','7','9') 
		group by
			customer_no
		) b on b.customer_no=a.customer_no
	join
		(
		select
			customer_no,customer_name,first_order_date
		from 
			csx_dw.dws_crm_w_a_customer_active
		where 
			sdt = '20211007' 
			and first_order_date between '20210701' and '20210930'
		) c on c.customer_no=a.customer_no
group by 
	a.sales_region_name,a.sales_province_name,a.business_type_name
;

--=======================================================================================================================================================================================

select
	a.sales_region_name,
	a.sales_province_name,
	a.business_type_name,
	count(distinct a.customer_no) as cnt1,
	sum(b.sales_value)as sales_value
from
	(
	select 
		customer_no,sales_region_name,sales_province_name,city_group_name,business_type_name
	from 
		csx_dw.dws_crm_w_a_customer
	where 
		sdt='20211007'
		and customer_no !=''
		and channel_code in ('1','7','9')
	) a 
	left join
		(
		select
			customer_no,
			sum(sales_value) as sales_value
		from 
			csx_dw.dws_sale_r_d_detail
		where 
			sdt between '20210401' and '20210630'
			and channel_code in ('1','7','9') 
		group by
			customer_no
		) b on b.customer_no=a.customer_no
	join
		(
		select
			customer_no,customer_name,first_order_date
		from 
			csx_dw.dws_crm_w_a_customer_active
		where 
			sdt = '20211007' 
			and first_order_date between '20210401' and '20210630'
		) c on c.customer_no=a.customer_no
group by 
	a.sales_region_name,a.sales_province_name,a.business_type_name
;

--=======================================================================================================================================================================================

select
	a.sales_region_name,
	a.sales_province_name,
	a.business_type_name,
	a.b_type,
	count(distinct a.customer_no) as cnt1,
	sum(b.sales_value)as sales_value
from
	(
	select 
		customer_no,sales_region_name,sales_province_name,city_group_name,business_type_name,
		case when category_name='日配' then '日配业务'
			when category_name='福利' then '福利业务'
			when category_name='BBC' then '福利业务'
			else category_name
		end as b_type
	from 
		csx_dw.dws_crm_w_a_customer lateral view explode(split(attribute_desc,',')) table_tmp as category_name
	where 
		sdt='20211007'
		and customer_no !=''
		and channel_code in ('1','7','9')
	) a 
	left join
		(
		select
			customer_no,
			case when business_type_name='城市服务商' then '日配业务' 
				when business_type_name='BBC' then '福利业务' 
			else business_type_name end as b_type_n,
			sum(sales_value) as sales_value
		from 
			csx_dw.dws_sale_r_d_detail
		where 
			sdt between '20210701' and '20210930'
			and channel_code in ('1','7','9') 
			and business_type_code in ('1','2','4','6') --1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超
		group by
			customer_no,
			case when business_type_name='城市服务商' then '日配业务' 
				when business_type_name='BBC' then '福利业务' 
			else business_type_name end
		) b on b.customer_no=a.customer_no and b.b_type_n=a.b_type
	join
		(
		select
			customer_no,customer_name,first_order_date
		from 
			csx_dw.dws_crm_w_a_customer_active
		where 
			sdt = '20211007' 
			and first_order_date between '20210701' and '20210930'
		) c on c.customer_no=a.customer_no
where
	a.b_type in ('日配业务','福利业务')
group by 
	a.sales_region_name,a.sales_province_name,a.business_type_name,a.b_type
;