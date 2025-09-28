
--==============================================================================================================================================================================

-- 全国_日配业务_销售额TOP100
select
	b.sales_region_name,
	b.province_name,
	a.customer_no,
	b.customer_name,
	b.work_no,
	b.sales_name,
	b.first_supervisor_work_no,
	b.first_supervisor_name,
	b.first_category_name,
	b.second_category_name,
	b.third_category_name,
	b.attribute_desc,
	a.sales_value,
	a.profit,
	a.profit_rate
from
	(
	select 
		customer_no,
		sum(sales_value) sales_value,
		sum(profit) as profit,
		sum(profit)/abs(sum(sales_value)) as profit_rate,
		row_number() over(order by sum(sales_value) desc) as rn
	from 
		csx_dw.dws_sale_r_d_detail
	where 
		sdt>='20210101' and sdt<='20211025'
		and channel_code in('1','7','9')
		and business_type_code ='1'
	group by 
		customer_no
	) a 
	left join 
		(
		select
			customer_no,customer_name,work_no,sales_name,first_supervisor_work_no,first_supervisor_name,first_category_name,second_category_name,third_category_name,attribute_desc,
			sales_region_name,sales_province_name,province_name,regexp_replace(substr(sign_time,1,10),'-','') as sign_date,estimate_contract_amount
		from
			csx_dw.dws_crm_w_a_customer
		where 
			sdt = 'current'
		)b on a.customer_no=b.customer_no
where
	rn<=100
;

--==============================================================================================================================================================================

-- 全国_日配业务_定价毛利额TOP100
select
	b.sales_region_name,
	b.province_name,
	a.customer_no,
	b.customer_name,
	b.work_no,
	b.sales_name,
	b.first_supervisor_work_no,
	b.first_supervisor_name,
	b.first_category_name,
	b.second_category_name,
	b.third_category_name,
	b.attribute_desc,
	a.sales_value,
	a.profit,
	a.profit_rate
from
	(
	select 
		customer_no,
		sum(sales_value) sales_value,
		sum(profit) as profit,
		sum(profit)/abs(sum(sales_value)) as profit_rate,
		row_number() over(order by sum(profit) desc) as rn
	from 
		csx_dw.dws_sale_r_d_detail
	where 
		sdt>='20210101' and sdt<='20211025'
		and channel_code in('1','7','9')
		and business_type_code ='1'
	group by 
		customer_no
	) a 
	left join 
		(
		select
			customer_no,customer_name,work_no,sales_name,first_supervisor_work_no,first_supervisor_name,first_category_name,second_category_name,third_category_name,attribute_desc,
			sales_region_name,sales_province_name,province_name,regexp_replace(substr(sign_time,1,10),'-','') as sign_date,estimate_contract_amount
		from
			csx_dw.dws_crm_w_a_customer
		where 
			sdt = 'current'
		)b on a.customer_no=b.customer_no
where
	rn<=100
;



--==============================================================================================================================================================================

-- 全国_BBC业务_销售额TOP100
select
	b.sales_region_name,
	b.province_name,
	a.customer_no,
	b.customer_name,
	b.work_no,
	b.sales_name,
	b.first_supervisor_work_no,
	b.first_supervisor_name,
	b.first_category_name,
	b.second_category_name,
	b.third_category_name,
	b.attribute_desc,
	a.sales_value,
	a.profit,
	a.profit_rate
from
	(
	select 
		customer_no,
		sum(sales_value) sales_value,
		sum(profit) as profit,
		sum(profit)/abs(sum(sales_value)) as profit_rate,
		row_number() over(order by sum(sales_value) desc) as rn
	from 
		csx_dw.dws_sale_r_d_detail
	where 
		sdt>='20210101' and sdt<='20211025'
		and channel_code in('1','7','9')
		and business_type_code ='6'
	group by 
		customer_no
	) a 
	left join 
		(
		select
			customer_no,customer_name,work_no,sales_name,first_supervisor_work_no,first_supervisor_name,first_category_name,second_category_name,third_category_name,attribute_desc,
			sales_region_name,sales_province_name,province_name,regexp_replace(substr(sign_time,1,10),'-','') as sign_date,estimate_contract_amount
		from
			csx_dw.dws_crm_w_a_customer
		where 
			sdt = 'current'
		)b on a.customer_no=b.customer_no
where
	rn<=100
;

--==============================================================================================================================================================================

-- 全国_BBC业务_定价毛利额TOP100
select
	b.sales_region_name,
	b.province_name,
	a.customer_no,
	b.customer_name,
	b.work_no,
	b.sales_name,
	b.first_supervisor_work_no,
	b.first_supervisor_name,
	b.first_category_name,
	b.second_category_name,
	b.third_category_name,
	b.attribute_desc,
	a.sales_value,
	a.profit,
	a.profit_rate
from
	(
	select 
		customer_no,
		sum(sales_value) sales_value,
		sum(profit) as profit,
		sum(profit)/abs(sum(sales_value)) as profit_rate,
		row_number() over(order by sum(profit) desc) as rn
	from 
		csx_dw.dws_sale_r_d_detail
	where 
		sdt>='20210101' and sdt<='20211025'
		and channel_code in('1','7','9')
		and business_type_code ='6'
	group by 
		customer_no
	) a 
	left join 
		(
		select
			customer_no,customer_name,work_no,sales_name,first_supervisor_work_no,first_supervisor_name,first_category_name,second_category_name,third_category_name,attribute_desc,
			sales_region_name,sales_province_name,province_name,regexp_replace(substr(sign_time,1,10),'-','') as sign_date,estimate_contract_amount
		from
			csx_dw.dws_crm_w_a_customer
		where 
			sdt = 'current'
		)b on a.customer_no=b.customer_no
where
	rn<=100
;




--==============================================================================================================================================================================

-- 省区_日配业务_销售额TOP50

select 
	* 
from 
	(
	select
		b.sales_region_name,
		b.province_name,
		a.customer_no,
		b.customer_name,
		b.work_no,
		b.sales_name,
		b.first_supervisor_work_no,
		b.first_supervisor_name,
		b.first_category_name,
		b.second_category_name,
		b.third_category_name,
		b.attribute_desc,
		a.sales_value,
		a.profit,
		a.profit_rate,
		row_number() over(partition by b.province_name order by sales_value desc) as rn
	from
		(
		select 
			customer_no,
			sum(sales_value) sales_value,
			sum(profit) as profit,
			sum(profit)/abs(sum(sales_value)) as profit_rate
		from 
			csx_dw.dws_sale_r_d_detail
		where 
			sdt>='20210101' and sdt<='20211025'
			and channel_code in('1','7','9')
			and business_type_code ='1'
		group by 
			customer_no
		) a 
		left join 
			(
			select
				customer_no,customer_name,work_no,sales_name,first_supervisor_work_no,first_supervisor_name,first_category_name,second_category_name,third_category_name,attribute_desc,
				sales_region_name,sales_province_name,province_name,regexp_replace(substr(sign_time,1,10),'-','') as sign_date,estimate_contract_amount
			from
				csx_dw.dws_crm_w_a_customer
			where 
				sdt = 'current'
			)b on a.customer_no=b.customer_no
	) as a 
where
	rn<=50
;


--==============================================================================================================================================================================

-- 省区_日配业务_毛利额TOP50

select 
	* 
from 
	(
	select
		b.sales_region_name,
		b.province_name,
		a.customer_no,
		b.customer_name,
		b.work_no,
		b.sales_name,
		b.first_supervisor_work_no,
		b.first_supervisor_name,
		b.first_category_name,
		b.second_category_name,
		b.third_category_name,
		b.attribute_desc,
		a.sales_value,
		a.profit,
		a.profit_rate,
		row_number() over(partition by b.province_name order by profit desc) as rn
	from
		(
		select 
			customer_no,
			sum(sales_value) sales_value,
			sum(profit) as profit,
			sum(profit)/abs(sum(sales_value)) as profit_rate
		from 
			csx_dw.dws_sale_r_d_detail
		where 
			sdt>='20210101' and sdt<='20211025'
			and channel_code in('1','7','9')
			and business_type_code ='1'
		group by 
			customer_no
		) a 
		left join 
			(
			select
				customer_no,customer_name,work_no,sales_name,first_supervisor_work_no,first_supervisor_name,first_category_name,second_category_name,third_category_name,attribute_desc,
				sales_region_name,sales_province_name,province_name,regexp_replace(substr(sign_time,1,10),'-','') as sign_date,estimate_contract_amount
			from
				csx_dw.dws_crm_w_a_customer
			where 
				sdt = 'current'
			)b on a.customer_no=b.customer_no
	) as a 
where
	rn<=50
;