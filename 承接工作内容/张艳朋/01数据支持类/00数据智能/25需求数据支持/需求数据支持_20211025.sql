-- ================================================================================================================
-- Q3各省区新开客户

insert overwrite directory '/tmp/zhangyanpeng/20211025_linshi_1' row format delimited fields terminated by '\t' 	

select
	b.sales_region_name,
	b.province_name,
	a.customer_no,
	a.customer_name,
	a.first_order_date,
	b.work_no,
	b.sales_name,
	b.first_supervisor_work_no,
	b.first_supervisor_name,
	b.first_category_name,
	b.second_category_name,
	b.third_category_name,
	b.attribute_desc,
	b.sign_date,
	c.sales_value,
	c.profit,
	c.profit_rate
from
	(
	select
		customer_no,customer_name,first_order_date
	from 
		csx_dw.dws_crm_w_a_customer_active
	where 
		sdt = 'current' 
		and first_order_date between '20210701' and '20210930'
	) a 
	join
		( 
		select
			customer_no,customer_name,work_no,sales_name,first_supervisor_work_no,first_supervisor_name,first_category_name,second_category_name,third_category_name,attribute_desc,
			sales_region_name,sales_province_name,province_name,regexp_replace(substr(sign_time,1,10),'-','') as sign_date,estimate_contract_amount
		from
			csx_dw.dws_crm_w_a_customer
		where 
			sdt = 'current'
			and province_name not like '%平台%'
		) b on b.customer_no=a.customer_no
	left join
		(
		select 
			customer_no,
			sum(sales_value) as sales_value,
			sum(profit) as profit,
			sum(profit)/abs(sum(sales_value)) as profit_rate
		from 
			csx_dw.dws_sale_r_d_detail
		where 
			sdt between '20210701' and '20210930'
			and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
		group by 
			customer_no
		) c on c.customer_no=a.customer_no
;


-- ================================================================================================================
-- Q3各省区断约客户

insert overwrite directory '/tmp/zhangyanpeng/20211025_linshi_2' row format delimited fields terminated by '\t' 	

select
	b.sales_region_name,
	b.province_name,
	a.customer_no,
	b.customer_name,
	a.max_sdt,
	b.work_no,
	b.sales_name,
	b.first_supervisor_work_no,
	b.first_supervisor_name,
	b.first_category_name,
	b.second_category_name,
	b.third_category_name,
	b.attribute_desc,
	b.sign_date,
	a.sales_value,
	a.profit,
	a.profit_rate
from
	(
	select 
		customer_no,
		sum(sales_value) as sales_value,
		sum(profit) as profit,
		sum(profit)/abs(sum(sales_value)) as profit_rate,
		max(sdt) as max_sdt
	from 
		csx_dw.dws_sale_r_d_detail
	where 
		sdt between '20210401' and '20210630'
		and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
		and business_type_code in ('1')
	group by 
		customer_no
	) a 
	join
		( 
		select
			customer_no,customer_name,work_no,sales_name,first_supervisor_work_no,first_supervisor_name,first_category_name,second_category_name,third_category_name,attribute_desc,
			sales_region_name,sales_province_name,province_name,regexp_replace(substr(sign_time,1,10),'-','') as sign_date,estimate_contract_amount
		from
			csx_dw.dws_crm_w_a_customer
		where 
			sdt = 'current'
			and province_name not like '%平台%'
		) b on b.customer_no=a.customer_no
	left join
		(
		select 
			customer_no,
			sum(sales_value) as sales_value,
			sum(profit) as profit,
			sum(profit)/abs(sum(sales_value)) as profit_rate
		from 
			csx_dw.dws_sale_r_d_detail
		where 
			sdt between '20210701' and '20210930'
			and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
			and business_type_code in ('1')
		group by 
			customer_no
		) c on c.customer_no=a.customer_no
where
	c.customer_no is null
		
		
