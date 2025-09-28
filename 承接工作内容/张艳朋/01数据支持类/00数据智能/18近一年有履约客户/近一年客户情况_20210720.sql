--============================================================================================================================================================
-- 近一年客户情况

insert overwrite directory '/tmp/zhangyanpeng/20210720_linshi_1' row format delimited fields terminated by '\t' 

select
	case when datediff(date_sub(current_date,1),to_date(from_unixtime(unix_timestamp(c.normal_last_order_date,'yyyyMMdd'))))<=30 then '活跃'
		when datediff(date_sub(current_date,1),to_date(from_unixtime(unix_timestamp(c.normal_last_order_date,'yyyyMMdd'))))<=60 then '沉默'
		when datediff(date_sub(current_date,1),to_date(from_unixtime(unix_timestamp(c.normal_last_order_date,'yyyyMMdd'))))<=90 then '预流失'
		when datediff(date_sub(current_date,1),to_date(from_unixtime(unix_timestamp(c.normal_last_order_date,'yyyyMMdd'))))>90 then '流失'
		else '其他'
	end as customer_type,
	b.dev_source_name,
	b.business_type_name,
	b.sales_province_name,
	b.city_group_name,
	a.customer_no,
	b.customer_name,
	b.attribute_desc,
	a.business_type_name,
	b.first_category_name,
	b.second_category_name,
	b.third_category_name,
	b.work_no,
	b.sales_name,
	b.first_supervisor_name,
	b.second_supervisor_name,
	b.sign_date,
	b.estimate_contract_amount,
	c.normal_first_order_date,
	c.normal_last_order_date,
	count(distinct sdt)/count(distinct s_month) as avg_cnt,
	count(distinct order_no)/count(distinct s_month) as avg_cnt2,
	sum(sales_value)/count(distinct s_month) as avg_sales,
	sum(sales_value) as sales_value,
	--sum(profit) as profit,
	sum(profit)/abs(sum(sales_value)) as profit_rate
from
	(
	select
		customer_no,sdt,substr(sdt,1,6) as s_month,business_type_name,order_no,
		sales_value,profit
	from 
		csx_dw.dws_sale_r_d_detail
	where 
		sdt >= '20200720' 
		and sdt <= '20210719'
		and channel_code in ('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
	) as a
	left join 
		(
		select 
			customer_no,customer_name,work_no,sales_name,first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name,
			dev_source_name,business_type_name,sales_province_name,city_group_name,attribute_desc,first_supervisor_name,second_supervisor_name,estimate_contract_amount*10000 as estimate_contract_amount,
			--regexp_replace(split_part(sign_time,' ',1),'-','') as sign_date
			regexp_replace(split(sign_time,' ')[0],'-','') as sign_date
		from 
			csx_dw.dws_crm_w_a_customer
		where 
			sdt='current'
		) b on a.customer_no=b.customer_no
	left join
		(
		select
			customer_no,normal_first_order_date,normal_last_order_date
		from
			csx_dw.dws_crm_w_a_customer_active
		where
			sdt='current'
		) c on c.customer_no=a.customer_no
group by 
	case when datediff(date_sub(current_date,1),to_date(from_unixtime(unix_timestamp(c.normal_last_order_date,'yyyyMMdd'))))<=30 then '活跃'
		when datediff(date_sub(current_date,1),to_date(from_unixtime(unix_timestamp(c.normal_last_order_date,'yyyyMMdd'))))<=60 then '沉默'
		when datediff(date_sub(current_date,1),to_date(from_unixtime(unix_timestamp(c.normal_last_order_date,'yyyyMMdd'))))<=90 then '预流失'
		when datediff(date_sub(current_date,1),to_date(from_unixtime(unix_timestamp(c.normal_last_order_date,'yyyyMMdd'))))>90 then '流失'
		else '其他'
	end,
	b.dev_source_name,
	b.business_type_name,
	b.sales_province_name,
	b.city_group_name,
	a.customer_no,
	b.customer_name,
	b.attribute_desc,
	a.business_type_name,
	b.first_category_name,
	b.second_category_name,
	b.third_category_name,
	b.work_no,
	b.sales_name,
	b.first_supervisor_name,
	b.second_supervisor_name,
	b.sign_date,
	b.estimate_contract_amount,
	c.normal_first_order_date,
	c.normal_last_order_date
		