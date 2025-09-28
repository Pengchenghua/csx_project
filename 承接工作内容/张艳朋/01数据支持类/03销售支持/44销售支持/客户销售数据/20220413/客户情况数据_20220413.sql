--=================================================================================================================================================================================
-- 客户销售数据

set current_day ='20220412';

insert overwrite directory '/tmp/zhangyanpeng/20220413_linshi_1' row format delimited fields terminated by '\t' 

select
	case when datediff(date_sub(current_date,1),to_date(from_unixtime(unix_timestamp(a.max_sdt,'yyyyMMdd'))))<=30 then '活跃'
		when datediff(date_sub(current_date,1),to_date(from_unixtime(unix_timestamp(a.max_sdt,'yyyyMMdd'))))<=60 then '沉默'
		when datediff(date_sub(current_date,1),to_date(from_unixtime(unix_timestamp(a.max_sdt,'yyyyMMdd'))))<=90 then '预流失'
		when datediff(date_sub(current_date,1),to_date(from_unixtime(unix_timestamp(a.max_sdt,'yyyyMMdd'))))>90 then '流失'
		else '其他'
	end as customer_type,
	b.channel_name,
	a.business_type_name,
	b.sales_province_name,
	b.city_group_name,
	a.customer_no,
	b.customer_name,
	b.contact_person,
	--b.contact_phone,
	coalesce(regexp_replace(b.contact_phone,'\n|\t|\r|\,|\"|\\\\n',''),'') as contact_phone,
	--b.attribute_desc,
	coalesce(regexp_replace(b.attribute_desc,'\n|\t|\r|\,|\"|\\\\n',''),'') as attribute_desc,
	b.first_category_name,
	b.second_category_name,
	b.third_category_name,
	b.work_no,
	b.sales_name,
	coalesce(b.first_supervisor_name,'') as first_supervisor_name,
	coalesce(b.second_supervisor_name,'') as second_supervisor_name,	
	b.sign_date,
	b.estimate_contract_amount,
	coalesce(c.normal_first_order_date,'') as normal_first_order_date,
	coalesce(c.normal_last_order_date,'') as normal_last_order_date,
	coalesce(c.normal_total_value,0) as normal_total_value,
	d.company_code,
	d.company_name,
	d.company_belong_name
from		
	(
	select 
		customer_no,
		business_type_name,
		max(sdt) as max_sdt,
		sum(sales_value) as sales_value, -- 销售额
		sum(profit) as profit, -- 定价毛利额
		sum(profit)/abs(sum(sales_value)) as profit_rate
	from 
		csx_dw.dws_sale_r_d_detail 
	where 
		sdt between '20190101' and ${hiveconf:current_day}
		and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)	
	group by 
		customer_no,business_type_name
	) as a 
	left join 
		(
		select 
			customer_no,customer_name,work_no,sales_name,first_category_name,second_category_name,third_category_name,channel_code,channel_name,sales_province_name,city_group_name,
			contact_person,contact_phone,attribute_desc,first_supervisor_name,regexp_replace(substr(sign_time,1,10),'-','') as sign_date,estimate_contract_amount/10000 as estimate_contract_amount,
			second_supervisor_name
		from 
			csx_dw.dws_crm_w_a_customer
		where 
			sdt=${hiveconf:current_day}
		) b on a.customer_no=b.customer_no
	left join
		(
		select
			customer_no,first_order_date,last_order_date,total_value,normal_first_order_date,normal_last_order_date,normal_total_value
		from
			csx_dw.dws_crm_w_a_customer_active
		where
			sdt=${hiveconf:current_day}
		) c on c.customer_no=a.customer_no
	left join
		(
		select
			distinct customer_no,company_code,company_name,if(company_belong=1,'彩食鲜','永辉') as company_belong_name
		from
			csx_dw.dws_crm_w_a_customer_company
		where
			sdt=${hiveconf:current_day}
		) d on d.customer_no=a.customer_no
