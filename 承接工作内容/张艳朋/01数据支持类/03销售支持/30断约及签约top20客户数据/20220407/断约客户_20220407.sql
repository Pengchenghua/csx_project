-- ================================================================================================================
-- 截止目前断约情况

set current_day ='20220331';

insert overwrite directory '/tmp/zhangyanpeng/20220407_linshi_2' row format delimited fields terminated by '\t' 

select
	b.sales_province_name,
	b.sales_city_name,
	a.customer_no,
	b.customer_name,
	b.attribute_desc,
	b.first_category_name,
	b.second_category_name,
	b.third_category_name,
	b.work_no,
	b.sales_name,
	b.first_supervisor_name,
	'' as manager_name,
	b.sign_date,
	c.diff_month,
	b.estimate_contract_amount,
	c.normal_total_value,
	c.normal_first_order_date,
	c.normal_last_order_date,
	a.break_date
from
	(
	select
		customer_no,break_date
	from
		(
		select 
			customer_no,
			regexp_replace(date_add(from_unixtime(unix_timestamp(max(sdt),'yyyyMMdd')),90),'-','') as break_date
		from 
			csx_dw.dws_sale_r_d_detail
		where 
			sdt between '20190101' and ${hiveconf:current_day}
			and business_type_code in ('1') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
			and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
			and sales_type !='fanli'
			and province_name='北京市'
		group by 
			customer_no
		) tmp1
	where
		break_date between '20220101' and ${hiveconf:current_day}
	) a 
	left join
		( 
		select
			customer_no,customer_name,work_no,sales_name,first_supervisor_work_no,first_supervisor_name,first_category_name,second_category_name,third_category_name,attribute_desc,
			sales_province_name,sales_city_name,regexp_replace(substr(sign_time,1,10),'-','') as sign_date,estimate_contract_amount
		from
			csx_dw.dws_crm_w_a_customer
		where 
			sdt = 'current'
		) b on b.customer_no=a.customer_no
	--left join
	--	(
	--	select
	--		customer_no,customer_name,normal_first_order_date,normal_last_order_date,normal_total_value,
	--		months_between(from_unixtime(unix_timestamp(normal_last_order_date,'yyyyMMdd')),from_unixtime(unix_timestamp(normal_first_order_date,'yyyyMMdd'))) as diff_month
	--	from 
	--		csx_dw.dws_crm_w_a_customer_active
	--	where 
	--		sdt = 'current' 
	--	) c on c.customer_no=a.customer_no
	left join
		(
		select 
			customer_no,
			min(sdt) as normal_first_order_date,
			max(sdt) as normal_last_order_date,
			sum(sales_value) as normal_total_value,
			months_between(from_unixtime(unix_timestamp(max(sdt),'yyyyMMdd')),from_unixtime(unix_timestamp(min(sdt),'yyyyMMdd'))) as diff_month
		from 
			csx_dw.dws_sale_r_d_detail
		where 
			sdt between '20190101' and ${hiveconf:current_day}
			and business_type_code in ('1') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
			and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
			and sales_type !='fanli'
			and province_name='北京市'
		group by 
			customer_no
		) c on c.customer_no=a.customer_no
;

			
		
