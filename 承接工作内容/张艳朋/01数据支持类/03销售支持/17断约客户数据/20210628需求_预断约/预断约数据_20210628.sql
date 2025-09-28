-- ================================================================================================================
-- 预断约客户明细

select
	a.province_name,
	a.city_group_name,
	a.customer_no,
	b.customer_name,
	b.attribute_name,
	b.first_category_name,
	b.second_category_name,
	b.third_category_name,
	b.work_no,
	b.sales_name,
	b.first_supervisor_name,
	b.second_supervisor_name,
	b.sign_date,
	b.estimate_contract_amount,
	d.normal_total_value,
	d.normal_first_order_date,
	d.normal_last_order_date
from	
	( -- Q1季度 下日配单客户
	select 
		province_name,city_group_name,customer_no,
		sum(sales_value) as sales_value
	from 
		csx_dw.dws_sale_r_d_detail 
	where 
		sdt between '20210101' and '20210331'
		and business_type_code in ('1') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
		and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
		and sales_type !='fanli'
	group by 
		province_name,city_group_name,customer_no
	) a
	join
		( -- 客户信息表
		select 
			customer_no,customer_name,attribute_name,first_supervisor_work_no,first_supervisor_name,work_no,sales_name,second_supervisor_name,regexp_replace(substr(sign_time,1,10),'-','') as sign_date,
			first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name,cooperation_mode_name,estimate_contract_amount
		from 
			csx_dw.dws_crm_w_a_customer
		where 
			sdt = 'current'
			and customer_no<>''	
			and cooperation_mode_code='01' -- 合作模式编码(01长期客户,02一次性客户)
		) b on b.customer_no = a.customer_no
	left join
		( -- Q2季度 下日配单客户
		select 
			customer_no
		from 
			csx_dw.dws_sale_r_d_detail 
		where 
			sdt between '20210401' and '20210627'
			and business_type_code in ('1') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
			and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
			and sales_type !='fanli'
		group by 
			customer_no
		) c on c.customer_no=a.customer_no
	left join
		(
		select 
			customer_no,
			sum(sales_value) as normal_total_value,
			min(sdt) as normal_first_order_date,
			max(sdt) as normal_last_order_date
		from 
			csx_dw.dws_sale_r_d_detail
		where 
			sdt between '20190101' and '20210627'
			and business_type_code in ('1') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
			and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
			and sales_type !='fanli'
		group by 
			customer_no
		) d on d.customer_no=a.customer_no
where
	c.customer_no is null
		