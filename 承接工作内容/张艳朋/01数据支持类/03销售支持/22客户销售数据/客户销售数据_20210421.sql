--============================================================================================================================================================
-- 日配客户销售数据

insert overwrite directory '/tmp/zhangyanpeng/20210421_linshi_1' row format delimited fields terminated by '\t' 

select	
	c.region_name,
	c.province_name,
	a.customer_no,
	b.customer_name,
	b.work_no,
	b.sales_name,
	b.first_category_name,
	b.second_category_name,
	b.third_category_name,
	b.sign_date,
	d.normal_first_order_date,
	d.normal_active_days,
	'-' as order_days,
	d.normal_last_order_date,
	'-' as non_order_days,
	sum(sales_value) as sales_value,
	sum(profit) as profit,
	sum(profit)/abs(sum(sales_value)) as profit_rate
from
	(
	select
		region_code,province_code,customer_no,business_type_name,
		-- regexp_replace(split(sign_time,' ')[0],'-','') as sign_date,
		-- regexp_replace(split_part(sign_time,' ',1),'-','') as sign_date,
		sales_value,profit
	from 
		csx_dw.dws_sale_r_d_detail
	where 
		sdt >= '20200701' 
		and sdt <= '20210420'
		and channel_code in ('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
		and business_type_code in ('1') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
	) as a
	left join 
		(
		select 
			customer_no,customer_name,work_no,sales_name,first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name,
			regexp_replace(split_part(sign_time,' ',1),'-','') as sign_date
		from 
			csx_dw.dws_crm_w_a_customer
		where 
			sdt='current'
		group by 
			customer_no,customer_name,work_no,sales_name,first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name,
			regexp_replace(split_part(sign_time,' ',1),'-','')
		) b on a.customer_no=b.customer_no
	left join 
		(
		select 
			province_code,province_name,region_code,region_name
		from 
			csx_dw.dws_sale_w_a_area_belong
		group by 
			province_code,province_name,region_code,region_name
		) c on c.province_code=a.province_code	
	left join 
		(
		select 
			customer_no,
			min(sdt) as normal_first_order_date,
			max(sdt) as normal_last_order_date,
			count(distinct sdt) as normal_active_days
		from 
			csx_dw.dws_sale_r_d_detail
		where 
			sdt>='20190101'
			and channel_code in ('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
			and business_type_code in ('1') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
			and sales_type !='fanli'
		group by 
			customer_no
		) d on d.customer_no=a.customer_no
where
	c.region_name='华西大区'
group by 
	c.region_name,
	c.province_name,
	a.customer_no,
	b.customer_name,
	b.work_no,
	b.sales_name,
	b.first_category_name,
	b.second_category_name,
	b.third_category_name,
	b.sign_date,
	d.normal_first_order_date,
	d.normal_active_days,
	'-',
	d.normal_last_order_date,
	'-'
		