--===============================================================================================
-- 百万精英奖

select
	concat('20201201-',regexp_replace(to_date(date_sub(now(),1)),'-','')) as sdt_s,
	a.region_name,a.province_name,a.city_group_name,coalesce(b.first_supervisor_work_no,'') as first_supervisor_work_no,coalesce(b.first_supervisor_name,'') as first_supervisor_name,
	a.work_no,a.sales_name,coalesce(d.begin_date,'') as begin_date,
	sum(a.sales_value)as sales_value,
	sum(a.profit)as profit
from 
	(
	select 
		region_name,channel_code,province_code,province_name,city_group_name,work_no,sales_name,customer_no,order_category_name,
		sum(sales_value)as sales_value,
		sum(profit)as profit
	from 
		csx_dw.dws_sale_r_d_detail 
	where 
		sdt between '20201201' and regexp_replace(to_date(date_sub(now(),1)),'-','')
		and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)	
		and (business_type_code in ('2','6') or (business_type_code in ('5') and order_category_code='2'))-- 业务类型编码(1.日配单 2.福利单 3.批发内购 4.城市服务商 5.省区大宗 6.BBC)
		and sales_name not rlike 'A|B|C|D|E|F'
	group by 
		region_name,channel_code,province_code,province_name,city_group_name,work_no,sales_name,customer_no,order_category_name
	) a  
	left join   
		(
		select 
			work_no,sales_name,first_supervisor_work_no,first_supervisor_name
		from 
			csx_dw.dws_crm_w_a_customer 
		where 
			sdt='current'
		group by 
			work_no,sales_name,first_supervisor_work_no,first_supervisor_name
		) b on b.work_no=a.work_no
	left join -- 正常销售维护客户
		(
		select 
			customer_no,customer_name,attribute,work_no,sales_name,first_supervisor_work_no,first_supervisor_name
		from 
			csx_dw.dws_crm_w_a_customer
		where 
			sdt = '20201130' -- 在12月1日前
			and customer_no<>''	
		group by
			customer_no,customer_name,attribute,work_no,sales_name,first_supervisor_work_no,first_supervisor_name
		) c on c.customer_no= a.customer_no	
	left join -- 员工信息
		(
		select 
			employee_code,employee_name,begin_date
		from 
			csx_dw.dws_basic_w_a_employee_org_m
		where 
			sdt = regexp_replace(to_date(date_sub(now(),1)),'-','')
			-- and begin_date >= '20201101'
		group by
			employee_code,employee_name,begin_date			
	) as d on d.employee_code=a.work_no
where
	c.customer_no is null 
	or (c.customer_no is not null and c.work_no is not null and c.sales_name not rlike 'A|B|C|D|E|F') -- 省区已履约且未计入特定销售人员业绩的客户，若在激励案实施期间转移至销售人员名下，该客户不计入此次履约销售额范畴
group by 
	a.region_name,a.province_name,a.city_group_name,coalesce(b.first_supervisor_work_no,''),coalesce(b.first_supervisor_name,''),a.work_no,a.sales_name,coalesce(d.begin_date,'')
order by
	sales_value desc

	
		
	
--===============================================================================================
-- 明细

insert overwrite directory '/tmp/zhangyanpeng/20210119_encourage_data' row format delimited fields terminated by '\t'
select
	concat('20201201-',regexp_replace(date_sub(current_date,1),'-','')) as sdt_s,
	a.region_name,a.province_name,a.city_group_name,b.first_supervisor_work_no,b.first_supervisor_name,a.work_no,a.sales_name,d.begin_date,
	a.customer_no,a.customer_name,a.attribute_name,concat("'",order_no) as order_no,a.order_category_name,
	case when a.channel_code in ('1','9') then '大客户' when a.channel_code='7' then 'BBC' else '其他' end as channel,
	a.sales_value,
	a.profit
from 
	(
	select 
		region_name,channel_code,province_code,province_name,city_group_name,work_no,sales_name,customer_no,customer_name,attribute_name,order_no,order_category_name,
		sum(sales_value)as sales_value,
		sum(profit)as profit
	from 
		csx_dw.dws_sale_r_d_detail 
	where 
		sdt between '20201201' and regexp_replace(date_sub(current_date,1),'-','')
		and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)	
		and (business_type_code in ('2','6') or (business_type_code in ('5') and order_category_code='2')) -- 业务类型编码(1.日配单 2.福利单 3.批发内购 4.城市服务商 5.省区大宗 6.BBC)
		and sales_name not rlike 'A|B|C|D|E|F'
	group by 
		region_name,channel_code,province_code,province_name,city_group_name,work_no,sales_name,customer_no,customer_name,attribute_name,order_no,order_category_name
	) a  
	left join   
		(
		select 
			work_no,sales_name,first_supervisor_work_no,first_supervisor_name
		from 
			csx_dw.dws_crm_w_a_customer
		where 
			sdt='current'
		group by 
			work_no,sales_name,first_supervisor_work_no,first_supervisor_name
		) b on b.work_no=a.work_no
	left join -- 正常销售维护客户
		(
		select 
			customer_no,customer_name,attribute,work_no,sales_name
		from 
			csx_dw.dws_crm_w_a_customer
		where 
			sdt = '20201130' -- 在12月1日前
			and customer_no<>''	
		group by
			customer_no,customer_name,attribute,work_no,sales_name
		) c on c.customer_no= a.customer_no	
	left join -- 员工信息
		(
		select 
			employee_code,employee_name,begin_date
		from 
			csx_dw.dws_basic_w_a_employee_org_m
		where 
			sdt = regexp_replace(date_sub(current_date,1),'-','')
			-- and begin_date >= '20201101'
		group by
			employee_code,employee_name,begin_date			
	) as d on d.employee_code=a.work_no
where
	c.customer_no is null 
	or (c.customer_no is not null and c.work_no is not null and c.sales_name not rlike 'A|B|C|D|E|F') -- 省区已履约且未计入特定销售人员业绩的客户，若在激励案实施期间转移至销售人员名下，该客户不计入此次履约销售额范畴






