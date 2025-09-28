--===============================================================================================
-- 百万精英奖

select
	'1201-1229' as sdt_s,
	region.region_name,t1.province_name,t1.city_real,t1.first_supervisor_work_no,t1.first_supervisor_name,t1.work_no,t1.sales_name,t2.begin_date,
	sum(sales_value) as sales_value, --含税销售额
	sum(profit) as profit --不含税毛利额
from
	(
	select
		a.province_code,a.province_name,a.city_real,b.first_supervisor_work_no,b.first_supervisor_name,a.work_no,a.sales_name,
		case when a.channel ='7' then 'BBC'	
			when a.channel in ('1','9') and b.attribute='合伙人客户' then '城市服务商' 
			when a.channel in ('1','9') and (b.customer_name like '%内%购%' or b.customer_name like '%临保%') then '批发内购'		
			when a.channel in ('1','9') and b.attribute='贸易客户' and d.order_profit_rate<=0.015 then '批发内购' 
			when a.channel in ('1','9') and b.attribute='贸易客户' and (d.order_profit_rate>0.015 or d.order_profit_rate is null) and a.order_kind='WELFARE' then '省区大宗' -- 个人奖项与王者战队不一致 需标识福利单
			when a.channel in ('1','9') and a.order_kind='WELFARE' then '福利单'   
			else '日配单' end sale_group, 	
		sum(a.sales_value)as sales_value,
		sum(a.profit)as profit,
		sum(a.front_profit) as front_profit
	from 
		(
		select 
			channel,province_code,province_name,city_real,work_no,sales_name,origin_order_no,order_no,coalesce(origin_order_no,order_no) order_no_new,
			customer_no,order_kind,
			sum(sales_value)as sales_value,
			sum(profit)as profit,
			sum(front_profit) as front_profit
		from 
			csx_dw.dws_sale_r_d_customer_sale 
		where 
			sdt between '20201201' and '20201229' 
			and sales_type in ('qyg','sapqyg','sapgc','sc','bbc')
			and channel in('1','7','9')	
			and is_self_sale = 1 -- 1自营，0联营(非自营)
			and sales_name not rlike 'A|B|C|D|E|F'
		group by 
			channel,province_code,province_name,city_real,work_no,sales_name,origin_order_no,order_no,coalesce(origin_order_no,order_no),customer_no,order_kind
		) a  
		left join   
			(
			select 
				customer_no,customer_name,attribute,work_no,sales_name,first_supervisor_work_no,first_supervisor_name
			from 
				csx_dw.dws_crm_w_a_customer_m_v1 
			where 
				sdt='20201229'
			group by 
				customer_no,customer_name,attribute,work_no,sales_name,first_supervisor_work_no,first_supervisor_name
			) b on b.customer_no=a.customer_no
		left join --尽量消除退货单误归到批发内购影响，算毛利率往前找近60天原单
			(
			select 
				coalesce(origin_order_no,order_no) order_no_new, 
				sum(profit)/abs(sum(sales_value)) order_profit_rate
			from 
				csx_dw.dws_sale_r_d_customer_sale
			where 
				sdt >= '20201001'
				and channel in ('1','7','9')
			group by 
				coalesce(origin_order_no,order_no)
			) d on a.order_no_new = d.order_no_new 
		left join -- 正常销售维护客户
			(
			select 
				customer_no,customer_name,attribute,work_no,sales_name,first_supervisor_work_no,first_supervisor_name
			from 
				csx_dw.dws_crm_w_a_customer_m_v1
			where 
				sdt = '20201130' -- 在12月1日前
				and customer_no<>''	
			group by
				customer_no,customer_name,attribute,work_no,sales_name,first_supervisor_work_no,first_supervisor_name
			) e on a.customer_no= e.customer_no	
	where
		e.customer_no is null 
		or (e.customer_no is not null and e.work_no is not null and e.sales_name not rlike 'A|B|C|D|E|F') -- 省区已履约且未计入特定销售人员业绩的客户，若在激励案实施期间转移至销售人员名下，该客户不计入此次履约销售额范畴
	group by 
		a.province_code,a.province_name,a.city_real,b.first_supervisor_work_no,b.first_supervisor_name,a.work_no,a.sales_name,
		case when a.channel ='7' then 'BBC'	
			when a.channel in ('1','9') and b.attribute='合伙人客户' then '城市服务商' 
			when a.channel in ('1','9') and (b.customer_name like '%内%购%' or b.customer_name like '%临保%') then '批发内购'		
			when a.channel in ('1','9') and b.attribute='贸易客户' and d.order_profit_rate<=0.015 then '批发内购' 
			when a.channel in ('1','9') and b.attribute='贸易客户' and (d.order_profit_rate>0.015 or d.order_profit_rate is null)  and a.order_kind='WELFARE' then '省区大宗' -- 省区大宗且标识了福利单
			when a.channel in ('1','9') and a.order_kind='WELFARE' then '福利单'   
			else '日配单' end
	) as t1
	left join -- 员工信息
		(
		select 
			employee_code,employee_name,begin_date
		from 
			csx_dw.dws_basic_w_a_employee_org_m
		where 
			sdt = '20201229'
			-- and begin_date >= '20201101'
		group by
			employee_code,employee_name,begin_date			
		) as t2 on t1.work_no=t2.employee_code
	left join -- 大区
		(
		select 
			province_code,province_name,region_code,region_name
		from 
			csx_dw.dim_area
		where
			area_rank=13
		group by
			province_code,province_name,region_code,region_name
		) region on region.province_code= t1.province_code
where
	t1.sale_group in ('BBC','省区大宗','福利单') 
group by
	region.region_name,t1.province_name,t1.city_real,t1.first_supervisor_work_no,t1.first_supervisor_name,t1.work_no,t1.sales_name,t2.begin_date
order by
	sales_value desc

	
		
	
--===============================================================================================
-- 明细

select
	'1201-1229' as sdt_s,
	region.region_name,t1.province_name,t1.city_real,t1.first_supervisor_work_no,t1.first_supervisor_name,t1.work_no,t1.sales_name,t2.begin_date,
	t1.customer_no,t1.customer_name,t1.attribute,concat('A',t1.order_no) as order_no,t1.order_kind,
	case when t1.channel in ('1','9') then '大客户' when t1.channel='7' then 'BBC' else '其他' end as channel,
	sales_value, --含税销售额
	profit --不含税毛利额
from
	(
	select
		a.province_code,a.province_name,a.city_real,b.first_supervisor_work_no,b.first_supervisor_name,a.work_no,a.sales_name,a.customer_no,a.customer_name,b.attribute,a.order_no,a.order_kind,a.channel,
		case when a.channel ='7' then 'BBC'	
			when a.channel in ('1','9') and b.attribute='合伙人客户' then '城市服务商' 
			when a.channel in ('1','9') and (b.customer_name like '%内%购%' or b.customer_name like '%临保%') then '批发内购'		
			when a.channel in ('1','9') and b.attribute='贸易客户' and d.order_profit_rate<=0.015 then '批发内购' 
			when a.channel in ('1','9') and b.attribute='贸易客户' and (d.order_profit_rate>0.015 or d.order_profit_rate is null) and a.order_kind='WELFARE' then '省区大宗' -- 个人奖项与王者战队不一致 需标识福利单
			when a.channel in ('1','9') and a.order_kind='WELFARE' then '福利单'   
			else '日配单' end sale_group, 	
		sum(a.sales_value)as sales_value,
		sum(a.profit)as profit,
		sum(a.front_profit) as front_profit
	from 
		(
		select 
			channel,province_code,province_name,city_real,work_no,sales_name,origin_order_no,order_no,coalesce(origin_order_no,order_no) order_no_new,
			customer_no,customer_name,order_kind,
			sum(sales_value)as sales_value,
			sum(profit)as profit,
			sum(front_profit) as front_profit
		from 
			csx_dw.dws_sale_r_d_customer_sale 
		where 
			sdt between '20201201' and '20201229' 
			and sales_type in ('qyg','sapqyg','sapgc','sc','bbc')
			and channel in('1','7','9')	
			and is_self_sale = 1 -- 1自营，0联营(非自营)
			and sales_name not rlike 'A|B|C|D|E|F'
		group by 
			channel,province_code,province_name,city_real,work_no,sales_name,origin_order_no,order_no,coalesce(origin_order_no,order_no),customer_no,customer_name,order_kind
		) a  
		left join   
			(
			select 
				customer_no,customer_name,attribute,work_no,sales_name,first_supervisor_work_no,first_supervisor_name
			from 
				csx_dw.dws_crm_w_a_customer_m_v1 
			where 
				sdt='20201229'
			group by 
				customer_no,customer_name,attribute,work_no,sales_name,first_supervisor_work_no,first_supervisor_name
			) b on b.customer_no=a.customer_no
		left join --尽量消除退货单误归到批发内购影响，算毛利率往前找近60天原单
			(
			select 
				coalesce(origin_order_no,order_no) order_no_new, 
				sum(profit)/abs(sum(sales_value)) order_profit_rate
			from 
				csx_dw.dws_sale_r_d_customer_sale
			where 
				sdt >= '20201001'
				and channel in ('1','7','9')
			group by 
				coalesce(origin_order_no,order_no)
			) d on a.order_no_new = d.order_no_new 
		left join -- 正常销售维护客户
			(
			select 
				customer_no,customer_name,attribute,work_no,sales_name,first_supervisor_work_no,first_supervisor_name
			from 
				csx_dw.dws_crm_w_a_customer_m_v1
			where 
				sdt = '20201130' -- 在12月1日前
				and customer_no<>''	
			group by
				customer_no,customer_name,attribute,work_no,sales_name,first_supervisor_work_no,first_supervisor_name
			) e on a.customer_no= e.customer_no	
	where
		e.customer_no is null 
		or (e.customer_no is not null and e.work_no is not null and e.sales_name not rlike 'A|B|C|D|E|F') -- 省区已履约且未计入特定销售人员业绩的客户，若在激励案实施期间转移至销售人员名下，该客户不计入此次履约销售额范畴
	group by 
		a.province_code,a.province_name,a.city_real,b.first_supervisor_work_no,b.first_supervisor_name,a.work_no,a.sales_name,a.customer_no,a.customer_name,b.attribute,a.order_no,a.order_kind,a.channel,
		case when a.channel ='7' then 'BBC'	
			when a.channel in ('1','9') and b.attribute='合伙人客户' then '城市服务商' 
			when a.channel in ('1','9') and (b.customer_name like '%内%购%' or b.customer_name like '%临保%') then '批发内购'		
			when a.channel in ('1','9') and b.attribute='贸易客户' and d.order_profit_rate<=0.015 then '批发内购' 
			when a.channel in ('1','9') and b.attribute='贸易客户' and (d.order_profit_rate>0.015 or d.order_profit_rate is null)  and a.order_kind='WELFARE' then '省区大宗' -- 省区大宗且标识了福利单
			when a.channel in ('1','9') and a.order_kind='WELFARE' then '福利单'   
			else '日配单' end
	) as t1
	left join -- 员工信息
		(
		select 
			employee_code,employee_name,begin_date
		from 
			csx_dw.dws_basic_w_a_employee_org_m
		where 
			sdt = '20201229'
			-- and begin_date >= '20201101'
		group by
			employee_code,employee_name,begin_date			
		) as t2 on t1.work_no=t2.employee_code
	left join -- 大区
		(
		select 
			province_code,province_name,region_code,region_name
		from 
			csx_dw.dim_area
		where
			area_rank=13
		group by
			province_code,province_name,region_code,region_name
		) region on region.province_code= t1.province_code
where
	t1.sale_group in ('BBC','省区大宗','福利单')





--===============================================================================================
-- 王者战队

select
	'1201-1229' as sdt_s,
	region.region_name,
	region.province_name,
	sum(sales_value) as sales_value, --含税销售额
	sum(excluding_tax_profit) as excluding_tax_profit --不含税毛利额
from
	(
	select
		a.province_code,a.province_name,a.smonth,
		case when a.channel ='7' then 'BBC'	
			when a.channel in ('1','9') and b.attribute='合伙人客户' then '城市服务商' 
			when a.channel in ('1','9') and (b.customer_name like '%内%购%' or b.customer_name like '%临保%') then '批发内购'		
			when a.channel in ('1','9') and b.attribute='贸易客户' and d.order_profit_rate<=0.015 then '批发内购' 
			when a.channel in ('1','9') and b.attribute='贸易客户' and (d.order_profit_rate>0.015 or d.order_profit_rate is null) then '省区大宗'
			when a.channel in ('1','9') and a.order_kind='WELFARE' then '福利单'   
			else '日配单' end sale_group, 	
		sum(a.sales_value)as sales_value,
		sum(a.excluding_tax_profit)as excluding_tax_profit,
		sum(a.front_profit) as front_profit
	from 
		(
		select 
			channel,province_code,province_name,substr(sdt,1,6) smonth,origin_order_no,order_no,coalesce(origin_order_no,order_no) order_no_new,
			customer_no,order_kind,
			sum(sales_value)as sales_value,
			sum(excluding_tax_profit)as excluding_tax_profit,
			sum(front_profit) as front_profit
		from 
			csx_dw.dws_sale_r_d_customer_sale 
		where 
			sdt between '20201201' and '20201229' 
			and sales_type in ('qyg','sapqyg','sapgc','sc','bbc')
			and channel in('1','7','9')	
			and is_self_sale = 1 -- 1自营，0联营(非自营)			
		group by 
			channel,province_code,province_name,substr(sdt,1,6),origin_order_no,order_no,coalesce(origin_order_no,order_no),customer_no,order_kind
		) a  
		left join   
			(
			select 
				*
			from 
				csx_dw.dws_crm_w_a_customer_m_v1 
			where 
				sdt=regexp_replace(to_date(date_sub(now(),1)),'-','')
			) b on b.customer_no=a.customer_no
		left join --尽量消除退货单误归到批发内购影响，算毛利率往前找近60天原单
			(
			select 
				coalesce(origin_order_no,order_no) order_no_new, 
				sum(profit)/abs(sum(sales_value)) order_profit_rate
			from 
				csx_dw.dws_sale_r_d_customer_sale
			where 
				sdt >= '20201001'
				and channel in ('1','7','9')
			group by 
				coalesce(origin_order_no,order_no)
			) d on a.order_no_new = d.order_no_new 	
	group by 
		a.province_code,a.province_name,a.smonth,
		case when a.channel ='7' then 'BBC'	
			when a.channel in ('1','9') and b.attribute='合伙人客户' then '城市服务商' 
			when a.channel in ('1','9') and (b.customer_name like '%内%购%' or b.customer_name like '%临保%') then '批发内购'		
			when a.channel in ('1','9') and b.attribute='贸易客户' and d.order_profit_rate<=0.015 then '批发内购' 
			when a.channel in ('1','9') and b.attribute='贸易客户' and (d.order_profit_rate>0.015 or d.order_profit_rate is null) then '省区大宗'
			when a.channel in ('1','9') and a.order_kind='WELFARE' then '福利单'   
			else '日配单' end
	) as t1
	left join -- 大区
		(
		select 
			province_code,province_name,region_code,region_name
		from 
			csx_dw.dim_area
		where
			area_rank=13
		group by
			province_code,province_name,region_code,region_name
		) region on region.province_code= t1.province_code			
where
	t1.sale_group in ('BBC','省区大宗','福利单','日配单') 
group by
	region.region_name,region.province_name
order by
	sales_value desc




