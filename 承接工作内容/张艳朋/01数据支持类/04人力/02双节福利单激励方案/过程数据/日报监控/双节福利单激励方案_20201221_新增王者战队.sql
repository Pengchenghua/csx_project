--===============================================================================================
-- 百万精英奖

select
	'1201-1221' as sdt_s,
	province_name,city_real,supervisor_work_no,supervisor_name,work_no,sales_name,t5.begin_date,
	sum(sales_value) as sales_value, --含税销售额
	sum(profit) as profit --含税毛利额
from
	(
	select
		province_name,city_real,supervisor_work_no,supervisor_name,work_no,sales_name,customer_no,order_no,order_kind,coalesce(origin_order_no,order_no) as order_no_new,
		sum(sales_value) as sales_value,
		sum(profit) as profit,
		sum(profit)/abs(sum(sales_value)) as order_profit_rate
	from
		csx_dw.dws_sale_r_d_customer_sale
	where 
		sdt between '20201201' and '20201221'
		and sales_type in ('qyg','sapqyg','sapgc','sc','bbc')
		and channel in ('1','7','9')
		and province_name not like '平台%'
		and is_self_sale = 1 -- 1自营，0联营(非自营)
	group by 
		province_name,city_real,supervisor_work_no,supervisor_name,work_no,sales_name,customer_no,order_no,order_kind,coalesce(origin_order_no,order_no)
	) as t1
	left join
		(
		select 
			customer_no,customer_name,attribute
		from 
			csx_dw.dws_crm_w_a_customer_m_v1
		where 
			sdt = '20201221'
			and customer_no<>''	
		group by
			customer_no,customer_name,attribute
		) as t2 on t1.customer_no=t2.customer_no
	left join
		(
		select 
			customer_no,customer_name,attribute
		from 
			csx_dw.dws_crm_w_a_customer_m_v1
		where 
			sdt = '20201130' -- 在12月1日前
			and customer_no<>''	
			--and work_no is not null 
			--and sales_name not rlike 'A|B|C|D|E|F' -- 虚拟销售
		group by
			customer_no,customer_name,attribute
		) as t3 on t1.customer_no=t3.customer_no
	left join -- 员工信息
		(
		select 
			employee_code,employee_name,begin_date
		from 
			csx_dw.dws_basic_w_a_employee_org_m
		where 
			sdt = '20201221'
			-- and begin_date >= '20201101'
		group by
			employee_code,employee_name,begin_date			
		) as t5 on t1.work_no=t5.employee_code
	left join --尽量消除退货单误归到批发内购影响，算毛利率往前找近60天原单
		(
		select 
			coalesce(origin_order_no,order_no) as order_no_new, 
			sum(profit)/abs(sum(sales_value)) as order_profit_rate
		from 
			csx_dw.dws_sale_r_d_customer_sale
		where 
			sdt >= '20201001' -- 尽量消除退货单误归到批发内购影响，算毛利率往前找近60天原单
			and channel in ('1','7','9')
		group by 
			coalesce(origin_order_no,order_no)
		) t6 on t1.order_no_new = t6.order_no_new 	
where
	(t3.customer_no is null or (t3.customer_no is not null and work_no is not null and sales_name not rlike 'A|B|C|D|E|F')) -- 省区已履约且未计入特定销售人员业绩的客户，若在激励案实施期间转移至销售人员名下，该客户不计入此次履约销售额范畴
	and t2.customer_name not like '%内%购%' -- 批发内购
	and t2.customer_name not like '%临保%' -- 批发内购
	and((t2.attribute not in ('合伙人客户','贸易客户') and t1.order_kind='WELFARE') -- 福利单
		or 
		(t2.attribute='贸易客户' and t1.order_kind='WELFARE' and (t6.order_profit_rate>0.015 or t6.order_profit_rate is null))) -- 贸易客户>1.5%大宗的福利单标识订单
group by
	province_name,city_real,supervisor_work_no,supervisor_name,work_no,sales_name,t5.begin_date
-- having
-- 	sales_value>=1000000 -- 销售额大于等于100万
order by
	sales_value desc; 



--==================================================================================================================
-- 双节福利王

select
	'1201-1221' as sdt_s,
	province_name,city_real,supervisor_work_no,supervisor_name,work_no,sales_name,t5.begin_date,
	sum(sales_value) as sales_value, --含税销售额
	sum(profit) as profit --含税毛利额
from
	(
	select
		province_name,city_real,supervisor_work_no,supervisor_name,work_no,sales_name,customer_no,order_no,order_kind,coalesce(origin_order_no,order_no) as order_no_new,
		sum(sales_value) as sales_value,
		sum(profit) as profit,
		sum(profit)/abs(sum(sales_value)) as order_profit_rate
	from
		csx_dw.dws_sale_r_d_customer_sale
	where 
		sdt between '20201201' and '20201221'
		and sales_type in ('qyg','sapqyg','sapgc','sc','bbc')
		and channel in ('1','7','9')
		and province_name not like '平台%'
		and is_self_sale = 1 -- 1自营，0联营(非自营)
	group by 
		province_name,city_real,supervisor_work_no,supervisor_name,work_no,sales_name,customer_no,order_no,order_kind,coalesce(origin_order_no,order_no)
	) as t1
	left join
		(
		select 
			customer_no,customer_name,attribute
		from 
			csx_dw.dws_crm_w_a_customer_m_v1
		where 
			sdt = '20201221'
			and customer_no<>''	
		group by
			customer_no,customer_name,attribute
		) as t2 on t1.customer_no=t2.customer_no
	left join
		(
		select 
			customer_no,customer_name,attribute
		from 
			csx_dw.dws_crm_w_a_customer_m_v1
		where 
			sdt = '20201130' -- 在12月1日前
			and customer_no<>''	
			--and work_no is not null 
			--and sales_name not rlike 'A|B|C|D|E|F' -- 虚拟销售
		group by
			customer_no,customer_name,attribute
		) as t3 on t1.customer_no=t3.customer_no
	left join -- 员工信息
		(
		select 
			employee_code,employee_name,begin_date
		from 
			csx_dw.dws_basic_w_a_employee_org_m
		where 
			sdt = '20201221'
			-- and begin_date >= '20201101'
		group by
			employee_code,employee_name,begin_date			
		) as t5 on t1.work_no=t5.employee_code
	left join --尽量消除退货单误归到批发内购影响，算毛利率往前找近60天原单
		(
		select 
			coalesce(origin_order_no,order_no) as order_no_new, 
			sum(profit)/abs(sum(sales_value)) as order_profit_rate
		from 
			csx_dw.dws_sale_r_d_customer_sale
		where 
			sdt >= '20201001' -- 尽量消除退货单误归到批发内购影响，算毛利率往前找近60天原单
			and channel in ('1','7','9')
		group by 
			coalesce(origin_order_no,order_no)
		) t6 on t1.order_no_new = t6.order_no_new 	
where
	(t3.customer_no is null or (t3.customer_no is not null and work_no is not null and sales_name not rlike 'A|B|C|D|E|F')) -- 省区已履约且未计入特定销售人员业绩的客户，若在激励案实施期间转移至销售人员名下，该客户不计入此次履约销售额范畴
	and t2.customer_name not like '%内%购%' -- 批发内购
	and t2.customer_name not like '%临保%' -- 批发内购
	and((t2.attribute not in ('合伙人客户','贸易客户') and t1.order_kind='WELFARE') -- 福利单
		or 
		(t2.attribute='贸易客户' and t1.order_kind='WELFARE' and (t6.order_profit_rate>0.015 or t6.order_profit_rate is null))) -- 贸易客户>1.5%大宗的福利单标识订单
group by
	province_name,city_real,supervisor_work_no,supervisor_name,work_no,sales_name,t5.begin_date
-- having
-- 	sales_value>=1000000 -- 销售额大于等于100万
order by
	sales_value desc; 
	
	
	
	
	
--==================================================================================================================
-- 福利新人王

select
	'1201-1221' as sdt_s,
	province_name,city_real,supervisor_work_no,supervisor_name,work_no,sales_name,t5.begin_date,
	sum(if(t4.first_order_date >t5.begin_date,t1.sales_value,null)) as sales_value, --含税销售额
	sum(if(t4.first_order_date >t5.begin_date,t1.profit,null)) as profit --含税毛利额
from
	(
	select
		province_name,city_real,supervisor_work_no,supervisor_name,work_no,sales_name,customer_no,order_no,order_kind,coalesce(origin_order_no,order_no) as order_no_new,
		sum(sales_value) as sales_value,
		sum(profit) as profit,
		sum(profit)/abs(sum(sales_value)) as order_profit_rate
	from
		csx_dw.dws_sale_r_d_customer_sale
	where 
		sdt between '20201201' and '20201221'
		and sales_type in ('qyg','sapqyg','sapgc','sc','bbc')
		and channel in ('1','7','9')
		and province_name not like '平台%'
		and is_self_sale = 1 -- 1自营，0联营(非自营)
	group by 
		province_name,city_real,supervisor_work_no,supervisor_name,work_no,sales_name,customer_no,order_no,order_kind,coalesce(origin_order_no,order_no)
	) as t1
	left join
		(
		select 
			customer_no,customer_name,attribute
		from 
			csx_dw.dws_crm_w_a_customer_m_v1
		where 
			sdt = '20201221'
			and customer_no<>''	
		group by
			customer_no,customer_name,attribute
		) as t2 on t1.customer_no=t2.customer_no
	left join
		(
		select 
			customer_no,customer_name,attribute
		from 
			csx_dw.dws_crm_w_a_customer_m_v1
		where 
			sdt = '20201130' -- 在12月1日前
			and customer_no<>''	
			-- and work_no is not null 
			-- and sales_name not rlike 'A|B|C|D|E|F' -- 虚拟销售
		group by
			customer_no,customer_name,attribute
		) as t3 on t1.customer_no=t3.customer_no
	left join -- 客户首单日期
		(
		select 
			customer_no, 
			first_order_date
		from 
			csx_dw.ads_crm_w_a_customer_active_info
		where 
			sdt = '20201221' --可取最新分区
			-- and first_order_date between '20200701' and '20200930' --客户的首次成交日期晚于入职日期
		group by
			customer_no,first_order_date			
		) as t4 on t1.customer_no=t4.customer_no
	left join -- 员工信息
		(
		select 
			employee_code,employee_name,begin_date
		from 
			csx_dw.dws_basic_w_a_employee_org_m
		where 
			sdt = '20201221'
			and begin_date >= '20201101'
		group by
			employee_code,employee_name,begin_date			
		) as t5 on t1.work_no=t5.employee_code	
	left join --尽量消除退货单误归到批发内购影响，算毛利率往前找近60天原单
		(
		select 
			coalesce(origin_order_no,order_no) as order_no_new, 
			sum(profit)/abs(sum(sales_value)) as order_profit_rate
		from 
			csx_dw.dws_sale_r_d_customer_sale
		where 
			sdt >= '20201001' -- 尽量消除退货单误归到批发内购影响，算毛利率往前找近60天原单
			and channel in ('1','7','9')
		group by 
			coalesce(origin_order_no,order_no)
		) t6 on t1.order_no_new = t6.order_no_new 			
where
	--可能是新增客户
	(t3.customer_no is null or (t3.customer_no is not null and work_no is not null and sales_name not rlike 'A|B|C|D|E|F')) -- 省区已履约且未计入特定销售人员业绩的客户，若在激励案实施期间转移至销售人员名下，该客户不计入此次履约销售额范畴
	and t2.customer_name not like '%内%购%' -- 批发内购
	and t2.customer_name not like '%临保%' -- 批发内购
	and((t2.attribute not in ('合伙人客户','贸易客户') and t1.order_kind='WELFARE') -- 福利单
		or 
		(t2.attribute='贸易客户' and t1.order_kind='WELFARE' and (t6.order_profit_rate>0.015 or t6.order_profit_rate is null))) -- 贸易客户>1.5%大宗的福利单标识订单
	and t5.begin_date is not null -- 11月1日及之后入职的销售员
group by
	province_name,city_real,supervisor_work_no,supervisor_name,work_no,sales_name,t5.begin_date
order by
	sales_value desc; 
	
	
	
	
--===============================================================================================
-- 明细

select
	'1201-1221' as sdt_s,
	province_name,city_real,supervisor_work_no,supervisor_name,work_no,sales_name,t5.begin_date,t1.customer_no,t2.customer_name,t2.attribute,t1.order_no,t1.order_kind,
	sales_value, --含税销售额
	profit --含税毛利额
from
	(
	select
		province_name,city_real,supervisor_work_no,supervisor_name,work_no,sales_name,customer_no,order_no,order_kind,coalesce(origin_order_no,order_no) as order_no_new,
		sum(sales_value) as sales_value,
		sum(profit) as profit,
		sum(profit)/abs(sum(sales_value)) as order_profit_rate
	from
		csx_dw.dws_sale_r_d_customer_sale
	where 
		sdt between '20201201' and '20201221'
		and sales_type in ('qyg','sapqyg','sapgc','sc','bbc')
		and channel in ('1','7','9')
		and province_name not like '平台%'
		and is_self_sale = 1 -- 1自营，0联营(非自营)
	group by 
		province_name,city_real,supervisor_work_no,supervisor_name,work_no,sales_name,customer_no,order_no,order_kind,coalesce(origin_order_no,order_no)
	) as t1
	left join
		(
		select 
			customer_no,customer_name,attribute
		from 
			csx_dw.dws_crm_w_a_customer_m_v1
		where 
			sdt = '20201221'
			and customer_no<>''	
		group by
			customer_no,customer_name,attribute
		) as t2 on t1.customer_no=t2.customer_no
	left join
		(
		select 
			customer_no,customer_name,attribute
		from 
			csx_dw.dws_crm_w_a_customer_m_v1
		where 
			sdt = '20201130' -- 在12月1日前
			and customer_no<>''	
			--and work_no is not null 
			--and sales_name not rlike 'A|B|C|D|E|F' -- 虚拟销售
		group by
			customer_no,customer_name,attribute
		) as t3 on t1.customer_no=t3.customer_no
	left join -- 员工信息
		(
		select 
			employee_code,employee_name,begin_date
		from 
			csx_dw.dws_basic_w_a_employee_org_m
		where 
			sdt = '20201221'
			-- and begin_date >= '20201101'
		group by
			employee_code,employee_name,begin_date			
		) as t5 on t1.work_no=t5.employee_code
	left join --尽量消除退货单误归到批发内购影响，算毛利率往前找近60天原单
		(
		select 
			coalesce(origin_order_no,order_no) as order_no_new, 
			sum(profit)/abs(sum(sales_value)) as order_profit_rate
		from 
			csx_dw.dws_sale_r_d_customer_sale
		where 
			sdt >= '20201001' -- 尽量消除退货单误归到批发内购影响，算毛利率往前找近60天原单
			and channel in ('1','7','9')
		group by 
			coalesce(origin_order_no,order_no)
		) t6 on t1.order_no_new = t6.order_no_new 	
where
	(t3.customer_no is null or (t3.customer_no is not null and work_no is not null and sales_name not rlike 'A|B|C|D|E|F')) -- 省区已履约且未计入特定销售人员业绩的客户，若在激励案实施期间转移至销售人员名下，该客户不计入此次履约销售额范畴
	and t2.customer_name not like '%内%购%' -- 批发内购
	and t2.customer_name not like '%临保%' -- 批发内购
	and((t2.attribute not in ('合伙人客户','贸易客户') and t1.order_kind='WELFARE') -- 福利单
		or 
		(t2.attribute='贸易客户' and t1.order_kind='WELFARE' and (t6.order_profit_rate>0.015 or t6.order_profit_rate is null))) -- 贸易客户>1.5%大宗的福利单标识订单
--group by
--	province_name,city_real,supervisor_work_no,supervisor_name,work_no,sales_name,t5.begin_date
-- having
-- 	sales_value>=1000000 -- 销售额大于等于100万
--order by
--	sales_value desc; 





--===============================================================================================
-- 王者战队

select
	'1201-1221' as sdt_s,
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
			sdt between '20201201' and '20201221' 
			and sales_type in ('qyg','sapqyg','sapgc','sc','bbc')
			and channel in('1','7','9')		
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




