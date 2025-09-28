--==============================================================================================================================================================================

-- 业务类型销售额

select
	b.sales_region_name,
	b.province_name,
	a.business_type_name,
	a.smonth,
	sum(a.sales_value) sales_value,
	sum(a.profit) as profit,
	sum(a.profit)/abs(sum(a.sales_value)) as profit_rate
from
	(
	select 
		customer_no,business_type_name,substr(sdt,1,6) as smonth,sales_value,profit
	from 
		csx_dw.dws_sale_r_d_detail
	where 
		sdt>='20210101' and sdt<='20210930'
		and channel_code in('1','7','9')
		--and business_type_code ='1'
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
group by
	b.sales_region_name,
	b.province_name,
	a.business_type_name,
	a.smonth		
;			
	
--==============================================================================================================================================================================

-- 新签客户

select
	b.sales_region_name,
	b.province_name,
	a.smonth,
	count(a.customer_no) as customer_cnt,
	count(case when b.attribute_desc like '%日配%' then a.customer_no else null end) as normal_customer_cnt
from
	(
	select
		customer_no,regexp_replace(substr(first_sign_time,1,7),'-','') as smonth,estimate_contract_amount
	from 
		csx_dw.dws_crm_w_a_customer
	where 
		sdt='current'
		and regexp_replace(substr(first_sign_time,1,7),'-','') between '202101' and '202109'
	) as a 
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
		)b on a.customer_no=b.customer_no	
group by 
	b.sales_region_name,
	b.province_name,
	a.smonth
;

--==============================================================================================================================================================================

-- 新履约客户及履约金额

select
	b.sales_region_name,
	b.province_name,
	a.smonth,
	count(a.customer_no) as customer_cnt,
	sum(c.sales_value) as sales_value
from
	(
	select
		sales_id,substr(first_order_date,1,6) as smonth,customer_no
	from
		csx_dw.dws_crm_w_a_customer_active
	where 
		sdt = 'current' 
		and substr(first_order_date,1,6) between '202101' and '202109'
	) as a 
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
		) b on a.customer_no=b.customer_no
	left join
		(
		select 
			customer_no,substr(sdt,1,6) as smonth,sum(sales_value) as sales_value
		from 
			csx_dw.dws_sale_r_d_detail
		where 
			sdt>='20210101' and sdt<='20210930'
			and channel_code in('1','7','9')
			--and business_type_code ='1'
		group by 
			customer_no,substr(sdt,1,6)
		) c on c.customer_no=a.customer_no and c.smonth=a.smonth
group by 
	b.sales_region_name,
	b.province_name,
	a.smonth
;	

--==============================================================================================================================================================================

-- 断约客户数

select
	b.sales_region_name,
	b.province_name,
	a.after_month,	
	count(distinct a.customer_no) as customer_cnt
from
	(
	select 
		customer_no,
		substr(regexp_replace(date_add(from_unixtime(unix_timestamp(max(sdt),'yyyyMMdd')),90),'-',''),1,6) as after_month
	from 
		csx_dw.dws_sale_r_d_detail 
	where 
		sdt between '20190101' and '20210930'
		and business_type_code in ('1') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
		and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
		and sales_type !='fanli'
	group by 
		customer_no
	) as a 
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
		) b on a.customer_no=b.customer_no	
where
	a.after_month between '202101' and '202109'
group by
	b.sales_region_name,
	b.province_name,
	a.after_month	
;	


-- ================================================================================================================
-- 商机数量 不含100%

select 
	t2.sales_region_name,t2.province_name,
	substr(t1.sdt,1,6) as smonth,
	count(distinct t1.business_number) as business_cnt,
	sum(t1.estimate_contract_amount) as estimate_contract_amount
from
	(
	select
		sdt,id,status,business_stage,work_no,sales_name,business_number,estimate_contract_amount
	from
		csx_dw.dws_crm_w_a_business_customer 
	where 
		(sdt='20210731'
		and status='1'
		and business_stage !=5)
		or
		(sdt='20210831'
		and status='1'
		and business_stage !=5)
		or
		(sdt='20210930'
		and status='1'
		and business_stage !=5)	
	) t1 
	join
		(
		select
			customer_id,customer_no,customer_name,work_no,sales_name,first_supervisor_work_no,first_supervisor_name,first_category_name,second_category_name,third_category_name,attribute_desc,
			sales_region_name,sales_province_name,province_name,regexp_replace(substr(sign_time,1,10),'-','') as sign_date,estimate_contract_amount
		from
			csx_dw.dws_crm_w_a_customer
		where 
			sdt = 'current'
			and province_name not like '%平台%'		
		)t2 on t2.customer_id=t1.id		
group by 
	t2.sales_region_name,t2.province_name,
	substr(t1.sdt,1,6)
;

-- ================================================================================================================
-- 商机数量 不含10%和100%

select 
	t2.sales_region_name,t2.province_name,
	substr(t1.sdt,1,6) as smonth,
	count(distinct t1.business_number) as business_cnt,
	sum(t1.estimate_contract_amount) as estimate_contract_amount
from
	(
	select
		sdt,id,status,business_stage,work_no,sales_name,business_number,estimate_contract_amount
	from
		csx_dw.dws_crm_w_a_business_customer 
	where 
		(sdt='20210731'
		and status='1'
		and business_stage !=5
		and business_stage !=1)
		or
		(sdt='20210831'
		and status='1'
		and business_stage !=5
		and business_stage !=1)
		or
		(sdt='20210930'
		and status='1'
		and business_stage !=5
		and business_stage !=1)	
	) t1 
	join
		(
		select
			customer_id,customer_no,customer_name,work_no,sales_name,first_supervisor_work_no,first_supervisor_name,first_category_name,second_category_name,third_category_name,attribute_desc,
			sales_region_name,sales_province_name,province_name,regexp_replace(substr(sign_time,1,10),'-','') as sign_date,estimate_contract_amount
		from
			csx_dw.dws_crm_w_a_customer
		where 
			sdt = 'current'
			and province_name not like '%平台%'		
		)t2 on t2.customer_id=t1.id		
group by 
	t2.sales_region_name,t2.province_name,
	substr(t1.sdt,1,6)
;

-- ================================================================================================================
-- 商机

select
	t2.sales_region_name,t2.province_name,t1.business_stage_type,
	count(distinct t1.business_number) as business_number_cnt,
	sum(t1.estimate_contract_amount) as estimate_contract_amount
from
	(
	select 
		id,business_number,estimate_contract_amount,attribute_desc,business_stage,
		case when business_stage=1 then '10%阶段'
			when business_stage=2 then '25%阶段'
			when business_stage=3 then '50%阶段'
			when business_stage=4 then '75%阶段'
			when business_stage=5 then '100%阶段'
			else '其他'
		end as business_stage_type
	from 
		csx_dw.dws_crm_w_a_business_customer
	where 
		sdt = '20211025'
	) t1 
	join
		(
		select
			customer_id,customer_no,customer_name,work_no,sales_name,first_supervisor_work_no,first_supervisor_name,first_category_name,second_category_name,third_category_name,attribute_desc,
			sales_region_name,sales_province_name,province_name,regexp_replace(substr(sign_time,1,10),'-','') as sign_date,estimate_contract_amount
		from
			csx_dw.dws_crm_w_a_customer
		where 
			sdt = 'current'
			and province_name not like '%平台%'		
		)t2 on t2.customer_id=t1.id	
group by 
	t2.sales_region_name,t2.province_name,t1.business_stage_type