--================================================================================================
	
insert overwrite directory '/tmp/zhangyanpeng/20210913_linshi_1' row format delimited fields terminated by '\t' 
	
select 
	*
from 
	csx_dw.dws_basic_w_a_employee_org_m
where 
	sdt = '20210906'
	--and emp_status='on'
;
	
--================================================================================================

select 
	work_no,sales_name,
	count(case when regexp_replace(substr(sign_time,1,10),'-','') between '20210301' and '20210831' then customer_no else null end)cnt1,
	count(case when regexp_replace(substr(sign_time,1,10),'-','') between '20210601' and '20210831' then customer_no else null end)cnt2,
	count(case when regexp_replace(substr(sign_time,1,10),'-','') between '20210801' and '20210831' then customer_no else null end)cnt3
from 
	csx_dw.dws_crm_w_a_customer
where 
	sdt='20210906'
	and work_no in ('80952743','80007454','80768089','80895348','80929704','80937132','81016757','81026931','80917566','80936091','80691224','80895351','81089088','81101470','80816155','80764642','80973546','80980614','80952742','81056954','81080592','81082956','81095855','80001032','80012225','80960666','80969699','80972915','81094022','80895350','80912701','80924363','80927331','80929710','80939525','81081095','81101897')
group by 
	work_no,sales_name
;
	
	
	
--================================================================================================
select
	a.sales_region_name,a.sales_province_name,a.city_group_name,a.b_type,a.business_type_name,
	count(distinct a.customer_no) as cnt1,
	sum(b.sales_value)as sales_value,
	count(distinct case when sign_date>='20210101' then a.customer_no else null end) as cnt2,
	sum(case when sign_date>='20210101' then a.avg_amount else 0 end) cnt3,
	count(distinct c.customer_no )cnt4,
	sum(c.sales_value )cnt5
from
	(
	select 
		customer_no,sales_region_name,sales_province_name,city_group_name,regexp_replace(substr(sign_time,1,10),'-','') as sign_date,business_type_name,
		estimate_contract_amount/count(customer_no) over(partition by customer_no) as avg_amount,estimate_contract_amount,
		case when category_name='日配' then '日配业务'
			when category_name='福利' then '福利业务'
			when category_name='大宗贸易' then '省区大宗'
			when category_name='内购' then '批发内购'
			else category_name
		end as b_type
	from 
		csx_dw.dws_crm_w_a_customer lateral view explode(split(attribute_desc,',')) table_tmp as category_name
	where 
		sdt='20210912'
		and channel_code in ('1','7','9')
		-- and regexp_replace(substr(sign_time,1,10),'-','')>='20210501'
		-- limit 100
	) a 
	left join
		(
		select
			customer_no,case when business_type_name='城市服务商' then '日配业务' else business_type_name end as b_type_n,
			sum(sales_value) as sales_value
		from 
			csx_dw.dws_sale_r_d_detail
		where 
			sdt between '20210101' and '20210912'
			and channel_code in ('1','7','9') 
		group by
			customer_no,case when business_type_name='城市服务商' then '日配业务' else business_type_name end
		) b on b.customer_no=a.customer_no and b.b_type_n=a.b_type
	left join
		(
		select
			customer_no,case when business_type_name='城市服务商' then '日配业务' else business_type_name end as b_type_n,
			min(sdt) as min_sdt,
			sum(sales_value) as sales_value
		from 
			csx_dw.dws_sale_r_d_detail
		where 
			sdt between '20190101' and '20210912'
			and channel_code in ('1','7','9') 
		group by
			customer_no,case when business_type_name='城市服务商' then '日配业务' else business_type_name end
		having
			min(sdt)>='20210101'
		) c on c.customer_no=a.customer_no and c.b_type_n=a.b_type
group by 
	a.sales_region_name,a.sales_province_name,a.city_group_name,a.b_type,a.business_type_name
		
;

--================================================================================================
select
	a.sales_region_name,a.sales_province_name,a.city_group_name,a.b_type,a.business_type_name,
	count(distinct a.customer_no) as cnt1,
	sum(b.sales_value)as sales_value,
	count(distinct case when sign_date>='20210901' then a.customer_no else null end) as cnt2,
	sum(case when sign_date>='20210901' then a.avg_amount else 0 end) cnt3,
	count(distinct c.customer_no )cnt4,
	sum(c.sales_value )cnt5
from
	(
	select 
		customer_no,sales_region_name,sales_province_name,city_group_name,regexp_replace(substr(sign_time,1,10),'-','') as sign_date,business_type_name,
		estimate_contract_amount/count(customer_no) over(partition by customer_no) as avg_amount,estimate_contract_amount,
		case when category_name='日配' then '日配业务'
			when category_name='福利' then '福利业务'
			when category_name='大宗贸易' then '省区大宗'
			when category_name='内购' then '批发内购'
			else category_name
		end as b_type
	from 
		csx_dw.dws_crm_w_a_customer lateral view explode(split(attribute_desc,',')) table_tmp as category_name
	where 
		sdt='20210912'
		and channel_code in ('1','7','9')
		-- and regexp_replace(substr(sign_time,1,10),'-','')>='20210501'
		-- limit 100
	) a 
	left join
		(
		select
			customer_no,case when business_type_name='城市服务商' then '日配业务' else business_type_name end as b_type_n,
			sum(sales_value) as sales_value
		from 
			csx_dw.dws_sale_r_d_detail
		where 
			sdt between '20210901' and '20210912'
			and channel_code in ('1','7','9') 
		group by
			customer_no,case when business_type_name='城市服务商' then '日配业务' else business_type_name end
		) b on b.customer_no=a.customer_no and b.b_type_n=a.b_type
	left join
		(
		select
			customer_no,case when business_type_name='城市服务商' then '日配业务' else business_type_name end as b_type_n,
			min(sdt) as min_sdt,
			sum(sales_value) as sales_value
		from 
			csx_dw.dws_sale_r_d_detail
		where 
			sdt between '20190101' and '20210912'
			and channel_code in ('1','7','9') 
		group by
			customer_no,case when business_type_name='城市服务商' then '日配业务' else business_type_name end
		having
			min(sdt)>='20210901'
		) c on c.customer_no=a.customer_no and c.b_type_n=a.b_type
group by 
	a.sales_region_name,a.sales_province_name,a.city_group_name,a.b_type,a.business_type_name

