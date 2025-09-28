select
	coalesce(b.province_name,a.province_name) as province_name,
	coalesce(b.city_group_name,a.city_group_name) as city_group_name,
	a.customer_id,
	b.customer_no,
	a.customer_name,
	coalesce(b.sales_name,a.sales_name) as sales_name,
	regexp_replace(substr(a.sign_time,1,10),'-','') as sign_time,
	coalesce(c.normal_first_order_date,'') as normal_first_order_date,
	a.estimate_contract_amount,
	a.contract_cycle,
	a.estimate_contract_amount/a.contract_cycle_type as avg_amount_target,
	coalesce(d.sales_value2,'') as sales_value2,
	coalesce(d.sales_value2/(a.estimate_contract_amount/a.contract_cycle_type),'') as achievement_1,
	coalesce(d.sales_value1,'') as sales_value1,
	if(d.sales_value1 is null and d.sales_value2 is null,'',coalesce(d.sales_value1,0)+coalesce(d.sales_value2,0)) as total_sales_value,
	coalesce(if(d.sales_value1 is null and d.sales_value2 is null,'',coalesce(d.sales_value1,0)+coalesce(d.sales_value2,0))/
	(a.estimate_contract_amount/a.contract_cycle_type*2),'') as total_achievement,
	d.sales_value_total,
	d.sales_value_total_cs,
	row_number()over(partition by coalesce(coalesce(b.province_name,a.province_name),b.city_group_name,a.city_group_name),coalesce(b.sales_name,a.sales_name)order by estimate_contract_amount desc) as rn
from
	(
	select
		customer_id,business_number,customer_name,attribute,attribute_name,sales_id,work_no,sales_name,contract_cycle,
		business_stage,stage_desc,status,estimate_contract_amount,sign_time,city_group_code,city_group_name,province_code,province_name,region_code,region_name,
		case when contract_cycle like '%个月' then regexp_replace(contract_cycle,'个月','')
			when contract_cycle like '%年' then regexp_replace(contract_cycle,'年','')*12
			when contract_cycle ='30日' or contract_cycle ='31日' then 1
			when contract_cycle ='365日' then 12
			else '其他'
		end as contract_cycle_type	
	from
		csx_dw.ads_crm_r_m_business_customer
	where
		month='202111'
		and status=1
		and business_stage=5
		and attribute='1' -- 新客户属性（商机属性） 1：日配客户 2：福利客户 3：大宗贸易 4：M端 5：BBC 6：内购
		and regexp_replace(substr(sign_time,1,10),'-','') between '20211101' and '20211130'
	) a 
	left join
		(
		select
			customer_id,customer_no,customer_name,sales_id,work_no,sales_name,province_code,province_name,sales_region_code,sales_region_name,city_group_code,city_group_name
		from
			csx_dw.dws_crm_w_a_customer
		where
			sdt='current'
		)b on b.customer_id=a.customer_id
	left join
		(
		select
			customer_no,customer_name,normal_first_order_date
		from 
			csx_dw.dws_crm_w_a_customer_active
		where 
			sdt = 'current' 
		)c on c.customer_no=b.customer_no
	left join
		(
		select 
			customer_no,
			sum(case when sdt between '20211101' and '20211130' and business_type_code='1' then sales_value else null end)/10000 as sales_value1,
			sum(case when sdt between '20211201' and '20211231' and business_type_code='1' then sales_value else null end)/10000 as sales_value2,
			sum(sales_value)/10000 as sales_value_total,
			sum(case when business_type_code='4' then sales_value else null end)/10000 as sales_value_total_cs
		from 
			csx_dw.dws_sale_r_d_detail 
		where 
			sdt between '20211101' and '20211216'
			and channel_code in ('1','7','9')
			and business_type_code in ('1','4') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
		group by 
			customer_no
		)d on d.customer_no=b.customer_no
;

--==============================================================================================================================================================================

select
	case when contract_cycle like '%个月' then regexp_replace(contract_cycle,'个月','')
		when contract_cycle like '%年' then regexp_replace(contract_cycle,'年','')*12
		when contract_cycle ='30日' or contract_cycle ='31日' then 1
		when contract_cycle ='365日' then 12
		else '其他'
	end as contract_cycle_type		
from
	csx_dw.ads_crm_r_m_business_customer
where
	month='202111'
	and status=1
	and business_stage=5
	and attribute='1' -- 新客户属性（商机属性） 1：日配客户 2：福利客户 3：大宗贸易 4：M端 5：BBC 6：内购
	and regexp_replace(substr(sign_time,1,10),'-','') between '20211101' and '20211130'

		
	