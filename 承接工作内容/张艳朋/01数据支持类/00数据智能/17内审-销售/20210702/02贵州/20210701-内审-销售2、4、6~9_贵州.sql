--==============================================================================================
--1.2 客户签约后超3个月仍未履约客户（1-4月至今未履约）

select 
	a.sales_province_name,
	a.city_group_name,
	a.customer_no,
	a.customer_name,
	a.sign_date,
	a.work_no,
	a.sales_name,
	a.first_supervisor_name,
	a.first_category_name,
	a.second_category_name,
	a.third_category_name,
	a.attribute_desc,
	a.cooperation_mode_name,
	a.dev_source_name,
	a.estimate_contract_amount*10000 as estimate_contract_amount
from 
	(
	select
		customer_no,customer_name,regexp_replace(substr(sign_time,1,10),'-','') as sign_date,sales_province_name,city_group_name,
		work_no,sales_name,first_supervisor_name,
		first_category_name,second_category_name,third_category_name,attribute_desc,cooperation_mode_name,dev_source_name,estimate_contract_amount
	from  
		csx_dw.dws_crm_w_a_customer 
	where 
		sdt='current'
		and regexp_replace(substr(sign_time,1,10),'-','')>='20210101'
		and regexp_replace(substr(sign_time,1,10),'-','')<='20210430'
		and sales_province_name='贵州省'
	) a 
	left join 
		(
		select
			customer_no
		from  
			csx_dw.dws_sale_r_d_detail
		where 
			sdt>='20210101'
			and sdt<='20210630'
		group by 
			customer_no
		) b on a.customer_no=b.customer_no
where 
	b.customer_no is null
	
	
	
--==============================================================================================	
--1.4 连续3个月无新成交客户的销售人员；

select 
	a.province_name,
	a.work_no,
	a.sales_name,
	a.position,
	b.begin_date,
	floor(months_between('2021-07-01',from_unixtime(unix_timestamp(b.begin_date,'yyyyMMdd'),'yyyy-MM-dd'))) as date_dif,
	a.sales_supervisor_name,
	a.org_name,
	d.channel_name
from 
	(
	select
		sales_id,work_no,sales_name,position,sales_supervisor_name,province_name,org_name
	from  
		csx_dw.dws_uc_w_a_sale_org_m 
	where 
		sdt='20210630'
		and position = 'SALES'
		and province_name='贵州省'
		and org_name !='商超'
	) a 
	left join
		(
		select
			employee_code,employee_name,begin_date,end_date,emp_status
		from
			csx_dw.dws_basic_w_a_employee_org_m
		where
			sdt='20210630'
		) b on b.employee_code=a.work_no	
	left join 
		(
		select
			sales_id,sales_name,first_order_date,last_order_date
		from  
			csx_dw.dws_crm_w_a_customer_active
		where 
			sdt='20210630'
			and first_order_date>='20210401'
			and first_order_date<='20210630'
		group by 
			sales_id,sales_name,first_order_date,last_order_date
		) c on a.sales_id=c.sales_id
	left join 
		(
		select
			id,leader_id,department_id,user_number,name,status,user_position,prov_name,
			case when channel='1' then '大客户'
				when channel='2' then '商超'
				when channel='4' then '大宗' 
				when channel='5' then '供应链(食百)' 
				when channel='6' then '供应链(生鲜)'
				when channel='7' then '企业购'
				when channel='8' then '其他'
				when channel='9' then '业务代理'
				else '其他' end as channel_name
		from
			csx_dw.dws_basic_w_a_user
		where
			sdt='20210630'
			-- and prov_name='四川省'
		) d on d.id=a.sales_id
where 
	c.sales_id is null;
	

--==============================================================================================	
--1.4.2 连续3个月无新成交客户的销售人员的拜访情况；

select 
	a.province_name,
	a.work_no,
	a.sales_name,
	a.position,
	b.begin_date,
	floor(months_between('2021-07-01',from_unixtime(unix_timestamp(b.begin_date,'yyyyMMdd'),'yyyy-MM-dd'))) as date_dif,
	a.sales_supervisor_name,
	a.org_name,
	d.channel_name,
	e.*
from 
	(
	select
		sales_id,work_no,sales_name,position,sales_supervisor_name,province_name,org_name
	from  
		csx_dw.dws_uc_w_a_sale_org_m 
	where 
		sdt='20210630'
		and position = 'SALES'
		and province_name='贵州省'
		and org_name !='商超'
	) a 
	left join
		(
		select
			employee_code,employee_name,begin_date,end_date,emp_status
		from
			csx_dw.dws_basic_w_a_employee_org_m
		where
			sdt='20210630'
		) b on b.employee_code=a.work_no	
	left join 
		(
		select
			sales_id,sales_name,first_order_date,last_order_date
		from  
			csx_dw.dws_crm_w_a_customer_active
		where 
			sdt='20210630'
			and first_order_date>='20210401'
			and first_order_date<='20210630'
		group by 
			sales_id,sales_name,first_order_date,last_order_date
		) c on a.sales_id=c.sales_id
	left join 
		(
		select
			id,leader_id,department_id,user_number,name,status,user_position,prov_name,
			case when channel='1' then '大客户'
				when channel='2' then '商超'
				when channel='4' then '大宗' 
				when channel='5' then '供应链(食百)' 
				when channel='6' then '供应链(生鲜)'
				when channel='7' then '企业购'
				when channel='8' then '其他'
				when channel='9' then '业务代理'
				else '其他' end as channel_name
		from
			csx_dw.dws_basic_w_a_user
		where
			sdt='20210630'
			-- and prov_name='四川省'
		) d on d.id=a.sales_id
	left join 
		( 
		select 
			*
		from
			csx_tmp.tmp_customer_visit_record_1 
		where 
			province_name = '贵州省'
		) e on a.sales_id = e.visit_person_id
where 
	c.sales_id is null;	


--==============================================================================================	
--6、合同管理-客户合同签订情况
--6.1 6月新签客户清单

select 
	sales_province_name,channel_name,customer_no,customer_name,
	attribute_desc,dev_source_name,cooperation_mode_name,
	work_no,sales_name,first_supervisor_work_no,first_supervisor_name,
	regexp_replace(split(first_sign_time, ' ')[0], '-', '') as first_sign_date,
	regexp_replace(split(sign_time, ' ')[0], '-', '') as sign_date,
	estimate_contract_amount*10000 estimate_contract_amount
from 
	csx_dw.dws_crm_w_a_customer
where 
	sdt='current'
	and channel_code in('1','7','9')
	and sales_province_name='贵州省'
	and regexp_replace(split(sign_time, ' ')[0], '-', '')>='20210401';


--==============================================================================================	
--6.2 逾期金额前5客户名称 认领口径=逾期金额-认领未核销金额

select
  a.province_name,a.customer_no,b.customer_name,a.company_code,a.payment_name,b.attribute_desc,b.work_no,b.sales_name,
  a.receivable_amount,a.over_amt,a.bad_debt_amount,a.max_over_days,
  b.estimate_contract_amount,b.sign_date,a.rno1
from
	(
	select 
		province_name,customer_no,customer_name,company_code,payment_name,receivable_amount,over_amt-(claim_amount-payment_amount_1) as over_amt,bad_debt_amount,max_over_days,
		rank() over (partition by province_name order by over_amt-(claim_amount-payment_amount_1) desc ) as rno1
	from 
		csx_dw.report_sss_r_d_cust_receivable_amount
	where 
		sdt=regexp_replace(date_sub(current_date, 1), '-', '')
		and province_name='贵州省' and channel_code in('1','7','9')
		and smonth='小计'
	)a
	left join 
		(
		select 
			sales_province_name,channel_name,customer_no,customer_name,
			attribute_desc,dev_source_name,cooperation_mode_name,
			work_no,sales_name,first_supervisor_work_no,first_supervisor_name,
			regexp_replace(split(first_sign_time, ' ')[0], '-', '') as first_sign_date,
			regexp_replace(split(sign_time, ' ')[0], '-', '') as sign_date,
			estimate_contract_amount*10000 estimate_contract_amount
		from 
			csx_dw.dws_crm_w_a_customer
		where 
			sdt='current'
			and channel_code in('1','7','9')
			and sales_province_name='贵州省'
		) b on b.customer_no=a.customer_no
where 
	a.rno1<=5;

--7、逾期管理-逾期客户稽核 认领口径=逾期金额-认领未核销金额
select
  a.province_name,a.customer_no,b.customer_name,a.company_code,a.payment_name,b.attribute_desc,b.work_no,b.sales_name,
  a.receivable_amount,a.over_amt,a.bad_debt_amount,a.max_over_days,
  b.estimate_contract_amount,b.contact_person,b.contact_phone,b.sign_date,
  a.rno1,a.rno2,a.rno3
from
(
  select province_name,customer_no,customer_name,company_code,payment_name,receivable_amount,over_amt-(claim_amount-payment_amount_1) as over_amt,bad_debt_amount,max_over_days,
    rank() over (partition by province_name order by over_amt-(claim_amount-payment_amount_1) desc ) as rno1, --逾期金额排名
	rank() over (partition by province_name order by (over_amt-(claim_amount-payment_amount_1))/receivable_amount desc ) as rno2, --逾期率排名
	rank() over (partition by province_name order by max_over_days desc ) as rno3 --逾期天数排名
  from csx_dw.report_sss_r_d_cust_receivable_amount
  where sdt=regexp_replace(date_sub(current_date, 1), '-', '')
  and province_name='四川省' and channel_code in('1','7','9')
  and smonth='小计'
  and channel_code<>'2' --剔除商超
  and (customer_name not like '%内%购%' and customer_name not like '%临保%')  --剔除内购
  and over_amt>0
  and receivable_amount>1
)a
left join 
(
  select sales_province_name,channel_name,customer_no,customer_name,
    attribute_desc,dev_source_name,cooperation_mode_name,
    work_no,sales_name,first_supervisor_work_no,first_supervisor_name,
    regexp_replace(split(first_sign_time, ' ')[0], '-', '') as first_sign_date,
    regexp_replace(split(sign_time, ' ')[0], '-', '') as sign_date,
    estimate_contract_amount*10000 estimate_contract_amount,contact_person,contact_phone
  from csx_dw.dws_crm_w_a_customer
  where sdt='current'
  and channel_code in('1','7','9')
  and sales_province_name='四川省'
)b on b.customer_no=a.customer_no
where a.rno1<=5 or a.rno2<=5 or a.rno3<=5;


--==============================================================================================
--8、逾期管理-长期未回款仍继续履约客户
select
	a.province_name,a.customer_no,b.customer_name,a.company_code,a.payment_name,b.attribute_desc,b.work_no,b.sales_name,
	a.receivable_amount,a.over_amt,a.bad_debt_amount,a.max_over_days,
	b.estimate_contract_amount,b.contact_person,b.contact_phone,b.sign_date,
	c.last_order_date,d.max_claim_date,d.diff_days
from
--逾期客户 认领口径=逾期金额-认领未核销金额
	(
	select 
		province_name,customer_no,customer_name,company_code,payment_name,receivable_amount,over_amt-(claim_amount-payment_amount_1) as over_amt,bad_debt_amount,max_over_days
	from 
		csx_dw.report_sss_r_d_cust_receivable_amount
	where 
		sdt=regexp_replace(date_sub(current_date, 1), '-', '')
		and province_name='贵州省' and channel_code in('1','7','9')
		and smonth='小计'
		and channel_code<>'2' --剔除商超
		and (customer_name not like '%内%购%' and customer_name not like '%临保%')  --剔除内购  
		and over_amt-(claim_amount-payment_amount_1)>0
		and receivable_amount>1
	)a
	left join 
		(
		select 
			sales_province_name,channel_name,customer_no,customer_name,
			attribute_desc,dev_source_name,cooperation_mode_name,
			work_no,sales_name,first_supervisor_work_no,first_supervisor_name,
			regexp_replace(split(first_sign_time, ' ')[0], '-', '') as first_sign_date,
			regexp_replace(split(sign_time, ' ')[0], '-', '') as sign_date,
			estimate_contract_amount*10000 estimate_contract_amount,contact_person,contact_phone
		from 
			csx_dw.dws_crm_w_a_customer
		where 
			sdt='current'
			and channel_code in('1','7','9')
			and sales_province_name='贵州省'
		) b on b.customer_no=a.customer_no
	--6月以后有销售
	left join 
		(
		select 
			customer_no,first_order_date,last_order_date --最后销售日期
		from 
			csx_dw.dws_crm_w_a_customer_active
		where 
			sdt='current'
			and last_order_date>='20210601'
		) c on c.customer_no=a.customer_no
	--超3个月未回款（认领）
	left join 
		(
		select
			customer_code as customer_no, -- 客户编码
			company_code, -- 公司代码
			--最后认领日期
			max(regexp_replace(substr(posting_time,1,10),'-','')) as max_claim_date,
			--最后认领日期距离昨日天数
			min(datediff(from_unixtime(unix_timestamp(date_sub(current_date, 1),'yyyy-mm-dd'),'yyyy-mm-dd'),from_unixtime(unix_timestamp(substr(posting_time,1,10),'yyyy-mm-dd'),'yyyy-mm-dd'))) diff_days
		from 
			csx_dw.dwd_sss_r_d_money_back -- sdt以过账日期分区，只看20200601及以后的，该表计算回款只计算已核销金额
		where 
			((sdt>='20200601' and sdt<=regexp_replace(date_sub(current_date, 1), '-', '')) 
			or (sdt='19990101' and regexp_replace(substr(posting_time,1,10),'-','')>='20200601' and regexp_replace(substr(posting_time,1,10),'-','')<=regexp_replace(date_sub(current_date, 1), '-', '')))
			and regexp_replace(substr(update_time,1,10),'-','')<=regexp_replace(date_sub(current_date, 1), '-', '')  --回款以过账日期为准，但每次已核销金额发生变化更新日期都会变化，此表无法查历史回款已核销金额
			and (paid_amount<>'0' or residual_amount<>'0') --剔除补救单和对应原单
		group by 
			customer_code,company_code
		) d on d.customer_no=a.customer_no
where 
	c.customer_no is not null 
	and d.diff_days>90;


--==============================================================================================
--9、返利客户稽核-客户调价返利情况

select 
	a.province_name,a.customer_no,b.customer_name,b.channel_name,b.attribute_desc,b.work_no,b.sales_name,b.sign_date,
	a.sales_value,a.rno1
from
	(
	select 
		province_name,customer_no,customer_name,
		sum(sales_value) sales_value,   --含税销售额
		rank() over (partition by province_name order by sum(sales_value) desc ) as rno1 --调价返利金额排名
	from 
		csx_dw.dws_sale_r_d_detail
	where 
		sdt>=regexp_replace(date_sub(current_date, 91), '-', '')  --90天内
		and sales_type='fanli'
		and province_name='贵州省'
	group by 
		province_name,customer_no,customer_name
	)a
	left join 
		(
		select 
			sales_province_name,channel_name,customer_no,customer_name,
			attribute_desc,dev_source_name,cooperation_mode_name,
			work_no,sales_name,first_supervisor_work_no,first_supervisor_name,
			regexp_replace(split(first_sign_time, ' ')[0], '-', '') as first_sign_date,
			regexp_replace(split(sign_time, ' ')[0], '-', '') as sign_date,
			estimate_contract_amount*10000 estimate_contract_amount
		from 
			csx_dw.dws_crm_w_a_customer
		where 
			sdt='current'
			--and channel_code in('1','7','9')
		and sales_province_name='贵州省'
		) b on b.customer_no=a.customer_no
where a.rno1<=5;























