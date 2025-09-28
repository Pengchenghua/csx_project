--6、合同管理-客户合同签订情况
--6.1 6月新签客户清单
select sales_province_name,channel_name,customer_no,customer_name,
  attribute_desc,dev_source_name,cooperation_mode_name,
  work_no,sales_name,first_supervisor_work_no,first_supervisor_name,
  regexp_replace(split(first_sign_time, ' ')[0], '-', '') as first_sign_date,
  regexp_replace(split(sign_time, ' ')[0], '-', '') as sign_date,
  estimate_contract_amount*10000 estimate_contract_amount
from csx_dw.dws_crm_w_a_customer
where sdt='current'
and channel_code in('1','7','9')
and sales_province_name='重庆市'
and regexp_replace(split(sign_time, ' ')[0], '-', '')>='20210601';

--6.2 逾期金额前5客户名称 认领口径=逾期金额-认领未核销金额
select
  a.province_name,a.customer_no,b.customer_name,a.company_code,a.payment_name,b.attribute_desc,b.work_no,b.sales_name,
  a.receivable_amount,a.over_amt,a.bad_debt_amount,a.max_over_days,
  b.estimate_contract_amount,b.sign_date,a.rno1
from
(
  select province_name,customer_no,customer_name,company_code,payment_name,receivable_amount,over_amt-(claim_amount-payment_amount_1) as over_amt,bad_debt_amount,max_over_days,
    rank() over (partition by province_name order by over_amt-(claim_amount-payment_amount_1) desc ) as rno1
  from csx_dw.report_sss_r_d_cust_receivable_amount
  where sdt=regexp_replace(date_sub(current_date, 1), '-', '')
  and province_name='重庆市'
  and smonth='小计'
)a
left join 
(
  select sales_province_name,channel_name,customer_no,customer_name,
    attribute_desc,dev_source_name,cooperation_mode_name,
    work_no,sales_name,first_supervisor_work_no,first_supervisor_name,
    regexp_replace(split(first_sign_time, ' ')[0], '-', '') as first_sign_date,
    regexp_replace(split(sign_time, ' ')[0], '-', '') as sign_date,
    estimate_contract_amount*10000 estimate_contract_amount
  from csx_dw.dws_crm_w_a_customer
  where sdt='current'
  --and channel_code in('1','7','9')
  and sales_province_name='重庆市'
)b on b.customer_no=a.customer_no
where a.rno1<=5;

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
  and province_name='重庆市'
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
  --and channel_code in('1','7','9')
  and sales_province_name='重庆市'
)b on b.customer_no=a.customer_no
where a.rno1<=5 or a.rno2<=5 or a.rno3<=5;

--8、逾期管理-长期未回款仍继续履约客户
select
  a.province_name,a.customer_no,b.customer_name,a.company_code,a.payment_name,b.attribute_desc,b.work_no,b.sales_name,
  a.receivable_amount,a.over_amt,a.bad_debt_amount,a.max_over_days,
  b.estimate_contract_amount,b.contact_person,b.contact_phone,b.sign_date,
  c.last_order_date,d.max_claim_date,d.diff_days
from
--逾期客户 认领口径=逾期金额-认领未核销金额
(
  select province_name,customer_no,customer_name,company_code,payment_name,receivable_amount,over_amt-(claim_amount-payment_amount_1) as over_amt,bad_debt_amount,max_over_days
  from csx_dw.report_sss_r_d_cust_receivable_amount
  where sdt=regexp_replace(date_sub(current_date, 1), '-', '')
  and province_name='重庆市'
  and smonth='小计'
  and channel_code<>'2' --剔除商超
  and (customer_name not like '%内%购%' and customer_name not like '%临保%')  --剔除内购  
  and over_amt-(claim_amount-payment_amount_1)>0
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
  --and channel_code in('1','7','9')
  and sales_province_name='重庆市'
)b on b.customer_no=a.customer_no
--6月以后有销售
left join 
(
  select customer_no,first_order_date,last_order_date --最后销售日期
  from csx_dw.dws_crm_w_a_customer_active
  where sdt='current'
  and last_order_date>='20210601'
  
)c on c.customer_no=a.customer_no
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
  from csx_dw.dwd_sss_r_d_money_back -- sdt以过账日期分区，只看20200601及以后的，该表计算回款只计算已核销金额
  where ((sdt>='20200601' and sdt<=regexp_replace(date_sub(current_date, 1), '-', '')) 
  or (sdt='19990101' and regexp_replace(substr(posting_time,1,10),'-','')>='20200601' and regexp_replace(substr(posting_time,1,10),'-','')<=regexp_replace(date_sub(current_date, 1), '-', '')))
  and regexp_replace(substr(update_time,1,10),'-','')<=regexp_replace(date_sub(current_date, 1), '-', '')  --回款以过账日期为准，但每次已核销金额发生变化更新日期都会变化，此表无法查历史回款已核销金额
  and (paid_amount<>'0' or residual_amount<>'0') --剔除补救单和对应原单
  group by customer_code,company_code
)d on d.customer_no=a.customer_no and d.company_code=a.company_code
where c.customer_no is not null 
and d.diff_days>90;


--9、返利客户稽核-客户调价返利情况
select a.province_name,a.customer_no,b.customer_name,b.channel_name,b.attribute_desc,b.work_no,b.sales_name,b.sign_date,
a.sales_value,a.rno1
from
(
  select province_name,customer_no,customer_name,
    sum(sales_value) sales_value,   --含税销售额
    rank() over (partition by province_name order by sum(sales_value) desc ) as rno1 --调价返利金额排名
  from csx_dw.dws_sale_r_d_detail
  where sdt>=regexp_replace(date_sub(current_date, 91), '-', '')  --90天内
  and sales_type='fanli'
  and province_name='重庆市'
  group by province_name,customer_no,customer_name
  )a
left join 
(
  select sales_province_name,channel_name,customer_no,customer_name,
    attribute_desc,dev_source_name,cooperation_mode_name,
    work_no,sales_name,first_supervisor_work_no,first_supervisor_name,
    regexp_replace(split(first_sign_time, ' ')[0], '-', '') as first_sign_date,
    regexp_replace(split(sign_time, ' ')[0], '-', '') as sign_date,
    estimate_contract_amount*10000 estimate_contract_amount
  from csx_dw.dws_crm_w_a_customer
  where sdt='current'
  --and channel_code in('1','7','9')
  and sales_province_name='重庆市'
)b on b.customer_no=a.customer_no
where a.rno1<=5;























