--签呈一 四川 116401
--
set i_sdate_11 =regexp_replace(date_sub(current_date,1),'-','');  --昨日
set i_sdate_12 =regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),0),'-','');  --昨日月1日
	
set i_sdate_22 =regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-1),'-','');	  --上月1日				
set i_sdate_23 =regexp_replace(last_day(add_months(date_sub(current_date,1),-1)),'-','');  --上月最后一日
set i_sdate_24 =regexp_replace(last_day(add_months(date_sub(current_date,1),-2)),'-','');  --上上月最后一日
set i_sdate_25 =regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-2),'-','');	  --上上月1日

set i_sdate_23_1 =last_day(add_months(date_sub(current_date,1),-1));  --上月最后一日
set i_sdate_24_1 =last_day(add_months(date_sub(current_date,1),-2));  --上上月最后一日

-- ('123415',
--'123086','123629','124821') 上海丁芳 直送单剔除  不计算毛利提成
drop table csx_tmp.temp_dali_cust_01;
create temporary table csx_tmp.temp_dali_cust_01
as
select a.smonth,
	a.province_name dist,
	a.customer_no,
	b.customer_name cust_name,
	b.sign_date,
	b.work_no,
	b.sales_name,
	sum(a.sales_value) sales_value,
	sum(a.profit) profit,
	sum(a.profit)/sum(a.sales_value) prorate,
	sum(a.front_profit) as fnl_profit,
	sum(a.front_profit)/sum(a.sales_value) as fnl_prorate,
	round(sum(a.sales_value)*0.02,2) salery_sales,
	round(if((sum(a.profit)/sum(a.sales_value)-0.15)>=0 and a.customer_no not in('104179','112092'),(sum(a.profit)/sum(a.sales_value)-0.15),0)*sum(a.sales_value)*0.5,2) salery_fnl,
	round(sum(a.sales_value)*0.02,2)+
	round(if((sum(a.profit)/sum(a.sales_value)-0.15)>=0 and a.customer_no not in('104179','112092'),(sum(a.profit)/sum(a.sales_value)-0.15),0)*sum(a.sales_value)*0.5,2) salery_cal,
	--round(sum(if((a.fnl_prorate-0.1)>=0 and a.customer_no not in('104179','112092'),(a.fnl_prorate-0.1),0)*a.sales_value*0.5),2) salery_fnl,
	--round(sum(a.sales_value)*0.02,2)+round(sum(if((a.fnl_prorate-0.1)>=0 and a.customer_no not in('104179','112092'),(a.fnl_prorate-0.1),0)*a.sales_value*0.5),2) salery_cal,
	sum(c.unpaid_amount)unpaid_amount_by,
	sum(d.unpaid_amount)unpaid_amount_sy,
	max(e.payment_amount_by) payment_amount_by,
	--sum(a.sales_value)-coalesce(sum(e.payment_amount_by),0) as unpay_amount_by,
	max(e.payment_amount_sy) payment_amount_sy
	--sum(a.sales_value)-coalesce(sum(e.payment_amount_sy),0) as unpay_amount_sy
from 
	(select substr(sdt,1,6) smonth,order_no,province_name,customer_no,
		sum(sales_value)sales_value,
		sum(profit) profit,sum(profit)/sum(sales_value) prorate,
		sum(front_profit) as front_profit,
		sum(front_profit)/sum(sales_value) as fnl_prorate
	from csx_dw.dws_sale_r_d_detail
	where sdt>='20200901' and sdt<${hiveconf:i_sdate_12}  --上上月1日 --昨日月1日

	group by substr(sdt,1,6),order_no,province_name,customer_no
	)a
join 
	(select distinct substr(sdt,1,6) smonth,customer_no,customer_name,
	case when customer_no='104179' then '-' 
		 when customer_no='112092' then '-'
		 else work_no end as work_no,
	case when customer_no='104179' then '徐桂莹' 
		 when customer_no='112092' then '蒋光平'
		 else sales_name end as sales_name,
	regexp_replace(split(first_sign_time, ' ')[0], '-', '') as sign_date
	from csx_dw.dws_crm_w_a_customer 
	where sdt=${hiveconf:i_sdate_23}  --上月最后1日
	and (channel_code='9' or customer_no in  ('123629'))
	--'122221' 已更正为业务代理人 '116401' 202205月开始不算业务代理人改为大客户 127155
	) b on b.customer_no=a.customer_no 
left join 
	(select 
		source_bill_no as order_no,	-- 来源单号
		customer_code as customer_no,	-- 客户编码
		company_code,	-- 签约公司编码
		happen_date,	-- 发生时间		
		overdue_date,	-- 逾期时间	
		source_statement_amount,	-- 源单据对账金额
		money_back_status,	-- 回款状态
		paid_amount,	-- 已回款金额
		unpaid_amount,	-- 未回款金额
		account_period_code,	--账期编码 
		account_period_name,	--账期名称 
		account_period_val	--账期值
	from csx_dw.dwd_sss_r_d_sale_order_statement_detail_20201116  --销售单对账
	--from csx_dw.dwd_sss_r_d_sale_order_statement_detail_20201019  --销售单对账
	where sdt=${hiveconf:i_sdate_23}
	and date(happen_date)<=${hiveconf:i_sdate_23_1}
	)c on c.order_no=a.order_no
left join 
	(select 
		source_bill_no as order_no,	-- 来源单号
		customer_code as customer_no,	-- 客户编码
		company_code,	-- 签约公司编码
		happen_date,	-- 发生时间		
		overdue_date,	-- 逾期时间	
		source_statement_amount,	-- 源单据对账金额
		money_back_status,	-- 回款状态
		paid_amount,	-- 已回款金额
		unpaid_amount,	-- 未回款金额
		account_period_code,	--账期编码 
		account_period_name,	--账期名称 
		account_period_val	--账期值
	from csx_dw.dwd_sss_r_d_sale_order_statement_detail_20201116  --销售单对账
	--from csx_dw.dwd_sss_r_d_sale_order_statement_detail_20201019  --销售单对账
	where sdt=${hiveconf:i_sdate_24}
	and date(happen_date)<=${hiveconf:i_sdate_24_1}
	)d on d.order_no=a.order_no	
left join 
	(
	--核销流水明细表中已核销金额
	select customer_code,regexp_replace(substr(happen_date,1,7),'-','') smonth,
--close_bill_no,
	sum(case when regexp_replace(substr(paid_time,1,10),'-','')>=${hiveconf:i_sdate_22} 
			  and regexp_replace(substr(paid_time,1,10),'-','')<= ${hiveconf:i_sdate_23}
			 then payment_amount end ) payment_amount_by,
	sum(case when regexp_replace(substr(paid_time,1,10),'-','')>=${hiveconf:i_sdate_25} 
			  and regexp_replace(substr(paid_time,1,10),'-','')<= ${hiveconf:i_sdate_24}
			 then payment_amount end ) payment_amount_sy,			 
		sum(payment_amount) payment_amount
	from
		csx_dw.dwd_sss_r_d_close_bill_account_record_20200908 a
		
	/*left join (select order_no
	from csx_dw.dws_sale_r_d_detail
	where 
	sdt>='20200901' and sdt<${hiveconf:i_sdate_12}
     and 	customer_no in  ('123415','123086','123629','124821') and logistics_mode_code='1'  --直送
	group by order_no
	)b on a.close_bill_no=b.order_no	
	
	where b.order_no is null */
	where (regexp_replace(substr(happen_date,1,10),'-','')<=${hiveconf:i_sdate_23} or happen_date='' or happen_date is NULL)
	and regexp_replace(substr(paid_time,1,10),'-','') <=${hiveconf:i_sdate_23} 
	--and regexp_replace(substr(posting_time,1,10),'-','') <=${hiveconf:i_sdate_23}
	and is_deleted ='0'
	--and money_back_id<>'0' --回款关联ID为0是微信支付、-1是退货系统核销
	group by customer_code,regexp_replace(substr(happen_date,1,7),'-','')
	--close_bill_no
	)e on e.customer_code=a.customer_no and e.smonth=a.smonth --e.close_bill_no=a.order_no	
group by a.smonth,a.province_name,a.customer_no,b.customer_name,b.sign_date,b.work_no,b.sales_name;


--202209 丁芳不算毛利提成
------提成计算有问题待核查
--结果1：客户提成
select a.*,
	b.paid_amount,
	b.residual_amount,
	e.residual_amount residual_amount_all,
	if(a.sign_date<='20200930',1.5,1) as sign_ratio,
	
    if((unpaid_amount_by is NULL or unpaid_amount_by=0)  and unpaid_amount_sy is NULL,0,
          if(unpaid_amount_by>=sales_value,0,
    	        if(unpaid_amount_sy is NULL,
				(1-if(coalesce(unpaid_amount_by,0)/sales_value>1,1,coalesce(unpaid_amount_by,0)/sales_value))*salery_cal,--unpaid_amount_sy is null,都取的这个数据
    			      if(coalesce(unpaid_amount_sy,0)>=coalesce(unpaid_amount_by,0),
    				        if(coalesce(unpaid_amount_sy,0)-coalesce(unpaid_amount_by,0)>sales_value,sales_value,
							      coalesce(unpaid_amount_sy,0)-coalesce(unpaid_amount_by,0)),0)/sales_value*salery_cal
								  )))
								  *if(a.sign_date<='20200930',1.5,1) salery_cal_1																						
from csx_tmp.temp_dali_cust_01 a
left join
( -- 获取本月客户回款金额
  select
    customer_code, -- 客户编码
    sum(claim_amount) as claim_amount,	--回款金额（未使用，含补救单）
    sum(paid_amount) as paid_amount,	--回款已核销金额
    sum(residual_amount) as residual_amount	--回款未核销金额
  from csx_dw.dwd_sss_r_d_money_back -- sdt以过账日期分区，只看20200601及以后的，该表计算回款只计算已核销金额
  where (sdt>=${hiveconf:i_sdate_22} and sdt<=${hiveconf:i_sdate_23}) 
  or (sdt='19990101' and regexp_replace(substr(posting_time,1,10),'-','')>=${hiveconf:i_sdate_22} and regexp_replace(substr(posting_time,1,10),'-','')<=${hiveconf:i_sdate_23})
  and regexp_replace(substr(update_time,1,10),'-','')<=${hiveconf:i_sdate_23}  --回款以过账日期为准，但每次已核销金额发生变化更新日期都会变化，此表无法查历史回款已核销金额
  and (paid_amount<>'0' or residual_amount<>'0') --剔除补救单和对应原单
  group by customer_code
) b on a.customer_no = b.customer_code
--历史回款金额
left join
( 
  select
    customer_code, -- 客户编码
    sum(claim_amount) as claim_amount,	--回款金额（未使用，含补救单）
    sum(paid_amount) as paid_amount,	--回款已核销金额
    sum(residual_amount) as residual_amount	--回款未核销金额
  from csx_dw.dwd_sss_r_d_money_back -- sdt以过账日期分区，只看20200601及以后的，该表计算回款只计算已核销金额
  where (sdt>='20200601' and sdt<=${hiveconf:i_sdate_23}) 
  or (sdt='19990101' and regexp_replace(substr(posting_time,1,10),'-','')>='20200601' and sdt<=${hiveconf:i_sdate_23})
  and regexp_replace(substr(update_time,1,10),'-','')<=${hiveconf:i_sdate_23}  --回款以过账日期为准，但每次已核销金额发生变化更新日期都会变化，此表无法查历史回款已核销金额
  and (paid_amount<>'0' or residual_amount<>'0') --剔除补救单和对应原单 
  group by customer_code
) e on a.customer_no = e.customer_code;


--结果2：四川签呈客户('104179','112092')，按照当月回款额*2%提成
--202103开始104179客户不再计算
--202201-04 计算 116401 202205月确认是否计算 不计算
select a.smonth,a.customer_no,b.customer_name,b.work_no,b.sales_name,
	claim_amount,paid_amount,residual_amount,
	paid_amount*0.02 as salery_cal_1
from 
	(select 
		regexp_replace(substr(posting_time,1,7),'-','') smonth,	
		customer_code customer_no, -- 客户编码
		sum(claim_amount) as claim_amount,	--回款金额（未使用，含补救单）
		sum(paid_amount) as paid_amount,	--回款已核销金额
		sum(residual_amount) as residual_amount	--回款未核销金额
	from csx_dw.dwd_sss_r_d_money_back -- sdt以过账日期分区，只看20200601及以后的，该表计算回款只计算已核销金额
	where customer_code  in ('112092','116401')
	--and ((sdt>='20200801' and sdt<='20201031') 
	--or (sdt='19990101' and regexp_replace(substr(posting_time,1,10),'-','')>='20200801' and regexp_replace(substr(posting_time,1,10),'-','')<='20201031'))
	and ((sdt>=${hiveconf:i_sdate_22} and sdt<=${hiveconf:i_sdate_23}) 	
	or (sdt='19990101' and regexp_replace(substr(posting_time,1,10),'-','')>=${hiveconf:i_sdate_22} and regexp_replace(substr(posting_time,1,10),'-','')<=${hiveconf:i_sdate_23}))  
	group by regexp_replace(substr(posting_time,1,7),'-',''),customer_code
	)a
left join 
	(select distinct substr(sdt,1,6) smonth,customer_no,customer_name,
	case when customer_no='104179' then '-' 
		 when customer_no='112092' then '-'
		 else work_no end as work_no,
	case when customer_no='104179' then '徐桂莹' 
		 when customer_no='112092' then '蒋光平'
		 else sales_name end as sales_name,
	regexp_replace(split(first_sign_time, ' ')[0], '-', '') as sign_date
	from csx_dw.dws_crm_w_a_customer 
	--where sdt =('20201031')  --上月最后1日
	where sdt=${hiveconf:i_sdate_23}  --上月最后1日
	and customer_no in ('112092','116401')
	) b on b.customer_no=a.customer_no;

