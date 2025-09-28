--方案实施日期：2020年7月1日起；
--合伙人提成：根据回款金额*0.2%，以回款为基础，按月核算奖励；
--销售员离职、转岗后，此奖励方案不予核算；同时，如城市服务商持续履约，则由后续对接销售员予以服务并奖励核算；
--城市服务商所开发客户转自营、自营客户转城市服务商配送，均不予核算奖励；


---方案实施日期：2022年7月1日起；
--合伙人提成：根据回款金额*0.06%，以回款为基础，按月核算奖励；



-- 昨日、昨日月1日，上月1日，上月最后一日,上上月最后一日
--select ${hiveconf:i_sdate_11},${hiveconf:i_sdate_12},${hiveconf:i_sdate_22},${hiveconf:i_sdate_23},${hiveconf:i_sdate_24};

set i_sdate_11 =regexp_replace(date_sub(current_date,1),'-','');  --昨日
set i_sdate_12 =regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),0),'-','');  --昨日月1日
	
set i_sdate_22 =regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-1),'-','');	  --上月1日				
set i_sdate_23 =regexp_replace(last_day(add_months(date_sub(current_date,1),-1)),'-','');  --上月最后一日
set i_sdate_24 =regexp_replace(last_day(add_months(date_sub(current_date,1),-2)),'-','');  --上上月最后一日
set i_sdate_25 =regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-2),'-','');	  --上上月1日

set i_sdate_23_1 =last_day(add_months(date_sub(current_date,1),-1));  --上月最后一日
set i_sdate_24_1 =last_day(add_months(date_sub(current_date,1),-2));  --上上月最后一日



drop table csx_tmp.temp_partner_cust_01;
create  table csx_tmp.temp_partner_cust_01
as
select a.smonth,
	a.province_name dist,a.customer_no,b.customer_name cust_name,a.business_type_name,b.work_no,b.sales_name,  --b.dev_source_name
	sum(a.sales_value)sales_value,
	sum(a.profit) profit,sum(a.profit)/sum(a.sales_value) prorate,
	sum(a.front_profit) as fnl_profit,
	sum(a.front_profit)/sum(a.sales_value) as fnl_prorate,
	round(sum(a.sales_value)*0.0006,2) salery_cal,
	sum(c.unpaid_amount)unpaid_amount_by,
	sum(d.unpaid_amount)unpaid_amount_sy,
	sum(e.payment_amount_by) payment_amount_by,
	sum(e.payment_amount_sy) payment_amount_sy	
from 
	(select substr(sdt,1,6) smonth,order_no,province_name,customer_no,business_type_name,
		sum(sales_value)sales_value,
		sum(profit) profit,sum(profit)/sum(sales_value) prorate,
		sum(front_profit) as front_profit,
		sum(front_profit)/sum(sales_value) as fnl_prorate
	from csx_dw.dws_sale_r_d_detail
	where sdt>='20200801' and sdt<${hiveconf:i_sdate_12}  --上月1日 --昨日月1日
	--where sdt>=${hiveconf:i_sdate_22} and sdt<${hiveconf:i_sdate_12}  --上月1日 --昨日月1日
	and channel_code in('1','7')
	and province_name='重庆市'
	--业务类型编码(1.日配单 2.福利单 3.批发内购 4.城市服务商 5.省区大宗 6.BBC)  --剔除内购客户、城市服务商
	--and business_type_code in('4')
    --4月签呈，以下客户按城市服务商算	
	--and (business_type_code in('4') or customer_no in('114265','117412'))
	and business_type_code in('4')
	group by substr(sdt,1,6),order_no,province_name,customer_no,business_type_name
	)a
left join 
	(select distinct substr(sdt,1,6) smonth,customer_no,customer_name,dev_source_name,work_no,sales_name
	from csx_dw.dws_crm_w_a_customer 
	where sdt=${hiveconf:i_sdate_23}  --上月最后1日
	) b on b.customer_no=a.customer_no
--业务类型编码(1.日配单 2.福利单 3.批发内购 4.城市服务商 5.省区大宗 6.BBC)  --剔除内购客户、城市服务商	
join 		
  (
	select distinct customer_no,substr(sdt,1,6) smonth 
	from csx_dw.dws_sale_r_d_detail 
	where (sdt>='20200801'
	and sdt<${hiveconf:i_sdate_12} 
	and business_type_code in('4'))
	or (substr(sdt,1,6)='202009' and customer_no in('114265','114248','114401','111933','113080','113392'))
	--4月签呈，以下客户按城市服务商算
	--or (substr(sdt,1,6)='202104' and customer_no in('114265','117412'))
  )x on x.customer_no=a.customer_no and x.smonth=a.smonth
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
	select close_bill_no,
	sum(case when regexp_replace(substr(paid_time,1,10),'-','')>=${hiveconf:i_sdate_22} 
			  and regexp_replace(substr(paid_time,1,10),'-','')<= ${hiveconf:i_sdate_23}
			 then payment_amount end ) payment_amount_by,
	sum(case when regexp_replace(substr(paid_time,1,10),'-','')>=${hiveconf:i_sdate_25} 
			  and regexp_replace(substr(paid_time,1,10),'-','')<= ${hiveconf:i_sdate_24}
			 then payment_amount end ) payment_amount_sy,			 
		sum(payment_amount) payment_amount
	from
		csx_dw.dwd_sss_r_d_close_bill_account_record_20200908
	where (regexp_replace(substr(happen_date,1,10),'-','')<=${hiveconf:i_sdate_23} or happen_date='' or happen_date is NULL)
	and regexp_replace(substr(paid_time,1,10),'-','') <=${hiveconf:i_sdate_23} 
	--and regexp_replace(substr(posting_time,1,10),'-','') <=${hiveconf:i_sdate_23}
	and is_deleted ='0'
	--and money_back_id<>'0' --回款关联ID为0是微信支付、-1是退货系统核销
	group by close_bill_no
	)e on e.close_bill_no=a.order_no	
group by a.smonth,a.province_name,a.customer_no,b.customer_name,a.business_type_name,b.work_no,b.sales_name;

--重庆合伙人的提成，后面不用管那个归属了哈，都只给黄丽一个人
select a.*,
	b.paid_amount,
	b.residual_amount,
	e.residual_amount residual_amount_all,
    if(unpaid_amount_by is null and unpaid_amount_sy is null,0,
          if(unpaid_amount_by>=sales_value,0,
    	        if(unpaid_amount_sy is null,(1-if(coalesce(unpaid_amount_by,0)/sales_value>1,1,coalesce(unpaid_amount_by,0)/sales_value))*salery_cal,
    			      if(coalesce(unpaid_amount_sy,0)>=coalesce(unpaid_amount_by,0),
    				        if(coalesce(unpaid_amount_sy,0)-coalesce(unpaid_amount_by,0)>sales_value,sales_value,
							      coalesce(unpaid_amount_sy,0)-coalesce(unpaid_amount_by,0)),0)/sales_value*salery_cal))) salery_cal_1		
from csx_tmp.temp_partner_cust_01 a
left join
( -- 获取本月客户回款金额
  select
    customer_code, -- 客户编码
    sum(claim_amount) as claim_amount,	--回款金额（未使用，含补救单）
    sum(paid_amount) as paid_amount,	--回款已核销金额
    sum(residual_amount) as residual_amount	--回款未核销金额
  from csx_dw.dwd_sss_r_d_money_back -- sdt以过账日期分区，只看20200601及以后的，该表计算回款只计算已核销金额
  where (sdt>=${hiveconf:i_sdate_22} and sdt<=${hiveconf:i_sdate_23}) 
  --or (sdt='19990101' and regexp_replace(substr(posting_time,1,10),'-','')>=${hiveconf:i_sdate_22})
  or (sdt='19990101' and regexp_replace(substr(posting_time,1,10),'-','')>=${hiveconf:i_sdate_22} and regexp_replace(substr(posting_time,1,10),'-','')<=${hiveconf:i_sdate_23})
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
  --or (sdt='19990101' and regexp_replace(substr(posting_time,1,10),'-','')>='20200601')
  or (sdt='19990101' and regexp_replace(substr(posting_time,1,10),'-','')>='20200601' and regexp_replace(substr(posting_time,1,10),'-','')<=${hiveconf:i_sdate_23})
  group by customer_code
) e on a.customer_no = e.customer_code;


