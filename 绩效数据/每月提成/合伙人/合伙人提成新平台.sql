
with temp_partner_cust_01 as (	
select a.smonth,
	a.performance_province_name dist,
	a.customer_code,
	b.customer_name cust_name,
	a.business_type_name,
	b.sales_user_number,
	b.sales_user_name,  -- b.dev_source_name
	sum(a.sale_amt)sale_amt,
	sum(a.profit) profit,sum(a.profit)/abs(sum(a.sale_amt)) prorate,
	round(sum(a.sale_amt)*0.0006,2) salery_cal,
	sum(c.unpaid_amount)unpaid_amt_by,
	sum(d.unpaid_amount)unpaid_amt_sy,
	sum(e.payment_amount_by) payment_amount_by,
	sum(e.payment_amount_sy) payment_amount_sy	
from 
	(select 
	    substr(sdt,1,6) smonth,
	    order_code,
		performance_province_name,
		customer_code,
		business_type_name,
		sum(sale_amt) sale_amt,
		sum(profit) profit,
		sum(profit)/abs(sum(sale_amt)) prorate
	from csx_dws.csx_dws_sale_detail_di
	where sdt>='20200801' and sdt<regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),0),'-','')  -- 上月1日 -- 昨日月1日
	and channel_code in('1','7')
	and performance_province_name='重庆市'
	and business_type_code in('4')
	group by substr(sdt,1,6),order_code,performance_province_name,customer_code,business_type_name
	)a
left join 
	(select distinct substr(sdt,1,6) smonth,customer_code,customer_name,sales_user_number,sales_user_name
	from csx_dim.csx_dim_crm_customer_info  
	where sdt=regexp_replace(last_day(add_months(date_sub(current_date,1),-1)),'-','')  -- 上月最后1日
	) b on b.customer_code=a.customer_code
-- 业务类型编码(1.日配单 2.福利单 3.批发内购 4.城市服务商 5.省区大宗 6.BBC)  -- 剔除内购客户、城市服务商	
join 		
  (
	select 
	  distinct customer_code,substr(sdt,1,6) smonth 
	from csx_dws.csx_dws_sale_detail_di 
	where (sdt>='20200801'
	and sdt<regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),0),'-','') 
	and business_type_code in('4'))
	or (substr(sdt,1,6)='202009' and customer_code in('114265','114248','114401','111933','113080','113392'))
  )x on x.customer_code=a.customer_code and x.smonth=a.smonth
left join 
	(select 
		source_bill_no as order_code,	--  来源单号
		sum(unpaid_amount) unpaid_amount	--  未回款金额

	from csx_dws.csx_dws_sss_order_credit_invoice_bill_settle_detail_di  -- 销售单对账
	where sdt=regexp_replace(last_day(add_months(date_sub(current_date,1),-1)),'-','')
	and date(happen_date)<=last_day(add_months(date_sub(current_date,1),-1))
	 group by source_bill_no
	)c on c.order_code=a.order_code
left join 
	(select 
		source_bill_no as order_code,	--  来源单号
		sum(unpaid_amount) unpaid_amount	--  未回款金额
	from csx_dws.csx_dws_sss_order_credit_invoice_bill_settle_detail_di  -- 销售单对账	
	where sdt=regexp_replace(last_day(add_months(date_sub(current_date,1),-2)),'-','')
	and date(happen_date)<=last_day(add_months(date_sub(current_date,1),-2))
	 group by source_bill_no
	)d on d.order_code=a.order_code	
left join 
	(
	-- 核销流水明细表中已核销金额
	select close_bill_code,
	sum(case when regexp_replace(substr(paid_time,1,10),'-','')>=regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-1),'-','') 
			  and regexp_replace(substr(paid_time,1,10),'-','')<= regexp_replace(last_day(add_months(date_sub(current_date,1),-1)),'-','')
			 then pay_amt end ) payment_amount_by,
	sum(case when regexp_replace(substr(paid_time,1,10),'-','')>=regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-2),'-','') 
			  and regexp_replace(substr(paid_time,1,10),'-','')<= regexp_replace(last_day(add_months(date_sub(current_date,1),-2)),'-','')
			 then pay_amt end ) payment_amount_sy,			 
		sum(pay_amt) payment_amount
	from
		csx_dwd.csx_dwd_sss_close_bill_account_record_di
	where (regexp_replace(substr(happen_date,1,10),'-','')<=regexp_replace(last_day(add_months(date_sub(current_date,1),-1)),'-','') 
	       or happen_date='' or happen_date is NULL)
	and regexp_replace(substr(paid_time,1,10),'-','') <=regexp_replace(last_day(add_months(date_sub(current_date,1),-1)),'-','') 
	and delete_flag ='0'
	group by close_bill_code
	)e on e.close_bill_code=a.order_code	
group by a.smonth,a.performance_province_name,a.customer_code,b.customer_name,a.business_type_name,b.sales_user_number,b.sales_user_name)

-- 重庆合伙人的提成，后面不用管那个归属了哈，都只给黄丽一个人
select a.*,
	b.paid_amt,
	b.residue_amt,
	e.residue_amt residue_amt_all,
    if(unpaid_amt_by is null and unpaid_amt_sy is null,0,
          if(unpaid_amt_by>=sale_amt,0,
    	        if(unpaid_amt_sy is null,(1-if(coalesce(unpaid_amt_by,0)/sale_amt>1,1,coalesce(unpaid_amt_by,0)/sale_amt))*salery_cal,
    			      if(coalesce(unpaid_amt_sy,0)>=coalesce(unpaid_amt_by,0),
    				        if(coalesce(unpaid_amt_sy,0)-coalesce(unpaid_amt_by,0)>sale_amt,sale_amt,
							      coalesce(unpaid_amt_sy,0)-coalesce(unpaid_amt_by,0)),0)/sale_amt*salery_cal))) salery_cal_1		
from temp_partner_cust_01 a
left join
( --  获取本月客户回款金额
  select
    customer_code, --  客户编码
    sum(claim_amt) as claim_amt,	-- 回款金额（未使用，含补救单）
    sum(paid_amt) as paid_amt,	-- 回款已核销金额
    sum(residue_amt) as residue_amt	-- 回款未核销金额
  from csx_dwd.csx_dwd_sss_money_back_di --  sdt以过账日期分区，只看20200601及以后的，该表计算回款只计算已核销金额
  where (sdt>=regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-1),'-','') and sdt<=regexp_replace(last_day(add_months(date_sub(current_date,1),-1)),'-','')) 
  -- or (sdt='19990101' and regexp_replace(substr(posting_time,1,10),'-','')>=regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-1),'-',''))
  or (sdt='19990101' and regexp_replace(substr(posting_time,1,10),'-','')>=regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-1),'-','') and regexp_replace(substr(posting_time,1,10),'-','')<=regexp_replace(last_day(add_months(date_sub(current_date,1),-1)),'-',''))
  group by customer_code
) b on a.customer_code = b.customer_code
-- 历史回款金额
left join
( 
  select
    customer_code, --  客户编码
    sum(claim_amt) as claim_amt,	-- 回款金额（未使用，含补救单）
    sum(paid_amt) as paid_amt,	-- 回款已核销金额
    sum(residue_amt) as residue_amt	-- 回款未核销金额
  from csx_dwd.csx_dwd_sss_money_back_di --  sdt以过账日期分区，只看20200601及以后的，该表计算回款只计算已核销金额
  where (sdt>='20200601' and sdt<=regexp_replace(last_day(add_months(date_sub(current_date,1),-1)),'-','')) 
  -- or (sdt='19990101' and regexp_replace(substr(posting_time,1,10),'-','')>='20200601')
  or (sdt='19990101' and regexp_replace(substr(posting_time,1,10),'-','')>='20200601' and regexp_replace(substr(posting_time,1,10),'-','')<=regexp_replace(last_day(add_months(date_sub(current_date,1),-1)),'-',''))
  group by customer_code
) e on a.customer_code = e.customer_code   