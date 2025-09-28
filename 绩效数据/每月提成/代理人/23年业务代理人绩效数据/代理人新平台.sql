-- 签呈一 四川 116401
-- 
 
with temp_dali_cust_01 as (		 
select 
    a.smonth,
	a.performance_province_name dist,
	a.customer_code,
	b.customer_name cust_name,
	b.sign_date,
	b.sales_user_number,
	b.sales_user_name,
	sum(a.sale_amt) sale_amt,
	sum(a.profit) profit,
	sum(a.profit)/sum(a.sale_amt) prorate,
	round(sum(a.sale_amt)*0.02,2) salery_sales,
	round(if((sum(a.profit)/sum(a.sale_amt)-0.15)>=0 and
	a.customer_code not in('104179','112092'),
	(sum(a.profit)/sum(a.sale_amt)-0.15),0)*sum(a.sale_amt)*0.5,2) salery_fnl,
	round(sum(a.sale_amt)*0.02,2)+
	round(if((sum(a.profit)/sum(a.sale_amt)-0.15)>=0 and a.customer_code not in('104179','112092'),(sum(a.profit)/sum(a.sale_amt)-0.15),0)*sum(a.sale_amt)*0.5,2) salery_cal,
	sum(c.unpaid_amount)unpaid_amt_by,
	sum(d.unpaid_amount)unpaid_amt_sy,
	max(e.payment_amount_by) payment_amount_by,
	max(e.payment_amount_sy) payment_amount_sy
from 

	(select substr(sdt,1,6) smonth,
	    order_code,
		performance_province_name,
		customer_code,
		sum(sale_amt)sale_amt,
		sum(profit) profit,sum(profit)/sum(sale_amt) prorate
	from csx_dws.csx_dws_sale_detail_di
	where sdt>='20200901' and sdt<regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),0),'-','')  -- 上上月1日 -- 昨日月1日
	and customer_code not in ('112092')
	group by substr(sdt,1,6),order_code,performance_province_name,customer_code
	)a
join 
  (
   select 
	distinct substr(sdt,1,6) smonth,
	customer_code,
	customer_name,
	case when customer_code in ('104179' ,'112092','113260') then '-'
		 else sales_user_number end as sales_user_number,
	case when customer_code='104179' then '徐桂莹' 
		 when customer_code='112092' then '蒋光平'
		 when customer_code='113260' then '於佳'
		 else sales_user_name end as sales_user_name,
	regexp_replace(split(first_sign_time, ' ')[0], '-', '') as sign_date
	from csx_dim.csx_dim_crm_customer_info 
	where sdt=regexp_replace(last_day(add_months(date_sub(current_date,1),-1)),'-','')  -- 上月最后1日
	and (channel_code='9' or customer_code in  ('123629'))
	-- '122221' 已更正为业务代理人 '116401' 202205月开始不算业务代理人改为大客户 127155
	) b on b.customer_code=a.customer_code 
left join 
	(
	 select 
		source_bill_no as order_code,	--  来源单号
		sum(unpaid_amount) unpaid_amount	--  未回款金额
	 from csx_dws.csx_dws_sss_order_credit_invoice_bill_settle_detail_di  -- 销售单对账
	 where sdt=regexp_replace(last_day(add_months(date_sub(current_date,1),-1)),'-','')
	       and date(happen_date)<=last_day(add_months(date_sub(current_date,1),-1))
	 group by source_bill_no
	)c on c.order_code=a.order_code
left join 
	(
	 select 
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
	select customer_code,regexp_replace(substr(happen_date,1,7),'-','') smonth,
	sum(case when regexp_replace(substr(paid_time,1,10),'-','')>=regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-1),'-','') 
			  and regexp_replace(substr(paid_time,1,10),'-','')<= regexp_replace(last_day(add_months(date_sub(current_date,1),-1)),'-','')
			 then pay_amt end ) payment_amount_by,
	sum(case when regexp_replace(substr(paid_time,1,10),'-','')>=regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-2),'-','') 
			  and regexp_replace(substr(paid_time,1,10),'-','')<= regexp_replace(last_day(add_months(date_sub(current_date,1),-2)),'-','')
			 then pay_amt end ) payment_amount_sy,			 
		sum(pay_amt) payment_amount
	from
		csx_dwd.csx_dwd_sss_close_bill_account_record_di a
	
	
	where  (regexp_replace(substr(happen_date,1,10),'-','')<=regexp_replace(last_day(add_months(date_sub(current_date,1),-1)),'-','') or happen_date='' or happen_date is NULL)
	and regexp_replace(substr(paid_time,1,10),'-','') <=     regexp_replace(last_day(add_months(date_sub(current_date,1),-1)),'-','') 
	and delete_flag ='0'
	group by customer_code,regexp_replace(substr(happen_date,1,7),'-','')
	)e on e.customer_code=a.customer_code and e.smonth=a.smonth -- e.close_bill_no=a.order_code	
group by a.smonth,a.performance_province_name,a.customer_code,b.customer_name,b.sign_date,b.sales_user_number,b.sales_user_name)

select *,
if(coalesce(payment_amount_by,0)=0,0,least(coalesce(sale_amt,0),coalesce(payment_amount_by,0))/coalesce(sale_amt,0)*coalesce(salery_sales,0)*sign_ratio+least(coalesce(payment_amount_by,0),coalesce(sale_amt,0))/coalesce(sale_amt,0)*coalesce(salery_fnl,0))	 as salery_cal_1 	
from (
select 
a.smonth
,a.dist
,a.customer_code
,a.cust_name
,a.sign_date
,a.sales_user_number
,a.sales_user_name
,coalesce(a.sale_amt,0) sale_amt
,coalesce(a.profit,0) profit
,coalesce(a.prorate,0) prorate
,coalesce(a.salery_sales,0) salery_sales
,coalesce(a.salery_fnl,0) salery_fnl
,coalesce(a.salery_cal,0) salery_cal
,coalesce(a.unpaid_amt_by,0) unpaid_amt_by
,coalesce(a.unpaid_amt_sy,0) unpaid_amt_sy
,coalesce(a.payment_amount_by,0) payment_amount_by
,coalesce(a.payment_amount_sy,0) payment_amount_sy
,coalesce(b.paid_amt,0) paid_amt
,coalesce(b.residue_amt,0) residue_amt
,coalesce(e.residue_amt ,0) residue_amt_all
,1 as sign_ratio
	-- if(a.sign_date<='20200930',1.5,1) 
    -- if((unpaid_amt_by is NULL or unpaid_amt_by=0)  and unpaid_amt_sy is NULL,0,
    --       if(unpaid_amt_by>=sale_amt,0,
    -- 	        if(unpaid_amt_sy is NULL,
				-- (1-if(coalesce(unpaid_amt_by,0)/sale_amt>1,1,coalesce(unpaid_amt_by,0)/sale_amt))*salery_cal,-- unpaid_amt_sy is null,都取的这个数据
    -- 			      if(coalesce(unpaid_amt_sy,0)>=coalesce(unpaid_amt_by,0),
    -- 				        if(coalesce(unpaid_amt_sy,0)-coalesce(unpaid_amt_by,0)>sale_amt,sale_amt,
				-- 			      coalesce(unpaid_amt_sy,0)-coalesce(unpaid_amt_by,0)),0)/sale_amt*salery_cal
				-- 				  ))) salery_cal_1	
						 
	--  *if(a.sign_date<='20200930',1.5,1) 																					
from temp_dali_cust_01 a
left join
( --  获取本月客户回款金额
  select
    customer_code, --  客户编码
    sum(claim_amt) as claim_amt,	-- 回款金额（未使用，含补救单）
    sum(paid_amt) as paid_amt,	-- 回款已核销金额
    sum(residue_amt) as residue_amt	-- 回款未核销金额
  from csx_dwd.csx_dwd_sss_money_back_di --  sdt以过账日期分区，只看20200601及以后的，该表计算回款只计算已核销金额
  where (sdt>=regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-1),'-','') and sdt<=regexp_replace(last_day(add_months(date_sub(current_date,1),-1)),'-','')) 
  and regexp_replace(substr(update_time,1,10),'-','')<=regexp_replace(last_day(add_months(date_sub(current_date,1),-1)),'-','')  -- 回款以过账日期为准，但每次已核销金额发生变化更新日期都会变化，此表无法查历史回款已核销金额
  and (paid_amt<>'0' or residue_amt<>'0') -- 剔除补救单和对应原单
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
  and regexp_replace(substr(update_time,1,10),'-','')<=regexp_replace(last_day(add_months(date_sub(current_date,1),-1)),'-','')  -- 回款以过账日期为准，但每次已核销金额发生变化更新日期都会变化，此表无法查历史回款已核销金额
  and (paid_amt<>'0' or residue_amt<>'0') -- 剔除补救单和对应原单 
  group by customer_code
) e on a.customer_code = e.customer_code    
) a 
