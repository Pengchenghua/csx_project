-- 已验证20231213
-- 	回款比例	毛利超出16%
-- 227054	2%	
-- 225582		
-- 122322	3%	50%
-- 236143	3%	50
-- 124484		
-- 128865	2%	
-- 130078	2%	
-- 108494		
-- 114872		
-- 128734		
-- 113260		
-- 130733		
-- 128336
with temp_dali_cust_01 as (		 
select 
    a.smonth,
	a.performance_province_name dist,
	a.performance_city_name,
	a.customer_code,
	a.customer_name cust_name,
	a.sign_date,
	a.sales_user_number,
	a.sales_user_name,
	sum(a.sale_amt) sale_amt,
	sum(a.profit) profit,
	sum(a.profit)/sum(a.sale_amt) prorate,
	round(sum(a.sale_amt)*0.02,2) salery_sales,
	round(if((sum(a.profit)/sum(a.sale_amt)-0.15)>0 and a.customer_code not in('104179','112092','113260'),(sum(a.profit)/sum(a.sale_amt)-0.15),0)*max(e.payment_amount_by)*0.5,4) salery_fnl,
	round(sum(a.sale_amt)*0.02,2)+round(if((sum(a.profit)/sum(a.sale_amt)-0.15)>=0 and a.customer_code not in('104179','112092','113260'),(sum(a.profit)/sum(a.sale_amt)-0.15),0)*sum(a.sale_amt)*0.5,2) salery_cal,
	sum(c.unpaid_amount)unpaid_amt_by,
	sum(d.unpaid_amount)unpaid_amt_sy,
	max(e.payment_amount_by) payment_amount_by,
	max(e.payment_amount_sy) payment_amount_sy
from 
	(select substr(sdt,1,6) smonth,
	    a.order_code,
		a.performance_province_name,
		a.performance_city_name,
		a.customer_code,
		b.customer_name,
		b.sales_user_number,
		b.sales_user_name,
		b.sign_date,
		sum(sale_amt)sale_amt,
		sum(profit) profit,
		sum(profit)/sum(sale_amt) prorate
	from csx_dws.csx_dws_sale_detail_di a
join 
  (select DISTINCT substr(sdt,1,6) smonth,
		customer_code,
		customer_name,
	case when customer_code in ('104179','112092','116401','128951','106299','118676','130941','122773','113260','224504') then '-' 
		 else sales_user_number end as sales_user_number,
	case when customer_code='104179' then '徐桂莹' 
		 when customer_code='112092' then '蒋光平'
		 when customer_code='118676' then '陈荣耀'
		 when customer_code='130941' then '祝心雨'
		 when customer_code in('224504') then '何新莲'
		 when customer_code='106299' then '何丽琴'	
		 when customer_code='122773' then '陈威仁'
		 when customer_code='113260' then '於佳'
		 else sales_user_name end as sales_user_name,
	regexp_replace(to_date(first_sign_time), '-', '') as sign_date
	from csx_dim.csx_dim_crm_customer_info 
	where sdt=regexp_replace(last_day(add_months(date_sub(current_date,1),-1)),'-','')  -- 上月最后1日
	and (channel_code='9' or customer_code in  ('104179','123629','112092','128951','113260','116401','106299','118676','130941','122773','224504'
'113390',	-- 福建客户
'114038',
'102924',
'120465',
'101884',
'102524',
'230760',
'105750',
'105703',
'113679',
'113678',
'113746',
'113805',
'113760',
'118503',
'176508',
'106698',
'119213',
'119235',
'121054',
'116947',
'128672',
'125068',
'171028',
'166470',
'232541'
))  
	-- customer_code in  ('104179','123629','112092','128951','113260','116401','106299','118676','130941','122773','224504)  -- 签呈客户
	-- '122221' 已更正为业务代理人 '116401' 202205月开始不算业务代理人改为大客户 127155
	) b on b.customer_code=a.customer_code 
	where sdt>='20200901' and sdt<regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),0),'-','')  -- 上上月1日 -- 昨日月1日
--	and customer_code not in ('112092')
	group by substr(sdt,1,6), 
	    a.order_code,
		a.performance_province_name,
		a.performance_city_name,
		a.customer_code,
		b.customer_name,
		b.sales_user_number,
		b.sales_user_name,
		b.sign_date
		)a
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
	)e on e.customer_code=a.customer_code and e.smonth=a.smonth 
	-- e.close_bill_no=a.order_code	
group by  a.smonth,
	a.performance_province_name  ,
	a.performance_city_name,
	a.customer_code,
	a.customer_name  ,
	a.sign_date,
	a.sales_user_number,
	a.sales_user_name
	)	,
by_paid as 	( --  获取本月客户回款金额
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
),
-- 历史回款金额
ls_paid as ( 
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
)
select a.smonth
,a.dist
,performance_city_name
,a.customer_code
,a.cust_name
,a.sign_date
,a.sales_user_number
,a.sales_user_name
,sale_amt
,profit
,prorate
,salery_sales	-- 回款奖金包
,salery_fnl		-- 毛利奖金包
,salery_cal		-- 奖金包-乘系数前
,unpaid_amt_by	-- 本月应收
,unpaid_amt_sy	-- 上月应收
,payment_amount_by	-- 本月核销
,payment_amount_sy  -- 上月核销
,paid_amt					-- 本月回款已核销
,residue_amt				-- 本月回款未核销金额
, residue_amt_all			-- 上月回款未核销金额
--, payment_ratio_amt
,sign_ratio,
-- 113260 按照毛利率区间计算16%-17% 6.17% 、17%-18% 7.19%、18%-19% 8.22%、19%-20% 9.25%、20%以上 10.28%
--  customer_code in ('128951','127649')签呈客户 按照回款比例计算
case when a.customer_code='113260' and prorate >='0.16' and prorate<'0.17' then payment_amount_by*0.0617 
    when a.customer_code='113260' and prorate >='0.17' and prorate<'0.18' then  payment_amount_by*0.0719
    when a.customer_code='113260' and prorate >='0.18' and prorate<'0.19' then  payment_amount_by*0.0822
    when a.customer_code='113260' and prorate >='0.19' and prorate<'0.20' then  payment_amount_by*0.0925
    when a.customer_code='113260' and prorate >='0.20'   then payment_amount_by*0.1028
	when a.customer_code='128951' then payment_amount_by*0.04 
	when a.customer_code in('130941','224504') then payment_amount_by*0.03
	-- 118676 三明消防毛利奖金包+销售回款基础包
	when a.customer_code in('118676') then  if(prorate>0.15,(prorate-0.15)*0.5*payment_amount_by,0)+payment_amount_by*0.02
	when a.customer_code in('122322','236143') then  if(prorate>0.16,(prorate-0.16)*0.5*payment_amount_by,0)+payment_amount_by*0.03
	when a.customer_code in ('112092','116401','106299','122773','227054','128865','130078') then payment_amount_by*0.02
	else 
	-- if(coalesce(payment_amount_by,0)=0,0,least(coalesce(sale_amt,0),coalesce(payment_amount_by,0))/coalesce(sale_amt,0)*coalesce(salery_sales,0)*sign_ratio
	-- +least(coalesce(payment_amount_by,0),coalesce(sale_amt,0))/coalesce(sale_amt,0)*coalesce(salery_fnl,0))	
	salery_fnl+salery_sales
	end as salery_cal_1,
   case when a.customer_code in  ('104179','123629','112092','128951','113260','116401','106299','118676','130941','122773','224504')  then '1' else '0' end cust_flag,
	case when a.customer_code='113260' and prorate >='0.16' and prorate<'0.17' then payment_amount_by*0.0617 
    when a.customer_code='113260' and prorate >='0.17' and prorate<'0.18' then  payment_amount_by*0.0719
    when a.customer_code='113260' and prorate >='0.18' and prorate<'0.19' then  payment_amount_by*0.0822
    when a.customer_code='113260' and prorate >='0.19' and prorate<'0.20' then  payment_amount_by*0.0925
    when a.customer_code='113260' and prorate >='0.20'   then payment_amount_by*0.1028
	when a.customer_code='128951' then payment_amount_by*0.04 
	when a.customer_code in('130941','224504') then payment_amount_by*0.03
	-- 118676 三明消防毛利奖金包+销售回款基础包 当大于15%毛利率时 按照毛利额*50%
	when a.customer_code in('118676') then if(prorate>0.15,(prorate-0.15)*0.5*profit,0)+payment_amount_by*0.02
	when a.customer_code in('122322','236143') then  if(prorate>0.15,(prorate-0.15)*0.5*payment_amount_by,0)+payment_amount_by*0.03
	when a.customer_code in ('112092','116401','106299','122773','227054','128865','130078') then payment_amount_by*0.02
	else 
	    if( coalesce(unpaid_amt_by,0)=0 and unpaid_amt_sy is NULL,0,
          if(unpaid_amt_by>=sale_amt,0,
    	        if(unpaid_amt_sy is NULL,
				(1-if(coalesce(unpaid_amt_by,0)/sale_amt>1,1,coalesce(unpaid_amt_by,0)/sale_amt))*salery_cal,-- unpaid_amt_sy is null,都取的这个数据
    			      if(coalesce(unpaid_amt_sy,0)>=coalesce(unpaid_amt_by,0),
    				        if(coalesce(unpaid_amt_sy,0)-coalesce(unpaid_amt_by,0)>sale_amt,sale_amt,
							      coalesce(unpaid_amt_sy,0)-coalesce(unpaid_amt_by,0)),0)/sale_amt*salery_cal
							))) end  salery_cal_2	 	
-- 113260 按照毛利率区间计算16%-17% 6.17% 、17%-18% 7.19%、18%-19% 8.22%、19%-20% 9.25%、20%以上 10.28%
--  ,case when a.customer_code='113260' and prorate >='0.16' and prorate<'0.17' then b.paid_amt*0.0617 
--     when a.customer_code='113260' and prorate >='0.17' and prorate<'0.18' then b.paid_am*0.0719
--     when a.customer_code='113260' and prorate >='0.18' and prorate<'0.19' then b.paid_am*0.0822
--     when a.customer_code='113260' and prorate >='0.19' and prorate<'0.20' then b.paid_am*0.0925
--     when a.customer_code='113260' and prorate >='0.20'   then b.paid_am*0.1028
--     else 0 end salery_cal_2
from (
select 
a.smonth
,a.dist
,performance_city_name
,a.customer_code
,a.cust_name
,a.sign_date
,a.sales_user_number
,a.sales_user_name
,coalesce(a.sale_amt,0) sale_amt
,coalesce(a.profit,0) profit
,coalesce(a.prorate,0) prorate
,case when coalesce(payment_amount_by/10000,0)<=50 then payment_amount_by*0.02
      when coalesce(payment_amount_by/10000,0)>50 and  coalesce(payment_amount_by/10000,0)<=100 then payment_amount_by*0.022
      when coalesce(payment_amount_by/10000,0)>100 and  coalesce(payment_amount_by/10000,0)<=200 then payment_amount_by*0.024
      when coalesce(payment_amount_by/10000,0)>200 and  coalesce(payment_amount_by/10000,0)<=300 then payment_amount_by*0.026
      when coalesce(payment_amount_by/10000,0)>300  then payment_amount_by*0.028
 end salery_sales	-- 回款奖金包
,coalesce(a.salery_fnl,0) salery_fnl		-- 毛利奖金包
,coalesce(a.salery_cal,0) salery_cal		-- 奖金包-乘系数前
,coalesce(a.unpaid_amt_by,0) unpaid_amt_by	-- 本月应收
,coalesce(a.unpaid_amt_sy,0) unpaid_amt_sy	-- 上月应收
,coalesce(a.payment_amount_by,0) payment_amount_by	-- 本月核销
,coalesce(a.payment_amount_sy,0) payment_amount_sy  -- 上月核销
,coalesce(b.paid_amt,0) paid_amt					-- 本月回款已核销
,coalesce(b.residue_amt,0) residue_amt				-- 本月回款未核销金额
,coalesce(e.residue_amt ,0) residue_amt_all			-- 上月回款未核销金额
,case when coalesce(payment_amount_by/10000,0)<=50 then payment_amount_by*0.02
      when coalesce(payment_amount_by/10000,0)>50 and  coalesce(payment_amount_by/10000,0)<=100 then payment_amount_by*0.022
      when coalesce(payment_amount_by/10000,0)>100 and  coalesce(payment_amount_by/10000,0)<=200 then payment_amount_by*0.024
      when coalesce(payment_amount_by/10000,0)>200 and  coalesce(payment_amount_by/10000,0)<=300 then payment_amount_by*0.026
      when coalesce(payment_amount_by/10000,0)>300  then payment_amount_by*0.028
 end payment_ratio_amt
,1 as sign_ratio
 -- 逻辑里有这个判断截止上月应收-截止本月应收<本月核销，按数值小的计算 
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
        by_paid b on a.customer_code = b.customer_code
left join
        ls_paid e on a.customer_code = e.customer_code    
) a 

-- where a.customer_code in ('235185','230038')
;



 -- 增加城市20231115

with temp_dali_cust_01 as (		 
select 
    a.smonth,
	a.performance_province_name dist,
	a.performance_city_name,
	a.customer_code,
	a.customer_name cust_name,
	a.sign_date,
	a.sales_user_number,
	a.sales_user_name,
	sum(a.sale_amt) sale_amt,
	sum(a.profit) profit,
	sum(a.profit)/sum(a.sale_amt) prorate,
	round(sum(a.sale_amt)*0.02,2) salery_sales,
	round(if((sum(a.profit)/sum(a.sale_amt)-0.15)>=0 and a.customer_code not in('104179','112092','113260'),(sum(a.profit)/sum(a.sale_amt)-0.15),0)*sum(a.sale_amt)*0.5,2) salery_fnl,
	round(sum(a.sale_amt)*0.02,2)+round(if((sum(a.profit)/sum(a.sale_amt)-0.15)>=0 and a.customer_code not in('104179','112092','113260'),(sum(a.profit)/sum(a.sale_amt)-0.15),0)*sum(a.sale_amt)*0.5,2) salery_cal,
	sum(c.unpaid_amount)unpaid_amt_by,
	sum(d.unpaid_amount)unpaid_amt_sy,
	max(e.payment_amount_by) payment_amount_by,
	max(e.payment_amount_sy) payment_amount_sy
from 
	(select substr(sdt,1,6) smonth,
	    a.order_code,
		a.performance_province_name,
		a.performance_city_name,
		a.customer_code,
		b.customer_name,
		b.sales_user_number,
		b.sales_user_name,
		b.sign_date,
		sum(sale_amt)sale_amt,
		sum(profit) profit,
		sum(profit)/sum(sale_amt) prorate
	from csx_dws.csx_dws_sale_detail_di a
join 
  (select DISTINCT substr(sdt,1,6) smonth,
		customer_code,
		customer_name,
	case when customer_code in ('104179','112092','116401','128951','106299','118676','130941','122773','113260','224504') then '-' 
		 else sales_user_number end as sales_user_number,
	case when customer_code='104179' then '徐桂莹' 
		 when customer_code='112092' then '蒋光平'
		 when customer_code='118676' then '陈荣耀'
		 when customer_code in('130941','224504') then '何新莲'
		 when customer_code='106299' then '何丽琴'	
		 when customer_code='122773' then '陈威仁'
		 when customer_code='113260' then '於佳'
		 else sales_user_name end as sales_user_name,
	regexp_replace(to_date(first_sign_time), '-', '') as sign_date
	from csx_dim.csx_dim_crm_customer_info 
	where sdt=regexp_replace(last_day(add_months(date_sub(current_date,1),-1)),'-','')  -- 上月最后1日
	and (channel_code='9' or customer_code in  ('104179','123629','112092','128951','113260','116401','106299','118676','130941','122773','224504'))  
	-- customer_code in  ('104179','123629','112092','128951','113260','116401','106299','118676','130941','122773','224504)  -- 签呈客户
	-- '122221' 已更正为业务代理人 '116401' 202205月开始不算业务代理人改为大客户 127155
	) b on b.customer_code=a.customer_code 
	where sdt>='20200901' and sdt<regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),0),'-','')  -- 上上月1日 -- 昨日月1日
--	and customer_code not in ('112092')
	group by substr(sdt,1,6), 
	    a.order_code,
		a.performance_province_name,
		a.performance_city_name,
		a.customer_code,
		b.customer_name,
		b.sales_user_number,
		b.sales_user_name,
		b.sign_date
		)a
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
	)e on e.customer_code=a.customer_code and e.smonth=a.smonth 
	-- e.close_bill_no=a.order_code	
group by  a.smonth,
	a.performance_province_name  ,
	a.performance_city_name,
	a.customer_code,
	a.customer_name  ,
	a.sign_date,
	a.sales_user_number,
	a.sales_user_name
	)	,
by_paid as 	( --  获取本月客户回款金额
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
),
-- 历史回款金额
ls_paid as ( 
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
)
select a.* ,
-- 113260 按照毛利率区间计算16%-17% 6.17% 、17%-18% 7.19%、18%-19% 8.22%、19%-20% 9.25%、20%以上 10.28%
--  customer_code in ('128951','127649')签呈客户 按照回款比例计算
case when a.customer_code='113260' and prorate >='0.16' and prorate<'0.17' then payment_amount_by*0.0617 
    when a.customer_code='113260' and prorate >='0.17' and prorate<'0.18' then  payment_amount_by*0.0719
    when a.customer_code='113260' and prorate >='0.18' and prorate<'0.19' then  payment_amount_by*0.0822
    when a.customer_code='113260' and prorate >='0.19' and prorate<'0.20' then  payment_amount_by*0.0925
    when a.customer_code='113260' and prorate >='0.20'   then payment_amount_by*0.1028
	when a.customer_code='128951' then payment_amount_by*0.04 
	when a.customer_code in('130941','224504') then payment_amount_by*0.03
	-- 118676 三明消防毛利奖金包+销售回款基础包
	when a.customer_code in('118676') then  if(prorate>0.15,(prorate-0.15)*0.5*profit,0)+payment_amount_by*0.02
	when a.customer_code in ('112092','116401','106299','122773') then payment_amount_by*0.02
	else 
	-- if(coalesce(payment_amount_by,0)=0,0,least(coalesce(sale_amt,0),coalesce(payment_amount_by,0))/coalesce(sale_amt,0)*coalesce(salery_sales,0)*sign_ratio
	-- +least(coalesce(payment_amount_by,0),coalesce(sale_amt,0))/coalesce(sale_amt,0)*coalesce(salery_fnl,0))	

	end as salery_cal_1,
   case when a.customer_code in  ('104179','123629','112092','128951','113260','116401','106299','118676','130941','122773','224504')  then '1' else '0' end cust_flag,
	case when a.customer_code='113260' and prorate >='0.16' and prorate<'0.17' then payment_amount_by*0.0617 
    when a.customer_code='113260' and prorate >='0.17' and prorate<'0.18' then  payment_amount_by*0.0719
    when a.customer_code='113260' and prorate >='0.18' and prorate<'0.19' then  payment_amount_by*0.0822
    when a.customer_code='113260' and prorate >='0.19' and prorate<'0.20' then  payment_amount_by*0.0925
    when a.customer_code='113260' and prorate >='0.20'   then payment_amount_by*0.1028
	when a.customer_code='128951' then payment_amount_by*0.04 
	when a.customer_code in('130941','224504') then payment_amount_by*0.03
	-- 118676 三明消防毛利奖金包+销售回款基础包 当大于15%毛利率时 按照毛利额*50%
	when a.customer_code in('118676') then if(prorate>0.15,(prorate-0.15)*0.5*profit,0)+payment_amount_by*0.02
	when a.customer_code in ('112092','116401','106299','122773') then payment_amount_by*0.02
	else 
	    if( coalesce(unpaid_amt_by,0)=0 and unpaid_amt_sy is NULL,0,
          if(unpaid_amt_by>=sale_amt,0,
    	        if(unpaid_amt_sy is NULL,
				(1-if(coalesce(unpaid_amt_by,0)/sale_amt>1,1,coalesce(unpaid_amt_by,0)/sale_amt))*salery_cal,-- unpaid_amt_sy is null,都取的这个数据
    			      if(coalesce(unpaid_amt_sy,0)>=coalesce(unpaid_amt_by,0),
    				        if(coalesce(unpaid_amt_sy,0)-coalesce(unpaid_amt_by,0)>sale_amt,sale_amt,
							      coalesce(unpaid_amt_sy,0)-coalesce(unpaid_amt_by,0)),0)/sale_amt*salery_cal
							))) end  salery_cal_2	 	
-- 113260 按照毛利率区间计算16%-17% 6.17% 、17%-18% 7.19%、18%-19% 8.22%、19%-20% 9.25%、20%以上 10.28%
--  ,case when a.customer_code='113260' and prorate >='0.16' and prorate<'0.17' then b.paid_amt*0.0617 
--     when a.customer_code='113260' and prorate >='0.17' and prorate<'0.18' then b.paid_am*0.0719
--     when a.customer_code='113260' and prorate >='0.18' and prorate<'0.19' then b.paid_am*0.0822
--     when a.customer_code='113260' and prorate >='0.19' and prorate<'0.20' then b.paid_am*0.0925
--     when a.customer_code='113260' and prorate >='0.20'   then b.paid_am*0.1028
--     else 0 end salery_cal_2
from (
select 
a.smonth
,a.dist
,performance_city_name
,a.customer_code
,a.cust_name
,a.sign_date
,a.sales_user_number
,a.sales_user_name
,coalesce(a.sale_amt,0) sale_amt
,coalesce(a.profit,0) profit
,coalesce(a.prorate,0) prorate
,coalesce(a.salery_sales,0) salery_sales	-- 销售奖金包
,coalesce(a.salery_fnl,0) salery_fnl		-- 毛利奖金包
,coalesce(a.salery_cal,0) salery_cal		-- 奖金包-乘系数前
,coalesce(a.unpaid_amt_by,0) unpaid_amt_by	-- 本月应收
,coalesce(a.unpaid_amt_sy,0) unpaid_amt_sy	-- 上月应收
,coalesce(a.payment_amount_by,0) payment_amount_by	-- 本月核销
,coalesce(a.payment_amount_sy,0) payment_amount_sy  -- 上月核销
,coalesce(b.paid_amt,0) paid_amt					-- 本月回款已核销
,coalesce(b.residue_amt,0) residue_amt				-- 本月回款未核销金额
,coalesce(e.residue_amt ,0) residue_amt_all			-- 上月回款未核销金额
,1 as sign_ratio
 -- 逻辑里有这个判断截止上月应收-截止本月应收<本月核销，按数值小的计算 
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
        by_paid b on a.customer_code = b.customer_code
left join
        ls_paid e on a.customer_code = e.customer_code    
) a 


;


-- 特殊客户签呈客户处理 112092 按照回款4%，其他按照回款2% 计算

select a.smonth,a.customer_code,b.customer_name,b.sales_user_number,b.sales_user_name,
	claim_amt,paid_amt,residue_amt,
	case when a.customer_code='128951' then paid_amt*0.04
		 when a.customer_code in('130941','224504') then paid_amt*0.03
		  else paid_amt*0.02 end 
		  as salery_cal_1
from 
	(select 
		regexp_replace(substr(posting_time,1,7),'-','') smonth,	
		customer_code customer_code, --  客户编码
		sum(claim_amt) as claim_amt,	-- 回款金额（未使用，含补救单）
		sum(paid_amt) as paid_amt,	-- 回款已核销金额
		sum(residue_amt) as residue_amt	-- 回款未核销金额
	from csx_dwd.csx_dwd_sss_money_back_di --  sdt以过账日期分区，只看20200601及以后的，该表计算回款只计算已核销金额
	where 
	-- 按照回款签呈处理
	customer_code in ('112092','116401','128951','106299','118676','130941','122773','224504')
	-- 122773 从23年11月开始
	 and sdt>=regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-1),'-','') 
	 and sdt<=regexp_replace(last_day(add_months(date_sub(current_date,1),-1)),'-','')	  
	group by regexp_replace(substr(posting_time,1,7),'-',''),customer_code
	)a
left join 
	(select DISTINCT substr(sdt,1,6) smonth,
		customer_code,
		customer_name,
	case when customer_code in ('112092','116401','128951','106299','118676','130941','122773','113260','224504') then '-' 
		 else sales_user_number end as sales_user_number,
	case when customer_code='104179' then '徐桂莹' 
		 when customer_code='112092' then '蒋光平'
		 when customer_code='118676' then '陈荣耀'
		 when customer_code='130941' then '何新莲'
		 when customer_code='106299' then '何丽琴'	
		 when customer_code='122773' then '陈威仁'
		 when customer_code='113260' then '於佳'
		 else sales_user_name end as sales_user_name,
	regexp_replace(to_date(first_sign_time), '-', '') as sign_date
	from csx_dim.csx_dim_crm_customer_info 
	where sdt=regexp_replace(last_day(add_months(date_sub(current_date,1),-1)),'-','')  -- 上月最后1日
	and customer_code in ('112092','116401','128951','106299','118676','130941','122773','224504')
	) b on b.customer_code=a.customer_code   

	;

with temp_dali_cust_01 as (		 
select 
    a.smonth,
	a.performance_province_name dist,
	a.performance_city_name,
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
		performance_city_name,
		customer_code,
		sum(sale_amt)sale_amt,
		sum(profit) profit,sum(profit)/sum(sale_amt) prorate
	from csx_dws.csx_dws_sale_detail_di
	where sdt>='20200901' and sdt<regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),0),'-','')  -- 上上月1日 -- 昨日月1日
	and customer_code not in ('112092')
	group by substr(sdt,1,6),order_code,performance_province_name,customer_code,performance_city_name
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
group by a.smonth,a.performance_province_name,performance_city_name,a.customer_code,b.customer_name,b.sign_date,b.sales_user_number,b.sales_user_name)

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

;


--- impala 查询方式 计算方式有问题待处理

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
	if(sum(a.sale_amt)=0,0,sum(a.profit)/sum(a.sale_amt)) prorate,
--	round(sum(a.sale_amt)*0.02,2) salery_sales,
--	round(if(if(sum(a.sale_amt)=0,0,(sum(a.profit)/sum(a.sale_amt))-0.15)>=0 and a.customer_code not in('104179','112092'),if(sum(a.sale_amt)=0,0,(sum(a.profit)/sum(a.sale_amt)-0.15)),0)*sum(a.sale_amt)*0.5,2) salery_fnl,
--	round(sum(a.sale_amt)*0.02,2)+round(if(if(sum(a.sale_amt)=0,0,(sum(a.profit)/sum(a.sale_amt)-0.15))>=0 and a.customer_code not in('104179','112092'),if(sum(a.sale_amt)=0,0,(sum(a.profit)/sum(a.sale_amt)-0.15)),0)*sum(a.sale_amt)*0.5,2) salery_cal,
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
	where sdt>='20200901' and sdt<'20231001'  -- 上上月1日 -- 昨日月1日
	and customer_code not in ('112092')
	group by substr(sdt,1,6),order_code,performance_province_name,customer_code
	)a
join 
  (
   select 
	distinct substr(sdt,1,6) smonth,
	customer_code,
	customer_name,
	case when customer_code='104179' then '-' 
		 when customer_code='112092' then '-'
		 else sales_user_number end as sales_user_number,
	case when customer_code='104179' then '徐桂莹' 
		 when customer_code='112092' then '蒋光平'
		 else sales_user_name end as sales_user_name,
	regexp_replace(to_date(first_sign_time), '-', '') as sign_date
	from csx_dim.csx_dim_crm_customer_info 
	where sdt='20230930'  -- 上月最后1日
	and (channel_code='9' or customer_code in  ('123629'))
	-- '122221' 已更正为业务代理人 '116401' 202205月开始不算业务代理人改为大客户 127155
	) b on b.customer_code=a.customer_code 
left join 
	(
	 select 
		source_bill_no as order_code,	--  来源单号
		sum(unpaid_amount) unpaid_amount	--  未回款金额
	 from csx_dws.csx_dws_sss_order_credit_invoice_bill_settle_detail_di  -- 销售单对账
	 where sdt='20230930'
	       and to_date(happen_date)<='2023-09-30'
	 group by source_bill_no
	)c on c.order_code=a.order_code
left join 
	(
	 select 
		source_bill_no as order_code,	--  来源单号
		sum(unpaid_amount) unpaid_amount	--  未回款金额
	from csx_dws.csx_dws_sss_order_credit_invoice_bill_settle_detail_di  -- 销售单对账
	where sdt='20230831'
	and to_date(happen_date)<='2023-08-31'
	group by source_bill_no
	)d on d.order_code=a.order_code	
left join 
	(
	-- 核销流水明细表中已核销金额
	select customer_code,regexp_replace(to_date(happen_date),'-','') smonth,
	sum(case when regexp_replace(to_date(paid_time),'-','')>='20230901' 
			  and regexp_replace(to_date(paid_time),'-','')<= '20230930'
			 then pay_amt end ) payment_amount_by,
	sum(case when regexp_replace(to_date(paid_time),'-','')>='20230801' 
			  and regexp_replace(to_date(paid_time),'-','')<= '20230831'
			 then pay_amt end ) payment_amount_sy,			 
		sum(pay_amt) payment_amount
	from
		csx_dwd.csx_dwd_sss_close_bill_account_record_di a
	where  (regexp_replace(to_date(happen_date),'-','')<='20230930' or happen_date='' or happen_date is NULL)
	and regexp_replace(to_date(paid_time ),'-','') <=     '20230930' 
	and delete_flag ='0'
	group by customer_code,regexp_replace(to_date(happen_date ),'-','')
	)e on e.customer_code=a.customer_code and e.smonth=a.smonth -- e.close_bill_no=a.order_code	
group by a.smonth,a.performance_province_name,a.customer_code,b.customer_name,b.sign_date,b.sales_user_number,b.sales_user_name)
-- 202209 丁芳不算毛利提成
-- -- -- 提成计算有问题待核查
-- 结果1：客户提成
select *,
	if(coalesce(payment_amount_by,0)=0,0,if(coalesce(sale_amt,0)*coalesce(salery_sales,0)=0,0,least(cast(coalesce(sale_amt,0) as decimal(32,6)),cast(coalesce(payment_amount_by,0) as decimal(32,6)) )/coalesce(sale_amt,0)*coalesce(salery_sales,0)*1)+if(coalesce(sale_amt,0)*coalesce(salery_fnl,0)=0,0,least(cast(coalesce(payment_amount_by,0) as decimal(32,6)) ,cast(coalesce(sale_amt,0) as decimal(32,6)) )/coalesce(sale_amt,0)*coalesce(salery_fnl,0)))	
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
from temp_dali_cust_01 a
left join
( --  获取本月客户回款金额
  select
    customer_code, --  客户编码
    sum(claim_amt) as claim_amt,	-- 回款金额（未使用，含补救单）
    sum(paid_amt) as paid_amt,	-- 回款已核销金额
    sum(residue_amt) as residue_amt	-- 回款未核销金额
  from csx_dwd.csx_dwd_sss_money_back_di --  sdt以过账日期分区，只看20200601及以后的，该表计算回款只计算已核销金额
  where (sdt>='20230901' and sdt<='20230930') 

  and regexp_replace(to_date(update_time),'-','')<='20230930'  -- 回款以过账日期为准，但每次已核销金额发生变化更新日期都会变化，此表无法查历史回款已核销金额
  and (paid_amt<>0 or residue_amt<>0) -- 剔除补救单和对应原单
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
  where (sdt>='20200601' and sdt<='20230930') 
  and regexp_replace(to_date(update_time),'-','')<='20230930'  -- 回款以过账日期为准，但每次已核销金额发生变化更新日期都会变化，此表无法查历史回款已核销金额
  and (paid_amt<>0 or residue_amt<>0) -- 剔除补救单和对应原单 
  group by customer_code
) e on a.customer_code = e.customer_code  
) a
-- where a.sales_user_name='金可成'
