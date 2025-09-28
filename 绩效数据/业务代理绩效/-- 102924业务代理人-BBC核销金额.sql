-- 102924业务代理人-BBC核销金额
with tmp_sale_detail as
 (
select substr(sdt,1,6) smonth,
	   a.order_code,
	    case when substr(split(a.order_code,'-')[0],1,1)='B' and substr(split(a.order_code,'-')[0],-1,1) in ('A','B','C','D','E') then substr(split(a.order_code,'-')[0],2,length(split(a.order_code,'-')[0])-2)
			 when substr(split(a.order_code,'-')[0],1,1)='B' and substr(split(a.order_code,'-')[0],-1,1) not in ('A','B','C','D','E') then substr(split(a.order_code,'-')[0],2,length(split(a.order_code,'-')[0])-1)
			 else split(a.order_code,'-')[0]
		 end as source_bill_no_new, 
		a.performance_province_name,
		a.performance_city_name,
		a.customer_code,
		a.customer_name,
		a.sales_user_number,
		a.sales_user_name ,
		a.sign_company_code,
		sum(sale_amt )sale_amt,
		sum(profit) profit,
		coalesce(sum(profit)/sum( sale_amt ),0) prorate,
		business_type_name
	from csx_dws.csx_dws_sale_detail_di  a 
	where sdt>='20250101' and  a.customer_code in ('102924')
	   -- and ='BBC'
	group by  
	    substr(sdt,1,6)  ,
	   a.order_code,
	    case when substr(split(a.order_code,'-')[0],1,1)='B' and substr(split(a.order_code,'-')[0],-1,1) in ('A','B','C','D','E') then substr(split(a.order_code,'-')[0],2,length(split(a.order_code,'-')[0])-2)
			 when substr(split(a.order_code,'-')[0],1,1)='B' and substr(split(a.order_code,'-')[0],-1,1) not in ('A','B','C','D','E') then substr(split(a.order_code,'-')[0],2,length(split(a.order_code,'-')[0])-1)
			 else split(a.order_code,'-')[0]
		 end ,
		a.performance_province_name,
		a.performance_city_name,
		a.customer_code,
		a.customer_name,
		a.sales_user_number,
		a.sales_user_name ,
 		a.sign_company_code,
		 
		business_type_name 
),

	-- 本月历史回款金额
tmp_bill_account_record_snapshot as (
select a.customer_code,
    a.source_bill_no_new,
    a.company_code,
    sum(a.pay_amt) pay_amt,
-- 	sum(b_pay_amt) b_pay_amt,
	sum(last_pay_amt) last_pay_amt,
	sum(unpay_amt) unpay_amt,
	max(pay_amt_bill) pay_amt_bill,
	sum(case when regexp_replace(substr(a.paid_date,1,10),'-','')>= regexp_replace(add_months(trunc('${yesterdate}','MM'),-1),'-','') 
			  and regexp_replace(substr(a.paid_date,1,10),'-','')<= regexp_replace(last_day(add_months('${yesterdate}',-1)),'-','')
			 then a.new_pay_amt end ) payment_amount_by,   -- 本月核销
	sum(case when regexp_replace(substr(a.paid_date,1,10),'-','')>=regexp_replace(add_months(trunc('${yesterdate}','MM'),-2),'-','') 
		  and regexp_replace(substr(a.paid_date,1,10),'-','')<= regexp_replace(last_day(add_months('${yesterdate}',-2)),'-','')
			 then coalesce(last_pay_amt,0) end ) payment_amount_sy
	from (
            SELECT 
                a.customer_code,
                a.smonth,
                a.source_bill_no_new,
                a.company_code,
                a.paid_date,
                SUM(a.pay_amt_old) AS pay_amt,
                SUM(unpay_amt) AS unpay_amt,
                MAX(a.order_amt) AS pay_amt_bill,
                SUM(pay_amt ) AS new_pay_amt
            FROM csx_analyse.csx_analyse_customer_verification_detail_mf a
                where smt = substr(regexp_replace(last_day(add_months('${yesterdate}',-1)),'-',''),1,6)
            GROUP BY 
                a.customer_code,
                a.smonth,
                a.source_bill_no_new,
                a.company_code,
                a.paid_date
            )a 
            left join 
            ( SELECT 
                a.customer_code,
                a.source_bill_no_new,
                paid_date,
                SUM(pay_amt ) AS last_pay_amt
            FROM csx_analyse.csx_analyse_customer_verification_detail_mf a
                where smt = substr(regexp_replace(last_day(add_months('${yesterdate}',-2)),'-',''),1,6)
            GROUP BY 
                a.source_bill_no_new,
                a.customer_code,
                paid_date
            )b on a.source_bill_no_new=b.source_bill_no_new and a.paid_date=b.paid_date
    group by a.customer_code,
    a.source_bill_no_new,
    a.company_code
)

select 
    a.smonth,
	a.performance_province_name performance_province_name,
	a.performance_city_name,
	business_type_name,
	a.customer_code,
	a.customer_name cust_name,
	sign_company_code,
	a.sales_user_number,
	a.sales_user_name,
	sum(a.sale_amt) sale_amt,
	sum(a.profit) profit,
	coalesce(sum(a.profit)/(sum(a.sale_amt)),0) prorate,
-- 	round(sum(a.sale_amt)*0.02,2) salery_sales,
	sum(coalesce(e.payment_amount_by,0)) payment_amount_by,   -- 本月核销
	sum(coalesce(e.payment_amount_sy,0)) payment_amount_sy,	    -- 上月核销
	sum(coalesce(e.payment_amount_by,0)) *0.03 as push_money,
	regexp_replace('${yesterdate}','-','') as sdt
from 
	 tmp_sale_detail  a
left join 
	tmp_bill_account_record_snapshot e on   a.sign_company_code=e.company_code and a.customer_code=e.customer_code
	and e.source_bill_no_new=a.source_bill_no_new 
 where a.customer_code in ('102924')
-- where   a.source_bill_no_new='OM25042600083354'
group by  a.smonth,
	a.performance_province_name  ,
	a.performance_city_name,
	a.customer_code,
	a.customer_name  ,
	a.sign_company_code,
	a.sales_user_number,
	a.sales_user_name,
	business_type_name

  
;
 


 -- 使用impala 执行 定时调度使用
 -- 102924业务代理人-BBC核销金额
with tmp_sale_detail as
 (
select substr(sdt,1,6) smonth,
	   a.order_code,
	    CASE
  WHEN substr(split_part(a.order_code, '-', 1), 1, 1) = 'B'
       AND substr(split_part(a.order_code, '-', 1), -1, 1) IN ('A','B','C','D','E')
    THEN substr(split_part(a.order_code, '-', 1), 2, length(split_part(a.order_code, '-', 1)) - 2)
  WHEN substr(split_part(a.order_code, '-', 1), 1, 1) = 'B'
       AND substr(split_part(a.order_code, '-', 1), -1, 1) NOT IN ('A','B','C','D','E')
    THEN substr(split_part(a.order_code, '-', 1), 2, length(split_part(a.order_code, '-', 1)) - 1)
  ELSE split_part(a.order_code, '-', 1)
END as source_bill_no_new, 
		a.performance_province_name,
		a.performance_city_name,
		a.customer_code,
		a.customer_name,
		a.sales_user_number,
		a.sales_user_name ,
		a.sign_company_code,
		sum(sale_amt )sale_amt,
		sum(profit) profit,
		if(sum( sale_amt )=0,0,coalesce(sum(profit)/sum( sale_amt ),0)) prorate,
		business_type_name
	from csx_dws.csx_dws_sale_detail_di  a 
	where sdt>='20250101' and  a.customer_code in ('102924')
	   -- and ='BBC'
	group by  
	    substr(sdt,1,6)  ,
	   a.order_code,
	    CASE
  WHEN substr(split_part(a.order_code, '-', 1), 1, 1) = 'B'
       AND substr(split_part(a.order_code, '-', 1), -1, 1) IN ('A','B','C','D','E')
    THEN substr(split_part(a.order_code, '-', 1), 2, length(split_part(a.order_code, '-', 1)) - 2)
  WHEN substr(split_part(a.order_code, '-', 1), 1, 1) = 'B'
       AND substr(split_part(a.order_code, '-', 1), -1, 1) NOT IN ('A','B','C','D','E')
    THEN substr(split_part(a.order_code, '-', 1), 2, length(split_part(a.order_code, '-', 1)) - 1)
  ELSE split_part(a.order_code, '-', 1)
END,
		a.performance_province_name,
		a.performance_city_name,
		a.customer_code,
		a.customer_name,
		a.sales_user_number,
		a.sales_user_name ,
 		a.sign_company_code,
		business_type_name 
),

	-- 本月历史回款金额
tmp_bill_account_record_snapshot as (
select a.customer_code,
    a.source_bill_no_new,
    a.company_code,
    sum(a.pay_amt) pay_amt,
-- 	sum(b_pay_amt) b_pay_amt,
	sum(last_pay_amt) last_pay_amt,
	sum(unpay_amt) unpay_amt,
	max(pay_amt_bill) pay_amt_bill,
	sum(case when regexp_replace(substr(a.paid_date,1,10),'-','')>= regexp_replace(to_date(add_months(trunc('2025-09-22','MM'),-1)),'-','') 
			  and regexp_replace(substr(a.paid_date,1,10),'-','')<= regexp_replace(to_date(last_day(add_months('2025-09-22',-1))),'-','')
			 then a.new_pay_amt end ) payment_amount_by,   -- 本月核销
	sum(case when regexp_replace(substr(a.paid_date,1,10),'-','')>=regexp_replace(to_date(add_months(trunc('2025-09-22','MM'),-2)),'-','') 
		  and regexp_replace(substr(a.paid_date,1,10),'-','')<= regexp_replace(to_date(last_day(add_months('2025-09-22',-2))),'-','')
			 then coalesce(last_pay_amt,0) end ) payment_amount_sy
	from (
            SELECT 
                a.customer_code,
                a.smonth,
                a.source_bill_no_new,
                a.company_code,
                a.paid_date,
                SUM(a.pay_amt_old) AS pay_amt,
                SUM(unpay_amt) AS unpay_amt,
                MAX(a.order_amt) AS pay_amt_bill,
                SUM(pay_amt ) AS new_pay_amt
            FROM csx_analyse.csx_analyse_customer_verification_detail_mf a
                where smt = substr(regexp_replace(to_date(last_day(add_months('2025-09-22',-1))),'-',''),1,6)
            GROUP BY 
                a.customer_code,
                a.smonth,
                a.source_bill_no_new,
                a.company_code,
                a.paid_date
            )a 
            left join 
            ( SELECT 
                a.customer_code,
                a.source_bill_no_new,
                paid_date,
                SUM(pay_amt ) AS last_pay_amt
            FROM csx_analyse.csx_analyse_customer_verification_detail_mf a
                where smt = substr(regexp_replace(to_date(last_day(add_months('2025-09-22',-2))),'-',''),1,6)
            GROUP BY 
                a.source_bill_no_new,
                a.customer_code,
                paid_date
            )b on a.source_bill_no_new=b.source_bill_no_new and a.paid_date=b.paid_date
    group by a.customer_code,
    a.source_bill_no_new,
    a.company_code
)

select 
    a.smonth,
	a.performance_province_name performance_province_name,
	a.performance_city_name,
	business_type_name,
	a.customer_code,
	a.customer_name cust_name,
	sign_company_code,
	a.sales_user_number,
	a.sales_user_name,
	sum(a.sale_amt) sale_amt,
	sum(a.profit) profit,
	coalesce(sum(a.profit)/(sum(a.sale_amt)),0) prorate,
	round(sum(a.sale_amt)*0.02,2) salery_sales,
	sum(coalesce(e.payment_amount_by,0)) payment_amount_by,   -- 本月核销
	sum(coalesce(e.payment_amount_sy,0)) payment_amount_sy,	    -- 上月核销
	sum(coalesce(e.payment_amount_by,0)) *0.03 as push_money,
	regexp_replace(to_date('2025-09-22'),'-','') as sdt
from 
	 tmp_sale_detail  a
left join 
	tmp_bill_account_record_snapshot e on   a.sign_company_code=e.company_code and a.customer_code=e.customer_code
	and e.source_bill_no_new=a.source_bill_no_new 
 where a.customer_code in ('102924')
-- where   a.source_bill_no_new='OM25042600083354'
group by  a.smonth,
	a.performance_province_name  ,
	a.performance_city_name,
	a.customer_code,
	a.customer_name  ,
	a.sign_company_code,
	a.sales_user_number,
	a.sales_user_name,
	business_type_name

  
;
 