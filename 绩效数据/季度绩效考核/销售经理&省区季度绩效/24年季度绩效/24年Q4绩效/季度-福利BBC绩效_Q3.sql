--大福利季度绩效
--B端销售汇总
select
    a.province_name      sales_province_name,
    a.city_group_name,
    c.province_manager_user_number fourth_supervisor_work_no,
    c.province_manager_user_name fourth_supervisor_name,
    c.city_manager_user_number third_supervisor_work_no,
    c.city_manager_user_name third_supervisor_name, 
    c.sales_manager_user_number second_supervisor_work_no,
    c.sales_manager_user_name second_supervisor_name, 
    c.supervisor_user_number first_supervisor_work_no,
    c.supervisor_user_name first_supervisor_name,   
    sum(sale_amt) sale_amt,
    sum(excluding_tax_profit)/sum(excluding_tax_sales) as excluding_tax_profit_rate,
    sum(profit) profit,
    sum(excluding_tax_sales) excluding_tax_sales,
    sum(excluding_tax_profit) excluding_tax_profit
from   (
        select 
        customer_code,
				performance_province_name province_name,
				performance_city_name city_group_name,
				substr(sdt,1,6) smonth,
				business_type_name,
				sum(sale_amt) as sale_amt,
				sum(profit) profit,
				sum(profit_no_tax) excluding_tax_profit,
				sum(sale_amt_no_tax) excluding_tax_sales
           from   csx_dws.csx_dws_sale_detail_di
           where sdt >='20241001'  and sdt <= '20241231'
		   -- and customer_code not in ('120459','121206')
			 and business_type_code in ('2','6') 
       -- and channel_code in ('1','7','9') 
		   group by customer_code,performance_province_name,performance_city_name,substr(sdt,1,6),business_type_name
		)a			
LEFT join
          (
            select *
            from csx_dim.csx_dim_crm_customer_info
            where sdt= '20241231'
         --  and channel_code  in ('1','7','9')
          ) c on a.customer_code=c.customer_code 
group by 
      a.province_name,
      a.city_group_name,
      c.province_manager_user_number ,
      c.province_manager_user_name ,
      c.city_manager_user_number ,
      c.city_manager_user_name , 
      c.sales_manager_user_number ,
      c.sales_manager_user_name , 
      c.supervisor_user_number ,
      c.supervisor_user_name ;


-- B端销售明细
select
    a.province_name      sales_province_name,
    a.city_group_name,
    c.province_manager_user_number fourth_supervisor_work_no,
    c.province_manager_user_name fourth_supervisor_name,
    c.city_manager_user_number third_supervisor_work_no,
    c.city_manager_user_name third_supervisor_name, 
    c.sales_manager_user_number second_supervisor_work_no,
    c.sales_manager_user_name second_supervisor_name, 
    c.supervisor_user_number first_supervisor_work_no,
    c.supervisor_user_name first_supervisor_name,   
    c.sales_user_number,
	  c.sales_user_name,
    a.customer_code customer_no,
    c.customer_name,
    a.smonth,
    a.business_type_name,
    sum(sale_amt) sale_amt,
    sum(profit) profit,
    sum(excluding_tax_sales) excluding_tax_sales,
    sum(excluding_tax_profit) excluding_tax_profit
from   (
          select 
                customer_code,
				performance_province_name province_name,
				performance_city_name city_group_name,
				substr(sdt,1,6) smonth,
				business_type_name,
				sum(sale_amt) as sale_amt,
				sum(profit) profit,
				sum(profit_no_tax) excluding_tax_profit,
				sum(sale_amt_no_tax) excluding_tax_sales
           from   csx_dws.csx_dws_sale_detail_di
           where sdt >='20241001'  and sdt <= '20241231'
		   -- and customer_code not in ('120459','121206')
			 and business_type_code in ('2','6') 
        --     and channel_code in ('1','7','9') 
		   group by customer_code,performance_province_name,performance_city_name,substr(sdt,1,6),business_type_name
		)a			
LEFT join
          (
            select *
            from csx_dim.csx_dim_crm_customer_info
            where sdt= '20241231'
        --   and channel_code  in ('1','7','9')
          ) c on a.customer_code=c.customer_code 
group by 
      a.province_name,
      a.city_group_name,
      c.province_manager_user_number ,
      c.province_manager_user_name ,
      c.city_manager_user_number ,
      c.city_manager_user_name , 
      c.sales_manager_user_number ,
      c.sales_manager_user_name , 
      c.supervisor_user_number ,
      c.supervisor_user_name,
	  c.sales_user_number,
	  c.sales_user_name,
      a.customer_code,
      c.customer_name,
      a.business_type_name,
	  a.smonth  ;

		   
-- 应收周转


-----应收周转天数用期末城市 销售取含税计算
with temp_company_credit as 
  ( select
  customer_code,
  credit_code,
  customer_name,
  business_attribute_code,
  business_attribute_name,
  company_code,
  status,
  is_history_compensate
from
    csx_dim.csx_dim_crm_customer_company_details
where
  sdt = 'current'
  -- and status=1
group by customer_code,
    credit_code,
    customer_name,
    business_attribute_code,
    business_attribute_name,
    company_code,
    status,
    is_history_compensate
) 
  select
  c.performance_province_name,
  c.performance_city_name,
  c.fourth_supervisor_work_no,
  c.fourth_supervisor_name,
  c.third_supervisor_work_no,
  c.third_supervisor_name, 
  c.second_supervisor_work_no,
  c.second_supervisor_name, 
  c.first_supervisor_work_no,
  c.first_supervisor_name,
  DATEDIFF('2024-12-31','2024-09-30')as accounting_cnt,
  coalesce(max(sale_amt),0) sale_amt,
  coalesce(max(excluding_tax_sales),0) excluding_tax_sales,
  avg(receivable_amount) receivable_amount,
  if(avg(receivable_amount)=0 or coalesce(max(sale_amt),0)=0,0,DATEDIFF('2024-12-31','2024-09-30')/(coalesce(max(sale_amt),0)/avg(receivable_amount))) as turnover_days
from 
(
  select
    a.sdt,
    c.performance_province_name,
    c.performance_city_name,
    c.province_manager_user_number fourth_supervisor_work_no,
    c.province_manager_user_name fourth_supervisor_name,
    c.city_manager_user_number third_supervisor_work_no,
    c.city_manager_user_name third_supervisor_name, 
    c.sales_manager_user_number second_supervisor_work_no,
    c.sales_manager_user_name second_supervisor_name, 
    c.supervisor_user_number first_supervisor_work_no,
    c.supervisor_user_name first_supervisor_name, 
-- 	c.sales_user_number,
-- 	c.sales_user_name,
-- 	a.customer_code,
-- 	c.customer_name,
	sum(sale_amt) sale_amt,
    sum(b.excluding_tax_sales) excluding_tax_sales,
    sum(a.receivable_amount) receivable_amount,
    if(avg(receivable_amount)=0 or coalesce(max(sale_amt),0)=0,0,DATEDIFF('2024-10-01','2024-07-01')/(coalesce(max(sale_amt),0)/avg(receivable_amount))) as turnover_days
  from (
    select
         sdt,
         channel_name,
        -- a.company_code,
         performance_province_name province_name,
         performance_city_name city_group_name,
         a.customer_code, 
        -- b.customer_name,
         sum(receivable_amount)  receivable_amount --应收账款
    from 
        -- csx_dws.csx_dws_sss_customer_credit_invoice_bill_settle_stat_di
        -- 取SAP 
        csx_dws.csx_dws_sap_customer_credit_settle_detail_di a
        left join temp_company_credit b on a.customer_code=b.customer_code 
            and a.credit_code=b.credit_code
            and a.company_code=b.company_code
       where   sdt='20241231'
         and b.business_attribute_code in ('2','5')
       group by sdt,
         channel_name,
         performance_province_name,
         performance_city_name,
         a.customer_code

    )a 
LEFT join (
  			select 
              customer_code,
              business_type_code,
              sum(sale_amt) sale_amt,
  			  sum(sale_amt_no_tax) as excluding_tax_sales
            from  csx_dws.csx_dws_sale_detail_di
              where sdt >='20241001'   and sdt <='20241231'
  			and  business_type_code in ('2','6')
  			group by customer_code,
  			    business_type_code
  			)b on a.customer_code=b.customer_code 
  			   
  LEFT join
          (
              select *
              from csx_dim.csx_dim_crm_customer_info
              where sdt= '20241231'
             and channel_code  in ('1','7','9')
           ) c on a.customer_code=c.customer_code 
  group by a.sdt,
    c.performance_province_name,
    c.performance_city_name,
    c.province_manager_user_number ,
    c.province_manager_user_name ,
    c.city_manager_user_number ,
    c.city_manager_user_name , 
    c.sales_manager_user_number ,
    c.sales_manager_user_name , 
    c.supervisor_user_number ,
    c.supervisor_user_name 

)c	
group by c.performance_province_name,
    c.performance_city_name,
    c.fourth_supervisor_work_no,
    c.fourth_supervisor_name,
    c.third_supervisor_work_no,
    c.third_supervisor_name, 
    c.second_supervisor_work_no,
    c.second_supervisor_name, 
    c.first_supervisor_work_no,
    c.first_supervisor_name
 ; 
		
--应收周转明细

------------明细数据 周转 销售取含税计算
with temp_company_credit as (
  select
    customer_code,
    credit_code,
    customer_name,
    business_attribute_code,
    business_attribute_name,
    company_code,
    status,
    is_history_compensate
  from
    csx_dim.csx_dim_crm_customer_company_details
  where
    sdt = 'current' -- and status=1
  group by
    customer_code,
    credit_code,
    customer_name,
    business_attribute_code,
    business_attribute_name,
    company_code,
    status,
    is_history_compensate
)
select
  c.performance_province_name,
  c.performance_city_name,
  c.fourth_supervisor_work_no,
  c.fourth_supervisor_name,
  c.third_supervisor_work_no,
  c.third_supervisor_name,
  c.second_supervisor_work_no,
  c.second_supervisor_name,
  c.first_supervisor_work_no,
  c.first_supervisor_name,
  c.sales_user_number,
  c.sales_user_name,
  c.customer_code,
  c.customer_name,
  DATEDIFF('2024-12-31','2024-09-30') as accounting_cnt,
  coalesce(max(sale_amt), 0) sale_amt,
  coalesce(max(excluding_tax_sales), 0) excluding_tax_sales,
  avg(receivable_amount) receivable_amount
from
  (
    select
      a.sdt,
      c.performance_province_name,
      c.performance_city_name,
      c.province_manager_user_number fourth_supervisor_work_no,
      c.province_manager_user_name fourth_supervisor_name,
      c.city_manager_user_number third_supervisor_work_no,
      c.city_manager_user_name third_supervisor_name,
      c.sales_manager_user_number second_supervisor_work_no,
      c.sales_manager_user_name second_supervisor_name,
      c.supervisor_user_number first_supervisor_work_no,
      c.supervisor_user_name first_supervisor_name,
      c.sales_user_number,
      c.sales_user_name,
      a.customer_code,
      c.customer_name,
      sum(sale_amt) sale_amt,
      sum(b.excluding_tax_sales) excluding_tax_sales,
      sum(a.receivable_amount) receivable_amount,
      if(
        avg(receivable_amount) = 0
        or coalesce(max(sale_amt), 0) = 0,
        0,
        DATEDIFF('2024-12-31','2024-09-30') / (coalesce(max(sale_amt), 0) / avg(receivable_amount))
      ) as turnover_days
    from
      (
        select
          sdt,
          channel_name,
          performance_province_name province_name,
          performance_city_name city_group_name,
          a.customer_code,
          sum(receivable_amount) receivable_amount --应收账款
        from
          -- csx_dws.csx_dws_sss_customer_credit_invoice_bill_settle_stat_di
          -- 取SAP
          csx_dws.csx_dws_sap_customer_credit_settle_detail_di a
          left join temp_company_credit b on a.customer_code = b.customer_code
          and a.credit_code = b.credit_code
          and a.company_code = b.company_code -- csx_dws_sss_customer_invoice_bill_settle_stat_di
        where
          sdt in ('20240930', '20241231')
          and b.business_attribute_code in ('2', '5') --  and customer_code not in ('120459','121206')
        group by
          sdt,
          channel_name,
          performance_province_name,
          performance_city_name,
          a.customer_code
      ) a
      LEFT join (
        select
          customer_code,
          sum(sale_amt) sale_amt,
          sum(sale_amt_no_tax) as excluding_tax_sales
        from
          csx_dws.csx_dws_sale_detail_di
        where
          sdt >= '20241001'
          and sdt <= '20241231'
          and business_type_code in ('2', '6')
        group by
          customer_code
      ) b on a.customer_code = b.customer_code
      LEFT join (
        select
          *
        from
          csx_dim.csx_dim_crm_customer_info
        where
          sdt = '20241231'
          and channel_code in ('1', '7', '9')
      ) c on a.customer_code = c.customer_code
    group by
      a.sdt,
      c.performance_province_name,
      c.performance_city_name,
      c.province_manager_user_number,
      c.province_manager_user_name,
      c.city_manager_user_number,
      c.city_manager_user_name,
      c.sales_manager_user_number,
      c.sales_manager_user_name,
      c.supervisor_user_number,
      c.supervisor_user_name,
      c.sales_user_number,
      c.sales_user_name,
      a.customer_code,
      c.customer_name
  ) c -- where c.performance_province_name='福建省'
group by
  c.performance_province_name,
  c.performance_city_name,
  c.fourth_supervisor_work_no,
  c.fourth_supervisor_name,
  c.third_supervisor_work_no,
  c.third_supervisor_name,
  c.second_supervisor_work_no,
  c.second_supervisor_name,
  c.first_supervisor_work_no,
  c.first_supervisor_name,
  c.sales_user_number,
  c.sales_user_name,
  c.customer_code,
  c.customer_name;


-- 当期客户期初期末对外B端应收账款 统一取中台
-- 取SAP应收表 输出表：csx_analyse.csx_analyse_fr_sap_subject_customer_credit_account_analyse_df
-- 中台核销 输出表：csx_dws.csx_dws_sss_customer_credit_invoice_bill_settle_stat_di
 select
         sdt,
         sdt,
         channel_name,
         performance_province_name province_name,
         performance_city_name city_group_name,
         customer_code,
		     customer_name,
         sum(receivable_amount)  receivable_amount -- 应收账款
       from 
         csx_dws.csx_dws_sss_customer_credit_invoice_bill_settle_stat_di   中台核销
      --  csx_analyse.csx_analyse_fr_sap_subject_customer_credit_account_analyse_df
		  -- csx_dws_sss_customer_invoice_bill_settle_stat_di
       where (sdt='20240331'  or sdt='20241231')  
         and channel_name  in ('大客户','项目供应商','虚拟销售员','业务代理','','其他') -- and province_name='福建省'
       group by  
         channel_name,
         performance_province_name province_name,
         performance_city_name city_group_name,
         customer_code,
         customer_name;



-- 当期客户期初期末对外B端应收账款 
-- 取SAP应收表 输出表：csx_analyse.csx_analyse_fr_sap_subject_customer_credit_account_analyse_df
-- Q2取中台
  select
         sdt,
         channel_name,
         performance_province_name province_name,
         performance_city_name city_group_name,
         customer_code,
		     customer_name,
         sum(receivable_amount)  receivable_amount -- 应收账款
       from 
        csx_dws.csx_dws_sap_subject_customer_settle_detail
      --  csx_analyse.csx_analyse_fr_sap_subject_customer_credit_account_analyse_df
		  -- csx_dws_sss_customer_invoice_bill_settle_stat_di
       where (sdt='20240630'  or sdt='20241231')  
         and channel_code  in ('1','7','9','13') -- and province_name='福建省'
       group by  sdt,
         channel_name,
         performance_province_name ,
         performance_city_name ,
         customer_code,
         customer_name;


-- 商机新客异常查找：
select
  a.*,
  b.min_sdt,
  b.max_sdt
from
  (
    select
      sdt,
      customer_code,
      business_number,
      cast(business_type_code as STRING) business_type_code,
      from_unixtime(
        unix_timestamp(business_sign_time, 'yyyy-MM-dd HH:mm:ss')
      ) business_sign_time,
      regexp_replace(substr(business_sign_time, 1, 10), '-', '') start_date,
      case
        when business_type_code = 6
        and other_needs_code = '1' then '餐卡'
        when business_type_code = 6
        and (
          other_needs_code <> '1'
          or other_needs_code is null
        ) then '非餐卡'
        else '其他'
      end as other_needs_code,
      business_stage
    from
      csx_dim.csx_dim_crm_business_info
    where
      sdt = 'current' --   business_stage = 5
      --  and to_date(business_sign_time )>='2024-04-01'
      and business_type_code in (1, 2, 6)
      and customer_code in ('211611')
  ) a
  left join (
    select
      customer_code,
      business_type_code,
      max(sdt) max_sdt,
      min(sdt) min_sdt
    from
      csx_dws.csx_dws_sale_detail_di
    where
      sdt >= '20240101'
    group by customer_code,
      business_type_code
  )b on a.customer_code=b.customer_code and a.business_type_code=b.business_type_code