---------财务周转天数
--SAP:csx_tmp.ads_fr_account_receivables
--新系统的：csx_dws.csx_dws_sss_customer_invoice_bill_settle_stat_di
-- 昨日季度 1日，昨日、上季度1日，上季度末; 
/*set i_sdate_m11 ='20221001';
set i_sdate_m12 ='20221231';
set i_sdate_n12 ='2022-11-01';
set i_sdate_n11 ='2022-10-01';
set i_sdate_m21 ='20220701';		
set i_sdate_m22 ='20220930';

set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

insert overwrite table csx_tmp.sale_d_customer_accounting_period  partition(sdt)
select
  c.sales_province_name,
  c.city_group_name,
  c.fourth_supervisor_work_no,
  c.fourth_supervisor_name,
  c.third_supervisor_work_no,
  c.third_supervisor_name, 
  c.second_supervisor_work_no,
  c.second_supervisor_name, 
  c.first_supervisor_work_no,
  c.first_supervisor_name,
  DATEDIFF('2023-04-01','2023-01-01')as accounting_cnt,
  coalesce(max(excluding_tax_sales),0) excluding_tax_sales,
  avg(receivable_amount) receivable_amount,
  current_date as update_date,
  '20230630' sdt	
from 
(
  select
    a.sdt,
    a.province_name    sales_province_name,
    a.city_group_name,
    c.province_manager_user_number fourth_supervisor_work_no,
    c.province_manager_user_name fourth_supervisor_name,
    c.city_manager_user_number third_supervisor_work_no,
    c.city_manager_user_name third_supervisor_name, 
    c.sales_manager_user_number second_supervisor_work_no,
    c.sales_manager_user_name second_supervisor_name, 
    c.supervisor_user_number first_supervisor_work_no,
    c.supervisor_user_name first_supervisor_name, 
    sum(b.excluding_tax_sales) excluding_tax_sales,
    sum(a.receivable_amount) receivable_amount
  from (
       
  	 select
         sdt,
         channel_name,
         province_name,
         city_group_name,
         customer_code,
         sum(receivable_amount)  receivable_amount --应收账款
       from 
         csx_dws.csx_dws_sss_customer_invoice_bill_settle_stat_di
       where (sdt='20230630'  or sdt='20221231')  
         and channel_code  in ('1','7','9')
       group by sdt,
         channel_name,
         province_name,
         city_group_name,
         customer_code
  	   )a
  LEFT join (
  			select 
                customer_code,
  			  sum(sale_amt_no_tax) as excluding_tax_sales
              from   csx_dws.csx_dws_sale_detail_di
              where sdt >='20230401'   and sdt <='20230630'
  			and  channel_code in ('1','7','9') 
  			group by customer_code
  			)b on a.customer_code=b.customer_code 
  LEFT join
          (
              select *
              from csx_dim.csx_dim_crm_customer_info
              where sdt= '20230630'
             and channel_code  in ('1','7','9')
           ) c on a.customer_code=c.customer_code 
  group by a.sdt,
          a.province_name,
a.city_group_name,
c.province_manager_user_number ,
c.province_manager_user_name ,
c.city_manager_user_number ,
c.city_manager_user_name , 
c.sales_manager_user_number ,
c.sales_manager_user_name , 
c.supervisor_user_number ,
c.supervisor_user_name 
)c	
group by c.sales_province_name,
         c.city_group_name,
         c.fourth_supervisor_work_no,
         c.fourth_supervisor_name,
         c.third_supervisor_work_no,
         c.third_supervisor_name, 
         c.second_supervisor_work_no,
         c.second_supervisor_name, 
         c.first_supervisor_work_no,
         c.first_supervisor_name; */
-- business_type_name增加字段;

-----应收周转天数用期末城市

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
  DATEDIFF('2023-10-01','2023-07-01')as accounting_cnt,
  coalesce(max(excluding_tax_sales),0) excluding_tax_sales,
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
    sum(b.excluding_tax_sales) excluding_tax_sales,
    sum(a.receivable_amount) receivable_amount
  from 
   ( 
  	 select
         sdt,
         channel_name,
         province_name,
         city_group_name,
         customer_code,
         sum(receivable_amount)  receivable_amount --应收账款
       from 
         csx_dws.csx_dws_sss_customer_credit_invoice_bill_settle_stat_di
		 -- csx_dws_sss_customer_invoice_bill_settle_stat_di
       where (sdt='20230930'  or sdt='20230630')  
         and channel_code  in ('1','7','9')
		  and customer_code not in ('120459','121206')
       group by sdt,
         channel_name,
         province_name,
         city_group_name,
         customer_code
  	   )a
  LEFT join (
  			select 			
                customer_code,
  			  sum(sale_amt_no_tax) as excluding_tax_sales
              from   csx_dws.csx_dws_sale_detail_di
              where sdt >='20230701'   and sdt <='20230930'
  			and  channel_code in ('1','7','9') 
  			group by customer_code
  			)b on a.customer_code=b.customer_code 
  LEFT join
          (
              select *
              from csx_dim.csx_dim_crm_customer_info
              where sdt= '20230930'
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
group by   c.performance_province_name,
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
		

		
------------明细数据 周转
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
  DATEDIFF('2023-10-01','2023-07-01')as accounting_cnt,
  coalesce(max(excluding_tax_sales),0) excluding_tax_sales,
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
    sum(b.excluding_tax_sales) excluding_tax_sales,
    sum(a.receivable_amount) receivable_amount
  from (
       
  	 select
         sdt,
         channel_name,
         province_name,
         city_group_name,
         customer_code,
         sum(receivable_amount)  receivable_amount --应收账款
       from 
         csx_dws.csx_dws_sss_customer_credit_invoice_bill_settle_stat_di
		 -- csx_dws_sss_customer_invoice_bill_settle_stat_di
       where (sdt='20230930'  or sdt='20230630')
         and customer_code not in ('120459','121206')	   
         and channel_code  in ('1','7','9')
       group by sdt,
         channel_name,
         province_name,
         city_group_name,
         customer_code
  	   )a
  LEFT join (
  			select 
                customer_code,
  			  sum(sale_amt_no_tax) as excluding_tax_sales
              from   csx_dws.csx_dws_sale_detail_di
              where sdt >='20230701'   and sdt <='20230930'
  			and  channel_code in ('1','7','9') 
  			group by customer_code
  			)b on a.customer_code=b.customer_code 
  LEFT join
          (
              select *
              from csx_dim.csx_dim_crm_customer_info
              where sdt= '20230930'
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
c.supervisor_user_name ,
c.sales_user_number,
	c.sales_user_name,
	a.customer_code,
	c.customer_name
)c	
-- where c.performance_province_name='福建省'
group by c.performance_province_name,
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
		 
insert overwrite table csx_tmp.sale_d_customer_finance  partition(sdt)
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
           where sdt >='20230701'  and sdt <= '20230930'
		    and customer_code not in ('120459','121206') -- 城市总正常考核
			    and business_type_code in ('1','2','4','6') and channel_code in ('1','7','9') 
		   group by customer_code,performance_province_name,performance_city_name,substr(sdt,1,6),business_type_name
		)a			
LEFT join
          (
            select *
            from csx_dim.csx_dim_crm_customer_info
            where sdt= '20230930'
           and channel_code  in ('1','7','9')
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
	  a.smonth ;
	  
---------------  应收账款明细  当期客户期初期末对外B端应收账款
 select
         sdt,
         channel_name,
         province_name,
         city_group_name,
         customer_code,
		 customer_name,
         sum(receivable_amount)  receivable_amount -- 应收账款
       from 
         csx_dws.csx_dws_sss_customer_credit_invoice_bill_settle_stat_di
		-- csx_dws_sss_customer_invoice_bill_settle_stat_di
       where (sdt='20230930'  or sdt='20230630')  
         and channel_code  in ('1','7','9') -- and province_name='福建省'
       group by sdt,
         channel_name,
         province_name,
         city_group_name,
         customer_code,customer_name;
