UPDATE performance_daily_paper_target set target_sale_amt=600
where performance_province_name='湖北省'
and smt='202308'
and channel_name='大客户'

csx_analyse.csx_analyse_temp_partner_cust_di 
-- -- 使用20230331 分区追加20230401 分区数据后  可更改使用目标表sdt前日数据
增加业务类型名称，履约日期
以福利商品为主
i_sdate n-1
i_sdate1 n-2

最后发送结果汇总为两张结果表的合集

-- 商机明细
select a.*,c.sales_user_number,c.sales_user_name,b.sale_amt
from
(
select * from csx_analyse.csx_analyse_sale_d_customer_sign_new_about_di 
where smonth in ('202307','202308','202309')
union all
select * from  csx_analyse.csx_analyse_sale_d_customer_new_about_di
where smonth in  ('202307','202308','202309')
 )a
LEFT join
  (
     select *
     from csx_dim.csx_dim_crm_customer_info
     where sdt='current'
           and channel_code  in ('1','7','9')
  ) c  on a.customer_no=c.customer_code 
left join 
   (
     select 
              substr(sdt,1,6) smonth,
               customer_code,
               business_type_code,
                sum(sale_amt) as sale_amt
     from   csx_dws.csx_dws_sale_detail_di
     where  sdt>='20230701' and sdt<='20230930'
                and business_type_code in (1,2,6) and channel_code in ('1','7','9') 
     group by substr(sdt,1,6),
			 customer_code,
             business_type_code
             )b on a.customer_no=b.customer_code and a.business_type_code=b.business_type_code 
			   and a.smonth=b.smonth ;

			   
select 
province_name		销售省区名称,
city_group_name		销售城市名称,
fourth_supervisor_work_no		省区总工号,
fourth_supervisor_name		省区总姓名,
third_supervisor_work_no		城市总工号,
third_supervisor_name		城市总姓名,
second_supervisor_work_no		销售经理工号,
second_supervisor_name		销售经理姓名,
first_supervisor_work_no		主管工号,
first_supervisor_name		主管姓名,
business_type_code		业务类型编码,
business_type_name		业务类型名称,
customer_no		客户编码,
customer_name		客户名称,
update_date		更新日期,
end_date		履约日期,
smonth		统计月
from  csx_analyse.csx_analyse_sale_d_customer_sign_new_about_di
union all		   
select 
province_name		销售省区名称,
city_group_name		销售城市名称,
fourth_supervisor_work_no		省区总工号,
fourth_supervisor_name		省区总姓名,
third_supervisor_work_no		城市总工号,
third_supervisor_name		城市总姓名,
second_supervisor_work_no		销售经理工号,
second_supervisor_name		销售经理姓名,
first_supervisor_work_no		主管工号,
first_supervisor_name		主管姓名,
business_type_code		业务类型编码,
business_type_name		业务类型名称,
customer_no		客户编码,
customer_name		客户名称,
update_date		更新日期,
first_business_sale_date	履约日期,
smonth		统计月
from  csx_analyse.csx_analyse_sale_d_customer_new_about_di

------sdt=20231010	更新bbc 其他包含null的问题	   
	已经重新230705	   
insert overwrite table csx_analyse.csx_analyse_sale_d_customer_business_number_di partition (sdt)
select 
  a.customer_code as customer_no,
  a.business_number,
  a.business_type_code,
  a.business_sign_time as sign_time,
  a.start_date,
  if(ook=1 and b.customer_code is not null,'${i_sdate}','99990101') as end_date,
  if(ook=1 and b.customer_code is not null,'已核销','未核销') as business_type,
  other_needs_code,
  '${i_sdate}' as sdt
from 
  (
     select
      customer_code,
		  business_number,
		  business_type_code,
		  business_sign_time,
		  start_date,
		  other_needs_code,
		  row_number() over(partition by customer_code,business_type_code,other_needs_code order by business_sign_time) ook
        from
		(
		select
          customer_no as customer_code,
		  business_number,
		  business_type_code,
		  from_unixtime(unix_timestamp(sign_time,'yyyy-MM-dd HH:mm:ss')) as business_sign_time,
		  start_date,
		   other_needs_code
        from csx_analyse.csx_analyse_sale_d_customer_business_number_di
        where  sdt='${i_sdate1}' and start_date>='20210801' and  business_type='未核销'
		union all 
		/*select
          customer_no as customer_code,
		  business_number,
		  business_type_code,
		  from_unixtime(unix_timestamp(sign_time,'yyyy-MM-dd HH:mm:ss')) as business_sign_time,
		  sign_date as start_date,
		  if(business_type_code='1','其他','餐卡') as other_needs_code
        from csx_analyse.csx_analyse_temp_partner_cust_di
        where sdt='${i_sdate1}' and  business_type='未核销'
        union all*/
        select
           customer_code,
		   business_number,
           cast(business_type_code as string) business_type_code,
           from_unixtime(unix_timestamp(business_sign_time,'yyyy-MM-dd HH:mm:ss')) business_sign_time,
           regexp_replace(substr(business_sign_time,1,10),'-','')  start_date,
		   case when business_type_code=6 and other_needs_code='1' then '餐卡'
		        when business_type_code=6 and (other_needs_code<>'1' or other_needs_code is null) then '非餐卡'
				else '其他'  end  as other_needs_code
        FROM
              csx_dim.csx_dim_crm_business_info       
        WHERE  sdt='${i_sdate}' and business_stage = 5 
             and business_type_code in (1,2,6)  
        	 AND regexp_replace(substr(business_sign_time,1,10),'-','')='${i_sdate}' 
      )a
  )a		
left join   
-- 销售表
(
   select    
	a.customer_code,
	a.business_type_code,
    if(a.business_type_code in (1,2),'其他',b.credit_pay_type_name) as credit_pay_type_name
   from  csx_dws.csx_dws_sale_detail_di a
   left join 
      (
	    SELECT 
          id,
          if(credit_pay_type_name='餐卡','餐卡','非餐卡') as credit_pay_type_name
        from csx_dws.csx_dws_bbc_sale_detail_di
        where  sdt='${i_sdate}' 
	   )b on a.id=b.id
   where a.sdt='${i_sdate}'  
         AND  a.order_channel_code<>4 
         and a.business_type_code in (1,2,6)
   group by customer_code,business_type_code,if(a.business_type_code in (1,2),'其他',b.credit_pay_type_name)
)b on a.customer_code=b.customer_code and a.business_type_code=b.business_type_code and a.other_needs_code=b.credit_pay_type_name;



------------------2222222222222222222222

insert overwrite table csx_analyse.csx_analyse_sale_d_customer_sign_new_about_di partition (smonth)

select
c.performance_province_name      sales_province_name,
c.performance_city_name     city_group_name,
c.province_manager_user_number fourth_supervisor_work_no,
c.province_manager_user_name fourth_supervisor_name,
c.city_manager_user_number third_supervisor_work_no,
c.city_manager_user_name third_supervisor_name, 
c.sales_manager_user_number second_supervisor_work_no,
c.sales_manager_user_name second_supervisor_name, 
c.supervisor_user_number first_supervisor_work_no,
c.supervisor_user_name first_supervisor_name, 
a.business_type_code,
a.customer_code as customer_no,
c.customer_name,
current_date as update_date,
case when a.business_type_code='1' then '日配业务'
     when a.business_type_code='2' then '福利业务'
	 when a.business_type_code='6' then 'BBC'  end as business_type_name,
a.end_date,
a.smonth 	
from
(
 
SELECT substr(sdt,1,6) smonth,business_type_code, customer_no as customer_code,min(end_date) end_date
from csx_analyse.csx_analyse_sale_d_customer_business_number_di 
where sdt>=regexp_replace(trunc('${i_sdate}','MM'),'-','')  and sdt<=regexp_replace('${i_sdate}','-','') 
and business_type='已核销' and other_needs_code in ('非餐卡','其他') and business_type_code in ('2','6')
group by   substr(sdt,1,6) ,business_type_code, customer_no
union all
select
substr(end_date,1,6) as smonth,a.business_type_code,a.customer_code,end_date
from 
    ( 
	SELECT 
	   business_type_code, customer_no as customer_code,min(end_date) end_date
    from csx_analyse.csx_analyse_sale_d_customer_business_number_di 
     where sdt>=regexp_replace(trunc('${i_sdate}', 'YYYY'),'-','')  and sdt<=regexp_replace('${i_sdate}','-','') 
       and business_type='已核销' and other_needs_code in ('餐卡','其他') and business_type_code in ('1','6')
     group by   business_type_code, customer_no
	 ) a 
left join 
		(
		 select
          customer_no as customer_code,
		  business_type_code
        from csx_analyse.csx_analyse_temp_partner_cust_di
        where sdt='20230331' and  business_type='已核销'
		group by customer_no,
		  business_type_code
		  )b on a.customer_code=b.customer_code and a.business_type_code=b.business_type_code
where b.customer_code is null
and substr(end_date,1,6)=substr(regexp_replace('${i_sdate}','-',''),1,6)
)a
LEFT join
          (
            select *
            from csx_dim.csx_dim_crm_customer_info
            where sdt= regexp_replace('${i_sdate}','-','')
           and channel_code  in ('1','7','9')
          ) c on a.customer_code=c.customer_code 
		  
group by c.performance_province_name,
c.performance_city_name,
c.province_manager_user_number ,
c.province_manager_user_name ,
c.city_manager_user_number ,
c.city_manager_user_name , 
c.sales_manager_user_number ,
c.sales_manager_user_name , 
c.supervisor_user_number ,
c.supervisor_user_name ,
a.business_type_code,
a.smonth,
a.customer_code,
c.customer_name,
case when a.business_type_code='1' then '日配业务'
     when a.business_type_code='2' then '福利业务'
	 when a.business_type_code='6' then 'BBC'  end as business_type_name,
a.end_date
;
---------------------------------------



--------------------------------------------

insert overwrite table csx_analyse.csx_analyse_sale_d_customer_new_about_di partition (smonth)	  
select
c.performance_province_name      sales_province_name,
c.performance_city_name     city_group_name,
c.province_manager_user_number fourth_supervisor_work_no,
c.province_manager_user_name fourth_supervisor_name,
c.city_manager_user_number third_supervisor_work_no,
c.city_manager_user_name third_supervisor_name, 
c.sales_manager_user_number second_supervisor_work_no,
c.sales_manager_user_name second_supervisor_name, 
c.supervisor_user_number first_supervisor_work_no,
c.supervisor_user_name first_supervisor_name, 
f.business_type_code,
f.customer_code as customer_no,
c.customer_name,
current_date as update_date,
f.business_type_name,
f.first_business_sale_date,
substr(regexp_replace('${i_sdate}','-',''),1,6) smonth	
from  (SELECT a.customer_code,a.customer_name,a.business_type_code,a.business_type_name,a.first_business_sale_date
            FROM (select customer_code,
						customer_name,
						business_type_code,
						business_type_name,
						first_business_sale_date						
			      from csx_dws.csx_dws_crm_customer_business_active_di
			      where  sdt='current' and 
				   business_type_code in (1,2,6)
				  and  first_business_sale_date>=regexp_replace(trunc('${i_sdate}','MM'),'-','') and first_business_sale_date<=regexp_replace('${i_sdate}','-','')
				  ) a 
            left join 
			   (select * from csx_analyse.csx_analyse_sale_d_customer_sign_new_about_di where smonth=substr(regexp_replace('${i_sdate}','-',''),1,6) ) b 
			     on a.customer_code=b.customer_no	and a.business_type_code=b.business_type_code
            WHERE b.customer_no is null 			
			) f 
LEFT join
          (
            select *
            from csx_dim.csx_dim_crm_customer_info
            where sdt= regexp_replace('${i_sdate}','-','')
           and channel_code  in ('1','7','9')
          ) c on f.customer_code=c.customer_code 
 group by   c.performance_province_name,
c.performance_city_name,
c.province_manager_user_number ,
c.province_manager_user_name ,
c.city_manager_user_number ,
c.city_manager_user_name , 
c.sales_manager_user_number ,
c.sales_manager_user_name , 
c.supervisor_user_number ,
c.supervisor_user_name ,
f.business_type_code,
f.customer_code,
c.customer_name,
f.business_type_name,
f.first_business_sale_date;