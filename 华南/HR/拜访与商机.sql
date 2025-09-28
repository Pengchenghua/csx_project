
-- 拜访明细
select
  id,
  b.province_name,
  b.city_name,
  a.customer_id,
  a.customer_code,
  a.customer_name,
  a.business_attribute_code,
  a.business_attribute_name,
  last_business_sale_date,
  a.sales_user_id,
  a.sales_user_name,
  a.first_category_code,
  a.first_category_name,
  a.second_category_code,
  a.second_category_name,
  a.third_category_code,
  a.third_category_name,
  a.sign_time,
  a.visit_user_id,
  a.visit_user_number,
  a.visit_user_name,
  a.visit_user_position,
  a.visit_type_code,
  a.visit_type_name,
  a.visit_target_code,
  a.visit_target_name,
  a.visit_imgs_url,
  a.visit_time,
  a.visit_summary,
  a.visit_location,
  a.contact_person,
  a.contact_phone,
  a.address,
  a.create_by,
  a.create_time,
  a.update_by,
  a.update_time,
  a.sdt
from
  (
    select
      *
    from
      csx_dws.csx_dws_crm_customer_visit_record_di
    where
      sdt >= '20231101'
      and sdt <= '20231130'
  ) a
  left join (
    select
      user_id,
      province_name,
      city_name
    from
      csx_dim.csx_dim_uc_user
    where
      sdt = 'current'
      and user_id is not null
      and user_id <> 0
  ) b on b.user_id = a.visit_user_id
  left join (
    select
      customer_code,
      customer_name,
      if(business_type_code = 6, 5, business_type_code) business_type_code,
      business_type_name,
      last_business_sale_date
    from
      csx_dws.csx_dws_crm_customer_business_active_di
    where
      sdt = 'current'
      and business_type_code in (1, 2, 6)
  ) c on a.customer_code = c.customer_code
  and a.business_attribute_code = c.business_type_code
where
  b.province_name in('福建省', '广东省', '江西省');
--商机数据

--商机数据
select province_name 
,a.city_group_name   
,a.fourth_supervisor_work_no 
,a.fourth_supervisor_name    
,a.third_supervisor_work_no   
,a.third_supervisor_name     
,a.second_supervisor_work_no 
,a.second_supervisor_name    
,a.first_supervisor_work_no  
,a.first_supervisor_name     
,a.business_type_code        
,a.business_type_name        
,a.customer_no               
,a.customer_name             
,a.end_date                  
,a.smonth                    
,sales_user_number           
,sales_user_name             
,sale_amt                    
,a.update_date               
from
(
select * from csx_analyse.csx_analyse_sale_d_customer_sign_new_about_di 
where smonth in ('202311')
union all
select * from  csx_analyse.csx_analyse_sale_d_customer_new_about_di
where smonth in  ('202311')
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
                cast(business_type_code as string)  business_type_code,
                sum(sale_amt) as sale_amt
     from   csx_dws.csx_dws_sale_detail_di
     where  sdt>='20231101' and sdt<='20231131'
                and business_type_code in (1,2,6) and channel_code in ('1','7','9') 
     group by substr(sdt,1,6),
			 customer_code,
             business_type_code
             )b on a.customer_no=b.customer_code and a.business_type_code=b.business_type_code 
			   and a.smonth=b.smonth
where a.province_name in('福建省','广东省','江西省'); 			   

