
-- 客户评价明细
 
with sales_info as   
(  
    select    smt,
        performance_region_name,  
        performance_province_name,
        a.customer_no, 
        customer_name,
        service_user_work_no,  
        service_user_name,  
        service_manager_user_position 
    from      csx_analyse.csx_analyse_fr_hr_red_balck_service_manager_info   a 
    join 
    (select customer_code,
        substr(sdt,1,6) as s_month
    from  csx_dws.csx_dws_sale_detail_di 
        where sdt>= '20250101'
            and sdt<='20250430'
        and business_type_code=1
        and order_channel_code =1   -- B端出库
    group by customer_code,
        substr(sdt,1,6) 
    ) b on a.customer_no=b.customer_code and a.smt=b.s_month
    where smt >= '202501'
        and business_type_code in ('1','2','5')
      --  and service_manager_user_position='CUSTOMER_SERVICE_MANAGER'
    group by performance_region_name,  
        a.customer_no, 
        customer_name,
        service_user_work_no ,  
        service_user_name ,  
        -- service_manager_user_id ,
        service_manager_user_position,
        performance_province_name,
        smt
) 
 select a.smt,
 performance_region_name,
 performance_province_name,
 service_user_work_no,
 service_user_name,
 customer_no,
 customer_name,
 sum(answer_score) answer_score,
 if(answer_flag=1,'是','否') answer_flag
 from ( 
    select  si.smt,
        si.performance_region_name,  
        performance_province_name,
        si.service_user_work_no, 
        si.service_user_name,
        si.customer_no,
        si.customer_name,
        if(se.answer is not null ,1,0) as answer_flag,
        case   
            when se.answer = '非常满意' then 10  
            when se.answer = '满意' then 8  
            when se.answer = '一般' then 6  
            when se.answer = '不满意' then 2  
            when se.answer = '非常不满意' then 0  
            else 30   
        end as answer_score  
    from sales_info si  
    left join 
    (select
  a.id,
  a.form_key,
  a.form_item_id,
  a.type,
  a.label,
  answer,
  a.customer_code, 
  a.user_id,
  a.create_time,
   substr(regexp_replace(add_months(create_time,-1),'-',''),1,6) as create_month,
  rn
from
csx_analyse_tmp.csx_analyse_tmp_hr_service_evaluation a) se on se.customer_code = si.customer_no  and  create_month = si.smt
) a 

where service_user_work_no is not null and  service_user_work_no!=''
-- and a.customer_no='176210'
group by performance_region_name,
 service_user_work_no,
 service_user_name,
 customer_no,
 customer_name,
 if(answer_flag=1,'是','否') ,
 performance_province_name,
 a.smt

;


select distinct substr(create_time,1,7) from  csx_analyse_tmp.csx_analyse_tmp_hr_service_evaluation
-- 客户评价
-- drop table csx_analyse_tmp.csx_analyse_tmp_hr_service_evaluation;
create table csx_analyse_tmp.csx_analyse_tmp_hr_service_evaluation as 
with tmp_study_fm_user_form_data as (
  select
    a.id,
    a.form_key,
    regexp_replace(a.original_data, '\\\{|\\\}', '') as original_data,
    b.tag_code as customer_code,
    a.user_id,
    a.create_time,
     substr(regexp_replace(add_months(a.create_time,-1),'-',''),1,6)  create_month
  from
      csx_ods.csx_ods_csx_b2b_study_fm_user_form_data_df a
    LEFT JOIN csx_ods.csx_ods_csx_b2b_study_questionnaire_paper_df b ON a.paper_id = b.id
  WHERE
    b.is_delete = 0
    and a.create_time<'2025-05-14 00:00:00'   -- 次月需要限制创建日期
    and a.create_time>='2025-01-01 00:00:00 '
    -- AND a.form_key = 'Gznp3ieG'
),
tmp_study_fm_user_form_data_clean as (
  select
    t1.id,
    t1.form_key,
    t1.key_element as form_item_id,
    coalesce(t2.type, '') as type,
    coalesce(t2.label, '') as label,
    t1.value_element as answer,
    t1.customer_code,
    t1.user_id,
    t1.create_time,
    create_month
  from
    (
      select
        id,
        form_key,
        trim(split(original_data_element, ':') [ 0 ]) as key_element,
        trim(split(original_data_element, ':') [ 1 ]) as value_element,
        customer_code,
        user_id,
        create_time,
        create_month
      from
        (
          select
            id,
            form_key,
            trim(regexp_replace(original_data_element, '\\\"', '')) as original_data_element,
            customer_code,
            user_id,
            create_time,
            create_month
          from
            tmp_study_fm_user_form_data lateral view explode(split(original_data, ",")) t as original_data_element
        ) tmp
    ) t1
    left join csx_ods.csx_ods_csx_b2b_study_fm_user_form_item_df t2 on t1.key_element = t2.form_item_id
    and t1.form_key = t2.form_key
)
select
  a.id,
  a.form_key,
  a.form_item_id,
  a.type,
  a.label,
  coalesce(b.answer, a.answer) as answer,
  a.customer_code,
  a.user_id,
  a.create_time,
  rn
from
  (
    select
      *,
      dense_rank()over(partition by customer_code,create_month order by id desc ) as rn 
    from
      tmp_study_fm_user_form_data_clean
    WHERE
      form_item_id not like '%label'
  ) a
  left join (
    select
      id,
      form_key,
      split(form_item_id, 'label') [ 0 ] as original_form_item_id,
      answer
    from
      tmp_study_fm_user_form_data_clean
    WHERE
      form_item_id like '%label'
  ) b on a.id = b.id
  and a.form_key = b.form_key
  and a.form_item_id = b.original_form_item_id
  where a.type='RADIO'
    and rn=1 ;
    
    select * from csx_analyse_tmp.csx_analyse_tmp_hr_service_evaluation where customer_code='254208'