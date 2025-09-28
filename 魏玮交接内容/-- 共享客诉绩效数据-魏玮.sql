-- 共享客诉绩效数据-魏玮
select  
    coalesce(b.user_number,c.user_number) user_number,
    a.hand_person,
    a.name as user_name,
    a.complaint_code,
    coalesce(b.source_user_position,c.source_user_position) source_user_position,
    d.name as postition_name
from 

(select
    performance_province_code,
    hand_person,
    name,
    complaint_code
from
    csx_analyse.csx_analyse_fr_oms_complaint_detail_new_di
   LATERAL VIEW explode(split(hand_person, ',')) t AS name
where
  ((sdt >= '20250726'
  and sdt<='20250825')
  or complaint_status_code not in (30, -1)
  )
  and reject_reason!=''
--   and hand_person ='刘芳,林秀钗,黄凤'
)a 
left join 
(select user_number,user_name,province_id ,source_user_position
from csx_dim.csx_dim_uc_user where sdt='current') b 
on a.name=b.user_name and performance_province_code=province_id
left join 
(select user_number,user_name ,source_user_position
from csx_dim.csx_dim_uc_user 
where sdt='current'
    and user_source_business=1) c on a.name=c.user_name
 left join 
 (select dic_key as code,dic_value as name
       from csx_ods.csx_ods_csx_b2b_ucenter_user_dic_df
       where sdt=regexp_replace(date_sub(current_date(),1),'-','')
       and dic_type = 'POSITION'
) d on coalesce(b.source_user_position,c.source_user_position) =d.code
where d.name like '%共享%'
group by   
    a.hand_person,
    a.name,
    a.complaint_code,
    d.name,
    coalesce(b.user_number,c.user_number) ,
    coalesce(b.source_user_position,c.source_user_position) 
 ;