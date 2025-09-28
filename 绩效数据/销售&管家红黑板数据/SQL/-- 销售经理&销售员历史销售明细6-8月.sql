-- 销售经理&销售员历史销售明细6-8月
-- 销售员信息
-- drop table csx_analyse_tmp.csx_analyse_tmp_hr_sale_info_01;
create table csx_analyse_tmp.csx_analyse_tmp_hr_sale_info_01 as 
with position_dic as 
(select dic_key as code,dic_value as name
       from csx_ods.csx_ods_csx_b2b_ucenter_user_dic_df
       where sdt=regexp_replace(date_sub(current_date(),1),'-','')
       and dic_type = 'POSITION'
),
leader_info as 
  (select a.*,
    c.name as leader_user_position_name,
    b.name as leader_source_user_position_name 
    from 
    (SELECT
      *,
      row_number() over(PARTITION BY user_id,sdt ORDER BY distance asc) AS rank
    FROM     csx_dim.csx_dim_uc_user_extend 
    WHERE sdt ='20250131'
   -- and  leader_user_position in ('POSITION-26064','POSITION-26623','POSITION-25844')
   -- and user_position_type='SALES'
    AND status=0
    )a 
    left join position_dic b on user_position=b.code
    left join position_dic c on a.user_position_type=c.code
    where rank=1
  )
select 
  a.sdt,
  a.user_id,
  a.user_number,
  a.user_name,
  coalesce(a.user_position,source_user_position)user_position ,
  replace(c.name,'（旧）','') user_position_name,
  d.name as sub_position_name,
  a.begin_date,
  a.source_user_position,
  a.leader_user_id,
  a.new_leader_user_id,
  f.user_number as new_leader_user_number,
  f.user_name as new_leader_user_name,
  a.province_id,
  a.province_name,
  a.city_code,
  a.city_name,
  b.user_number leader_user_number,
  b.user_name leader_user_name,
  b.user_position_type leader_user_position,
  b.leader_user_position_name,
  b.user_position leader_source_user_position,
  b.leader_source_user_position_name  
from 
 (
select
  a.sdt,
  user_id,
  user_number,
  user_name,
  coalesce(user_position,source_user_position)  user_position,
  begin_date,
  source_user_position,
  if(a.user_position in ('SALES_CITY_MANAGER','SALES_MANAGER'), user_id, leader_user_id ) leader_user_id,
   case when a.province_id='6' then '1000000565219'
      when a.city_code='320500' then '1000000567463'
      when a.province_id='26' then '1000000426003'
      when a.city_code='440300' then '1000000426252'
      when a.city_code='340100' then '1000000596953'
      else ''
  end new_leader_user_id,
  province_id,
  province_name,
  city_code,
  city_name
  from 
     csx_dim.csx_dim_uc_user a 
  left  join 
    (select distinct
        employee_name,
        employee_code,
        begin_date,
        record_type_name,
        sdt
    from csx_dim.csx_dim_basic_employee 
        where sdt ='20250131'
        and card_type=0 
      --  and record_type_code	!=4
    )b on a.user_number=b.employee_code and a.sdt=b.sdt
    where
    a.sdt ='20250131'
  --  and status=0 
 -- and (user_position like 'SALES%'
-- and user_name in ('江苏B','许佳惠')
  )a 
 left join leader_info  b on a.leader_user_id=b.user_id and a.sdt=b.sdt
 left join position_dic c on a.user_position=c.code
 left join position_dic d on a.source_user_position=d.code
 left join leader_info f on a.new_leader_user_id=f.user_id  and a.sdt=f.sdt

  ;
  
    
 select a.*,b.name,replace(b.name,'（旧）','') ,c.sub_name from   csx_analyse_tmp.csx_analyse_tmp_hr_sale_info  a 
 left join 
 (select dic_key as code,dic_value as name
       from csx_ods.csx_ods_csx_b2b_ucenter_user_dic_df
       where sdt='20240921'
       and dic_type = 'POSITION'
    ) b on a.user_position	=b.code
 left join 
 (select dic_key as code,dic_value as sub_name
       from csx_ods.csx_ods_csx_b2b_ucenter_user_dic_df
       where sdt='20240921'
       and dic_type = 'POSITION'
    ) c on a.source_user_position=c.code
 where user_name='李佩丽'
 
 
 ;

-- 销售明细

--  drop table  csx_analyse_tmp.csx_analyse_tmp_hr_sales_sale_01 ;
create table csx_analyse_tmp.csx_analyse_tmp_hr_sales_sale_01 as 
with 
    sale as 
    (select substr(sdt, 1, 6) sale_month,
        performance_province_name,
        performance_region_name,
        performance_city_name,
        a.business_type_code,
        a.business_type_name,
        a.customer_code,
        customer_name,
        sales_user_name,
        sales_user_number,
        sales_user_position,
        case when a.customer_code in ('245127') then  '81001273' 
            when a.customer_code in ('252180','252393','252460') then '80946479'
            else sales_user_number end new_sales_user_number,
        case when a.customer_code in ('245127') then  '徐培召' 
            when a.customer_code in ('252180','252393','252460') then '於佳'
            else sales_user_name end new_sales_user_name,    
        if(b.customer_code is not null, 1, 0) as new_customer_flag,
        sum(sale_amt) sale_amt,
        sum(profit) profit
    from csx_dws.csx_dws_sale_detail_di a   
    left join 
    -- 关联商机新客
      (select smonth,
            a.customer_no customer_code,
            business_type_code
        from
        (
        select smonth,customer_no,business_type_code
        from csx_analyse.csx_analyse_sale_d_customer_sign_new_about_di 
            where smonth in ('202501','202412','202411')
        union all
        select smonth,customer_no,business_type_code 
        from  csx_analyse.csx_analyse_sale_d_customer_new_about_di
            where smonth in  ('202501','202412','202411')
         )a) b on a.customer_code=b.customer_code and a.business_type_code=b.business_type_code  and substr(sdt,1,6)=b.smonth
    where sdt >= '20241101'
        and sdt <= '20250131'   
        and (a.business_type_code in ('1','2','6')  -- 1-日配、2-福利、6-BBC
            or (sales_user_number in ('81244592','81079752','80897025','81022821','81190209',
                                      '80946479','81102471','81254457','81119082','81149084',
                                      '81103064','81029025','81013168','81149084','81103064','81254457')
               and a.business_type_code =4)
            )
        and a.customer_code not in ('234036','224656','247525','243799','244172','237768')
        and sales_user_number not in ('81208614','81206921')
    group by substr(sdt, 1, 6),
        performance_province_name,
        performance_region_name,
        performance_city_name,
        a.business_type_code,
        a.customer_code,
        customer_name,
        sales_user_name,
        sales_user_number,
        sales_user_position,
        business_type_name,
        case when a.customer_code in ('245127') then  '81001273' 
            when a.customer_code in ('252180','252393','252460') then '80946479'
            else sales_user_number end ,
        case when a.customer_code in ('245127') then  '徐培召' 
            when a.customer_code in ('252180','252393','252460')then  '於佳'
            else sales_user_name end,
        if(b.customer_code is not null, 1, 0) 
    ),
    sale_01 as 
    (select  a.sale_month,
        performance_province_name,
        performance_region_name,
        performance_city_name,
        a.business_type_code,
        business_type_name,
        a.customer_code,
        customer_name,
        sales_user_name,
        sales_user_number,
        sales_user_position,
        user_position_name,
        sub_position_name,        
        begin_date,
        new_sales_user_number,
        new_sales_user_name,
        if(customer_code in ('235479'),'81180572',leader_user_number) leader_user_number,
        if(customer_code in ('235479'),'余杰', leader_user_name) leader_user_name,
        if(customer_code in ('235479'),'销售经理', leader_source_user_position_name) leader_source_user_position_name,
        new_leader_user_number,
        new_leader_user_name,
        new_customer_flag,
        sale_amt,
        profit
    from sale a 
    left join 
    ( select * ,
        substr(sdt,1,6) sale_month
    from csx_analyse_tmp.csx_analyse_tmp_hr_sale_info_01
   --  where user_number ='80879367'
    ) b on   a.new_sales_user_number=b.user_number
        -- and a.sale_month=b.sale_month
    ) 
     select * from   sale_01
   -- where sales_user_number='80879367'
     
     
       select a.sale_month,
        performance_province_name,
        performance_region_name,
        performance_city_name,
        -- a.business_type_code,
        business_type_name,
        a.customer_code,
        customer_name,
        sales_user_name,
        sales_user_number,
        sales_user_position,
        user_position_name,
        sub_position_name,
        new_sales_user_number,
        new_sales_user_name,
        begin_date,
        leader_user_number,
        leader_user_name,
        leader_source_user_position_name,    
        new_leader_user_number,
        new_leader_user_name,
        new_customer_flag,
        sum(sale_amt) sale_amt ,
        sum(profit) profit,
        sum(profit)/sum(sale_amt) profit_rate
    from csx_analyse_tmp.csx_analyse_tmp_hr_sales_sale_01 a 
    -- where leader_user_number != coalesce(new_leader_user_number,'')
    group by sale_month,
        performance_province_name,
        performance_region_name,
        performance_city_name,
        a.business_type_name,
        a.customer_code,
        customer_name,
        sales_user_name,
        sales_user_number,
        sales_user_position,
        user_position_name,
        sub_position_name,
        begin_date,
        leader_user_number,
        leader_user_name,
        leader_source_user_position_name,
        new_leader_user_number,
        new_leader_user_name,
        new_customer_flag,
        new_sales_user_number,
        new_sales_user_name
    
      ;
   -- where sales_user_number='80879367'
     
     
       select a.sale_month,
        performance_province_name,
        performance_region_name,
        performance_city_name,
        sales_user_name,
        sales_user_number,
        sales_user_position,
        user_position_name,
        sub_position_name,
        begin_date,
        leader_user_number,
        leader_user_name,
        leader_source_user_position_name,    
        new_customer_flag,
        sum(sale_amt) sale_amt ,
        sum(profit) profit,
        sum(profit)/sum(sale_amt) profit_rate
    from sale_01 a 
    -- where leader_user_number != coalesce(new_leader_user_number,'')
    group by sale_month,
        performance_province_name,
        performance_region_name,
        performance_city_name,
        sales_user_name,
        sales_user_number,
        sales_user_position,
        user_position_name,
        sub_position_name,
        begin_date,
        leader_user_number,
        leader_user_name,
        leader_source_user_position_name,
        new_customer_flag


        ;
-- 销售员汇总
  select
  a.sale_month,
  performance_province_name,
  performance_region_name,
  performance_city_name,
  new_sales_user_number,
  new_sales_user_name,
--   sales_user_position,
  user_position_name,
  sub_position_name,
  begin_date,
  leader_user_number,
  leader_user_name,
  leader_source_user_position_name,
  new_customer_flag,
  sum(sale_amt) sale_amt,
  sum(profit) profit,
  sum(profit) / sum(sale_amt) profit_rate
from
  csx_analyse_tmp.csx_analyse_tmp_hr_sales_sale_01 a -- where leader_user_number != coalesce(new_leader_user_number,'')
group by
  sale_month,
  performance_province_name,
  performance_region_name,
  performance_city_name,
 new_sales_user_number,
 new_sales_user_name,
--   sales_user_position,
  user_position_name,
  sub_position_name,
  begin_date,
  leader_user_number,
  leader_user_name,
  leader_source_user_position_name,
  new_sales_user_number,
        new_sales_user_name,
  new_customer_flag
  ;

  -- 销售经理汇总
 select a.sale_month,
        performance_province_name,
        performance_region_name,
        performance_city_name,
        leader_user_number,
        leader_user_name,
        leader_source_user_position_name,    
        sum(sale_amt) sale_amt ,
        sum(profit) profit,
        sum(profit)/sum(sale_amt) profit_rate
    from csx_analyse_tmp.csx_analyse_tmp_hr_sales_sale_01 a 
    where coalesce(new_leader_user_number,'')!= leader_user_number

    group by sale_month,
        performance_province_name,
        performance_region_name,
        performance_city_name,
      
        leader_user_number,
        leader_user_name,
        leader_source_user_position_name
      union all 
    select a.sale_month,
        performance_province_name,
        performance_region_name,
        performance_city_name,
        new_leader_user_number,
        new_leader_user_name,
        '' as leader_source_user_position_name,
        sum(sale_amt) sale_amt ,
        sum(profit) profit,
        sum(profit)/sum(sale_amt) profit_rate
    from csx_analyse_tmp.csx_analyse_tmp_hr_sales_sale_01 a 
    where new_leader_user_number is not null 
    group by sale_month,
        performance_province_name,
        performance_region_name,
        performance_city_name,
        new_leader_user_number,
        new_leader_user_name 
 