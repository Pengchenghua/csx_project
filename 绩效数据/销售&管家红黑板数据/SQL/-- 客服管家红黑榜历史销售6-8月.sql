 -- 客服管家红黑榜历史销售6-8月
 -- 管家红黑榜
-- 管家信息
 
-- drop table csx_analyse_tmp.csx_analyse_tmp_hr_service_info;
create table csx_analyse_tmp.csx_analyse_tmp_hr_service_info as 
with sales_info as (
  select substr(sdt, 1, 6) as smt,
    performance_region_name,
    performance_province_name,
    performance_city_name,
    customer_code as customer_no,
    customer_name,
    case when customer_code in ('250879') then '81241667' 
        when customer_code in ('249548') then '81261644'
        -- when customer_code in ('247826') then 
    else service_manager_user_number end  service_user_work_no,
    case when customer_code in ('250879') then '肖少萍' 
        when customer_code in ('249548') then '王毅姣'
    else service_manager_user_name  end service_user_name,
    business_attribute_code attribute_code,
    business_attribute_name attribute_name,
    case
      when business_attribute_code = 1 then 1
      when business_attribute_code = 2 then 2
      when business_attribute_code = 5 then 6
    end business_type_code,
    service_manager_user_position,
    sales_user_name,
    sales_user_number,
    sales_user_position,
    -- count() over(partition by customer_code, business_attribute_code ) as cnt,
    -- row_number() over(partition by customer_code,business_attribute_code order by service_manager_user_number asc  ) as ranks,
    current_timestamp() as update_time
  from csx_dim.csx_dim_crm_customer_business_ownership
  where 
  sdt = '20241130' 
  and (
    customer_code not in ('250879', '249548', '127661', '104281')
    or (
      customer_code = '104281' 
      and service_manager_user_number != '81273957'  -- 剔除程智
    )
    or (
      customer_code in ('250879', '249548')
      and service_manager_user_position = 'CUSTOMER_SERVICE_MANAGER'
    )
    )
  
       
    -- and customer_code='237857'
    -- and service_manager_user_id <> 0 
    -- and customer_code='111207'
    -- and business_attribute_code='1'
)
select *,
    count() over(partition by customer_no, attribute_code ) as cnt,
    row_number() over(partition by customer_no,attribute_code order by service_user_work_no asc  ) as ranks
from sales_info 
-- where ranks=1
;


-- drop table csx_analyse_tmp.csx_analyse_tmp_hr_service_sale_01 ;
create table csx_analyse_tmp.csx_analyse_tmp_hr_service_sale_01 as 
with tmp_sale_detail as 
    (select substr(sdt, 1, 6) sale_month,
        performance_province_name,
        performance_region_name,
        performance_city_name,
        -- 这样判断主要是管家信息中没有前置仓业务，日配业务中包含前置仓
        if(a.business_type_code='4','1',a.business_type_code) new_business_type_code,
        -- business_type_code,
        a.customer_code,
        customer_name,
        sales_user_name,
        sales_user_number,
        sales_user_position,
        if(substr(first_business_sale_date,1,6)= substr(sdt,1,6), 1, 0) as new_customer_flag,
        sum(sale_amt) sale_amt,
        sum(profit) profit
    from csx_dws.csx_dws_sale_detail_di a   
    left join 
    -- 关联商机新客
      (select customer_code,
              business_type_code,
              first_business_sale_date
       from csx_dws.csx_dws_crm_customer_business_active_di
        where sdt='current' 
      ) b on a.customer_code=b.customer_code and a.business_type_code=b.business_type_code 
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
        and shipper_code='YHCSX'
    group by substr(sdt, 1, 6),
        performance_province_name,
        performance_region_name,
        performance_city_name,
        if(a.business_type_code='4','1',a.business_type_code),
        -- a.business_type_code,
        a.customer_code,
        customer_name,
        sales_user_name,
        sales_user_number,
        sales_user_position,
        if(substr(first_business_sale_date,1,6)= substr(sdt,1,6), 1, 0) 
    ),
    tmp_sale_detail_hr as 
    (select  sale_month,
        a.performance_province_name,
        a.performance_region_name,
        a.performance_city_name,
        a.new_business_type_code,
        a.customer_code,
        a.customer_name,
        a.sales_user_name,
        a.sales_user_number,
        a.sales_user_position,
        service_user_work_no,
        service_user_name,
        service_manager_user_position,
        new_service_user_work_no,
        new_service_user_name,
        new_service_manager_user_position,
        new_customer_flag,        
        sum(sale_amt) sale_amt,
        sum(profit) profit
     from
     (select  sale_month,
        a.performance_province_name,
        a.performance_region_name,
        a.performance_city_name,
        a.new_business_type_code,
        a.customer_code,
        a.customer_name,
        a.sales_user_name,
        a.sales_user_number,
        a.sales_user_position,
        service_user_work_no,
        service_user_name,
        service_manager_user_position,
        case when coalesce(b.service_manager_user_position,'') ='CUSTOMER_SERVICE_MANAGER'   then b.service_user_work_no
            -- when a.sales_user_position='CUSTOMER_SERVICE_MANAGER' and  coalesce(b.service_manager_user_position,'') =''  then a.sales_user_number 
            when a.sales_user_position='CUSTOMER_SERVICE_MANAGER' and  a.sales_user_number !=b.service_user_work_no  then a.sales_user_number
            else '' end new_service_user_work_no,
        case when coalesce(b.service_manager_user_position,'') ='CUSTOMER_SERVICE_MANAGER' then b.service_user_name
            when a.sales_user_position='CUSTOMER_SERVICE_MANAGER' and  a.sales_user_number !=b.service_user_work_no  then a.sales_user_name
            else '' end new_service_user_name,
        case when coalesce(b.service_manager_user_position,'') ='CUSTOMER_SERVICE_MANAGER' then b.service_manager_user_position
            when a.sales_user_position='CUSTOMER_SERVICE_MANAGER' and  a.sales_user_number !=b.service_user_work_no then a.sales_user_position
            else '' end  new_service_manager_user_position,
        new_customer_flag,        
        sale_amt,
        profit
    from tmp_sale_detail a 
    left join 
    (select * from  csx_analyse.csx_analyse_fr_hr_red_balck_service_manager_info 
    where smt= substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
        and sdt= regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
    )  b on a.customer_code=b.customer_no and a.new_business_type_code=b.business_type_code
    )a 
    group by 
     sale_month,
        a.performance_province_name,
        a.performance_region_name,
        a.performance_city_name,
        a.new_business_type_code,
        a.customer_code,
        a.customer_name,
        a.sales_user_name,
        a.sales_user_number,
        a.sales_user_position,
        service_user_work_no,
        service_user_name,
        service_manager_user_position,
        new_service_user_work_no,
        new_service_user_name,
        new_service_manager_user_position,
        new_customer_flag
    )
    select a.*,
        sale_amt/c.cnt as avg_sale_amt,
        profit/c.cnt as avg_profit,
        c.cnt
    from   tmp_sale_detail_hr a
     left join 
    (select 
        sale_month,
        customer_code,
        new_business_type_code,
        count(  new_service_user_work_no) cnt
    from tmp_sale_detail_hr
    group by customer_code,
        new_business_type_code,
        sale_month
    ) c on a.customer_code=c.customer_code
         and a.new_business_type_code=c.new_business_type_code
         and a.sale_month=c.sale_month

    ;

    
select sale_month,
        a.performance_province_name,
        a.performance_region_name,
        a.performance_city_name,
        a.new_business_type_code,
        a.customer_code,
        a.customer_name,
        new_service_user_work_no,
        new_service_user_name,
        new_service_manager_user_position,
        new_customer_flag,        
        sale_amt,
        profit,
        profit/sale_amt as profit_rate,
        avg_sale_amt,
        avg_profit,
        avg_profit/avg_sale_amt as avg_profit_rate,
        cnt
from csx_analyse_tmp.csx_analyse_tmp_hr_service_sale_01 a 
;


select  a.sale_month,
        a.performance_province_name,
        a.performance_region_name,
        a.performance_city_name,
        a.new_service_user_work_no,
        a.new_service_user_name,
        sum(avg_sale_amt) avg_sale_amt,
        sum(avg_profit)  avg_profit,
        if(sum(avg_sale_amt)=0,0,sum(avg_profit)/sum(avg_sale_amt)) as profit_rate
    from csx_analyse_tmp.csx_analyse_tmp_hr_service_sale_01 a
    group by a.sale_month,
        a.performance_province_name,
        a.performance_region_name,
        a.performance_city_name,
        a.new_service_user_work_no,
        a.new_service_user_name
  