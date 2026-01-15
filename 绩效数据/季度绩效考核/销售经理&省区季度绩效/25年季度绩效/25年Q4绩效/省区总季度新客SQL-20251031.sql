


drop table  csx_analyse_tmp.csx_analyse_tmp_break_cust_flag_Q4;
create table csx_analyse_tmp.csx_analyse_tmp_break_cust_flag_Q4 as 
with tmp_break_cust AS (
SELECT 
    c.performance_province_name,
    c.performance_city_name,
    a.customer_code,
    c.customer_name,
    a.max_sdt,
    regexp_replace(date_add( from_unixtime(unix_timestamp(a.max_sdt, 'yyyyMMdd'),'yyyy-MM-dd'), 90), '-', '') syear,
    a.business_type_code,
    -- 新增断约判断字段
    CASE WHEN regexp_replace(date_add( from_unixtime(unix_timestamp(a.max_sdt, 'yyyyMMdd'),'yyyy-MM-dd'), 90) , '-', '') < regexp_replace(trunc(last_day(add_months('2026-01-03',-3)),'MM'),'-','') 
         THEN '是' ELSE '否' END AS is_break_3month
FROM
    (
    SELECT 
        customer_code,
        business_type_code,
        MAX(sdt) AS max_sdt
    FROM 
        csx_dws.csx_dws_sale_detail_di 
    WHERE 
        sdt BETWEEN '20200101' 
        -- 判断客户销售在上上月末次销售日期
        AND   regexp_replace(last_day(add_months('2026-01-03',-4)),'-','') 
        AND business_type_code IN ('1') 
        AND channel_code IN ('1','7','9')
        AND order_channel_code NOT IN ('4','6','5')  -- 剔除调价、返利、价格补救
        AND refund_order_flag<>1    -- 剔除退货
    GROUP BY 
        customer_code,
        business_type_code
    ) a
LEFT JOIN   
    (
    SELECT 
        customer_code,
        performance_province_name,
        performance_city_name,
        customer_name
    FROM csx_dim.csx_dim_crm_customer_info
    WHERE sdt = 'current'
        AND channel_code IN ('1','7','9')
    ) c ON a.customer_code = c.customer_code 
),
-- 判断3个月以上客户断约后，新建商机属于新客
tmp_business as 
(
    select *,
        row_number() over(partition by customer_code order by business_sign_time desc) rn
    from 
    (
        select 
            a.customer_code,
            a.business_sign_time,
            b.max_sdt,
            b.is_break_3month,
            a.business_type_code,
            b.syear
        from csx_dim.csx_dim_crm_business_info a 
        join tmp_break_cust b on a.customer_code=b.customer_code 
        where a.sdt='current'
            -- 签约日期小于月初，取本月之前
            and to_date(a.business_sign_time) <= '2025-12-31'
            and a.business_attribute_code=1
            and a.status=1
    ) sub
),
-- 当月有销售的客户
tmp_sale as 
(
    select 
         
        performance_region_code,
        performance_region_name,
        performance_province_code,
        performance_province_name,
        performance_city_code,
        performance_city_name,
        business_type_code,
        business_type_name,
        customer_code,
        customer_name,
        sales_user_name,
        sales_user_number,
        min(sdt) min_sdt,
        sum(sale_amt) sale_amt,
        sum(profit) profit
    from csx_dws.csx_dws_sale_detail_di   
    where sdt >=       regexp_replace(trunc(last_day(add_months('2026-01-03',-3)),'MM'),'-','')
        and sdt <= '20251231'
        and shipper_code='YHCSX'
        and business_type_code in ('1','2','6','10')
        AND order_channel_code NOT IN ('4','6','5')  
        and refund_order_flag <>1    
    group by 
        performance_region_code,
        performance_region_name,
        performance_province_code,
        performance_province_name,
        performance_city_code,
        performance_city_name,
        business_type_code,
        business_type_name,
        customer_code,
        customer_name,
        sales_user_name,
        sales_user_number 
)
-- 断约客户
select 
    a.*,
    b.max_sdt as max_sdt,
    to_date(b.business_sign_time) business_sign_time,
    a.min_sdt as min_sale_sdt,
    -- 判断 断约前最后日期小于签约日期，且签约日期小于本月履约的最近日期，且is_break_3month='是' 为断约新开客
    case when a.business_type_code='1' 
            and (from_unixtime(unix_timestamp(b.max_sdt, 'yyyyMMdd'),'yyyy-MM-dd') <= to_date(b.business_sign_time) 
                 and to_date(b.business_sign_time) <= from_unixtime(unix_timestamp(a.min_sdt, 'yyyyMMdd'),'yyyy-MM-dd') 
                 and a.min_sdt <=   regexp_replace(last_day(add_months('2026-01-03',-1)),'-','')
                 )
        then '1' else '0' end flag,
    b.is_break_3month,
    row_number() over(partition by a.customer_code, a.business_type_code) rn
from tmp_sale a 
left join 
(
    select * from tmp_business   
    where is_break_3month='是'
        AND rn=1
) b on a.customer_code=b.customer_code and a.business_type_code=b.business_type_code
;




-- drop table csx_analyse_tmp.csx_analyse_tmp_break_cust_flag_Q4_01;
 create table csx_analyse_tmp.csx_analyse_tmp_break_cust_flag_Q4_01 as 
-- 准新客表
with tmp_cust_active as 
(
    select
        customer_code,
        cast(business_type_code as string) business_type_code,
        business_type_name,
        first_business_sign_date,
        first_business_sale_date min_sdt,
        last_business_sale_date max_sdt,
        substr(last_business_sale_date,1,6) as sale_month,
        '1' active_new_flag
    from csx_dws.csx_dws_crm_customer_business_active_di
    where sdt='current'
        and first_business_sale_date >=   regexp_replace(trunc(last_day(add_months('2026-01-03',-3)),'MM'),'-','') 
        and first_business_sale_date <= regexp_replace(last_day(add_months('2026-01-03',-1)),'-','') 
),
-- 商机新客 主要BBC\福利
tmp_business_new_cust as 
(
    select 
        a.customer_code,
        a.business_type_name,
        b.business_type_code,
        a.min_end_date,
        max(b.business_sign_date) business_sign_date
    from 
    (
        select
            customer_no customer_code,
            business_type_name,
            min(end_date) min_end_date  
        from csx_analyse.csx_analyse_sale_d_customer_sign_new_about_di
        left join 
        (
            select 
                customer_code,
                performance_province_name,
                performance_region_name
            from csx_dim.csx_dim_crm_customer_info
            where sdt='current'
        ) b on customer_no = b.customer_code
        where smonth >=    substr(regexp_replace(trunc(last_day(add_months('2026-01-03',-3)),'MM'),'-',''), 1, 6) 
        and  smonth <=      substr(regexp_replace(trunc(last_day(add_months('2026-01-03',-1)),'MM'),'-',''), 1, 6) 
            and business_type_code != 1 
        group by 
            customer_no,
            business_type_name
    ) a 
    left join 
    (
        select 
            customer_code,
            regexp_replace(to_date(business_sign_time),'-','') business_sign_date, 
            business_number,
            business_type_code,
            business_type_name
        from csx_dim.csx_dim_crm_business_info 
        where sdt='current' 
    ) b on a.customer_code=b.customer_code 
        and a.min_end_date >= b.business_sign_date 
        and a.business_type_name = b.business_type_name
    group by 
        a.customer_code,
        a.business_type_name,
        a.min_end_date,
        b.business_type_code
)
    
select 
    performance_region_code,
    performance_region_name,
    performance_province_code,
    performance_province_name,
    performance_city_code,
    performance_city_name,
    a.sales_user_name,
    a.sales_user_number,
    a.business_type_code,
    a.business_type_name,
    a.customer_code,
    a.customer_name,
    coalesce(regexp_replace(to_date(a.business_sign_time),'-',''), b.first_business_sign_date, c.business_sign_date) business_sign_date,
    coalesce(a.min_sale_sdt, b.min_sdt, min_end_date) first_sale_data,
    a.max_sdt as break_sale_sdt,            -- 断约日期末次销售 
    a.is_break_3month,
    case when a.flag='1' and a.is_break_3month='是' then '1' 
         when b.customer_code is not null and b.active_new_flag='1' then '1'
         when c.customer_code is not null then '1'
         else '0' 
    end new_flag 
from 
(
    select * from csx_analyse_tmp.csx_analyse_tmp_break_cust_flag_Q4
    where rn=1 
) a 
left join tmp_cust_active b on a.customer_code=b.customer_code and a.business_type_code=b.business_type_code
left join tmp_business_new_cust c on a.customer_code=c.customer_code and a.business_type_code=c.business_type_code
;


-- drop table csx_analyse_tmp.csx_analyse_sales_new_customer_info_mf_2025q3;

-- select * from csx_analyse_tmp.csx_analyse_tmp_break_cust_01 where customer_code='101585'



 create table csx_analyse_tmp.csx_analyse_sales_new_customer_info_mf_2025q3 as 
-- 新开客明细
with tmp_sale_info as   
(
    select
        case when a.province_name = '上海市' then '上海'
             when city_name in ('苏州市') then '上海'
             when city_name in ('深圳市') then '广东深圳' 
             when city_name in ('广州市') then '广东广州'
             when city_name in ('南京市') then '江苏南京'
             else province_name 
        end province_name,
        case when province_name in ('重庆市') then '重庆市' 
             when city_name in ('松江区') then '上海松江'
             when city_name in ('深圳市') then '广东深圳'
             when city_name in ('广州市') then '广东广州'
             when city_name in ('苏州市') then '江苏苏州'
             when city_name in ('南京市') then '南京主城'
             else a.city_name 
        end city_name,
        user_number,
        a.user_name,
        sub_position_name,
        begin_date,
        ceil(months_between(last_day(add_months('2026-01-03',-1)), from_unixtime(unix_timestamp(begin_date,'yyyyMMdd'),'yyyy-MM-dd'))) as work_age -- 工䶖月向上取
    from csx_analyse.csx_analyse_fr_hr_red_black_sale_info a 
    where smt=substr(regexp_replace(trunc(last_day(add_months('2026-01-03',-1)),'MM'),'-',''),1,6) 
        and employee_status='在职'
), 
tmp_sale_detail as 
(
    select
        substr(sdt,1,6) s_month,
        performance_region_code,
        performance_region_name,
        performance_province_code,
        performance_province_name,
        performance_city_code,
        performance_city_name,
        sales_user_number,
        sales_user_name,
        customer_code,
        customer_name,
        business_type_code,
        business_type_name,
        CASE
        WHEN business_type_code in ('2', '10') then '福利'
        when business_type_code = '1' then '日配'
        when business_type_name = '批发内购' then '内购'
        when business_type_name = '省区大宗' then '大宗贸易'
        else business_type_name
      end new_business_type_name,
        sum(sale_amt) sale_amt,
        sum(profit) profit 
    from csx_dws.csx_dws_sale_detail_di
    where sdt >=   regexp_replace(trunc(last_day(add_months('2026-01-03',-3)),'MM'),'-','')
        and sdt <= regexp_replace(last_day(add_months('2026-01-03',-1)),'-','')
    group by 
        substr(sdt,1,6),
        performance_region_code,
        performance_region_name,
        performance_province_code,
        performance_province_name,
        performance_city_code,
        performance_city_name,
        sales_user_number,
        sales_user_name,
        customer_code,    
        customer_name,
        business_type_code,
        business_type_name,
        CASE
        WHEN business_type_code in ('2', '10') then '福利'
        when business_type_code = '1' then '日配'
        when business_type_name = '批发内购' then '内购'
        when business_type_name = '省区大宗' then '大宗贸易'
        else business_type_name
      end 
),
tmp_new_cust_sale as 
(
    select
        b.s_month,
        b.performance_region_code,
        b.performance_region_name,
        b.performance_province_code,
        b.performance_province_name,
        b.performance_city_code,
        b.performance_city_name,
        c.province_manager_user_number,
        c.province_manager_user_name,
        c.province_manager_position,
        c.city_manager_user_number,
        c.city_manager_user_name,
        c.city_manager_position,
        c.sales_manager_user_number,
        c.sales_manager_user_name,
        c.sales_manager_position,
        c.supervisor_user_number,
        c.supervisor_user_name,
        c.supervisor_position,
        c.sale_number as sales_user_number,
        c.sale_name as sales_user_name,
        b.customer_code,
        b.customer_name,
        b.business_type_code,
        b.business_type_name,
        b.sale_amt,
        b.profit,
        a.first_sale_data,
        a.business_sign_date,
        a.break_sale_sdt,
        a.is_break_3month,
        a.new_flag
   from csx_analyse_tmp.csx_analyse_tmp_break_cust_flag_Q4_01 a 
    left join tmp_sale_detail b on a.customer_code=b.customer_code and a.business_type_code=b.business_type_code
    LEFT join csx_analyse_tmp.csx_analyse_tmp_quarterly_sale_info c on a.customer_code = c.customer_no 
        and  b.new_business_type_name     = c.attribute_name
    where a.new_flag=1
)
-- insert overwrite table csx_analyse.csx_analyse_sales_new_customer_info_mf partition(smt)
select 
    s_month,
    performance_region_code,
    performance_region_name,
    performance_province_code,
    performance_province_name,
    performance_city_code,
    performance_city_name,
    CASE
    WHEN a.performance_city_name IN ('北京市','福州市','重庆主城','深圳市','成都市','上海松江','南京主城','合肥市','西安市','石家庄市','江苏苏州','杭州市','郑州市','广东广州') THEN 'A/B'
    WHEN a.performance_city_name IN ('厦门市','宁波市','泉州市','莆田市','南平市','南昌市','贵阳市','宜宾','武汉市' ) THEN 'C'
    WHEN a.performance_city_name IN ('三明市', '阜阳市', '台州市', '龙岩市', '万州区', '江苏盐城', '黔江区', '永川区') then 'D'
    else 'D'
   END as city_type_name,
    province_manager_user_number,
    province_manager_user_name,
    province_manager_position,
    city_manager_user_number,
    city_manager_user_name,
    city_manager_position,
    sales_manager_user_number,
    sales_manager_user_name,
    sales_manager_position,
    supervisor_user_number,
    supervisor_user_name,
    supervisor_position,
    sales_user_number,
    sales_user_name,
    b.begin_date,
    b.work_age,
    b.sub_position_name,
    customer_code,
    customer_name,
    business_type_code,
    business_type_name,
    -- new_business_type_name,
    sale_amt,
    profit,
    first_sale_data,
    business_sign_date,
    break_sale_sdt,
    is_break_3month,
    new_flag,
    count(customer_code) over(partition by sales_user_number) cnt,
    current_timestamp() update_time,
    s_month as smt
from tmp_new_cust_sale a 
left join tmp_sale_info b on a.sales_user_number=b.user_number
;

 create table csx_analyse_tmp.csx_analyse_tmp_break_cust_flag_Q4_02 as 
with   tmp_customer_level as (select
  region_name,
  province_name,
  city_group_name,
  customer_no,
  customer_name,
  first_category_name,
  second_category_name,
  last_order_date,			-- 最后一次下单时间 需要注意这个是全业务的，下季度要调整20250731
  work_no,
  sales_name,
  sales_value,
  profit,
  profit_rate,
  customer_large_level,
  customer_small_level,
  customer_level_tag,
  if_new_order_cus,
  first_order_date,
  quarter,
  '日配业务' as business_type_name
from
    csx_analyse.csx_analyse_report_sale_customer_level_qf
where
  tag = '2'
  and quarter = '20254'
--   and customer_large_level in ('A','B')
  ),
  
-- 明细
  tmp_new_cust_detail as 
(select 
    a.performance_region_name,
    a.performance_province_name,
    a.performance_city_name,
    a.city_type_name,
    a.province_manager_user_number,
    a.province_manager_user_name,
    a.province_manager_position,
    a.city_manager_user_number,
    a.city_manager_user_name,
    a.city_manager_position,
    a.sales_manager_user_number,
    a.sales_manager_user_name,
    a.sales_manager_position,
    a.supervisor_user_number,
    a.supervisor_user_name,
    a.supervisor_position,
    a.sales_user_number,
    a.sales_user_name,
    a.begin_date,
    a.work_age,
    a.sub_position_name,
    a.customer_code,
    a.customer_name,
    a.business_type_name,
    a.first_sale_data,
    a.business_sign_date,	
    a.break_sale_sdt,
    a.is_break_3month,
    a.rn
from 
(select
    performance_region_name,
    performance_province_name,
    performance_city_name,
    city_type_name,
    customer_code,
    customer_name,
    business_type_name,
    province_manager_user_number,
    province_manager_user_name,
    province_manager_position,
    city_manager_user_number,
    city_manager_user_name,
    city_manager_position,
    sales_manager_user_number,
    sales_manager_user_name,
    sales_manager_position,
    supervisor_user_number,
    supervisor_user_name,
    supervisor_position,
    sales_user_number,
    sales_user_name,
    begin_date,
    work_age,
    sub_position_name,
    new_flag,
    first_sale_data,
    business_sign_date,	
    break_sale_sdt,	
    is_break_3month,
    row_number()over(partition by customer_code,business_type_name order by sale_amt desc ) rn
from
   (select * from  csx_analyse_tmp.csx_analyse_sales_new_customer_info_mf_2025q3 
    )a 
   
  )a 
 
 where rn=1
) ,
tmp_sale as 
(
    select 
        substr(sdt,1,6) sale_month,
        performance_region_code,
        performance_region_name,
        performance_province_code,
        performance_province_name,
        performance_city_code,
        performance_city_name,
        business_type_code,
        business_type_name,
        customer_code,
        customer_name,
        sales_user_name,
        sales_user_number,
        min(sdt) min_sdt,
        sum(sale_amt) sale_amt,
        sum(profit) profit
    from csx_dws.csx_dws_sale_detail_di   
    where sdt >= '20251001' and sdt<='20251231'
        and shipper_code='YHCSX'
        and business_type_code in ('1','2','6','10')
        -- AND order_channel_code NOT IN ('4','6','5')  
        -- and refund_order_flag <>1    
    group by 
        performance_region_code,
        performance_region_name,
        performance_province_code,
        performance_province_name,
        performance_city_code,
        performance_city_name,
        business_type_code,
        business_type_name,
        customer_code,
        customer_name,
        sales_user_name,
        sales_user_number,
        substr(sdt,1,6)
),
tmp_top_cust as (
select 
    a.*,
    b.sale_month,
    (sale_amt) sale_amt,
    (profit) profit,
    row_number()over(partition by a.customer_code,a.business_type_name order by b.sale_amt desc ) sale_rn
    from tmp_new_cust_detail a 
    left join  tmp_sale  b on a.customer_code=b.customer_code and a.business_type_name=b.business_type_name
)     select a.*,b.customer_large_level
    from tmp_top_cust a 
    left join tmp_customer_level b on a.customer_code=b.customer_no and a.business_type_name=b.business_type_name
  
     ;

select * from  csx_analyse_tmp.csx_analyse_tmp_break_cust_flag_Q4_02；



-- 汇总

select performance_region_name,
    performance_province_name,
    performance_city_name,
    province_manager_user_number,
    province_manager_user_name,
    province_manager_position,
    city_manager_user_number,
    city_manager_user_name,
    city_manager_position,
    sales_manager_user_number,
    sales_manager_user_name,
    sales_manager_position,
    supervisor_user_number,
    supervisor_user_name,
    supervisor_position,
    sum(if(customer_large_level='A',1,0)) as customer_A_cnt,
    sum(if(customer_large_level='A',sale_amt,0)) sale_A_amt,
    sum(if(customer_large_level='A',profit,0 )) profit_A,
    sum(if(customer_large_level='B',1,0)) as customer_B_cnt,
    sum(if(customer_large_level='B',sale_amt,0) ) sale_B_amt,
    sum(if(customer_large_level='B',profit,0 )) profit_B,
    count(customer_code) cnt,
    sum(sale_amt) sale_amt,
    sum(profit) profit 
    from csx_analyse_tmp.csx_analyse_tmp_break_cust_flag_Q4_02 
    where sale_rn=1
  group by performance_region_name,
    performance_province_name,
    performance_city_name,
    province_manager_user_number,
    province_manager_user_name,
    province_manager_position,
    city_manager_user_number,
    city_manager_user_name,
    city_manager_position,
    sales_manager_user_number,
    sales_manager_user_name,
    sales_manager_position,
    supervisor_user_number,
    supervisor_user_name,
    supervisor_position
     ;
