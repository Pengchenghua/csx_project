--断约 3个月以上及新客、BBC福利客户
-- drop table  csx_analyse_tmp.csx_analyse_tmp_break_cust ;
-- 统计新客按照上月20至本月20日区间
create table csx_analyse_tmp.csx_analyse_tmp_break_cust as 
-- 日配断约3个月断约客户
with tmp_break_cust AS (
SELECT 
    c.performance_province_name,
    c.performance_city_name,
    a.customer_code,
    customer_name,
    max_sdt,
    regexp_replace(date_add( from_unixtime(unix_timestamp(max_sdt, 'yyyyMMdd'),'yyyy-MM-dd'), 90), '-', '') syear,
    business_type_code,
    -- 新增断约判断字段
    CASE WHEN regexp_replace(date_add( from_unixtime(unix_timestamp(max_sdt, 'yyyyMMdd'),'yyyy-MM-dd'), 90) , '-', '') <=    regexp_replace(date_add(trunc(last_day(add_months('${sdt_yes_date}',-1)),'MM'),19),'-','') 
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
        sdt BETWEEN '20200101' AND   regexp_replace(last_day(add_months('${sdt_yes_date}',-2)),'-','')
        AND business_type_code IN ('1') 
        AND channel_code IN ('1','7','9')
        AND order_channel_code NOT IN (4)            
    GROUP BY 
        customer_code,
        business_type_code
    ) a
LEFT JOIN   
    (
    SELECT *
    FROM csx_dim.csx_dim_crm_customer_info
    WHERE sdt = 'current'
        AND channel_code IN ('1','7','9')
    ) c ON a.customer_code = c.customer_code 
-- 动态筛选断约客户（保留历史数据可注释掉）
-- WHERE  regexp_replace(date_add( from_unixtime(unix_timestamp(max_sdt, 'yyyyMMdd'),'yyyy-MM-dd'), 90) , '-', '') <=  regexp_replace(last_day(add_months('${sdt_yes_date}',-2)),'-','')
-- and a.customer_code in ('100563','101543')

),
--判断3个月以上客户断约后，新建商机属于新客
tmp_business as 
(select *,row_number()over(partition by customer_code order by business_sign_time desc ) rn
from 
(
select a.customer_code,
    business_sign_time,
    max_sdt,
    is_break_3month,
    a.business_type_code,
    syear
    -- if(from_unixtime(unix_timestamp(max_sdt, 'yyyyMMdd'),'yyyy-MM-dd')<= to_date(business_sign_time) ,1,0) is_flag,
    
from  csx_dim.csx_dim_crm_business_info a 
 join tmp_break_cust b on a.customer_code=b.customer_code 
where sdt='current'
and to_date(business_sign_time)<=   date_add(trunc(last_day(add_months('${sdt_yes_date}',-1)),'MM'),19)
and business_attribute_code=1
and status=1
) a 
-- where rn=1
) 
-- select * from tmp_business where customer_code='116015'
,
-- 准新客表
tmp_cust_active as 
(select
  customer_code,
  cast(business_type_code as string ) business_type_code,
  business_type_name,
  first_business_sign_date,
  first_business_sale_date min_sdt,
  last_business_sale_date max_sdt,
  substr(last_business_sale_date,1,6) as sale_month
 from csx_dws.csx_dws_crm_customer_business_active_di a
where sdt=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
    -- and a.customer_code in ('259610','259696','257923')
    and first_business_sale_date >=  regexp_replace(date_add(trunc(last_day(add_months('${sdt_yes_date}',-2)),'MM'),19),'-','')
    and first_business_sale_date <=  regexp_replace(date_add(trunc(last_day(add_months('${sdt_yes_date}',-1)),'MM'),19),'-','') 
 
),
-- 商机新客 主要BBC\福利
tmp_business_new_cust as 
(select 
     a.customer_code,
    cast(business_type_code as string ) business_type_code,
    business_type_name,
    smonth  sale_month
from
 (
    select province_name,
    city_group_name,
    a.customer_no customer_code,
    customer_name,
    business_type_code,
    business_type_name,
    smonth
   from    csx_analyse.csx_analyse_sale_d_customer_sign_new_about_di  a
     where smonth BETWEEN substr(regexp_replace(date_add(trunc(last_day(add_months('${sdt_yes_date}',-2)),'MM'),19),'-',''),1,6)  
            and substr( regexp_replace(date_add(trunc(last_day(add_months('${sdt_yes_date}',-1)),'MM'),19),'-',''),1,6)
     
        and end_date>=  regexp_replace(date_add(trunc(last_day(add_months('${sdt_yes_date}',-2)),'MM'),19),'-','')
        and end_date<= regexp_replace(date_add(trunc(last_day(add_months('${sdt_yes_date}',-1)),'MM'),19),'-','') 
     and business_type_code!=1
    union all
    select province_name,
    city_group_name,
    a.customer_no customer_code,
    customer_name,
    business_type_code,
    business_type_name,
    smonth
   from    csx_analyse.csx_analyse_sale_d_customer_new_about_di a 
   where smonth BETWEEN substr(regexp_replace(date_add(trunc(last_day(add_months('${sdt_yes_date}',-2)),'MM'),19),'-',''),1,6)  
            and substr( regexp_replace(date_add(trunc(last_day(add_months('${sdt_yes_date}',-1)),'MM'),19),'-',''),1,6)
        and first_business_sale_date >=  regexp_replace(date_add(trunc(last_day(add_months('${sdt_yes_date}',-2)),'MM'),19),'-','')
    and first_business_sale_date <=   regexp_replace(date_add(trunc(last_day(add_months('${sdt_yes_date}',-1)),'MM'),19),'-','') 
         )a
         where business_type_code!=1
),
tmp_sale as 
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
        min(sdt) min_sdt,
        sum(sale_amt) sale_amt,
        sum(profit) profit
    from csx_dws.csx_dws_sale_detail_di a   
    where sdt >=  regexp_replace(date_add(trunc(last_day(add_months('${sdt_yes_date}',-2)),'MM'),19),'-','')
        and sdt <=    regexp_replace(date_add(trunc(last_day(add_months('${sdt_yes_date}',-1)),'MM'),19),'-','') 
        and shipper_code='YHCSX'
        and business_type_code in  ('1','2','6')
        AND order_channel_code NOT IN (4)    
    group by substr(sdt, 1, 6),
        performance_province_name,
        performance_region_name,
        performance_city_name,
        a.business_type_code,
        a.business_type_name,
        a.customer_code,
        customer_name,
        sales_user_name,
        sales_user_number 
    )
    
    select '202504' as sale_month,
        a.performance_province_name,
        a.performance_region_name,
        a.performance_city_name,
        a.business_type_code,
        a.business_type_name,
        a.customer_code,
        a.customer_name,
        a.sales_user_name,
        a.sales_user_number ,
        a.business_sign_time,
        coalesce(a.min_sale_sdt,b.min_sdt ) first_sale_data,
        a.max_sdt as break_sale_sdt,            --断约日期末次销售 
        flag,
        case when a.flag=1 then '1' 
            when b.customer_code is not null then '1'
            when c.customer_code is not null then '1'
        else 0 end new_flag
    from (
    select a.*,b.max_sdt max_sdt,
        to_date(business_sign_time) business_sign_time,
        min_sdt as min_sale_sdt,
        case when  a.business_type_code='1' 
            and  (from_unixtime(unix_timestamp(b.max_sdt, 'yyyyMMdd'),'yyyy-MM-dd')<= to_date(business_sign_time) 
                 and   to_date(business_sign_time) <=from_unixtime(unix_timestamp(min_sdt, 'yyyyMMdd'),'yyyy-MM-dd') 
                 )
            then '1' else '0' end flag,
            is_break_3month
    from tmp_sale a 
        left join 
        (select * from tmp_business a   where a.is_break_3month='是'
            AND a.rn=1
        )b on a.customer_code=b.customer_code and a.business_type_code=b.business_type_code
       
    ) a 
    left join tmp_cust_active b on a.customer_code=b.customer_code and a.business_type_code=b.business_type_code
    left join tmp_business_new_cust c on a.customer_code=c.customer_code and a.business_type_code=c.business_type_code
    

    ;
    
    select * from csx_analyse_tmp.csx_analyse_tmp_break_cust where new_flag=1

-- 创建新客表
create table csx_analyse.csx_analyse_hr_break_new_customer_flag  as
(sale_month	string	comment		'月份',
performance_region_code    string	comment		'大区编码',
performance_region_name	string	comment		'大区',
performance_province_code	string	comment		'省区编码',
performance_province_name	string	comment		'省区',
performance_city_code	string	comment		'城市编码',
performance_city_name	string	comment		'城市',
business_type_code	int	comment		'业务类型编码',
business_type_name	string	comment		'业务类型名称',
customer_code	string	comment		'客户编码',
customer_name	string	comment		'客户名称',
sales_user_name	string	comment		'销售员',
sales_user_number	string	comment		'销售员工号',
business_sign_date	string	comment		'签约日期',
first_sale_data	string	comment		'首次成交日期',
break_sale_sdt	string	comment		'断约日期',
break_flag	string	comment		'是否断约',
new_flag	string	comment		'新客标识',
update_time    timestamp	comment		'更新时间'
)comment '断约客户重建新客含纯新客表'
partitioned by (smt string)
stored as parquet;




create table csx_analyse_tmp.csx_analyse_tmp_break_cust_flag as 
with tmp_break_cust AS (
SELECT 
    c.performance_province_name,
    c.performance_city_name,
    a.customer_code,
    customer_name,
    max_sdt,
    regexp_replace(date_add( from_unixtime(unix_timestamp(max_sdt, 'yyyyMMdd'),'yyyy-MM-dd'), 90), '-', '') syear,
    business_type_code,
    -- 新增断约判断字段
    CASE WHEN regexp_replace(date_add( from_unixtime(unix_timestamp(max_sdt, 'yyyyMMdd'),'yyyy-MM-dd'), 90) , '-', '') <=    regexp_replace(date_add(trunc(last_day(add_months('${sdt_yes_date}',-1)),'MM'),19),'-','') 
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
        sdt BETWEEN '20200101' AND   regexp_replace(last_day(add_months('${sdt_yes_date}',-2)),'-','')
        AND business_type_code IN ('1') 
        AND channel_code IN ('1','7','9')
        AND order_channel_code    not in ('4','6','5')  
        and refund_order_flag<>1 
    GROUP BY 
        customer_code,
        business_type_code
    ) a
LEFT JOIN   
    (
    SELECT *
    FROM csx_dim.csx_dim_crm_customer_info
    WHERE sdt = 'current'
        AND channel_code IN ('1','7','9')
    ) c ON a.customer_code = c.customer_code 
-- 动态筛选断约客户（保留历史数据可注释掉）
-- WHERE  regexp_replace(date_add( from_unixtime(unix_timestamp(max_sdt, 'yyyyMMdd'),'yyyy-MM-dd'), 90) , '-', '') <=  regexp_replace(last_day(add_months('${sdt_yes_date}',-2)),'-','')
-- and a.customer_code in ('100563','101543')

),
-- 判断3个月以上客户断约后，新建商机属于新客
tmp_business as 
(select *,row_number()over(partition by customer_code order by business_sign_time desc ) rn
from 
(
select a.customer_code,
    business_sign_time,
    max_sdt,
    is_break_3month,
    a.business_type_code,
    syear
    -- if(from_unixtime(unix_timestamp(max_sdt, 'yyyyMMdd'),'yyyy-MM-dd')<= to_date(business_sign_time) ,1,0) is_flag,
    
from  csx_dim.csx_dim_crm_business_info a 
 join tmp_break_cust b on a.customer_code=b.customer_code 
where sdt='current'
and to_date(business_sign_time)<    date_add(trunc(last_day(add_months('${sdt_yes_date}',-1)),'MM'),19)
and business_attribute_code=1
and status=1
) a 
-- where rn=1
) ,
tmp_sale as 
    (select substr(sdt,1,6) sale_month,
        performance_province_name,
        performance_region_name,
        performance_city_name,
        a.business_type_code,
        a.business_type_name,
        a.customer_code,
        customer_name,
        sales_user_name,
        sales_user_number,
        min(sdt) min_sdt,
        sum(sale_amt) sale_amt,
        sum(profit) profit
    from csx_dws.csx_dws_sale_detail_di a   
    where sdt >=  regexp_replace(date_add(trunc(last_day(add_months('${sdt_yes_date}',-2)),'MM'),19),'-','')
        and sdt <=   regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','') 
        and shipper_code='YHCSX'
        and business_type_code in  ('1','2','6','10')
        AND order_channel_code    not in ('4','6','5')  
        and refund_order_flag <>1    
    group by 
        performance_province_name,
        performance_region_name,
        performance_city_name,
        a.business_type_code,
        a.business_type_name,
        a.customer_code,
        customer_name,
        sales_user_name,
        sales_user_number ,
         substr(sdt,1,6)
    )
 -- 断约客户
 
    select a.*,
    b.max_sdt max_sdt,
    to_date(business_sign_time) business_sign_time,
    min_sdt as min_sale_sdt,
    case when  a.business_type_code='1' 
            and  (from_unixtime(unix_timestamp(b.max_sdt, 'yyyyMMdd'),'yyyy-MM-dd')<= to_date(business_sign_time) 
                 and to_date(business_sign_time) <=from_unixtime(unix_timestamp(min_sdt, 'yyyyMMdd'),'yyyy-MM-dd') 
                 and min_sdt<= regexp_replace( date_add(trunc(last_day(add_months('${sdt_yes_date}',-1)),'MM'),19),'-','')
                 )
        then '1' else '0' end flag,
        is_break_3month,
    row_number()over(partition by a.customer_code,a.business_type_code) rn
    from tmp_sale a 
        left join 
        (select * from tmp_business a   where a.is_break_3month='是'
            AND a.rn=1
        )b on a.customer_code=b.customer_code and a.business_type_code=b.business_type_code
       
 

;



create table csx_analyse_tmp.csx_analyse_tmp_break_cust  as 
-- 准新客表
with  tmp_cust_active as 
(select
  customer_code,
  cast(business_type_code as string ) business_type_code,
  business_type_name,
  first_business_sign_date,
  first_business_sale_date min_sdt,
  last_business_sale_date max_sdt,
  substr(last_business_sale_date,1,6) as sale_month,
  case when first_business_sale_date >=  regexp_replace(date_add(trunc(last_day(add_months('${sdt_yes_date}',-2)),'MM'),19),'-','') 
        and first_business_sale_date <   regexp_replace(date_add(trunc(last_day(add_months('${sdt_yes_date}',-1)),'MM'),19),'-','') 
        and business_type_code =1 then '1' 
    when  first_business_sale_date >=  regexp_replace(trunc(last_day(add_months('${sdt_yes_date}',-1)),'MM'),'-','') 
        and first_business_sale_date <=    regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','') 
        and business_type_code <> 1 
        then '1'
    else '0' end active_new_flag
 from csx_dws.csx_dws_crm_customer_business_active_di a
where sdt=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
    and  first_business_sale_date >=  regexp_replace(date_add(trunc(last_day(add_months('${sdt_yes_date}',-2)),'MM'),19),'-','') 
),
--  select * from tmp_cust_active a where active_new_flag=1
-- --  ;,
-- 商机新客 主要BBC\福利
tmp_business_new_cust as 
(
select a.customer_code,
    a.business_type_name,
    b.business_type_code,
    min_end_date,
    max(business_sign_date)  business_sign_date
from 
 (
  select
  customer_no customer_code,
  business_type_name,
  min(end_date) min_end_date  
 from  csx_analyse.csx_analyse_sale_d_customer_sign_new_about_di a   -- 商机新客
 left join 
 (select customer_code,
    performance_province_name,
    performance_region_name
 from csx_dim.csx_dim_crm_customer_info
    where sdt='current'
    ) b on a.customer_no= b.customer_code
where
   smonth =  substr(regexp_replace( date_add(trunc(last_day(add_months('${sdt_yes_date}',-1)),'MM'),19),'-',''), 1, 6) 
--   and business_type_code !=1 
  where business_type_code!=1
  group by  customer_no  ,
     business_type_name
  
  ) a 
  left  join 
  (
  select customer_code,
    regexp_replace(to_date(business_sign_time ),'-','') business_sign_date , 
    business_number,
    business_type_code ,
    business_type_name
  from csx_dim.csx_dim_crm_business_info 
    where sdt='current' 
    -- and to_date(business_sign_time )<= date_add(trunc(last_day(add_months('${sdt_yes_date}',-1)),'MM'),19)
  ) b on a.customer_code=b.customer_code and min_end_date>=business_sign_date and a.business_type_name=b.business_type_name
group by a.customer_code,
    a.business_type_name,
    min_end_date,
    b.business_type_code
)
    
    select substr(regexp_replace(date_add(trunc(last_day(add_months('${sdt_yes_date}',-1)),'MM'),19),'-',''),1,6)  as sale_month,
        a.performance_province_name,
        a.performance_region_name,
        a.performance_city_name,
        a.business_type_code,
        a.business_type_name,
        a.customer_code,
        a.customer_name,
        a.sales_user_name,
        a.sales_user_number ,
        coalesce(a.business_sign_time,
                b.first_business_sign_date,
                c.business_sign_date) business_sign_date,
        coalesce(a.min_sale_sdt, b.min_sdt,min_end_date  ) first_sale_data,
        a.max_sdt as break_sale_sdt,            --断约日期末次销售 
        flag,
        case when a.flag='1' and is_break_3month='是' then '1' 
            when b.customer_code is not null and b.active_new_flag='1' then '1'
            when  c.customer_code is not null then  '1'
        else '0' end new_flag 
    from 
        (select * from csx_analyse_tmp.csx_analyse_tmp_break_cust_flag where rn=1 ) a 
    left join tmp_cust_active b on a.customer_code=b.customer_code and a.business_type_code=b.business_type_code
    left join tmp_business_new_cust c on a.customer_code=c.customer_code and a.business_type_code=c.business_type_code
 ;




 -- 销售红黑榜新客客户明细表 

-- drop table csx_analyse.csx_analyse_hr_red_black_new_customer_info_mf;
create table csx_analyse.csx_analyse_hr_red_black_new_customer_info_mf (
s_month	string	comment	'销售月',
performance_region_code	string	comment	'大区编码',
performance_region_name	string	comment	'大区名称',
performance_province_code	string	comment	'省区编码',
performance_province_name	string	comment	'省区名称',
performance_city_code	string	comment	'城市编码',
performance_city_name	string	comment	'城市名称',
customer_code	string	comment	'客户编码',
customer_name	string	comment	'客户名称',
business_type_code	int	comment	'业务类型',
business_type_name	string	comment	'业务名称',
sales_user_number	string	comment	'销售员工号',
sales_user_name	string	comment	'销售员',
business_sign_date    string	comment	'签约日期',
first_sale_data	string	comment	'首次成交日期',
break_sale_sdt	string	comment	'断约日期',
flag	string	comment	'断约标识',
new_flag	string	comment	'新客标识',
update_time	string	comment	'更新时间'
)comment '销售员新开客客户明细表'
partitioned by (smt string comment '分区月smt=yyyymm')
stored as parquet;