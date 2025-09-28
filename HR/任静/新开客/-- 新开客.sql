-- 新开客

create table csx_analyse_tmp.csx_analyse_tmp_break_cust_flag_01 as 
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
    CASE WHEN regexp_replace(date_add( from_unixtime(unix_timestamp(max_sdt, 'yyyyMMdd'),'yyyy-MM-dd'), 90) , '-', '') <=    regexp_replace(trunc(last_day(add_months('${sdt_yes_date}',-1)),'MM'),'-','') 
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

)
-- select * from tmp_break_cust where customer_code='130934'
,
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
and to_date(business_sign_time) <   trunc(last_day(add_months('${sdt_yes_date}',-1)),'MM')
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
    where sdt >=   regexp_replace(trunc(last_day(add_months('${sdt_yes_date}',-1)),'MM'),'-','')
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
                 and min_sdt<= regexp_replace( trunc(last_day(add_months('${sdt_yes_date}',-1)),'MM'),'-','')
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




-- drop table  csx_analyse_tmp.csx_analyse_tmp_break_cust_01;
create table csx_analyse_tmp.csx_analyse_tmp_break_cust_01  as 
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
  '1' active_new_flag
 from csx_dws.csx_dws_crm_customer_business_active_di a
where sdt='current'
    and  first_business_sale_date >=  regexp_replace(trunc(last_day(add_months('${sdt_yes_date}',-1)),'MM'),'-','') 
        and first_business_sale_date <=    regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','') 
    
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
   smonth =  substr(regexp_replace( trunc(last_day(add_months('${sdt_yes_date}',-1)),'MM'),'-',''), 1, 6) 
  and business_type_code !=1 
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
    
    select substr(regexp_replace(trunc(last_day(add_months('${sdt_yes_date}',-1)),'MM'),'-',''),1,6)  as sale_month,
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
        (select * from csx_analyse_tmp.csx_analyse_tmp_break_cust_flag_01 where rn=1 ) a 
    left join tmp_cust_active b on a.customer_code=b.customer_code and a.business_type_code=b.business_type_code
    left join tmp_business_new_cust c on a.customer_code=c.customer_code and a.business_type_code=c.business_type_code
 ;

select * from csx_analyse_tmp.csx_analyse_tmp_break_cust_01 where new_flag=1


-- 销售人员信息表
drop table csx_analyse_tmp.csx_analyse_tmp_sale_info ;
create table csx_analyse_tmp.csx_analyse_tmp_sale_info as 
with position_dic as 
(select dic_key as code,dic_value as name
       from csx_ods.csx_ods_csx_b2b_ucenter_user_dic_df
       where sdt=regexp_replace(to_date(date_sub(current_timestamp(),1)),'-','')
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
    WHERE sdt='current'
   -- and  leader_user_position in ('POSITION-26064','POSITION-26623','POSITION-25844')
   -- and user_position_type='SALES'
    AND status=0
    )a 
    left join position_dic b on user_position=b.code
    left join position_dic c on a.user_position_type=c.code
    where rank=1
  ),
 tmp_sale_info as (
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
  cast(a.user_id as string )  user_id,
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
  city_name,
  status
  from 
     csx_dim.csx_dim_uc_user a 
  left  join 
    (select distinct
        employee_name,
        employee_code,
        begin_date,
        record_type_name,
        cost_center_name,
        sdt
    from    csx_dim.csx_dim_basic_employee 
        where sdt='current'
        and card_type='0' 
      --  and record_type_code	!=4
    )b on a.user_number=b.employee_code 
    -- and a.sdt=b.sdt
    where 
    a.sdt ='current'
    and status=0 
    and cost_center_name not like '%大客户七部%'
 -- and (user_position like 'SALES%'
-- and user_name in ('江苏B','许佳惠')
  )a 
 left join leader_info  b on cast(a.leader_user_id as string ) =cast(b.user_id as string )  
--  and a.sdt=b.sdt
 left join position_dic c on a.user_position=c.code
 left join position_dic d on a.source_user_position=d.code
 left join leader_info f on a.new_leader_user_id=f.user_id  
--  and a.sdt=f.sdt
)select * from tmp_sale_info
;


-- 新开客明细

create table csx_analyse_tmp.csx_analyse_tmp_new_cust_sale_01 as 
with tmp_sale_info as   
(select
  case when a.province_name  ='上海市' then '上海'
        when city_name in ('苏州市') then '上海'
        when city_name in ('深圳市') then '广东深圳' 
        when city_name in ('广州市') then '广东广州'
        when city_name in ('南京市') then '江苏南京'
        else province_name 
        end province_name
    ,
  case when province_name in ('重庆市') then '重庆市' 
        when city_name in ('松江区') then '上海松江'
        when city_name in ('深圳市') then '广东深圳'
        when city_name in ('广州市') then '广东广州'
        when city_name in ('苏州市') then '江苏苏州'
        when city_name in ('南京市') then '南京主城'
        else a.city_name end city_name ,
  user_number,
  a.user_name,
  sub_position_name,
  begin_date
from
  csx_analyse_tmp.csx_analyse_tmp_sale_info a
where
  sub_position_name in ('销售岗', '首席销售', '销售经理', '高级销售经理', '福利销售经理', '福利销售BD')
  ) 
  ,
tmp_sale_detail as 
 
(select
  substr(sdt,1,6) s_month,
  performance_region_name,
  performance_province_name,
  performance_city_name,
  sales_user_number,
  sales_user_name,
  customer_code,
  customer_name,
  business_type_code,
  business_type_name,
  sum(sale_amt) sale_amt,
  sum(profit) profit ,
  to_date(first_business_sale_date)
from
    csx_analyse.csx_analyse_bi_sale_detail_di a 
where
   sdt >= '20250401'
  and sdt<='20250430'
--   and customer_code='106775'
 group by substr(sdt,1,6)  ,
  performance_region_name,
  performance_province_name,
  performance_city_name,
  sales_user_number,
  sales_user_name,
  customer_code,    
  customer_name,
  business_type_code,
  business_type_name,
 to_date(first_business_sale_date)
),
tmp_new_cust_sale as 
 (select
  b.s_month,
  b.performance_region_name,
  b.performance_province_name,
  b.performance_city_name,
  b.sales_user_number,
  b.sales_user_name,
  b.customer_code,
  b.customer_name,
  b.business_type_code,
  b.business_type_name,
  (sale_amt) sale_amt,
  (profit) profit ,
  first_sale_data
from
  csx_analyse_tmp.csx_analyse_tmp_break_cust_01 a 
  left join 
  tmp_sale_detail b on a.customer_code=b.customer_code and a.business_type_code=b.business_type_code
  where new_flag=1
  )
   select *,count(customer_code)over(partition by sales_user_number) cnt from  tmp_new_cust_sale 

; 

-- 销售员统计
with tmp_sale_info as   
(select  
  
  case when a.province_name  ='上海市' then '上海'
        when city_name in ('苏州市') then '上海'
        when city_name in ('深圳市') then '广东深圳' 
        when city_name in ('广州市') then '广东广州'
        when city_name in ('南京市') then '江苏南京'
        when city_name in ('盐城市') then '江苏南京'
        when province_name in ('东丰合资') then '东北'
        else province_name 
        end province_name
    ,
  case when province_name in ('重庆市') then '重庆市' 
        when city_name in ('松江区') then '上海松江'
        when city_name in ('深圳市') then '广东深圳'
        when city_name in ('广州市') then '广东广州'
        when city_name in ('苏州市') then '江苏苏州'
        when city_name in ('南京市') then '南京主城'
        when city_name in ('盐城市') then '江苏盐城'
        else a.city_name end city_name ,
  user_number,
  a.user_name,
  sub_position_name,
  begin_date
from
  csx_analyse_tmp.csx_analyse_tmp_sale_info a
 
where
  sub_position_name in ('销售岗', '首席销售', '销售经理', '高级销售经理', '福利销售经理', '福利销售BD')
  ) 
  ,
  tmp_sale_total as 
  (select 
        sales_user_number	,
        sales_user_name,
        sum(if(business_type_name='日配业务',1,0)) rp_cust_cnt,
        sum(if(business_type_name='BBC',1,0)) BBC_cust_cnt,
         sum(if(business_type_name IN ('福利业务','福利小店'),1,0)) fl_cust_cnt,
        sum(if(business_type_name='日配业务',sale_amt,0))/10000 rp_sale_amt,
        sum(if(business_type_name='BBC',sale_amt,0))/10000 BBC_sale_amt,
        sum(if(business_type_name IN ('福利业务','福利小店'),sale_amt,0)) /10000 fl_sale_amt,
        sum(if(business_type_name='日配业务',profit,0))/10000 rp_profit,
        sum(if(business_type_name='BBC',profit,0))/10000  BBC_profit,
        sum(if(business_type_name IN ('福利业务','福利小店'),profit,0))/10000 fl_profit
    from  csx_analyse_tmp.csx_analyse_tmp_new_cust_sale_01
        group by sales_user_number	,
            sales_user_name
  )
  select c.performance_region_name,
    performance_province_code,
    performance_province_name,
    a.*,b.rp_cust_cnt,
    b.BBC_cust_cnt,
    b.fl_cust_cnt,
    b.rp_sale_amt,
    b.BBC_sale_amt,
    b.fl_sale_amt,
    b.rp_profit,
    b.BBC_profit,
    b.fl_profit,
   if( b.sales_user_number is null ,1,0) as no_customer_flag
  from tmp_sale_info a 
  left join tmp_sale_total b on a.user_number=b.sales_user_number
   left join 
  (select performance_region_name,
    performance_province_code,
    performance_province_name
  from csx_dim.csx_dim_crm_customer_info where sdt='current'
  group by  performance_region_name,
    performance_province_code,
    performance_province_name
  
    ) c on province_name=c.performance_province_name
    where performance_province_name is not null
    ;

select * from csx_analyse_tmp.csx_analyse_tmp_sale_info a where sub_position_name in ('销售岗','首席销售','销售经理','高级销售经理','福利销售经理','福利销售BD')






select * from  csx_analyse_tmp.csx_analyse_tmp_break_cust_flag_01 where customer_code='130934'

is_break_3month='是' and flag=1;


select * from  csx_dim.csx_dim_crm_business_info a 
 where sdt='current'
 and customer_code='130934'



 -- 销售员新开客客户明细表
create table csx_analyse.csx_analyse_sales_new_customer_info_di (
s_month	string	comment	'销售月',
performance_region_code	string	comment	'大区编码',
performance_region_name	string	comment	'大区名称',
performance_province_code	string	comment	'省区编码',
performance_province_name	string	comment	'省区名称',
performance_city_code	string	comment	'城市编码',
performance_city_name	string	comment	'城市名称',
sales_user_number	string	comment	'销售员工号',
sales_user_name	string	comment	'销售员',
begin_date  string	comment	'入职时间',
-- 工龄
work_age	int	comment	'工龄月份',
sub_position_name	string	comment	'职位名称',
customer_code	string	comment	'客户编码',
customer_name	string	comment	'客户名称',
business_type_code	int	comment	'业务类型',
business_type_name	string	comment	'业务名称',
sale_amt	decimal(30,6)	comment	'销售额',
profit	decimal(30,6)	comment	'毛利额',
first_sale_data	string	comment	'首次成交日期',
cnt	bigint	comment	'销售员客户数',
update_time	string	comment	'更新时间'
)comment '销售员新开客客户明细表'
partitioned by (smt string comment '分区月smt=yyyymm')
stored as parquet;

-- 服务管家服务客户数统计
create table csx_analyse.csx_analyse_service_user_cust_cnt_mf (
s_month	string	comment	'销售月',
quarter_of_year	string	comment '季度',
performance_region_code	string	comment	'大区编码',
performance_region_name	string	comment	'大区名称',
performance_province_code	string	comment	'省区编码',
performance_province_name	string	comment	'省区名称',
performance_city_code	string	comment	'城市编码',
performance_city_name	string	comment	'城市名称',
customer_code	string	comment	'客户编码',
customer_name	string	comment	'客户名称',
sub_customer_code	string	comment	'子客户编码',
sub_customer_name	string	comment	'子客户名称',
service_user_work_no	string	comment	'服务管家工号',
service_user_name	string	comment	'服务管家',
begin_date  string	comment	'入职时间',
work_age	int	comment	'工龄月份',
sub_position_name	string	comment	'职位名称',
sale_amt	decimal(38,6)	comment	'销售额',
profit	decimal(38,6)	comment	'毛利额',
update_time  string	comment	'更新时间',
)comment '服务管家服务客户数统计'
partitioned by (smt string comment '分区月smt=yyyymm')
stored as parquet;




  
-- create table csx_analyse_tmp.csx_analyse_tmp_new_cust_sale_01 as 
with tmp_sale_info as   
(select
  case when a.province_name  ='上海市' then '上海'
        when city_name in ('苏州市') then '上海'
        when city_name in ('深圳市') then '广东深圳' 
        when city_name in ('广州市') then '广东广州'
        when city_name in ('南京市') then '江苏南京'
        else province_name 
        end province_name
    ,
  case when province_name in ('重庆市') then '重庆市' 
        when city_name in ('松江区') then '上海松江'
        when city_name in ('深圳市') then '广东深圳'
        when city_name in ('广州市') then '广东广州'
        when city_name in ('苏州市') then '江苏苏州'
        when city_name in ('南京市') then '南京主城'
        else a.city_name end city_name ,
  user_number,
  a.user_name,
  sub_position_name,
  begin_date,
  ceil(months_between(last_day(add_months('${sdt_yes_date}',-1)), from_unixtime(unix_timestamp(begin_date,'yyyyMMdd'),'yyyy-MM-dd'))) as work_age -- 工䶖月向上取
from csx_analyse.csx_analyse_fr_hr_red_black_sale_info a 
where smt=substr(regexp_replace(trunc(last_day(add_months('${sdt_yes_date}',-1)),'MM'),'-',''),1,6) 
 ) 
  ,tmp_sale_detail as (
  select
    a.month_of_year,
    quarter_of_year,
    performance_region_name,
    performance_province_name,
    performance_city_name,
    sales_user_number,
    sales_user_name,
    customer_code,
    customer_name,
    sub_customer_code,
    sub_customer_name,
    business_type_code,
    business_type_name,
    sum(sale_amt) sale_amt,
    sum(profit) profit
  from
    css_dws.csx_dws_sale_detail_di a
    left join (
      select
        month_of_year,
        quarter_of_year,
        calday
      from
          csx_dim.csx_dim_basic_date
    ) b on a.sdt = b.calday
  where
    sdt >= regexp_replace(trunc(last_day(add_months('${sdt_yes_date}',-1)),'MM'),'-','')
    and sdt <=  regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
    and business_type_code in (1) --   and first_business_sale_date>=sdt
  group by
    quarter_of_year,
    performance_region_name,
    performance_province_name,
    performance_city_name,
    sales_user_number,
    sales_user_name,
    customer_code,
    customer_name,
    business_type_code,
    business_type_name,
    sub_customer_code,
    sub_customer_name,
    a.month_of_year
),
tmp_sever_info as (
  select
    *
  from
    (
      select
        substr(sdt, 1, 6) s_month,
        customer_code as customer_no,
        service_manager_user_number service_user_work_no,
        service_manager_user_name service_user_name,
        service_manager_user_id service_user_id,
        if( business_attribute_code = 5, 6,  business_attribute_code  ) attribute_code,
        business_attribute_name attribute_name,
        row_number() over( partition by customer_code, business_attribute_code order by service_manager_user_id asc ) as ranks
      from
        csx_dim.csx_dim_crm_customer_business_ownership
      where
        sdt=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
        and shipper_code = 'YHCSX'
        and service_manager_user_id <> 0      
        and business_attribute_code = 1 
    ) a distribute by customer_no,
    attribute_code sort by customer_no,
    attribute_code,
    ranks
)
insert overwrite into table csx_analyse.csx_analyse_service_user_cust_cnt_mf partition(smt)
select
  a.month_of_year as s_month,
  a.quarter_of_year,
  performance_region_code,
  performance_region_name,
  performance_province_code,
  performance_province_name,
  performance_city_code,
  performance_city_name,
  customer_code,
  customer_name,
  sub_customer_code,
  sub_customer_name,
  b.service_user_work_no,
  b.service_user_name,
  c.begin_date,
  c.work_age,
  c.sub_position_name,
  sale_amt sale_amt,
  profit profit
from
  tmp_sale_detail a
  left join tmp_sever_info b on a.customer_code = b.customer_no
  and a.business_type_code = b.attribute_code
  left join tmp_sale_info c on b.service_user_work_no = c.user_number
  ;

-- 销售数
with tmp_sale_info as   
(select  
  
  case when a.province_name  ='上海市' then '上海'
        when city_name in ('苏州市') then '上海'
        when city_name in ('深圳市') then '广东深圳' 
        when city_name in ('广州市') then '广东广州'
        when city_name in ('南京市') then '江苏南京'
        when city_name in ('盐城市') then '江苏南京'
        when province_name in ('东丰合资') then '东北'
        else province_name 
        end province_name
    ,
  case when province_name in ('重庆市') then '重庆市' 
        when city_name in ('松江区') then '上海松江'
        when city_name in ('深圳市') then '广东深圳'
        when city_name in ('广州市') then '广东广州'
        when city_name in ('苏州市') then '江苏苏州'
        when city_name in ('南京市') then '南京主城'
        when city_name in ('盐城市') then '江苏盐城'
        else a.city_name end city_name ,
  user_number,
  a.user_name,
  sub_position_name,
  begin_date
from
  csx_analyse_tmp.csx_analyse_tmp_sale_info a
 
where
  sub_position_name in ('销售岗', '首席销售', '销售经理', '高级销售经理', '福利销售经理', '福利销售BD')
  ) 
  ,
  tmp_sale_total as 
  (select 
        sales_user_number	,
        sales_user_name,
        sum(if(business_type_name='日配业务',1,0)) rp_cust_cnt,
        sum(if(business_type_name='BBC',1,0)) BBC_cust_cnt,
         sum(if(business_type_name IN ('福利业务','福利小店'),1,0)) fl_cust_cnt,
        sum(if(business_type_name='日配业务',sale_amt,0))/10000 rp_sale_amt,
        sum(if(business_type_name='BBC',sale_amt,0))/10000 BBC_sale_amt,
        sum(if(business_type_name IN ('福利业务','福利小店'),sale_amt,0)) /10000 fl_sale_amt,
        sum(if(business_type_name='日配业务',profit,0))/10000 rp_profit,
        sum(if(business_type_name='BBC',profit,0))/10000  BBC_profit,
        sum(if(business_type_name IN ('福利业务','福利小店'),profit,0))/10000 fl_profit
    from  csx_analyse_tmp.csx_analyse_tmp_new_cust_sale_01
        group by sales_user_number	,
            sales_user_name
  )
  select c.performance_region_name,
    performance_province_code,
    performance_province_name,
    a.*,b.rp_cust_cnt,
    b.BBC_cust_cnt,
    b.fl_cust_cnt,
    b.rp_sale_amt,
    b.BBC_sale_amt,
    b.fl_sale_amt,
    b.rp_profit,
    b.BBC_profit,
    b.fl_profit,
   if( b.sales_user_number is null ,1,0) as no_customer_flag
  from tmp_sale_info a 
  left join tmp_sale_total b on a.user_number=b.sales_user_number
   left join 
  (select performance_region_name,
    performance_province_code,
    performance_province_name
  from csx_dim.csx_dim_crm_customer_info where sdt='current'
  group by  performance_region_name,
    performance_province_code,
    performance_province_name
  
    ) c on province_name=c.performance_province_name
    where performance_province_name is not null
    ;

 -- 管家数帆软报表SQL
with tmp_sale_info as   
(select  
  
  case when a.province_name  ='上海市' then '上海'
        when city_name in ('苏州市') then '上海'
        when city_name in ('深圳市') then '广东深圳' 
        when city_name in ('广州市') then '广东广州'
        when city_name in ('南京市') then '江苏南京'
        when city_name in ('盐城市') then '江苏南京'
        when province_name in ('东丰合资') then '东北'
        else province_name 
        end province_name
    ,
  case when province_name in ('重庆市') then '重庆市' 
        when city_name in ('松江区') then '上海松江'
        when city_name in ('深圳市') then '广东深圳'
        when city_name in ('广州市') then '广东广州'
        when city_name in ('苏州市') then '江苏苏州'
        when city_name in ('南京市') then '南京主城'
        when city_name in ('盐城市') then '江苏盐城'
        else a.city_name end city_name ,
  user_number,
  a.user_name,
  sub_position_name,
  begin_date
from
  csx_analyse.csx_analyse_fr_hr_red_black_sale_info a 
where smt='${smt}' 
  and  begin_date!=''
  and sub_position_name in ('销售岗', '首席销售', '销售经理', '高级销售经理', '福利销售经理', '福利销售BD')
  ) 
  
select a.performance_region_name,
a.performance_province_name,
a.performance_city_name,
a.service_user_work_no,
a.service_user_name	,
a.begin_date,
b.sub_position_name,
max(work_age )work_age
sum(cust_cnt) as cust_cnt,
sum(sub_customer_cnt) as sub_customer_cnt,
sum(sale_amt) as sale_amt,
sum(profit) as profit,
if(coalesce(sum(sale_amt),0)=0,0,sum(profit)/sum(sale_amt)) as profit_rate,
sum(last_cust_cnt) as last_cust_cnt,
sum(last_sub_customer_cnt) as last_sub_customer_cnt,
sum(last_sale_amt) as last_sale_amt,
sum(last_profit) as last_profit,
if(coalesce(sum(last_sale_amt),0)=0,0,sum(last_profit)/sum(last_sale_amt)) as last_profit_rate
from 
(select 
performance_region_name,
performance_province_name,
performance_city_name,
service_user_work_no,
service_user_name	,
begin_date,
max(work_age )work_age,
sub_position_name,
count(distinct case when s_month='${smt}' then  customer_code end ) cust_cnt,
sum(if(s_month='${smt}',1 ,0)) sub_customer_cnt,
sum(if(s_month='${smt}',sale_amt,0))/10000 sale_amt,
sum(if(s_month='${smt}',profit ,0))/10000 profit ,
count(distinct case when s_month='${l_smt}' then  customer_code end ) last_cust_cnt,
sum(if(s_month='${l_smt}',1 ,0)) last_sub_customer_cnt,
sum(if(s_month='${l_smt}',sale_amt,0))/10000 last_sale_amt,
sum(if(s_month='${l_smt}',profit ,0))/10000 last_profit 
from  csx_analyse.csx_analyse_service_user_cust_cnt_mf
where smt >= '${l_smt}'
and smt <= '${smt}'
group by 
performance_region_name,
performance_province_name,
performance_city_name,
begin_date,
sub_position_name,
service_user_work_no,
service_user_name
)a 
left join tmp_sale_info b on a.service_user_work_no=b.user_number
where service_user_work_no!='' 
and begin_date!=''
group by a.performance_region_name,
a.performance_province_name,
a.performance_city_name,
a.service_user_work_no,
a.service_user_name	,
a.begin_date,
b.sub_position_name,


-- create table csx_analyse_tmp.csx_analyse_tmp_new_cust_sale_01 as 
with tmp_sale_info as   
(select
  case when a.province_name  ='上海市' then '上海'
        when city_name in ('苏州市') then '上海'
        when city_name in ('深圳市') then '广东深圳' 
        when city_name in ('广州市') then '广东广州'
        when city_name in ('南京市') then '江苏南京'
        else province_name 
        end province_name
    ,
  case when province_name in ('重庆市') then '重庆市' 
        when city_name in ('松江区') then '上海松江'
        when city_name in ('深圳市') then '广东深圳'
        when city_name in ('广州市') then '广东广州'
        when city_name in ('苏州市') then '江苏苏州'
        when city_name in ('南京市') then '南京主城'
        else a.city_name end city_name ,
  user_number,
  a.user_name,
  sub_position_name,
  begin_date,
  ceil(months_between(last_day(add_months('${sdt_yes_date}',-1)), from_unixtime(unix_timestamp(begin_date,'yyyyMMdd'),'yyyy-MM-dd'))) as work_age -- 工䶖月向上取
from csx_analyse.csx_analyse_fr_hr_red_black_sale_info a 
where smt=substr(regexp_replace(trunc(last_day(add_months('${sdt_yes_date}',-1)),'MM'),'-',''),1,6) 
  and sub_position_name in ('销售岗', '首席销售', '销售经理', '高级销售经理', '福利销售经理', '福利销售BD')
 ) 





 with tmp_sale_info as   
(select  
  
  case when a.province_name  ='上海市' then '上海'
        when city_name in ('苏州市') then '上海'
        when city_name in ('深圳市') then '广东深圳' 
        when city_name in ('广州市') then '广东广州'
        when city_name in ('南京市') then '江苏南京'
        when city_name in ('盐城市') then '江苏南京'
        when province_name in ('东丰合资') then '东北'
        else province_name 
        end province_name
    ,
  case when province_name in ('重庆市') then '重庆市' 
        when city_name in ('松江区') then '上海松江'
        when city_name in ('深圳市') then '广东深圳'
        when city_name in ('广州市') then '广东广州'
        when city_name in ('苏州市') then '江苏苏州'
        when city_name in ('南京市') then '南京主城'
        when city_name in ('盐城市') then '江苏盐城'
        else a.city_name end city_name ,
  user_number,
  a.user_name,
  sub_position_name,
  begin_date
from
  csx_analyse.csx_analyse_fr_hr_red_black_sale_info a 
where smt='${smt}' 
  and sub_position_name in ('销售岗', '首席销售', '销售经理', '高级销售经理', '福利销售经理', '福利销售BD')
  ) 
  ,
  tmp_sale_total as 
  (select 
        sales_user_number	,
        sales_user_name,
        sum(if(business_type_name='日配业务',1,0)) rp_cust_cnt,
        sum(if(business_type_name='BBC',1,0)) BBC_cust_cnt,
        sum(if(business_type_name IN ('福利业务','福利小店'),1,0)) fl_cust_cnt,
        sum(if(business_type_name='日配业务',sale_amt,0))/10000 rp_sale_amt,
        sum(if(business_type_name='BBC',sale_amt,0))/10000 BBC_sale_amt,
        sum(if(business_type_name IN ('福利业务','福利小店'),sale_amt,0)) /10000 fl_sale_amt,
        sum(if(business_type_name='日配业务',profit,0))/10000 rp_profit,
        sum(if(business_type_name='BBC',profit,0))/10000  BBC_profit,
        sum(if(business_type_name IN ('福利业务','福利小店'),profit,0))/10000 fl_profit
    from  csx_analyse.csx_analyse_sales_new_customer_info_mf
        where smt='${smt}'
        group by sales_user_number	,
            sales_user_name
  )
  select c.performance_region_name,
    performance_province_code,
    performance_province_name,
    a.*,b.rp_cust_cnt,
    b.BBC_cust_cnt,
    b.fl_cust_cnt,
    b.rp_sale_amt,
    b.BBC_sale_amt,
    b.fl_sale_amt,
    b.rp_profit,
    b.BBC_profit,
    b.fl_profit,
   if( b.sales_user_number is null ,1,0) as no_customer_flag
  from tmp_sale_info a 
  left join tmp_sale_total b on a.user_number=b.sales_user_number
   left join 
  (select performance_region_name,
    performance_province_code,
    performance_province_name
  from csx_dim.csx_dim_crm_customer_info where sdt='current'
  group by  performance_region_name,
    performance_province_code,
    performance_province_name
  
    ) c on province_name=c.performance_province_name
    where performance_province_name is not null