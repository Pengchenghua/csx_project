-- 销售员新客统计
with tmp_sale_detail as (select smt,
 a.performance_region_name,
 a.performance_province_name,
 a.performance_city_name,
 a.customer_code,
 a.customer_name,
 a.sales_user_number,
 a.sales_user_name,
 a.begin_date,
 a.business_type_code,
 b.business_type_name,
 sum(sale_amt) sale_amt,
 sum(profit) profit 
 from 
 (SELECT 
    smt, 
    performance_region_name,
    performance_province_name,
    performance_city_name,
    customer_code,
    customer_name,
    sales_user_number,
    sales_user_name,
    begin_date,
    exploded_business_type_code AS business_type_code,
    business_type_name
FROM 
    csx_analyse.csx_analyse_fr_tc_sales_new_customer_business_mi 
LATERAL VIEW 
    explode(split(business_type_code, '，')) exploded_table AS exploded_business_type_code
WHERE 
    smt >= '202503' 
    AND is_new_customer = '是'
 )a 
 left join 
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
    ) b on a.customer_code=b.customer_code and a.business_type_code=b.business_type_code and a.smt=b.s_month
group by  
 a.performance_region_name,
 a.performance_province_name,
 a.performance_city_name,
 a.customer_code,
 a.customer_name,
 a.sales_user_number,
 a.sales_user_name,
 a.begin_date,
 a.business_type_code,
 b.business_type_name ,
 smt   
 )
 select  
 a.performance_region_name,
 a.performance_province_name,
 a.performance_city_name,
 a.sales_user_number,
 a.sales_user_name,
 a.begin_date, 
  sum(if(business_type_code=1,1,0) ) rp_cust_cn,
  sum(if(business_type_code=2,1,0) ) fl_cust_cn,
  sum(if(business_type_code=6,1,0) ) bbc_cust_cn,
  sum(case when business_type_code=1 then sale_amt end ) rp_sale_amt,
  sum(case when business_type_code=2 then sale_amt end ) fl_sale_amt, 
  sum(case when business_type_code=6 then sale_amt end ) bbc_sale_amt,
  sum(case when business_type_code=1 then profit end ) rp_profit,
  sum(case when business_type_code=2 then profit end ) fl_profit, 
  sum(case when business_type_code=6 then profit end ) bbc_profit
 from tmp_sale_detail a
 group by a.performance_region_name,
 a.performance_province_name,
 a.performance_city_name,
 a.sales_user_number,
 a.sales_user_name,
 a.begin_date
;

--管家服务客户
--子客户管家服务客户
with tmp_sale_detail as (
  select
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
    csx_analyse.csx_analyse_bi_sale_detail_di a
    left join (
      select
        quarter_of_year,
        calday
      from
        csx_dim.csx_dim_basic_date
    ) b on a.sdt = b.calday
  where
    sdt >= '20241001'
    and sdt <= '20250331'
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
    sub_customer_name
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
        if(
          business_attribute_code = 5,
          6,
          business_attribute_code
        ) attribute_code,
        business_attribute_name attribute_name,
        row_number() over(
          partition by customer_code,
          business_attribute_code
          order by
            service_manager_user_id asc
        ) as ranks
      from
        csx_dim.csx_dim_crm_customer_business_ownership
      where
        sdt in ('20250331')
        and shipper_code = 'YHCSX'
        and service_manager_user_id <> 0
        and business_attribute_code = 1 -- and customer_code='111207'
    ) a distribute by customer_no,
    attribute_code sort by customer_no,
    attribute_code,
    ranks
)
select
  quarter_of_year,
  performance_region_name,
  performance_province_name,
  performance_city_name,
  customer_code,
  customer_name,
  sub_customer_code,
  sub_customer_name,
  service_user_work_no,
  service_user_name,
  sum(sale_amt) sale_amt,
  sum(profit) profit
from
  tmp_sale_detail a
  left join tmp_sever_info b on a.customer_code = b.customer_no
  and a.business_type_code = b.attribute_code
group by
  quarter_of_year,
  performance_region_name,
  performance_province_name,
  performance_city_name,
  customer_code,
  customer_name,
  sub_customer_code,
  sub_customer_name,
  service_user_work_no,
  service_user_name
;

-- 单纯新客
  with tmp_sale_detail as 
(select
--   substr(sdt,1,6) s_month,
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
  to_date(first_business_sale_date) first_business_sale_date,
  if(to_date(first_business_sale_date) between '2025-01-01' and '2025-03-31',1,0) as is_new_flag
from
    csx_analyse.csx_analyse_bi_sale_detail_di a 
where
    sdt >= '20250101'
  and sdt<='20250331'
  and business_type_code in (1,2,6)
--   and first_business_sale_date>=sdt 
 group by
--  substr(sdt,1,6)  ,
  performance_region_name,
  performance_province_name,
  performance_city_name,
  sales_user_number,
  sales_user_name,
  customer_code,
  customer_name,
  business_type_code,
  business_type_name,
  to_date(first_business_sale_date) ,
  if(to_date(first_business_sale_date) between '2025-01-01' and '2025-03-31',1,0)
  )
  select performance_region_name,
  performance_province_name,
  performance_city_name,
  sales_user_number,
  sales_user_name,
  sum(if(business_type_code=1,1,0) ) rp_cust_cn,
  sum(if(business_type_code=2,1,0) ) fl_cust_cn,
  sum(if(business_type_code=6,1,0) ) bbc_cust_cn,

  sum(case when business_type_code=1 then sale_amt end ) rp_sale_amt,
  sum(case when business_type_code=2 then sale_amt end ) fl_sale_amt, 
  sum(case when business_type_code=6 then sale_amt end ) bbc_sale_amt,
  sum(case when business_type_code=1 then profit end ) rp_profit,
  sum(case when business_type_code=2 then profit end ) fl_profit, 
  sum(case when business_type_code=6 then profit end ) bbc_profit
  from tmp_sale_detail where is_new_flag=1
  group by performance_region_name,
  performance_province_name,
  performance_city_name,
  sales_user_number,
  sales_user_name

  -- 增加断约3个月的客户

  with tmp_break_cust as 
(select 			
	c.performance_province_name,
	c.performance_city_name,
	a.customer_code,
	customer_name,
	max_sdt,
	syear
from
	(
	 select 
		customer_code,
		max(sdt) as max_sdt,
		regexp_replace(cast(date_add(from_unixtime(unix_timestamp(max(sdt),'yyyyMMdd')),90) as string),'-','')  as syear
	 from 
		csx_dws.csx_dws_sale_detail_di 
	 where 
		sdt  between '20200101' and '20250331'
		and business_type_code in ('1') 
		and channel_code in('1','7','9')
		and order_channel_code not in (4)			
	 group by 
		customer_code
	) a
	left join   
		(
          select *
          from csx_dim.csx_dim_crm_customer_info
          where sdt= 'current'
            and channel_code  in ('1','7','9')
        ) c on a.customer_code=c.customer_code 
where syear < '20250401'
) ,
tmp_business as 
(
select * from
(
select
           customer_code,
		   business_number,
           cast(business_type_code as string) business_type_code,
           business_sign_time,
           regexp_replace(substr(business_sign_time,1,10),'-','')  start_date,
           row_number()over(partition by customer_code,business_type_code order by business_sign_time desc ) rn 
        FROM
              csx_dim.csx_dim_crm_business_info       
        WHERE  sdt='current' 
             and business_stage = 5 
             and business_type_code in (1)  
             and shipper_code='YHCSX'
        	 AND regexp_replace(substr(business_sign_time,1,10),'-','')>='20240101'
    )a 
    where rn=1 
    ) ,
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
  min(sdt) min_sdt,
  sum(sale_amt) sale_amt,
  sum(profit) profit ,
  to_date(first_business_sale_date) first_business_sale_date
from
    csx_analyse.csx_analyse_bi_sale_detail_di a 
where
   sdt >= '20250401'
  and sdt<='20250430'
  and business_type_code=1
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
tmp_new_cust_detail as 
(
select  s_month,
 a.performance_region_name,
 a.performance_province_name,
 a.performance_city_name,
 a.customer_code,
 a.customer_name,
 b.sales_user_number,
 b.sales_user_name,
 a.business_type_code,
 b.business_type_name,
 sum(sale_amt) sale_amt,
 sum(profit) profit 
 from 
 (SELECT 
    distinct

    performance_region_name,
    performance_province_name,
    performance_city_name,
    customer_code,
    customer_name,
    exploded_business_type_code AS business_type_code 
FROM 
    csx_analyse.csx_analyse_fr_tc_sales_new_customer_business_mi 
LATERAL VIEW 
    explode(split(business_type_code, '，')) exploded_table AS exploded_business_type_code
WHERE 
    smt = '202504' 
    AND is_new_customer = '是'
 )a 
 left join 
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
  to_date(first_business_sale_date) first_business_sale_date
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
    ) b on a.customer_code=b.customer_code and a.business_type_code=b.business_type_code  
group by  
 a.performance_region_name,
 a.performance_province_name,
 a.performance_city_name,
 a.customer_code,
 a.customer_name,
 b.sales_user_number,
 b.sales_user_name,
 a.business_type_code,
 b.business_type_name ,
 s_month   
 )
  select  
 a.performance_region_name,
 a.performance_province_name,
 a.performance_city_name,
 a.sales_user_number,
 a.sales_user_name,
--  a.begin_date, 
  sum(if(business_type_code=1,1,0) ) rp_cust_cn,
  sum(if(business_type_code=2,1,0) ) fl_cust_cn,
  sum(if(business_type_code=6,1,0) ) bbc_cust_cn,
  sum(case when business_type_code=1 then sale_amt end ) rp_sale_amt,
  sum(case when business_type_code=2 then sale_amt end ) fl_sale_amt, 
  sum(case when business_type_code=6 then sale_amt end ) bbc_sale_amt,
  sum(case when business_type_code=1 then profit end ) rp_profit,
  sum(case when business_type_code=2 then profit end ) fl_profit, 
  sum(case when business_type_code=6 then profit end ) bbc_profit
 from (
select  a.s_month,
 a.performance_region_name,
 a.performance_province_name,
 a.performance_city_name,
 a.customer_code,
 a.customer_name,
 a.sales_user_number,
 a.sales_user_name,
 a.business_type_code,
 a.business_type_name,
 sum(a.sale_amt) sale_amt,
 sum(a.profit) profit 
 from 
(select a.*,b.syear,c.business_sign_time from tmp_sale_detail  a 
left join 
tmp_break_cust b on a.customer_code=b.customer_code 
left join 
tmp_business c on a.customer_code=c.customer_code
where b.customer_code is not null 
and c.business_sign_time is not null 
and a.sale_amt>0
)a  
left join tmp_new_cust_detail b on a.customer_code=b.customer_code and a.business_type_code=cast(b.business_type_code as int )
where b.customer_code is null 
group by  a.s_month,
 a.performance_region_name,
 a.performance_province_name,
 a.performance_city_name,
 a.customer_code,
 a.customer_name,
 a.sales_user_number,
 a.sales_user_name,
 a.business_type_code,
 a.business_type_name
union all 
select  a.s_month,
 a.performance_region_name,
 a.performance_province_name,
 a.performance_city_name,
 a.customer_code,
 a.customer_name,
 a.sales_user_number,
 a.sales_user_name,
 cast(a.business_type_code as int ) business_type_code,
 a.business_type_name,
 (a.sale_amt) sale_amt,
 (a.profit) profit
 from 
  tmp_new_cust_detail a
 )a 
 group by a.performance_region_name,
 a.performance_province_name,
 a.performance_city_name,
 a.sales_user_number,
 a.sales_user_name

 -- 销售新额履约额 1-630
 WITH tmp_new_cust_info AS (
    SELECT
        performance_region_code,
        performance_region_name,
        performance_province_code,
        performance_province_name,
        performance_city_code,
        performance_city_name,
        sales_user_number,
        sales_user_name,
        begin_date,
        sub_position_name,
        customer_code,
        customer_name,
        business_type_code,
        business_type_name,
        MIN(first_sale_data) AS first_sale_date
    FROM csx_analyse.csx_analyse_sales_new_customer_info_mf
    WHERE smt >= '202501'
    GROUP BY
        performance_region_code,
        performance_region_name,
        performance_province_code,
        performance_province_name,
        performance_city_code,
        performance_city_name,
        sales_user_number,
        sales_user_name,
        begin_date,
        sub_position_name,
        customer_code,
        customer_name,
        business_type_code,
        business_type_name
),
tmp_sale_detail AS (
    SELECT
        customer_code,
        business_type_code,
        SUM(sale_amt) AS sale_amt,
        SUM(profit) AS profit,
        sdt
    FROM csx_dws.csx_dws_sale_detail_di
    WHERE sdt >= '20250101'
    and sdt<='20250630'
    GROUP BY customer_code, business_type_code, sdt
)
SELECT
    a.performance_region_code,
    a.performance_region_name,
    a.performance_province_code,
    a.performance_province_name,
    a.performance_city_code,
    a.performance_city_name,
    a.sales_user_number,
    a.sales_user_name,
    a.begin_date,
    a.sub_position_name,
    a.customer_code,
    a.customer_name,
    a.business_type_code,
    a.business_type_name,
    a.first_sale_date,
    SUM(b.sale_amt) AS sale_amt,
    SUM(b.profit) AS profit,
    SUM(b.profit) / NULLIF(SUM(b.sale_amt), 0) AS profit_rate
FROM tmp_new_cust_info a
LEFT JOIN tmp_sale_detail b
    ON a.customer_code = b.customer_code
    AND a.business_type_code = b.business_type_code
    AND b.sdt >= a.first_sale_date
GROUP BY
    a.performance_region_code,
    a.performance_region_name,
    a.performance_province_code,
    a.performance_province_name,
    a.performance_city_code,
    a.performance_city_name,
    a.sales_user_number,
    a.sales_user_name,
    a.begin_date,
    a.sub_position_name,
    a.customer_code,
    a.customer_name,
    a.business_type_code,
    a.business_type_name,
    a.first_sale_date;