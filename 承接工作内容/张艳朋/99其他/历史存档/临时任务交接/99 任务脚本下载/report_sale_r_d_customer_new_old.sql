--毛利专题：新老客户毛利（负毛利共用）月至今毛利跟踪

-- 动态分区
set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

--select ${hiveconf:sdate_by1},${hiveconf:sdate_by2},${hiveconf:sdate_fml_sy1},${hiveconf:sdate_fml_sy2},${hiveconf:sdate_d90};

-- 昨日月1日，昨日、第前90天，4季度前首日;
set sdate_by1 =regexp_replace(trunc(date_sub(current_date,1),'MM'),'-','');
set sdate_by2 =regexp_replace(date_sub(current_date,1),'-','');
set sdate_d90 =regexp_replace(date_sub(current_date,91),'-','');
set before_4qua=regexp_replace(to_date(concat(date_format(date_sub(current_date, 1),'y'),'-',
                      floor(cast(date_format(date_sub(current_date, 1),'M') as int)/3.1)*3+1-3*3,'-',
                      date_format(trunc(date_sub(current_date, 1),'MM'),'dd'))
                  ),'-','');
set i_sdate_dd =from_utc_timestamp(current_timestamp(),'GMT');  ------当前时间


--insert overwrite directory '/tmp/raoyanhua/07fumaoli' row format delimited fields terminated by '\t'
insert overwrite table csx_dw.report_sale_r_d_customer_new_old partition(sdt)
select 
  -- 唯一主键 
  concat_ws('&', a.province_code,a.city_group_code,a.customer_no,${hiveconf:sdate_by2}) as biz_id,    
  a.region_code,a.region_name,a.province_code,a.province_name,a.city_group_code,a.city_group_name,a.customer_no,b.customer_name,
  substr(b.sign_date,1,6) sign_month,
  substr(c.first_sales_date,1,6) first_sales_month,
  b.attribute_name,
  case when c.first_sales_date<${hiveconf:before_4qua} then '老客'
     else concat(substr(from_unixtime(unix_timestamp(c.first_sales_date,'yyyymmdd'),'yyyy-mm-dd'),1,4),'Q',
            (floor(substr(from_unixtime(unix_timestamp(c.first_sales_date,'yyyymmdd'),'yyyy-mm-dd'),6,2)/3.1))+1,'新客')
     end as cust_group,
  a.sales_value,a.profit,a.front_profit,a.prorate,e.prorate_d90,
  a.prorate-e.prorate_d90 prorate_diff,
  a.SKU,a.count_days,
  'raoyanhua' create_by,
  ${hiveconf:i_sdate_dd} create_time,
  ${hiveconf:sdate_by2} sdt  
from 
  (select
    region_code,region_name,province_code,province_name,city_group_code,city_group_name,customer_no,
    sum(sales_value)as sales_value,
    sum(profit)as profit,
    sum(front_profit) as front_profit,
    sum(profit)/abs(sum(sales_value)) as prorate,
    count(distinct goods_code) SKU,
    count(distinct sdt) count_days
  from csx_dw.dws_sale_r_d_detail
  where sdt >=${hiveconf:sdate_by1} and sdt <=${hiveconf:sdate_by2}
  and channel_code in('1','7','9')
  and ((business_type_code='1' and dc_code not in('W0K4')) or business_type_code<>'1')
  and (order_no not in ('OC200529000043','OC200529000044','OC200529000045','OC200529000046',
          'OC20111000000021','OC20111000000022','OC20111000000023','OC20111000000024','OC20111000000025') or order_no is null)
  group by region_code,region_name,province_code,province_name,city_group_code,city_group_name,customer_no
  )a
left join   --CRM客户信息昨日
  (select customer_no,customer_name,attribute_desc as attribute_name,attribute as attribute_code,
    regexp_replace(split(sign_time, ' ')[0], '-', '') as sign_date
  from csx_dw.dws_crm_w_a_customer
  where sdt=${hiveconf:sdate_by2}  --昨日
  )b on b.customer_no=a.customer_no
left join --首单日期
  (select customer_no,min(first_order_date) first_sales_date
  from csx_dw.dws_crm_w_a_customer_active
  where sdt = ${hiveconf:sdate_by2}
  group by customer_no
  )c on c.customer_no=a.customer_no
left join   --客户近90天毛利率
  (select
    customer_no,
    sum(sales_value)as sales_value,
    sum(profit)as profit,
    sum(profit)/abs(sum(sales_value)) as prorate_d90
  from csx_dw.dws_sale_r_d_detail
  where sdt >=${hiveconf:sdate_d90} and sdt <=${hiveconf:sdate_by2}  --近90天
  and channel_code in('1','7','9')
    and ((business_type_code='1' and dc_code not in('W0K4')) or business_type_code<>'1')
  and (order_no not in ('OC200529000043','OC200529000044','OC200529000045','OC200529000046',
          'OC20111000000021','OC20111000000022','OC20111000000023','OC20111000000024','OC20111000000025') or order_no is null)
  group by customer_no
  )e on e.customer_no=a.customer_no
--where a.profit<-500
;