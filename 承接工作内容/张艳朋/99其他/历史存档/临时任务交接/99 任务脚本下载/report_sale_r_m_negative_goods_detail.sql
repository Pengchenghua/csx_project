-- 商品简报  B端负毛利商品明显
-- 核心逻辑：筛选B端负毛利商品明显

-- 切换tez计算引擎
set mapred.job.name=report_sale_r_m_negative_goods_detail;
-- set hive.execution.engine=tez;
set tez.queue.name=caishixian;

-- 动态分区
set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions =1000;
set hive.exec.max.dynamic.partitions.pernode =1000;

-- 中间结果压缩
set mapred.output.compression.codec=org.apache.hadoop.io.compress.snappycodec;
set mapred.output.compression.type=block;
set parquet.compression=snappy;

-- 启用引号识别
set hive.support.quoted.identifiers=none;

-- 统计日期
set current_day = regexp_replace(date_sub(current_date, 1), '-', '');

-- 当月月初
set current_start_day = regexp_replace(trunc(date_sub(current_date, 1), 'MM'),'-','');

-- 库存操作起始日期
set wms_start_day = regexp_replace(add_months(trunc(date_sub(current_date, 1), 'MM'), -11),'-','');

-- 当前月
set currnet_month = substr(${hiveconf:current_day}, 1, 6);

-- 销售明细表
set source_sale_detail = csx_dw.dws_sale_r_d_detail;

-- 库存操作明细表
set source_wms_stock_detail = csx_dw.dws_wms_r_d_batch_detail;

-- 工厂订单明细
set source_factory_order = csx_dw.dws_mms_r_a_factory_order;

-- 目标表
set target_table = csx_dw.report_sale_r_m_negative_goods_detail;

-- B端销售明细
with 2b_sale_detail as 
(
  select 
    id,
    sdt,
    order_no,
    origin_order_no,
    business_type_code,
    business_type_name,
    channel_code,
    channel_name,
    city_group_code,
    city_group_name,
    dc_code,
    dc_name,
    split(id, '&')[0] as credential_no,
    region_code,
    region_name,
    province_code,
    province_name,
    customer_no,
    customer_name,
    attribute_code,
    attribute_name,
    first_category_code,
    first_category_name,
    goods_code,
    goods_name,
    department_code,
    department_name,
    category_large_code,
    category_large_name,
    category_middle_code,
    category_middle_name,
    is_factory_goods,
    sales_qty,
    sales_value,
    profit,
    front_profit,
	purchase_price_flag,
    cost_price,
    case when purchase_price_flag='1' then purchase_price end as purchase_price,
    middle_office_price,
    sales_price,
    sum(sales_value) over(partition by goods_code) as m_goods_group_sales_value,
    sum(profit) over(partition by goods_code) as m_goods_group_profit,
    sum(sales_value) over(partition by goods_code, sdt) as d_goods_group_sales_value,
    sum(profit) over(partition by goods_code, sdt) as d_goods_group_profit,
    sum(sales_value) over(partition by customer_no) as m_customer_group_sales_value,
    sum(profit) over(partition by customer_no) as m_customer_group_profit,
    sum(sales_value) over(partition by customer_no, sdt) as d_customer_group_sales_value,
    sum(profit) over(partition by customer_no, sdt) as d_customer_group_profit
  from ${hiveconf:source_sale_detail} 
  where sdt >= ${hiveconf:current_start_day} and sdt <= ${hiveconf:current_day} and channel_code in ('1', '7', '9')
),
-- 库存明细
wms_stock_detail as 
(
  select
    goods_code,
    credential_no,
    source_order_no,
    sum(qty) as qty
  from ${hiveconf:source_wms_stock_detail} 
  where sdt >= ${hiveconf:wms_start_day} 
  and move_type in ('107A', '108A')
  group by goods_code, credential_no, source_order_no
),
-- 工厂订单
factory_order as 
(
  select 
  	goods_code,
  	order_code,
    sum(fact_values)/sum(goods_reality_receive_qty) as fact_price
  from ${hiveconf:source_factory_order} 
  where sdt >= ${hiveconf:wms_start_day} and mrp_prop_key in('3061','3010')
  group by goods_code, order_code
),
-- 凭证商品的工厂物料价
goods_factory_price as 
 (
  select
    t2.goods_code,
    t2.credential_no,
    sum(t2.qty) as qty,
    sum(t3.fact_price*t2.qty)/sum(case when t3.fact_price is not null then t2.qty end) fact_price
  from wms_stock_detail t2 
  left outer join factory_order t3 on t2.source_order_no = t3.order_code and t2.goods_code = t3.goods_code
  group by t2.goods_code,t2.credential_no
 )
insert overwrite table ${hiveconf:target_table} partition(month) 
select 
  t1.id,
  t1.sdt,
  t1.order_no,
  t1.origin_order_no,
  t1.business_type_code,
  t1.business_type_name,
  t1.channel_code,
  t1.channel_name,
  t1.city_group_code,
  t1.city_group_name,
  t1.dc_code,
  t1.dc_name,
  t1.credential_no,
  t1.region_code,
  t1.region_name,
  t1.province_code,
  t1.province_name,
  t1.customer_no,
  t1.customer_name,
  t1.attribute_code,
  t1.attribute_name,
  t1.first_category_code,
  t1.first_category_name,
  t1.goods_code,
  t1.goods_name,
  t1.department_code,
  t1.department_name,
  t1.category_large_code,
  t1.category_large_name,
  t1.category_middle_code,
  t1.category_middle_name,
  t1.is_factory_goods,
  t1.sales_qty,
  t1.sales_value,
  t1.profit,
  t1.front_profit,
  t1.cost_price,
  t1.purchase_price,
  t1.middle_office_price,
  t1.sales_price,
  t2.fact_price,
  case when t1.m_goods_group_sales_value > 0 and t1.m_goods_group_profit < 0 then '是' else '否' end as month_negative_goods_flag,
  case when t1.d_goods_group_sales_value > 0 and t1.d_goods_group_profit < 0 then '是' else '否' end as day_negative_goods_flag,
  case when t1.m_customer_group_sales_value > 0 and t1.m_customer_group_profit < 0 then '是' else '否' end as month_negative_customer_flag,
  case when t1.d_customer_group_sales_value > 0 and t1.d_customer_group_profit < 0 then '是' else '否' end as day_negative_customer_flag,
  ${hiveconf:currnet_month} as month
from 2b_sale_detail t1
left outer join goods_factory_price t2 
	on t1.goods_code = t2.goods_code and t1.credential_no = t2.credential_no ;
