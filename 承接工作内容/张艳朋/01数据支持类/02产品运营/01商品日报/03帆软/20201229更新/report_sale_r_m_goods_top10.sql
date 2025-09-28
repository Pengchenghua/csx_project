-- 商品简报 销售额top10信息
-- 核心逻辑： 统计省区城市top10商品信息

-- 切换tez计算引擎
set mapred.job.name=report_sale_r_m_goods_top10;
SET hive.execution.engine=tez;
SET tez.queue.name=caishixian;

-- 动态分区
SET hive.exec.parallel=true;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions =1000;
SET hive.exec.max.dynamic.partitions.pernode =1000;

-- 中间结果压缩
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
SET mapred.output.compression.type=BLOCK;
SET parquet.compression=SNAPPY;

-- 启用引号识别
set hive.support.quoted.identifiers=none;

-- 计算日期
set current_day = regexp_replace(date_sub(current_date, 1), '-', '');

-- 当月第一天
set current_start_day = regexp_replace(trunc(date_sub(current_date, 1), 'MM'), '-', '');

-- b端客户月统计销售表
set source_sale_detail=csx_dw.dws_sale_r_d_detail;

-- 目标表
set target_table=csx_dw.report_sale_r_m_goods_top10;

insert overwrite table ${hiveconf:target_table} partition(month) 
with goods_sales as 
(
  select 
    concat_ws('-', city_group_name, goods_code, substr(${hiveconf:current_day}, 1, 6)) as id,
    province_name,
    city_group_name,
    goods_code,
    goods_name,
    sum(sales_value) as sales_value,
    sum(profit) as profit,
    sum(profit) / abs(sum(sales_value)) as profit_prorate,
    count(distinct customer_no) as customer_amount,
    row_number() over(partition by city_group_name order by sum(sales_value) desc) as goods_sale_no
  from ${hiveconf:source_sale_detail} 
  where sdt >= ${hiveconf:current_start_day} and sdt <= ${hiveconf:current_day} 
    and channel_code in ('1','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
	and business_type_code !=4 -- 业务类型编码(1.日配单 2.福利单 3.批发内购 4.城市服务商 5.省区大宗 6.BBC)
  group by province_name, city_group_name, goods_code, goods_name
),
goods_sales_customer as 
(
  select 
    province_name,
    city_group_name,
    count(distinct customer_no) as customer_cnt
  from ${hiveconf:source_sale_detail} 
  where sdt >= ${hiveconf:current_start_day} and sdt <= ${hiveconf:current_day} 
    and channel_code in ('1','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
	and business_type_code !=4 -- 业务类型编码(1.日配单 2.福利单 3.批发内购 4.城市服务商 5.省区大宗 6.BBC)
  group by province_name, city_group_name
)

select 
  t1.id,
  t1.province_name,
  t1.city_group_name,
  t1.goods_code,
  t1.goods_name,
  t1.sales_value,
  t1.profit,
  t1.profit_prorate,
  t1.customer_amount,
  t1.customer_amount/t2.customer_cnt as customer_amount_prorate,
  t1.goods_sale_no,
  substr(${hiveconf:current_day}, 1, 6) as month
from goods_sales as t1 left join goods_sales_customer as t2 on t2.city_group_name=t1.city_group_name
where t1.goods_sale_no <= 10;