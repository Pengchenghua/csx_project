
-- 商品简报 负毛利top10商品
-- 核心逻辑： 统计省区城市负毛利top10商品

-- 切换tez计算引擎
set mapred.job.name=report_sale_r_d_negative_goods_top10;
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
set target_table=csx_dw.report_sale_r_d_negative_goods_top10;

-- 筛选负毛利商品
with negative_sale as 
(
  select 
  	province_name, 
  	city_group_name,
  	case when division_code in ('10', '11') then '生鲜'
  	  when division_code in ('12') then '食品'
  	  else '非食品' end as division_type,					-- 部类分组
  	department_name,										-- 课组
    goods_code,
    regexp_replace(goods_name, '\n|\t|\r', '') as goods_name,
    sum(sales_value) as total_sales_value,					-- 总销售额
    sum(profit) as total_profit,							-- 总毛利
    sum(profit) / abs(sum(sales_value)) as total_profit_prorate,		-- 总毛利率
    sum(sales_qty) as total_sales_qty,							-- 总销售数量
    count(distinct customer_no) as customer_amount,			-- 总客户数
    row_number() over(partition by city_group_name order by sum(profit) asc) as city_profit_no  -- 城市商品负毛利排名
  from ${hiveconf:source_sale_detail} 
  where sdt >= regexp_replace(date_sub(current_date,if(pmod(datediff(current_date, '2020-04-06'), 7) = 0, 3, 1) ),'-','') -- 周一跑三天数据 其它时间跑前一天数据
    and channel_code in ('1','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
	and business_type_code !=4 -- 业务类型编码(1.日配单 2.福利单 3.批发内购 4.城市服务商 5.省区大宗 6.BBC)
  group by province_name, city_group_name, 
    case when division_code in ('10', '11') then '生鲜'
  	  when division_code in ('12') then '食品'
  	  else '非食品' end,
  	department_name,								
    goods_code,
    regexp_replace(goods_name, '\n|\t|\r', '')
  having sum(sales_value) > 0 and sum(profit) < 0
),
goods_negative_days as 
(
  select 
    city_group_name,
    goods_code,
    count(distinct sdt) as sale_days -- 负毛利销售天数
  from 
  (
    select 
      city_group_name,
      goods_code,
      sdt,
      sum(profit) as total_profit
    from ${hiveconf:source_sale_detail} 
    where sdt >= ${hiveconf:current_start_day} and sdt <= ${hiveconf:current_day} 
    and channel_code in ('1','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
	and business_type_code !=4 -- 业务类型编码(1.日配单 2.福利单 3.批发内购 4.城市服务商 5.省区大宗 6.BBC)
    group by city_group_name, goods_code, sdt
    having sum(profit) < 0
  )t1 
  group by city_group_name, goods_code
)
insert overwrite table ${hiveconf:target_table} 
select 
  t1.province_name,
  t1.city_group_name,
  t1.goods_code,
  t1.goods_name,
  t1.division_type,
  t1.department_name,
  t1.total_sales_value,
  t1.total_profit,
  t1.total_profit_prorate,
  t1.total_sales_qty,
  t1.customer_amount,
  t2.sale_days,
  t1.city_profit_no
from negative_sale t1 left outer join goods_negative_days t2 
on t1.city_group_name = t2.city_group_name and t1.goods_code = t2.goods_code
where t1.city_profit_no <= 10;