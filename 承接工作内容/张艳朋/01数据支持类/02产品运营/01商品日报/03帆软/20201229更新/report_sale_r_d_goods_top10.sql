-- 商品简报 销售额top10信息
-- 核心逻辑： 统计省区城市top10商品信息

-- 切换tez计算引擎
set mapred.job.name=report_sale_r_d_goods_top10;
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
-- 周一跑三天数据 其它时间跑前一天数据
set start_day = regexp_replace(date_sub(current_date,if(pmod(datediff(current_date, '2020-04-06'), 7) = 0, 3, 1) ),'-',''); 

-- b端客户月统计销售表
set source_sale_detail=csx_dw.dws_sale_r_d_detail;

-- 目标表
set target_table=csx_dw.report_sale_r_d_goods_top10;

with goods_sale_rank as 
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
    row_number() over(partition by city_group_name order by sum(sales_value) desc) as city_sale_no  -- 城市商品负毛利排名
  from ${hiveconf:source_sale_detail} 
  where sdt >= ${hiveconf:start_day} -- 周一跑三天数据 其它时间跑前一天数据
    and channel_code in ('1','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
	and business_type_code !=4 -- 业务类型编码(1.日配单 2.福利单 3.批发内购 4.城市服务商 5.省区大宗 6.BBC)
  group by province_name, city_group_name, 
    case when division_code in ('10', '11') then '生鲜'
  	  when division_code in ('12') then '食品'
  	  else '非食品' end,
  	department_name,								
    goods_code,
    regexp_replace(goods_name, '\n|\t|\r', '')
)
insert overwrite table ${hiveconf:target_table} 
select 
  *
from goods_sale_rank 
where city_sale_no <= 10;