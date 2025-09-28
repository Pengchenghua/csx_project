-- 商品简报 课组统计
-- 核心逻辑： 商品课组销售统计

-- 切换tez计算引擎
set mapred.job.name=report_sale_r_m_department;
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
set target_table=csx_dw.report_sale_r_m_department;

with goods_sale_department as
(
  select
    province_name,city_group_name,division_type,department_name,total_sales_value,total_profit_prorate,
	total_sales_value/ sum(total_sales_value) over(partition by city_group_name) as department_sale_prorate -- 该课组销售占比
  from 
   (
    select 
      province_name, 
      city_group_name,
      case when division_code in ('10', '11') then '生鲜'
    	when division_code in ('12') then '食品'
    	else '非食品' end as division_type,					-- 部类分组
      department_name,										-- 课组
      sum(sales_value) as total_sales_value,					-- 总销售额
      sum(profit) / abs(sum(sales_value)) as total_profit_prorate		-- 总毛利率
    from ${hiveconf:source_sale_detail} 
    where sdt >= ${hiveconf:current_start_day} and sdt <= ${hiveconf:current_day}
      and channel_code in ('1','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
      and business_type_code !=4 -- 业务类型编码(1.日配单 2.福利单 3.批发内购 4.城市服务商 5.省区大宗 6.BBC)
    group by province_name, city_group_name, 
      case when division_code in ('10', '11') then '生鲜'
    	  when division_code in ('12') then '食品'
    	  else '非食品' end,
    	department_name
	)t1
)
	
insert overwrite table ${hiveconf:target_table} partition(month) 
select 
  *,
  substr(${hiveconf:current_day}, 1, 6) as month
from goods_sale_department 