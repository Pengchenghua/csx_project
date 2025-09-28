-- 切换tez计算引擎
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

-- 昨日
set one_day_ago = regexp_replace(date_sub(current_date,1),'-','');

-- 目标表
set target_table=csx_tmp.report_wms_r_d_province_stock;

with current_province_stock as 	
(	
select
	a.sales_region_code,
	a.sales_region_name,
	a.performance_province_code,
	a.performance_province_name,
	a.performance_city_code,
	a.performance_city_name,
	sum(b.amt_no_tax) as amt_no_tax,
	sum(b.amt) as amt
from
	(
	select 
		shop_id,shop_name,province_code,province_name,city_code,city_name,sales_region_code,sales_region_name,
		sales_province_code,sales_province_name,city_group_code,city_group_name,
		performance_province_code,performance_province_name,performance_city_code,performance_city_name
	from 
		csx_dw.dws_basic_w_a_csx_shop_m 
	where 
		sdt = ${hiveconf:one_day_ago}
		and table_type=1
	) a 
	left join
		(
		select
			biz_id,goods_code,goods_name,unit,division_code,division_name,department_id,department_name,dc_code,dc_name,company_code,company_name,
			qty,price,amt,amt_no_tax
		from
			csx_dw.dws_wms_r_d_accounting_stock_m
		where
			sdt=${hiveconf:one_day_ago}
			and sys='new'
			and substr(reservoir_area_code, 1, 2) <> 'PD'
			and substr(reservoir_area_code, 1, 2) <> 'TS'
		) b on a.shop_id=b.dc_code
group by 
	a.sales_region_code,
	a.sales_region_name,
	a.performance_province_code,
	a.performance_province_name,
	a.performance_city_code,
	a.performance_city_name	
)

insert overwrite table ${hiveconf:target_table} partition(sdt)	

select
	concat_ws('&',performance_city_code,${hiveconf:one_day_ago}) as biz_id,
	sales_region_code as region_code,
	sales_region_name as region_name,
	performance_province_code as province_code,
	performance_province_name as province_name,
	performance_city_code as city_code,
	performance_city_name as city_name,
	amt_no_tax,
	amt,
	from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time, -- 更新时间
	${hiveconf:one_day_ago} as sdt
from
	current_province_stock
;

--INVALIDATE METADATA csx_tmp.report_wms_r_d_province_stock;
	

/*
--------------------------------- hive建表语句 -------------------------------
-- csx_tmp.report_wms_r_d_province_stock  省区库存金额

drop table if exists csx_tmp.report_wms_r_d_province_stock;
create table csx_tmp.report_wms_r_d_province_stock(
`biz_id`                         string              COMMENT    '业务主键',
`region_code`                    string              COMMENT    '大区编码',
`region_name`                    string              COMMENT    '大区名称',
`province_code`                  string              COMMENT    '省区编码',
`province_name`                  string              COMMENT    '省区名称',
`city_group_code`                string              COMMENT    '城市编码',
`city_group_name`                string              COMMENT    '城市',
`amt_no_tax`                     decimal(15,6)       COMMENT    '不含税金额',
`amt`                            decimal(15,6)       COMMENT    '含税金额',
`update_time`                    string              COMMENT    '数据更新时间'

) COMMENT '省区库存金额'
PARTITIONED BY (sdt string COMMENT '日期分区')
STORED AS TEXTFILE;

*/	


		
	