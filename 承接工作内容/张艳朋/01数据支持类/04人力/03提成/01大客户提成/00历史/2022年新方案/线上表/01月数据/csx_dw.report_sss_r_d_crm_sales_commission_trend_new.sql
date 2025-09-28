---财务销售结算 crm 销售员销售结算 提成趋势

SET hive.execution.engine=mr;
--动态分区
SET hive.exec.parallel=true;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

--中间结果压缩
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
SET mapred.output.compression.type=BLOCK;
SET parquet.compression=SNAPPY;
--启用引号识别
set hive.support.quoted.identifiers=none;
set hive.map.aggr=true;

----昨天
set last_1day='20220131';
set created_time = from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss');

insert overwrite table csx_dw.report_sss_r_d_crm_sales_commission_trend_new partition(sdt) 
select
	concat_ws('&',cast(sales_id as string),${hiveconf:last_1day}) as biz_id,
	sales_id,
	work_no,
	sales_name,
	'20220131' as sales_date,
	sum(sales_value_commion) as sales_value_commion,
	sum(ripei_bbc_sales_value_commion) as ripei_bbc_sales_value_commion,
	sum(fuli_sales_value_commion) as fuli_sales_value_commion,
	sum(profit_commion) as profit_commion,
	sum(ripei_bbc_profit_commion) as ripei_bbc_profit_commion,
	sum(fuli_profit_commion) as fuli_profit_commion,
	sum(commion_total) as commion_total,
	sum(commion_ripei_bbc_total) as commion_ripei_bbc_total,
	sum(commion_fuli_total) as commion_fuli_total,
	smonth,
	from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as updated_time,
	${hiveconf:last_1day} as sdt
from 
	csx_dw.report_sss_r_m_crm_sales_customer_commission_new 
where 
	smonth='202201'
group by 
	sales_id,work_no,sales_name,smonth;