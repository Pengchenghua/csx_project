-- 华北日报 
-- 核心逻辑：按省份维度统计应收逾期金额情况

-- 切换tez计算引擎
set mapred.job.name=report_sss_r_m_receivable_ovd_amt_pr;
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

-- 目标表
set target_table=csx_tmp.report_sss_r_m_receivable_ovd_amt_pr;
	
with current_receivable_overdue_amount as 	
(
select 
    b.region_code,
    b.region_name,
    a.province_code,
	a.province_name,
	a.receivable_amount,
	a.overdue_amount,
	a.overdue_amount1,
	a.overdue_amount15,
	a.overdue_amount30,
	a.overdue_amount60,
	a.overdue_amount90,
	a.overdue_amount120,
	a.overdue_amount180,
	a.overdue_amount365,
	a.overdue_amount730,
	a.overdue_amount1095
from 
	(
	select 
        province_code,
		province_name,
		sum(case when receivable_amount>=0 then receivable_amount else 0 end) receivable_amount,	
		sum(case when overdue_amount>=0 and receivable_amount>0 then overdue_amount else 0 end) overdue_amount,
		sum(case when overdue_amount>=0 and receivable_amount>0 then overdue_amount1 else 0 end) overdue_amount1,
		sum(case when overdue_amount>=0 and receivable_amount>0 then overdue_amount15 else 0 end) overdue_amount15,
		sum(case when overdue_amount>=0 and receivable_amount>0 then overdue_amount30 else 0 end) overdue_amount30,
		sum(case when overdue_amount>=0 and receivable_amount>0 then overdue_amount60 else 0 end) overdue_amount60,
		sum(case when overdue_amount>=0 and receivable_amount>0 then overdue_amount90 else 0 end) overdue_amount90,
		sum(case when overdue_amount>=0 and receivable_amount>0 then overdue_amount120 else 0 end) overdue_amount120,
		sum(case when overdue_amount>=0 and receivable_amount>0 then overdue_amount180 else 0 end) overdue_amount180,
		sum(case when overdue_amount>=0 and receivable_amount>0 then overdue_amount365 else 0 end) overdue_amount365,
		sum(case when overdue_amount>=0 and receivable_amount>0 then overdue_amount730 else 0 end) overdue_amount730,		
		sum(case when overdue_amount>=0 and receivable_amount>0 then overdue_amount1095 else 0 end) overdue_amount1095
	from  
		csx_dw.dws_sss_r_a_customer_accounts 
	where 
		sdt=${hiveconf:current_day}
	group by 
		province_code,province_name 
	)a
	left join
		( 
		select 
			region_code,region_name,province_code,province_name
		from 
			csx_dw.dws_sale_w_a_area_belong
		group by 
			region_code,region_name,province_code,province_name
		) b on a.province_code = b.province_code 	
)

insert overwrite table ${hiveconf:target_table} partition(month)			
			
select
    region_code,
    region_name,
    province_code,
	province_name,
	receivable_amount,
	overdue_amount,
	overdue_amount1,
	overdue_amount15,
	overdue_amount30,
	overdue_amount60,
	overdue_amount90,
	overdue_amount120,
	overdue_amount180,
	overdue_amount365,
	overdue_amount730,
	overdue_amount1095,
	from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time, -- 更新时间
	substr(${hiveconf:current_day}, 1, 6) as month
from
	current_receivable_overdue_amount
;



INVALIDATE METADATA csx_tmp.report_sss_r_m_receivable_ovd_amt_pr;	



/*
--------------------------------- hive建表语句 -------------------------------
-- csx_tmp.report_sss_r_m_receivable_ovd_amt_pr  华北日报 应收逾期金额统计-省份

drop table if exists csx_tmp.report_sss_r_m_receivable_ovd_amt_pr;
create table csx_tmp.report_sss_r_m_receivable_ovd_amt_pr(
`region_code`                    string              COMMENT    '大区编码',
`region_name`                    string              COMMENT    '大区名称',
`province_code`                  string              COMMENT    '省份编码',
`province_name`                  string              COMMENT    '省份名称',
`receivable_amount`              decimal(26,6)       COMMENT    '应收金额',
`overdue_amount`                 decimal(26,6)       COMMENT    '逾期金额',
`overdue_amount1`                decimal(26,6)       COMMENT    '逾期1-15天',
`overdue_amount15`               decimal(26,6)       COMMENT    '逾期15-30天',
`overdue_amount30`               decimal(26,6)       COMMENT    '逾期30-60天',
`overdue_amount60`               decimal(26,6)       COMMENT    '逾期60-90天',
`overdue_amount90`               decimal(26,6)       COMMENT    '逾期90-120天',
`overdue_amount120`              decimal(26,6)       COMMENT    '逾期120-180天',
`overdue_amount180`              decimal(26,6)       COMMENT    '逾期180-365天',
`overdue_amount365`              decimal(26,6)       COMMENT    '逾期1-2年',
`overdue_amount730`              decimal(26,6)       COMMENT    '逾期2-3年',
`overdue_amount1095`             decimal(26,6)       COMMENT    '逾期3年以上',
`update_time`                    string              COMMENT    '数据更新时间'
) COMMENT 'zhangyanpeng:华北日报-应收逾期金额统计-省份'
PARTITIONED BY (month string COMMENT '日期分区')
STORED AS TEXTFILE;

*/	