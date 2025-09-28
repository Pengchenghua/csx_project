-- 动态分区
SET hive.exec.parallel=true;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions =1000;
SET hive.exec.max.dynamic.partitions.pernode =1000;

-- 中间结果压缩
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
SET mapred.output.compression.type=BLOCK;

-- 启用引号识别
set hive.support.quoted.identifiers=none;
	
with current_receivable_overdue_amount as 	
(
select 
	b.performance_region_code,
	b.performance_region_name,
	b.performance_province_code,
	b.performance_province_name,
	b.performance_city_code,
	b.performance_city_name,
	sum(case when a.receivable_amount>=0 then a.receivable_amount else 0 end) receivable_amount,	
	sum(case when a.overdue_amount>=0 and a.receivable_amount>0 then a.overdue_amount else 0 end) overdue_amount,
	sum(case when a.overdue_amount>=0 and a.receivable_amount>0 then a.overdue_amount_15_day else 0 end) overdue_amount_15_day,
	sum(case when a.overdue_amount>=0 and a.receivable_amount>0 then a.overdue_amount_30_day else 0 end) overdue_amount_30_day,
	sum(case when a.overdue_amount>=0 and a.receivable_amount>0 then a.overdue_amount_60_day else 0 end) overdue_amount_60_day,
	sum(case when a.overdue_amount>=0 and a.receivable_amount>0 then a.overdue_amount_90_day else 0 end) overdue_amount_90_day,
	sum(case when a.overdue_amount>=0 and a.receivable_amount>0 then a.overdue_amount_120_day else 0 end) overdue_amount_120_day,
	sum(case when a.overdue_amount>=0 and a.receivable_amount>0 then a.overdue_amount_180_day else 0 end) overdue_amount_180_day,
	sum(case when a.overdue_amount>=0 and a.receivable_amount>0 then a.overdue_amount_1_year else 0 end) overdue_amount_1_year,
	sum(case when a.overdue_amount>=0 and a.receivable_amount>0 then a.overdue_amount_2_year else 0 end) overdue_amount_2_year,		
	sum(case when a.overdue_amount>=0 and a.receivable_amount>0 then a.overdue_amount_3_year else 0 end) overdue_amount_3_year,
	sum(case when a.overdue_amount>=0 and a.receivable_amount>0 then a.overdue_amount_more_3_year else 0 end) overdue_amount_more_3_year
from 
	(
	select 
		*
	from  
		csx_dws.csx_dws_sss_customer_invoice_bill_settle_stat_di
	where 
		sdt='${ytd}'
	)a
	join -- 客户信息
		(
		select 
			customer_code,customer_name,performance_region_code,performance_region_name,performance_province_code,performance_province_name,
			performance_city_code,performance_city_name,sales_user_number,sales_user_name,supervisor_user_number,supervisor_user_name
		from 
			csx_dim.csx_dim_crm_customer_info
		where 
			sdt='current'
		) as b on b.customer_code=a.customer_code
group by 
	b.performance_region_code,
	b.performance_region_name,
	b.performance_province_code,
	b.performance_province_name,
	b.performance_city_code,
	b.performance_city_name	
)

insert overwrite table csx_analyse.csx_analyse_fr_sss_receivable_ovd_amt_pr_mi partition(month)			
			
select
	performance_region_code,
	performance_region_name,
	performance_province_code,
	performance_province_name,
	performance_city_code,
	performance_city_name,
	receivable_amount,
	overdue_amount,
	overdue_amount_15_day,
	overdue_amount_30_day,
	overdue_amount_60_day,
	overdue_amount_90_day,
	overdue_amount_120_day,
	overdue_amount_180_day,
	overdue_amount_1_year,
	overdue_amount_2_year,
	overdue_amount_3_year,
	overdue_amount_more_3_year,
	from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time, -- 更新时间
	substr('${ytd}', 1, 6) as month
from
	current_receivable_overdue_amount
;


/*
--------------------------------- hive建表语句 -------------------------------
-- csx_analyse.csx_analyse_fr_sss_receivable_ovd_amt_pr_mi  应收逾期金额统计-省份

drop table if exists csx_analyse.csx_analyse_fr_sss_receivable_ovd_amt_pr_mi;
create table csx_analyse.csx_analyse_fr_sss_receivable_ovd_amt_pr_mi(
`performance_region_code`        string              COMMENT    '大区编码',
`performance_region_name`        string              COMMENT    '大区名称',
`performance_province_code`      string              COMMENT    '省份编码',
`performance_province_name`      string              COMMENT    '省份名称',
`performance_city_code`          string              COMMENT    '城市编码',
`performance_city_name`          string              COMMENT    '城市名称',
`receivable_amount`              decimal(26,6)       COMMENT    '应收金额',
`overdue_amount`                 decimal(26,6)       COMMENT    '逾期金额',
`overdue_amount_15_day`          decimal(26,6)       COMMENT    '逾期1-15天',
`overdue_amount_30_day`          decimal(26,6)       COMMENT    '逾期15-30天',
`overdue_amount_60_day`          decimal(26,6)       COMMENT    '逾期30-60天',
`overdue_amount_90_day`          decimal(26,6)       COMMENT    '逾期60-90天',
`overdue_amount_120_day`         decimal(26,6)       COMMENT    '逾期90-120天',
`overdue_amount_180_day`         decimal(26,6)       COMMENT    '逾期120-180天',
`overdue_amount_1_year`          decimal(26,6)       COMMENT    '逾期180-365天',
`overdue_amount_2_year`          decimal(26,6)       COMMENT    '逾期1-2年',
`overdue_amount_3_year`          decimal(26,6)       COMMENT    '逾期2-3年',
`overdue_amount_more_3_year`     decimal(26,6)       COMMENT    '逾期3年以上',
`update_time`                    string              COMMENT    '数据更新时间'
) COMMENT '应收逾期金额统计-省份'
PARTITIONED BY (month string COMMENT '日期分区')
STORED AS TEXTFILE;

*/	