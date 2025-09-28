-- OKR数据-逾期客户 
-- 核心逻辑： 

-- 切换tez计算引擎
set mapred.job.name=report_sss_r_m_customer_accounts;
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
set target_table=csx_tmp.report_sss_r_m_customer_accounts;
	
with current_customer_accounts as 	
(
	select
		sales_region_code,
		sales_region_name,
		sales_province_code,
		sales_province_name,
		city_group_code,
		city_group_name,
		a.customer_no,
		b.customer_name,
		b.work_no,
		b.sales_name,
		b.sign_date,
		a.receivable_amount,
		a.overdue_amount,
		a.overdue_amount/a.receivable_amount as overdue_amount_rate
	from -- 应收逾期
		(
		select 
			customer_no,
			sum(case when receivable_amount>=0 then receivable_amount else 0 end) receivable_amount, -- 应收金额
			sum(case when overdue_amount>=0 and receivable_amount>0 then overdue_amount else 0 end) overdue_amount	-- 逾期金额
		from
			csx_dw.dws_sss_r_a_customer_accounts
		where
			sdt=${hiveconf:current_day}
		group by 
			customer_no
		) a 
	left join -- 客户信息
		(
		select 
			customer_no,customer_name,work_no,sales_name,sales_region_code,sales_region_name,sales_province_code,sales_province_name,city_group_code,city_group_name,
			regexp_replace(split(sign_time,' ')[0],'-','') as sign_date
		from 
			csx_dw.dws_crm_w_a_customer
		where 
			sdt='current'
		group by 
			customer_no,customer_name,work_no,sales_name,sales_region_code,sales_region_name,sales_province_code,sales_province_name,city_group_code,city_group_name,
			regexp_replace(split(sign_time,' ')[0],'-','')
		) b on a.customer_no=b.customer_no
)

insert overwrite table ${hiveconf:target_table} partition(quarter)			
			
select
	'' as biz_id,
	sales_region_code as region_code,
	sales_region_name as region_name,
	sales_province_code as province_code,
	sales_province_name as province_name,
	city_group_code,
	city_group_name,
	customer_no,
	customer_name,
	work_no,
	sales_name,
	sign_date,
	coalesce(receivable_amount,0) as receivable_amount,
	coalesce(overdue_amount) as overdue_amount,
	coalesce(overdue_amount_rate,0) as overdue_amount_rate,
	from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time, -- 更新时间
	'202102' as quarter -- 月份
from
	current_customer_accounts	
;



INVALIDATE METADATA csx_tmp.report_sss_r_m_customer_accounts;	



/*
--------------------------------- hive建表语句 -------------------------------
-- csx_tmp.report_sss_r_m_customer_accounts  OKR数据 逾期客户

drop table if exists csx_tmp.report_sss_r_m_customer_accounts;
create table csx_tmp.report_sss_r_m_customer_accounts(
`biz_id`                         string              COMMENT    '业务主键',
`region_code`                    string              COMMENT    '大区编码',
`region_name`                    string              COMMENT    '大区名称',
`province_code`                  string              COMMENT    '省份编码',
`province_name`                  string              COMMENT    '省份名称',
`city_group_code`                string              COMMENT    '城市组编码',
`city_group_name`                string              COMMENT    '城市组名称',
`customer_no`                    string              COMMENT    '客户编码',
`customer_name`                  string              COMMENT    '客户名称',
`work_no`                        string              COMMENT    '销售员工号',
`sales_name`                     string              COMMENT    '销售员名称',
`sign_date`                      string              COMMENT    '签约日期',
`receivable_amount`              decimal(26,6)       COMMENT    '应收账款',
`overdue_amount`                 decimal(26,6)       COMMENT    '逾期账款',
`overdue_amount_rate`            decimal(26,6)       COMMENT    '逾期率',
`update_time`                    string              COMMENT    '数据更新时间'
) COMMENT 'zhangyanpeng:OKR数据-逾期客户'
PARTITIONED BY (quarter string COMMENT '日期分区')
STORED AS TEXTFILE;

*/	