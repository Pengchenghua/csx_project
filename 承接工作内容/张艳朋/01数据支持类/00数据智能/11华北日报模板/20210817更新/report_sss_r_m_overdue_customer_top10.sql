-- 华北日报 
-- 核心逻辑：按省区各客户统计逾期金额排名

-- 切换tez计算引擎
set mapred.job.name=report_sss_r_m_overdue_customer_top10;
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
set target_table=csx_tmp.report_sss_r_m_overdue_customer_top10;
	
with current_receivable_overdue_amount as 	
(
select 
	sales_region_code,
	sales_region_name,
	sales_province_code,
	sales_province_name,
	customer_no,
	customer_name,
	work_no,
	sales_name,
	first_supervisor_work_no,
	first_supervisor_name,
	overdue_amount,
	ranking
from
	(
	select
		b.sales_region_code,
		b.sales_region_name,
		b.sales_province_code,
		b.sales_province_name,
		a.customer_no,
		b.customer_name,
		b.work_no,
		b.sales_name,
		b.first_supervisor_work_no,
		b.first_supervisor_name,
		a.overdue_amount,
		row_number() over(partition by sales_province_name order by overdue_amount desc) as ranking
	from
		( 
		select --应收逾期
			customer_no,
			sum(case when overdue_amount>=0 and receivable_amount>0 then overdue_amount else 0 end) overdue_amount	-- 逾期金额
		from
			csx_dw.dws_sss_r_a_customer_accounts
		where
			sdt=${hiveconf:current_day}
		group by 
			customer_no
		)a
		left join -- 客户信息
			(
			select 
				customer_no,customer_name,sales_region_code,sales_region_name,sales_province_code,sales_province_name,work_no,sales_name,first_supervisor_work_no,first_supervisor_name
			from 
				csx_dw.dws_crm_w_a_customer
			where 
				sdt='current'
			) as b on b.customer_no=a.customer_no
	) as tmp1
where
	ranking<=10
)

insert overwrite table ${hiveconf:target_table} partition(month)			
			
select
	sales_region_code as region_code,
	sales_region_name as region_name,
	sales_province_code as province_code,
	sales_province_name as province_name,
	customer_no,
	customer_name,
	work_no,
	sales_name,
	first_supervisor_work_no as supervisor_work_no,
	first_supervisor_name as supervisor_name,
	overdue_amount,
	ranking,
	from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time, -- 更新时间
	substr(${hiveconf:current_day}, 1, 6) as month
from
	current_receivable_overdue_amount
;



INVALIDATE METADATA csx_tmp.report_sss_r_m_overdue_customer_top10;	



/*
--------------------------------- hive建表语句 -------------------------------
-- csx_tmp.report_sss_r_m_overdue_customer_top10  华北日报 各省份客户逾期排名

drop table if exists csx_tmp.report_sss_r_m_overdue_customer_top10;
create table csx_tmp.report_sss_r_m_overdue_customer_top10(
`region_code`                    string              COMMENT    '大区编码',
`region_name`                    string              COMMENT    '大区名称',
`province_code`                  string              COMMENT    '省份编码',
`province_name`                  string              COMMENT    '省份名称',
`customer_no`                    string              COMMENT    '客户编码',
`customer_name`                  string              COMMENT    '客户名称',
`work_no`                        string              COMMENT    '销售员工号',
`sales_name`                     string              COMMENT    '销售员名称',
`supervisor_work_no`             string              COMMENT    '主管工号',
`supervisor_name`                string              COMMENT    '主管名称',
`overdue_amount`                 decimal(26,6)       COMMENT    '逾期金额',
`ranking`                        int                 COMMENT    '排名',
`update_time`                    string              COMMENT    '数据更新时间'
) COMMENT 'zhangyanpeng:华北日报-各省份客户逾期排名'
PARTITIONED BY (month string COMMENT '日期分区')
STORED AS TEXTFILE;

*/	