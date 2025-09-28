-- OKR数据-缺货数据 
-- 核心逻辑： 

-- 切换tez计算引擎
set mapred.job.name=report_sss_r_m_customer_statement;
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
set target_table=csx_tmp.report_sss_r_m_customer_statement;
	
with current_statement as 	
(
select
	b.sales_region_code,
	b.sales_region_name,
	b.sales_province_code,
	b.sales_province_name,
	b.city_group_code,
	b.city_group_name,
	sum(case when statement_state='20' then source_statement_amount else 0 end) as finish_statement,
	sum(source_statement_amount) as total_statement
from
	(
	select
		customer_code,source_statement_amount,statement_date,statement_state
	from
		csx_dw.dwd_sss_r_d_source_bill
	where
		regexp_replace(to_date(statement_date),'-','') between '20210401' and '20210630'
	) as a 
	left join 
		(
		select 
			customer_no,customer_name,work_no,sales_name,first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name,
			sales_region_code,sales_region_name,sales_province_code,sales_province_name,city_group_code,city_group_name
		from 
			csx_dw.dws_crm_w_a_customer
		where 
			sdt='current'
		group by 
			customer_no,customer_name,work_no,sales_name,first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name,
			sales_region_code,sales_region_name,sales_province_code,sales_province_name,city_group_code,city_group_name
		) b on b.customer_no=a.customer_code
group by 
	b.sales_region_code,
	b.sales_region_name,
	b.sales_province_code,
	b.sales_province_name,
	b.city_group_code,
	b.city_group_name
)

insert overwrite table ${hiveconf:target_table} partition(quarter)			
			
select 
	'' as biz_id,
	sales_region_code as region_code,
	sales_region_name as region_name,
	sales_province_code,
	sales_province_name,
	city_group_code,
	city_group_name,
	coalesce(finish_statement,0) as finish_statement,
	coalesce(total_statement,0) as total_statement,
	from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time, -- 更新时间
	'202102' as quarter -- 季度
from 
	current_statement
	
;



INVALIDATE METADATA csx_tmp.report_sss_r_m_customer_statement;	



/*
--------------------------------- hive建表语句 -------------------------------
-- csx_tmp.report_sss_r_m_customer_statement  OKR数据 对账数据

drop table if exists csx_tmp.report_sss_r_m_customer_statement;
create table csx_tmp.report_sss_r_m_customer_statement(
`biz_id`                         string              COMMENT    '业务主键',
`region_code`                    string              COMMENT    '大区编码',
`region_name`                    string              COMMENT    '大区名称',
`province_code`                  string              COMMENT    '省份编码',
`province_name`                  string              COMMENT    '省份名称',
`city_group_code`                string              COMMENT    '城市组编码',
`city_group_name`                string              COMMENT    '城市组名称',
`finish_statement`               decimal(26,6)       COMMENT    '完成对账金额',
`total_statement`                decimal(26,6)       COMMENT    '总金额',
`update_time`                    string              COMMENT    '数据更新时间'
) COMMENT 'zhangyanpeng:OKR数据-对账数据'
PARTITIONED BY (quarter string COMMENT '日期分区')
STORED AS TEXTFILE;

*/	