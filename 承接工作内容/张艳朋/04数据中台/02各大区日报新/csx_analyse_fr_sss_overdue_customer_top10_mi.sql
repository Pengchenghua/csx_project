
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
	performance_region_code,
	performance_region_name,
	performance_province_code,
	performance_province_name,
	performance_city_code,
	performance_city_name,
	customer_code,
	customer_name,
	sales_user_number,
	sales_user_name,
	supervisor_user_number,
	supervisor_user_name,
	overdue_amount,
	ranking
from
	(
	select
		b.performance_region_code,
		b.performance_region_name,
		b.performance_province_code,
		b.performance_province_name,
		b.performance_city_code,
		b.performance_city_name,
		a.customer_code,
		b.customer_name,
		b.sales_user_number,
		b.sales_user_name,
		b.supervisor_user_number,
		b.supervisor_user_name,
		a.overdue_amount,
		row_number() over(partition by performance_city_name order by overdue_amount desc) as ranking
	from
		( 
		select --应收逾期
			customer_code,
			sum(case when overdue_amount>=0 and receivable_amount>0 then overdue_amount else 0 end) overdue_amount	-- 逾期金额
		from
			csx_dws.csx_dws_sss_customer_invoice_bill_settle_stat_di
		where
			sdt='${ytd}'
		group by 
			customer_code
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
	) as tmp1
where
	ranking<=10
)

insert overwrite table csx_analyse.csx_analyse_fr_sss_overdue_customer_top10_mi partition(month)			
			
select
	performance_region_code,
	performance_region_name,
	performance_province_code,
	performance_province_name,
	performance_city_code,
	performance_city_name,
	customer_code,
	customer_name,
	sales_user_number,
	sales_user_name,
	supervisor_user_number,
	supervisor_user_name,
	overdue_amount,
	ranking,
	from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time, -- 更新时间
	substr('${ytd}', 1, 6) as month
from
	current_receivable_overdue_amount
;



/*
--------------------------------- hive建表语句 -------------------------------
-- csx_analyse.csx_analyse_fr_sss_overdue_customer_top10_mi  各省份客户逾期排名

drop table if exists csx_analyse.csx_analyse_fr_sss_overdue_customer_top10_mi;
create table csx_analyse.csx_analyse_fr_sss_overdue_customer_top10_mi(
`performance_region_code`        string              COMMENT    '大区编码',
`performance_region_name`        string              COMMENT    '大区名称',
`performance_province_code`      string              COMMENT    '省份编码',
`performance_province_name`      string              COMMENT    '省份名称',
`performance_city_code`          string              COMMENT    '城市编码',
`performance_city_name`          string              COMMENT    '城市名称',
`customer_code`                  string              COMMENT    '客户编码',
`customer_name`                  string              COMMENT    '客户名称',
`sales_user_number`              string              COMMENT    '销售员工号',
`sales_user_name`                string              COMMENT    '销售员名称',
`supervisor_user_number`         string              COMMENT    '主管工号',
`supervisor_user_name`           string              COMMENT    '主管名称',
`overdue_amount`                 decimal(26,6)       COMMENT    '逾期金额',
`ranking`                        int                 COMMENT    '排名',
`update_time`                    string              COMMENT    '数据更新时间'
) COMMENT '各省份客户逾期排名'
PARTITIONED BY (month string COMMENT '日期分区')
STORED AS TEXTFILE;

*/	