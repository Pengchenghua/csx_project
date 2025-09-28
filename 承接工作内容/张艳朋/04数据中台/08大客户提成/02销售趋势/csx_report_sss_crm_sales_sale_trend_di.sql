-- 动态分区
SET hive.exec.parallel=true;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;


insert overwrite table csx_report.csx_report_sss_crm_sales_sale_trend_di

select
	*
from
	csx_analyse.csx_analyse_report_sss_crm_sales_sale_trend_di
where
	sdt>='${last_month_s_day}'
;

/*
create table csx_report.csx_report_sss_crm_sales_sale_trend_di(
`biz_id`                         string              COMMENT    '业务主键',
`sales_user_id`                  string              COMMENT    '销售员id',
`sales_user_number`              string              COMMENT    '销售员工号',
`sales_user_name`                string              COMMENT    '销售员名称',
`sale_date`                      string              COMMENT    '销售日期',
`sale_amt`                       decimal(20,6)       COMMENT    '销售额',
`ripei_sale_amt`                 decimal(20,6)       COMMENT    '日配销售额',
`bbc_sale_amt`                   decimal(20,6)       COMMENT    'BBC销售额',
`ripei_bbc_sale_amt`             decimal(20,6)       COMMENT    '日配&BBC销售额',
`fuli_sale_amt`                  decimal(20,6)       COMMENT    '福利销售额',
`profit`                         decimal(20,6)       COMMENT    '定价毛利额',
`ripei_profit`                   decimal(20,6)       COMMENT    '日配定价毛利额',
`bbc_profit`                     decimal(20,6)       COMMENT    'BBC定价毛利额',
`ripei_bbc_profit`               decimal(20,6)       COMMENT    '日配&BBC定价毛利额',
`fuli_profit`                    decimal(20,6)       COMMENT    '福利定价毛利额',
`refund_sale_amt`                decimal(20,6)       COMMENT    '退货金额',
`refund_ripei_sale_amt`          decimal(20,6)       COMMENT    '日配退货金额',
`refund_bbc_sale_amt`            decimal(20,6)       COMMENT    'BBC退货金额',
`refund_ripei_bbc_sale_amt`      decimal(20,6)       COMMENT    '日配&BBC退货金额',
`refund_fuli_sale_amt`           decimal(20,6)       COMMENT    '福利退货金额',
`smonth`                         string              COMMENT    '年月',
`updated_time`                   string              COMMENT    '更新时间',
`sdt`                      		 string              COMMENT    '销售日期'
) COMMENT '业务员销售额趋势表'
STORED AS PARQUET;

*/	