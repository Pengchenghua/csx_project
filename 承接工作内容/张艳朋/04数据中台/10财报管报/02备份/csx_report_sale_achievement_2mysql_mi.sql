-- 动态分区
set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions =1000;
set hive.exec.max.dynamic.partitions.pernode =1000;


insert overwrite table csx_report.csx_report_sale_achievement_2mysql_mi

select 
	*
from 
	csx_analyse.csx_analyse_report_sale_achievement_mi
where
	sdt>=regexp_replace(add_months(trunc('${ytd_date}','MM'),-1),'-','') and sdt<='${ytd}'


/*
create table csx_report.csx_report_sale_achievement_2mysql_mi(
`biz_id`                         string              COMMENT    '业务唯一id',
`performance_region_code`        string              COMMENT    '大区编码',
`performance_region_name`        string              COMMENT    '大区名称',
`performance_province_code`      string              COMMENT    '省区编码',
`performance_province_name`      string              COMMENT    '省区名称',
`performance_city_code`          string              COMMENT    '城市组',
`performance_city_name`          string              COMMENT    '城市组名称',
`channel_code`                   string              COMMENT    '渠道编码',
`channel_name`                   string              COMMENT    '渠道名称',
`customer_code`                  string              COMMENT    '客户编码',
`customer_name`                  string              COMMENT    '客户名称',
`attribute_code`                 string              COMMENT    '客户属性',
`is_new_sign`                    string              COMMENT    '是否新签约客户',
`is_new_sale`                    string              COMMENT    '是否新下单客户',
`sale_amt`                       decimal(20,6)       COMMENT    '销售额',
`sale_cost`                      decimal(20,6)       COMMENT    '销售成本',
`profit`                         decimal(20,6)       COMMENT    '毛利',
`sale_amt_no_tax`                decimal(20,6)       COMMENT    '不含税销售额',
`sale_cost_no_tax`               decimal(20,6)       COMMENT    '不含税成本',
`profit_no_tax`                  decimal(20,6)       COMMENT    '不含税毛利',
`sdt` 							 string 			 COMMENT 	'日期分区'

) COMMENT '财报管报-业绩达成'
STORED AS PARQUET;

*/	