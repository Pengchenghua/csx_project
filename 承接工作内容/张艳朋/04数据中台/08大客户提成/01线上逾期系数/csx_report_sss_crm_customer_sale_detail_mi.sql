-- 动态分区
SET hive.exec.parallel=true;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;


-- 刷新数据
insert overwrite table csx_report.csx_report_sss_crm_customer_sale_detail_2mysql_mi

select
	*
from
	csx_analyse.csx_analyse_report_sss_crm_customer_sale_detail_mi
where 
    month>='${last_month}'
;


/*

create table csx_report.csx_report_sss_crm_customer_sale_detail_2mysql_mi(
`biz_id`                         string              COMMENT    '业务主键',
`performance_region_code`        string              COMMENT    '大区编码',
`performance_region_name`        string              COMMENT    '大区名称',
`performance_province_code`      string              COMMENT    '省区编码',
`performance_province_name`      string              COMMENT    '省区名称',
`performance_city_code`          string              COMMENT    '城市组编码',
`performance_city_name`          string              COMMENT    '城市组名称',
`customer_id`                    string              COMMENT    '客户id',
`customer_code`                  string              COMMENT    '客户编码',
`customer_name`                  string              COMMENT    '客户名称',
`sales_user_id`                  string              COMMENT    '销售员id',
`sales_user_number`              string              COMMENT    '销售员工号',
`sales_user_name`                string              COMMENT    '销售员名称',
`business_type_code`             string              COMMENT    '业务类型编码',
`business_type_name`             string              COMMENT    '业务类型名称',
`service_user_id`                string              COMMENT    '服务管家ID',
`service_user_number`            string              COMMENT    '服务管家工号',
`service_user_name`              string              COMMENT    '服务管家名称',
`supervisor_user_id`             string              COMMENT    '销售主管id',
`supervisor_user_number`         string              COMMENT    '销售主管工号',
`supervisor_user_name`           string              COMMENT    '销售主管名称',
`sales_date`                     string              COMMENT    '销售日期',
`sale_amt`                       decimal(20,6)       COMMENT    '销售额',
`profit`                         decimal(20,6)       COMMENT    '定价毛利额',
`refund_sale_amt`                decimal(20,6)       COMMENT    '退货金额',
`year_month`                     string              COMMENT    '年月',
`create_by`                      string              COMMENT    '创建人',
`create_time`                    timestamp           COMMENT    '创建时间',
`update_time`                    timestamp           COMMENT    '更新时间',
`month`                     	 string              COMMENT    '年月'

) COMMENT '客户销售表'
STORED AS PARQUET;

*/	
