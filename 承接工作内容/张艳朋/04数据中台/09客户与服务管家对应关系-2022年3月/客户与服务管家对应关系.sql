-- hive
create table csx_analyse.csx_analyse_report_crm_customer_service_manager_info_202203_yf(
`province_name`                  string              COMMENT    '省区名称',
`customer_id`                    string              COMMENT    '客户id',
`customer_no`                    string              COMMENT    '客户编码',
`customer_name`                  string              COMMENT    '客户名称',
`work_no`                        string              COMMENT    '销售员工号',
`sales_name`                     string              COMMENT    '销售员名称',
`rp_service_user_work_no`        string              COMMENT    '日配服务管家工号',
`rp_service_user_name`           string              COMMENT    '日配服务管家名称',
`fl_service_user_work_no`        string              COMMENT    '福利服务管家工号',
`fl_service_user_name`           string              COMMENT    '福利服务管家名称',
`bbc_service_user_work_no`       string              COMMENT    'BBC服务管家工号',
`bbc_service_user_name`          string              COMMENT    'BBC服务管家名称',
`rp_sales_sale_fp_rate`          string              COMMENT    '日配销售员_销售额分配比例',
`rp_sales_profit_fp_rate`        string              COMMENT    '日配销售员_毛利分配比例',
`fl_sales_sale_fp_rate`          string              COMMENT    '福利销售员_销售额分配比例',
`fl_sales_profit_fp_rate`        string              COMMENT    '福利销售员_毛利分配比例',
`bbc_sales_sale_fp_rate`         string              COMMENT    'BBC销售员_销售额分配比例',
`bbc_sales_profit_fp_rate`       string              COMMENT    'BBC销售员_毛利分配比例',
`rp_service_user_sale_fp_rate`   string              COMMENT    '日配服务管家_销售额分配比例',
`rp_service_user_profit_fp_rate` string              COMMENT    '日配服务管家_毛利分配比例',
`fl_service_user_sale_fp_rate`   string              COMMENT    '福利服务管家_销售额分配比例',
`fl_service_user_profit_fp_rate` string              COMMENT    '福利服务管家_毛利分配比例',
`bbc_service_user_sale_fp_rate`  string              COMMENT    'BBC服务管家_销售额分配比例',
`bbc_service_user_profit_fp_rate`string              COMMENT    'BBC服务管家_毛利分配比例',
`is_sale`                        string              COMMENT    '是否有销售',
`is_overdue`                     string              COMMENT    '是否有逾期'

) COMMENT '客户与服务管家对应关系-2022年3月'
STORED AS TEXTFILE;


-- mysql
CREATE TABLE `crm_customer_service_manager_info_202203` (
`id` 							 bigint(20)     	  NOT NULL AUTO_INCREMENT COMMENT '主键',
`province_name`                  varchar(32)          DEFAULT NULL  COMMENT    '省区名称',
`customer_id`                    varchar(64)          DEFAULT NULL  COMMENT    '客户id',
`customer_no`                    varchar(64)          DEFAULT NULL  COMMENT    '客户编码',
`customer_name`                  varchar(64)          DEFAULT NULL  COMMENT    '客户名称',
`work_no`                        varchar(64)          DEFAULT NULL  COMMENT    '销售员工号',
`sales_name`                     varchar(64)          DEFAULT NULL  COMMENT    '销售员名称',
`rp_service_user_work_no`        varchar(64)          DEFAULT NULL  COMMENT    '日配服务管家工号',
`rp_service_user_name`           varchar(64)          DEFAULT NULL  COMMENT    '日配服务管家名称',
`fl_service_user_work_no`        varchar(64)          DEFAULT NULL  COMMENT    '福利服务管家工号',
`fl_service_user_name`           varchar(64)          DEFAULT NULL  COMMENT    '福利服务管家名称',
`bbc_service_user_work_no`       varchar(64)          DEFAULT NULL  COMMENT    'BBC服务管家工号',
`bbc_service_user_name`          varchar(64)          DEFAULT NULL  COMMENT    'BBC服务管家名称',
`rp_sales_sale_fp_rate`          decimal(20,6)        DEFAULT NULL  COMMENT    '日配销售员_销售额分配比例',
`rp_sales_profit_fp_rate`        decimal(20,6)        DEFAULT NULL  COMMENT    '日配销售员_毛利分配比例',
`fl_sales_sale_fp_rate`          decimal(20,6)        DEFAULT NULL  COMMENT    '福利销售员_销售额分配比例',
`fl_sales_profit_fp_rate`        decimal(20,6)        DEFAULT NULL  COMMENT    '福利销售员_毛利分配比例',
`bbc_sales_sale_fp_rate`         decimal(20,6)        DEFAULT NULL  COMMENT    'BBC销售员_销售额分配比例',
`bbc_sales_profit_fp_rate`       decimal(20,6)        DEFAULT NULL  COMMENT    'BBC销售员_毛利分配比例',
`rp_service_user_sale_fp_rate`   decimal(20,6)        DEFAULT NULL  COMMENT    '日配服务管家_销售额分配比例',
`rp_service_user_profit_fp_rate` decimal(20,6)        DEFAULT NULL  COMMENT    '日配服务管家_毛利分配比例',
`fl_service_user_sale_fp_rate`   decimal(20,6)        DEFAULT NULL  COMMENT    '福利服务管家_销售额分配比例',
`fl_service_user_profit_fp_rate` decimal(20,6)        DEFAULT NULL  COMMENT    '福利服务管家_毛利分配比例',
`bbc_service_user_sale_fp_rate`  decimal(20,6)        DEFAULT NULL  COMMENT    'BBC服务管家_销售额分配比例',
`bbc_service_user_profit_fp_rate`decimal(20,6)        DEFAULT NULL  COMMENT    'BBC服务管家_毛利分配比例',
`is_sale`                        varchar(64)          DEFAULT NULL  COMMENT    '是否有销售',
`is_overdue`                     varchar(64)          DEFAULT NULL  COMMENT    '是否有逾期',

  PRIMARY KEY (`id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='客户与服务管家对应关系-2022年3月';