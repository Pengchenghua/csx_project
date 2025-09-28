-- hive
create table if not exists `csx_analyse_fr_crm_customer_special_rules_mi` (
`biz_id`                         string              COMMENT    '业务主键',
`region_code`                    string              COMMENT    '大区编码',
`region_name`                    string              COMMENT    '大区名称',
`province_code`                  string              COMMENT    '省份编码',
`province_name`                  string              COMMENT    '省份名称',
`city_group_code`                string              COMMENT    '城市组编码',
`city_group_name`                string              COMMENT    '城市组名称',
`customer_id`                    string              COMMENT    '客户id',
`customer_code`                  string              COMMENT    '客户编码',
`customer_name`                  string              COMMENT    '客户名称',
`sales_user_id`                  string              COMMENT    '销售员id',
`sales_user_number`              string              COMMENT    '销售员工号',
`sales_user_name`                string              COMMENT    '销售员名称',
`rp_service_user_id`             string              COMMENT    '日配服务管家id',
`rp_service_user_number`         string              COMMENT    '日配服务管家工号',
`rp_service_user_name`           string              COMMENT    '日配服务管家名称',
`fl_service_user_id`             string              COMMENT    '福利服务管家id',
`fl_service_user_number`         string              COMMENT    '福利服务管家工号',
`fl_service_user_name`           string              COMMENT    '福利服务管家名称',
`bbc_service_user_id`            string              COMMENT    'BBC服务管家id',
`bbc_service_user_number`        string              COMMENT    'BBC服务管家工号',
`bbc_service_user_name`          string              COMMENT    'BBC服务管家名称',
`effective_period`               string              COMMENT    '时间期限',
`amount_value`                   string              COMMENT    '调整金额',
`rp_amount_value`                string              COMMENT    '调整金额_日配',
`fl_amount_value`                string              COMMENT    '调整金额_福利',
`bbc_amount_value`               string              COMMENT    '调整金额_BBC',
`rp_rate`                        string              COMMENT    '调整比例_日配',
`fl_rate`                        string              COMMENT    '调整比例_福利',
`bbc_rate`                       string              COMMENT    '调整比例_BBC',
`service_fee`                    string              COMMENT    '服务费金额',
`category`                       string              COMMENT    '类别',
`remark`                         string              COMMENT    '备注',
`qc_yearmonth`                   string              COMMENT    '签呈年月',
`sdt_date`                       string              COMMENT    '分区年月'

) comment '大客户提成-单客特殊规则'
partitioned by (sdt string comment '日期分区')
STORED AS TEXTFILE;

-- mysql
CREATE TABLE `crm_customer_special_rules` (
`id` 							 bigint(20)     	  NOT NULL AUTO_INCREMENT COMMENT '主键',
`biz_id`                         varchar(64)          NOT NULL  COMMENT    '业务主键',
`region_code`                    varchar(64)          DEFAULT NULL  COMMENT    '大区编码',
`region_name`                    varchar(64)          DEFAULT NULL  COMMENT    '大区名称',
`province_code`                  varchar(64)          DEFAULT NULL  COMMENT    '省份编码',
`province_name`                  varchar(64)          DEFAULT NULL  COMMENT    '省份名称',
`city_group_code`                varchar(64)          DEFAULT NULL  COMMENT    '城市组编码',
`city_group_name`                varchar(64)          DEFAULT NULL  COMMENT    '城市组名称',
`customer_id`                    varchar(64)          DEFAULT NULL  COMMENT    '客户id',
`customer_code`                  varchar(64)          DEFAULT NULL  COMMENT    '客户编码',
`customer_name`                  varchar(64)          DEFAULT NULL  COMMENT    '客户名称',
`sales_user_id`                  varchar(64)          DEFAULT NULL  COMMENT    '销售员id',
`sales_user_number`              varchar(64)          DEFAULT NULL  COMMENT    '销售员工号',
`sales_user_name`                varchar(64)          DEFAULT NULL  COMMENT    '销售员名称',
`rp_service_user_id`             varchar(64)          DEFAULT NULL  COMMENT    '日配服务管家id',
`rp_service_user_number`         varchar(64)          DEFAULT NULL  COMMENT    '日配服务管家工号',
`rp_service_user_name`           varchar(64)          DEFAULT NULL  COMMENT    '日配服务管家名称',
`fl_service_user_id`             varchar(64)          DEFAULT NULL  COMMENT    '福利服务管家id',
`fl_service_user_number`         varchar(64)          DEFAULT NULL  COMMENT    '福利服务管家工号',
`fl_service_user_name`           varchar(64)          DEFAULT NULL  COMMENT    '福利服务管家名称',
`bbc_service_user_id`            varchar(64)          DEFAULT NULL  COMMENT    'BBC服务管家id',
`bbc_service_user_number`        varchar(64)          DEFAULT NULL  COMMENT    'BBC服务管家工号',
`bbc_service_user_name`          varchar(64)          DEFAULT NULL  COMMENT    'BBC服务管家名称',
`effective_period`               varchar(64)          DEFAULT NULL  COMMENT    '时间期限',
`amount_value`                   decimal(20,6)        DEFAULT NULL  COMMENT    '调整金额',
`rp_amount_value`                decimal(20,6)        DEFAULT NULL  COMMENT    '调整金额_日配',
`fl_amount_value`                decimal(20,6)        DEFAULT NULL  COMMENT    '调整金额_福利',
`bbc_amount_value`               decimal(20,6)        DEFAULT NULL  COMMENT    '调整金额_BBC',
`rp_rate`                        decimal(20,6)        DEFAULT NULL  COMMENT    '调整比例_日配',
`fl_rate`                        decimal(20,6)        DEFAULT NULL  COMMENT    '调整比例_福利',
`bbc_rate`                       decimal(20,6)        DEFAULT NULL  COMMENT    '调整比例_BBC',
`service_fee`                    decimal(20,6)        DEFAULT NULL  COMMENT    '服务费金额',
`category`                       varchar(64)          DEFAULT NULL  COMMENT    '类别',
`remark`                         varchar(64)          DEFAULT NULL  COMMENT    '备注',
`qc_yearmonth`                   varchar(64)          DEFAULT NULL  COMMENT    '签呈年月',
`sdt_date`                       varchar(64)          DEFAULT NULL  COMMENT    '分区年月',

  PRIMARY KEY (`id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='大客户提成-单客特殊规则';

insert overwrite directory '/tmp/zhangyanpeng/20220827_01' row format delimited fields terminated by '\t'

select * from csx_tmp.report_sss_r_m_crm_customer_special_rules;

select count(*) from crm_customer_special_rules

