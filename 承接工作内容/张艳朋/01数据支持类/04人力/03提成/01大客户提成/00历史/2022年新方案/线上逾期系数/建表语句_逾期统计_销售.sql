--hive 客户销售表
drop table if exists csx_tmp.report_sss_r_m_crm_customer_sale_detail;
create table csx_tmp.report_sss_r_m_crm_customer_sale_detail(
`biz_id`                         string              COMMENT    '业务主键',
`region_code`                    string              COMMENT    '大区编码',
`region_name`                    string              COMMENT    '大区名称',
`province_code`                  string              COMMENT    '省区编码',
`province_name`                  string              COMMENT    '省区名称',
`city_group_code`                string              COMMENT    '城市组编码',
`city_group_name`                string              COMMENT    '城市组名称',
`customer_id`                    string              COMMENT    '客户id',
`customer_no`                    string              COMMENT    '客户编码',
`customer_name`                  string              COMMENT    '客户名称',
`sales_id`                       string              COMMENT    '销售员id',
`work_no`                        string              COMMENT    '销售员工号',
`sales_name`                     string              COMMENT    '销售员名称',
`business_type_code`             string              COMMENT    '业务类型编码',
`business_type_name`             string              COMMENT    '业务类型名称',
`service_user_id`                string              COMMENT    '服务管家ID',
`service_user_work_no`           string              COMMENT    '服务管家工号',
`service_user_name`              string              COMMENT    '服务管家名称',
`first_supervisor_code`          string              COMMENT    '销售主管id',
`first_supervisor_work_no`       string              COMMENT    '销售主管工号',
`first_supervisor_name`          string              COMMENT    '销售主管名称',
`sales_date`                     string              COMMENT    '销售日期',
`sales_value`                    decimal(20,6)       COMMENT    '销售额',
`profit`                         decimal(20,6)       COMMENT    '定价毛利额',
`refund_sales_value`             decimal(20,6)       COMMENT    '退货金额',
`year_month`                     string              COMMENT    '年月',
`create_by`                      string              COMMENT    '创建人',
`create_time`                    timestamp           COMMENT    '创建时间',
`update_time`                    timestamp           COMMENT    '更新时间'

) COMMENT '客户销售表'
PARTITIONED BY (month string COMMENT '日期分区')
STORED AS TEXTFILE;


--mysql 客户销售表
CREATE TABLE `report_sss_r_m_crm_customer_sale_detail` (
`id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
`biz_id`                         varchar(64)          NOT NULL  COMMENT    '业务主键',
`region_code`                    varchar(16)          DEFAULT NULL COMMENT    '大区编码',
`region_name`                    varchar(16)          DEFAULT NULL COMMENT    '大区名称',
`province_code`                  varchar(16)          DEFAULT NULL COMMENT    '省区编码',
`province_name`                  varchar(16)          DEFAULT NULL COMMENT    '省区名称',
`city_group_code`                varchar(16)          DEFAULT NULL COMMENT    '城市组编码',
`city_group_name`                varchar(16)          DEFAULT NULL COMMENT    '城市组名称',
`customer_id`                    varchar(64)          DEFAULT NULL COMMENT    '客户id',
`customer_no`                    varchar(64)          DEFAULT NULL COMMENT    '客户编码',
`customer_name`                  varchar(64)          DEFAULT NULL COMMENT    '客户名称',
`sales_id`                       varchar(64)          DEFAULT NULL COMMENT    '销售员id',
`work_no`                        varchar(64)          DEFAULT NULL COMMENT    '销售员工号',
`sales_name`                     varchar(64)          DEFAULT NULL COMMENT    '销售员名称',
`business_type_code`             varchar(64)          DEFAULT NULL COMMENT    '业务类型编码',
`business_type_name`             varchar(64)          DEFAULT NULL COMMENT    '业务类型名称',
`service_user_id`                varchar(64)          DEFAULT NULL COMMENT    '服务管家ID',
`service_user_work_no`           varchar(64)          DEFAULT NULL COMMENT    '服务管家工号',
`service_user_name`              varchar(64)          DEFAULT NULL COMMENT    '服务管家名称',
`first_supervisor_code`          varchar(64)          DEFAULT NULL COMMENT    '销售主管id',
`first_supervisor_work_no`       varchar(64)          DEFAULT NULL COMMENT    '销售主管工号',
`first_supervisor_name`          varchar(64)          DEFAULT NULL COMMENT    '销售主管名称',
`sales_date`                     varchar(16)          DEFAULT NULL COMMENT    '销售日期',
`sales_value`                    decimal(20,6)        DEFAULT NULL COMMENT    '销售额',
`profit`                         decimal(20,6)        DEFAULT NULL COMMENT    '定价毛利额',
`refund_sales_value`             decimal(20,6)        DEFAULT NULL COMMENT    '退货金额',
`year_month`                     varchar(16)          DEFAULT NULL COMMENT    '年月',
`create_by`                      varchar(64)          DEFAULT NULL COMMENT    '创建人',
`create_time`                    datetime             DEFAULT NULL COMMENT    '创建时间',
`update_time`                    datetime             DEFAULT NULL COMMENT    '更新时间',
`month`                          varchar(16)          DEFAULT NULL COMMENT    '统计月份',

  PRIMARY KEY (`id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='客户销售表';




