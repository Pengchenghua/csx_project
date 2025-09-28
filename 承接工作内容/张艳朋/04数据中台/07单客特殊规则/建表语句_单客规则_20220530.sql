--hive 客户提成宽表
drop table if exists csx_tmp.report_sss_r_m_crm_customer_special_rules;
create table csx_tmp.report_sss_r_m_crm_customer_special_rules(
`biz_id`                         string              COMMENT    '业务主键',
`region_code`                    string              COMMENT    '大区编码',
`region_name`                    string              COMMENT    '大区名称',
`province_code`                  string              COMMENT    '省份编码',
`province_name`                  string              COMMENT    '省份名称',
`city_group_code`                string              COMMENT    '城市组编码',
`city_group_name`                string              COMMENT    '城市组名称',
`customer_id`                    string              COMMENT    '客户id',
`customer_no`                    string              COMMENT    '客户编码',
`customer_name`                  string              COMMENT    '客户名称',
`sales_id`                       string              COMMENT    '销售员id',
`work_no`                        string              COMMENT    '销售员工号',
`sales_name`                     string              COMMENT    '销售员名称',
`rp_service_user_id`             string              COMMENT    '日配服务管家id',
`rp_service_user_work_no`        string              COMMENT    '日配服务管家工号',
`rp_service_user_name`           string              COMMENT    '日配服务管家名称',
`fl_service_user_id`             string              COMMENT    '福利服务管家id',
`fl_service_user_work_no`        string              COMMENT    '福利服务管家工号',
`fl_service_user_name`           string              COMMENT    '福利服务管家名称',
`bbc_service_user_id`            string              COMMENT    'BBC服务管家id',
`bbc_service_user_work_no`       string              COMMENT    'BBC服务管家工号',
`bbc_service_user_name`          string              COMMENT    'BBC服务管家名称',
`effective_period`               string              COMMENT    '时间期限',
`amount_value`                   decimal(20,6)       COMMENT    '调整金额',
`rp_amount_value`                decimal(20,6)       COMMENT    '调整金额_日配',
`fl_amount_value`                decimal(20,6)       COMMENT    '调整金额_福利',
`bbc_amount_value`               decimal(20,6)       COMMENT    '调整金额_BBC',
`rp_rate`                        decimal(20,6)       COMMENT    '调整比例_日配',
`fl_rate`                        decimal(20,6)       COMMENT    '调整比例_福利',
`bbc_rate`                       decimal(20,6)       COMMENT    '调整比例_BBC',
`service_fee`                    decimal(20,6)       COMMENT    '服务费金额',
`category`                       string              COMMENT    '类别',
`remark`                         string              COMMENT    '备注',
`qc_yearmonth`                   string              COMMENT    '签呈年月',
`create_by`                      string              COMMENT    '创建人',
`create_time`                    timestamp           COMMENT    '创建时间',
`update_time`                    timestamp           COMMENT    '更新时间'

) COMMENT '单客特殊规则表'
PARTITIONED BY (sdt string COMMENT '日期分区')
STORED AS TEXTFILE;
