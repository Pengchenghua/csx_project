--提成信息表
drop table if exists csx_dw.report_sss_r_m_crm_sales_customer_commission_new;
create table csx_dw.report_sss_r_m_crm_sales_customer_commission_new(
`biz_id`                         string              COMMENT    '业务主键',
`customer_no`                    string              COMMENT    '客户编码',
`customer_name`                  string              COMMENT    '客户名称',
`sales_id`                       string              COMMENT    '销售员id',
`work_no`                        string              COMMENT    '销售员工号',
`sales_name`                     string              COMMENT    '销售员名称',
`sales_value`                    decimal(20,6)       COMMENT    '销售额',
`ripei_bbc_sales_value`          decimal(20,6)       COMMENT    '日配&BBC销售额',
`fuli_sales_value`               decimal(20,6)       COMMENT    '福利销售额',
`sales_value_commion`            decimal(20,6)       COMMENT    '销售额提成',
`ripei_bbc_sales_value_commion`  decimal(20,6)       COMMENT    '日配&BBC销售额提成',
`fuli_sales_value_commion`       decimal(20,6)       COMMENT    '福利销售额提成',
`profit`                         decimal(20,6)       COMMENT    '定价毛利额',
`ripei_bbc_profit`               decimal(20,6)       COMMENT    '日配&BBC定价毛利额',
`fuli_profit`                    decimal(20,6)       COMMENT    '福利定价毛利额',
`profit_commion`                 decimal(20,6)       COMMENT    '定价毛利额提成',
`ripei_bbc_profit_commion`       decimal(20,6)       COMMENT    '日配&BBC定价毛利额提成',
`fuli_profit_commion`            decimal(20,6)       COMMENT    '福利定价毛利额提成',
`prorate`                        decimal(20,6)       COMMENT    '定价毛利率',
`ripei_bbc_prorate`              decimal(20,6)       COMMENT    '日配&BBC定价毛利率',
`fuli_prorate`                   decimal(20,6)       COMMENT    '福利定价毛利率',
`commion_total`                  decimal(20,6)       COMMENT    '提成总计',
`commion_ripei_bbc_total`        decimal(20,6)       COMMENT    '日配&BBC提成总计',
`commion_fuli_total`             decimal(20,6)       COMMENT    '福利提成总计',
`overdue_rate`                   decimal(20,6)       COMMENT    '逾期系数',
`refund_sales_value`             decimal(20,6)       COMMENT    '退货金额',
`ripei_bbc_refund_sales_value`   decimal(20,6)       COMMENT    '日配&BBC退货金额',
`fuli_refund_sales_value`        decimal(20,6)       COMMENT    '福利退货金额',
`sdt`                            string              COMMENT    '同步时间',
`updated_time`                   string              COMMENT    '更新时间'

) COMMENT '提成信息表'
PARTITIONED BY (smonth string COMMENT '按年月分区')
STORED AS TEXTFILE;


--提成趋势表
drop table if exists csx_dw.report_sss_r_d_crm_sales_commission_trend_new;
create table csx_dw.report_sss_r_d_crm_sales_commission_trend_new(
`biz_id`                         string              COMMENT    '业务主键',
`sales_id`                       string              COMMENT    '销售员id',
`work_no`                        string              COMMENT    '销售员工号',
`sales_name`                     string              COMMENT    '销售员名称',
`sales_date`                     string              COMMENT    '销售日期',
`sales_value_commion`            decimal(20,6)       COMMENT    '销售额提成',
`ripei_bbc_sales_value_commion`  decimal(20,6)       COMMENT    '日配&BBC销售额提成',
`fuli_sales_value_commion`       decimal(20,6)       COMMENT    '福利销售额提成',
`profit_commion`                 decimal(20,6)       COMMENT    '定价毛利额提成',
`ripei_bbc_profit_commion`       decimal(20,6)       COMMENT    '日配&BBC定价毛利额提成',
`fuli_profit_commion`            decimal(20,6)       COMMENT    '福利定价毛利额提成',
`commion_total`                  decimal(20,6)       COMMENT    '提成总计',
`commion_ripei_bbc_total`        decimal(20,6)       COMMENT    '日配&BBC提成总计',
`commion_fuli_total`             decimal(20,6)       COMMENT    '福利提成总计',
`smonth`                         string              COMMENT    '年月',
`updated_time`                   string              COMMENT    '更新时间'

) COMMENT '提成趋势表'
PARTITIONED BY (sdt string COMMENT '提成日期分区')
STORED AS TEXTFILE;


--销售趋势表
drop table if exists csx_dw.report_sss_r_d_crm_sales_sale_trend_new;
create table csx_dw.report_sss_r_d_crm_sales_sale_trend_new(
`biz_id`                         string              COMMENT    '业务主键',
`sales_id`                       string              COMMENT    '销售员id',
`work_no`                        string              COMMENT    '销售员工号',
`sales_name`                     string              COMMENT    '销售员名称',
`sales_date`                     string              COMMENT    '销售日期',
`sales_value`                    decimal(20,6)       COMMENT    '销售额',
`ripei_bbc_sales_value`          decimal(20,6)       COMMENT    '日配&BBC销售额',
`fuli_sales_value`               decimal(20,6)       COMMENT    '福利销售额',
`profit`                         decimal(20,6)       COMMENT    '定价毛利额',
`ripei_bbc_profit`               decimal(20,6)       COMMENT    '日配&BBC定价毛利额',
`fuli_profit`                    decimal(20,6)       COMMENT    '福利定价毛利额',
`refund_sales_value`             decimal(20,6)       COMMENT    '退货金额',
`ripei_bbc_refund_sales_value`   decimal(20,6)       COMMENT    '日配&BBC退货金额',
`fuli_refund_sales_value`        decimal(20,6)       COMMENT    '福利退货金额',
`smonth`                         string              COMMENT    '年月',
`updated_time`                   string              COMMENT    '更新时间'

) COMMENT '销售趋势表'
PARTITIONED BY (sdt string COMMENT '销售日期分区')
STORED AS TEXTFILE;
