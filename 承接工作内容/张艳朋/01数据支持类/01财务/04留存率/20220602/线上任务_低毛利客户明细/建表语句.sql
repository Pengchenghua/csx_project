
create table csx_analyse.csx_analyse_fr_finance_low_profit_customer_detail_wf(
`performance_province_name`             string              COMMENT    '省份',
`performance_city_name`                 string              COMMENT    '城市',
`new_smonth`                            string              COMMENT    '首月',
`smonth`                                string              COMMENT    '销售月',
`customer_code`                         string              COMMENT    '客户编号',
`customer_name`                         string              COMMENT    '客户名称',
`first_category_name`                   string              COMMENT    '一级分类',
`second_category_name`                  string              COMMENT    '二级分类',
`third_category_name`                   string              COMMENT    '三级分类',
`classify_large_name`                   string              COMMENT    '管理大级',
`classify_middle_name`                  string              COMMENT    '管理中类',
`classify_small_name`                   string              COMMENT    '管理小类',
`sales_user_number`                     string              COMMENT    '销售员工号',
`sales_user_name`                       string              COMMENT    '销售员名称',
`sale_amt`                              decimal(26,6)       COMMENT    '销售额',
`profit`                                decimal(26,6)       COMMENT    '毛利额',
`profit_rate`                           decimal(26,6)       COMMENT    '毛利率',
`sale_amt_no_tax`                       decimal(26,6)       COMMENT    '不含税销售额',
`profit_no_tax`                         decimal(26,6)       COMMENT    '不含税毛利额',
`profit_no_tax_rate`                    decimal(26,6)       COMMENT    '不含税毛利率',
`months_cnt`                            int                 COMMENT    '履约月数'
) COMMENT '低毛利客户明细表'
STORED AS PARQUET;
