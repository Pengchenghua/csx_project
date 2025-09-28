drop table if exists csx_tmp.csx_tmp_report_sss_incidental_write_off_detail_di;
create table csx_tmp.csx_tmp_report_sss_incidental_write_off_detail_di(
`biz_id`                         string              COMMENT    '业务主键',
`region_code`                    string              COMMENT    '大区编码',
`region_name`                    string              COMMENT    '大区名称',
`province_code`                  string              COMMENT    '省份编码',
`province_name`                  string              COMMENT    '省份名称',
`city_group_code`                string              COMMENT    '城市组编码',
`city_group_name`                string              COMMENT    '城市组名称',
`incidental_expenses_no`         string              COMMENT    '杂项用款单号',
`receiving_customer_code`        string              COMMENT    '收款客户编码',
`receiving_customer_name`        string              COMMENT    '收款客户名称',
`first_category_code`            string              COMMENT    '一级客户分类编码',
`first_category_name`            string              COMMENT    '一级客户分类名称',
`second_category_code`           string              COMMENT    '二级客户分类编码',
`second_category_name`           string              COMMENT    '二级客户分类名称',
`third_category_code`            string              COMMENT    '三级客户分类编码',
`third_category_name`            string              COMMENT    '三级客户分类名称',
`custom_category`                string              COMMENT    '自定义分类名称',
`business_scene`                 string              COMMENT    '业务场景名称',
`business_scene_code`            string              COMMENT    '业务场景代码 1:投标保证金  2:履约保证金 3:投标转履约',
`payment_unit_name`              string              COMMENT    '签约主体',
`payment_company_code`           string              COMMENT    '实际付款公司编码',
`payment_company_name`           string              COMMENT    '实际付款公司名称',
`approved_date`                  string              COMMENT    '单据审批通过日期',
`break_contract_date`            string              COMMENT    '断约时间',
`target_payment_time`            string              COMMENT    '目标回款时间',
`account_diff`                   string              COMMENT    '账期天数 断约时间为空取单据审核通过时间',
`account_type`                   string              COMMENT    '账期类型 断约时间为空取单据审核通过时间',
`account_diff2`                  string              COMMENT    '账期天数 断约时间为空不统计',
`account_type2`                  string              COMMENT    '账期类型 断约时间为空不统计',
`payment_amount`                 decimal(20,6)       COMMENT    '付款金额',
`write_off_amount`               decimal(20,6)       COMMENT    '核销金额',
`money_back_no_write_off_amount` decimal(20,6)       COMMENT    '已回款未核销金额',
`lave_write_off_amount`          decimal(20,6)       COMMENT    '剩余待核销金额',
`lave_write_off_amount_90`       decimal(20,6)       COMMENT    '90天以上未回款金额',
`lave_write_off_amount_365`      decimal(20,6)       COMMENT    '1年以上未回款金额',
`create_by`                      string              COMMENT    '创建人',
`create_time`                    timestamp           COMMENT    '创建时间'
) COMMENT '保证金看板明细'
PARTITIONED BY (sdt string COMMENT '日期分区-审批通过日期')
STORED AS TEXTFILE;




--自定义客户分类
--drop table if exists csx_tmp.tmp_crm_customer_custom_category;
--create table csx_tmp.tmp_crm_customer_custom_category(
--`first_category_code`            string              COMMENT    '一级客户分类编码',
--`first_category_name`            string              COMMENT    '一级客户分类名称',
--`second_category_code`           string              COMMENT    '二级客户分类编码',
--`second_category_name`           string              COMMENT    '二级客户分类名称',
--`custom_category`                string              COMMENT    '自定义客户分类'
--
--) COMMENT '自定义客户分类表'
--row format delimited fields terminated by ','
--stored as textfile;
--
----补充收入组并校验
--load data inpath '/tmp/zhangyanpeng/customer_custom_category.csv' overwrite into table csx_tmp.tmp_crm_customer_custom_category;
--select * from csx_tmp.tmp_crm_customer_custom_category;



--1、客户活跃状态：取最新状态？
--2、超60天以上账期率
--3、收款客户编码/杂项用款单号
--4、"客户明细" 履约取客户明细，同一个客户会对应多个杂项用款单号 需要聚合？