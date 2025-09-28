-- hive
create table csx_analyse.csx_analyse_report_crm_customer_custom_category_yf(
`first_category_code`            string              COMMENT    '一级客户分类编码',
`first_category_name`            string              COMMENT    '一级客户分类名称',
`second_category_code`           string              COMMENT    '二级客户分类编码',
`second_category_name`           string              COMMENT    '二级客户分类名称',
`custom_category`                string              COMMENT    '自定义客户分类'

) COMMENT '自定义客户分类'
STORED AS TEXTFILE;


-- mysql
CREATE TABLE `crm_customer_custom_category` (
`id` 							 bigint(20)     	  NOT NULL AUTO_INCREMENT COMMENT '主键',
`first_category_code`            varchar(64)          NOT NULL  COMMENT    '一级客户分类编码',
`first_category_name`            varchar(64)          NOT NULL  COMMENT    '一级客户分类名称',
`second_category_code`           varchar(64)          NOT NULL  COMMENT    '二级客户分类编码',
`second_category_name`           varchar(64)          NOT NULL  COMMENT    '二级客户分类名称',
`custom_category`                varchar(64)          NOT NULL  COMMENT    '自定义客户分类',

  PRIMARY KEY (`id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='自定义客户分类';



CREATE TABLE `tmp_qyg_sale_data_20221201` (
`id` 							 bigint(20)     	  NOT NULL AUTO_INCREMENT COMMENT '主键',
`smonth`            varchar(64)          NOT NULL  COMMENT    '一级客户分类编码',
`sale_amt`            decimal(26,6)          NOT NULL  COMMENT    '一级客户分类名称',
`order_cnt`            decimal(26,6)          NOT NULL  COMMENT    '一级客户分类名称',
`sale_rate`            decimal(26,6)         NOT NULL  COMMENT    '二级客户分类编码',
`order_rate`           decimal(26,6)        NOT NULL  COMMENT    '二级客户分类名称',

  PRIMARY KEY (`id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='临时数据';