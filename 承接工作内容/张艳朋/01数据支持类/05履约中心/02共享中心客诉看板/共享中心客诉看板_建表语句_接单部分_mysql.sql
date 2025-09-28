-- mysql
CREATE TABLE `csx_analyse_report_oms_bulletin_board_order_di` (
`id` 							 bigint(20)     	  NOT NULL AUTO_INCREMENT COMMENT '主键',
`region_code`                    varchar(32)         DEFAULT NULL  COMMENT    '大区编码',
`region_name`                    varchar(32)         DEFAULT NULL  COMMENT    '大区名称',
`province_code`                  varchar(32)         DEFAULT NULL  COMMENT    '省区编码',
`province_name`                  varchar(32)         DEFAULT NULL  COMMENT    '省区名称',
`city_group_code`                varchar(32)         DEFAULT NULL  COMMENT    '城市组编码',
`city_group_name`                varchar(32)         DEFAULT NULL  COMMENT    '城市组名称',
`customer_no`                    varchar(64)         DEFAULT NULL  COMMENT    '客户编码',
`customer_name`                  varchar(128)        DEFAULT NULL  COMMENT    '客户名称',
`child_customer_code`            varchar(32)         DEFAULT NULL  COMMENT    '子客户编码',
`child_customer_name`            varchar(128)        DEFAULT NULL  COMMENT    '子客户名称',
`created_user_id`                varchar(32)         DEFAULT NULL  COMMENT    '下单用户id',
`created_user_name`              varchar(128)        DEFAULT NULL  COMMENT    '下单用户名称',
`recep_user_number`              varchar(32)         DEFAULT NULL  COMMENT    '接单人工号',
`recep_order_by`                 varchar(32)         DEFAULT NULL  COMMENT    '接单人',
`cost_center_code`               varchar(32)         DEFAULT NULL  COMMENT    '接单人成本中心编码',
`cost_center_name`               varchar(128)        DEFAULT NULL  COMMENT    '接单人成本中心名称',
`employee_org_code`              varchar(32)         DEFAULT NULL  COMMENT    '接单人员组织编码',
`employee_org_name`              varchar(128)        DEFAULT NULL  COMMENT    '接单人员组织名称',
`recep_order_date`               varchar(32)         DEFAULT NULL  COMMENT    '接单日期',
`order_category_desc`            varchar(32)         DEFAULT NULL  COMMENT    '订单类型',
`order_category_name`            varchar(32)         DEFAULT NULL  COMMENT    '订单类型名称',
`order_date`                     varchar(32)         DEFAULT NULL  COMMENT    '下单日期',
`order_cnt`                      bigint(20)          DEFAULT NULL  COMMENT    '订单数',
`goods_cnt`                      bigint(20)          DEFAULT NULL  COMMENT    'SKU数',

  PRIMARY KEY (`id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='共享中心客诉看板-接单部分';





