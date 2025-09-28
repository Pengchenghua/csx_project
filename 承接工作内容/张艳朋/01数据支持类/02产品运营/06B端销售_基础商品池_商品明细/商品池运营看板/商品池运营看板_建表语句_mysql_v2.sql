-- 商品池运营看板-商品池线上化程度-商品池
CREATE TABLE `csx_analyse_report_yszx_dc_product_pool_df` (
`id`                             bigint(20)          NOT NULL AUTO_INCREMENT COMMENT    '主键id',
`biz_id`                         bigint(20)          NOT NULL DEFAULT '0' COMMENT    '业务主键id',
`inventory_dc_code`              varchar(32)          NOT NULL DEFAULT '' COMMENT    'dc编码',
`product_code`                   varchar(32)          NOT NULL DEFAULT '' COMMENT    '商品编码',
`product_name`                   varchar(32)          NOT NULL DEFAULT '' COMMENT    '商品名称',
`sync_customer_product_flag`     int                 NOT NULL DEFAULT '0' COMMENT    '是否同步客户商品池 0-否 1-是',
`base_product_tag`               int                 NOT NULL DEFAULT '1' COMMENT    '基础商品标签 0-否 1-是',
`base_product_status`            int                 NOT NULL DEFAULT '0' COMMENT    '主数据商品状态：0-正常 3-停售 6-退场 7-停购',
`created_by`                     varchar(32)          NOT NULL DEFAULT '' COMMENT    '创建者',
`updated_by`                     varchar(32)          NOT NULL DEFAULT '' COMMENT    '更新者',
`update_time`                    datetime            NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT    '更新时间',
`create_time`                    datetime            NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT    '创建时间',
`classify_large_code`            varchar(32)          NOT NULL DEFAULT '' COMMENT    '管理大类编号',
`classify_large_name`            varchar(32)          NOT NULL DEFAULT '' COMMENT    '管理大类名称',
`classify_middle_code`           varchar(32)          NOT NULL DEFAULT '' COMMENT    '管理中类编号',
`classify_middle_name`           varchar(32)          NOT NULL DEFAULT '' COMMENT    '管理中类名称',
`classify_small_code`            varchar(32)          NOT NULL DEFAULT '' COMMENT    '管理小类编号',
`classify_small_name`            varchar(32)          NOT NULL DEFAULT '' COMMENT    '管理小类名称',
`business_division_code`         varchar(32)          NOT NULL DEFAULT '' COMMENT    '业务部编码(11.生鲜 12.食百)',
`business_division_name`         varchar(32)          NOT NULL DEFAULT '' COMMENT    '业务部名称(11.生鲜 12.食百)',
`purchase_price`                 decimal(26,6)       NOT NULL DEFAULT '0' COMMENT    '采购报价',
`suggest_price_mid`              decimal(26,6)       NOT NULL DEFAULT '0' COMMENT    '建议售价-中',
`sdt_date`                       varchar(16)          NOT NULL DEFAULT '' COMMENT    '日期分区',

  PRIMARY KEY (`id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='库存地点商品关系';

alter table  csx_analyse_report_yszx_dc_product_pool_df  add index `idx_union` (`sdt_date`,`base_product_tag`,`base_product_status`,`inventory_dc_code`,`product_code`);


-- 商品池运营看板-商品池线上化程度-客户商品池规则
CREATE TABLE `csx_analyse_report_yszx_customer_dc_rule_df` (
`id`                             bigint(20)          NOT NULL AUTO_INCREMENT COMMENT    '主键id',
`biz_id`                         bigint(20)          NOT NULL DEFAULT '0' COMMENT    '业务主键id',
`inventory_dc_code`              varchar(32)          NOT NULL DEFAULT '' COMMENT    'dc编码',
`customer_code`                  varchar(32)          NOT NULL DEFAULT '' COMMENT    '客户编码',
`customer_name`                  varchar(32)          NOT NULL DEFAULT '' COMMENT    '客户名称',
`customer_flag`                  varchar(32)          NOT NULL DEFAULT '' COMMENT    '客户标识',
`bind_common_product_flag`       int                 DEFAULT NULL  COMMENT    '绑定基础商品池 0-否 1-是',
`create_order_auto_add_flag`     int                 DEFAULT NULL  COMMENT    '下单自动添加标识 0-未启动 1-已启动',
`price_auto_add_flag`            int                 DEFAULT NULL  COMMENT    '报价自动添加标识 0-未启动 1-已启动',
`remove_customer_product_flag`   int                 DEFAULT NULL  COMMENT    '自动移除商品池 0-关闭 1-开启',
`must_sale_auto_add_flag`        int                 DEFAULT NULL  COMMENT    '必售商品自动添加标识 0-未启动 1-已启动',
`lock_customer_product_flag`     int                 DEFAULT NULL  COMMENT    '锁定小程序商品池 0-未锁定 1-已锁定',
`update_by`                      varchar(32)          NOT NULL DEFAULT '' COMMENT    '更新者',
`update_time`                    datetime            NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT    '更新时间',
`create_by`                      varchar(32)          NOT NULL DEFAULT '' COMMENT    '创建者',
`create_time`                    datetime            NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT    '创建时间',
`lock_mall_product_flag`         int                 DEFAULT NULL  COMMENT    '锁定中台商品池 0-未锁定 1-已锁定',
`filter_zs_flag`                 int                 DEFAULT NULL  COMMENT    '过滤直送单标识 0-否，1-是',
`filter_patch_flag`              int                 DEFAULT NULL  COMMENT    '过滤补单标识 0-否，1-是',
`sdt_date`                       varchar(16)          NOT NULL DEFAULT '' COMMENT    '日期分区',

  PRIMARY KEY (`id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='客户商品池规则';

alter table   csx_analyse_report_yszx_customer_dc_rule_df   add index  `idx_union` (`sdt_date`,`inventory_dc_code`,`customer_flag`,`customer_code`);


-- 商品池运营看板-商品池线上化程度-商品池移除规则
CREATE TABLE `csx_analyse_report_yszx_customer_product_remove_rule_df` (
`id`                             bigint(20)          NOT NULL AUTO_INCREMENT COMMENT    '主键id',
`biz_id`                         bigint(20)          NOT NULL DEFAULT '0' COMMENT    '业务主键id',
`inventory_dc_code`              varchar(32)          NOT NULL DEFAULT '' COMMENT    'dc编码',
`big_category_code`              varchar(32)          NOT NULL DEFAULT '' COMMENT    '大类编码',
`mid_category_code`              varchar(32)          NOT NULL DEFAULT '' COMMENT    '中类编码',
`small_category_code`            varchar(32)          NOT NULL DEFAULT '' COMMENT    '小类编码',
`category_type`                  int                 DEFAULT NULL  COMMENT    '类型（1.小类 2.中类 3.大类）',
`category_name`                  varchar(64)          NOT NULL DEFAULT '' COMMENT    '类名称',
`remove_date`                    int                 DEFAULT NULL  COMMENT    '移除期限（天数）',
`status`                         int                 DEFAULT NULL  COMMENT    '规则状态： 0-禁用 1-启用',
`create_time`                    datetime            NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT    '创建时间',
`create_by`                      varchar(32)          NOT NULL DEFAULT '' COMMENT    '创建者',
`update_time`                    datetime            NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT    '更新时间',
`update_by`                      varchar(32)          NOT NULL DEFAULT '' COMMENT    '更新者',
`sdt_date`                       varchar(16)          NOT NULL DEFAULT '' COMMENT    '日期分区',

  PRIMARY KEY (`id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='商品池移除规则';

alter table   csx_analyse_report_yszx_customer_product_remove_rule_df  add index  `idx_union` (`sdt_date`,`inventory_dc_code`);


-- 商品池运营看板-商品推荐落地表现-换品配置表
-- 任务名：job_mysql2mysql_csx_analyse_report_yszx_change_product_config_df
CREATE TABLE `csx_analyse_report_yszx_change_product_config_df` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键id',
  `biz_id` bigint(20) NOT NULL DEFAULT '0' COMMENT '业务主键id',
  `inventory_dc_code` varchar(32) NOT NULL DEFAULT '' COMMENT '库存dc编码',
  `inventory_dc_name` varchar(128) NOT NULL DEFAULT '' COMMENT '库存dc名称',
  `main_product_code` varchar(10) NOT NULL DEFAULT '' COMMENT '待换品编码',
  `main_product_name` varchar(64) NOT NULL DEFAULT '' COMMENT '待换品名称',
  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `create_by` varchar(30) NOT NULL DEFAULT 'sys' COMMENT '提交人',
  `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  `update_by` varchar(30) NOT NULL DEFAULT 'sys' COMMENT '更新人',
  `given_customer_type` int(1) NOT NULL DEFAULT '0' COMMENT '指定客户类型 0-不限 1-指定客户',
  `sdt_date` varchar(16) NOT NULL DEFAULT '' COMMENT '日期分区',

  PRIMARY KEY (`id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='换品配置表';

alter table  csx_analyse_report_yszx_change_product_config_df  add index   `idx_union` (`sdt_date`,`inventory_dc_code`,`biz_id`);


-- 商品池运营看板-商品推荐落地表现-客户商品清单维护表
-- 任务名：job_mysql2mysql_csx_analyse_report_yszx_customer_product_df
CREATE TABLE `csx_analyse_report_yszx_customer_product_df` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键id',
  `biz_id` bigint(20) NOT NULL DEFAULT '0' COMMENT '业务主键id',
  `customer_code` varchar(20) NOT NULL DEFAULT '' COMMENT '主客户编码',
  `customer_name` varchar(128) NOT NULL DEFAULT '' COMMENT '主客户名称',
  `inventory_dc_code` varchar(20) NOT NULL DEFAULT '' COMMENT '库存地点',
  `product_code` varchar(20) NOT NULL DEFAULT '' COMMENT '商品编码',
  `product_name` varchar(128) NOT NULL DEFAULT '' COMMENT '商品名称',
  `customer_product_name` varchar(128) NOT NULL DEFAULT '' COMMENT '客户化商品名称',
  `big_category_code` varchar(32) NOT NULL DEFAULT '' COMMENT '大类',
  `mid_category_code` varchar(32) NOT NULL DEFAULT '' COMMENT '中类',
  `small_category_code` varchar(32) NOT NULL DEFAULT '' COMMENT '小类',
  `created_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `created_by` varchar(30) NOT NULL DEFAULT 'sys' COMMENT '创建者',
  `updated_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  `updated_by` varchar(30) NOT NULL DEFAULT 'sys' COMMENT '更新者',
  `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `base_product_status` int(1) NOT NULL DEFAULT '0' COMMENT '主数据商品状态：0-正常 3-停售 6-退场 7-停购',
  `data_source` int(1) NOT NULL DEFAULT '0' COMMENT '数据来源：0-手动添加 1-客户订单 2-报价 3-商品池模板 4-必售商品 5-商品池模板替换 6-新品 7-基础商品池 8-CRM换品 9-销售添加',
  `product_type` int(1) NOT NULL DEFAULT '0' COMMENT '商品类型 0：普通商品 1：固定商品',
  `sdt_date` varchar(16) NOT NULL DEFAULT '' COMMENT '日期分区',

  PRIMARY KEY (`id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='客户商品清单维护表';

alter table csx_analyse_report_yszx_customer_product_df add index `update_time` (`inventory_dc_code`,`update_time`);

-- 商品池运营看板-商品推荐落地表现-换品配置详情表
-- 任务名：
CREATE TABLE `csx_analyse_report_yszx_change_product_config_detail_df` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键id',
  `biz_id` bigint(20) NOT NULL DEFAULT '0' COMMENT '业务主键ID',
  `inventory_dc_code` varchar(32) NOT NULL DEFAULT '' COMMENT '库存dc编码',
  `config_id` bigint(20) NOT NULL COMMENT '换品配置ID',
  `change_product_code` varchar(10) NOT NULL DEFAULT '' COMMENT '推荐品编码',
  `change_product_name` varchar(64) NOT NULL DEFAULT '' COMMENT '推荐品名称',
  `unit` varchar(20) NOT NULL DEFAULT '' COMMENT '单位',
  `level` tinyint(4) NOT NULL COMMENT '优先级',
  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `create_by` varchar(30) NOT NULL DEFAULT 'sys' COMMENT '提交人',
  `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  `update_by` varchar(30) NOT NULL DEFAULT 'sys' COMMENT '更新人',
  `sdt_date` varchar(16) NOT NULL DEFAULT '' COMMENT '日期分区',

  PRIMARY KEY (`id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='换品配置详情表';

alter table csx_analyse_report_yszx_change_product_config_detail_df   add index `IDX_CONFIG_ID` (`sdt_date`,`config_id`);



alter table  csx_analyse_report_yszx_dc_product_pool_df  add index `idx_union` (`sdt_date`,`base_product_tag`,`base_product_status`,`inventory_dc_code`,`product_code`);
alter table   csx_analyse_report_yszx_customer_dc_rule_df   add index  `idx_union` (`sdt_date`,`inventory_dc_code`,`customer_flag`,`customer_code`);
alter table   csx_analyse_report_yszx_customer_product_remove_rule_df  add index  `idx_union` (`sdt_date`,`inventory_dc_code`);
alter table csx_analyse_report_yszx_change_product_task_df add index   `idx_union` (`sdt_date`,`inventory_dc_code`,`config_id`);
alter table  csx_analyse_report_yszx_change_product_config_df  add index   `idx_union` (`sdt_date`,`inventory_dc_code`,`biz_id`);
alter table csx_analyse_report_yszx_customer_product_df add index `update_time` (`inventory_dc_code`,`update_time`);
alter table csx_analyse_report_yszx_change_product_config_detail_df   add index `IDX_CONFIG_ID` (`sdt_date`,`config_id`);

