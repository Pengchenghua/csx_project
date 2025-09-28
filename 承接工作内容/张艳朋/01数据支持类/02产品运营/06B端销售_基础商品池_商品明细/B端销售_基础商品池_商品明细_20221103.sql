CREATE TABLE `yszx_dc_product_pool` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键ID',
  `inventory_dc_code` varchar(20) NOT NULL DEFAULT '' COMMENT '库存地点',
  `product_code` varchar(20) NOT NULL DEFAULT '' COMMENT '商品编码',
  `sync_customer_product_flag` int(1) NOT NULL DEFAULT '0' COMMENT '是否同步客户商品池 0-否 1-是',
  `base_product_tag` int(1) NOT NULL DEFAULT '1' COMMENT '基础商品标签 0-否 1-是',
  `base_product_status` int(1) NOT NULL DEFAULT '0' COMMENT '主数据商品状态：0-正常 3-停售 6-退场 7-停购',
  `created_by` varchar(30) NOT NULL DEFAULT 'sys' COMMENT '创建者',
  `updated_by` varchar(30) NOT NULL DEFAULT 'sys' COMMENT '更新者',
  `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `udx` (`inventory_dc_code`,`product_code`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=28048 DEFAULT CHARSET=utf8mb4 COMMENT='库存地点商品关系';


	select
		a.inventory_dc_code,
		a.product_code,
		b.product_name,
		b.unit,
		a.base_product_status,
		case a.base_product_status
			when 0 then '正常'
			when 3 then '停售'
			when 6 then '退场'
			when 7 then '停购'
		end as base_product_status_name,
		a.sync_customer_product_flag,
		case a.sync_customer_product_flag
			when 0 then '否' when 1 then '是'
		end as sync_customer_product_flag_name,
		a.create_time,
		a.created_by
	from
		b2b_mall_prod.yszx_dc_product_pool a 
		left join b2b_mall_prod.yszx_base_product b on b.product_code=a.product_code
	where
		a.base_product_tag=1;
		
CREATE TABLE `yszx_cus_product_rule` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键ID',
  `customer_code` varchar(20) NOT NULL DEFAULT '' COMMENT '主客户编码',
  `price_auto_add_flag` int(1) NOT NULL DEFAULT '0' COMMENT '报价自动添加标识 0-未启动 1-已启动',
  `create_order_auto_add_flag` int(1) NOT NULL DEFAULT '0' COMMENT '下单自动添加标识 0-未启动 1-已启动',
  `must_sale_auto_add_flag` int(1) NOT NULL DEFAULT '0' COMMENT '必售商品自动添加标识 0-未启动 1-已启动',
  `lock_customer_product_flag` int(1) NOT NULL DEFAULT '0' COMMENT '锁定小程序商品池 0-未锁定 1-已锁定',
  `update_by` varchar(30) NOT NULL DEFAULT 'sys' COMMENT '更新者',
  `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  `create_by` varchar(30) NOT NULL DEFAULT 'sys' COMMENT '创建者',
  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `remove_customer_product_flag` int(1) NOT NULL DEFAULT '0' COMMENT '自动移除商品池 0-关闭 1-开启',
  `lock_mall_product_flag` int(1) NOT NULL DEFAULT '0' COMMENT '锁定中台商品池 0-未锁定 1-已锁定',
  `filter_zs_flag` int(1) NOT NULL DEFAULT '0' COMMENT '过滤直送单标识 0-否，1-是',
  `filter_patch_flag` int(1) NOT NULL DEFAULT '0' COMMENT '过滤补单标识 0-否，1-是',
  `bind_common_product_flag` int(1) NOT NULL DEFAULT '0' COMMENT '绑定基础商品池 0-否 1-是',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `idx_customer_code` (`customer_code`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=24165 DEFAULT CHARSET=utf8mb4 COMMENT='客户商品池规则';

CREATE TABLE `yszx_out_cus_info` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键ID',
  `out_customer_code` varchar(32) NOT NULL DEFAULT '' COMMENT '外部客户编码',
  `out_customer_name` varchar(128) NOT NULL DEFAULT '' COMMENT '外部客户名称',
  `out_sub_customer_code` varchar(32) NOT NULL DEFAULT '' COMMENT '外部子客户编码',
  `out_sub_customer_name` varchar(128) NOT NULL DEFAULT '' COMMENT '外部子客户名称',
  `sap_cus_code` varchar(32) NOT NULL DEFAULT '' COMMENT '客户编码',
  `cust_con` varchar(64) NOT NULL DEFAULT '' COMMENT '收货人姓名',
  `con_tel` varchar(16) NOT NULL DEFAULT '' COMMENT '收货人电话',
  `receive_addr` varchar(255) NOT NULL DEFAULT '' COMMENT '收货地址',
  `project_flag` varchar(32) NOT NULL DEFAULT '' COMMENT '对接项目标识(1002:童帮项目)',
  `status` int(2) NOT NULL DEFAULT '0' COMMENT '状态：(0:待处理 1:已处理)',
  `create_by` varchar(30) NOT NULL DEFAULT 'sys' COMMENT '创建人',
  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_by` varchar(30) NOT NULL DEFAULT 'sys' COMMENT '更新人',
  `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_unique_index` (`out_customer_code`,`out_sub_customer_code`,`project_flag`) USING BTREE COMMENT '外部客户+外部子客户 +客户来源',
  KEY `idx_out_customer_code` (`out_customer_code`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=4 DEFAULT CHARSET=utf8mb4 COMMENT='外部客户信息表';



select
	a.customer_code,
	coalesce(b.customer_name,c.customer_name,d.customer_name) as customer_name,
	-- a.price_auto_add_flag,
	case a.price_auto_add_flag when 0 then '未启动' when 1 then '已启动' end as price_auto_add_flag_name,
	-- a.create_order_auto_add_flag,
	case a.create_order_auto_add_flag when 0 then '未启动' when 1 then '已启动' end as create_order_auto_add_flag_name,
	-- a.bind_common_product_flag,
	case a.bind_common_product_flag when 0 then '否' when 1 then '是' end as bind_common_product_flag_name,
	-- a.lock_customer_product_flag,
	case a.lock_customer_product_flag when 0 then '未锁定' when 1 then '已锁定' end as lock_customer_product_flag_name,
	-- a.lock_mall_product_flag,
	case a.lock_mall_product_flag when 0 then '未锁定' when 1 then '已锁定' end lock_mall_product_flag_name,
	-- a.remove_customer_product_flag,
	case a.remove_customer_product_flag when 0 then '关闭' when 1 then '开启' end as remove_customer_product_flag_name,
	a.create_by,
	a.create_time
from
	b2b_mall_prod.yszx_cus_product_rule a 
	-- left join (select distinct out_customer_code,out_customer_name from b2b_mall_prod.yszx_out_cus_info) b on b.out_customer_code=a.customer_code
	left join (select distinct customer_number,customer_name from csx_b2b_crm.customer) b on b.customer_number=a.customer_code
	left join (select distinct customer_number,customer_name from csx_b2b_crm.customer_20191223) c on c.customer_number=a.customer_code
	left join (select distinct customer_number,customer_name from csx_b2b_crm.customer_20200518) d on d.customer_number=a.customer_code
	