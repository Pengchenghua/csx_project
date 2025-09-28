CREATE TABLE `exceptions_monitor` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键',
  `out_no` varchar(32) NOT NULL DEFAULT '' COMMENT '外部单号',
  `dc_code` varchar(8) NOT NULL DEFAULT '' COMMENT '库存dc 编码',
  `dc_name` varchar(64) NOT NULL DEFAULT '' COMMENT '库存dc 名称',
  `shop_code` varchar(32) NOT NULL DEFAULT '' COMMENT '门店编码',
  `shop_name` varchar(32) NOT NULL DEFAULT '' COMMENT '门店名称',
  `ex_big_class` tinyint(4) NOT NULL DEFAULT '0' COMMENT '异常大类（1- 生单异常 2-签收异常）',
  `ex_small_class` tinyint(4) NOT NULL COMMENT '异常小类：\r\n11-订单明细不存在\r\n12-门店编码不存在\r\n13-商品信息不存在\r\n14-供应商编码未配置库存地点\r\n15-门店采购组未绑定库存地点\r\n16-商品在库存地点下无档\r\n17-云超下发价格异常\r\n18-货到即配主单未查询到子单\r\n19-货到即配主单与子单不匹配\r\n\r\n21-订单未生单\r\n22-订单未接单\r\n23-订单未发货\r\n24-供应商直送单供应链未审核\r\n25-订单发货信息未推送\r\n26-订单已完成',
  `status` tinyint(4) NOT NULL DEFAULT '1' COMMENT '状态（1- 初始 2-已更新 3-已解决）',
  `supplier_code` varchar(64) NOT NULL DEFAULT '' COMMENT '供应商编码',
  `remarks` longtext COMMENT '补充说明',
  `inner_order_no` varchar(32) NOT NULL DEFAULT '' COMMENT '拆单号，生单异常没有拆单号',
  `created_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '申请时间(创建时间)',
  `updated_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  `created_by` varchar(32) NOT NULL DEFAULT 'sys' COMMENT '申请人',
  `updated_by` varchar(32) NOT NULL DEFAULT 'sys' COMMENT '更新人',
  `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  PRIMARY KEY (`id`) USING BTREE,
  KEY `idx_query` (`out_no`,`dc_code`,`shop_code`) USING BTREE,
  KEY `idx_query_time` (`created_time`,`updated_time`) USING BTREE,
  KEY `idx_status_ex_big_class_shop_code` (`status`,`ex_big_class`,`shop_code`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=45907 DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC COMMENT='异常监控';




select * from csx_b2b_sell.exceptions_monitor where out_no='PO84612209150001';

select 
	out_no,shop_code,shop_name,dc_code,dc_name,supplier_code,ex_big_class,
	case ex_big_class
		when 1 then '生单异常' 
		when 2 then '签收异常' 
		when 3 then '退货申请异常'
		when 4 then '退货入库异常'
		when 5 then '联营VB单异常'
	end as ex_big_class_name,
	ex_small_class,
	case ex_small_class 
		when 11 then '订单明细不存在' 
		when 12 then '门店编码不存在' 
		when 13 then '商品信息不存在' 
		when 14 then '供应商编码未配置库存地点' 
		when 15 then '门店采购组未绑定库存地点'
		when 16 then '商品在库存地点下无档' 
		when 17 then '云超下发价格异常' 
		when 18 then '货到即配主单未查询到子单' 
		when 19 then '货到即配主单与子单不匹配' 
		when 21 then '订单未生单' 
		when 22 then '订单未接单' 
		when 23 then '订单未发货' 
		when 24 then '供应商直送单供应链未审核' 
		when 25 then '订单发货信息未推送' 
		when 26 then '订单已完成' 
		when 27 then '订单出库成本WMS未全部返回'
		when 31 then '原正向单不存在'
		when 32 then '原正向单未完成'
		when 41 then '原正向单补单未出库'
		when 51 then '商品在虚拟门店下无档'
	end as ex_small_class_name,
	status,
	case status 
		when 1 then '初始' 
		when 2 then '已更新' 
		when 3 then '已解决' 
	end as status_name,
	remarks,inner_order_no,created_time,updated_time
from 
	csx_b2b_sell.exceptions_monitor