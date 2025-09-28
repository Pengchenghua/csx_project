CREATE TABLE `yszx_change_product_task` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键ID',
  `config_id` bigint(20) NOT NULL COMMENT '换品配置ID',
  `inventory_dc_code` varchar(32) NOT NULL DEFAULT '' COMMENT '库存dc编码',
  `inventory_dc_name` varchar(128) NOT NULL DEFAULT '' COMMENT '库存dc名称',
  `sap_cus_code` varchar(32) NOT NULL DEFAULT '' COMMENT '客户编码',
  `sap_cus_name` varchar(100) NOT NULL DEFAULT '' COMMENT '客户名称',
  `main_product_code` varchar(10) NOT NULL DEFAULT '' COMMENT '待换品编码',
  `main_product_name` varchar(64) NOT NULL DEFAULT '' COMMENT '待换品名称',
  `unit` varchar(20) NOT NULL DEFAULT '' COMMENT '单位',
  `sales_amount` decimal(13,2) NOT NULL DEFAULT '0.00' COMMENT '销售额',
  `profit_margin` decimal(13,2) NOT NULL DEFAULT '0.00' COMMENT '毛利率',
  `profit_margin_type` tinyint(4) NOT NULL DEFAULT '2' COMMENT '毛利率类型： 1：负毛利 2：低毛利 3：正常毛利',
  `change_product_code` varchar(10) NOT NULL DEFAULT '' COMMENT '推荐品编码',
  `change_product_name` varchar(64) NOT NULL DEFAULT '' COMMENT '推荐品名称',
  `change_unit` varchar(20) NOT NULL DEFAULT '' COMMENT '推荐品单位',
  `status` tinyint(4) unsigned NOT NULL DEFAULT '1' COMMENT '换品任务状态 1：待处理 2：已完成 3：已拒绝',
  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `create_by` varchar(30) NOT NULL DEFAULT 'sys' COMMENT '提交人',
  `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  `update_by` varchar(30) NOT NULL DEFAULT 'sys' COMMENT '更新者',
  PRIMARY KEY (`id`),
  KEY `idx_status_sap_code` (`status`,`sap_cus_code`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=37207 DEFAULT CHARSET=utf8mb4 COMMENT='换品任务表';
;

select
	inventory_dc_code,inventory_dc_name,main_product_code,main_product_name,unit,
	sales_amount,profit_margin,sap_cus_code,sap_cus_name,
	-- status,
	case status when 1 then '待处理' when 2 then '已完成' when 3 then '已拒绝' end as status_name,
	-- create_by,
	update_by,
	change_product_code,change_product_name,change_unit,create_time
	-- update_time
from
	b2b_mall_prod.yszx_change_product_task
;



select
	inventory_dc_code,
	inventory_dc_name,
	main_product_code,
	replace(main_product_name,char(9),'') as main_product_name,
	unit,
	sales_amount,profit_margin,sap_cus_code,
	sap_cus_name,
	-- status,
	case status when 1 then '待处理' when 2 then '已完成' when 3 then '已拒绝' end as status_name,
	-- create_by,
	update_by,
	change_product_code,
	change_product_name,
	change_unit,create_time,
	update_time
from
	b2b_mall_prod.yszx_change_product_task
	b2b_mall_prod.yszx_change_product_config
where
	1=1
	-- status in (2,3)
	-- and date(update_time)>='2022-10-20'
;

select
	a.inventory_dc_code,a.inventory_dc_name,a.main_product_code,a.main_product_name,a.unit,a.sales_amount,a.profit_margin,a.sap_cus_code,
	a.sap_cus_name,a.status_name,a.update_by,a.change_product_code,a.change_product_name,a.change_unit,
	b.update_time, -- 操作时间
	a.update_time -- 完成时间
from
	(
	select
		inventory_dc_code,inventory_dc_name,main_product_code,
		replace(main_product_name,char(9),'') as main_product_name,
		unit,sales_amount,profit_margin,sap_cus_code,sap_cus_name,
		-- status,
		case status when 1 then '待处理' when 2 then '已完成' when 3 then '已拒绝' end as status_name,
		-- create_by,
		update_by,change_product_code,change_product_name,change_unit,
		-- create_time,
		update_time
	from
		b2b_mall_prod.yszx_change_product_task
	where
		1=1
		-- status in (2,3)
		-- and date(update_time)>='2022-10-20'
	) a 
	left join
		(
		select
			inventory_dc_code,main_product_code,update_time
		from
			b2b_mall_prod.yszx_change_product_config
		group by 
			inventory_dc_code,main_product_code,update_time
		) b on b.inventory_dc_code=a.inventory_dc_code and b.main_product_code=a.main_product_code
;