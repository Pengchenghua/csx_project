SET hive.tez.container.size=8192;
set hive.merge.tezfiles=false;

insert overwrite table csx_analyse.csx_analyse_wms_requisition_order_detail_di

select
	a.id,a.order_code,a.dc_code,a.dc_name,a.shipper_code,a.shipper_name,a.goods_code,a.goods_bar_code,a.goods_name,a.unit_name,a.price,a.requisition_qty,a.requisition_amt,
	a.price_no_tax,a.requisition_amt_no_tax,a.requisition_type_code,a.requisition_type_name,a.cost_center_code,a.cost_center_name,a.status,a.remark,a.create_time,a.create_by,
	a.update_time,a.update_by,a.excute_time,a.excute_by,a.production_date,a.expiry_date,a.store_location_code,a.approval_status,a.approve_remarks,a.user_id,a.stock_attribute_code,
	b.purchase_group_code,b.purchase_group_name,b.category_large_code,b.category_large_name,b.category_middle_code,b.category_middle_name,b.category_small_code,b.category_small_name,
	c.area_code,c.area_name,c.province_code,c.province_name,c.city_code,c.city_name,c.first_code,c.first_name,c.second_code,c.second_name,
	regexp_replace(to_date(a.excute_time),'-','') as excute_date,
	a.sdt
from 
	(
	select 
		*
	from 
		csx_dwd.csx_dwd_wms_requisition_order_detail_di
	) a 
	left join
		(
		select 
			goods_code,goods_name,purchase_group_code,purchase_group_name,category_large_code,category_large_name,category_middle_code,category_middle_name,category_small_code,category_small_name
		from 
			csx_dim.csx_dim_basic_goods
		where 
			sdt='current' 
		) b on b.goods_code=a.goods_code
	left join
		(
		select 
			cost_center,cost_center_name,area_code,area_name,province_code,province_name,city_code,city_name,first_code,first_name,second_code,second_name
		from 
			(
			select 
				*,row_number()over(partition by cost_center order by sdt desc,pstng_date desc) num
			from 
				csx_ods.csx_ods_csx_b2b_finance_config_cost_area_relation_df
			where
				sdt>='${90days_ago}'
			) aa 
		where 
			num=1
		group by 
			cost_center,cost_center_name,area_code,area_name,province_code,province_name,city_code,city_name,first_code,first_name,second_code,second_name
		) c on c.cost_center=a.cost_center_code
distribute by sdt
;
	
/*
--------------------------------- hive建表语句 -------------------------------
-- csx_analyse.csx_analyse_wms_requisition_order_detail_di  领用明细表

drop table if exists csx_analyse.csx_analyse_wms_requisition_order_detail_di;
create table csx_analyse.csx_analyse_wms_requisition_order_detail_di(
`id`                             bigint              COMMENT    '主键id',
`order_code`                     string              COMMENT    '领用单号',
`dc_code`                        string              COMMENT    '仓库编号',
`dc_name`                        string              COMMENT    '仓库名称',
`shipper_code`                   string              COMMENT    '货主编号',
`shipper_name`                   string              COMMENT    '货主名称',
`goods_code`                     string              COMMENT    '商品编号',
`goods_bar_code`                 string              COMMENT    '商品条码',
`goods_name`                     string              COMMENT    '商品名称',
`unit_name`                      string              COMMENT    '单位',
`price`                          decimal(12,4)       COMMENT    '领用商品单价',
`requisition_qty`                decimal(12,3)       COMMENT    '领用数量',
`requisition_amt`                decimal(12,4)       COMMENT    '领用商品总金额',
`price_no_tax`                   decimal(12,4)       COMMENT    '不含税领用商品单价',
`requisition_amt_no_tax`         decimal(12,4)       COMMENT    '不含税领用商品总金额',
`requisition_type_code`          string              COMMENT    '领用类型编号',
`requisition_type_name`          string              COMMENT    '领用类型名称',
`cost_center_code`               string              COMMENT    '成本中心编号',
`cost_center_name`               string              COMMENT    '成本中心名称',
`status`                         int                 COMMENT    '状态 0-初始1-已执行 2-已关闭',
`remark`                         string              COMMENT    '备注',
`create_time`                    timestamp           COMMENT    '创建时间',
`create_by`                      string              COMMENT    '创建者',
`update_time`                    timestamp           COMMENT    '更新时间',
`update_by`                      string              COMMENT    '更新者',
`excute_time`                    timestamp           COMMENT    '执行时间',
`excute_by`                      string              COMMENT    '执行人',
`production_date`                timestamp           COMMENT    '生产日期',
`expiry_date`                    timestamp           COMMENT    '有效期至',
`store_location_code`            string              COMMENT    '储位编码',
`approval_status`                int                 COMMENT    '审批状态',
`approve_remarks`                string              COMMENT    '审批意见',
`user_id`                        bigint              COMMENT    '发起审批人ID',
`stock_attribute_code`           string              COMMENT    '库存属性编码',
`purchase_group_code`            string              COMMENT    '采购组(课组)编码',
`purchase_group_name`            string              COMMENT    '采购组(课组)名称',
`category_large_code`            string              COMMENT    '大类编号',
`category_large_name`            string              COMMENT    '大类名称',
`category_middle_code`           string              COMMENT    '中类编号',
`category_middle_name`           string              COMMENT    '中类名称',
`category_small_code`            string              COMMENT    '小类编号',
`category_small_name`            string              COMMENT    '小类名称',
`area_code`                      string              COMMENT    '大区编码',
`area_name`                      string              COMMENT    '大区名称',
`province_code`                  string              COMMENT    '省区编码',
`province_name`                  string              COMMENT    '省区名称',
`city_code`                      string              COMMENT    '城市编码',
`city_name`                      string              COMMENT    '城市名称',
`first_code`                     string              COMMENT    '一级部门编码',
`first_name`                     string              COMMENT    '一级部门名称',
`second_code`                    string              COMMENT    '二级部门编码',
`second_name`                    string              COMMENT    '二级部门名称',
`excute_date`                    string              COMMENT    '执行日期',
`sdt`                            string              COMMENT    '日期分区'
) COMMENT '领用明细表'
STORED AS PARQUET;

*/


