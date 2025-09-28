-- 退货明细-复盘订单共享
select
	b.performance_region_name,     --  销售大区名称(业绩划分)
	b.performance_province_name,     --  销售归属省区名称
	b.performance_city_name,     --  城市组名称(业绩划分)
	a.inventory_dc_code,     --	库存DC编码
	a.inventory_dc_name,     --	库存DC名称
	a.source_type_name,  -- 订单来源(0-签收差异或退货 1-改单退货)
	a.sdt,     --	退货申请日期
	weekofyear(date_sub(to_date(from_unixtime(unix_timestamp(a.sdt,'yyyyMMdd'))),-2)) week,
	a.refund_code,     --	退货单号
	a.sale_order_code,     --	销售单号
	a.customer_code,     --	客户编码
	a.customer_name,     --	客户名称
	a.sub_customer_code,     --	子客户编码
	a.sub_customer_name,     --	子客户名称
	a.goods_code,     --	商品编码
	a.goods_name,     --	商品名称
	a.source_biz_type_name,     --	订单来源
	a.refund_operation_type_name,     --	退货处理方式
	a.has_goods_name,     --	是否有实物退回
	a.responsibility_reason,     --	定责原因
	a.reason_detail,     --	原因说明
	a.business_type_name,     --	业务类型
	a.delivery_type_name,     --	物流模式
	a.refund_order_type_name,     --	退货单类型
	a.refund_qty,     --	退货数量
	a.refund_total_amt,     --	退货总金额
	a.refund_scale_total_amt,     --	处理后退货金额
	c.delivery_date,    -- 出库日期 
	c.sale_price,    -- 团购价
	c.send_qty,    -- 发货数量(基础单位)
	c.send_qty*c.sale_price as sale_amt,
	a.first_level_reason_name,
	a.second_level_reason_name,	
	d.stock_process_type,  -- 库存处理方式：1-报损 2-退供 3-调拨 4-二次上架
	d.stock_process_confirm,  -- 是否确认：0-待确认 1-已确认	
	e.responsible_department_name,  -- 责任部门名称
	e.status,  -- 10-待判责 20-待处理 21-已申诉 22-申诉驳回 30-已完成 -1-已取消	
	e.is_appeal, -- 是否申诉
	e.appeal_reason,  -- 申诉理由	
	a.child_return_type_name,     --	子退货单类型	
	from_utc_timestamp(current_timestamp(),'GMT') update_time,
	c.order_status_name,  -- 订单状态
	a.refund_qty*a.sale_price as refund_amt,
	a.order_status_name as refund_order_status_name  -- 退货单状态
from 
(
select
	inventory_dc_code,
	inventory_dc_name, 
	sdt,
	-- case order_channel_code
	-- when 1 then 'b端'
	-- when 2 then 'm端'
	-- when 3 then 'bbc'
	-- end as order_channel_name,	
	refund_code,
	order_status_code,  -- 退货单状态: 10-差异待审(预留) 20-处理中 30-处理完成 -1-差异拒绝
	case order_status_code
	when -1 then '差异拒绝'
	when 10 then '差异待审'
	when 20 then '处理中'
	when 30 then '处理完成'
	else order_status_code end as order_status_name,  -- 退货单状态
	sale_order_code,
	customer_code,
	regexp_replace(regexp_replace(customer_name,'\n',''),'\r','') as customer_name,
	sub_customer_code,
	regexp_replace(regexp_replace(sub_customer_name,'\n',''),'\r','') as sub_customer_name,
	goods_code,
	regexp_replace(regexp_replace(goods_name,'\n',''),'\r','') as goods_name, 
	case source_biz_type
	when -1 then 'B端订单管理退货'
	when 0 then 'OMS物流审核'
	when 1 then '结算调整数量'
	when 2 then 'OMS调整数量'
	when 3 then 'CRM客诉退货'
	when 4 then 'CRM订单售后退货'
	when 5 then 'CRM预退货审核'
	when 6 then 'CRM签收'
	when 7 then '司机送达时差异'
	when 8 then '司机发起退货'
	when 9 then '实物退仓收货差异'
	when 10 then 'OMS签收'
	end as source_biz_type_name,  -- 订单业务来源（-1-B端订单管理退货 0-OMS物流审核 1-结算调整数量 2-OMS调整数量 3-CRM客诉退货 4-CRM订单售后退货 5-CRM预退货审核 6-CRM签收 7-司机送达时差异 8-司机发起退货 9-实物退仓收货差异 10-OMS签收）
	case refund_operation_type
	when -1 then '不处理'
	when 0 then '立即退'
	when 1 then '跟车退'
	end as refund_operation_type_name,  -- 退货处理方式 -1-不处理 0-立即退 1-跟车退
	case has_goods
	when 0 then '无实物'
	when 1 then '有实物'
	end as has_goods_name,	
	responsibility_reason,
	regexp_replace(regexp_replace(reason_detail,'\n',''),'\r','') as reason_detail,
	case source_type
	when 0 then '签收差异或退货'
	when 1 then '改单退货'
	end as source_type_name,  -- 订单来源(0-签收差异或退货 1-改单退货)
	case 
	when city_supplier_relation_refund_code<>'' then '项目供应商'
	when order_business_type_code=1 then '日配'
	when order_business_type_code=2 then '福利'
	when order_business_type_code=3 then '大宗贸易'
	when order_business_type_code=4 then '内购' end as business_type_name,
	case delivery_type_code
	when 1 then '配送'
	when 2 then '直送'
	when 3 then '自提'
	when 4 then '直通'
	end as delivery_type_name,  -- 配送方式: 1-配送 2-直送 3-自提 4-直通
	-- partner_type_code,
	case child_return_type_code
	when 0 then '父退货单'
	when 1 then '子退货单逆向'
	when 2 then '子退货单正向'
	end as child_return_type_name,  -- 子退货单类型 ：0-父退货单 1-子退货单逆向 2-子退货单正向
	case refund_order_type_code
	when 0 then '差异单'
	when 1 then '退货单'
	end as refund_order_type_name,	-- 退货单类型(0:差异单 1:退货单）
	refund_qty,
	sale_price,
	refund_total_amt,
	refund_scale_total_amt,
	first_level_reason_name,
	regexp_replace(regexp_replace(second_level_reason_name,'\n',''),'\r','') as second_level_reason_name
from csx_dwd.csx_dwd_oms_sale_refund_order_detail_di
where sdt>='${sdt_bf7d}'
and child_return_type_code=1
and parent_refund_code<>''
)a
left join 
(
	select  
		bloc_code,     --  集团编码
		bloc_name,     --  集团名称
		parent_id,customer_id,
		customer_code,
		customer_name,     --  客户名称
		first_category_name,     --  一级客户分类名称
		second_category_name,     --  二级客户分类名称
		performance_region_name,     --  销售大区名称(业绩划分)
		performance_province_name,     --  销售归属省区名称
		performance_city_name     --  城市组名称(业绩划分)
	from csx_dim.csx_dim_crm_customer_info
	where sdt='current'
	and customer_type_code=4
)b on a.customer_code=b.customer_code
left join 
(
  select
	inventory_dc_code,    -- 库存地点编码
	inventory_dc_name,    -- 库存地点名称
	customer_code,    -- 客户编码
	customer_name,    -- 客户名称	
	sub_customer_code,    -- 子客户编码
	sub_customer_name,    -- 子客户名称
	sign_company_code,    -- 签约公司编码
	sign_company_name,    -- 签约公司名称
	order_code,    -- 订单编号	
    -- 订单状态: 10-待接单  20-待发货  30-部分发货  40-配送中  50-待确认 60-已签收  70-已完成  -1-已取消
	case order_status_code
	when 10 then '待接单'
	when 20 then '待发货'
	when 30 then '部分发货'
	when 40 then '配送中'
	when 50 then '待确认'
	when 60 then '已签收'
	when 70 then '已完成'
	when -1 then '已取消'
	else order_status_code end as order_status_name,  -- 订单状态
	delivery_type_code,  -- 配送类型编码：1-配送 2-直送 3-自提
	sale_price*purchase_qty*purchase_unit_rate purchase_amt,    -- 购买金额 
    -- regexp_replace(substr(order_time, 1, 10), '-', '') as order_date,
    regexp_replace(substr(delivery_time, 1, 10), '-', '') as delivery_date,    -- 出库日期 
    -- regexp_replace(substr(sign_time, 1, 10), '-', '') as sign_date,
	goods_code,    -- 商品编码
	goods_name,    -- 商品名称
	sale_price,
	purchase_qty*purchase_unit_rate as purchase_qty,    -- 购买数量
	send_qty,    -- 发货数量(基础单位)
	-- sale_unit_send_qty,    -- 发货数量(销售单位)
	delivery_qty,    -- 送达数量
	sign_qty,    -- 签收数量
    sdt
  from csx_dwd.csx_dwd_oms_sale_order_detail_di
  where sdt >='20230101'
  and order_status_code<>-1  -- 订单状态 -1-已取消
)c on a.sale_order_code=c.order_code and a.goods_code=c.goods_code
-- 客退责任单行表
left join 
(
select 
	responsible_no,	  -- 判责单号
	product_code,	  -- 商品编码
	sale_order_no,
	parent_refund_no,  -- 退货主单号
	refund_no,  -- 退货子单号
	-- stock_process_type,  -- 库存处理方式：1-报损 2-退供 3-调拨 4-二次上架
	case 
	when stock_process_type='1' then '报损'
	when stock_process_type='2' then '退供'
	when stock_process_type='3' then '调拨'
	when stock_process_type='4' then '二次上架'
	else stock_process_type end as stock_process_type,
	-- stock_process_confirm  -- 是否确认：0-待确认 1-已确认
	case 
	when stock_process_confirm='0' then '待确认'
	when stock_process_confirm='1' then '已确认'
	else stock_process_confirm end as stock_process_confirm	
from csx_ods.csx_ods_csx_b2b_oms_refund_responsible_item_df
-- where sdt='20231121'
)d on a.refund_code=d.refund_no and a.goods_code=d.product_code
-- 客退责任单部门处理表
left join 
(
select 
	responsible_no,	  -- 判责单号
	product_code,	  -- 商品编码 
	responsible_department_name,  -- 责任部门名称
	-- status,  -- 10-待判责 20-待处理 21-已申诉 22-申诉驳回 30-已完成 -1-已取消
	case 
	when status='10' then '待判责'
	when status='20' then '待处理'
	when status='21' then '已申诉'
	when status='22' then '申诉驳回'
	when status='30' then '已完成'
	when status='-1' then '已取消'
	else status end as status,	
	if(appeal_person_name<>'','是','否') as is_appeal, -- 是否申诉
	regexp_replace(regexp_replace(appeal_reason,'\n',''),'\r','') as appeal_reason  -- 申诉理由
from csx_ods.csx_ods_csx_b2b_oms_refund_responsible_department_deal_df
-- where sdt='20231121'
)e on d.responsible_no=e.responsible_no and d.product_code=e.product_code
;
