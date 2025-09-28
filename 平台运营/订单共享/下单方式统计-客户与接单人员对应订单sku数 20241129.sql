fixation_report_order_sku
csx_report_csms_order_sku_2m

csx_ods_b2b_mall_prod_yszx_order_extend_real_di 订单表扩展表

 `requirement_order_flag` int(1) NOT NULL DEFAULT '0' COMMENT '需求单标识 0-否，1-是, 默认是0',
 
 yszx_demand_order_ai
 
 
-- 下单方式统计-客户与接单人员对应订单sku数 20241016
with 
demand_order_goods as
(
	select
	c.order_code,
	a1.demand_order_code,
	a1.source,  -- 需求来源：1:销售中台 2:小程序 3：CRM 4:表格自动化
	a.search_key,  -- 	需求名称
	a.goods_code,
	b.product_name as product_name_ai
	from
	(
	select 
	demand_order_code,
	source  -- 需求来源：1:销售中台 2:小程序 3：CRM 4:表格自动化
	from csx_dwd.csx_dwd_csms_yszx_demand_order_df  -- 需求订单表  
	group by 
	demand_order_code,
	source
	)a1 
	left join 
	(
	select demand_order_code,
	search_key,  -- 	需求名称
	goods_code
	from csx_dwd.csx_dwd_csms_yszx_demand_order_item_mf
	where smt>='20241001'
	)a on a1.demand_order_code=a.demand_order_code
	-- 临时同步表 AI识别中商品名称可能多条
	left join 
	(
	select distinct demand_order_no,product_name
	from dev.csx_ods_b2b_mall_prod_yszx_demand_order_item_ai_df
	)b on b.demand_order_no=a.demand_order_code and b.product_name=a.search_key
	left join 
	(
	select distinct demand_order_code,order_code
	from csx_dwd.csx_dwd_csms_yszx_demand_split_order_relation_df
	)c on c.demand_order_code=a.demand_order_code
),

order_goods_counts as
(
	select
		d.order_code,
		c.source, -- 需求来源：1:销售中台 2:小程序 3：CRM 4:表格自动化
		count(d.goods_code) count_sku,
		count(case when c.product_name_ai is not null then d.goods_code end) count_sku_ai
	from demand_order_goods c
	right join 
	(
	select 
	customer_code,   -- 客户编码
	customer_name,   -- 客户名称
	sub_customer_code,   -- 子客户编码
	sub_customer_name,   -- 子客户名称
	order_code,   -- 交易单号
	order_status,   -- 订单状态：created:待支付 paid:待确认 confirmed:待截单 cutted:待出库 stockout:配送中 site:服务站签收 fetched:已自提 home:买家已签收 r_apply:退货申请 r_permit:退货中 r_back:退货回库 r_pay:退款中 r_success:退货成功 r_reject:退货关闭（拒绝退货） success:已完成 cancelled:已取消
	mapp_order_status,   -- 小程序订单状态：created:待支付 confirmed:待截单 cutted:待出库 stockout:配送中 home:买家已签收 cancelled:已取消
	recep_order_user_number,   -- 接单人工号
	recep_order_by,   -- 接单人
	require_delivery_date,   -- 要求送货日期
	goods_code
	from csx_dwd.csx_dwd_csms_yszx_order_detail_di
	where sdt>='20241001'  -- 下单日期
	and require_delivery_date>=regexp_replace(date_sub(date_sub(current_date,1),90),'-','')	-- 要求送货日期
	and order_status not in ('CANCELLED','PAID','CONFIRMED') 	
	)d on c.order_code=d.order_code and c.goods_code=d.goods_code
	group by d.order_code,c.source	
)
select 
e.performance_region_name as `大区`,
a.performance_province_name as `省区`,
a.performance_city_name as `城市`,
a.customer_code as `客户编码`,
e.customer_name as `客户名称`,
a.sub_customer_code as `子客户编码`,
a.sub_customer_name as `子客户名称`,
a.order_code as `订单号`,
a.recep_order_time as `接单时间`,
a.operator_id as `接单员工号`,
a.operator_name as `接单员姓名`,
a.order_sku as `接单sku`,
f.count_sku as `需求单_sku`,
f.count_sku_ai as `需求单微信AI_sku`,
a.sdt as `接单日期`,
case 
when substr(a.order_code,1,2)='OW' then '小程序'
when f.source=4 then '表格自动化'
when f.count_sku_ai>0 then '销售中台-微信AI'
when f.source=1 then '销售中台-正常手工单'
else '销售中台-正常手工单' end as `下单方式`
from 
( 
  select * 
  from csx_report.csx_report_csms_order_sku -- 数据中心 接单与签收 明细表
  where sdt between '20241026' and '20241125'
  and mode=1 -- 模式 接单:1 签收:2
)a 
left join
  (
    select
      customer_code,
      customer_name,
      performance_region_code,
      performance_region_name,
      performance_province_code,
      performance_province_name,
      performance_city_code,
      performance_city_name,
	  sales_user_number,
	  sales_user_name	  
    from csx_dim.csx_dim_crm_customer_info
    where sdt = 'current'
	and customer_type_code=4
  )e on e.customer_code=a.customer_code 
left join order_goods_counts f on f.order_code=a.order_code
;









left join 
(
select distinct demand_order_code,order_code
from csx_dwd.csx_dwd_csms_yszx_demand_split_order_relation_df
)a2 on a2.order_code=a.order_code
-- 订单表扩展表 判断是否来自需求单
left join 
(
  select order_no,requirement_order_flag
  from csx_ods.csx_ods_b2b_mall_prod_yszx_order_extend_real_di
  where sdt>='20240101'
  group by order_no,requirement_order_flag
)b on a.order_code=b.order_no
left join 
(
  select 
  demand_order_code,
  source  -- 需求来源：1:销售中台 2:小程序 3：CRM 4:表格自动化
  from csx_dwd.csx_dwd_csms_yszx_demand_order_df  -- 需求订单表  
  group by 
  demand_order_code,
  source
)c on a2.demand_order_code=c.demand_order_code
left join 
(
  select demand_order_no
  from csx_ods.csx_ods_b2b_mall_prod_yszx_demand_order_ai_df
  group by demand_order_no
)d on d.demand_order_no=c.demand_order_code
left join 
(
  select demand_order_no,
	count(product_name) count_sku_ai
  from csx_ods.csx_ods_b2b_mall_prod_yszx_demand_order_item_ai_df
  group by demand_order_no
)d2 on d2.demand_order_no=d.demand_order_code



csx_dwd_csms_yszx_demand_order_item_mf(需求订单商品表)




