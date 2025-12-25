-- 客诉清单表
-- drop table csx_analyse_tmp.complaint_code_list_use;
-- csx_analyse_tmp.complaint_code_list_use


-- ===============================================================================================================================================================================
-- 签收SKU数 只取渠道为大客户、业务代理，业务类型剔除BBC，城市服务商；
-- drop table if exists csx_analyse_tmp.csx_analyse_tmp_oms_complaint_report_01;
-- create table csx_analyse_tmp.csx_analyse_tmp_oms_complaint_report_01
-- as
select 
	substr(a.sdt,1,6) as smonth,a.performance_region_name,a.performance_province_name,b.classify_large_name,b.classify_middle_name,b.business_division_name,
	count(a.goods_code) as sku_cnt
from
	( 
	select
		sdt,customer_code,order_code,goods_code,sale_amt,profit,performance_region_name,performance_province_name
	from csx_dws.csx_dws_sale_detail_di
	where 
		sdt between '20250401' and '20250430'
		and channel_code in ('1','7','9') 
		and business_type_code not in (4,6) -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.bbc 7.大宗一部 8.大宗二部 9.商超)
		and order_channel_code = 1 -- 1-b端 2-m端 3-bbc 4-客户返利管理 5-价格补救 6-客户调价管理 -1-sap
		and refund_order_flag= 0  -- 剔除退货
		and performance_province_name not in ( '平台-B'	,'东北')  
		and direct_delivery_type not in (1,2)  -- 直送类型: 0-p(普通) 1-r(融单)、2-z(过账)、11-(临时加单)、12-(紧急补货)
		and shipper_code='YHCSX'
		
	) a 
	left join
	(
	select
		goods_code,goods_name,classify_large_name,classify_middle_name,classify_small_name,business_division_code,business_division_name
	from csx_dim.csx_dim_basic_goods
	where sdt='current'
	) b on b.goods_code=a.goods_code	
group by 
	substr(a.sdt,1,6),a.performance_region_name,a.performance_province_name,b.classify_large_name,b.classify_middle_name,b.business_division_name;



-- 客诉去重 不含责任部门 如果同一个客诉单号涉及多个责任部门，则按照每个责任部门统计时分别+1
-- drop table if exists csx_analyse_tmp.csx_analyse_tmp_oms_complaint_report_02;
-- create table csx_analyse_tmp.csx_analyse_tmp_oms_complaint_report_02
-- as
select 
	smonth,sdt,week_of_year,performance_region_name,performance_province_name,customer_code,customer_name,first_category_name,
	second_category_name,classify_large_name,classify_middle_name,business_division_name,goods_code,goods_name,customer_large_level,
	main_category_name,sub_category_name,
	sum(ks_cnt) ks_cnt
from 
(select 
	substr(a.sdt,1,6) as smonth,a.sdt,e.week_of_year,a.performance_region_name,a.performance_province_name,a.customer_code,c.customer_name,c.first_category_name,c.second_category_name,
	coalesce(b.classify_large_name,'') as classify_large_name,
	coalesce(b.classify_middle_name,'') as classify_middle_name,
	coalesce(b.business_division_name,'') as business_division_name,
	a.goods_code,coalesce(b.goods_name,'') as goods_name,
	coalesce(d.customer_large_level,'') as customer_large_level,
	a.main_category_name,
	a.sub_category_name,
	case 
		when coalesce(f.direct_delivery_type,j.direct_delivery_type) is null then '其他'
		when coalesce(f.direct_delivery_type,j.direct_delivery_type) ='' then '其他'
		else coalesce(f.direct_delivery_type,j.direct_delivery_type) end as direct_delivery_type,
	count(distinct a.complaint_code) as ks_cnt
from
	(
	select
		substr(sdt,1,6) as smonth,sdt,performance_region_name,performance_province_name,customer_code,sale_order_code,goods_code,goods_name,complaint_code,
		first_level_department_name,second_level_department_name,main_category_name,sub_category_name
	from csx_dws.csx_dws_oms_complaint_detail_di
	where 
		sdt between '20250401' and '20250430'
		and performance_province_name not in ('东北','平台-B')
		-- and complaint_status_code in(20,30) -- 客诉单状态 10'待判责' 20'处理中' 21'待审核' 30'已完成' -1'已取消'
		-- ★and complaint_deal_status in(40) -- 责任环节状态 10'待处理' 20'待修改' 30'已处理待审' 31'已驳回待审核' 40'已完成' -1'已取消'
		and complaint_deal_status not in(-1)
		-- and complaint_source = 1  --客诉来源:1-单独发起客诉单 2-客退单生成 3-补货单生成
		-- and customer_name not like '%XM%' 
		and shipper_code='YHCSX'
		and complaint_code in( select complaint_code from csx_analyse_tmp.complaint_code_list_use)		
	) a
	left join
		(
		select
			goods_code,goods_name,classify_large_name,classify_middle_name,classify_small_name,business_division_code,business_division_name
		from csx_dim.csx_dim_basic_goods
		where sdt='current'
		) b on b.goods_code=a.goods_code		
	left join
		(
		select
			customer_code,customer_name,performance_region_name,performance_province_name,performance_city_name,first_category_name,second_category_name,third_category_name
		from csx_dim.csx_dim_crm_customer_info
		where sdt='current'
		) c on c.customer_code=a.customer_code
	left join
		(
		select customer_no,customer_large_level,month
		from csx_analyse.csx_analyse_report_sale_customer_level_mf
		where month>='202410' and month<='202512'
			-- and customer_large_level in ('A','B')
			and tag=1 --数据标识：1：全量数据；2：剔除不统计业绩仓数据
		) d on d.customer_no=a.customer_code and d.month=a.smonth
	left join
		(
		select calday,week_of_year
		from csx_dim.csx_dim_basic_date
		) e on e.calday=a.sdt
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
		case 
		when delivery_type_code<>2 then ''
		when delivery_flag=1 then '直送1'
		when delivery_flag=2 then '直送2'
		else '其他' end as direct_delivery_type,
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
	where sdt >=regexp_replace(trunc(add_months(date_format('${sdt_star_date}', 'yyyy-MM-dd'),-6),'MM'),'-','')
		and order_status_code<>-1  -- 订单状态 -1-已取消
	)f on a.sale_order_code=f.order_code and a.goods_code=f.goods_code	
	-- 通过销售表取业务类型和直送类型
	left join 
	(
	select *
	from 
		(
		select 
			-- customer_code,
			-- order_code,    -- 订单编号
			original_order_code,
			goods_code,    -- 商品编码
			goods_name,    -- 商品名称
			business_type_code,    -- 业务类型编码(1.日配业务,2.福利业务,3.批发内购,4.城市服务商,5.省区大宗,6.bbc,7.大宗一部,8.大宗二部,9.商超)
			business_type_name,    -- 业务类型名称
			-- direct_delivery_type   -- 直送类型: 0-p(普通) 1-r(融单)、2-z(过账)、11-(临时加单)、12-(紧急补货)
			delivery_type_name,
			case 
				when delivery_type_name<>'直送' then ''
				when direct_delivery_type=1 then '直送1'
				when direct_delivery_type=2 then '直送2'
				 else '其他' end direct_delivery_type,
			row_number() over(partition by order_code,goods_code order by delivery_time asc) as rno
		from csx_dws.csx_dws_sale_detail_di
		where sdt >=regexp_replace(trunc(add_months(date_format('${sdt_star_date}', 'yyyy-MM-dd'),-2),'MM'),'-','')
		-- and refund_order_flag=1 -- 退货单
		and order_channel_code not in(4,5,6) -- '调价返利价格补救'
		)a where rno=1
	)j on a.sale_order_code=j.original_order_code and a.goods_code=j.goods_code				
group by 
	substr(a.sdt,1,6),a.sdt,e.week_of_year,a.performance_region_name,a.performance_province_name,a.customer_code,c.customer_name,c.first_category_name,c.second_category_name,
	coalesce(b.classify_large_name,''),
	coalesce(b.classify_middle_name,''),
	coalesce(b.business_division_name,''),
	a.goods_code,coalesce(b.goods_name,''),
	coalesce(d.customer_large_level,''),
	a.main_category_name,
	a.sub_category_name,
	case 
		when coalesce(f.direct_delivery_type,j.direct_delivery_type) is null then '其他'
		when coalesce(f.direct_delivery_type,j.direct_delivery_type) ='' then '其他'
		else coalesce(f.direct_delivery_type,j.direct_delivery_type) end	
)a 
where direct_delivery_type='' or direct_delivery_type not in('直送1','直送2')	
group by 
	smonth,sdt,week_of_year,performance_region_name,performance_province_name,customer_code,customer_name,first_category_name,
	second_category_name,classify_large_name,classify_middle_name,business_division_name,goods_code,goods_name,customer_large_level,
	main_category_name,sub_category_name
;



-- 责任部门统计 同一个客诉单涉及多个责任部门则每个部门+1
select 
	smonth,sdt,week_of_year,performance_region_name,performance_province_name,customer_code,customer_name,first_category_name,
	second_category_name,classify_large_name,classify_middle_name,business_division_name,goods_code,goods_name,customer_large_level,
	main_category_name,sub_category_name,first_level_department_name,second_level_department_name,
	sum(ks_cnt) ks_cnt
from 
(select 
	substr(a.sdt,1,6) as smonth,a.sdt,e.week_of_year,a.performance_region_name,a.performance_province_name,a.customer_code,c.customer_name,c.first_category_name,c.second_category_name,
	coalesce(b.classify_large_name,'') as classify_large_name,
	coalesce(b.classify_middle_name,'') as classify_middle_name,
	coalesce(b.business_division_name,'') as business_division_name,
	a.goods_code,coalesce(b.goods_name,'') as goods_name,
	coalesce(d.customer_large_level,'') as customer_large_level,
	a.main_category_name,
	a.sub_category_name,
	a.first_level_department_name,
	a.second_level_department_name,
	case 
		when coalesce(f.direct_delivery_type,j.direct_delivery_type) is null then '其他'
		when coalesce(f.direct_delivery_type,j.direct_delivery_type) ='' then '其他'
		else coalesce(f.direct_delivery_type,j.direct_delivery_type) end as direct_delivery_type,
	count(distinct a.complaint_code) as ks_cnt
from
	(
	select
		substr(sdt,1,6) as smonth,sdt,performance_region_name,performance_province_name,customer_code,sale_order_code,goods_code,goods_name,complaint_code,
		first_level_department_name,second_level_department_name,main_category_name,sub_category_name
	from csx_dws.csx_dws_oms_complaint_detail_di
	where sdt between '20250401' and '20250430'
		and performance_province_name not in ('东北','平台-B')
		-- and complaint_status_code in(20,30) -- 客诉单状态 10'待判责' 20'处理中' 21'待审核' 30'已完成' -1'已取消'
		-- ★and complaint_deal_status in(40) -- 责任环节状态 10'待处理' 20'待修改' 30'已处理待审' 31'已驳回待审核' 40'已完成' -1'已取消'
		and complaint_deal_status not in(-1)
		-- and complaint_source = 1  --客诉来源:1-单独发起客诉单 2-客退单生成 3-补货单生成
		-- and customer_name not like '%XM%' 
		and shipper_code='YHCSX'
		and complaint_code in( select complaint_code from csx_analyse_tmp.complaint_code_list_use)	
	) a
	left join
		(
		select
			goods_code,goods_name,classify_large_name,classify_middle_name,classify_small_name,business_division_code,business_division_name
		from csx_dim.csx_dim_basic_goods
		where sdt='current'
		) b on b.goods_code=a.goods_code		
	left join
		(
		select
			customer_code,customer_name,performance_region_name,performance_province_name,performance_city_name,first_category_name,second_category_name,third_category_name
		from csx_dim.csx_dim_crm_customer_info
		where sdt='current'
		and shipper_code='YHCSX'
		) c on c.customer_code=a.customer_code
	left join
		(
		select customer_no,customer_large_level,month
		from csx_analyse.csx_analyse_report_sale_customer_level_mf
		where month>='202410' and month<='202512'
			-- and customer_large_level in ('A','B')
			and tag=1 --数据标识：1：全量数据；2：剔除不统计业绩仓数据
		) d on d.customer_no=a.customer_code and d.month=a.smonth
	left join
		(
		select calday,week_of_year
		from csx_dim.csx_dim_basic_date
		) e on e.calday=a.sdt
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
		case 
		when delivery_type_code<>2 then ''
		when delivery_flag=1 then '直送1'
		when delivery_flag=2 then '直送2'
		else '其他' end as direct_delivery_type,
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
	where sdt >=regexp_replace(trunc(add_months(date_format('${sdt_star_date}', 'yyyy-MM-dd'),-6),'MM'),'-','')
	and order_status_code<>-1  -- 订单状态 -1-已取消
	)f on a.sale_order_code=f.order_code and a.goods_code=f.goods_code	
	-- 通过销售表取业务类型和直送类型
	left join 
	(
	select *
	from 
		(
		select 
			-- customer_code,
			-- order_code,    -- 订单编号
			original_order_code,
			goods_code,    -- 商品编码
			goods_name,    -- 商品名称
			business_type_code,    -- 业务类型编码(1.日配业务,2.福利业务,3.批发内购,4.城市服务商,5.省区大宗,6.bbc,7.大宗一部,8.大宗二部,9.商超)
			business_type_name,    -- 业务类型名称
			-- direct_delivery_type   -- 直送类型: 0-p(普通) 1-r(融单)、2-z(过账)、11-(临时加单)、12-(紧急补货)
			delivery_type_name,
			case 
				when delivery_type_name<>'直送' then ''
				when direct_delivery_type=1 then '直送1'
				when direct_delivery_type=2 then '直送2'
				else '其他' end direct_delivery_type,
			row_number() over(partition by order_code,goods_code order by delivery_time asc) as rno
		from csx_dws.csx_dws_sale_detail_di
		where sdt >=regexp_replace(trunc(add_months(date_format('${sdt_star_date}', 'yyyy-MM-dd'),-2),'MM'),'-','')
		-- and refund_order_flag=1 -- 退货单
		and order_channel_code not in(4,5,6) -- '调价返利价格补救'
		and shipper_code='YHCSX'
		)a where rno=1
	)j on a.sale_order_code=j.original_order_code and a.goods_code=j.goods_code				
group by 
	substr(a.sdt,1,6),a.sdt,e.week_of_year,a.performance_region_name,a.performance_province_name,a.customer_code,c.customer_name,c.first_category_name,c.second_category_name,
	coalesce(b.classify_large_name,''),
	coalesce(b.classify_middle_name,''),
	coalesce(b.business_division_name,''),
	a.goods_code,coalesce(b.goods_name,''),
	coalesce(d.customer_large_level,''),
	a.main_category_name,
	a.sub_category_name,
	a.first_level_department_name,
	a.second_level_department_name,
	case 
		when coalesce(f.direct_delivery_type,j.direct_delivery_type) is null then '其他'
		when coalesce(f.direct_delivery_type,j.direct_delivery_type) ='' then '其他'
		else coalesce(f.direct_delivery_type,j.direct_delivery_type) end	
)a 
where direct_delivery_type='' or direct_delivery_type not in('直送1','直送2')	
group by 
	smonth,sdt,week_of_year,performance_region_name,performance_province_name,customer_code,customer_name,first_category_name,
	second_category_name,classify_large_name,classify_middle_name,business_division_name,goods_code,goods_name,customer_large_level,
	main_category_name,sub_category_name,first_level_department_name,second_level_department_name
;

-- 客户
select
	substr(a.sdt,1,6) as smonth,
	a.customer_code,
	f.customer_large_level,
	sum(a.sale_amt) sale_amt
from 
	(
	select
		sdt,customer_code,business_type_code,business_type_name,sale_amt,sale_amt_no_tax,profit,profit_no_tax,credit_code,
		performance_region_name,performance_province_name,performance_city_name
	from csx_dws.csx_dws_sale_detail_di
	where sdt between '20250401' and '20250430' 
	and channel_code in ('1', '7', '9') 
	and business_type_code in (1)
	and shipper_code='YHCSX'
	) a 
	left join
		(
		select customer_no,month,customer_large_level
		from csx_analyse.csx_analyse_report_sale_customer_level_mf
		where month>='202408' and month<='202512'
		and tag=1 --数据标识：1：全量数据；2：剔除不统计业绩仓数据
		group by 
			customer_no,month,customer_large_level
		) f on f.customer_no=a.customer_code and f.month=substr(a.sdt,1,6)
group by 
	substr(a.sdt,1,6),
	a.customer_code,
	f.customer_large_level
;

-- 数据中心 客诉详情

select
	regexp_replace(substr(a.complaint_date_time,1,7),'年','') as smt,
	regexp_replace(regexp_replace(substr(a.complaint_date_time,1,10),'年',''),'月','') as sdt,
	a.performance_region_name,
	a.complaint_code,
	a.performance_province_name,
	a.performance_city_name,
	a.complaint_date_time,
	a.complaint_time_de,
	if(a.delivery_time='1999-01-01 00:00:00','',a.delivery_time) as delivery_time,
	a.delivery_date,
	a.inventory_dc_code,
	a.delivery_type_name,
	a.sales_user_name,
	a.rp_service_user_name_new,
	regexp_replace(regexp_replace(a.customer_code,'\n',''),'\r','') as customer_code,
	a.customer_name,
	a.sub_customer_code,
	regexp_replace(regexp_replace(a.sub_customer_name,'\n',''),'\r','') as sub_customer_name,
	a.sale_order_code,
	a.main_category_name,
	a.sub_category_name,
	a.goods_code,
	a.goods_name,

	concat(cast(a.purchase_qty as decimal(10,1)),a.purchase_unit_name) as purchase_qty, -- 下单数量
	regexp_replace(regexp_replace(a.complaint_describe,'\n',''),'\r','') as complaint_describe,
	a.refund_qty,
	concat(cast(a.complaint_qty as decimal(10,1)),a.unit_name) as complaint_qty,
	a.complaint_amt,
	a.evidence_imgs,
	a.department_name,
	a.department_responsible_user_name,
	regexp_replace(regexp_replace(a.result,'\n',''),'\r','') as `result`,
	a.complaint_deal_time,
	a.processing_time,
	regexp_replace(regexp_replace(a.reason,'\n',''),'\r','') as reason,
	regexp_replace(regexp_replace(a.plan,'\n',''),'\r','') as plan,
	regexp_replace(regexp_replace(a.detail_plan,'\n',''),'\r','') as detail_plan,
	a.is_repeat,
	a.classify_large_name,
	a.classify_middle_name,
	a.classify_small_name,
	a.complaint_status_name,
	a.complaint_deal_status_name,
	a.complaint_node_name,
	a.create_by_user_number,
	a.create_by,
	a.complaint_level_name,

	-- purchase_qty,
	-- purchase_unit_name,
	-- complaint_qty,
	-- unit_name,
	a.strategy_status_name,
	a.strategy_user_name,
	regexp_replace(regexp_replace(a.disagree_reason,'\n',''),'\r','') as disagree_reason,
	a.update_by,
	regexp_replace(regexp_replace(a.cancel_reason,'\n',''),'\r','') as cancel_reason,
	a.complaint_source_name,
	a.refund_code,
	a.replenishment_order_code,
	-- a.customer_large_level_name as customer_large_level,
	b.customer_large_level,
	a.first_person,
	a.hand_person,
	a.end_person,
	a.sdt_refund,
	a.has_goods_name,
	a.reason_detail,
	a.refund_create_by,
	a.stock_process_type,
	a.reason_original,
	a.change_content_after,
	a.change_content_1,
	a.goods_remarks,
	a.supplier_info
	from
	(
		select *
		from  csx_analyse.csx_analyse_fr_oms_complaint_detail_new_di 
		where sdt between '20250401' and '20250430' 
			-- and performance_province_name not in ('东北','平台-B')
			-- and complaint_status_code in(20,30) -- 客诉单状态 10'待判责' 20'处理中' 21'待审核' 30'已完成' -1'已取消'
			-- and complaint_deal_status in(10,40) -- 责任环节状态 10'待处理' 20'待修改' 30'已处理待审' 31'已驳回待审核' 40'已完成' -1'已取消'
			-- and complaint_source = 1  --客诉来源:1-单独发起客诉单 2-客退单生成 3-补货单生成
			-- ★and main_category_name<>'送货后调整数量'
			and complaint_code in( select complaint_code from csx_analyse_tmp.complaint_code_list_use)
	)a 
	-- 客户等级取上月
	left join
	(
		select customer_no,customer_large_level,month
		from csx_analyse.csx_analyse_report_sale_customer_level_mf
		-- where month =substr(regexp_replace(add_months(date_sub(current_date,1),-1),'-',''),1,6)
		where tag=1 --数据标识：1：全量数据；2：剔除不统计业绩仓数据
	)b on a.customer_code=b.customer_no and substr(a.sdt,1,6)=b.month;









