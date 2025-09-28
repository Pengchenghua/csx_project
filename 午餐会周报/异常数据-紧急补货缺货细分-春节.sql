
 -- 线上表客户定价类型
drop table if exists csx_analyse_tmp.tmp_customer_price_type_business;
create table csx_analyse_tmp.tmp_customer_price_type_business as 
select 
	t1.*,
	case t1.price_type
		when '客户定价->合同固定价' then '客户定价'
		when '客户定价->单独议价' then '客户定价'
		when '对标定价->全对标' then '对标定价'
		when '' then '自主定价'
		when null then '自主定价'
		when '空' then '自主定价'
		when '自主定价->采购或车间定价' then '自主定价'
		when '临时报价->单品项' then '临时定价'
		when '临时报价->下单时' then '临时定价'
		when '客户定价->多方比价' then '客户定价'
		when '对标定价->半对标' then '对标定价'
		when '自主定价->建议售价' then '自主定价'
		end as price_type1,	
	case t1.price_type
		when '客户定价->合同固定价' then '合同固定价'
		when '客户定价->单独议价' then '单独议价'
		when '对标定价->全对标' then '全对标'
		when '' then '建议售价'
		when null then '建议售价'
		when '空' then '建议售价'
		when '自主定价->采购或车间定价' then '建议售价'   
		when '临时报价->单品项' then '临时定价'
		when '临时报价->下单时' then '临时定价'
		when '客户定价->多方比价' then '多方比价'
		when '对标定价->半对标' then '半对标'
		when '自主定价->建议售价' then '建议售价'
	end as price_type2 
from 
(select 
    customer_code,
    (case when a.price_period_code=1 then '每日' 
          when a.price_period_code=2 then '每周' 
          when a.price_period_code=3 then '每半月' 
          when a.price_period_code=4 then '每月' end) as price_period_name,-- 报价周期 
    price_date_name,
	concat(a.price_set_type_first,'->',a.price_set_type_sec) as price_type -- 定价类型 
from 
    (select 
        *,
        (case when split(price_set_type,',')[0]='1' then '对标定价' 
              when split(price_set_type,',')[0]='4' then '客户定价' 
              when split(price_set_type,',')[0]='8' then '自主定价' 
              when split(price_set_type,',')[0]='11' then '临时报价' 
              when split(price_set_type,',')[0]='2' then '全对标' 
              when split(price_set_type,',')[0]='3' then '半对标' 
              when split(price_set_type,',')[0]='5' then '合同固定价' 
              when split(price_set_type,',')[0]='6' then '多方比价' 
              when split(price_set_type,',')[0]='7' then '单独议价' 
              when split(price_set_type,',')[0]='9' then '采购或车间定价' 
              when split(price_set_type,',')[0]='10' then '建议售价' 
              when split(price_set_type,',')[0]='12' then '下单时' 
              when split(price_set_type,',')[0]='13' then '单品项' 
        end) as price_set_type_first,
        (case when split(price_set_type,',')[1]='1' then '对标定价' 
              when split(price_set_type,',')[1]='4' then '客户定价' 
              when split(price_set_type,',')[1]='8' then '自主定价' 
              when split(price_set_type,',')[1]='11' then '临时报价' 
              when split(price_set_type,',')[1]='2' then '全对标' 
              when split(price_set_type,',')[1]='3' then '半对标' 
              when split(price_set_type,',')[1]='5' then '合同固定价' 
              when split(price_set_type,',')[1]='6' then '多方比价' 
              when split(price_set_type,',')[1]='7' then '单独议价' 
              when split(price_set_type,',')[1]='9' then '采购或车间定价' 
              when split(price_set_type,',')[1]='10' then '建议售价' 
              when split(price_set_type,',')[1]='12' then '下单时' 
              when split(price_set_type,',')[1]='13' then '单品项' 
        end) as price_set_type_sec, 
        row_number()over(partition by customer_code order by business_attribute_code asc,business_number desc) as ranks 
    from csx_dim.csx_dim_crm_business_info 
    where sdt='current' 
    and business_attribute_code in(1,2,5)  -- 日配福利BBC
    and status=1 
    -- and sign_type_code=1 
    )a 
where a.ranks=1
) t1  
;



-- 日配异常分析 
-- drop table csx_analyse_tmp.csx_analyse_tmp_sale_detail_di_yc; 
create table csx_analyse_tmp.csx_analyse_tmp_sale_detail_di_yc as 
select 
	a.smonth,
	a.week,
	a.sdt,
	a.original_order_code,
	a.order_code,
	a.performance_region_name,
	a.performance_province_name,	
	a.performance_city_name,
	a.business_type_name,
	a.inventory_dc_code,
	d.first_category_name,
	d.second_category_name,
	d.third_category_name,
	a.customer_code,
	d.customer_name,
	a.sub_customer_code,
	a.sub_customer_name,
	nvl(f.fir_price_type,e.price_type1) as price_type1,
	nvl(f.sec_price_type,e.price_type2) as price_type2,
	e.price_period_name,
	e.price_date_name,
	c.classify_large_name,
	c.classify_middle_name,
	c.classify_small_name,
	a.goods_code,
	c.goods_name,
	case when a.delivery_type_code=2 then '直送单' end as is_zs,
	case when a.order_channel_code=6 then '调价单' end as is_tj,
	case when a.order_channel_code=4 then '返利单' end as is_fl,
	-- 退货异常只看退货中“子退货单类型 ：1-子退货单逆向” 且 “订单来源字段 不是 改单退货的”
	case when a.refund_order_flag=1 and h.source_type_name is not null then '退货单' end as is_th,	
	-- case when a.order_channel_detail_code=26 then '价格补救单' end as is_jgbj, 
	case 
		when a.delivery_type_name<>'直送' then ''
		when a.direct_delivery_type=1 then 'R直送1'
		when a.direct_delivery_type=2 then 'Z直送2'
		when a.direct_delivery_type=11 then '临时加单'
		when a.direct_delivery_type=12 then '紧急补货'
		when a.direct_delivery_type=0 then '普通' else '普通' end direct_delivery_type,
	a.sale_amt,
	a.profit,
	-- a.sale_qty,
	if(a.order_channel_detail_code=26,0,a.sale_qty) sale_qty,
	if(a.delivery_type_name='直送' and a.direct_delivery_type=12 and a.order_channel_code not in(4,6) and a.refund_order_flag=0,k.purchase_qty,0) as purchase_qty,    -- 购买数量
	if(a.delivery_type_name='直送' and a.direct_delivery_type=12 and a.order_channel_code not in(4,6) and a.refund_order_flag=0,k.send_qty,0) as send_qty,    -- 发货数量(基础单位)
	if(a.delivery_type_name='直送' and a.direct_delivery_type=12 and a.order_channel_code not in(4,6) and a.refund_order_flag=0,k.delivery_qty,0) as delivery_qty,    -- 送达数量
	if(a.delivery_type_name='直送' and a.direct_delivery_type=12 and a.order_channel_code not in(4,6) and a.refund_order_flag=0,k.sign_qty,0) as sign_qty,    -- 签收数量	
	-- sum(case when weeks='本周' then a.sale_amt end ) as sale_amt,
	-- sum(case when weeks='本周' then a.profit   end ) as profit,
	-- sum(case when weeks='本周' then a.sale_qty end ) as sale_qty ,
	-- sum(case when weeks='上周' then a.sale_amt end ) as last_sale_amt,
	-- sum(case when weeks='上周' then a.profit   end ) as last_profit,
	-- sum(case when weeks='上周' then a.sale_qty end ) as last_sale_qty 
	
	-- 调价
	g.adjust_reason,
	
	-- 退货
	h.refund_code,
	-- h.sdt,  -- 退货申请日期
	h.source_biz_type_name,  -- 订单业务来源
	h.has_goods_name,
	h.child_return_type_name,  -- 子退货单类型 ：0-父退货单 1-子退货单逆向 2-子退货单正向
	h.refund_order_type_name,	-- 退货单类型(0:差异单 1:退货单）
	h.refund_reason,
	h.first_level_reason_name,
	h.second_level_reason_name,
	
	
	-- 紧急补货
	j.replenishment_relation_order_code,
	i.replenishment_order_code,
	i.scm_source_type,     -- 来源类型 0-缺货 1-客诉 2-签收差异	3-订单补货 4-临时加单
	-- 对比紧急补货原单的下单数量和发货数量，判断是否发车后缺货
	case when a.direct_delivery_type=12 and a.order_channel_code not in(4,6) and a.refund_order_flag=0 and i.scm_source_type='缺货' and k.send_qty=0 then '缺货'
		 when a.direct_delivery_type=12 and a.order_channel_code not in(4,6) and a.refund_order_flag=0 and i.scm_source_type='缺货' and k.send_qty>0 then '缺货(差异)'
		 else i.scm_source_type end as scm_source_type_new,
	i.reason,     -- 补货原因
	if(g2.smonth=a.smonth,1,0) is_same_month,
	if(g2.week=a.week,1,0) is_same_week
from 
(
	select * ,
		substr(sdt,1,6) smonth,
		weekofyear(date_sub(to_date(from_unixtime(unix_timestamp(sdt,'yyyyMMdd'))),-2)) week,
		if((order_channel_code in ('4','5','6') or refund_order_flag=1),original_order_code,order_code) as order_code_new
	from csx_dws.csx_dws_sale_detail_di 
	where sdt >=regexp_replace(add_months(trunc('${i_sdate}','MM'),-1),'-','') 
	and sdt <= regexp_replace(add_months(date_sub(current_date,1),0),'-','') 
	and business_type_code in ('1') 
	and (delivery_type_code=2 -- '直送单'
	or order_channel_code=6 -- '调价单'
	or order_channel_code=4 -- '返利单'
	or refund_order_flag=1 -- '退货单'
	)
	and inventory_dc_code not in  ('W0J2','W0AJ','W0G6','WB71')
) a 
join 
(
	select * 
	from csx_dim.csx_dim_shop  
	where sdt='current' 
	and shop_low_profit_flag=0 
) b on a.inventory_dc_code=b.shop_code 
left join 
(
	select * 
	from csx_dim.csx_dim_basic_goods 
	where sdt='current' 
) c on a.goods_code=c.goods_code 
left join 
(
	select * 
	from csx_dim.csx_dim_crm_customer_info 
	where sdt='current'
) d on a.customer_code=d.customer_code 
 -- 线上表客户定价类型
left join csx_analyse_tmp.tmp_customer_price_type_business e on a.customer_code=e.customer_code 
-- 线下表客户定价类型
left join csx_ods.csx_ods_data_analysis_prd_cus_price_type_231206_df f on a.customer_code=f.customer_code 
-- 调价单原单日期
left join 
(
	select order_code,goods_code,
		substr(max_sdt,1,6) smonth,
		weekofyear(date_sub(to_date(from_unixtime(unix_timestamp(max_sdt,'yyyyMMdd'))),-2)) week
	from 
	(
		select distinct order_code,goods_code,max(sdt) max_sdt
			-- if((order_channel_code in ('4','5','6') or refund_order_flag=1),original_order_code,order_code) as order_code_new
		from csx_dws.csx_dws_sale_detail_di 
		where sdt >=regexp_replace(add_months(trunc('${i_sdate}','MM'),-1),'-','') 
		and sdt <= regexp_replace(add_months(date_sub(current_date,1),0),'-','') 
		and business_type_code in ('1') 
		-- and order_channel_code=6 -- '调价单'
		and inventory_dc_code not in  ('W0J2','W0AJ','W0G6','WB71')
		group by order_code,goods_code
	)a
) g2 on g2.order_code=a.original_order_code and g2.goods_code=a.goods_code and a.order_channel_code=6
-- 调价类型	
left join	
(
	select * 
	from 
	(
	select 
		original_order_code,
		adjust_price_order_code,
		product_code,
		-- a.adjusted_total_amount,
		(case when adjust_reason_code='10' then '报价错误-报价失误' 
				when adjust_reason_code='11' then '报价错误-报价客户不认可' 
				when adjust_reason_code='20' then '客户对账差异-税率调整' 
				when adjust_reason_code='21' then '客户对账差异-其他' 
				when adjust_reason_code='30' then '后端履约问题-商品等级/规格未达要求' 
				when adjust_reason_code='31' then '后端履约问题-商品质量问题折扣处理' 
				when adjust_reason_code='32' then '后端履约问题-其他' 
				when adjust_reason_code='40' then '发货后报价类型' 
				when adjust_reason_code='50' then '无原单退款' 
				when adjust_reason_code='60' then '其他' 
				when adjust_reason_code='70' then '单据超90天未处理' end) as adjust_reason,
		row_number() over(partition by adjust_price_order_code,product_code order by update_time desc,adjusted_total_amount desc) as rno 				
	from csx_dwd.csx_dwd_sss_customer_credit_adjust_price_item_di 
	where sdt>='20231001'
 	)a
	where rno=1
)g on a.order_code=g.adjust_price_order_code and a.goods_code=g.product_code
-- 退货类型	
left join	
(
select *
from 
(
	select row_number() over(partition by sale_order_code,goods_code order by refund_total_amt desc) as rno,
		inventory_dc_code,
		inventory_dc_name, 
		sdt,	
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
		case source_type
		when 0 then '签收差异或退货'
		when 1 then '改单退货'
		end as source_type_name,  -- 订单来源(0-签收差异或退货 1-改单退货)		
		responsibility_reason,
		regexp_replace(regexp_replace(reason_detail,'\n',''),'\r','') as reason_detail,
		case child_return_type_code
		when 0 then '父退货单'
		when 1 then '子退货单逆向'
		when 2 then '子退货单正向'
		end as child_return_type_name,  -- 子退货单类型 ：0-父退货单 1-子退货单逆向 2-子退货单正向
		case refund_order_type_code
		when 0 then '差异单'
		when 1 then '退货单'
		end as refund_order_type_name,	-- 退货单类型(0:差异单 1:退货单）
		-- refund_qty,
		-- sale_price,
		-- refund_total_amt,
		-- refund_scale_total_amt,
		refund_reason,
		first_level_reason_name,
		regexp_replace(regexp_replace(second_level_reason_name,'\n',''),'\r','') as second_level_reason_name
	from csx_dwd.csx_dwd_oms_sale_refund_order_detail_di
	where sdt>='20231001'
	and child_return_type_code in(1)  -- 子退货单类型 ：0-父退货单 1-子退货单逆向 2-子退货单正向
	and parent_refund_code<>''
	and source_type=0   -- 订单来源(0-签收差异或退货 1-改单退货)	
 	)a
	where rno=1	
)h on a.original_order_code=h.sale_order_code and a.goods_code=h.goods_code
-- 紧急补货类型	
left join
(
select a.*
from 
(
	select 
		e.replenishment_order_code,e.apply_code,e.goods_code,e.replace_goods_code,
		e.scm_source_type,f.sale_order_code,e.reason,
		row_number() over(partition by f.sale_order_code,e.goods_code order by e.update_time desc) as rno
	from 
	(
		select 
			replenishment_order_code,     -- 补货单号
			apply_code,    -- 申请补货订单
			case scm_source_type
			when 0 then '缺货'
			when 1 then '客诉'
			when 2 then '签收差异'
			when 3 then '订单补货'
			when 4 then '临时加单'
			else scm_source_type end as scm_source_type, -- 来源类型 0-缺货 1-客诉 2-签收差异 3-订单补货 4-临时加单
			customer_code,
			goods_code,replace_goods_code,
			regexp_replace(regexp_replace(reason,'\n',''),'\r','') as reason,
			update_time
			-- row_number() over(partition by apply_code,goods_code order by update_time desc) as rno		
		from csx_dwd.csx_dwd_oms_emergency_replenishment_detail_df
		where item_create_time>='2023-01-01'  --无分区
		and (coalesce(cancel_flag,0)<>1 or cancel_flag is NULL or cancel_flag is null)	
	)e 
	left join  -- 补货单关联销售单
	(
		select replenishment_order_code,sale_order_code
		from csx_dwd.csx_dwd_oms_emergency_sale_order_relation_df
	)f on e.replenishment_order_code =f.replenishment_order_code
)a	
where a.rno=1		
)i on i.sale_order_code= a.order_code_new -- 补货之后的单子
      -- i.sale_order_code= a.order_code -- 补货之后的单子
	  -- i.apply_code =a.original_order_code  -- 补货最早单子
and i.goods_code =a.goods_code
-- where b.shop_code is null

-- 紧急补货的原单
left join 
(
  select 
	replenishment_relation_order_code,
	order_code,    -- 订单编号
	goods_code
  from csx_dwd.csx_dwd_oms_sale_order_detail_di
  where sdt >=regexp_replace(add_months(trunc('${i_sdate}','MM'),-1),'-','') 
    and order_status_code <> -1  -- 订单状态 -1-已取消
	and replenishment_relation_order_code <> ''
	-- group by replenishment_relation_order_code,order_code,goods_code
) j on j.order_code=a.order_code and j.goods_code=a.goods_code 
-- 紧急补货的原单对应的下单 发货 送达数量
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
	case delivery_type_code
	when 1 then '配送'
	when 2 then '直送'
	when 3 then '自提' end as delivery_type_name,    -- 配送类型编码：1-配送 2-直送 3-自提
	order_business_type_code,    -- 订单业务类型: 1-日配 2-福利 3-大宗贸易 4-内购
	case order_business_type_code
	when 1 then '日配'
	when 2 then '福利'
	when 3 then '大宗贸易'
	when 4 then '内购'
	end as order_business_type_name,
    order_status_code,  -- 订单状态: 10-待接单  20-待发货  30-部分发货  40-配送中  50-待确认 60-已签收  70-已完成  -1-已取消
    delivery_type_code,  -- 配送类型编码：1-配送 2-直送 3-自提
	sale_price*purchase_qty*purchase_unit_rate purchase_amt,    -- 购买金额 
    -- regexp_replace(substr(order_time, 1, 10), '-', '') as order_date,
    -- regexp_replace(substr(delivery_time, 1, 10), '-', '') as delivery_date,
    -- regexp_replace(substr(sign_time, 1, 10), '-', '') as sign_date,
	goods_code,    -- 商品编码
	goods_name,    -- 商品名称
	purchase_qty*purchase_unit_rate as purchase_qty,    -- 购买数量
	send_qty,    -- 发货数量(基础单位)
	-- sale_unit_send_qty,    -- 发货数量(销售单位)
	delivery_qty,    -- 送达数量
	sign_qty,    -- 签收数量
    require_delivery_date,  -- 要求送货日期
	tms_sign_flag_code,  -- 0-中台签收 1-tms签收
    sdt
  from csx_dwd.csx_dwd_oms_sale_order_detail_di
  where sdt >=regexp_replace(add_months(trunc('${i_sdate}','MM'),-1),'-','') 
    and order_status_code <> -1  -- 订单状态 -1-已取消
) k on j.replenishment_relation_order_code=k.order_code and j.goods_code=k.goods_code
;



-- 异常数据 仅异常
-- drop table csx_analyse_tmp.csx_analyse_tmp_sale_yc; 
create table csx_analyse_tmp.csx_analyse_tmp_sale_yc as 
select 
	performance_region_name,
	performance_province_name,
	performance_city_name,
	business_type_name,
	-- inventory_dc_code,
	first_category_name,
	second_category_name,
	-- third_category_name,
	customer_code,
	customer_name,
	sub_customer_code,
	sub_customer_name,	
	price_type1,
	price_type2,
	price_period_name,
	price_date_name,
	classify_large_name,
	classify_middle_name,
	classify_small_name,
	goods_code,
	goods_name,
	is_zs,
	is_tj,
	is_fl,
	is_th,
	direct_delivery_type,
	-- 调价
	adjust_reason,
	-- 退货
	has_goods_name,
	child_return_type_name,  -- 子退货单类型
	refund_order_type_name,	-- 退货单类型(0:差异单 1:退货单）
	refund_reason,
	first_level_reason_name,
	second_level_reason_name,	
	-- 紧急补货
	scm_source_type,     -- 来源类型 0-缺货 1-客诉 2-签收差异
	scm_source_type_new,	
	reason,     -- 补货原因		
	sum(case when a.sdt >= regexp_replace(trunc('${i_sdate}','MM'),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',0),'-','') then a.sale_amt end) by_sale_amt,
	sum(case when a.sdt >= regexp_replace(trunc('${i_sdate}','MM'),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',0),'-','') then a.sale_qty end) by_sale_qty,
	sum(case when a.sdt >= regexp_replace(trunc('${i_sdate}','MM'),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',0),'-','') then a.profit end) by_profit,
	sum(case when a.sdt >= regexp_replace(trunc('${i_sdate}','MM'),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',0),'-','') then a.purchase_qty end) by_purchase_qty, -- 购买数量
	sum(case when a.sdt >= regexp_replace(trunc('${i_sdate}','MM'),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',0),'-','') then a.send_qty end) by_send_qty,    -- 发货数量(基础单位)
	sum(case when a.sdt >= regexp_replace(trunc('${i_sdate}','MM'),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',0),'-','') then a.delivery_qty end) by_delivery_qty,    -- 送达数量
	sum(case when a.sdt >= regexp_replace(trunc('${i_sdate}','MM'),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',0),'-','') then a.sign_qty end) by_sign_qty,    -- 签收数量		
	
	sum(case when a.sdt >= regexp_replace(add_months(trunc('${i_sdate}','MM'),-1),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',-1),'-','') then a.sale_amt end) sy_sale_amt,
	sum(case when a.sdt >= regexp_replace(add_months(trunc('${i_sdate}','MM'),-1),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',-1),'-','') then a.sale_qty end) sy_sale_qty,
	sum(case when a.sdt >= regexp_replace(add_months(trunc('${i_sdate}','MM'),-1),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',-1),'-','') then a.profit end) sy_profit,	
	sum(case when a.sdt >= regexp_replace(add_months(trunc('${i_sdate}','MM'),-1),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',-1),'-','') then a.purchase_qty end) sy_purchase_qty, -- 购买数量
	sum(case when a.sdt >= regexp_replace(add_months(trunc('${i_sdate}','MM'),-1),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',-1),'-','') then a.send_qty end) sy_send_qty,    -- 发货数量(基础单位)
	sum(case when a.sdt >= regexp_replace(add_months(trunc('${i_sdate}','MM'),-1),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',-1),'-','') then a.delivery_qty end) sy_delivery_qty,    -- 送达数量
	sum(case when a.sdt >= regexp_replace(add_months(trunc('${i_sdate}','MM'),-1),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',-1),'-','') then a.sign_qty end) sy_sign_qty,    -- 签收数量
	
	sum(case when a.sdt between '20240209' and '20240217' then a.sale_amt end) bz_sale_amt,
	sum(case when a.sdt between '20240209' and '20240217' then a.sale_qty end) bz_sale_qty,
	sum(case when a.sdt between '20240209' and '20240217' then a.profit end) bz_profit,
	sum(case when a.sdt between '20240209' and '20240217' then a.purchase_qty end) bz_purchase_qty, -- 购买数量
	sum(case when a.sdt between '20240209' and '20240217' then a.send_qty end) bz_send_qty,    -- 发货数量(基础单位)
	sum(case when a.sdt between '20240209' and '20240217' then a.delivery_qty end) bz_delivery_qty,    -- 送达数量
	sum(case when a.sdt between '20240209' and '20240217' then a.sign_qty end) bz_sign_qty,    -- 签收数量
	
	sum(case when a.sdt between '20240131' and '20240208' then a.sale_amt end) sz_sale_amt,
	sum(case when a.sdt between '20240131' and '20240208' then a.sale_qty end) sz_sale_qty,
	sum(case when a.sdt between '20240131' and '20240208' then a.profit end) sz_profit,
	sum(case when a.sdt between '20240131' and '20240208' then a.purchase_qty end) sz_purchase_qty, -- 购买数量
	sum(case when a.sdt between '20240131' and '20240208' then a.send_qty end) sz_send_qty,    -- 发货数量(基础单位)
	sum(case when a.sdt between '20240131' and '20240208' then a.delivery_qty end) sz_delivery_qty,    -- 送达数量
	sum(case when a.sdt between '20240131' and '20240208' then a.sign_qty end) sz_sign_qty,    -- 签收数量
	
	is_same_month,
	is_same_week,
	coalesce(is_tj,is_fl,is_th,is_zs) yc_flag
from csx_analyse_tmp.csx_analyse_tmp_sale_detail_di_yc a
where coalesce(is_tj,is_fl,is_th,is_zs) is not null
group by 
	performance_region_name,
	performance_province_name,
	performance_city_name,
	business_type_name,
	-- inventory_dc_code,
	first_category_name,
	second_category_name,
	-- third_category_name,
	customer_code,
	customer_name,
	sub_customer_code,
	sub_customer_name,		
	price_type1,
	price_type2,
	price_period_name,
	price_date_name,
	classify_large_name,
	classify_middle_name,
	classify_small_name,
	goods_code,
	goods_name,
	is_zs,
	is_tj,
	is_fl,
	is_th,
	direct_delivery_type,
	-- 调价
	adjust_reason,
	-- 退货
	has_goods_name,
	child_return_type_name,  -- 子退货单类型
	refund_order_type_name,	-- 退货单类型(0:差异单 1:退货单）
	refund_reason,
	first_level_reason_name,
	second_level_reason_name,	
	-- 紧急补货
	scm_source_type,     -- 来源类型 0-缺货 1-客诉 2-签收差异
	scm_source_type_new,	
	reason,
	is_same_month,
	is_same_week,	
	coalesce(is_tj,is_fl,is_th,is_zs)	
having by_sale_amt is not null or sy_sale_amt is not null or bz_sale_amt is not null or sz_sale_amt is not null
;


-- 结果表1：异常明细数据
select *
from csx_analyse_tmp.csx_analyse_tmp_sale_yc;


-- 结果表2：缺货：各省区春节（本周）紧急补货各省区缺货TOP3品类的金额、毛利率、top小类

-- drop table csx_analyse_tmp.sale_detail_yc_top_middle_list;
create table csx_analyse_tmp.sale_detail_yc_top_middle_list
as
with 
sale_detail_yc as -- 本周紧急补货-缺货明细
(
  select *, case when performance_province_name='上海' then performance_city_name else performance_province_name end as performance_province_name_new
  from csx_analyse_tmp.csx_analyse_tmp_sale_yc
  where yc_flag='直送单'
  and direct_delivery_type='紧急补货'
  and bz_sale_amt is not null
  and scm_source_type_new='缺货'
),

sale_detail_yc_top_middle as -- 省区缺货品类排名
(
select
	performance_region_name,
	performance_province_name_new,	
	-- performance_city_name,    
	classify_large_name,
	classify_middle_name,
  --  总销量
  sum(bz_sale_qty) as bz_sale_qty,
  --  总毛利额
  sum(bz_profit) as bz_profit,
  --  总销售额
  sum(bz_sale_amt) as bz_sale_amt,
  sum(bz_profit)/abs(sum(bz_sale_amt)) as bz_profit_rate,
  --  省区品类业绩排名
  row_number() over(partition by performance_province_name_new order by sum(bz_sale_amt) desc) as rno1
from 
(
	select 	
		performance_region_name,
		performance_province_name_new,	
		performance_city_name,    
		classify_large_name,
		classify_middle_name,
		classify_small_name,
		bz_sale_qty,
		bz_profit,
		bz_sale_amt
	from sale_detail_yc
	union all 
	select 	
		'全国' as performance_region_name,
		'全国' as performance_province_name_new,	
		'全国' as performance_city_name,    
		classify_large_name,
		classify_middle_name,
		classify_small_name,
		bz_sale_qty,
		bz_profit,
		bz_sale_amt
	from sale_detail_yc
)a
group by performance_region_name,
	performance_province_name_new,	
	-- performance_city_name,    
	classify_large_name,
	classify_middle_name
),
sale_detail_yc_top_small as -- 省区缺货小类排名
(
select
	performance_region_name,
	performance_province_name_new,	
	-- performance_city_name,    
	classify_large_name,
	classify_middle_name,
	classify_small_name,
  --  总销量
  sum(bz_sale_qty) as bz_sale_qty,
  --  总毛利额
  sum(bz_profit) as bz_profit,
  --  总销售额
  sum(bz_sale_amt) as bz_sale_amt,
  sum(bz_profit)/abs(sum(bz_sale_amt)) as bz_profit_rate,
  --  省区品类业绩排名
  row_number() over(partition by performance_province_name_new,classify_middle_name order by sum(bz_sale_amt) desc) as rno2
from 
(
	select 	
		performance_region_name,
		performance_province_name_new,	
		performance_city_name,    
		classify_large_name,
		classify_middle_name,
		classify_small_name,
		bz_sale_qty,
		bz_profit,
		bz_sale_amt
	from sale_detail_yc
	union all 
	select 	
		'全国' as performance_region_name,
		'全国' as performance_province_name_new,	
		'全国' as performance_city_name,    
		classify_large_name,
		classify_middle_name,
		classify_small_name,
		bz_sale_qty,
		bz_profit,
		bz_sale_amt
	from sale_detail_yc
)a
group by performance_region_name,
	performance_province_name_new,	
	-- performance_city_name,    
	classify_large_name,
	classify_middle_name,
	classify_small_name
),

sale_detail_yc_top_small_list as -- 省区缺货小类list
(
select 
	performance_region_name,
	performance_province_name_new,
	classify_large_name,
	classify_middle_name,
concat_ws('；',collect_list(concat(classify_small_name,'',round(bz_sale_amt,0),'元'))) as class_list
from sale_detail_yc_top_small
where rno2<=3
and bz_sale_amt>=50
group by 
	performance_region_name,
	performance_province_name_new,
	classify_large_name,
	classify_middle_name
)

select 
	a.performance_region_name,
	a.performance_province_name_new,	  
	a.classify_middle_name as classify_middle_name_1,
	a.bz_sale_amt as bz_sale_amt_1,
	a.bz_profit_rate as bz_profit_rate_1,
	d1.class_list as class_list_1,
	
	b.classify_middle_name as classify_middle_name_2,
	b.bz_sale_amt as bz_sale_amt_2,
	b.bz_profit_rate as bz_profit_rate_2,	
	d2.class_list as class_list_2,

	c.classify_middle_name as classify_middle_name_3,
	c.bz_sale_amt as bz_sale_amt_3,
	c.bz_profit_rate as bz_profit_rate_3,
	d3.class_list as class_list_3
	
from 
(
select *
from sale_detail_yc_top_middle
where rno1=1
)a 
left join 
(
select *
from sale_detail_yc_top_middle
where rno1=2
)b on a.performance_province_name_new=b.performance_province_name_new
left join 
(
select *
from sale_detail_yc_top_middle
where rno1=3
)c on a.performance_province_name_new=c.performance_province_name_new
left join sale_detail_yc_top_small_list d1 on d1.performance_province_name_new=a.performance_province_name_new and d1.classify_middle_name=a.classify_middle_name
left join sale_detail_yc_top_small_list d2 on d2.performance_province_name_new=b.performance_province_name_new and d2.classify_middle_name=b.classify_middle_name
left join sale_detail_yc_top_small_list d3 on d3.performance_province_name_new=c.performance_province_name_new and d3.classify_middle_name=c.classify_middle_name


-- 缺货
select *
from csx_analyse_tmp.sale_detail_yc_top_middle_list








/*
select *
from csx_analyse_tmp.csx_analyse_tmp_sale_yc
where yc_flag='直送单'
and direct_delivery_type='紧急补货'
and (bz_sale_amt is not null or sz_sale_amt is not null)


  select 
	replenishment_relation_order_code,
	order_code,    -- 订单编号
	goods_code
  from csx_dwd.csx_dwd_oms_sale_order_detail_di
  where sdt >=regexp_replace(add_months(trunc('${i_sdate}','MM'),-1),'-','') 
    and order_status_code <> -1  -- 订单状态 -1-已取消
	and replenishment_relation_order_code <> ''
	and order_code='OM24010800005805'
	
	
	
select	
	purchase_qty*purchase_unit_rate as purchase_qty,    -- 购买数量
	send_qty,    -- 发货数量(基础单位)
	-- sale_unit_send_qty,    -- 发货数量(销售单位)
	delivery_qty,    -- 送达数量
	sign_qty,    -- 签收数量
	*
  from csx_dwd.csx_dwd_oms_sale_order_detail_di
  where sdt >=regexp_replace(add_months(trunc('${i_sdate}','MM'),-1),'-','') 	
  and order_code='OM24010700002405'
  and goods_code='1180403'
  
  
  

-- 异常明细（客户商品）+普通 数据源
with 
no_yc as
(
select 
	performance_region_name,
	performance_province_name,
	performance_city_name,
	business_type_name,
	-- inventory_dc_code,
	'' as first_category_name,
	'' as second_category_name,
	-- third_category_name,
	'' as customer_code,
	'' as customer_name,
	'' as sub_customer_code,
	'' as sub_customer_name,		
	'' as price_type1,
	'' as price_type2,
	'' as price_period_name,
	'' as price_date_name,
	classify_large_name,
	classify_middle_name,
	classify_small_name,
	goods_code,
	goods_name,
	'' as is_zs,
	'' as is_tj,
	'' as is_fl,
	'' as is_th,
	'' as direct_delivery_type,
	-- 调价
	'' as adjust_reason,
	-- 退货
	'' as has_goods_name,
	'' as child_return_type_name,  -- 子退货单类型
	'' as refund_order_type_name,	-- 退货单类型(0:差异单 1:退货单）
	'' as refund_reason,
	'' as first_level_reason_name,
	'' as second_level_reason_name,	
	-- 紧急补货
	'' as scm_source_type,     -- 来源类型 0-缺货 1-客诉 2-签收差异	
	'' as reason,	
	sum(case when a.sdt >= regexp_replace(trunc('${i_sdate}','MM'),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',0),'-','') then a.sale_amt end) by_sale_amt,
	sum(case when a.sdt >= regexp_replace(trunc('${i_sdate}','MM'),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',0),'-','') then a.sale_qty end) by_sale_qty,
	sum(case when a.sdt >= regexp_replace(trunc('${i_sdate}','MM'),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',0),'-','') then a.profit end) by_profit,	
	
	sum(case when a.sdt >= regexp_replace(add_months(trunc('${i_sdate}','MM'),-1),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',-1),'-','') then a.sale_amt end) sy_sale_amt,
	sum(case when a.sdt >= regexp_replace(add_months(trunc('${i_sdate}','MM'),-1),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',-1),'-','') then a.sale_qty end) sy_sale_qty,
	sum(case when a.sdt >= regexp_replace(add_months(trunc('${i_sdate}','MM'),-1),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',-1),'-','') then a.profit end) sy_profit,	
	
	sum(case when a.week=weekofyear(date_sub(current_date, 3)) then a.sale_amt end) bz_sale_amt,
	sum(case when a.week=weekofyear(date_sub(current_date, 3)) then a.sale_qty end) bz_sale_qty,
	sum(case when a.week=weekofyear(date_sub(current_date, 3)) then a.profit end) bz_profit,
	
	sum(case when a.week=weekofyear(date_sub(current_date, 10)) then a.sale_amt end) sz_sale_amt,
	sum(case when a.week=weekofyear(date_sub(current_date, 10)) then a.sale_qty end) sz_sale_qty,
	sum(case when a.week=weekofyear(date_sub(current_date, 10)) then a.profit end) sz_profit,
	0 as is_same_month,
	0 as is_same_week,
	'普通' as yc_flag
from 
(
	select * ,
		substr(sdt,1,6) smonth,
		weekofyear(date_sub(to_date(from_unixtime(unix_timestamp(sdt,'yyyyMMdd'))),-2)) week,
		if((order_channel_code in ('4','5','6') or refund_order_flag=1),original_order_code,order_code) as order_code_new
	from csx_dws.csx_dws_sale_detail_di 
	where sdt >=regexp_replace(add_months(trunc('${i_sdate}','MM'),-1),'-','') 
	and sdt <= regexp_replace(add_months(date_sub(current_date,1),0),'-','') 
	and business_type_code in ('1') 
	and delivery_type_code not in(2) -- '直送单'
	and order_channel_code not in(6) -- '调价单'
	and order_channel_code not in(4) -- '返利单'
	and refund_order_flag not in(1) -- '退货单'
	and inventory_dc_code not in  ('W0J2','W0AJ','W0G6','WB71')
) a 
join 
(
	select * 
	from csx_dim.csx_dim_shop  
	where sdt='current' 
	and shop_low_profit_flag=0 
) b on a.inventory_dc_code=b.shop_code 
group by 
	performance_region_name,
	performance_province_name,
	performance_city_name,
	business_type_name,
	classify_large_name,
	classify_middle_name,
	classify_small_name,
	goods_code,
	goods_name
having by_sale_amt is not null or sy_sale_amt is not null or bz_sale_amt is not null or sz_sale_amt is not null
)

-- 结果表：异常明细数据导出（含非异常作为日配整体）
select *
from csx_analyse_tmp.csx_analyse_tmp_sale_yc
union all
select *
from no_yc
;
*/




-- 异常订单明细
select *
from csx_analyse_tmp.csx_analyse_tmp_sale_detail_di_yc
where substr(sdt,1,6)='202401'
and performance_province_name='北京市'
and direct_delivery_type='紧急补货'




--日配剔除直送仓、剔除监狱海军仓 整体数据
select 
	a. performance_region_name,
	a.performance_province_name,
	a.performance_city_name,
	sum(case when a.sdt >= regexp_replace(trunc('${i_sdate}','MM'),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',0),'-','') then a.sale_amt end) by_sale_amt,
	-- sum(case when a.sdt >= regexp_replace(trunc('${i_sdate}','MM'),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',0),'-','') then a.sale_qty end) by_sale_qty,
	sum(case when a.sdt >= regexp_replace(trunc('${i_sdate}','MM'),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',0),'-','') then a.profit end) by_profit,	
	
	sum(case when a.sdt >= regexp_replace(add_months(trunc('${i_sdate}','MM'),-1),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',-1),'-','') then a.sale_amt end) sy_sale_amt,
	-- sum(case when a.sdt >= regexp_replace(add_months(trunc('${i_sdate}','MM'),-1),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',-1),'-','') then a.sale_qty end) sy_sale_qty,
	sum(case when a.sdt >= regexp_replace(add_months(trunc('${i_sdate}','MM'),-1),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',-1),'-','') then a.profit end) sy_profit,	
	
	sum(case when a.sdt between '20240209' and '20240217' then a.sale_amt end) bz_sale_amt,
	-- sum(case when a.week=weekofyear(date_sub(current_date, 3)) then a.sale_qty end) bz_sale_qty,
	sum(case when a.sdt between '20240209' and '20240217' then a.profit end) bz_profit,
	
	sum(case when a.sdt between '20240131' and '20240208' then a.sale_amt end) sz_sale_amt,
	-- sum(case when a.week=weekofyear(date_sub(current_date, 10)) then a.sale_qty end) sz_sale_qty,
	sum(case when a.sdt between '20240131' and '20240208' then a.profit end) sz_profit		
from 
(
	select * ,
		substr(sdt,1,6) smonth,
		weekofyear(date_sub(to_date(from_unixtime(unix_timestamp(sdt,'yyyyMMdd'))),-2)) week,
		if((order_channel_code in ('4','5','6') or refund_order_flag=1),original_order_code,order_code) as order_code_new
	from csx_dws.csx_dws_sale_detail_di 
	where sdt >=regexp_replace(add_months(trunc('${i_sdate}','MM'),-1),'-','') 
	and sdt <= regexp_replace(add_months(date_sub(current_date,1),0),'-','') 
	and business_type_code in ('1') 
	-- and (delivery_type_code=2 -- '直送单'
	-- or order_channel_code=6 -- '调价单'
	-- or order_channel_code=4 -- '返利单'
	-- or refund_order_flag=1 -- '退货单'
	-- )
	and inventory_dc_code not in  ('W0J2','W0AJ','W0G6','WB71')
) a 
join 
(
	select * 
	from csx_dim.csx_dim_shop  
	where sdt='current' 
	and shop_low_profit_flag=0 
) b on a.inventory_dc_code=b.shop_code 
group by 
	a. performance_region_name,
	a.performance_province_name,
	a.performance_city_name;
	

/*


---- 异常top客户品类数据-早期发省区反馈的  dense_rank
select *
from 
(
select middle_ranks_asc,middle_ranks_desc,customer_ranks_asc,customer_ranks_desc,
	if (yc_flag = '直送单',customer_ranks_desc,customer_ranks_asc) as customer_ranks_use,
	yc_flag,
	performance_region_name,
	performance_province_name,			
	customer_code,customer_name,
	second_category_name,
	price_type1,price_type2,
	price_period_name,price_date_name,
	customer_bz_sale_amt,customer_profit,customer_profit_rate,
	province_bz_sale_amt,province_profit,province_profit_rate,
	classify_middle_name,
	bz_sale_amt,bz_profit,
	bz_profit/abs(bz_sale_amt) as bz_profit_rate	
from 
(select 
	a.*,
	-- row_number() over(partition by yc_flag,customer_code order by  abs(bz_sale_amt)  desc) as middle_ranks_desc_abs,-- 1234 客户下品类影响排名
	row_number() over(partition by yc_flag,customer_code order by  bz_sale_amt  asc) as middle_ranks_asc,-- 1234 客户下品类影响排名
	row_number() over(partition by yc_flag,customer_code order by  bz_sale_amt  desc) as middle_ranks_desc,
	-- row_number() over(partition by yc_flag,performance_province_name order by  abs(customer_bz_sale_amt)  desc) as customer_ranks_desc_abs,
	row_number() over(partition by yc_flag,performance_province_name order by  customer_bz_sale_amt  asc) as customer_ranks_asc,
	row_number() over(partition by yc_flag,performance_province_name order by  customer_bz_sale_amt  desc) as customer_ranks_desc -- 1122 省区下客户影响排名
from
	(select 
		a.*,
		sum(bz_sale_amt)over(partition by yc_flag,customer_code) customer_bz_sale_amt, -- 客户异常业绩合计
		sum(bz_profit)over(partition by yc_flag,customer_code) customer_profit,
		sum(bz_profit)over(partition by yc_flag,customer_code) /abs(sum(bz_sale_amt)over(partition by yc_flag,customer_code)) customer_profit_rate,sum(bz_sale_amt)over(partition by yc_flag,performance_province_name) province_bz_sale_amt, -- 省区异常业绩合计
		sum(bz_profit)over(partition by yc_flag,performance_province_name) province_profit,	
		sum(bz_profit)over(partition by yc_flag,performance_province_name)/abs(sum(bz_sale_amt)over(partition by yc_flag,performance_province_name))province_profit_rate
	from csx_analyse_tmp.csx_analyse_tmp_sale_yc a
	where bz_sale_amt is not null
	)a
)a
 -- where if (yc_flag = '直送单',middle_ranks_desc=1 and customer_ranks_desc<=5,middle_ranks_asc=1 and customer_ranks_asc<=5)
)a
 where middle_ranks_desc=1 and customer_ranks_use<=5
 order by yc_flag,performance_region_name,performance_province_name,customer_ranks_use;






-- 查数 427307

	select count(1) aa
	from csx_dws.csx_dws_sale_detail_di 
	where sdt>='20231201' 
	and sdt<'20240201'
	and business_type_code in (1) 
	and (delivery_type_code=2 -- '直送单'
	or order_channel_code=6 -- '调价单'
	or order_channel_code=4 -- '返利单'
	or refund_order_flag=1 -- '退货单'
	)
	and inventory_dc_code not in  ('W0J2','W0AJ','W0G6','WB71')




-- 异常影响

-- 生鲜食百对毛利影响--------------------------------------
-- 备注：1、价格补救应该补救到原单上算毛利；
select 
	(case when a.performance_province_name='河南省' then '华北大区' 
	      when a.performance_province_name in ('安徽省','湖北省') then '华东大区' 
	else a.performance_region_name end) as `大区`,
	a.performance_province_name as `省区`,
	(case when a.performance_province_name in ('上海松江') then '上海松江' 
	      when a.performance_province_name in ('江苏苏州') then '江苏苏州'  
	else a.performance_city_name end) as `城市`,
	substr(c.business_division_name,1,2) as `生鲜or食百`,
	-- c.classify_middle_name as `管理中类`,
	-- c.classify_small_name as `管理小类`,
	-- a.customer_code as `客户编码`,
	-- max(a.customer_name) as `客户名称`,
	-- nvl(f.fir_price_type,e.price_type1) as `定价类型1`,
	-- nvl(f.sec_price_type,e.price_type2) as `定价类型2`,
	-- e.price_period_name as `报价周期`,
	-- e.price_date_name as `报价日期`,
	-- a.goods_code as `商品编码`,
	-- c.goods_name as `商品名称`,
	-- 直送>调价>返利，>退货
	case 
		when a.inventory_dc_code in ('W0J2') then '监狱仓'
		when a.inventory_dc_code in ('W0AJ','W0G6','WB71') then '海军仓'
		when a.order_channel_code in ('6')  then '调价'
		when a.order_channel_code in ('4')  then '返利'
		when a.refund_order_flag=1 then '退货'
		when a.delivery_type_code=2 then '直送'		
		else '其他' end as group_1,
	case 
		when a.delivery_type_name<>'直送' then ''
		when a.direct_delivery_type=1 then 'R直送1'
		when a.direct_delivery_type=2 then 'Z直送2'
		when a.direct_delivery_type=11 then '临时加单'
		when a.direct_delivery_type=12 then '紧急补货'
		when a.direct_delivery_type=0 then '普通' else '普通' end as zhisong_flag,	
	nvl(sum(case when a.sdt >= regexp_replace(trunc('${yes_sdt_date}','MM'),'-','') and a.sdt <= regexp_replace(add_months('${yes_sdt_date}',0),'-','') then a.sale_amt end),0) as `本月销售额`,
	nvl(sum(case when a.sdt >= regexp_replace(trunc('${yes_sdt_date}','MM'),'-','') and a.sdt <= regexp_replace(add_months('${yes_sdt_date}',0),'-','') and d.original_order_code is not null then a.profit-d.sale_cost 
				 when a.sdt >= regexp_replace(trunc('${yes_sdt_date}','MM'),'-','') and a.sdt <= regexp_replace(add_months('${yes_sdt_date}',0),'-','') then a.profit end),0) as `本月毛利额`,	
	nvl(sum(case when a.sdt >= regexp_replace(trunc('${yes_sdt_date}','MM'),'-','') and a.sdt <= regexp_replace(add_months('${yes_sdt_date}',0),'-','') then a.sale_qty end),0) as `本月销量`,

	nvl(sum(case when a.sdt >= regexp_replace(add_months(trunc('${yes_sdt_date}','MM'),-1),'-','') and a.sdt <= regexp_replace(add_months('${yes_sdt_date}',-1),'-','') then a.sale_amt end),0) as `上月销售额`,
	nvl(sum(case when a.sdt >= regexp_replace(add_months(trunc('${yes_sdt_date}','MM'),-1),'-','') and a.sdt <= regexp_replace(add_months('${yes_sdt_date}',-1),'-','') and d.original_order_code is not null then a.profit-d.sale_cost 
				 when a.sdt >= regexp_replace(add_months(trunc('${yes_sdt_date}','MM'),-1),'-','') and a.sdt <= regexp_replace(add_months('${yes_sdt_date}',-1),'-','') then a.profit end),0) as `上月毛利额`,	
	nvl(sum(case when a.sdt >= regexp_replace(add_months(trunc('${yes_sdt_date}','MM'),-1),'-','') and a.sdt <= regexp_replace(add_months('${yes_sdt_date}',-1),'-','') then a.sale_qty end),0) as `上月销量`,
	
	nvl(sum(case when a.week=weekofyear(date_sub(current_date, 3)) then a.sale_amt end),0) as `本周销售额`,
	nvl(sum(case when a.week=weekofyear(date_sub(current_date, 3)) and d.original_order_code is not null then a.profit-d.sale_cost
				 when a.week=weekofyear(date_sub(current_date, 3)) then a.profit end),0) as `本周毛利额`,
	nvl(sum(case when a.week=weekofyear(date_sub(current_date, 3)) then a.sale_qty end),0) as `本周销量`,	
	nvl(sum(case when a.week=weekofyear(date_sub(current_date, 3+7)) then a.sale_amt end),0) as `上周销售额`,
	nvl(sum(case when a.week=weekofyear(date_sub(current_date, 3+7)) and d.original_order_code is not null then a.profit-d.sale_cost
				 when a.week=weekofyear(date_sub(current_date, 3+7)) then a.profit end),0) as `上周毛利额`,
	nvl(sum(case when a.week=weekofyear(date_sub(current_date, 3+7)) then a.sale_qty end),0) as `上周销量`
from 
	(select *,weekofyear(date_sub(to_date(from_unixtime(unix_timestamp(sdt,'yyyyMMdd'))),-2)) week
	from csx_dws.csx_dws_sale_detail_di 
	where sdt>=regexp_replace(add_months(trunc('${yes_sdt_date}','MM'),-1),'-','') 
	and sdt<=regexp_replace('${yes_sdt_date}','-','')   
	and business_type_code='1' 
	-- and order_channel_code not in ('4','6') 
	-- and delivery_type_code<>2 
	-- and refund_order_flag<>1 
	-- and inventory_dc_code not in ('W0J2','W0AJ','W0G6','WB71') 
	-- and (order_channel_detail_code<>26 or order_channel_detail_code is null) 
	-- and classify_middle_name in('蛋','米','食用油类','调味品类')
	) a 
	left join 
	(select * 
	from csx_dim.csx_dim_shop  
	where sdt='current' 
	and shop_low_profit_flag=1 
	) b 
	on a.inventory_dc_code=b.shop_code 
	left join 
	(select * 
	from csx_dim.csx_dim_basic_goods 
	where sdt='current'
	) c 
	on a.goods_code=c.goods_code 
	left join 
	(select * 
	from csx_dws.csx_dws_sale_detail_di 
	where sdt>=regexp_replace(add_months(trunc('${yes_sdt_date}','MM'),-1),'-','') 
	and sdt<=regexp_replace('${yes_sdt_date}','-','') 
	and business_type_code='1' 
	and order_channel_detail_code=26
	) d 
	on a.order_code=d.original_order_code and a.goods_code=d.goods_code 
	left join 
	csx_analyse_tmp.tmp_customer_price_type_business_tmp1 e 
	on a.customer_code=e.customer_code 
	-- left join 
	-- -- 线下表客户定价类型
	-- csx_ods.csx_ods_data_analysis_prd_cus_price_type_231206_df f  
	-- on a.customer_code=f.customer_code 
where b.shop_code is null 
group by 
	(case when a.performance_province_name='河南省' then '华北大区' 
	      when a.performance_province_name in ('安徽省','湖北省') then '华东大区' 
	else a.performance_region_name end),
	a.performance_province_name,
	(case when a.performance_province_name in ('上海松江') then '上海松江' 
	      when a.performance_province_name in ('江苏苏州') then '江苏苏州'  
	else a.performance_city_name end),
	substr(c.business_division_name,1,2),
	-- c.classify_middle_name,
	-- c.classify_small_name,
	-- a.goods_code,
	-- c.goods_name,
	-- a.customer_code,
	-- nvl(f.fir_price_type,e.price_type1),
	-- nvl(f.sec_price_type,e.price_type2),
	-- e.price_period_name,
	-- e.price_date_name
	case 
		when a.inventory_dc_code in ('W0J2') then '监狱仓'
		when a.inventory_dc_code in ('W0AJ','W0G6','WB71') then '海军仓'
		when a.order_channel_code in ('6')  then '调价'
		when a.order_channel_code in ('4')  then '返利'
		when a.refund_order_flag=1 then '退货'
		when a.delivery_type_code=2 then '直送'		
		else '其他' end,
	case 
		when a.delivery_type_name<>'直送' then ''
		when a.direct_delivery_type=1 then 'R直送1'
		when a.direct_delivery_type=2 then 'Z直送2'
		when a.direct_delivery_type=11 then '临时加单'
		when a.direct_delivery_type=12 then '紧急补货'
		when a.direct_delivery_type=0 then '普通' else '普通' end	
;



-- 生鲜食百对毛利影响-  年对比-------------------------------------
-- 备注：1、价格补救应该补救到原单上算毛利；
select 
	a.performance_region_name as `大区`,
	a.performance_province_name as `省区`,
	a.performance_city_name as `城市`,
	substr(c.business_division_name,1,2) as `生鲜or食百`,
	-- c.classify_middle_name as `管理中类`,
	-- c.classify_small_name as `管理小类`,
	-- a.customer_code as `客户编码`,
	-- max(a.customer_name) as `客户名称`,
	-- nvl(f.fir_price_type,e.price_type1) as `定价类型1`,
	-- nvl(f.sec_price_type,e.price_type2) as `定价类型2`,
	-- e.price_period_name as `报价周期`,
	-- e.price_date_name as `报价日期`,
	-- a.goods_code as `商品编码`,
	-- c.goods_name as `商品名称`,
	-- 直送>调价>返利，>退货
	case 
		when a.inventory_dc_code in ('W0J2') then '监狱仓'
		when a.inventory_dc_code in ('W0AJ','W0G6','WB71') then '海军仓'
		when a.order_channel_code in ('6')  then '调价'
		when a.order_channel_code in ('4')  then '返利'
		when a.refund_order_flag=1 then '退货'
		when a.delivery_type_code=2 then '直送'		
		else '其他' end as group_1,
	case 
		when a.delivery_type_name<>'直送' then ''
		when a.direct_delivery_type=1 then 'R直送1'
		when a.direct_delivery_type=2 then 'Z直送2'
		when a.direct_delivery_type=11 then '临时加单'
		when a.direct_delivery_type=12 then '紧急补货'
		when a.direct_delivery_type=0 then '普通' else '普通' end as zhisong_flag,	
	nvl(sum(case when a.sdt >= '20230101' and a.sdt <= '20231231' then a.sale_amt end),0) as `本年销售额`,
	nvl(sum(case when a.sdt >= '20230101' and a.sdt <= '20231231' and d.original_order_code is not null then a.profit-d.sale_cost 
				 when a.sdt >= '20230101' and a.sdt <= '20231231' then a.profit end),0) as `本年毛利额`,	
	nvl(sum(case when a.sdt >= '20230101' and a.sdt <= '20231231' then a.sale_qty end),0) as `本年销量`,

	nvl(sum(case when a.sdt >= '20220101' and a.sdt <= '20221231' then a.sale_amt end),0) as `上年销售额`,
	nvl(sum(case when a.sdt >= '20220101' and a.sdt <= '20221231' and d.original_order_code is not null then a.profit-d.sale_cost 
				 when a.sdt >= '20220101' and a.sdt <= '20221231' then a.profit end),0) as `上年毛利额`,	
	nvl(sum(case when a.sdt >= '20220101' and a.sdt <= '20221231' then a.sale_qty end),0) as `上年销量`
from 
	(select *,weekofyear(date_sub(to_date(from_unixtime(unix_timestamp(sdt,'yyyyMMdd'))),-2)) week
	from csx_dws.csx_dws_sale_detail_di 
	where sdt>='20220101' 
	and sdt<='20231231'  
	and business_type_code='1' 
	-- and order_channel_code not in ('4','6') 
	-- and delivery_type_code<>2 
	-- and refund_order_flag<>1 
	-- and inventory_dc_code not in ('W0J2','W0AJ','W0G6','WB71') 
	-- and (order_channel_detail_code<>26 or order_channel_detail_code is null) 
	-- and classify_middle_name in('蛋','米','食用油类','调味品类')
	) a 
	left join 
	(select * 
	from csx_dim.csx_dim_shop  
	where sdt='current' 
	and shop_low_profit_flag=1 
	) b 
	on a.inventory_dc_code=b.shop_code 
	left join 
	(select * 
	from csx_dim.csx_dim_basic_goods 
	where sdt='current'
	) c 
	on a.goods_code=c.goods_code 
	left join 
	(select * 
	from csx_dws.csx_dws_sale_detail_di 
	where sdt>='20220101' 
	and sdt<='20231231' 
	and business_type_code='1' 
	and order_channel_detail_code=26
	) d 
	on a.order_code=d.original_order_code and a.goods_code=d.goods_code 
	left join 
	csx_analyse_tmp.tmp_customer_price_type_business_tmp1 e 
	on a.customer_code=e.customer_code 
	-- left join 
	-- -- 线下表客户定价类型
	-- csx_ods.csx_ods_data_analysis_prd_cus_price_type_231206_df f  
	-- on a.customer_code=f.customer_code 
where b.shop_code is null 
group by 
	a.performance_region_name,
	a.performance_province_name,
	a.performance_city_name,
	substr(c.business_division_name,1,2),
	-- c.classify_middle_name,
	-- c.classify_small_name,
	-- a.goods_code,
	-- c.goods_name,
	-- a.customer_code,
	-- nvl(f.fir_price_type,e.price_type1),
	-- nvl(f.sec_price_type,e.price_type2),
	-- e.price_period_name,
	-- e.price_date_name
	case 
		when a.inventory_dc_code in ('W0J2') then '监狱仓'
		when a.inventory_dc_code in ('W0AJ','W0G6','WB71') then '海军仓'
		when a.order_channel_code in ('6')  then '调价'
		when a.order_channel_code in ('4')  then '返利'
		when a.refund_order_flag=1 then '退货'
		when a.delivery_type_code=2 then '直送'		
		else '其他' end,
	case 
		when a.delivery_type_name<>'直送' then ''
		when a.direct_delivery_type=1 then 'R直送1'
		when a.direct_delivery_type=2 then 'Z直送2'
		when a.direct_delivery_type=11 then '临时加单'
		when a.direct_delivery_type=12 then '紧急补货'
		when a.direct_delivery_type=0 then '普通' else '普通' end	
;
*/

