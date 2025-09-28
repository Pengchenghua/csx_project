-- 销售表中各商品大区内最低成本价:日配采购参与，剔除直送调价返利退货，及售价下浮的采购单子
drop table if exists csx_analyse_tmp.csx_analyse_tmp_cust_goods_sale_mincost_1;
create table csx_analyse_tmp.csx_analyse_tmp_cust_goods_sale_mincost_1 as 
with 
-- 售价下浮的采购单子
scm_order_product as
(
select a.*
from 
(
select 
	a.order_code  as scm_order_code,   -- 采购订单号
	a.link_order_code,   -- 关联单号 记录此订单的上级单号，对应申请单号
	a.zm_direct_flag,   -- 是否账面直通(0-否、1-是)
	a.supplier_code,   -- 供应商编码
	a.supplier_name,   -- 供应商名称
	a.customer_code,   -- 客户编码
	a.sub_customer_code,   -- 子客户号	
	-- b.order_code,   -- 订单号
	b.goods_code,   -- 商品编码
	b.price1_include_tax,   -- 单价1(含税)
	b.amount1_include_tax,   -- 金额1(含税)
	b.price2_include_tax,   -- 单价2(含税)
	b.amount2_include_tax   -- 金额2(含税)		
from 
	(
	select 
		order_code,   -- 订单号
		link_order_code,   -- 关联单号 记录此订单的上级单号，对应申请单号
		zm_direct_flag,   -- 是否账面直通(0-否、1-是)
		supplier_code,   -- 供应商编码
		supplier_name,   -- 供应商名称
		customer_code,   -- 客户编码
		sub_customer_code   -- 子客户号
	from csx_dwd.csx_dwd_scm_order_header_di
	where sdt>=regexp_replace(add_months(trunc('${yes_sdt_date}','MM'),-3),'-','')
	and sdt<='${yes_sdt}'
	and link_order_code<>''
	)a
	left join 
	(
	select 
		order_code,   -- 订单号
		goods_code,   -- 商品编码
		price1_include_tax,   -- 单价1(含税)
		amount1_include_tax,   -- 金额1(含税)
		price2_include_tax,   -- 单价2(含税)
		amount2_include_tax   -- 金额2(含税)
	from csx_dwd.csx_dwd_scm_order_product_price_di
	where sdt>=regexp_replace(add_months(trunc('${yes_sdt_date}','MM'),-3),'-','')
	and sdt<='${yes_sdt}'
	)b on a.order_code=b.order_code
)a 	
join
	(
	select order_code,goods_code
	from csx_dws.csx_dws_scm_order_detail_di
	where sdt>=regexp_replace(add_months(trunc('${yes_sdt_date}','MM'),-3),'-','')
	-- and price_remedy_flag <> '1'  -- 补救标识(原单号、退货单号标识)新采购单未标识 剔除价格补救单，以防计算成本价错误
	-- -- and is_supply_stock_tag = '1'  -- 是否集采仓
	-- and super_class = '1'  -- 单据类型(1-供应商订单、2-供应商退货订单、3-配送订单、4-返配订单)
	-- and navy_order_flag ='0' -- #是否海军订单 0-否,1-是
	-- and direct_delivery_type=0  -- 	#直送类型 0-P(普通) 1-R(融单)、2-Z(过账)
	-- and (source_type in ('1','10','19','23')   -- 来源采购订单类型
	-- and items_status in (1,2,3,4)) --   头表状态(1-已创建、2-已发货、3-部分入库、4-已完成、5-已取消)
	-- and price_include_tax>0 and order_qty>0 
	-- 剔除价格类型为售价下浮的，品类背靠背支持
	and price_type=2   -- 价格类型 1-周期进价 2-售价下浮 3-不指定
	)c on a.scm_order_code=c.order_code and a.goods_code=c.goods_code
) ,
-- 全量客户本期数据：日配采购参与，剔除直送调价返利退货，及售价下浮的采购单子
all_cust_goods_sale as
(select 
	a.performance_region_name,
	a.performance_province_name,
	a.performance_city_name,
	a.customer_code,
	d.customer_name,
	e.classify_large_code,
    e.classify_large_name,
    e.classify_middle_code,
    e.classify_middle_name,
    e.classify_small_code,
    e.classify_small_name,
	a.goods_code,
	e.goods_name,
	sum(sale_qty) as sale_qty,
	sum(sale_amt) as sale_amt,
	sum(profit)as profit,
	sum(profit)/abs(sum(sale_amt)) profit_rate,
	sum(sale_cost) sale_cost,
	sum(sale_amt)/sum(sale_qty) as avg_sj,  -- 平均售价
	sum(sale_cost)/sum(sale_qty) as avg_cb  -- 平均成本
from 
	(
	select *
	from 
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt>='${bq01}' and sdt <='${yes_sdt}'
		and business_type_code=1  
		and shipper_code='YHCSX' 
	    and order_channel_code not in ('4','6','5') -- 剔除所有异常
	    and refund_order_flag<>1 
	    and delivery_type_code<>2 
	) a		
	    -- -----客户数据
	left join 
		(select * 
		from csx_dim.csx_dim_crm_customer_info 
		where sdt='current' 
		and shipper_code='YHCSX'
		) d
		on a.customer_code=d.customer_code 
	left join 
		-- -----商品数据
		(select * 
		from csx_dim.csx_dim_basic_goods 
		where sdt='current' 
		) e 
		on a.goods_code=e.goods_code 		
	left join 
        (select
            code as type,
            max(name) as name,
            max(extra) as extra 
        from csx_dim.csx_dim_basic_topic_dict_df
        where parent_code = 'direct_delivery_type' 
        group by code 
        ) h 
        on a.direct_delivery_type=h.type 
		-- 剔除售价下浮的采购单
		left join csx_analyse_tmp.scm_order_product i on a.original_order_code=i.link_order_code and a.goods_code=i.goods_code
    where h.extra='采购参与'
	and i.goods_code is null
group by 
	a.performance_region_name,
	a.performance_province_name,
	a.performance_city_name,
	a.customer_code,
	d.customer_name,
	e.classify_large_code,
    e.classify_large_name,
    e.classify_middle_code,
    e.classify_middle_name,
    e.classify_small_code,
    e.classify_small_name,
	a.goods_code,
	e.goods_name
)
-- 销售表中大区内最低成本价
select b1.* 
from 
	(select 
		*,
		row_number()over(partition by performance_region_name,goods_code order by avg_cb asc,sale_amt desc) as rn	 -- 按大区排名
	from all_cust_goods_sale
	)b1
where rn=1
;

-- -- top30客户商品销售：两期
drop table if exists csx_analyse_tmp.csx_analyse_tmp_cust_goods_sale;
create table csx_analyse_tmp.csx_analyse_tmp_cust_goods_sale as 
with cust_goods_sale as 
(
select 
    a.performance_region_name,
    a.performance_province_name,
    a.performance_city_name,
    a.customer_code,
    d.customer_name,
    e.classify_large_code,
    e.classify_large_name,
    e.classify_middle_code,
    e.classify_middle_name,
    e.classify_small_code,
    e.classify_small_name,
    a.goods_code,
    e.goods_name,		
    sum(case when a.sdt>='${sq01}' and a.sdt<='${sq02}' then a.sale_qty end) as sale_qty_sq,
    sum(case when a.sdt>='${sq01}' and a.sdt<='${sq02}' then a.sale_amt end) as sale_amt_sq,
    sum(case when a.sdt>='${sq01}' and a.sdt<='${sq02}' then a.profit end) as profit_sq,			
			
    sum(case when a.sdt>='${bq01}' and a.sdt<='${yes_sdt}' then a.sale_qty end) as sale_qty_bq,
    sum(case when a.sdt>='${bq01}' and a.sdt<='${yes_sdt}' then a.sale_amt end) as sale_amt_bq,
    sum(case when a.sdt>='${bq01}' and a.sdt<='${yes_sdt}' then a.profit end) as profit_bq
	
from 
    (select * 
    from csx_dws.csx_dws_sale_detail_di  
    where sdt>='${sq01}'   
    and sdt<='${yes_sdt}'    
    and business_type_code=1  
    and order_channel_code not in ('4','6','5') -- 剔除所有异常
    and refund_order_flag<>1 
	and delivery_type_name<>'直送'
    and shipper_code='YHCSX' 
    -- and customer_code in ('222798','252181','258261','124524','255475','126387','233646',
    --                       '255101','121061','131129','237905','126377','115769','249942',
    --                       '131187','224985','256667','241458','223283','220106','106775',
    --                       '249548','250259','250879','247826','128359','252038','236853',
    --                       '128371','163315')
    and customer_code in('237857'
,'231868'
,'111365'
,'102924'
,'176027'
,'175548'
,'110693'
,'162716'
,'115656'
,'218936'
,'103332'
)
    ) a 
    left join 
    (select * 
    from csx_dim.csx_dim_crm_customer_info 
    where sdt='current' 
    and shipper_code='YHCSX'
    ) d
    on a.customer_code=d.customer_code 
    left join 
    -- -----商品数据
    (select * 
    from csx_dim.csx_dim_basic_goods 
    where sdt='current' 
    ) e 
    on a.goods_code=e.goods_code 
    left join 
    (select
        code as type,
        max(name) as name,
        max(extra) as extra 
    from csx_dim.csx_dim_basic_topic_dict_df
    where parent_code = 'direct_delivery_type' 
    group by code 
    ) h 
    on a.direct_delivery_type=h.type 
where h.extra='采购参与'
group by 
    a.performance_region_name,
    a.performance_province_name,
    a.performance_city_name,
    a.customer_code,
    d.customer_name,
    e.classify_large_code,
    e.classify_large_name,
    e.classify_middle_code,
    e.classify_middle_name,
    e.classify_small_code,
    e.classify_small_name,
    a.goods_code,
    e.goods_name
), 

cust_sale as 
(
select 
customer_code,
sum(sale_amt_bq) as sale_amt_bq_all,
sum(profit_bq) as profit_bq_all
from cust_goods_sale
group by customer_code
)

select 
	a.*,
	a.sale_amt_bq/b.sale_amt_bq_all as sale_amt_bq_zb,    -- 客户商品销售额/客户销售额
	b.sale_amt_bq_all,
	b.profit_bq_all/abs(sale_amt_bq_all) as profit_rate_bq_all   -- 客户毛利率
from 
	(
	select *,
		row_number() over(partition by customer_code order by nvl(sale_amt_bq,0) desc) as rno
	from cust_goods_sale
	)a 
	left join cust_sale b on a.customer_code=b.customer_code
	order by customer_code,rno
	;


-- 入库成本价
drop table csx_analyse_tmp.tmp_dc_goods_received_sdt; 
create table csx_analyse_tmp.tmp_dc_goods_received_sdt as 	
    select 
	  d.performance_region_code,
	  d.performance_region_name,
	  d.performance_province_code,
	  d.performance_province_name,	
	  d.performance_city_code,
	  d.performance_city_name,	  
	  a.dc_code,
	  d.shop_name as dc_name,      
	  b.classify_large_code,
	  b.classify_large_name,
	  b.classify_middle_code,
	  b.classify_middle_name,
	  b.classify_small_code,
	  b.classify_small_name,	  
	  b.spu_goods_code,
	  b.spu_goods_name,
	  a.goods_code,
	  b.goods_name,	
	  b.standard,  -- 规格
	  b.unit_name,  -- 计量单位描述	
	  a.supplier_code,  -- 供应商编码
	  a.supplier_name,  -- 供应商名称
	  a.local_purchase_flag,  -- 是否地采
	  -- a.business_type_name,  -- 业务类型名称	  
	  a.sdt,
      a.order_price*a.receive_qty as receive_amt,
      a.receive_qty,
	  -- a.receive_price,
      a.order_price 
    from 
      -- 入库数据
      (
		select 
		-- performance_region_code,
		-- performance_region_name,  -- 大区名称
		-- -- province_code as performance_province_code,
		-- -- province_name as performance_province_name,  -- 业绩省区名称
		-- -- city_code as performance_city_code,
		-- -- city_name as performance_city_name,  -- 业绩城市名称
		-- performance_province_code,
		-- performance_province_name,  -- 业绩省区名称
		-- performance_city_code,
		-- performance_city_name,  -- 业绩城市名称		  
		source_type as source_type_code,  -- 来源采购订单类型
		-- a1.config_value as source_type_name,  -- 来源采购订单名称
		super_class,  -- 单据类型编码
		-- super_class_name,  -- 单据类型名称
		order_code as purchase_order_code,  -- 采购订单号
		-- order_code,  -- 入库/出库单号
		target_location_code as dc_code,  -- dc编码
		target_location_name as dc_name,  -- dc名称
		goods_code,  -- 商品编码
		goods_name,  -- 商品名称
		unit as unit_name,  -- 单位
		classify_large_name,  -- 管理一级名称
		classify_middle_name,  -- 管理二级名称
		supplier_code,  -- 供应商编码
		supplier_name,  -- 供应商名称
		local_purchase_flag,  -- 是否地采
		-- business_type_name,  -- 业务类型名称
		sdt,
		-- price_include_tax,  -- 单价1
		-- order_price2,  -- 单价2
		order_qty as receive_qty,  -- 库数量
		amount_include_tax as receive_amt,  -- 库金额
		price_include_tax as order_price,
		urgency_flag,  -- 紧急补货
		order_type  -- 订单类型(0-普通供应商订单 1-囤货订单 2-日采订单 3-计划订单)		  
		-- from csx_analyse.csx_analyse_scm_purchase_order_flow_di 
		from csx_dws.csx_dws_scm_order_detail_di
		where sdt>=regexp_replace(date_add(from_unixtime(unix_timestamp('${yes_sdt}','yyyyMMdd'),'yyyy-MM-dd'),-30),'-','')
		and sdt<='${yes_sdt}' 
		-- where sdt='${yes_sdt}' 
		and price_remedy_flag <> '1'  -- 补救标识(原单号、退货单号标识)新采购单未标识 剔除价格补救单，以防计算成本价错误
		-- and is_supply_stock_tag = '1'  -- 是否集采仓
		and super_class = '1'  -- 单据类型(1-供应商订单、2-供应商退货订单、3-配送订单、4-返配订单)
		and navy_order_flag ='0' -- 是否海军订单 0-否,1-是
		and direct_delivery_type=0  -- 	直送类型 0-P(普通) 1-R(融单)、2-Z(过账)
		and (source_type in ('1','10','19','23')   -- 来源采购订单类型
		and items_status in (1,2,3,4)) --   头表状态(1-已创建、2-已发货、3-部分入库、4-已完成、5-已取消)
		and price_include_tax>0 
    and order_qty>0 
		-- 剔除价格类型为售价下浮的，品类背靠背支持
		and price_type<>2   -- 价格类型 1-周期进价 2-售价下浮 3-不指定
      ) a 
	-- left join 
	-- (
	-- select config_key,config_value
	-- from csx_ods.csx_ods_csx_b2b_scm_scm_configuration_df a 
	-- where a.config_type = 'PURCHASE_ORDER_SOURCE_TYPE'
	-- and sdt=regexp_replace(date_sub(current_date(),1),'-','')
	-- )a1 on a.source_type=a1.config_key	  
    -- 关联价格补救订单数据 
    left join 
    (select 
        * 
     from csx_dws.csx_dws_scm_order_received_di 
     where sdt>=regexp_replace(date_add(from_unixtime(unix_timestamp('${yes_sdt}','yyyyMMdd'),'yyyy-MM-dd'),-30),'-','') 
     and sdt<='${yes_sdt}' 
     and price_remedy_flag=1 
    ) a2 on a.purchase_order_code=a2.original_order_code and a.goods_code=a2.goods_code 	  
      left join 
      (
        select * 
        from csx_dim.csx_dim_basic_goods 
        where sdt='current' 
      ) b on a.goods_code=b.goods_code 	  
	left join 
	(select 
		purchase_org,
		purchase_org_name,
		performance_region_code,
		performance_region_name,
		performance_city_code,
		performance_city_name,
		performance_province_code,
		performance_province_name,		
		shop_code ,
		shop_name ,
		company_code ,
		company_name ,
		city_code,
		city_name,
		province_code,
		province_name,
		purpose,
		purpose_name
	from csx_dim.csx_dim_shop
	where sdt='current'
	) d on a.dc_code=d.shop_code	
	where a2.original_order_code is null  -- 剔除价格补救原单
	-- 生鲜取近7天，食百取近30天
	and (
	(((b.business_division_name like '%生鲜%' and b.classify_middle_code='B0101') or  b.business_division_name like '%食百%') and a.sdt>=regexp_replace(date_add(from_unixtime(unix_timestamp('${yes_sdt}','yyyyMMdd'),'yyyy-MM-dd'),-30),'-','') and a.sdt<='${yes_sdt}' )
	or 
	(b.business_division_name like '%生鲜%' and (b.classify_middle_code<>'B0101' or b.classify_middle_code is null) and a.sdt>=regexp_replace(date_add(from_unixtime(unix_timestamp('${yes_sdt}','yyyyMMdd'),'yyyy-MM-dd'),-6),'-','') and a.sdt<='${yes_sdt}')
	)	
    -- where b.classify_middle_name in('猪肉','家禽','牛羊','水产','蔬菜','水果','预制菜','干货')
;


/*-- 昨日入库成本，若该商品无入库, 生鲜往前追溯7天, 食百30天, 取追溯时间段内最后一天数据
drop table csx_analyse_tmp.tmp_dc_goods_received_sdt_last; 
create table csx_analyse_tmp.tmp_dc_goods_received_sdt_last as 
with receive_goods_sdt as 
( 
select 
  performance_region_code,
  performance_region_name,
  performance_province_code,
  performance_province_name,	
  performance_city_code,
  performance_city_name,	  
  -- dc_code,
  -- dc_name,      
  classify_large_code,
  classify_large_name,
  classify_middle_code,
  classify_middle_name,
  classify_small_code,
  classify_small_name,	  
  spu_goods_code,
  spu_goods_name,
  goods_code,
  goods_name,	
  standard,  -- 规格
  unit_name,  -- 计量单位描述	
  supplier_code,  -- 供应商编码
  supplier_name,  -- 供应商名称
  local_purchase_flag,  -- 是否地采
  -- business_type_name,  -- 业务类型名称	  
  sdt,
  sum(receive_amt) as receive_amt,
  sum(receive_qty) as receive_qty,
  -- a.receive_price,
  sum(receive_amt)/sum(receive_qty) as receive_price
from csx_analyse_tmp.tmp_dc_goods_received_sdt
group by  
  performance_region_code,
  performance_region_name,
  performance_province_code,
  performance_province_name,	
  performance_city_code,
  performance_city_name,	  
  -- dc_code,
  -- dc_name,      
  classify_large_code,
  classify_large_name,
  classify_middle_code,
  classify_middle_name,
  classify_small_code,
  classify_small_name,	  
  spu_goods_code,
  spu_goods_name,
  goods_code,
  goods_name,	
  standard,  -- 规格
  unit_name,  -- 计量单位描述	
  supplier_code,  -- 供应商编码
  supplier_name,  -- 供应商名称
  local_purchase_flag,  -- 是否地采
  -- business_type_name,  -- 业务类型名称	  
  sdt
),

-- 最近一天入库成本价
receive_goods_sdt_last as 
(
select *
from 
(
select *,
  row_number() over(partition by performance_region_code,performance_city_code,goods_code order by sdt desc) as rno_sdt
from receive_goods_sdt
)a 
where rno_sdt=1
)

select 
	b.performance_region_name,
	b.goods_code,
	b.performance_city_name as performance_city_name_dqmin,
	b.receive_price as receive_price_dqmin -- 大区最低成本
from 
	(
	select b1.* 
	from 
		(select *,
			row_number()over(partition by performance_region_name,goods_code order by receive_price asc) as rn	 -- 按大区排名
		from receive_goods_sdt_last 
		)b1
	where rn=1
	)b;

-- 大区最低按最近一次入库价
select 
	a.*,
	profit_rate_bq-profit_rate_sq as profit_rate_hb,
	sale_price_bq-sale_price_sq as sale_price_hb,
	if(this_week_avg_market_price = 0 or last_week_avg_market_price = 0, 0, this_week_avg_market_price - last_week_avg_market_price) as avg_market_price_hb,
	cost_price_bq-cost_price_sq as cost_price_hb
from 	
	(select 
		a.performance_region_name,
		a.performance_province_name,
		a.performance_city_name,
		a.customer_code,
		a.customer_name,
		a.classify_large_name,
		a.classify_middle_name,
		a.classify_small_name,
		a.goods_code,
		a.goods_name,
		a.sale_qty_sq,
		a.sale_amt_sq,
		a.profit_sq,
		a.profit_sq/abs(a.sale_amt_sq) as profit_rate_sq,
		a.sale_amt_sq/a.sale_qty_sq as sale_price_sq,
		d.last_week_avg_market_price,
		(a.sale_amt_sq-a.profit_sq)/a.sale_qty_sq as cost_price_sq,
			
		a.sale_qty_bq,
		a.sale_amt_bq,
		a.profit_bq,
		a.profit_bq/abs(a.sale_amt_bq) as profit_rate_bq,
		a.sale_amt_bq/a.sale_qty_bq as sale_price_bq,
		d.this_week_avg_market_price,
		(a.sale_amt_bq-a.profit_bq)/a.sale_qty_bq as cost_price_bq,
			
		-- b.performance_city_name as performance_city_name_dq_min,
		-- b.customer_code as customer_code_dq_min,
		-- b.customer_name as customer_name_dq_min,
		-- b.avg_cb as avg_cb_dq_min, -- 大区最低成本  -- 销售订单平均成本
		
		c.performance_city_name_dqmin,
		c.receive_price_dqmin -- 大区最低成本	-- 入库成本
			
	from 
	(select * from csx_analyse_tmp.csx_analyse_tmp_cust_goods_sale
	) a
	left join csx_analyse_tmp.csx_analyse_tmp_cust_goods_sale_mincost_1 b 
	on a.performance_region_name=b.performance_region_name and a.goods_code=b.goods_code
	
	left join csx_analyse_tmp.tmp_dc_goods_received_sdt_last c on a.performance_region_name=c.performance_region_name and a.goods_code=c.goods_code
	
	left join
	(select distinct 
		customer_code,
		goods_code,
		shop_code,
		shop_name,
		last_week_avg_market_price, -- 上周市调
		this_week_avg_market_price -- 本周市调
	from csx_analyse.csx_analyse_sale_cost_price_vs_market_price_wf 
	where sdt='20250601'  
	) d on a.customer_code=d.customer_code and a.goods_code=d.goods_code
)a;
*/


-- 平均入库成本
drop table csx_analyse_tmp.tmp_dc_goods_received_week; 
create table csx_analyse_tmp.tmp_dc_goods_received_week as 
	select 
		performance_region_name,
		performance_province_name,
		performance_city_name,
		goods_code,
		sum(receive_amt) as receive_amt,
		sum(receive_qty) as receive_qty,
		sum(receive_amt)/sum(receive_qty) as receive_price
	from csx_analyse_tmp.tmp_dc_goods_received_sdt
	where sdt >='${bq01}' and sdt <='${yes_sdt}' 
	group by 
		performance_region_name,
		performance_province_name,
		performance_city_name,
		goods_code;
		
		
-- 平均入库成本最低
drop table csx_analyse_tmp.tmp_dc_goods_received_week_dqmin; 
create table csx_analyse_tmp.tmp_dc_goods_received_week_dqmin as 
select *
from 		
	(select 
		*,
		row_number()over(partition by performance_region_name,goods_code order by receive_price asc) as rn	 -- 按大区排名
	from csx_analyse_tmp.tmp_dc_goods_received_week
	) a 
	where rn=1;
  
   
-- --------------------------------------------------------------------------------
-- ----------市调数据中间表
drop table if exists csx_analyse_tmp.last_week_avg_shop_price_tmp_11_cost_vs_market; 
create table if not exists csx_analyse_tmp.last_week_avg_shop_price_tmp_11_cost_vs_market as 
select 
    c5.performance_province_name,
    c2.location_code,
    c4.classify_large_code,
    c4.classify_large_name,
    c4.classify_middle_code,
    c4.classify_middle_name,
    c4.classify_small_code,
    c4.classify_small_name,
    c2.product_code,
    c1.shop_code,
    c1.shop_name,
    (case when c1.market_source_type_code=1 then '永辉' 
          when c1.market_source_type_code=4 then '一批' 
          when c1.market_source_type_code=5 then '二批' 
          when c1.market_source_type_code=6 then '终端'  
    end) as market_source_type_name,
    c1.market_research_price,
    regexp_replace(substr(c1.price_begin_time,1,10),'-','') as price_begin_date,
    regexp_replace(substr(c1.price_end_time_new,1,10),'-','') as price_end_date 
from 
    (-- 目前失效数据数据
    select 
      t1.* 
    from 
      (select 
          product_id as market_goods_id,
          source_type_code as market_source_type_code,
          shop_code,
          shop_name,
          cast(price as decimal(20,6)) as market_research_price,
          cast(price_begin_time as string) as price_begin_time,
          cast(price_end_time as string) as price_end_time,
          cast((case when status=0 and price_end_time<update_time then price_end_time else update_time end) as string) as price_end_time_new
      from csx_dwd.csx_dwd_market_research_not_yh_price_di 
      where substr((case when status=0 and price_end_time>update_time then update_time else price_end_time end),1,10)>=add_months(trunc(from_unixtime(unix_timestamp('${yes_sdt}','yyyyMMdd'),'yyyy-MM-dd'),'MM'),-2) 
      and product_status=0 
      ) t1 
      left join 
      (select 
        bmk_code 
      from csx_ods.csx_ods_data_analysis_prd_profit_control_bmk_ky_df 
      group by bmk_code 
      ) t2 
      on t1.shop_code=t2.bmk_code 
    where t2.bmk_code is not null 

    union all 
    -- 目前生效市调数据
    select 
      tt1.* 
    from 
      (select 
          product_id as market_goods_id,
          source_type_code as market_source_type_code,
          shop_code,
          shop_name,
          cast(price as decimal(20,6)) as market_research_price,
          cast(price_begin_time as string) as price_begin_time,
          cast(price_end_time as string) as price_end_time,
          cast(price_end_time as string) as price_end_time_new  
      from csx_ods.csx_ods_csx_price_prod_market_research_not_yh_price_effective_df 
      where substr(price_end_time,1,10)>=add_months(trunc(from_unixtime(unix_timestamp('${yes_sdt}','yyyyMMdd'),'yyyy-MM-dd'),'MM'),-2)  
      and product_status=0  
      ) tt1 
      left join 
      (select 
        bmk_code 
      from csx_ods.csx_ods_data_analysis_prd_profit_control_bmk_ky_df 
      group by bmk_code 
      ) tt2 
      on tt1.shop_code=tt2.bmk_code 
    where tt2.bmk_code is not null 

    union all 
    -- 永辉门店市调数据
    select 
      tt3.* 
    from 
      (select 
          market_goods_id as market_goods_id,
          cast(market_source_type_code as int) as market_source_type_code,
          shop_code,
          shop_name,
          cast(market_research_price as decimal(20,6)) as market_research_price,
          cast(price_begin_time as string) as price_begin_time,
          cast(price_end_time as string) as price_end_time,
          cast(price_end_time as string) as price_end_time_new  
      from csx_dwd.csx_dwd_price_market_research_price_di  
      where market_source_type_code='1' 
      and substr(price_end_time,1,10)>=add_months(trunc(from_unixtime(unix_timestamp('${yes_sdt}','yyyyMMdd'),'yyyy-MM-dd'),'MM'),-2)  
      ) tt3 
      left join 
      (select 
        bmk_code 
      from csx_ods.csx_ods_data_analysis_prd_profit_control_bmk_ky_df 
      group by bmk_code 
      ) tt4 
      on tt3.shop_code=tt4.bmk_code 
    where tt4.bmk_code is not null 
    ) c1 
    left join 
    (select * 
    from csx_ods.csx_ods_csx_price_prod_market_research_product_df 
    where sdt='${yes_sdt}'
    ) c2 
    on c1.market_goods_id=c2.id 
    left join 
    (select * 
    from csx_dim.csx_dim_basic_goods 
    where sdt='current' 
    ) c4 
    on c2.product_code=c4.goods_code 
    left join 
    (select * 
    from csx_dim.csx_dim_shop 
    where sdt='current'
    ) c5 
    on c2.location_code=c5.shop_code 
;
-- ------------------------------------------------------------------------------------------
-- ---------市调数据最终表
drop table if exists csx_analyse_tmp.last_week_avg_shop_price_11_cost_vs_market; 
create table if not exists csx_analyse_tmp.last_week_avg_shop_price_11_cost_vs_market as 
select 
  c4.sdt,
  c3.performance_region_name,
  c3.performance_province_name,
  c3.performance_city_name,
  c3.classify_large_name,
  c3.classify_middle_name,
  c3.product_code,
  c3.shop_code,
  max(c3.shop_name) as shop_name,
  avg(c3.market_research_price) as last_week_market_research_price 
from 
  (select 
      t2.* 
   from 
      (
       select 
          t1.* 
       from 
              (select 
                c1.* ,
                c5.performance_region_name,
                -- c5.performance_province_name,
                c5.performance_city_name 
              from 
                  csx_analyse_tmp.last_week_avg_shop_price_tmp_11_cost_vs_market c1 
                  -- TOP商品数据
                  left join 
                  (select * 
                  from csx_dim.csx_dim_shop 
                  where sdt='current'  
                  ) c5 
                  on c1.location_code=c5.shop_code 
                  left join 
                  (select 
                    dc_code,
                    bmk_code,
                    classify_middle_code 
                  from csx_ods.csx_ods_data_analysis_prd_profit_control_bmk_ky_df 
                  group by 
                    dc_code,
                    bmk_code,
                    classify_middle_code 
                  ) c6 
                  on c1.location_code=c6.dc_code and c1.classify_middle_code=c6.classify_middle_code and c1.shop_code=c6.bmk_code 
              where c5.shop_code is not null and c6.bmk_code is not null 
             ) t1 
        ) t2 
  ) c3
  cross join 
  (select distinct calday as sdt 
  from csx_dim.csx_dim_basic_date 
  where calday>=regexp_replace(add_months(trunc(from_unixtime(unix_timestamp('${yes_sdt}','yyyyMMdd'),'yyyy-MM-dd'),'MM'),-2),'-','')  
  and calday<='${yes_sdt}' 
  ) c4 
  where c3.price_begin_date<=c4.sdt and c3.price_end_date>=c4.sdt 
group by 
  c4.sdt,
  c3.performance_region_name,
  c3.performance_province_name,
  c3.performance_city_name,
  c3.classify_large_name,
  c3.classify_middle_name,
  c3.product_code,
  c3.shop_code  
;
-- ------------------------------------------------------------------------------------------
-- ---------市调数据最终表(24.8.23最终表)
drop table if exists csx_analyse_tmp.last_week_avg_shop_price_11_cost_vs_market_final; 
create table if not exists csx_analyse_tmp.last_week_avg_shop_price_11_cost_vs_market_final as 
select 
    b.*,
    row_number()over(partition by b.sdt,b.performance_city_name,b.product_code order by b.shop_code desc) as pm 
from 
    (select 
      a.sdt,
      a.performance_region_name,
      a.performance_province_name,
      a.performance_city_name,
      a.classify_large_name,
      a.classify_middle_name,
      a.product_code,
      a.shop_code as shop_code,
      a.shop_name as shop_name,
      avg(a.last_week_market_research_price) as last_week_market_research_price 
    from 
      (select 
          * 
      from csx_analyse_tmp.last_week_avg_shop_price_11_cost_vs_market 
      union all 
      select 
          sdt,
          performance_region_name,
          performance_province_name,
          '黔江区' as performance_city_name,
          classify_large_name,
          classify_middle_name,
          product_code,
          shop_code,
          shop_name,
          last_week_market_research_price 
      from csx_analyse_tmp.last_week_avg_shop_price_11_cost_vs_market 
      where performance_city_name='重庆主城'
      union all 
      select 
          sdt,
          performance_region_name,
          performance_province_name,
          '万州区' as performance_city_name,
          classify_large_name,
          classify_middle_name,
          product_code,
          shop_code,
          shop_name,
          last_week_market_research_price 
      from csx_analyse_tmp.last_week_avg_shop_price_11_cost_vs_market 
      where performance_city_name='重庆主城'
      union all 
      select 
          sdt,
          performance_region_name,
          performance_province_name,
          '长寿区' as performance_city_name,
          classify_large_name,
          classify_middle_name,
          product_code,
          shop_code,
          shop_name,
          last_week_market_research_price 
      from csx_analyse_tmp.last_week_avg_shop_price_11_cost_vs_market 
      where performance_city_name='重庆主城'
      ) a 
    group by 
      a.sdt,
      a.performance_region_name,
      a.performance_province_name,
      a.performance_city_name,
      a.classify_large_name,
      a.classify_middle_name,
      a.product_code,
      a.shop_code,
      a.shop_name
    ) b 
;  
  
  

-- 结果数据

select a.*
from 
	(select 
		a.performance_region_name,
		a.performance_province_name,
		a.performance_city_name,
		a.customer_code,
		a.customer_name,
		a.classify_large_name,
		a.classify_middle_name,
		a.classify_small_name,
		a.goods_code,
		a.goods_name,
		-- a.sale_qty_sq,
		-- a.sale_amt_sq,
		-- a.profit_sq,
		-- a.profit_sq/abs(a.sale_amt_sq) as profit_rate_sq,
		-- a.sale_amt_sq/a.sale_qty_sq as sale_price_sq,
		-- d.last_week_avg_market_price,
		-- (a.sale_amt_sq-a.profit_sq)/a.sale_qty_sq as cost_price_sq,
			
		a.sale_qty_bq,
		a.sale_amt_bq,
		a.profit_bq,
		a.profit_bq/abs(a.sale_amt_bq) as profit_rate_bq,
		a.sale_amt_bq/a.sale_qty_bq as sale_price_bq,
		d.this_week_avg_market_price,
		(a.sale_amt_bq-a.profit_bq)/a.sale_qty_bq as cost_price_bq,
		e.receive_price as avg_receive_price_week,

		-- b.performance_city_name as performance_city_name_dq_min,
		-- b.customer_code as customer_code_dq_min,
		-- b.customer_name as customer_name_dq_min,
		-- b.avg_cb as avg_cb_dq_min, -- 大区最低成本  -- 销售订单平均成本
		
		f.performance_city_name as performance_city_name_dqmin,
		f.receive_price as receive_price_dqmin -- 大区最低成本	-- 入库成本
			
	from 
	(select * from csx_analyse_tmp.csx_analyse_tmp_cust_goods_sale
	) a
	left join csx_analyse_tmp.csx_analyse_tmp_cust_goods_sale_mincost_1 b 
	on a.performance_region_name=b.performance_region_name and a.goods_code=b.goods_code
	
	-- 最近一天的入库价
	-- left join csx_analyse_tmp.tmp_dc_goods_received_sdt_last c on a.performance_region_name=c.performance_region_name and a.goods_code=c.goods_code
	
	-- 市调价
	left join
	(select 
		performance_region_name,
		performance_province_name,
		performance_city_name,		
		max(shop_code) as shop_code,
		max(shop_name) as shop_name,
		product_code,
		avg(case when sdt>='${sq01}' and  sdt<='${sq02}' then last_week_market_research_price end) as last_week_avg_market_price, 		
		avg(case when sdt>='${bq01}' and  sdt<='${yes_sdt}'  then last_week_market_research_price end) as this_week_avg_market_price 
	from csx_analyse_tmp.last_week_avg_shop_price_11_cost_vs_market_final 
	where pm=1 
	group by 
		performance_region_name,
		performance_province_name,
		performance_city_name,
		product_code
	) d on a.performance_city_name=d.performance_city_name and a.goods_code=d.product_code
	
	left join csx_analyse_tmp.tmp_dc_goods_received_week e on a.performance_city_name=e.performance_city_name and a.goods_code=e.goods_code
	left join csx_analyse_tmp.tmp_dc_goods_received_week_dqmin f on a.performance_region_name=f.performance_region_name and a.goods_code=f.goods_code
	
	)a
where sale_qty_bq is not null;