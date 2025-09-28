

TOP30TOP商品成本 把每个客户top单品品类拉出来，横向对比大区最低价在哪里同步出来，采购确认怎么优化成本或者明确背靠背目标



and customer_code in ('222798','252181','258261','124524','255475','126387','233646','255101','121061','131129','237905','126377','115769','249942','131187','224985','256667','241458','223283','220106','106775','249548','250259','250879','247826','128359','252038','236853','128371','163315')



-- -- 各客户商品销售
drop table if exists csx_analyse_tmp.csx_analyse_tmp_cust_goods_sale_tmp;
create table csx_analyse_tmp.csx_analyse_tmp_cust_goods_sale_tmp as 
with cust_goods_sale as
(
select 
        a.performance_region_name,
        a.performance_province_name,
        a.performance_city_name,
        -- a.inventory_dc_code,
        a.customer_code,
        d.customer_name,
        -- a.sub_customer_code,
        -- max(a.sub_customer_name) as sub_customer_name,
        f.rp_service_user_work_no_new,
        f.rp_service_user_name_new,
        e.classify_large_code,
        e.classify_large_name,
        e.classify_middle_code,
        e.classify_middle_name,
        e.classify_small_code,
        e.classify_small_name,
        a.goods_code,
        e.goods_name,
        e.unit_name,
    
        -- sum(case when a.sdt>=regexp_replace(add_months(trunc('${yes_date}','MM'),-1),'-','') and a.sdt<regexp_replace(trunc('${yes_date}','MM'),'-','') then a.sale_qty end) as sale_qty_sq,
        -- sum(case when a.sdt>=regexp_replace(add_months(trunc('${yes_date}','MM'),-1),'-','') and a.sdt<regexp_replace(trunc('${yes_date}','MM'),'-','') then a.sale_amt end) as sale_amt_sq,
        -- sum(case when a.sdt>=regexp_replace(add_months(trunc('${yes_date}','MM'),-1),'-','') and a.sdt<regexp_replace(trunc('${yes_date}','MM'),'-','') then a.profit end) as profit_sq,

        sum(case when a.sdt>=regexp_replace(trunc('${yes_date}','MM'),'-','') and a.sdt<='${yes_sdt}' then a.sale_qty end) as sale_qty_bq,
        sum(case when a.sdt>=regexp_replace(trunc('${yes_date}','MM'),'-','') and a.sdt<='${yes_sdt}' then a.sale_amt end) as sale_amt_bq,
        sum(case when a.sdt>=regexp_replace(trunc('${yes_date}','MM'),'-','') and a.sdt<='${yes_sdt}' then a.profit end) as profit_bq,
		
        sum(case when a.sdt>=regexp_replace(date_add(from_unixtime(unix_timestamp('${yes_sdt}','yyyyMMdd'),'yyyy-MM-dd'),-6),'-','') and a.sdt<='${yes_sdt}' then a.sale_qty end) as sale_qty_7d,
        sum(case when a.sdt>=regexp_replace(date_add(from_unixtime(unix_timestamp('${yes_sdt}','yyyyMMdd'),'yyyy-MM-dd'),-6),'-','') and a.sdt<='${yes_sdt}' then a.sale_amt end) as sale_amt_7d,
        sum(case when a.sdt>=regexp_replace(date_add(from_unixtime(unix_timestamp('${yes_sdt}','yyyyMMdd'),'yyyy-MM-dd'),-6),'-','') and a.sdt<='${yes_sdt}' then a.profit end) as profit_7d	
    from 
        (select * 
        from csx_dws.csx_dws_sale_detail_di  
        where sdt>=regexp_replace(add_months(trunc('${yes_date}','MM'),-0),'-','')  
        and sdt<='${yes_sdt}'    
        and business_type_code=1  
        and order_channel_code not in ('4','6','5') -- 剔除所有异常
        and refund_order_flag<>1 
		and delivery_type_name<>'直送'
        and shipper_code='YHCSX' 
        and customer_code in ('222798','252181','258261','124524','255475','126387','233646','255101','121061','131129','237905','126377','115769','249942','131187','224985','256667','241458','223283','220106','106775','249548','250259','250879','247826','128359','252038','236853','128371','163315')
        ) a 
        left join 
        csx_analyse_tmp.csx_analyse_tmp_abnormal_goods_ky_target_month c 
        on a.performance_region_name=c.performance_region_name and a.performance_province_name=c.performance_province_name and a.performance_city_name=c.performance_city_name and a.goods_code=c.goods_code 
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
        -- -----服务管家
        (select * 
        from csx_analyse.csx_analyse_report_crm_customer_sale_service_manager_info_df    
        where sdt='${yes_sdt}' 
        ) f 
        on a.customer_code=f.customer_no 
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
    -- and c.goods_code is null 
    group by 
        a.performance_region_name,
        a.performance_province_name,
        a.performance_city_name,
        -- a.inventory_dc_code,
        a.customer_code,
        d.customer_name,
        -- a.sub_customer_code,
        e.classify_large_code,
        e.classify_large_name,
        e.classify_middle_code,
        e.classify_middle_name,
        e.classify_small_code,
        e.classify_small_name,
        a.goods_code,
        e.goods_name,
        e.unit_name,
        f.rp_service_user_work_no_new,
        f.rp_service_user_name_new 
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

select a.*,
a.sale_amt_bq/b.sale_amt_bq_all as sale_amt_bq_zb,
b.sale_amt_bq_all,
b.profit_bq_all/abs(sale_amt_bq_all) as profit_rate_bq_all
from 
(
select *,
  row_number() over(partition by customer_code order by nvl(sale_amt_bq,0) desc) as rno
from cust_goods_sale
)a 
left join cust_sale b on a.customer_code=b.customer_code
-- where a.rno<=100
order by customer_code,rno
;




-- 入库成本价
drop table csx_analyse_tmp.tmp_dc_goods_received_sdt; 
create table csx_analyse_tmp.tmp_dc_goods_received_sdt as 	
    select 
	  a.performance_region_code,
	  a.performance_region_name,
	  a.performance_province_code,
	  a.performance_province_name,	
	  a.performance_city_code,
	  a.performance_city_name,	  
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
	  a.business_type_name,  -- 业务类型名称	  
	  a.sdt,
      a.order_price*a.receive_qty as receive_amt,
      a.receive_qty,
	  -- a.receive_price,
      a.order_price 
    from 
      -- 入库数据
      (
        select 
		  performance_region_code,
		  performance_region_name,  -- 大区名称
		  -- province_code as performance_province_code,
		  -- province_name as performance_province_name,  -- 业绩省区名称
		  -- city_code as performance_city_code,
		  -- city_name as performance_city_name,  -- 业绩城市名称
		  performance_province_code,
		  performance_province_name,  -- 业绩省区名称
		  performance_city_code,
		  performance_city_name,  -- 业绩城市名称		  
		  source_type_code,  -- 来源采购订单类型
		  source_type_name,  -- 来源采购订单名称
		  super_class_code,  -- 单据类型编码
		  super_class_name,  -- 单据类型名称
		  purchase_order_code,  -- 采购订单号
		  order_code,  -- 入库/出库单号
		  dc_code,  -- dc编码
		  dc_name,  -- dc名称
		  goods_code,  -- 商品编码
		  goods_name,  -- 商品名称
		  unit_name,  -- 单位
		  classify_large_name,  -- 管理一级名称
		  classify_middle_name,  -- 管理二级名称
		  supplier_code,  -- 供应商编码
		  supplier_name,  -- 供应商名称
		  local_purchase_flag,  -- 是否地采
		  business_type_name,  -- 业务类型名称
		  sdt,
		  order_price1,  -- 单价1
		  order_price2,  -- 单价2
		  receive_qty,  -- 库数量
		  receive_amt,  -- 库金额
		  -- if (order_price2 = 0, order_price1, order_price2) as order_price,
		  order_price1 as order_price,
		  urgency_flag,  -- 紧急补货
		  order_type  -- 订单类型(0-普通供应商订单 1-囤货订单 2-日采订单 3-计划订单)		  
        from csx_analyse.csx_analyse_scm_purchase_order_flow_di 
		where sdt>=regexp_replace(date_add(from_unixtime(unix_timestamp('${yes_sdt}','yyyyMMdd'),'yyyy-MM-dd'),-30),'-','')
		and sdt<='${yes_sdt}' 
        -- where sdt='${yes_sdt}' 
		and remedy_flag <> '1'  -- 补救标识(原单号、退货单号标识)新采购单未标识 剔除价格补救单，以防计算成本价错误
		-- and is_supply_stock_tag = '1'  -- 是否集采仓
		and super_class_code = '1'  -- 单据类型编码:供应商订单
		and navy_order_flag ='0' -- #是否海军订单 0-否,1-是
		and direct_delivery_type=0  -- 	#直送类型 0-P(普通) 1-R(融单)、2-Z(过账)
		and (source_type_code in ('1','10','19','23')   -- 来源采购订单类型
		and order_goods_status in (1,2,3,4)) --   头表状态(1-已创建、2-已发货、3-部分入库、4-已完成、5-已取消)
        and order_price1>0 and receive_qty>0 
      ) a 
    -- 关联价格补救订单数据 
    left join 
    (select 
        * 
     from csx_dws.csx_dws_scm_order_received_di 
     where sdt>=regexp_replace(date_add(from_unixtime(unix_timestamp('${yes_sdt}','yyyyMMdd'),'yyyy-MM-dd'),-30),'-','') 
     and sdt<='${yes_sdt}' 
     and price_remedy_flag=1 
    ) a2 on a.order_code=a2.original_order_code and a.goods_code=a2.goods_code 	  
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
		basic_performance_region_code performance_region_code,
		basic_performance_region_name performance_region_name,
		shop_code ,
		shop_name ,
		company_code ,
		company_name ,
		city_code,
		city_name,
		province_code,
		province_name,
		purpose,
		purpose_name,
		basic_performance_city_code as performance_city_code,
		basic_performance_city_name as performance_city_name,
		basic_performance_province_code as performance_province_code,
		basic_performance_province_name as performance_province_name
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


-- 昨日入库成本，若该商品无入库, 生鲜往前追溯7天, 食百30天, 取追溯时间段内最后一天数据
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
  business_type_name,  -- 业务类型名称	  
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
  business_type_name,  -- 业务类型名称	  
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

-- 大区最低入库成本
-- select a.*,
-- 	b.performance_city_name as performance_city_name_dqmin,
-- 	b.receive_price as receive_price_dqmin -- 大区最低成本
-- from receive_goods_sdt_last a
-- 	left join 
-- 	(
-- 	select b1.* 
-- 	from 
-- 		(select *,
-- 			row_number()over(partition by performance_region_name,goods_code order by receive_price asc) as rn	 -- 按大区排名
-- 		from receive_goods_sdt_last 
-- 		)b1
-- 	where rn=1
-- 	) b on a.performance_region_name=b.performance_region_name and a.goods_code=b.goods_code 


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
	)b
;


-- 大区最低成本 销售单成本+入库成本
drop table if exists csx_analyse_tmp.csx_analyse_tmp_cust_goods_sale_mincost;
create table csx_analyse_tmp.csx_analyse_tmp_cust_goods_sale_mincost as 
select a.*,
	b.performance_city_name as performance_city_name_dq_min,
	b.customer_code as customer_code_dq_min,
	b.customer_name as customer_name_dq_min,
	b.avg_cost_price as avg_cb_dq_min, -- 大区最低成本
	
	c.performance_city_name_dqmin,
	c.receive_price_dqmin -- 大区最低成本	
from 
(
  select 
      performance_region_name,
      performance_province_name,
      performance_city_name,
      -- inventory_dc_code,
      customer_code,
      customer_name,
      -- rp_service_user_work_no_new,
      -- rp_service_user_name_new,
      -- classify_large_code,
      classify_large_name,
      -- classify_middle_code,
      classify_middle_name,
      -- classify_small_code,
      classify_small_name,
      goods_code,
      goods_name,
      unit_name,
	  -- rno,
      sale_qty_bq,
      sale_amt_bq,
      profit_bq,
	  profit_bq/abs(sale_amt_bq) as profit_rate_bq,
	  sale_amt_bq/sale_qty_bq as sale_price_bq,
	  (sale_amt_bq-profit_bq)/sale_qty_bq as cost_price_bq,
  	
      sale_qty_7d,
      sale_amt_7d,
      profit_7d,
	  profit_7d/abs(sale_amt_7d) as profit_rate_7d,
	  sale_amt_7d/sale_qty_7d as sale_price_7d,
	  (sale_amt_7d-profit_7d)/sale_qty_7d as cost_price_7d	  
  from csx_analyse_tmp.csx_analyse_tmp_cust_goods_sale_tmp		
)a 
-- 销售表中大区内最低成本价
left join 
(
select b1.* 
from 
	(select *,
		(sale_amt_7d-profit_7d)/sale_qty_7d as avg_cost_price,
		row_number()over(partition by performance_region_name,goods_code order by ((sale_amt_7d-profit_7d)/sale_qty_7d) asc,sale_amt_7d desc) as rn	 -- 按大区排名
	from csx_analyse_tmp.csx_analyse_tmp_cust_goods_sale_tmp
	)b1
where rn=1
) b on a.performance_region_name=b.performance_region_name and a.goods_code=b.goods_code 
left join csx_analyse_tmp.tmp_dc_goods_received_sdt_last c on a.performance_region_name=c.performance_region_name and a.goods_code=c.goods_code
;


-- 结果 
select * from csx_analyse_tmp.csx_analyse_tmp_cust_goods_sale_mincost;






