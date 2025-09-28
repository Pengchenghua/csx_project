-- 商品销售&入库入销占比分析
-- 管理中类分析
with sale as (select 
  inventory_dc_code,
  goods_code,
  goods_name,
  classify_large_code,
  classify_large_name,
  classify_middle_code,
  classify_middle_name,
  sum(sale_cost) sale_cost,
  sum(sale_amt) as sale_amt,
  sum(profit) profit,
  sum(sale_qty) sale_qty
from 
    csx_analyse.csx_analyse_bi_sale_detail_di a 
where
  sdt >= '20231001'
  and sdt <= '20231031'
  and business_type_code=1 
  and shop_low_profit_flag =0 
  group by 
  inventory_dc_code,
  goods_code,
  classify_large_code,
  classify_large_name,
  classify_middle_code,
  classify_middle_name,
  goods_name
 ),
  -- 入库减供应商退货作为最终入库
	purach as (	select * ,
			cast(received_amount_all/received_qty_all as decimal(20,6)) as received_price_avg
		from 
		(select 
			b1.target_location_code,
			b1.goods_code,
			b4.classify_large_code,
            b4.classify_large_name,
            b4.classify_middle_code,
            b4.classify_middle_name,
			sum((case when b1.received_amount<0 then 0 else if(b2.received_amount is not null and b3.shipped_amount is null,b2.received_amount,b1.received_amount) end)
				-nvl((case when b3.shipped_amount<0 then 0 else b3.shipped_amount end),0)) as received_amount_all,
			sum((case when b1.received_qty<0 then 0 else if(b2.received_qty is not null and b3.shipped_amount is null,b2.received_qty,b1.received_qty) end)
				-nvl((case when b3.shipped_qty<0 then 0 else b3.shipped_qty end),0)) as received_qty_all,
			cast(max(case when b1.received_amount<0 then 0 
				else if(b2.received_amount is not null,b2.received_price2,
						if(coalesce(b1.received_price2,0)=0,b1.received_price1,b1.received_price2)) end)
			as decimal(20,6)) as received_price_max			
		from 
			-- 入库数据
			(
				select target_location_code, 
					order_code,goods_code,sdt,
					sum(received_amount) received_amount,
					sum(received_qty) received_qty,
					sum(received_amount)/sum(received_qty) as received_price1,
					max(received_price2) received_price2
				from   csx_dws.csx_dws_scm_order_received_di 
				where sdt>='20231001'
				and sdt<='20231031' 
				and super_class in (1,3) -- 加上调拨入库的数据 供应商订单
				and header_status=4 
				and source_type not in (4,15,18) -- 剔除项目合伙人
				-- and local_purchase_flag='0' -- 剔除地采，是否地采(0-否、1-是)
				-- and direct_delivery_type='0' -- 直送类型 0-P(普通) 1-R(融单)、2-Z(过账)
				-- and target_location_code in ('W0BK')
				group by target_location_code,order_code,goods_code,sdt
			) b1 
			-- 关联价格补救订单数据，如果有价格补救则成本取补救单中的价格
			left join 
			(
				select original_order_code,goods_code,sum(received_amount) as received_amount ,sum(received_qty) received_qty,max(received_price2) received_price2
				from csx_dws.csx_dws_scm_order_received_di 
				where  sdt>='20231001'
				and sdt<='20231031' 
				 and target_location_code in ('W0BK') 
				and price_remedy_flag=1 
				group by original_order_code,goods_code
			) b2 on b1.order_code=b2.original_order_code and b1.goods_code=b2.goods_code 
			-- 关联供应商退货订单
			left join 
			(
				select original_order_code, goods_code,sum(shipped_amount) shipped_amount,sum(shipped_qty)shipped_qty
				from csx_dws.csx_dws_scm_order_shipped_di   
				where  sdt>='20231001'
				and sdt<='20231031' 
				and super_class in (2) 
				-- and target_location_code in ('W0BK') 
				group by original_order_code, goods_code
			) b3 on b1.order_code=b3.original_order_code and b1.goods_code=b3.goods_code 
			left join 
			(
				select * 
				from csx_dim.csx_dim_basic_goods 
				where sdt='current' 
			) b4 on b1.goods_code=b4.goods_code 
		-- where b2.original_order_code is null 
		where 1=1
		group by b1.target_location_code,b1.goods_code,
		b4.classify_large_code,
        b4.classify_large_name,
        b4.classify_middle_code,
        b4.classify_middle_name
		) a 
		)
select  basic_performance_region_code,	basic_performance_region_name	,basic_performance_province_code,	basic_performance_province_name,	basic_performance_city_code	,basic_performance_city_name,
  inventory_dc_code,shop_name,
  a.classify_large_code,
  a.classify_large_name,
  a.classify_middle_code,
  a.classify_middle_name,
  sum(sale_cost) sale_cost,
  sum(sale_amt) as sale_amt,
  sum(profit) profit,
   sum(sale_qty) sale_qty,
  sum(received_amount_all)received_amount_all,
  sum(received_qty_all)received_qty_all
 from 
 (select 
   
  inventory_dc_code,
  classify_large_code,
  classify_large_name,
  classify_middle_code,
  classify_middle_name,
  sum(sale_cost) sale_cost,
  sum(sale_amt) as sale_amt,
  sum(profit) profit,
  sum(sale_qty) sale_qty
from 
    sale a 
where 1=1 
  group by  
   inventory_dc_code,
   classify_large_code,
   classify_large_name,
   classify_middle_code,
   classify_middle_name
    
 ) a 
left join 
(select 
		target_location_code,
		classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name,
		sum(received_amount_all) as received_amount_all,
		sum(received_qty_all) as received_qty_all
	from purach b
group by 	target_location_code,
			 classify_large_code,
             classify_large_name,
             classify_middle_code,
             classify_middle_name
)b on a.inventory_dc_code=b.target_location_code and a.classify_middle_code=b.classify_middle_code 
left join 
(select basic_performance_region_code,	basic_performance_region_name	,basic_performance_province_code,	basic_performance_province_name,	basic_performance_city_code	,basic_performance_city_name,shop_code,shop_name	 from csx_dim.csx_dim_shop where sdt='current') c on a.inventory_dc_code=c.shop_code
group by basic_performance_region_code,	basic_performance_region_name	,basic_performance_province_code,	basic_performance_province_name,	basic_performance_city_code	,basic_performance_city_name,
  inventory_dc_code,shop_name,
  a.classify_large_code,
  a.classify_large_name,
  a.classify_middle_code,
  a.classify_middle_name
   
  ; 


-- 商品入销分析按照主数据管理大区归属

-- 商品入销分析按照主数据管理大区归属
with sale as (select 
  inventory_dc_code,
  goods_code,
  goods_name,
  classify_large_code,
  classify_large_name,
  classify_middle_code,
  classify_middle_name,
  sum(sale_cost) sale_cost,
  sum(sale_amt) as sale_amt,
  sum(profit) profit,
  sum(sale_qty)sale_qty
from 
    csx_analyse.csx_analyse_bi_sale_detail_di a 
where
  sdt >= '20231001'
  and sdt <= '20231031'
  and business_type_code!=4
   and channel_code !=2
  and shop_low_profit_flag =0 
  group by  
  inventory_dc_code,
  goods_code,
  classify_large_code,
  classify_large_name,
  classify_middle_code,
  classify_middle_name,
  goods_name
 ),
  
  -- 入库减供应商退货作为最终入库
	purach as (	select * ,
			cast(received_amount_all/received_qty_all as decimal(20,6)) as received_price_avg
		from 
		(select 
			b1.target_location_code,
			b1.goods_code,
			b1.goods_name,
			b4.classify_large_code,
            b4.classify_large_name,
            b4.classify_middle_code,
            b4.classify_middle_name,
			sum((case when b1.received_amount<0 then 0 else if(b2.received_amount is not null and b3.shipped_amount is null,b2.received_amount,b1.received_amount) end)
				-nvl((case when b3.shipped_amount<0 then 0 else b3.shipped_amount end),0)) as received_amount_all,
			sum((case when b1.received_qty<0 then 0 else if(b2.received_qty is not null and b3.shipped_amount is null,b2.received_qty,b1.received_qty) end)
				-nvl((case when b3.shipped_qty<0 then 0 else b3.shipped_qty end),0)) as received_qty_all,
			cast(max(case when b1.received_amount<0 then 0 
				else if(b2.received_amount is not null,b2.received_price2,
						if(coalesce(b1.received_price2,0)=0,b1.received_price1,b1.received_price2)) end)
			as decimal(20,6)) as received_price_max			
		from 
			-- 入库数据
			(
				select target_location_code, 
					order_code,goods_code,goods_name,sdt,
					sum(received_amount) received_amount,
					sum(received_qty) received_qty,
					sum(received_amount)/sum(received_qty) as received_price1,
					max(received_price2) received_price2
				from   csx_dws.csx_dws_scm_order_received_di 
				where sdt>='20231001'
				and sdt<='20231031' 
				and super_class in (1,3) -- 加上调拨入库的数据 供应商订单
				and header_status=4 
				and source_type not in (4,15,18) -- 剔除项目合伙人
				-- and local_purchase_flag='0' -- 剔除地采，是否地采(0-否、1-是)
				-- and direct_delivery_type='0' -- 直送类型 0-P(普通) 1-R(融单)、2-Z(过账)
				-- and target_location_code in ('W0BK')
				group by target_location_code,order_code,goods_code,sdt,goods_name
			) b1 
			-- 关联价格补救订单数据，如果有价格补救则成本取补救单中的价格
			left join 
			(
				select original_order_code,goods_code,sum(received_amount) as received_amount ,sum(received_qty) received_qty,max(received_price2) received_price2
				from csx_dws.csx_dws_scm_order_received_di 
				where  sdt>='20231001'
				and sdt<='20231031' 
				-- and target_location_code in ('W0BK') 
				and price_remedy_flag=1 
				group by original_order_code,goods_code
			) b2 on b1.order_code=b2.original_order_code and b1.goods_code=b2.goods_code 
			-- 关联供应商退货订单
			left join 
			(
				select original_order_code, goods_code,sum(shipped_amount) shipped_amount,sum(shipped_qty)shipped_qty
				from csx_dws.csx_dws_scm_order_shipped_di   
				where  sdt>='20231001'
				and sdt<='20231031' 
				and super_class in (2) 
				-- and target_location_code in ('W0BK') 
				group by original_order_code, goods_code
			) b3 on b1.order_code=b3.original_order_code and b1.goods_code=b3.goods_code 
			left join 
			(
				select * 
				from csx_dim.csx_dim_basic_goods 
				where sdt='current' 
			) b4 on b1.goods_code=b4.goods_code 
		-- where b2.original_order_code is null 
		where 1=1
		group by b1.target_location_code,b1.goods_code,
		b4.classify_large_code,
        b4.classify_large_name,
        b4.classify_middle_code,
        b4.classify_middle_name,
        b1.goods_name
		) a 
		)
		,
		stock as (select dc_area_code,goods_code, (stock_qty)stock_qty, (stock_amt) stock_amt ,last_receive_date,last_receive_qty,last_receive_amt,last_unmoved_date ,unsold_flag,nearly30days_transfer from  csx_report.csx_report_cas_accounting_turnover_stock_cost_goods_detail_df_new where sdt='20231109')
		
select basic_performance_region_code,	basic_performance_region_name	,basic_performance_province_code,	basic_performance_province_name,	basic_performance_city_code	,basic_performance_city_name,
  inventory_dc_code,shop_name,
  a.goods_code,
  a.goods_name,
  a.classify_large_code,
  a.classify_large_name,
  a.classify_middle_code,
  a.classify_middle_name,
  (sale_cost) sale_cost,
  (sale_amt) as sale_amt,
  (profit) profit,
  (sale_qty)sale_qty,
  received_amount_all,
  received_qty_all,
   (stock_qty)stock_qty, (stock_amt) stock_amt ,last_receive_date,last_receive_qty,last_receive_amt,last_unmoved_date ,unsold_flag,nearly30days_transfer 
  from (
  
select basic_performance_region_code,	basic_performance_region_name	,basic_performance_province_code,	basic_performance_province_name,	basic_performance_city_code	,basic_performance_city_name,
  inventory_dc_code,shop_name,
  a.goods_code,
  a.goods_name,
  a.classify_large_code,
  a.classify_large_name,
  a.classify_middle_code,
  a.classify_middle_name,
   sum(sale_cost) sale_cost,
   sum(sale_amt) as sale_amt,
   sum(profit) profit,
   sum(sale_qty)sale_qty,
  sum(received_amount_all) received_amount_all,
  sum(received_qty_all) received_qty_all
  from (
select  
  inventory_dc_code,
  a.goods_code,
  a.goods_name,
  a.classify_large_code,
  a.classify_large_name,
  a.classify_middle_code,
  a.classify_middle_name,
   (sale_cost) sale_cost,
   (sale_amt) as sale_amt,
   (profit) profit,
   sale_qty,
  0 received_amount_all,
  0 received_qty_all
 from sale a 
union  all 
select  
  target_location_code inventory_dc_code,
  goods_code,
  goods_name,
  classify_large_code,
  classify_large_name,
  classify_middle_code,
  classify_middle_name,
  0 sale_cost,
  0 as sale_amt,
  0 profit,
  0 sale_qty,
  received_amount_all,
  received_qty_all
  from
  (select 
  target_location_code,
  a.goods_code,
  a.goods_name,
  a.classify_large_code,
  a.classify_large_name,
  a.classify_middle_code,
  a.classify_middle_name,
  received_amount_all,
  received_qty_all
  from purach a 
join 
(select  inventory_dc_code 
from sale b 
group by   inventory_dc_code) b on a.target_location_code=b.inventory_dc_code
) b 
) a 
left join 
(select basic_performance_region_code,	basic_performance_region_name	,basic_performance_province_code,	basic_performance_province_name,	basic_performance_city_code	,basic_performance_city_name,shop_code,shop_name	 from csx_dim.csx_dim_shop where sdt='current') c on a.inventory_dc_code=c.shop_code
group by  basic_performance_region_code,	basic_performance_region_name	,basic_performance_province_code,	basic_performance_province_name,	basic_performance_city_code	,basic_performance_city_name,
  shop_name,
  inventory_dc_code,
  goods_code,
  goods_name,
  classify_large_code,
  classify_large_name,
  classify_middle_code,
  classify_middle_name
   ) a 
   left join 
   stock b on a.inventory_dc_code=b.dc_area_code and a.goods_code=b.goods_code