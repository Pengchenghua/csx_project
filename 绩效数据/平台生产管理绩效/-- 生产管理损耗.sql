-- 生产管理损耗
WITH tmp_factory_loss as
(
	select  plan_month,
	        inventory_dc_province_name,
	        inventory_dc_city_name,
	        dc_code,
	        dc_name,
	        a.goods_code,
	        b.goods_name,
	        b.classify_large_code,
	        b.classify_large_name,
	        b.classify_middle_code,
	        b.classify_middle_name,
	        workshop_code,
	        workshop_name,
	        zero_qty,
	        raw_material_used_qty,
	        production_loss_qty,
	        quantity_loss_qty,
	        inventory_difference_qty,
	        qty_loss_rate,
	        qty_loss_rate_chain,
	        raw_material_used_amt,
	        production_loss_amt,
	        quantity_loss_amt,
	        inventory_difference_amt,
	        amt_loss_rate,
	        amt_loss_rate_chain,
	        raw_material_sum_cost,
	        raw_material_unit smt
	from csx_report.csx_report_factory_single_product_loss_mi a
	JOIN
	(
		select  goods_code,
		        goods_name,
		        classify_large_code,
		        classify_large_name,
		        classify_middle_code,
		        classify_middle_name
		from csx_dim.csx_dim_basic_goods
		where sdt = 'current'
	) b
	on a.goods_code = b.goods_code
	left join
	( -- 获取库存地点信息 
		select  shop_code,
		        shop_name     as inventory_dc_name,
		        province_code as inventory_dc_province_code,
		        province_name as inventory_dc_province_name,
		        city_code     as inventory_dc_city_code,
		        city_name     as inventory_dc_city_name
		from csx_dim.csx_dim_shop
	) c
	on a.dc_code = c.shop_code
	where a.smt = '202502'
	and workshop_name in ('干货车间',  '调理品车间',  '蔬果车间' , '净菜车间',  '肉禽产车间')
	and dc_code in ('W0AR'
,'W088'
,'W0R8'
,'W080'
,'W0P3'
,'W0BT'
,'W039'
,'W079'
,'W0S9'
,'W053'
,'W0BJ'
,'W048'
,'W0Q8'
,'W0BG'
,'W0P6'
,'W0BZ'
,'WA93'
,'WB00'
,'W0AZ'
,'WB03'
,'WB04'
       )
       )
select  plan_month,
        inventory_dc_province_name,
        inventory_dc_city_name,
        classify_large_name,
        classify_middle_name,
        sum(production_loss_amt) production_loss_amt,
        sum(nvl(quantity_loss_amt,0)) quantity_loss_amt,
        sum(nvl(inventory_difference_amt,0))inventory_difference_amt,
        sum(raw_material_sum_cost) raw_material_sum_cost,
        sum(production_loss_amt) + sum(nvl(quantity_loss_amt,0)) - sum(nvl(inventory_difference_amt,0)) loss_amt,
        sum(raw_material_sum_cost) + sum(nvl(quantity_loss_amt,0)) - sum(nvl(inventory_difference_amt,0)) rwa_amt,
        (( sum(production_loss_amt) + sum(nvl(quantity_loss_amt,0)) - sum(nvl(inventory_difference_amt,0)) ) / ( sum(raw_material_sum_cost) + sum(nvl(quantity_loss_amt,0)) - sum(nvl(inventory_difference_amt,0))) ) as amt_loss_rate
from tmp_factory_loss
group by  plan_month,
         inventory_dc_province_name,
        inventory_dc_city_name,
        classify_large_name,
        classify_middle_name
;


    -- 生产管理\生产管理--生产计划分析.sql
WITH tmp_factory_detail as
(
	select  substr(sdt,1,6) s_month
,-- basic_performance_provnce_code, 
	        inventory_dc_province_name,
	        inventory_dc_city_name,
	        location_code,
	        inventory_dc_name,
	        sale_order_code,
	        product_code,
	        goods_name,
	        classify_large_code,
	        classify_large_name,
	        classify_middle_code,
	        classify_middle_name,
	        plan_date,
	        produced_date,
	        sale_channel,
	        split_group_name,
	        split_group_code,
	        customer_code,
	        customer_name,
	        sub_customer_code,
	        sub_customer_name,
	        plan_qty,
	        delivery_qty,
	        update_by,
	        update_time,
	        create_by,
	        create_time,
	        unit,
	        spec_max,
	        conver_plan_qty,
	        conver_delivey_qty,
	        fill_rate,
	        workshop_code,
	        workshop_name
	from csx_dwd.csx_dwd_factory_workshop_delivery_detail_di a
	JOIN
	(
		select  goods_code,
		        goods_name,
		        classify_large_code,
		        classify_large_name,
		        classify_middle_code,
		        classify_middle_name
		from csx_dim.csx_dim_basic_goods
		where sdt = 'current'
	) b
	on a.product_code = b.goods_code
	left join
	(
		-- 获取库存地点信息 
		select  shop_code,
		        shop_name     as inventory_dc_name,
		        province_code as inventory_dc_province_code,
		        province_name as inventory_dc_province_name,
		        city_code     as inventory_dc_city_code,
		        city_name     as inventory_dc_city_name
		from csx_dim.csx_dim_shop
		where sdt = 'current' 
	) c
	on a.location_code = c.shop_code
	where sdt >= regexp_replace(trunc('${sdt_yes_date}', 'MM'), '-', '')
	and sdt <= regexp_replace('${sdt_yes_date}', '-', '') 
	and sale_order_code like 'OM%'
  and  location_code in ('W0AR'
,'W088'
,'W0R8'
,'W080'
,'W0P3'
,'W0BT'
,'W039'
,'W079'
,'W0S9'
,'W053'
,'W0BJ'
,'W048'
,'W0Q8'
,'W0BG'
,'W0P6'
,'W0BZ'
,'WA93'
,'WB00'
,'W0AZ'
,'WB03'
,'WB04'
       )
)select  s_month,
        inventory_dc_province_name,
        inventory_dc_city_name,
        -- classify_large_code,
        classify_large_name,
        -- classify_middle_code,
        classify_middle_name,
        sum(if(coalesce(plan_qty ,0) != 0,1 ,0)) as all_sku,
        sum(if(coalesce(delivery_qty ,0) > 0,1 ,0)) as real_sku
from tmp_factory_detail
group by  classify_large_code,
          classify_large_name,
          classify_middle_code,
          classify_middle_name,
          s_month,
          inventory_dc_province_name,
          inventory_dc_city_name
;


-- csx_report.csx_report_factory_single_product_loss_mi 
-- 日配销售额
select substr(sdt,1,6) as sales_months,
    performance_region_name,
    performance_province_name,
    inventory_dc_city_name,
    case when b.classify_large_code in ('B04','B05','B06','B07','B08','B09') then '食百'else b.classify_large_name end classify_large_name,
    b.classify_middle_code,
    b.classify_middle_name,
    sum(sale_amt)sale_amt,
    sum(profit) profit
from      csx_dws.csx_dws_sale_detail_di a
left join 
(select goods_code,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name
    from csx_dim.csx_dim_basic_goods 
    where sdt='current'
   
) b on a.goods_code=b.goods_code
left join 
	(select shop_code,shop_low_profit_flag from csx_dim.csx_dim_shop where sdt='current') c on a.inventory_dc_code=c.shop_code
  where sdt >=  regexp_replace(trunc('${sdt_yes_date}','MM'),'-','') 
    and sdt <=  regexp_replace('${sdt_yes_date}','-','') 
     and is_factory_goods_flag=1
    and business_type_code='1'
    and inventory_dc_code in ('W0AS'
,'W0A5'
,'W0R9'
,'W0A2'
,'W0N0'
,'W0BR'
,'W0A7'
,'W0A6'
,'W0Q2'
,'W0A8'
,'W0BK'
,'W0A3'
,'W0Q9'
,'W0BH'
,'W0P8'

  )
    -- and  direct_delivery_type in ('0','11','12','16','17')     -- 日配-采购管理  --剔除 18-委外（供应链指定）
group by  case when b.classify_large_code in ('B04','B05','B06','B07','B08','B09') then '食百'else b.classify_large_name end ,
    b.classify_middle_code,
    b.classify_middle_name,
    business_type_code,
    business_type_name,
     substr(sdt,1,6),
     performance_province_name,
     performance_region_name,
     inventory_dc_city_name
     
  ;