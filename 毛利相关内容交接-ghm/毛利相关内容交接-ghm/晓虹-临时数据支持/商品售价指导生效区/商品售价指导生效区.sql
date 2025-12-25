select
	warehouse_code
	,product_code
	,regexp_replace(product_name,'\n|\t|\r|\,|\"|\\\\n','')
	,product_unit
	,big_management_classify_name
	,mid_management_classify_name
	,small_management_classify_name
	,raw
	,working_cost
	,periodic_purchase_price
	,stock_cost_price
	,purchase_quotation
	,case 
		when purchase_price_type=1 then '采购报价' 
	    when purchase_price_type=2 then '库存成本价' 
		when purchase_price_type=3 then '供应链周期进价' 
		when purchase_price_type=4 then '数据系统成本' 
	 else purchase_price_type end as purchase_price_type
	,cost_price_latest
	,case 
		when cost_price_type_latest=1 then '采购报价' 
	    when cost_price_type_latest=2 then '库存成本价' 
		when cost_price_type_latest=3 then '供应链周期进价' 
		when cost_price_type_latest=4 then '数据系统成本' 
		
	 else cost_price_type_latest end as cost_price_type_latest
	,cost_price_latest_wave
	,suggest_price_high_latest
	,suggest_price_mid_latest
	,suggest_price_low_latest
	,suggest_price_high
	,suggest_price_mid
	,suggest_price_low
	,estimate_gross_margin
	,suggest_price_compare
	,market_price_diff
	,yh_shop_price
	,terminal_price
	,one_batch_price
	,two_batch_price
	,customer_avg_sales_money_30
	,customer_max_sales_money_30
	,case 
		when suggest_price_type =1 then '目标定价法'
		when suggest_price_type =2 then '市调价格'
		when suggest_price_type =3 then '手动导入'
		when suggest_price_type =4 then '固定价'
		when suggest_price_type =5 then '上期价格'
		when suggest_price_type =6 then '上期价格人工bom表'	
		when suggest_price_type =7 then '目标定价法人工bom表'	
	else suggest_price_type end suggest_price_type	
	,price_time
	,price_begin_time
	,price_end_time
	,create_by

from goods_price_guide
where is_expired = false -- 是否失效
	and substr(price_begin_time, 1, 10) <= current_date
    and substr(price_end_time, 1, 10) >= current_date;
	
	
------ 数仓表数据：	
select
	warehouse_code
	,product_code
	,regexp_replace(product_name,'\n|\t|\r|\,|\"|\\\\n','')product_name
	,product_unit
	,big_management_classify_name
	,mid_management_classify_name
	,small_management_classify_name
	,get_json_object(get_json_object(raw,'$[0]') ,'$.mProductPrice')raw
	,working_cost
	,periodic_purchase_price
	,stock_cost_price
	,purchase_quotation
	,case 
		when purchase_price_type=1 then '采购报价' 
	    when purchase_price_type=2 then '库存成本价' 
		when purchase_price_type=3 then '供应链周期进价' 
		when purchase_price_type=4 then '数据系统成本' 
	 else purchase_price_type end as purchase_price_type
	,cost_price_latest
	,case 
		when cost_price_type_latest=1 then '采购报价' 
	    when cost_price_type_latest=2 then '库存成本价' 
		when cost_price_type_latest=3 then '供应链周期进价' 
		when cost_price_type_latest=4 then '数据系统成本' 
		
	 else cost_price_type_latest end as cost_price_type_latest
	,cost_price_latest_wave
	,suggest_price_high_latest
	,suggest_price_mid_latest
	,suggest_price_low_latest
	,suggest_price_high
	,suggest_price_mid
	,suggest_price_low
	,estimate_gross_margin
	,suggest_price_compare  
	,market_price_diff
	,yh_shop_price
	,terminal_price
	,one_batch_price
	,two_batch_price
	,customer_avg_sales_money_30
	,customer_max_sales_money_30
	,case 
		when suggest_price_type =1 then '目标定价法'
		when suggest_price_type =2 then '市调价格'
		when suggest_price_type =3 then '手动导入'
		when suggest_price_type =4 then '固定价'
		when suggest_price_type =5 then '上期价格'
		when suggest_price_type =6 then '上期价格人工bom表'	
		when suggest_price_type =7 then '目标定价法人工bom表'	
	else suggest_price_type end suggest_price_type	
	,price_time
	,price_begin_time
	,price_end_time
	,create_by

from csx_dwd.csx_dwd_price_goods_price_guide_di
-- 失效数据：
where is_expired = true and sdt>='20250401' 
	and ((price_begin_time>='2025-05-01' and price_begin_time<='2025-06-23') or (price_end_time>='2025-05-01' and price_end_time<='2025-06-23'))

-- 生效数据：
where is_expired = false 
	and substr(price_begin_time, 1, 10) <= current_date
    and substr(price_end_time, 1, 10) >= current_date;