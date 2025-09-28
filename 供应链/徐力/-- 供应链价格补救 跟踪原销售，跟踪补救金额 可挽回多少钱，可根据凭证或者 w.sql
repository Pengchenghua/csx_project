-- 供应链价格补救 跟踪原销售，跟踪补救金额 可挽回多少钱，可根据凭证或者 wms_order_code 进行关联
-- 供应链价格补救 跟踪原销售，跟踪补救金额 可挽回多少钱，可根据凭证或者 wms_order_code 进行关联
select a.sdt,
    b.sdt as sale_sdt,
    a.performance_region_code, -- 大区编码
	a.performance_region_name, -- 大区名称
	a.performance_province_code, -- 省份编码
	a.performance_province_name, -- 省份名称
	a.performance_city_code, -- 城市编码
	a.performance_city_name, -- 城市名称,
	a.original_order_code,
	a.order_code,
	a.customer_code,
	a.customer_name,
	a.goods_code,
	a.goods_name,
	a.classify_large_code,
    a.classify_large_name,
    a.classify_middle_code,
    a.classify_middle_name,
    a.classify_small_code,
    a.classify_small_name,
	a.inventory_dc_code,
	a.cost_price,
-- 	a.sale_price,
-- 	a.sale_amt,
	a.profit,
	b.cost_price as original_cost,
	b.sale_price as original_price,
	b.sale_qty ,
	b.sale_amt original_sale_amt,
	b.profit original_profit,
	b.profit+a.profit new_profit,
	b.cost_price+a.cost_price new_cost
	from 
(	select * ,
		substr(sdt,1,6) smonth,
		weekofyear(date_sub(to_date(from_unixtime(unix_timestamp(sdt,'yyyyMMdd'))),-2)) week,
		if((order_channel_code in ('4','5','6') or refund_order_flag=1),original_order_code,order_code) as order_code_new
	from    csx_analyse.csx_analyse_bi_sale_detail_di
	where sdt >='20240301'
	and sdt <= regexp_replace(add_months(date_sub(current_date,1),0),'-','') 
	and business_type_code in ('1') 
	and shop_low_profit_flag!=1
    and 	order_channel_detail_code ='26'         -- 价格补救 order_channel_code =5
 )a 
 left join 
 (	select * ,
		substr(sdt,1,6) smonth,
		weekofyear(date_sub(to_date(from_unixtime(unix_timestamp(sdt,'yyyyMMdd'))),-2)) week,
		if((order_channel_code in ('4','5','6') or refund_order_flag=1),original_order_code,order_code) as order_code_new
	from   csx_analyse.csx_analyse_bi_sale_detail_di
	where sdt >='20230101'
	and sdt <= regexp_replace(add_months(date_sub(current_date,1),0),'-','') 
	and business_type_code in ('1') 
    and 	order_channel_detail_code !='26'         -- 价格补救 order_channel_code =5
    and shop_low_profit_flag!=1
   )b 
-- 	and original_order_code='OM24011100004429'
-- 	and goods_code='1115753'
on a.original_order_code=b.order_code and a.goods_code=b.goods_code and a.wms_order_code=b.wms_order_code