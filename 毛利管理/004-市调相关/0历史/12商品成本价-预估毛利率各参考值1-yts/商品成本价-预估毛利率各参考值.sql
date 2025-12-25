
-- 商品成本价-预估毛利率各参考值
insert overwrite table csx_analyse.csx_analyse_fr_ts_profit_yg_goods_cost_price
select 
	c.performance_region_name,
	c.performance_province_name,
	c.performance_city_name,
	d.business_division_name,
	a.location_code,
	d.classify_middle_name,
	a.product_code as goods_code,	
	d.goods_name,
	-- 供应链周期进价生效区平均值 
	a.purchase_price_avg,
	-- 取库存平均价
	a.stock_price_avg,
	-- 生鲜近7天食百近1个月最近一次入库价
	a.price_last_stock,
	-- 生效采购报价
	a.purchase_price,
	coalesce(a.purchase_price_avg,a.stock_price_avg,a.price_last_stock,a.purchase_price) as fin_price,
from_utc_timestamp(current_timestamp(),'GMT') update_time	
from
(
	select 
		location_code,
		product_code,		
		avg(purchase_price_avg) as purchase_price_avg,
		avg(stock_price_avg) as stock_price_avg,
		avg(price_last_stock) as price_last_stock,
		avg(purchase_price) as purchase_price		
	from
	(
		-- 供应链周期进价生效区平均值 
		select 
			location_code,
			product_code,
			avg(purchase_price) as purchase_price_avg,
			null as stock_price_avg,
			null as price_last_stock,
			null as purchase_price
		from csx_ods.csx_ods_csx_b2b_scm_scm_product_purchase_cycle_price_df  
		where sdt='${sdt_yes}' 
		and cycle_price_status=0 
		group by location_code,product_code
		
		union all
		-- 取库存平均价
		select
			-- shipper_code,
			dc_code as location_code,
			goods_code as product_code,
			-- sum(amt) as amt,
			-- sum(qty) as qty,
			-- sum(amt_no_tax) as amt_no_tax,
			null as purchase_price_avg,
			sum(amt)/sum(qty) as stock_price_avg,
			null as price_last_stock,
			null as purchase_price			
		from csx_dws.csx_dws_cas_accounting_stock_m_df
		where sdt='${sdt_yes}'
		and is_bz_reservoir = 1 
		and qty > 0 
		group by dc_code,goods_code
	
		union all
		-- 生鲜近7天食百近1一个月最近一次入库价
		select 
			location_code,
			product_code,
			null as purchase_price_avg,
			null as stock_price_avg,
			price as price_last_stock,
			null as purchase_price
		from 
		(
			select a.*,
				row_number()over(partition by a.location_code,a.product_code order by a.update_time desc) as rn
			from csx_ods.csx_ods_csx_b2b_accounting_accounting_last_in_stock_df a
			left join 
			(
				select * 
				from csx_dim.csx_dim_basic_goods 
				where sdt='current' 
			) b on a.product_code=b.goods_code  
			where (
			(((b.business_division_name like '%生鲜%' and b.classify_middle_code='B0101') or  b.business_division_name like '%食百%') 
				and regexp_replace(substr(a.update_time,1,10),'-','')>=regexp_replace(date_add(from_unixtime(unix_timestamp('${sdt_yes}','yyyyMMdd'),'yyyy-MM-dd'),-30),'-',''))
			or 
			(b.business_division_name like '%生鲜%' and (b.classify_middle_code<>'B0101' or b.classify_middle_code is null) 
				and regexp_replace(substr(a.update_time,1,10),'-','')>=regexp_replace(date_add(from_unixtime(unix_timestamp('${sdt_yes}','yyyyMMdd'),'yyyy-MM-dd'),-7),'-',''))
			)
		)a 
		where rn=1	
	
		union all
		-- 生效采购报价表 一天中间可能有新的采购报价生效致1天多条
		select 
			warehouse_code as location_code,
			product_code,
			null as purchase_price_avg,
			null as stock_price_avg,
			null as price_last_stock,
			purchase_price
		from 
		(
			select 
				warehouse_code,
				product_code,
				purchase_price,
				row_number()over(partition by warehouse_code,product_code order by price_begin_time desc) as rn
			from csx_dwd.csx_dwd_price_effective_purchase_prices_di 
			where sdt>=regexp_replace(date_sub(current_date,60),'-','')  -- 近60天末次采购报价
			and normal_status=0  -- 正常 = 0 异常 = 1
			and base_product_status=0  -- 0正常 3停售 6退场 7停购
			and regexp_replace(substr(price_end_time,1,10),'-','')>='${sdt_yes}'
			and regexp_replace(substr(price_begin_time,1,10),'-','')<='${sdt_yes}'
		)a 
		where rn=1
	)a
	group by 
		location_code,
		product_code
)a 
left join (select * from csx_dim.csx_dim_shop where sdt='current') c on c.shop_code=a.location_code
left join (select * from csx_dim.csx_dim_basic_goods where sdt='current')d on d.goods_code=a.product_code
where a.location_code in('W0R9','W0A5','W0N0','W0W7','W0X6','W0T1','W0N1','W0AS','W0A8','W0F4','W0L3','WB56','W0AH','W0G9','WA96','WB67','W0K6','W0BK','W0A7','W0X2','W0Z9','W0A6','W0Q2','W0A3','W0Q9','W0P8','W0A2','W0L4','W0BR','W0BH','WB95','WC53')
;






--hive 市调-商品成本价（预估毛利率各参考值）
drop table if exists csx_analyse.csx_analyse_fr_ts_profit_yg_goods_cost_price;
create table csx_analyse.csx_analyse_fr_ts_profit_yg_goods_cost_price(
`performance_region_name`	string	COMMENT	'大区',
`performance_province_name`	string	COMMENT	'省区',
`performance_city_name`	string	COMMENT	'城市',
`business_division_name`	string	COMMENT	'生鲜or食百',
`location_code`	string	COMMENT	'仓',
`classify_middle_name`	string	COMMENT	'管理中类',
`goods_code`	string	COMMENT	'商品编码',
`goods_name`	string	COMMENT	'商品名称',
`purchase_price_avg`	decimal(20,6)	COMMENT	'供应链周期进价生效区平均值',
`stock_price_avg`	decimal(20,6)	COMMENT	'库存平均价',
`price_last_stock`	decimal(20,6)	COMMENT	'生鲜近7天食百近1个月最近一次入库价',
`purchase_price`	decimal(20,6)	COMMENT	'生效采购报价',
`fin_price`	decimal(20,6)	COMMENT	'最终价格',
`update_time`	string	COMMENT	'报表更新时间'
) COMMENT '市调-商品成本价（预估毛利率各参考值）'
;




