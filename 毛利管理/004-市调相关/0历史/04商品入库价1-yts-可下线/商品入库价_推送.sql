
insert overwrite table csx_analyse.csx_analyse_fr_ts_dc_product_price_in	
select 
	t3.performance_province_name,
	t2.business_division_name,
	t1.dc_code,
	t2.classify_middle_name,
	t1.goods_code,
	t2.goods_name, 
	t1.all_received_amount,
	t1.all_received_qty,
	t1.avg_price,
	t1.accounting_stock_price,
	t1.fin_price, 
	-- t1.all_not_t_received_amount,
	-- t1.all_gys_shipped_amount,
	-- t1.all_not_t_received_qty,
	-- t1.all_gys_shipped_qty
from_utc_timestamp(current_timestamp(),'GMT') update_time	
from 
	(
	select 
		(case when a.dc_code is not null then a.dc_code else b.target_location_code end) as dc_code,
		(case when a.goods_code is not null then a.goods_code else b.goods_code end) as goods_code,
		a.accounting_stock_price,
		b.avg_price,
		b.all_received_amount,
		b.all_received_qty,
		nvl(a.accounting_stock_price,b.avg_price) as fin_price,
		b.all_not_t_received_amount,
		b.all_gys_shipped_amount,
		b.all_not_t_received_qty,
		b.all_gys_shipped_qty   
	from 
		-- 昨日库存平均价
		(
		select 
			location_code as dc_code,
		    product_code as goods_code,
		    cast(coalesce(sum(amt)/sum(qty),0) as decimal(30,6)) accounting_stock_price -- 库存平均价
		from 
			csx_ods.csx_ods_csx_b2b_accounting_accounting_stock_df 
		where 
			sdt='${sdt_yes}' 
			AND substr(reservoir_area_code, 1, 2) <> 'PD'
			AND substr(reservoir_area_code, 1, 2) <> 'TS' 
			and abs(amt)>0 -- 一定要加这个条件，要不会出现好多无效数据
			and abs(qty)>0 
			and location_code in ('W0R9','W0A5','W0N0','W0W7','W0X6','W0T1','W0N1','W0AS','W0A8','W0F4','W0L3','WB56','W0AH','W0G9','WA96','WB67','W0K6','W0BK','W0A7','W0X2','W0Z9','W0A6','W0Q2','W0A3','W0Q9','W0P8','W0A2','W0L4','W0BR','W0BH','WB95') 
		group by 
			location_code,
		    product_code
		) a 
		full join 
			(
			select 
				b1.target_location_code,
				b1.goods_code,
				sum((case when b1.received_amount<0 then 0 else b1.received_amount end)) as all_not_t_received_amount,
				sum(nvl((case when b3.shipped_amount<0 then 0 else b3.shipped_amount end),0)) as all_gys_shipped_amount,
				sum((case when b1.received_qty<0 then 0 else b1.received_qty end)) as all_not_t_received_qty,
				sum(nvl((case when b3.shipped_qty<0 then 0 else b3.shipped_qty end),0)) as all_gys_shipped_qty,
				sum((case when b1.received_amount<0 then 0 else b1.received_amount end)-nvl((case when b3.shipped_amount<0 then 0 else b3.shipped_amount end),0)) as all_received_amount,
				sum((case when b1.received_qty<0 then 0 else b1.received_qty end)-nvl((case when b3.shipped_qty<0 then 0 else b3.shipped_qty end),0)) as all_received_qty,
				sum((case when b1.received_amount<0 then 0 else b1.received_amount end)-nvl((case when b3.shipped_amount<0 then 0 else b3.shipped_amount end),0))/
				sum((case when b1.received_qty<0 then 0 else b1.received_qty end)-nvl((case when b3.shipped_qty<0 then 0 else b3.shipped_qty end),0)) as avg_price 
			from 
				-- 入库数据
				(
				select 
					* 
				from 
					csx_dws.csx_dws_scm_order_received_di 
				where 
					sdt>=regexp_replace(date_add(from_unixtime(unix_timestamp('${sdt_yes}','yyyyMMdd'),'yyyy-MM-dd'),-90),'-','') 
					and sdt<='${sdt_yes}' 
					and super_class in (1,3) -- 加上调拨入库的数据
					and header_status=4 
					and source_type in (1,10,22,23) -- 订单类型
					and target_location_code in ('W0R9','W0A5','W0N0','W0W7','W0X6','W0T1','W0N1','W0AS','W0A8','W0F4','W0L3','WB56','W0AH','W0G9','WA96','WB67','W0K6','W0BK','W0A7','W0X2','W0Z9','W0A6','W0Q2','W0A3','W0Q9','W0P8','W0A2','W0L4','W0BR','W0BH','WB95')
				) b1 
				-- 关联价格补救订单数据
				left join 
					(
					select 
						* 
					from 
						csx_dws.csx_dws_scm_order_received_di 
					where 
						sdt>=regexp_replace(date_add(from_unixtime(unix_timestamp('${sdt_yes}','yyyyMMdd'),'yyyy-MM-dd'),-90),'-','') 
						and sdt<='${sdt_yes}' 
						and target_location_code in ('W0R9','W0A5','W0N0','W0W7','W0X6','W0T1','W0N1','W0AS','W0A8','W0F4','W0L3','WB56','W0AH','W0G9','WA96','WB67','W0K6','W0BK','W0A7','W0X2','W0Z9','W0A6','W0Q2','W0A3','W0Q9','W0P8','W0A2','W0L4','W0BR','W0BH','WB95')
						and price_remedy_flag=1 
					) b2 on b1.order_code=b2.original_order_code and b1.goods_code=b2.goods_code 
				-- 关联供应商退货订单
				left join 
					(
					select 
						* 
					from 
						csx_dws.csx_dws_scm_order_shipped_di   
					where 
						sdt>=regexp_replace(date_add(from_unixtime(unix_timestamp('${sdt_yes}','yyyyMMdd'),'yyyy-MM-dd'),-90),'-','') 
						and super_class in (2) 
						and target_location_code in ('W0R9','W0A5','W0N0','W0W7','W0X6','W0T1','W0N1','W0AS','W0A8','W0F4','W0L3','WB56','W0AH','W0G9','WA96','WB67','W0K6','W0BK','W0A7','W0X2','W0Z9','W0A6','W0Q2','W0A3','W0Q9','W0P8','W0A2','W0L4','W0BR','W0BH','WB95')
					) b3 on b1.order_code=b3.original_order_code and b1.goods_code=b3.goods_code 
				left join 
					(
					select 
						* 
					from 
						csx_dim.csx_dim_basic_goods 
					where 
						sdt='current' 
					) b4 on b1.goods_code=b4.goods_code 
			where 
				b2.original_order_code is null 
				and (
				(((b4.business_division_name like '%生鲜%' and b4.classify_middle_code='B0101') or  b4.business_division_name like '%食百%') and b1.sdt>=regexp_replace(date_add(from_unixtime(unix_timestamp('${sdt_yes}','yyyyMMdd'),'yyyy-MM-dd'),-90),'-','') and b1.sdt<='${sdt_yes}' )
				or 
				(b4.business_division_name like '%生鲜%' and (b4.classify_middle_code<>'B0101' or b4.classify_middle_code is null) and b1.sdt>=regexp_replace(date_add(from_unixtime(unix_timestamp('${sdt_yes}','yyyyMMdd'),'yyyy-MM-dd'),-6),'-','') and b1.sdt<='${sdt_yes}')
				)
			group by 
				b1.target_location_code,
				b1.goods_code 
			) b on a.dc_code=b.target_location_code and a.goods_code=b.goods_code 
	) t1 
	left join 
		(
		select 
			* 
		from 
			csx_dim.csx_dim_basic_goods 
		where 
			sdt='current' 
		) t2 on t1.goods_code=t2.goods_code 
	left join 
		(
		select 
			* 
		from 
			csx_dim.csx_dim_shop 
		where 
			sdt='current' 
		) t3 on t1.dc_code=t3.shop_code 
order by 
    t2.business_division_name,
	t1.dc_code,
	t2.classify_middle_name,
	t1.all_received_amount desc 
;


--hive 市调-商品入库价
drop table if exists csx_analyse.csx_analyse_fr_ts_dc_product_price_in;
create table csx_analyse.csx_analyse_fr_ts_dc_product_price_in(
`performance_province_name`	string	COMMENT	'省份',
`business_division_name`	string	COMMENT	'生鲜or食百',
`dc_code`	string	COMMENT	'仓',
`classify_middle_name`	string	COMMENT	'管理中类',
`goods_code`	string	COMMENT	'商品编码',
`goods_name`	string	COMMENT	'商品名称',
`all_received_amount`	decimal(20,6)	COMMENT	'入库额',
`all_received_qty`	decimal(20,6)	COMMENT	'入库量',
`avg_price`	decimal(20,6)	COMMENT	'入库平均价',
`accounting_stock_price`	decimal(20,6)	COMMENT	'昨日库存平均价',
`fin_price`	decimal(20,6)	COMMENT	'最终价格',
`update_time`	string	COMMENT	'报表更新时间'
) COMMENT '市调-商品入库价'
;

