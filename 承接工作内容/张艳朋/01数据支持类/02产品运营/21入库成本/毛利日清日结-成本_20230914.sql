-- 昨日销售订单的成本售价业绩毛利率、商品对应生鲜近7日食百近30日的入库价、对省区毛利影响
-- 对省区影响排名影响大的，供应链会关注这些商品的入库

-- 商品入库价
drop table if exists csx_analyse_tmp.csx_analyse_tmp_dc_product_price_received;
create table csx_analyse_tmp.csx_analyse_tmp_dc_product_price_received
as		
select 
	t1.performance_province_name,
	t2.business_division_name,
	-- t1.target_location_code,
	t1.goods_code,
	t2.goods_name, 
	t1.all_received_amount,
	t1.all_received_qty,
	t1.avg_price
from 
	(
	select
		b5.performance_province_name,
		-- b1.target_location_code,
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
			sdt>=regexp_replace(date_add(from_unixtime(unix_timestamp('${yes_date}','yyyyMMdd'),'yyyy-MM-dd'),-30),'-','') 
			and sdt<='${yes_date}' 
			and super_class in (1,3) -- 加上调拨入库的数据 供应商订单
			and header_status=4 
			and source_type not in (2,3,4,11,15,16)  -- 剔除项目合伙人
			and local_purchase_flag=0 -- 剔除地采，是否地采(0-否、1-是)
			and direct_delivery_type=0 -- 直送类型 0-P(普通) 1-R(融单)、2-Z(过账)
			and received_amount>0
		) b1 
		-- 关联价格补救订单数据
		left join 
			(
			select 
				* 
			from 
				csx_dws.csx_dws_scm_order_received_di 
			where 
				sdt>=regexp_replace(date_add(from_unixtime(unix_timestamp('${yes_date}','yyyyMMdd'),'yyyy-MM-dd'),-30),'-','') 
				and sdt<='${yes_date}' 
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
				sdt>=regexp_replace(date_add(from_unixtime(unix_timestamp('${yes_date}','yyyyMMdd'),'yyyy-MM-dd'),-30),'-','') 
				and super_class in (2) 
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
		left join 
			(
			select 
				* 
			from 
				csx_dim.csx_dim_shop 
			where 
				sdt='current' 
			) b5 on b1.target_location_code=b5.shop_code 
			
	where 
		b2.original_order_code is null 
		and (
		(((b4.business_division_name like '%生鲜%' and b4.classify_middle_code='B0101') or  b4.business_division_name like '%食百%') and b1.sdt>=regexp_replace(date_add(from_unixtime(unix_timestamp('${yes_date}','yyyyMMdd'),'yyyy-MM-dd'),-30),'-','') and b1.sdt<='${yes_date}' )
		or 
		(b4.business_division_name like '%生鲜%' and (b4.classify_middle_code<>'B0101' or b4.classify_middle_code is null) and b1.sdt>=regexp_replace(date_add(from_unixtime(unix_timestamp('${yes_date}','yyyyMMdd'),'yyyy-MM-dd'),-6),'-','') and b1.sdt<='${yes_date}')
		)
	group by 
		b5.performance_province_name,
		-- b1.target_location_code,
		b1.goods_code 
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
;

-- 商品影响毛利清单
drop table if exists csx_analyse_tmp.csx_analyse_tmp_goods_effect_detail;
create table csx_analyse_tmp.csx_analyse_tmp_goods_effect_detail
as	
select
	a.performance_province_name,a.goods_code,c.goods_name,c.business_division_name,c.classify_middle_name,a.sale_amt,a.profit,a.profit_rate,a.avg_sale_amt,
	a.avg_sale_cost,b.avg_price,a.sale_amt_province,a.profit_province,a.profit_rate_province,a.profit_effect,
	row_number()over(partition by a.performance_province_name order by a.profit_effect asc) as rn
from
	(
	select
		performance_province_name,goods_code,sale_amt,profit,profit_rate,avg_sale_amt,avg_sale_cost,sale_amt_province,profit_province,profit_rate_province,
		profit_rate_province-(profit_province-profit)/(sale_amt_province-sale_amt) as profit_effect
	from
		(
		select
			performance_province_name,goods_code,sale_amt,profit,profit_rate,avg_sale_amt,avg_sale_cost,
			sum(sale_amt)over(partition by performance_province_name) as sale_amt_province,
			sum(profit)over(partition by performance_province_name) as profit_province,
			sum(profit)over(partition by performance_province_name)/abs(sum(sale_amt)over(partition by performance_province_name)) as profit_rate_province
		from
			(
			select 
				a.performance_province_name,
				a.goods_code,
				sum(sale_amt) as sale_amt,
				sum(profit) as profit,
				sum(profit)/abs(sum(sale_amt)) as profit_rate,
				sum(sale_amt)/sum(sale_qty) as avg_sale_amt,
				sum(sale_cost)/sum(sale_qty) as avg_sale_cost
			from
				(
				select
					*
				from
					csx_dws.csx_dws_sale_detail_di
				where 
					sdt = '${yes_date}' 
					and channel_code in ('1','7','9') 
					and business_type_code=1
				) a 
				join (select * from csx_dim.csx_dim_shop where sdt='current' and shop_low_profit_flag =0) b on a.inventory_dc_code = b.shop_code
			group by 
				a.performance_province_name,
				goods_code	
			) a 
		) a 
	) a 
	left join csx_analyse_tmp.csx_analyse_tmp_dc_product_price_received b on b.performance_province_name=a.performance_province_name and b.goods_code=a.goods_code
	left join (select * from csx_dim.csx_dim_basic_goods where sdt='current') c on c.goods_code=a.goods_code 
where
	a.profit_effect<0
;
select * from csx_analyse_tmp.csx_analyse_tmp_goods_effect_detail