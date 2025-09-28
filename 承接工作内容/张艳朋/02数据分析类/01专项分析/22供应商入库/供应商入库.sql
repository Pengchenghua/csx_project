-- 各区域是否入库成本波动有没有异常或者统一加点
-- 重点关注供应商成本，然后管理者已知费用看是否有关联；还是我们要费用一起看，如果费用一起看我去问下这块哪里取数
-- 上海发现有异常

drop table if exists csx_analyse_tmp.csx_analyse_tmp_dc_product_price_bodong_00;
create table csx_analyse_tmp.csx_analyse_tmp_dc_product_price_bodong_00
as		
select 
	t3.performance_province_name,
	t1.supplier_code,
	t1.supplier_name,
	t1.csx_week,
	t1.target_location_code,
	t1.classify_middle_name,
	t1.business_division_name,
	t1.all_received_amount,
	t1.all_received_qty,
	t1.avg_price
from 
	(
	select
		b1.supplier_code,
		b1.supplier_name,
		b5.csx_week,
		b1.target_location_code,
		-- b1.goods_code,
		b4.classify_middle_name,
		b4.business_division_name,
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
			sdt>=regexp_replace(date_add(from_unixtime(unix_timestamp('${yes_date}','yyyyMMdd'),'yyyy-MM-dd'),-90),'-','') 
			and sdt<='${yes_date}' 
			and super_class in (1,3) -- 加上调拨入库的数据
			and header_status=4 
			and source_type in (1,10,22,23) -- 订单类型
		) b1 
		-- 关联价格补救订单数据
		left join 
			(
			select 
				* 
			from 
				csx_dws.csx_dws_scm_order_received_di 
			where 
				sdt>=regexp_replace(date_add(from_unixtime(unix_timestamp('${yes_date}','yyyyMMdd'),'yyyy-MM-dd'),-90),'-','') 
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
				sdt>=regexp_replace(date_add(from_unixtime(unix_timestamp('${yes_date}','yyyyMMdd'),'yyyy-MM-dd'),-90),'-','') 
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
				calday,quarter_of_year,csx_week,csx_week_begin,csx_week_end,month_of_year
			from
				csx_dim.csx_dim_basic_date
			) b5 on b5.calday=b1.sdt
	where 
		b2.original_order_code is null 
	group by 
		b1.supplier_code,
		b1.supplier_name,
		b5.csx_week,
		b1.target_location_code,
		-- b1.goods_code,
		b4.classify_middle_name,
		b4.business_division_name
	) t1 
	left join 
		(
		select 
			* 
		from 
			csx_dim.csx_dim_shop 
		where 
			sdt='current' 
		) t3 on t1.target_location_code=t3.shop_code 
;

select * from csx_analyse_tmp.csx_analyse_tmp_dc_product_price_bodong_00;

-- =========================================================================================================================================================================
drop table if exists csx_analyse_tmp.csx_analyse_tmp_dc_product_price_bodong_01;
create table csx_analyse_tmp.csx_analyse_tmp_dc_product_price_bodong_01
as
select
	
	select 
		t3.performance_province_name,
		t1.supplier_code,
		t1.supplier_name,
		t1.csx_week,
		t1.target_location_code,
		t1.classify_middle_name,
		t1.business_division_name,
		t1.all_received_amount,
		t1.all_received_qty,
		t1.avg_price
	from 
		(
		select
			b1.supplier_code,
			b1.supplier_name,
			b5.csx_week,
			b1.target_location_code,
			-- b1.goods_code,
			b4.classify_middle_name,
			b4.business_division_name,
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
				sdt>=regexp_replace(date_add(from_unixtime(unix_timestamp('${yes_date}','yyyyMMdd'),'yyyy-MM-dd'),-90),'-','') 
				and sdt<='${yes_date}' 
				and super_class in (1,3) -- 加上调拨入库的数据
				and header_status=4 
				and source_type in (1,10,22,23) -- 订单类型
			) b1 
			-- 关联价格补救订单数据
			left join 
				(
				select 
					* 
				from 
					csx_dws.csx_dws_scm_order_received_di 
				where 
					sdt>=regexp_replace(date_add(from_unixtime(unix_timestamp('${yes_date}','yyyyMMdd'),'yyyy-MM-dd'),-90),'-','') 
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
					sdt>=regexp_replace(date_add(from_unixtime(unix_timestamp('${yes_date}','yyyyMMdd'),'yyyy-MM-dd'),-90),'-','') 
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
					calday,quarter_of_year,csx_week,csx_week_begin,csx_week_end,month_of_year
				from
					csx_dim.csx_dim_basic_date
				) b5 on b5.calday=b1.sdt
		where 
			b2.original_order_code is null 
		group by 
			b1.supplier_code,
			b1.supplier_name,
			b5.csx_week,
			b1.target_location_code,
			-- b1.goods_code,
			b4.classify_middle_name,
			b4.business_division_name
		) t1 
		left join 
			(
			select 
				* 
			from 
				csx_dim.csx_dim_shop 
			where 
				sdt='current' 
			) t3 on t1.target_location_code=t3.shop_code 
;

select * from csx_analyse_tmp.csx_analyse_tmp_dc_product_price_bodong_01;