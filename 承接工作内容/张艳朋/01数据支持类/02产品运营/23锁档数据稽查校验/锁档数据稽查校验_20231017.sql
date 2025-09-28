

-- 商品池明细查询
select
	customer_code,
	-- customer_name,
	coalesce(regexp_replace(customer_name,'\n|\t|\r|\,|\"|\\\\n',''),'') as customer_name,
	inventory_dc_code,
	product_code,
	-- product_name,
	coalesce(regexp_replace(product_name,'\n|\t|\r|\,|\"|\\\\n',''),'') as product_name,
	updated_by,
	base_product_status,
	updated_time,
	data_source,
	case base_product_status
		when 0 then '正常'
		when 3 then '停售'
		when 6 then '退场'
		when 7 then '停购'
	end as base_product_status_name
from
	csx_ods.csx_ods_b2b_mall_prod_yszx_customer_product_df 
where
	base_product_status in (0,7)
	-- data_source=1 -- 数据来源：0-手动添加 1-客户订单 2-报价 3-商品池模板 4-必售商品 5-商品池模板替换 6-新品 7-基础商品池 8-CRM换品 9-销售添加
	-- and regexp_replace(to_date(updated_time),'-','') between concat(substr('20221109',1,6),'01') and '20221116'
;
select count(*) from (
	select
		a.performance_region_name,a.performance_province_name,a.inventory_dc_code,a.customer_code,e.customer_name,
		a.goods_code,c.goods_name,a.sale_amt,a.profit,a.sale_qty,
		if(b.customer_code is not null,'是','否') as customer_product_flag
	from
		(
		select
			performance_region_name,performance_province_name,inventory_dc_code,customer_code,goods_code,
			sum(sale_amt) as sale_amt,sum(profit) as profit,sum(sale_qty) as sale_qty
		from
			csx_dws.csx_dws_sale_detail_di
		where
			sdt>='${start_day}'
			and sdt<='${end_day}'
			and channel_code in ('1','7','9')
			and business_type_code in (1)
			and order_channel_detail_code in (11,12) -- 11系统手工单 12小程序大宗单 25客户返利 26价格补救 27客户调价
			and inventory_dc_code in ('W0A6','WB83','W0A7','W0X2','W0Z9','W0Q2','W0A2','W0BH','W0BR','W0A3','W0P8','W0Q9','W0A8','W0L3','WA96','W0K6','W0AH','WB56','WB67','W0F4','W0BK','WB61','W0N0','W0T1','W0W7','W0R9','W0A5','W0AS')
		group by 
			performance_region_name,performance_province_name,inventory_dc_code,customer_code,goods_code
		) a 
		left join
			(
			select
				distinct customer_code,inventory_dc_code,product_code
			from
				csx_ods.csx_ods_b2b_mall_prod_yszx_customer_product_df 
			where
				base_product_status in (0,7)
			) b on b.customer_code=a.customer_code and b.inventory_dc_code=a.inventory_dc_code and b.product_code=a.goods_code
		left join (select * from csx_dim.csx_dim_basic_goods where sdt='current') c on a.goods_code=c.goods_code 
		left join (select * from csx_dim.csx_dim_crm_customer_info where sdt='current') e on a.customer_code=e.customer_code 
) a 
				
				
				
-- 商品池明细查询
select
	count(*),count(distinct customer_code,inventory_dc_code,product_code)
from
	csx_ods.csx_ods_b2b_mall_prod_yszx_customer_product_df 
where
	base_product_status in (0,7)

-- 
drop table if exists csx_analyse_tmp.csx_analyse_tmp_customer_product_suodang_00;
create table csx_analyse_tmp.csx_analyse_tmp_customer_product_suodang_00
as
	select
		a.performance_region_name,a.performance_province_name,a.inventory_dc_code,a.customer_code,e.customer_name,
		a.goods_code,c.goods_name,a.sale_amt,a.profit,a.sale_qty,
		if(b.customer_code is not null,'是','否') as customer_product_flag,
		row_number()over() as rn
	from
		(
		select
			performance_region_name,performance_province_name,inventory_dc_code,customer_code,goods_code,
			sum(sale_amt) as sale_amt,sum(profit) as profit,sum(sale_qty) as sale_qty
		from
			csx_dws.csx_dws_sale_detail_di
		where
			sdt>='${start_day}'
			and sdt<='${end_day}'
			and channel_code in ('1','7','9')
			and business_type_code in (1)
			and order_channel_detail_code in (11,12) -- 11系统手工单 12小程序大宗单 25客户返利 26价格补救 27客户调价
			and inventory_dc_code in ('W0A6','WB83','W0A7','W0X2','W0Z9','W0Q2','W0A2','W0BH','W0BR','W0A3','W0P8','W0Q9','W0A8','W0L3','WA96','W0K6','W0AH','WB56','WB67','W0F4','W0BK','WB61','W0N0','W0T1','W0W7','W0R9','W0A5','W0AS')
		group by 
			performance_region_name,performance_province_name,inventory_dc_code,customer_code,goods_code
		) a 
		left join
			(
			select
				distinct customer_code,inventory_dc_code,product_code
			from
				csx_ods.csx_ods_b2b_mall_prod_yszx_customer_product_df 
			where
				base_product_status in (0,7)
			) b on b.customer_code=a.customer_code and b.inventory_dc_code=a.inventory_dc_code and b.product_code=a.goods_code
		left join (select * from csx_dim.csx_dim_basic_goods where sdt='current') c on a.goods_code=c.goods_code 
		left join (select * from csx_dim.csx_dim_crm_customer_info where sdt='current') e on a.customer_code=e.customer_code 
;
select * from csx_analyse_tmp.csx_analyse_tmp_customer_product_suodang_00 where rn<=490000;
select * from csx_analyse_tmp.csx_analyse_tmp_customer_product_suodang_00 where rn>490000;

