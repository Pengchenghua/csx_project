--====================================================================================================================================================	
--商品简报 全国大客户日配商品模块业绩概览（MTD）	
select
	region_name,
	sales_city,
	--整体
	coalesce(sum(sales_value),0) as sales_value,
	coalesce(round(sum(profit)/sum(sales_value),8),0) as profit_rate,
	count(distinct customer_no) as customer_amount,
	coalesce(round(sum(sku_amount)/sum(sales_date_amount),1),0) as sku_date_customer_rate,
	--生鲜
	coalesce(sum(shengxian_sales_value),0) as shengxian_sales_value,
	coalesce(round(sum(shengxian_sales_value)/sum(sales_value),8),0) as shengxian_sales_value_rate,
	coalesce(round(sum(shengxian_profit)/sum(shengxian_sales_value),8),0) as shengxian_profit_rate,
	coalesce(round(count(distinct if(shengxian_customer_amount>0,customer_no,null))/count(distinct customer_no),8),0) as shengxian_customer_rate,
	coalesce(round(sum(shengxian_sku_amount)/sum(shengxian_sales_date_amount),1),0) as shengxian_sku_date_customer_rate,
	--食百
	coalesce(sum(shibai_sales_value),0) as shibai_sales_value,
	coalesce(round(sum(shibai_sales_value)/sum(sales_value),8),0) as shibai_sales_value_rate,
	coalesce(round(sum(shibai_profit)/sum(shibai_sales_value),8),0) as shibai_profit_rate,
	coalesce(round(count(distinct if(shibai_customer_amount>0,customer_no,null))/count(distinct customer_no),8),0) as shibai_customer_rate,
	--coalesce(round(sum(shibai_sku_amount)/sum(shibai_sales_date_amount),1),0) as shibai_sku_date_customer_rate,
	--非食品（用品类）
	coalesce(sum(n_food_sales_value),0) as n_food_sales_value,
	--coalesce(round(sum(n_food_sales_value)/sum(sales_value),8),0) as n_food_sales_value_rate,
	coalesce(round(sum(n_food_profit)/sum(n_food_sales_value),8),0) as n_food_profit_rate,
	coalesce(round(count(distinct if(n_food_customer_amount>0,customer_no,null))/count(distinct customer_no),8),0) as n_food_customer_rate,
	coalesce(round(sum(n_food_sku_amount)/sum(n_food_sales_date_amount),1),0) as n_food_sku_date_customer_rate,
	--食品（食品类）
	coalesce(sum(food_sales_value),0) as food_sales_value,
	--coalesce(round(sum(food_sales_value)/sum(sales_value),8),0) as food_sales_value_rate,
	coalesce(round(sum(food_profit)/sum(food_sales_value),8),0) as food_profit_rate,
	coalesce(round(count(distinct if(food_customer_amount>0,customer_no,null))/count(distinct customer_no),8),0) as food_customer_rate,
	coalesce(round(sum(food_sku_amount)/sum(food_sales_date_amount),1),0) as food_sku_date_customer_rate
from
	(
	select
		region_name,
		sales_city,
		customer_no,
		--整体
		sum(sales_value) as sales_value,
		sum(profit) as profit,
		sum(sales_date_amount) as sales_date_amount,
		sum(sku_amount) as sku_amount,
		--生鲜
		sum(shengxian_sales_value) as shengxian_sales_value,
		sum(shengxian_profit) as shengxian_profit,
		sum(shengxian_customer_amount) as shengxian_customer_amount,
		sum(shengxian_sales_date_amount) as shengxian_sales_date_amount,
		sum(shengxian_sku_amount) as shengxian_sku_amount,
		--食百
		sum(shibai_sales_value) as shibai_sales_value,
		sum(shibai_profit) as shibai_profit,
		sum(shibai_customer_amount) as shibai_customer_amount,
		sum(shibai_sales_date_amount) as shibai_sales_date_amount,
		sum(shibai_sku_amount) as shibai_sku_amount,
		--非食品（用品类）
		sum(n_food_sales_value) as n_food_sales_value,
		sum(n_food_profit) as n_food_profit,
		sum(n_food_customer_amount) as n_food_customer_amount,
		sum(n_food_sales_date_amount) as n_food_sales_date_amount,
		sum(n_food_sku_amount) as n_food_sku_amount,
		--食品（食品类）
		sum(food_sales_value) as food_sales_value,
		sum(food_profit) as food_profit,
		sum(food_customer_amount) as food_customer_amount,
		sum(food_sales_date_amount) as food_sales_date_amount,
		sum(food_sku_amount) as food_sku_amount
	from
		(
		select
			region_name,
			sales_city,
			customer_no,
			sales_date,
			--整体
			sum(sales_value) as sales_value,
			sum(profit) as profit,
			count(distinct sales_date) as sales_date_amount,
			count(distinct goods_code) as sku_amount,
			--生鲜
			sum(if(division_name in ('生鲜部','加工部'),sales_value,null)) as shengxian_sales_value,
			sum(if(division_name in ('生鲜部','加工部'),profit,null)) as shengxian_profit,
			count(distinct if(division_name in ('生鲜部','加工部'),customer_no,null)) as shengxian_customer_amount,
			count(distinct if(division_name in ('生鲜部','加工部'),sales_date,null)) as shengxian_sales_date_amount,
			count(distinct if(division_name in ('生鲜部','加工部'),goods_code,null)) as shengxian_sku_amount,
			--食百
			sum(if(division_name in ('食品类','用品类','易耗品','服装'),sales_value,null)) as shibai_sales_value,
			sum(if(division_name in ('食品类','用品类','易耗品','服装'),profit,null)) as shibai_profit,
			count(distinct if(division_name in ('食品类','用品类','易耗品','服装'),customer_no,null)) as shibai_customer_amount,
			count(distinct if(division_name in ('食品类','用品类','易耗品','服装'),sales_date,null)) as shibai_sales_date_amount,
			count(distinct if(division_name in ('食品类','用品类','易耗品','服装'),goods_code,null)) as shibai_sku_amount,
			--非食品（用品类）
			sum(if(division_name in ('用品类','易耗品','服装'),sales_value,null)) as n_food_sales_value,
			sum(if(division_name in ('用品类','易耗品','服装'),profit,null)) as n_food_profit,
			count(distinct if(division_name in ('用品类','易耗品','服装'),customer_no,null)) as n_food_customer_amount,
			count(distinct if(division_name in ('用品类','易耗品','服装'),sales_date,null)) as n_food_sales_date_amount,
			count(distinct if(division_name in ('用品类','易耗品','服装'),goods_code,null)) as n_food_sku_amount,
			--食品（食品类）
			sum(if(division_name in ('食品类'),sales_value,null)) as food_sales_value,
			sum(if(division_name in ('食品类'),profit,null)) as food_profit,
			count(distinct if(division_name in ('食品类'),customer_no,null)) as food_customer_amount,
			count(distinct if(division_name in ('食品类'),sales_date,null)) as food_sales_date_amount,
			count(distinct if(division_name in ('食品类'),goods_code,null)) as food_sku_amount				
		from
			(
			select
				region.region_name,base.smonth,base.customer_no,base.province_code,base.province_name,base.sales_city,base.sales_date,base.goods_code,base.division_name,base.sales_value,base.profit
			from
				(
				select
					substr(sdt,1,6)smonth,customer_no,province_code,province_name,sales_city,sales_date,goods_code,division_name,sales_value,profit
				from
					csx_dw.dws_sale_r_d_customer_sale
				where 
					sdt >= regexp_replace(to_date(trunc(date_sub(current_date,1),'MM')),'-','')
					and sdt <= regexp_replace(to_date(date_sub(current_date,1)),'-','')
					and attribute_code != 5 --5为合伙人
					and channel_name like '大客户%' --分销渠道名称
					and sales_type in ('qyg','sapqyg','sapgc','sc','bbc')
				) as base
				-- 合伙人列表
				left join 
					(
					select 
						customer_no,substr(sdt,1,6)smonth 
					from 
						csx_dw.csx_partner_list
					group by
						customer_no,substr(sdt,1,6)
					) partner on partner.customer_no= base.customer_no and partner.smonth=base.smonth
				--大区
				left join 
					(
					select 
						province_code,province_name,region_code,region_name
					from 
						csx_dw.dim_area
					where
						area_rank='13'
					group by
						province_code,province_name,region_code,region_name
					) region on region.province_code= base.province_code		
			where 
				partner.customer_no is null	
			) as base
		group by
			region_name,
			sales_city,
			customer_no,
			sales_date
		) as base
	group by
		region_name,
		sales_city,
		customer_no
	) as base
group by
	region_name,
	sales_city
	
union all

--商品简报 全国大客户日配商品模块业绩概览（MTD）	
select
	region_name,
	sales_city,
	--整体
	coalesce(sum(sales_value),0) as sales_value,
	coalesce(round(sum(profit)/sum(sales_value),8),0) as profit_rate,
	count(distinct customer_no) as customer_amount,
	coalesce(round(sum(sku_amount)/sum(sales_date_amount),1),0) as sku_date_customer_rate,
	--生鲜
	coalesce(sum(shengxian_sales_value),0) as shengxian_sales_value,
	coalesce(round(sum(shengxian_sales_value)/sum(sales_value),8),0) as shengxian_sales_value_rate,
	coalesce(round(sum(shengxian_profit)/sum(shengxian_sales_value),8),0) as shengxian_profit_rate,
	coalesce(round(count(distinct if(shengxian_customer_amount>0,customer_no,null))/count(distinct customer_no),8),0) as shengxian_customer_rate,
	coalesce(round(sum(shengxian_sku_amount)/sum(shengxian_sales_date_amount),1),0) as shengxian_sku_date_customer_rate,
	--食百
	coalesce(sum(shibai_sales_value),0) as shibai_sales_value,
	coalesce(round(sum(shibai_sales_value)/sum(sales_value),8),0) as shibai_sales_value_rate,
	coalesce(round(sum(shibai_profit)/sum(shibai_sales_value),8),0) as shibai_profit_rate,
	coalesce(round(count(distinct if(shibai_customer_amount>0,customer_no,null))/count(distinct customer_no),8),0) as shibai_customer_rate,
	--coalesce(round(sum(shibai_sku_amount)/sum(shibai_sales_date_amount),1),0) as shibai_sku_date_customer_rate,
	--非食品（用品类）
	coalesce(sum(n_food_sales_value),0) as n_food_sales_value,
	--coalesce(round(sum(n_food_sales_value)/sum(sales_value),8),0) as n_food_sales_value_rate,
	coalesce(round(sum(n_food_profit)/sum(n_food_sales_value),8),0) as n_food_profit_rate,
	coalesce(round(count(distinct if(n_food_customer_amount>0,customer_no,null))/count(distinct customer_no),8),0) as n_food_customer_rate,
	coalesce(round(sum(n_food_sku_amount)/sum(n_food_sales_date_amount),1),0) as n_food_sku_date_customer_rate,
	--食品（食品类）
	coalesce(sum(food_sales_value),0) as food_sales_value,
	--coalesce(round(sum(food_sales_value)/sum(sales_value),8),0) as food_sales_value_rate,
	coalesce(round(sum(food_profit)/sum(food_sales_value),8),0) as food_profit_rate,
	coalesce(round(count(distinct if(food_customer_amount>0,customer_no,null))/count(distinct customer_no),8),0) as food_customer_rate,
	coalesce(round(sum(food_sku_amount)/sum(food_sales_date_amount),1),0) as food_sku_date_customer_rate
from
	(
	select
		region_name,
		sales_city,
		customer_no,
		--整体
		sum(sales_value) as sales_value,
		sum(profit) as profit,
		sum(sales_date_amount) as sales_date_amount,
		sum(sku_amount) as sku_amount,
		--生鲜
		sum(shengxian_sales_value) as shengxian_sales_value,
		sum(shengxian_profit) as shengxian_profit,
		sum(shengxian_customer_amount) as shengxian_customer_amount,
		sum(shengxian_sales_date_amount) as shengxian_sales_date_amount,
		sum(shengxian_sku_amount) as shengxian_sku_amount,
		--食百
		sum(shibai_sales_value) as shibai_sales_value,
		sum(shibai_profit) as shibai_profit,
		sum(shibai_customer_amount) as shibai_customer_amount,
		sum(shibai_sales_date_amount) as shibai_sales_date_amount,
		sum(shibai_sku_amount) as shibai_sku_amount,
		--非食品（用品类）
		sum(n_food_sales_value) as n_food_sales_value,
		sum(n_food_profit) as n_food_profit,
		sum(n_food_customer_amount) as n_food_customer_amount,
		sum(n_food_sales_date_amount) as n_food_sales_date_amount,
		sum(n_food_sku_amount) as n_food_sku_amount,
		--食品（食品类）
		sum(food_sales_value) as food_sales_value,
		sum(food_profit) as food_profit,
		sum(food_customer_amount) as food_customer_amount,
		sum(food_sales_date_amount) as food_sales_date_amount,
		sum(food_sku_amount) as food_sku_amount
	from
		(
		select
			'全国' as region_name,
			'全国' as sales_city,
			customer_no,
			sales_date,
			--整体
			sum(sales_value) as sales_value,
			sum(profit) as profit,
			count(distinct sales_date) as sales_date_amount,
			count(distinct goods_code) as sku_amount,
			--生鲜
			sum(if(division_name in ('生鲜部','加工部'),sales_value,null)) as shengxian_sales_value,
			sum(if(division_name in ('生鲜部','加工部'),profit,null)) as shengxian_profit,
			count(distinct if(division_name in ('生鲜部','加工部'),customer_no,null)) as shengxian_customer_amount,
			count(distinct if(division_name in ('生鲜部','加工部'),sales_date,null)) as shengxian_sales_date_amount,
			count(distinct if(division_name in ('生鲜部','加工部'),goods_code,null)) as shengxian_sku_amount,
			--食百
			sum(if(division_name in ('食品类','用品类','易耗品','服装'),sales_value,null)) as shibai_sales_value,
			sum(if(division_name in ('食品类','用品类','易耗品','服装'),profit,null)) as shibai_profit,
			count(distinct if(division_name in ('食品类','用品类','易耗品','服装'),customer_no,null)) as shibai_customer_amount,
			count(distinct if(division_name in ('食品类','用品类','易耗品','服装'),sales_date,null)) as shibai_sales_date_amount,
			count(distinct if(division_name in ('食品类','用品类','易耗品','服装'),goods_code,null)) as shibai_sku_amount,
			--非食品（用品类）
			sum(if(division_name in ('用品类','易耗品','服装'),sales_value,null)) as n_food_sales_value,
			sum(if(division_name in ('用品类','易耗品','服装'),profit,null)) as n_food_profit,
			count(distinct if(division_name in ('用品类','易耗品','服装'),customer_no,null)) as n_food_customer_amount,
			count(distinct if(division_name in ('用品类','易耗品','服装'),sales_date,null)) as n_food_sales_date_amount,
			count(distinct if(division_name in ('用品类','易耗品','服装'),goods_code,null)) as n_food_sku_amount,
			--食品（食品类）
			sum(if(division_name in ('食品类'),sales_value,null)) as food_sales_value,
			sum(if(division_name in ('食品类'),profit,null)) as food_profit,
			count(distinct if(division_name in ('食品类'),customer_no,null)) as food_customer_amount,
			count(distinct if(division_name in ('食品类'),sales_date,null)) as food_sales_date_amount,
			count(distinct if(division_name in ('食品类'),goods_code,null)) as food_sku_amount				
		from
			(
			select
				region.region_name,base.smonth,base.customer_no,base.province_code,base.province_name,base.sales_city,base.sales_date,base.goods_code,base.division_name,base.sales_value,base.profit
			from
				(
				select
					substr(sdt,1,6)smonth,customer_no,province_code,province_name,sales_city,sales_date,goods_code,division_name,sales_value,profit
				from
					csx_dw.dws_sale_r_d_customer_sale
				where 
					sdt >= regexp_replace(to_date(trunc(date_sub(current_date,1),'MM')),'-','')
					and sdt <= regexp_replace(to_date(date_sub(current_date,1)),'-','')
					and attribute_code != 5 --5为合伙人
					and channel_name like '大客户%' --分销渠道名称
					and sales_type in ('qyg','sapqyg','sapgc','sc','bbc')
				) as base
				-- 合伙人列表
				left join 
					(
					select 
						customer_no,substr(sdt,1,6)smonth 
					from 
						csx_dw.csx_partner_list
					group by
						customer_no,substr(sdt,1,6)
					) partner on partner.customer_no= base.customer_no and partner.smonth=base.smonth
				--大区
				left join 
					(
					select 
						province_code,province_name,region_code,region_name
					from 
						csx_dw.dim_area
					where
						area_rank='13'
					group by
						province_code,province_name,region_code,region_name
					) region on region.province_code= base.province_code		
			where 
				partner.customer_no is null	
			) as base
		group by
			region_name,
			sales_city,
			customer_no,
			sales_date
		) as base
	group by
		region_name,
		sales_city,
		customer_no
	) as base
	group by
		region_name,
		sales_city
		
		