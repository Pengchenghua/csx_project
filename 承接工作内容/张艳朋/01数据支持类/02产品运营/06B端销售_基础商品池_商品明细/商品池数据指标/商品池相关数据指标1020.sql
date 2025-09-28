-- 采购报价取数逻辑：
select 
	warehouse_code,
	product_code,
	price_begin_time,
	price_end_time,
	cast(purchase_price as decimal(16,2)) as purchase_price 
 from 
	(
	select 
		*,
		row_number()over(partition by warehouse_code,product_code order by last_put_time) as pm 
	from 
		csx_ods.csx_ods_csx_price_prod_effective_purchase_prices_df  
	where 
		sdt='${sdt_yes}' 
		and warehouse_code in ('W0A3')
		and regexp_replace(split(price_begin_time, ' ')[0], '-', '')<='${sdt_yes}' 
		and regexp_replace(split(price_end_time, ' ')[0], '-', '')>='${sdt_yes}' 
		and effective='true'
	) b1 
where 
	b1.pm=1
	
-- 商品售价指导：	
select 
	warehouse_code,
	product_code 
from 
	csx_ods.csx_ods_csx_price_prod_goods_price_guide_df 
where 
	sdt='${sdt_yes}' 
	and regexp_replace(substr(price_begin_time,1,10),'-','')<='${sdt_fweek}' 
	and regexp_replace(substr(price_end_time,1,10),'-','')>='${sdt_date}' 
	and warehouse_code='W0A6' 
	and suggest_price_type='3' 
	and is_expired='false' 
group by 
	warehouse_code,product_code
;

-- 有采购报价的基础商品
select 
	a.warehouse_code,
	count(distinct a.product_code) as purchase_price_goods_cnt
 from 
	(
	select 
		*,row_number()over(partition by warehouse_code,product_code order by last_put_time) as pm 
	from 
		csx_ods.csx_ods_csx_price_prod_effective_purchase_prices_df  
	where 
		sdt='20221020' 
		-- and warehouse_code in ('W0A3')
		and regexp_replace(to_date(price_begin_time), '-', '')<='20221020' 
		and regexp_replace(to_date(price_end_time), '-', '')>='20221020' 
		and effective='true'
		and base_product_status in (0,7)
	) a 
	join 
		(
		select
			distinct inventory_dc_code,product_code 
		from
			csx_ods.csx_ods_b2b_mall_prod_yszx_dc_product_pool_df
		where
			sdt='20221020'
			and base_product_tag=1
		) b on b.inventory_dc_code=a.warehouse_code and b.product_code=a.product_code
where 
	a.pm=1
group by 
	a.warehouse_code
	
-- 基础商品总数	
select
	inventory_dc_code,count(distinct product_code) as goods_cnt
from
	csx_ods.csx_ods_b2b_mall_prod_yszx_dc_product_pool_df
where
	sdt='20221020'
	and base_product_tag=1
	-- and inventory_dc_code='W0A3'
	and base_product_status in (0,7)
group by 
	inventory_dc_code
;

-- 有建议售价的基础商品：	
select 
	warehouse_code,
	product_code 
from 
	csx_ods.csx_ods_csx_price_prod_goods_price_guide_df 
where 
	sdt='20221020' 
	and regexp_replace(substr(price_begin_time,1,10),'-','')<='20221020' 
	and regexp_replace(substr(price_end_time,1,10),'-','')>='20221020' 
	-- and warehouse_code='W0A6' 
	-- and suggest_price_type='3' 
	and is_expired='false' 
group by 
	warehouse_code,product_code
;

-- 换品开始时间
select
	a.inventory_dc_code,a.create_date
from
	(
	select
		inventory_dc_code,to_date(create_time) as create_date,count(id),row_number()over(partition by inventory_dc_code order by count(id) desc) as rn
	from
		csx_ods.csx_ods_b2b_mall_prod_yszx_change_product_task_df
	where
		sdt='20221020'
		-- and status in (1,2)
	group by 
		inventory_dc_code,to_date(create_time)
	) a 
where
	rn=1
;

-- 绑定基础商品池的客户数
-- 下单自动添加、报价自动添加、绑定基础商品池、开启汰换，4项配置的开启数

	select
		b.dc_code,
		count(distinct a.customer_code) as customer_cnt, -- 绑定基础商品池的客户数
		--count(distinct case when a.bind_common_product_flag=1 and a.create_order_auto_add_flag=1 
		--	and a.price_auto_add_flag=1 and a.remove_customer_product_flag=1 
		--	and b.customer_flag in ('Ⅰ类客户','Ⅱ类客户') then a.customer_code else null end) as customer_on_rule_cnt, -- 配置开启的客户数
		sum(case when b.customer_flag in ('Ⅰ类客户','Ⅱ类客户') then (a.bind_common_product_flag+a.create_order_auto_add_flag
			+a.price_auto_add_flag+a.remove_customer_product_flag)/4 else 0 end) as customer_on_rule_cnt, -- 配置开启的客户数
		
		count(distinct case when b.customer_flag in ('Ⅰ类客户','Ⅱ类客户') then a.customer_code else null end) as customer_need_on_rule_cnt, -- 需开启总数
		sum(c.sale_amt) as sale_amt -- 落地客户日配销售额
	from
		(
		select
			customer_code,
			bind_common_product_flag, -- 绑定基础商品池 0-否 1-是
			create_order_auto_add_flag, -- 下单自动添加标识 0-未启动 1-已启动
			price_auto_add_flag, -- 报价自动添加标识 0-未启动 1-已启动
			remove_customer_product_flag -- 自动移除商品池 0-关闭 1-开启	
		from
			csx_ods.csx_ods_b2b_mall_prod_yszx_cus_product_rule_df
		where
			sdt='20221026'
			and bind_common_product_flag=1 -- 绑定基础商品池 0-否 1-是
		) a 
		join
			(
			select
				dc_code,customer_code,customer_flag
			from
				csx_analyse.csx_analyse_fr_crm_customer_dc_rule_mf
			group by 
				dc_code,customer_code,customer_flag
			) b on b.customer_code=a.customer_code
		left join -- 客户销售额
			(
			select
				inventory_dc_code,customer_code,sum(sale_amt) as sale_amt
			from
				csx_dws.csx_dws_sale_detail_di
			where
				sdt>='20220101'
				and sdt<='20221026'
				and channel_code in ('1','7','9')
				and business_type_code in (1)
			group by 
				inventory_dc_code,customer_code
			) c on c.customer_code=a.customer_code and c.inventory_dc_code=b.dc_code
	group by 
		b.dc_code
;

-- 配置汰换规则的品类与品类总数
select
	a.dc_code,
	count(distinct a.big_category_code) as category_total, -- 品类总数
	count(distinct case when a.status=1 then a.big_category_code else null end) as category_on, -- 配置汰换规则的品类数
	count(distinct case when a.status=1 then a.big_category_code else null end)/count(distinct a.big_category_code) as category_rate -- 汰换配置完成度
from
	(
	select
		dc_code,dc_name,big_category_code,status
	from
		csx_ods.csx_ods_b2b_mall_prod_yszx_cus_product_remove_rule_df
	where
		sdt='20221026'
	group by 
		dc_code,dc_name,big_category_code,status
	) a 
	join	
		(
		select
			big_category_code
		from
			csx_analyse.csx_analyse_fr_category_rule_config_mf
		group by 
			big_category_code
		) b on b.big_category_code=a.big_category_code
group by 
	a.dc_code
;

-- 
select
	a.inventory_dc_code,
	sum(b.sale_amt) as sale_amt -- DC总日配销售额
from
	(
	select
		inventory_dc_code,product_code 
	from
		csx_ods.csx_ods_b2b_mall_prod_yszx_dc_product_pool_df
	where
		sdt='20221026'
		-- and base_product_tag=1
		and base_product_status in (0,7)
	group by 
		inventory_dc_code,product_code 
	) a 
	left join -- DC总日配销售额
		(
		select
			inventory_dc_code,goods_code,sum(sale_amt) as sale_amt
		from
			csx_dws.csx_dws_sale_detail_di
		where
			sdt>='20220101'
			and sdt<='20221026'
			and channel_code in ('1','7','9')
			and business_type_code in (1)
		group by 
			inventory_dc_code,goods_code
		) b on b.inventory_dc_code=a.dc_code and b.goods_code=a.product_code
group by 
	a.inventory_dc_code
;
select
	a.inventory_dc_code, 
	count(a.product_code) as goods_total, -- 基础商品池总数
	count(b.product_code) as purchase_price_goods_cnt, -- 有采购报价的基础商品数
	count(c.product_code) as guide_price_goods_cnt, -- 有建议售价的基础商品数
	if(count(a.product_code)=0,0,count(b.product_code)/count(a.product_code)) as purchase_price_goods_rate, -- 采购报价覆盖率
	if(count(a.product_code)=0,0,count(c.product_code)/count(a.product_code)) as guide_price_goods_rate, -- 建议售价覆盖率
	count(d.inventory_dc_code) as goods_sale_cnt, -- 有动销的基础商品数
	count(d.inventory_dc_code)/count(a.product_code) as goods_sale_rate, -- 动销率
	sum(sale_amt) as sale_amt -- 有动销的基础商品日配销售额
from
	( -- 基础商品
	select
		inventory_dc_code,product_code 
	from
		csx_ods.csx_ods_b2b_mall_prod_yszx_dc_product_pool_df
	where
		sdt='20221026'
		and base_product_tag=1
		and base_product_status in (0,7)
	group by 
		inventory_dc_code,product_code 
	) a 
	left join -- 有采购报价的基础商品
		(
		select 
			warehouse_code,product_code
		from 
			(
			select 
				*,row_number()over(partition by warehouse_code,product_code order by last_put_time) as pm 
			from 
				csx_ods.csx_ods_csx_price_prod_effective_purchase_prices_df  
			where 
				sdt='20221026' 
				and regexp_replace(to_date(price_begin_time), '-', '')<='20221026' 
				and regexp_replace(to_date(price_end_time), '-', '')>='20221026' 
				and effective='true'
			) a 
		where
			a.pm=1
		group by 
			warehouse_code,product_code
		) b on b.warehouse_code=a.inventory_dc_code and b.product_code=a.product_code
	left join -- 有建议售价的基础商品
		(	
		select 
			warehouse_code,product_code 
		from 
			csx_ods.csx_ods_csx_price_prod_goods_price_guide_df 
		where 
			sdt='20221026'
			and regexp_replace(to_date(price_begin_time),'-','')<='20221026' 
			and regexp_replace(to_date(price_end_time),'-','')>='20221026' 
			-- and suggest_price_type='3' 
			and is_expired='false' 
		group by 
			warehouse_code,product_code
		) c on c.warehouse_code=a.inventory_dc_code and c.product_code=a.product_code
	left join
		(
		select
			inventory_dc_code,goods_code,sum(sale_amt) as sale_amt
		from
			csx_dws.csx_dws_sale_detail_di
		where
			sdt>='20220101'
			and sdt<='20221026'
			and channel_code in ('1','7','9')
			and business_type_code in (1)
		group by 
			inventory_dc_code,goods_code
		) d on d.inventory_dc_code=a.inventory_dc_code and d.goods_code=a.product_code
group by 
	a.inventory_dc_code
;

-- 待替换商品为基础商品的数量
-- 待替换商品不是基础商品的数量
select
	a.inventory_dc_code,
	count(distinct case when b.inventory_dc_code is not null then a.main_product_code else null end) as base_product_cnt,-- 待替换商品为基础商品的数量
	count(distinct case when b.inventory_dc_code is null then a.main_product_code else null end) as no_base_product_cnt, -- 待替换商品不是基础商品的数量
	count(distinct a.main_product_code) as product_cnt, -- 需换品商品数
	count(distinct case when a.status='1' then a.main_product_code else null end) as pending_product_cnt, -- 待处理商品数
	count(distinct case when a.status='2' then a.main_product_code else null end) as finish_product_cnt, -- 完成商品数
	count(distinct case when a.status='3' then a.main_product_code else null end) as refuse_product_cnt -- 拒绝商品数
from
	(
	select
		inventory_dc_code,main_product_code,sap_cus_code,status
	from
		csx_ods.csx_ods_b2b_mall_prod_yszx_change_product_task_df
	where
		sdt='20221026'
		-- and status in (1,2)
	group by 
		inventory_dc_code,main_product_code,sap_cus_code,status
	) a 
	left join
		(
		select
			inventory_dc_code,product_code 
		from
			csx_ods.csx_ods_b2b_mall_prod_yszx_dc_product_pool_df
		where
			sdt='20221026'
			and base_product_tag=1 -- 基础商品
			and base_product_status in (0,7)
		group by 
			inventory_dc_code,product_code 
		) b on b.inventory_dc_code=a.inventory_dc_code and b.product_code=a.main_product_code
group by 
	a.inventory_dc_code
;

-- 客户+商品销售额

select
	a.inventory_dc_code,
	sum(a.sale_amt) as total_sale_amt, -- 总销售额
	sum(case when a.status=2 then a.sale_amt else 0 end ) as finish_sale_amt, -- 完成商品销售额
	sum(case when a.status=3 then a.sale_amt else 0 end ) as refuse_sale_amt -- 拒绝商品销售额
from
	(
	select
		a.inventory_dc_code,a.main_product_code,a.sap_cus_code,a.status,a.create_date,a.month_ago_1,a.month_ago_2,a.month_ago_3,
		coalesce(a.month_ago_1_sale_amt,a.month_ago_2_sale_amt,a.month_ago_3_sale_amt,0) as sale_amt
	from
		(
		select
			a.inventory_dc_code,a.main_product_code,a.sap_cus_code,a.status,a.create_date,a.month_ago_1,a.month_ago_2,a.month_ago_3,
			sum(case when b.sdt>=a.month_ago_1 and b.sdt<=a.create_date then b.sale_amt else null end) as month_ago_1_sale_amt,
			sum(case when b.sdt>=a.month_ago_2 and b.sdt<=a.create_date then b.sale_amt else null end) as month_ago_2_sale_amt,
			sum(case when b.sdt>=a.month_ago_3 and b.sdt<=a.create_date then b.sale_amt else null end) as month_ago_3_sale_amt
		from
			(
			select
				inventory_dc_code,main_product_code,sap_cus_code,status,regexp_replace(to_date(create_time),'-','') as create_date,
				regexp_replace(add_months(to_date(create_time),-1),'-','') as month_ago_1,
				regexp_replace(add_months(to_date(create_time),-2),'-','') as month_ago_2,
				regexp_replace(add_months(to_date(create_time),-3),'-','') as month_ago_3
			from
				csx_ods.csx_ods_b2b_mall_prod_yszx_change_product_task_df
			where
				sdt='20221026'
				and inventory_dc_code='W0A3'
			) a 
			join
				(
				select
					sdt,inventory_dc_code,customer_code,goods_code,sum(sale_amt) as sale_amt
				from
					csx_dws.csx_dws_sale_detail_di
				where
					sdt>=(select regexp_replace(add_months(to_date(min(create_time)),-3),'-','') from csx_ods.csx_ods_b2b_mall_prod_yszx_change_product_task_df where sdt='20221026' and inventory_dc_code='W0A3')
					and sdt<='20221026'
					and channel_code in ('1','7','9')
					and business_type_code in (1)
					and inventory_dc_code='W0A3'
				group by 
					sdt,inventory_dc_code,customer_code,goods_code
				) b on b.inventory_dc_code=a.inventory_dc_code and b.customer_code=a.sap_cus_code and b.goods_code=a.main_product_code
		group by 
			a.inventory_dc_code,a.main_product_code,a.sap_cus_code,a.status,a.create_date,a.month_ago_1,a.month_ago_2,a.month_ago_3
		) a 
	) a 
group by 
	a.inventory_dc_code
;

-- 通过下单添加至客户商品池的商品

select
	a.inventory_dc_code,
	count(distinct a.product_code) as xd_goods_cnt,
	count(distinct b.product_code) as base_xd_goods_cnt,
	count(distinct b.product_code)/count(distinct a.product_code)
from
	(
	select
		customer_code,inventory_dc_code,product_code,data_source
	from
		csx_ods.csx_ods_b2b_mall_prod_yszx_customer_product_df
	where
		data_source=1 -- 数据来源：0-手动添加 1-客户订单 2-报价 3-商品池模板 4-必售商品 5-商品池模板替换 6-新品 7-基础商品池 8-CRM换品 9-销售添加
	)a 
	left join
		(
		select
			inventory_dc_code,product_code 
		from
			csx_ods.csx_ods_b2b_mall_prod_yszx_dc_product_pool_df
		where
			sdt='20221026'
			and base_product_tag=1 -- 基础商品
			and base_product_status in (0,7)
		group by 
			inventory_dc_code,product_code 
		) b on b.inventory_dc_code=a.inventory_dc_code and b.product_code=a.product_code
group by 
	a.inventory_dc_code
;

-- ==============================================================================================================================================================================
	-- if(count(a.product_code)=0,0,count(b.product_code)/count(a.product_code)) as purchase_price_goods_rate, -- 采购报价覆盖率
	-- if(count(a.product_code)=0,0,count(c.product_code)/count(a.product_code)) as guide_price_goods_rate, -- 建议售价覆盖率
	-- count(d.inventory_dc_code)/count(a.product_code) as goods_sale_rate, -- 动销率
select
	performance_province_name,
	inventory_dc_code,
	base_purchase_price_goods_cnt/base_goods_cnt as base_purchase_price_goods_rate,
	base_guide_price_goods_cnt/base_goods_cnt as base_guide_price_goods_rate,
	on_rule_customer_cnt/need_on_rule_customer_cnt as on_rule_rate,
	config_th_rule_category_cnt/category_cnt as config_th_rate,
	bind_common_product_customer_cnt,
	bind_common_product_customer_sale_amt/dc_sale_amt as sale_amt_rate,
	base_goods_cnt,
	base_sale_goods_cnt/base_goods_cnt as base_sale_goods_rate,
	base_goods_sale_amt/dc_sale_amt as sale_amt_coverage,
	create_date,
	base_dth_product_cnt,
	no_base_dth_product_cnt,
	coalesce((finish_product_cnt/(finish_product_cnt+refuse_product_cnt))*0.5,0)+coalesce((finish_sale_amt/(finish_sale_amt+refuse_sale_amt))*0.5,0) as hp_pass_rate,
	coalesce((finish_product_cnt+refuse_product_cnt)/(finish_product_cnt+refuse_product_cnt+pending_product_cnt),0) as hp_deal_rate,
	base_xd_goods_cnt/xd_goods_cnt as xd_goods_rate
from
	(
	select
		h.performance_region_code, -- 大区编码
		h.performance_region_name, -- 大区名称
		h.performance_province_code, -- 省份编码
		h.performance_province_name, -- 省份名称
		h.performance_city_code, -- 城市编码
		h.performance_city_name, -- 城市名称
		a.inventory_dc_code, -- dc编码
		a.create_date, -- 换品开始时间
		a.base_goods_cnt, -- 基础商品池总数
		a.base_purchase_price_goods_cnt, -- 有采购报价的基础商品数
		a.base_guide_price_goods_cnt, -- 有建议售价的基础商品数
		a.base_sale_goods_cnt, -- 有动销的基础商品数
		a.base_goods_sale_amt, -- 有动销的基础商品日配销售额
		b.dc_sale_amt, -- dc日配销售额
		c.bind_common_product_customer_cnt, -- 绑定基础商品池的客户数
		c.on_rule_customer_cnt, -- 配置开启的客户数
		c.need_on_rule_customer_cnt, -- 需开启总数
		c.bind_common_product_customer_sale_amt, -- 落地客户日配销售额
		d.category_cnt, -- 品类总数
		d.config_th_rule_category_cnt, -- 配置汰换规则的品类数
		e.base_dth_product_cnt,-- 待替换商品为基础商品的数量
		e.no_base_dth_product_cnt, -- 待替换商品不是基础商品的数量
		e.need_th_product_cnt, -- 需换品商品数
		e.pending_product_cnt, -- 待处理商品数
		e.finish_product_cnt, -- 完成商品数
		e.refuse_product_cnt, -- 拒绝商品数	
		f.total_sale_amt, -- 总销售额
		f.finish_sale_amt, -- 完成商品销售额
		f.refuse_sale_amt, -- 拒绝商品销售额	
		g.xd_goods_cnt, -- 通过下单添加至客户商品池的商品 商品总数
		g.base_xd_goods_cnt -- 基础商品数	
	from
		(
		select
			a.inventory_dc_code, -- dc编码
			e.create_date, -- 换品开始时间
			count(a.product_code) as base_goods_cnt, -- 基础商品池总数
			count(b.product_code) as base_purchase_price_goods_cnt, -- 有采购报价的基础商品数
			count(c.product_code) as base_guide_price_goods_cnt, -- 有建议售价的基础商品数
			count(d.inventory_dc_code) as base_sale_goods_cnt, -- 有动销的基础商品数
			sum(sale_amt) as base_goods_sale_amt -- 有动销的基础商品日配销售额
		from
			( -- 基础商品
			select
				inventory_dc_code,product_code 
			from
				csx_ods.csx_ods_b2b_mall_prod_yszx_dc_product_pool_df
			where
				sdt='20221026'
				and base_product_tag=1
				and base_product_status in (0,7)
			group by 
				inventory_dc_code,product_code 
			) a 
			left join -- 有采购报价的基础商品
				(
				select 
					warehouse_code,product_code
				from 
					(
					select 
						*,row_number()over(partition by warehouse_code,product_code order by last_put_time) as pm 
					from 
						csx_ods.csx_ods_csx_price_prod_effective_purchase_prices_df  
					where 
						sdt='20221026' 
						and regexp_replace(to_date(price_begin_time), '-', '')<='20221026' 
						and regexp_replace(to_date(price_end_time), '-', '')>='20221026' 
						and effective='true'
					) a 
				where
					a.pm=1
				group by 
					warehouse_code,product_code
				) b on b.warehouse_code=a.inventory_dc_code and b.product_code=a.product_code
			left join -- 有建议售价的基础商品
				(	
				select 
					warehouse_code,product_code 
				from 
					csx_ods.csx_ods_csx_price_prod_goods_price_guide_df 
				where 
					sdt='20221026'
					and regexp_replace(to_date(price_begin_time),'-','')<='20221026' 
					and regexp_replace(to_date(price_end_time),'-','')>='20221026' 
					-- and suggest_price_type='3' 
					and is_expired='false' 
				group by 
					warehouse_code,product_code
				) c on c.warehouse_code=a.inventory_dc_code and c.product_code=a.product_code
			left join
				(
				select
					inventory_dc_code,goods_code,sum(sale_amt) as sale_amt
				from
					csx_dws.csx_dws_sale_detail_di
				where
					sdt>='20221001'
					and sdt<='20221026'
					and channel_code in ('1','7','9')
					and business_type_code in (1)
				group by 
					inventory_dc_code,goods_code
				) d on d.inventory_dc_code=a.inventory_dc_code and d.goods_code=a.product_code
			left join
				(
				select
					a.inventory_dc_code,a.create_date
				from
					(
					select
						inventory_dc_code,to_date(update_time) as create_date,count(id),row_number()over(partition by inventory_dc_code order by count(id) desc) as rn
					from
						csx_ods.csx_ods_b2b_mall_prod_yszx_change_product_config_df -- 换品配置表
					where
						sdt='20221026'
					group by 
						inventory_dc_code,to_date(update_time)
					) a 
				where
					rn=1
				) e on e.inventory_dc_code=a.inventory_dc_code
		group by 
			a.inventory_dc_code,e.create_date
		) a 
		left join -- DC总日配销售额
			(
			select
				a.inventory_dc_code,
				sum(b.sale_amt) as dc_sale_amt -- DC总日配销售额
			from
				(
				select
					inventory_dc_code,product_code 
				from
					csx_ods.csx_ods_b2b_mall_prod_yszx_dc_product_pool_df
				where
					sdt='20221026'
					-- and base_product_tag=1
					-- and base_product_status in (0,7)
				group by 
					inventory_dc_code,product_code 
				) a 
				left join -- DC总日配销售额
					(
					select
						inventory_dc_code,goods_code,sum(sale_amt) as sale_amt
					from
						csx_dws.csx_dws_sale_detail_di
					where
						sdt>='20221001'
						and sdt<='20221026'
						and channel_code in ('1','7','9')
						and business_type_code in (1)
					group by 
						inventory_dc_code,goods_code
					) b on b.inventory_dc_code=a.inventory_dc_code and b.goods_code=a.product_code
			group by 
				a.inventory_dc_code
			) b on b.inventory_dc_code=a.inventory_dc_code
		left join
			(
			select
				b.dc_code,
				count(distinct a.customer_code) as bind_common_product_customer_cnt, -- 绑定基础商品池的客户数
				--count(distinct case when a.bind_common_product_flag=1 and a.create_order_auto_add_flag=1 
				--	and a.price_auto_add_flag=1 and a.remove_customer_product_flag=1 
				--	and b.customer_flag in ('Ⅰ类客户','Ⅱ类客户') then a.customer_code else null end) as customer_on_rule_cnt, -- 配置开启的客户数
				sum(case when b.customer_flag in ('Ⅰ类客户','Ⅱ类客户') then (a.bind_common_product_flag+a.create_order_auto_add_flag
					+a.price_auto_add_flag+a.remove_customer_product_flag)/4 else 0 end) as on_rule_customer_cnt, -- 配置开启的客户数		
				count(distinct case when b.customer_flag in ('Ⅰ类客户','Ⅱ类客户') then a.customer_code else null end) as need_on_rule_customer_cnt, -- 需开启总数
				sum(c.sale_amt) as bind_common_product_customer_sale_amt -- 落地客户日配销售额
			from
				(
				select
					customer_code,
					bind_common_product_flag, -- 绑定基础商品池 0-否 1-是
					create_order_auto_add_flag, -- 下单自动添加标识 0-未启动 1-已启动
					price_auto_add_flag, -- 报价自动添加标识 0-未启动 1-已启动
					remove_customer_product_flag -- 自动移除商品池 0-关闭 1-开启	
				from
					csx_ods.csx_ods_b2b_mall_prod_yszx_cus_product_rule_df
				where
					sdt='20221026'
					and bind_common_product_flag=1 -- 绑定基础商品池 0-否 1-是
				) a 
				join
					(
					select
						dc_code,customer_code,customer_flag
					from
						csx_analyse.csx_analyse_fr_crm_customer_dc_rule_mf
					group by 
						dc_code,customer_code,customer_flag
					) b on b.customer_code=a.customer_code
				left join -- 客户销售额
					(
					select
						inventory_dc_code,customer_code,sum(sale_amt) as sale_amt
					from
						csx_dws.csx_dws_sale_detail_di
					where
						sdt>='20221001'
						and sdt<='20221026'
						and channel_code in ('1','7','9')
						and business_type_code in (1)
					group by 
						inventory_dc_code,customer_code
					) c on c.customer_code=a.customer_code and c.inventory_dc_code=b.dc_code
			group by 
				b.dc_code
			) c on c.dc_code=a.inventory_dc_code
		left join -- 配置汰换规则的品类与品类总数
			(		
			select
				a.dc_code,
				count(distinct a.big_category_code) as category_cnt, -- 品类总数
				count(distinct case when a.status=1 then a.big_category_code else null end) as config_th_rule_category_cnt -- 配置汰换规则的品类数
			from
				(
				select
					dc_code,dc_name,big_category_code,status
				from
					csx_ods.csx_ods_b2b_mall_prod_yszx_cus_product_remove_rule_df
				where
					sdt='20221026'
				group by 
					dc_code,dc_name,big_category_code,status
				) a 
				join	
					(
					select
						big_category_code
					from
						csx_analyse.csx_analyse_fr_category_rule_config_mf
					group by 
						big_category_code
					) b on b.big_category_code=a.big_category_code
			group by 
				a.dc_code
			) d on d.dc_code=a.inventory_dc_code
		left join
			(
			select
				a.inventory_dc_code,
				count(distinct case when b.inventory_dc_code is not null then a.main_product_code else null end) as base_dth_product_cnt,-- 待替换商品为基础商品的数量
				count(distinct case when b.inventory_dc_code is null then a.main_product_code else null end) as no_base_dth_product_cnt, -- 待替换商品不是基础商品的数量
				count(distinct a.main_product_code) as need_th_product_cnt, -- 需换品商品数
				count(distinct case when a.status='1' then a.main_product_code else null end) as pending_product_cnt, -- 待处理商品数
				count(distinct case when a.status='2' then a.main_product_code else null end) as finish_product_cnt, -- 完成商品数
				count(distinct case when a.status='3' then a.main_product_code else null end) as refuse_product_cnt -- 拒绝商品数
			from
				(
				select
					inventory_dc_code,main_product_code,sap_cus_code,status
				from
					csx_ods.csx_ods_b2b_mall_prod_yszx_change_product_task_df
				where
					sdt='20221026'
					-- and status in (1,2)
				group by 
					inventory_dc_code,main_product_code,sap_cus_code,status
				) a 
				left join
					(
					select
						inventory_dc_code,product_code 
					from
						csx_ods.csx_ods_b2b_mall_prod_yszx_dc_product_pool_df
					where
						sdt='20221026'
						and base_product_tag=1 -- 基础商品
						and base_product_status in (0,7)
					group by 
						inventory_dc_code,product_code 
					) b on b.inventory_dc_code=a.inventory_dc_code and b.product_code=a.main_product_code
			group by 
				a.inventory_dc_code
			) e on e.inventory_dc_code=a.inventory_dc_code
		left join
			(
			select
				a.inventory_dc_code,
				sum(a.sale_amt) as total_sale_amt, -- 总销售额
				sum(case when a.status=2 then a.sale_amt else 0 end ) as finish_sale_amt, -- 完成商品销售额
				sum(case when a.status=3 then a.sale_amt else 0 end ) as refuse_sale_amt -- 拒绝商品销售额
			from
				(
				select
					a.inventory_dc_code,a.main_product_code,a.sap_cus_code,a.status,a.create_date,a.month_ago_1,a.month_ago_2,a.month_ago_3,
					coalesce(a.month_ago_1_sale_amt,a.month_ago_2_sale_amt,a.month_ago_3_sale_amt,0) as sale_amt
				from
					(
					select
						a.inventory_dc_code,a.main_product_code,a.sap_cus_code,a.status,a.create_date,a.month_ago_1,a.month_ago_2,a.month_ago_3,
						sum(case when b.sdt>=a.month_ago_1 and b.sdt<=a.create_date then b.sale_amt else null end) as month_ago_1_sale_amt,
						sum(case when b.sdt>=a.month_ago_2 and b.sdt<=a.create_date then b.sale_amt else null end) as month_ago_2_sale_amt,
						sum(case when b.sdt>=a.month_ago_3 and b.sdt<=a.create_date then b.sale_amt else null end) as month_ago_3_sale_amt
					from
						(
						select
							inventory_dc_code,main_product_code,sap_cus_code,status,regexp_replace(to_date(create_time),'-','') as create_date,
							regexp_replace(add_months(to_date(create_time),-1),'-','') as month_ago_1,
							regexp_replace(add_months(to_date(create_time),-2),'-','') as month_ago_2,
							regexp_replace(add_months(to_date(create_time),-3),'-','') as month_ago_3
						from
							csx_ods.csx_ods_b2b_mall_prod_yszx_change_product_task_df
						where
							sdt='20221026'
							-- and inventory_dc_code='W0A3'
						) a 
						join
							(
							select
								sdt,inventory_dc_code,customer_code,goods_code,sum(sale_amt) as sale_amt
							from
								csx_dws.csx_dws_sale_detail_di
							where
								sdt>=(select regexp_replace(add_months(to_date(min(create_time)),-3),'-','') from csx_ods.csx_ods_b2b_mall_prod_yszx_change_product_task_df where sdt='20221026')
								and sdt<='20221026'
								and channel_code in ('1','7','9')
								and business_type_code in (1)
								-- and inventory_dc_code='W0A3'
							group by 
								sdt,inventory_dc_code,customer_code,goods_code
							) b on b.inventory_dc_code=a.inventory_dc_code and b.customer_code=a.sap_cus_code and b.goods_code=a.main_product_code
					group by 
						a.inventory_dc_code,a.main_product_code,a.sap_cus_code,a.status,a.create_date,a.month_ago_1,a.month_ago_2,a.month_ago_3
					) a 
				) a 
			group by 
				a.inventory_dc_code
			) f on f.inventory_dc_code=a.inventory_dc_code
		left join -- 通过下单添加至客户商品池的商品
			(
			select
				a.inventory_dc_code,
				count(distinct a.product_code) as xd_goods_cnt, -- 通过下单添加至客户商品池的商品 商品总数
				count(distinct b.product_code) as base_xd_goods_cnt -- 基础商品数
			from
				(
				select
					customer_code,inventory_dc_code,product_code,data_source
				from
					csx_ods.csx_ods_b2b_mall_prod_yszx_customer_product_df
				where
					data_source=1 -- 数据来源：0-手动添加 1-客户订单 2-报价 3-商品池模板 4-必售商品 5-商品池模板替换 6-新品 7-基础商品池 8-CRM换品 9-销售添加
				)a 
				left join
					(
					select
						inventory_dc_code,product_code 
					from
						csx_ods.csx_ods_b2b_mall_prod_yszx_dc_product_pool_df
					where
						sdt='20221026'
						and base_product_tag=1 -- 基础商品
						and base_product_status in (0,7)
					group by 
						inventory_dc_code,product_code 
					) b on b.inventory_dc_code=a.inventory_dc_code and b.product_code=a.product_code
			group by 
				a.inventory_dc_code
			) g on g.inventory_dc_code=a.inventory_dc_code
		left join
			(
			select
				shop_code,performance_region_code,performance_region_name,performance_province_code,performance_province_name,performance_city_code,performance_city_name
			from
				csx_dim.csx_dim_shop
			where
				sdt='current'
			) h on h.shop_code=a.inventory_dc_code
	) a 
;

--============================================================================================================================================================================
-- 全国各省区整体维度数据
select
	performance_province_name,
	inventory_dc_code,
	base_purchase_price_goods_cnt/base_goods_cnt as base_purchase_price_goods_rate,
	base_guide_price_goods_cnt/base_goods_cnt as base_guide_price_goods_rate,
	on_rule_customer_cnt/need_on_rule_customer_cnt as on_rule_rate,
	config_th_rule_category_cnt/category_cnt as config_th_rate,
	bind_common_product_customer_cnt,
	bind_common_product_customer_sale_amt/dc_sale_amt as sale_amt_rate,
	base_goods_cnt,
	base_sale_goods_cnt/base_goods_cnt as base_sale_goods_rate,
	base_goods_sale_amt/dc_sale_amt as sale_amt_coverage,
	create_date,
	base_dth_product_cnt,
	no_base_dth_product_cnt,
	coalesce((finish_product_cnt/(finish_product_cnt+refuse_product_cnt))*0.5,0)+coalesce((finish_sale_amt/(finish_sale_amt+refuse_sale_amt))*0.5,0) as hp_pass_rate,
	coalesce((finish_product_cnt+refuse_product_cnt)/(finish_product_cnt+refuse_product_cnt+pending_product_cnt),0) as hp_deal_rate,
	base_xd_goods_cnt/xd_goods_cnt as xd_goods_rate
from
	(
	select
		h.performance_region_code, -- 大区编码
		h.performance_region_name, -- 大区名称
		h.performance_province_code, -- 省份编码
		h.performance_province_name, -- 省份名称
		h.performance_city_code, -- 城市编码
		h.performance_city_name, -- 城市名称
		a.inventory_dc_code, -- dc编码
		a.create_date, -- 换品开始时间
		a.base_goods_cnt, -- 基础商品池总数
		a.base_purchase_price_goods_cnt, -- 有采购报价的基础商品数
		a.base_guide_price_goods_cnt, -- 有建议售价的基础商品数
		a.base_sale_goods_cnt, -- 有动销的基础商品数
		a.base_goods_sale_amt, -- 有动销的基础商品日配销售额
		b.dc_sale_amt, -- dc日配销售额
		c.bind_common_product_customer_cnt, -- 绑定基础商品池的客户数
		c.on_rule_customer_cnt, -- 配置开启的客户数
		c.need_on_rule_customer_cnt, -- 需开启总数
		c.bind_common_product_customer_sale_amt, -- 落地客户日配销售额
		d.category_cnt, -- 品类总数
		d.config_th_rule_category_cnt, -- 配置汰换规则的品类数
		e.base_dth_product_cnt,-- 待替换商品为基础商品的数量
		e.no_base_dth_product_cnt, -- 待替换商品不是基础商品的数量
		e.need_th_product_cnt, -- 需换品商品数
		e.pending_product_cnt, -- 待处理商品数
		e.finish_product_cnt, -- 完成商品数
		e.refuse_product_cnt, -- 拒绝商品数	
		f.total_sale_amt, -- 总销售额
		f.finish_sale_amt, -- 完成商品销售额
		f.refuse_sale_amt, -- 拒绝商品销售额	
		g.xd_goods_cnt, -- 通过下单添加至客户商品池的商品 商品总数
		g.base_xd_goods_cnt -- 基础商品数	
	from
		(
		select
			a.inventory_dc_code, -- dc编码
			e.create_date, -- 换品开始时间
			count(a.product_code) as base_goods_cnt, -- 基础商品池总数
			count(b.product_code) as base_purchase_price_goods_cnt, -- 有采购报价的基础商品数
			count(c.product_code) as base_guide_price_goods_cnt, -- 有建议售价的基础商品数
			count(d.inventory_dc_code) as base_sale_goods_cnt, -- 有动销的基础商品数
			sum(sale_amt) as base_goods_sale_amt -- 有动销的基础商品日配销售额
		from
			( -- 基础商品
			select
				inventory_dc_code,product_code 
			from
				csx_ods.csx_ods_b2b_mall_prod_yszx_dc_product_pool_df
			where
				sdt='20221026'
				and base_product_tag=1
				and base_product_status in (0,7)
			group by 
				inventory_dc_code,product_code 
			) a 
			left join -- 有采购报价的基础商品
				(
				select 
					warehouse_code,product_code
				from 
					(
					select 
						*,row_number()over(partition by warehouse_code,product_code order by last_put_time) as pm 
					from 
						csx_ods.csx_ods_csx_price_prod_effective_purchase_prices_df  
					where 
						sdt='20221026' 
						and regexp_replace(to_date(price_begin_time), '-', '')<='20221026' 
						and regexp_replace(to_date(price_end_time), '-', '')>='20221026' 
						and effective='true'
					) a 
				where
					a.pm=1
				group by 
					warehouse_code,product_code
				) b on b.warehouse_code=a.inventory_dc_code and b.product_code=a.product_code
			left join -- 有建议售价的基础商品
				(	
				select 
					warehouse_code,product_code 
				from 
					csx_ods.csx_ods_csx_price_prod_goods_price_guide_df 
				where 
					sdt='20221026'
					and regexp_replace(to_date(price_begin_time),'-','')<='20221026' 
					and regexp_replace(to_date(price_end_time),'-','')>='20221026' 
					-- and suggest_price_type='3' 
					and is_expired='false' 
				group by 
					warehouse_code,product_code
				) c on c.warehouse_code=a.inventory_dc_code and c.product_code=a.product_code
			left join
				(
				select
					inventory_dc_code,goods_code,sum(sale_amt) as sale_amt
				from
					csx_dws.csx_dws_sale_detail_di
				where
					sdt>='20221001'
					and sdt<='20221026'
					and channel_code in ('1','7','9')
					and business_type_code in (1)
				group by 
					inventory_dc_code,goods_code
				) d on d.inventory_dc_code=a.inventory_dc_code and d.goods_code=a.product_code
			left join
				(
				select
					a.inventory_dc_code,a.create_date
				from
					(
					select
						inventory_dc_code,to_date(update_time) as create_date,count(id),row_number()over(partition by inventory_dc_code order by count(id) desc) as rn
					from
						csx_ods.csx_ods_b2b_mall_prod_yszx_change_product_config_df -- 换品配置表
					where
						sdt='20221026'
					group by 
						inventory_dc_code,to_date(update_time)
					) a 
				where
					rn=1
				) e on e.inventory_dc_code=a.inventory_dc_code
		group by 
			a.inventory_dc_code,e.create_date
		) a 
		left join -- DC总日配销售额
			(
			select
				inventory_dc_code,
				sum(sale_amt) as dc_sale_amt -- DC总日配销售额
			from
				csx_dws.csx_dws_sale_detail_di
			where
				sdt>='20221001'
				and sdt<='20221026'
				and channel_code in ('1','7','9')
				and business_type_code in (1)			
			group by 
				inventory_dc_code
			) b on b.inventory_dc_code=a.inventory_dc_code
		left join
			(
			select
				b.dc_code,
				count(distinct a.customer_code) as bind_common_product_customer_cnt, -- 绑定基础商品池的客户数
				--count(distinct case when a.bind_common_product_flag=1 and a.create_order_auto_add_flag=1 
				--	and a.price_auto_add_flag=1 and a.remove_customer_product_flag=1 
				--	and b.customer_flag in ('Ⅰ类客户','Ⅱ类客户') then a.customer_code else null end) as customer_on_rule_cnt, -- 配置开启的客户数
				sum(case when b.customer_flag in ('Ⅰ类客户','Ⅱ类客户') then (a.bind_common_product_flag+a.create_order_auto_add_flag
					+a.price_auto_add_flag+a.remove_customer_product_flag)/4 else 0 end) as on_rule_customer_cnt, -- 配置开启的客户数		
				count(distinct case when b.customer_flag in ('Ⅰ类客户','Ⅱ类客户') then a.customer_code else null end) as need_on_rule_customer_cnt, -- 需开启总数
				sum(c.sale_amt) as bind_common_product_customer_sale_amt -- 落地客户日配销售额
			from
				(
				select
					customer_code,
					bind_common_product_flag, -- 绑定基础商品池 0-否 1-是
					create_order_auto_add_flag, -- 下单自动添加标识 0-未启动 1-已启动
					price_auto_add_flag, -- 报价自动添加标识 0-未启动 1-已启动
					remove_customer_product_flag -- 自动移除商品池 0-关闭 1-开启	
				from
					csx_ods.csx_ods_b2b_mall_prod_yszx_cus_product_rule_df
				where
					sdt='20221026'
					and bind_common_product_flag=1 -- 绑定基础商品池 0-否 1-是
				) a 
				join
					(
					select
						dc_code,customer_code,customer_flag
					from
						csx_analyse.csx_analyse_fr_crm_customer_dc_rule_mf
					group by 
						dc_code,customer_code,customer_flag
					) b on b.customer_code=a.customer_code
				left join -- 客户销售额
					(
					select
						inventory_dc_code,customer_code,sum(sale_amt) as sale_amt
					from
						csx_dws.csx_dws_sale_detail_di
					where
						sdt>='20221001'
						and sdt<='20221026'
						and channel_code in ('1','7','9')
						and business_type_code in (1)
					group by 
						inventory_dc_code,customer_code
					) c on c.customer_code=a.customer_code and c.inventory_dc_code=b.dc_code
			group by 
				b.dc_code
			) c on c.dc_code=a.inventory_dc_code
		left join -- 配置汰换规则的品类与品类总数
			(		
			select
				a.dc_code,
				count(distinct a.big_category_code) as category_cnt, -- 品类总数
				count(distinct case when a.status=1 then a.big_category_code else null end) as config_th_rule_category_cnt -- 配置汰换规则的品类数
			from
				(
				select
					dc_code,dc_name,big_category_code,status
				from
					csx_ods.csx_ods_b2b_mall_prod_yszx_cus_product_remove_rule_df
				where
					sdt='20221026'
				group by 
					dc_code,dc_name,big_category_code,status
				) a 
				join	
					(
					select
						big_category_code
					from
						csx_analyse.csx_analyse_fr_category_rule_config_mf
					group by 
						big_category_code
					) b on b.big_category_code=a.big_category_code
			group by 
				a.dc_code
			) d on d.dc_code=a.inventory_dc_code
		left join
			(
			select
				a.inventory_dc_code,
				count(distinct case when b.inventory_dc_code is not null then a.main_product_code else null end) as base_dth_product_cnt,-- 待替换商品为基础商品的数量
				count(distinct case when b.inventory_dc_code is null then a.main_product_code else null end) as no_base_dth_product_cnt, -- 待替换商品不是基础商品的数量
				count(distinct a.main_product_code) as need_th_product_cnt, -- 需换品商品数
				count(distinct case when a.status='1' then a.main_product_code else null end) as pending_product_cnt, -- 待处理商品数
				count(distinct case when a.status='2' then a.main_product_code else null end) as finish_product_cnt, -- 完成商品数
				count(distinct case when a.status='3' then a.main_product_code else null end) as refuse_product_cnt -- 拒绝商品数
			from
				(
				select
					inventory_dc_code,main_product_code,sap_cus_code,status
				from
					csx_ods.csx_ods_b2b_mall_prod_yszx_change_product_task_df
				where
					sdt='20221026'
					-- and status in (1,2)
				group by 
					inventory_dc_code,main_product_code,sap_cus_code,status
				) a 
				left join
					(
					select
						inventory_dc_code,product_code 
					from
						csx_ods.csx_ods_b2b_mall_prod_yszx_dc_product_pool_df
					where
						sdt='20221026'
						and base_product_tag=1 -- 基础商品
						and base_product_status in (0,7)
					group by 
						inventory_dc_code,product_code 
					) b on b.inventory_dc_code=a.inventory_dc_code and b.product_code=a.main_product_code
			group by 
				a.inventory_dc_code
			) e on e.inventory_dc_code=a.inventory_dc_code
		left join
			(
			select
				a.inventory_dc_code,
				sum(a.sale_amt) as total_sale_amt, -- 总销售额
				sum(case when a.status=2 then a.sale_amt else 0 end ) as finish_sale_amt, -- 完成商品销售额
				sum(case when a.status=3 then a.sale_amt else 0 end ) as refuse_sale_amt -- 拒绝商品销售额
			from
				(
				select
					a.inventory_dc_code,a.main_product_code,a.sap_cus_code,a.status,a.create_date,a.month_ago_1,a.month_ago_2,a.month_ago_3,
					coalesce(a.month_ago_1_sale_amt,a.month_ago_2_sale_amt,a.month_ago_3_sale_amt,0) as sale_amt
				from
					(
					select
						a.inventory_dc_code,a.main_product_code,a.sap_cus_code,a.status,a.create_date,a.month_ago_1,a.month_ago_2,a.month_ago_3,
						sum(case when b.sdt>=a.month_ago_1 and b.sdt<=a.create_date then b.sale_amt else null end) as month_ago_1_sale_amt,
						sum(case when b.sdt>=a.month_ago_2 and b.sdt<=a.create_date then b.sale_amt else null end) as month_ago_2_sale_amt,
						sum(case when b.sdt>=a.month_ago_3 and b.sdt<=a.create_date then b.sale_amt else null end) as month_ago_3_sale_amt
					from
						(
						select
							inventory_dc_code,main_product_code,sap_cus_code,status,regexp_replace(to_date(create_time),'-','') as create_date,
							regexp_replace(add_months(to_date(create_time),-1),'-','') as month_ago_1,
							regexp_replace(add_months(to_date(create_time),-2),'-','') as month_ago_2,
							regexp_replace(add_months(to_date(create_time),-3),'-','') as month_ago_3
						from
							csx_ods.csx_ods_b2b_mall_prod_yszx_change_product_task_df
						where
							sdt='20221026'
							-- and inventory_dc_code='W0A3'
						) a 
						join
							(
							select
								sdt,inventory_dc_code,customer_code,goods_code,sum(sale_amt) as sale_amt
							from
								csx_dws.csx_dws_sale_detail_di
							where
								sdt>=(select regexp_replace(add_months(to_date(min(create_time)),-3),'-','') from csx_ods.csx_ods_b2b_mall_prod_yszx_change_product_task_df where sdt='20221026')
								and sdt<='20221026'
								and channel_code in ('1','7','9')
								and business_type_code in (1)
								-- and inventory_dc_code='W0A3'
							group by 
								sdt,inventory_dc_code,customer_code,goods_code
							) b on b.inventory_dc_code=a.inventory_dc_code and b.customer_code=a.sap_cus_code and b.goods_code=a.main_product_code
					group by 
						a.inventory_dc_code,a.main_product_code,a.sap_cus_code,a.status,a.create_date,a.month_ago_1,a.month_ago_2,a.month_ago_3
					) a 
				) a 
			group by 
				a.inventory_dc_code
			) f on f.inventory_dc_code=a.inventory_dc_code
		left join -- 通过下单添加至客户商品池的商品
			(
			select
				a.inventory_dc_code,
				count(distinct a.product_code) as xd_goods_cnt, -- 通过下单添加至客户商品池的商品 商品总数
				count(distinct b.product_code) as base_xd_goods_cnt -- 基础商品数
			from
				(
				select
					customer_code,inventory_dc_code,product_code,data_source
				from
					csx_ods.csx_ods_b2b_mall_prod_yszx_customer_product_df
				where
					data_source=1 -- 数据来源：0-手动添加 1-客户订单 2-报价 3-商品池模板 4-必售商品 5-商品池模板替换 6-新品 7-基础商品池 8-CRM换品 9-销售添加
				)a 
				left join
					(
					select
						inventory_dc_code,product_code 
					from
						csx_ods.csx_ods_b2b_mall_prod_yszx_dc_product_pool_df
					where
						sdt='20221026'
						and base_product_tag=1 -- 基础商品
						and base_product_status in (0,7)
					group by 
						inventory_dc_code,product_code 
					) b on b.inventory_dc_code=a.inventory_dc_code and b.product_code=a.product_code
			group by 
				a.inventory_dc_code
			) g on g.inventory_dc_code=a.inventory_dc_code
		left join
			(
			select
				shop_code,performance_region_code,performance_region_name,performance_province_code,performance_province_name,performance_city_code,performance_city_name
			from
				csx_dim.csx_dim_shop
			where
				sdt='current'
			) h on h.shop_code=a.inventory_dc_code
	) a 

-- ============================================================================================================================================================================
-- 省区换品进度
select
	inventory_dc_code,count(distinct main_product_code,sap_cus_code)
from
	csx_ods.csx_ods_b2b_mall_prod_yszx_change_product_task_df
where
	sdt='20221026'
	and status in (2) -- 换品任务状态 1：待处理 2：已完成 3：已拒绝
	and inventory_dc_code='W0A3'
group by 
	inventory_dc_code
;

-- 换品复盘看板（省区内个人维度）	
select
	a.inventory_dc_code,
	a.operator,
	coalesce((finish_product_cnt/(finish_product_cnt+refuse_product_cnt))*0.5,0)+coalesce((finish_sale_amt/(finish_sale_amt+refuse_sale_amt))*0.5,0) as hp_pass_rate,
	(finish_product_cnt+refuse_product_cnt)/(pending_product_cnt+finish_product_cnt+refuse_product_cnt) as processing_progress,
	pending_product_cnt+finish_product_cnt+refuse_product_cnt as need_hp_cnt,
	finish_product_cnt,
	refuse_product_cnt,
	hp_sale_goods_cnt
from
	(
	select
		a.inventory_dc_code,
		a.operator,
		count(distinct case when a.status='1' then a.main_product_code else null end) as pending_product_cnt, -- 待处理商品数
		count(distinct case when a.status='2' then a.main_product_code else null end) as finish_product_cnt, -- 完成商品数
		count(distinct case when a.status='3' then a.main_product_code else null end) as refuse_product_cnt, -- 拒绝商品数		
		sum(a.sale_amt) as total_sale_amt, -- 总销售额
		sum(case when a.status=2 then a.sale_amt else 0 end ) as finish_sale_amt, -- 完成商品销售额
		sum(case when a.status=3 then a.sale_amt else 0 end ) as refuse_sale_amt, -- 拒绝商品销售额
		count(distinct case when a.status='2' and a.sale_amt>0 then a.main_product_code else null end) as hp_sale_goods_cnt
	from
		(
		select
			a.inventory_dc_code,a.main_product_code,a.sap_cus_code,a.status,a.operation_date,a.operator,
			coalesce(a.month_ago_1_sale_amt,a.month_ago_2_sale_amt,a.month_ago_3_sale_amt,0) as sale_amt
		from
			(
			select
				a.inventory_dc_code,a.main_product_code,a.sap_cus_code,a.status,a.operation_date,
				if(a.status=1,coalesce(c.rp_service_user_name_new,c.sales_name_new,'-'),a.update_by) as operator,
				sum(case when b.sdt>=a.month_ago_1 and b.sdt<=a.operation_date then b.sale_amt else null end) as month_ago_1_sale_amt,
				sum(case when b.sdt>=a.month_ago_2 and b.sdt<=a.operation_date then b.sale_amt else null end) as month_ago_2_sale_amt,
				sum(case when b.sdt>=a.month_ago_3 and b.sdt<=a.operation_date then b.sale_amt else null end) as month_ago_3_sale_amt
			from
				(
				select
					a.inventory_dc_code,a.main_product_code,a.sap_cus_code,a.status,update_by,regexp_replace(update_time,'-','') as operation_date,
					regexp_replace(add_months(update_time,-1),'-','') as month_ago_1,
					regexp_replace(add_months(update_time,-2),'-','') as month_ago_2,
					regexp_replace(add_months(update_time,-3),'-','') as month_ago_3
				from
					(
					select
						inventory_dc_code,main_product_code,sap_cus_code,status,update_by
					from
						csx_ods.csx_ods_b2b_mall_prod_yszx_change_product_task_df
					where
						sdt='20221031'
					) a 
					left join
						(
						select
							inventory_dc_code,main_product_code,to_date(update_time) as update_time
						from
							csx_ods.csx_ods_b2b_mall_prod_yszx_change_product_config_df -- 换品配置表
						where
							sdt='20221031'
						group by 
							inventory_dc_code,main_product_code,to_date(update_time)
						) b on b.inventory_dc_code=a.inventory_dc_code and b.main_product_code=a.main_product_code
				) a 
				left join
					(
					select
						sdt,inventory_dc_code,customer_code,goods_code,sum(sale_amt) as sale_amt
					from
						csx_dws.csx_dws_sale_detail_di
					where
						sdt>=(select regexp_replace(add_months(to_date(min(create_time)),-3),'-','') from csx_ods.csx_ods_b2b_mall_prod_yszx_change_product_task_df where sdt='20221031')
						and sdt<='20221031'
						and channel_code in ('1','7','9')
						and business_type_code in (1)
						-- and inventory_dc_code='W0A3'
					group by 
						sdt,inventory_dc_code,customer_code,goods_code
					) b on b.inventory_dc_code=a.inventory_dc_code and b.customer_code=a.sap_cus_code and b.goods_code=a.main_product_code
				left join
					(
					select
						customer_no,rp_service_user_id_new,rp_service_user_work_no_new,rp_service_user_name_new,
						sales_id_new,work_no_new,sales_name_new
					from
						csx_analyse.csx_analyse_report_crm_customer_sale_service_manager_info_df
					where
						sdt='20221031'
					) c on c.customer_no=a.sap_cus_code
			group by 
				a.inventory_dc_code,a.main_product_code,a.sap_cus_code,a.status,a.operation_date,
				if(a.status=1,coalesce(c.rp_service_user_name_new,c.sales_name_new,'-'),a.update_by)
			) a 
		) a 
	group by 
		a.inventory_dc_code,a.operator
	) a 

			
