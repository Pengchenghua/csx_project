-- ---------------------------------------
-- ---市调数据按照优先级取值
drop table if exists csx_analyse_tmp.csx_analyse_tmp_market_research_price;
create table if not exists csx_analyse_tmp.csx_analyse_tmp_market_research_price as 
select 
	t.*,
	row_number()over(partition by t.dc_code,t.shop_code,t.goods_code order by t.price_date desc,cast(t.market_research_price as decimal(20,6)) desc) as pm 
from 
	(-- 生效区通用市调
	select 
		'通用市调' as type, 
		b2.location_code as dc_code,
	    b1.shop_code,
	    b1.shop_name,
	    cast(b1.price_date as string) as price_date,
	    b2.product_code as goods_code,
	    (case when b1.product_status=1 then '促销' else '非促销' end) as product_status,-- 1促销 0正常
	    cast(b1.price as decimal(20,6)) as market_research_price 
	from 
	    (select * 
	    from csx_ods.csx_ods_csx_price_prod_market_research_not_yh_price_effective_df 
	    where substr(price_end_time,1,10)>='${yes_date}'  
	    and shop_code not in ('RW507','RW565','RW789','ZD230','ZD231','YW648','YW8',-- 华北
							'YW500','YS606','ZD512','ZD562','ZD513','ZD374','YS703','ZD581','YS784','ZD374', -- 华南
							'ZD631','RW207','ZD636', -- 华东
							'ZD114','ZD741','ZD701','ZD122','ZD775','ZD5','ZD674','RW391','ZD114','ZD807','ZD116','ZD157','ZD125','ZD371','ZD572','ZD149','ZD408','ZD438','ZD145','ZD152',
							'ZD430','ZD168','ZD286','ZD222','ZD730','ZD731','ZD120','ZD234','RS20','ZD292','ZD706','ZD557','RW790','RW110','RW810'-- 华西
				)
	    ) b1 
	    -- 只取TOP商品的对标市调地点
	    left join 
	    (select * 
		from csx_ods.csx_ods_csx_price_prod_market_research_product_df 
		where sdt='${yes_sdt}'
		) b2 
		on b1.product_id=b2.id 
		left join 
		(select 
			* 
		from csx_dim.csx_dim_shop 
		where sdt='current' 
		) b3  
		on b2.location_code=b3.shop_code 
	where (b3.performance_region_name='华南大区' and b1.shop_name not like '%综合价%') or b3.performance_region_name<>'华南大区' 	
	union all 
	-- 昨日失效的创价网价格
	select 
		'通用市调' as type, 
		c2.location_code as dc_code,
	    c1.shop_code,
	    c1.shop_name,
	    cast(c1.price_date as string) as price_date,
	    c2.product_code as goods_code,
	    (case when c1.product_status=1 then '促销' else '非促销' end) as product_status,-- 1促销 0正常
	    cast(c1.price as decimal(20,6)) as market_research_price 
	from 
		(select * 
		from csx_dwd.csx_dwd_market_research_not_yh_price_di 
		where substr((case when status=0 and price_end_time>update_time then update_time else price_end_time end),1,10)>='${yes_date}' 
		and shop_code  not in ('RW507','RW565','RW789','ZD230','ZD231','YW648','YW8',-- 华北
								'YW500','YS606','ZD512','ZD562','ZD513','ZD374','YS703','ZD581','YS784','ZD374', -- 华南
								'ZD631','RW207','ZD636', -- 华东
								'ZD114','ZD741','ZD701','ZD122','ZD775','ZD5','ZD674','RW391','ZD114','ZD807','ZD116','ZD157','ZD125','ZD371','ZD572','ZD149','ZD408','ZD438','ZD145','ZD152',
								'ZD430','ZD168','ZD286','ZD222','ZD730','ZD731','ZD120','ZD234','RS20','ZD292','ZD706','ZD557','RW790','RW110','RW810'-- 华西
								) 
		) c1  
		left join 
		(select * 
		from csx_ods.csx_ods_csx_price_prod_market_research_product_df 
		where sdt='${yes_sdt}'  
		) c2 
		on c1.product_id=c2.id 
		left join 
		(select 
			* 
		from csx_dim.csx_dim_shop 
		where sdt='current' 
		) c3 
		on c2.location_code=c3.shop_code 
	where (c3.performance_region_name='华南大区' and c1.shop_name not like '%综合价%') or c3.performance_region_name<>'华南大区' 
	union all 
	-- 生效区客户市调
	select 
		'客户市调' as type,
		c1.location_code as dc_code,
	    c1.market_code as shop_code,
	    c1.market_name as shop_name,
	    cast(c1.price_date as string) as price_date,-- 市调日期
	    c1.goods_code as goods_code,
	    (case when c1.product_status=1 then '促销' else '非促销' end) as product_status,
	    cast(c1.price as decimal(20,6)) as market_research_price 
	from 
	    (select 
	    	* 
	    from csx_dwd.csx_dwd_price_market_customer_research_price_effective_di 
	    where substr(price_end_time,1,10)>='${yes_date}'  
	    and market_code not in ('RW507','RW565','RW789','ZD230','ZD231','YW648','YW8',-- 华北
							'YW500','YS606','ZD512','ZD562','ZD513','ZD374','YS703','ZD581','YS784','ZD374', -- 华南
							'ZD631','RW207','ZD636', -- 华东
							'ZD114','ZD741','ZD701','ZD122','ZD775','ZD5','ZD674','RW391','ZD114','ZD807','ZD116','ZD157','ZD125','ZD371','ZD572','ZD149','ZD408','ZD438','ZD145','ZD152',
							'ZD430','ZD168','ZD286','ZD222','ZD730','ZD731','ZD120','ZD234','RS20','ZD292','ZD706','ZD557','RW790','RW110','RW810'-- 华西

	    						)
	    ) c1 
	    left join 
	    (select 
	    	* 
	    from csx_dim.csx_dim_shop 
	    where sdt='current' 
	    ) c2 
	    on c1.location_code=c2.shop_code 
	where (c2.performance_region_name='华南大区' and c1.market_name not like '%综合价%') or c2.performance_region_name<>'华南大区' 
	-- where t2.price_pm=1 
	union all 
	-- 失效区客户市调
	select 
		'客户市调' as type, 
		d2.location_code as dc_code,
	    d1.market_code as shop_code,
	    d1.market_name as shop_name,
	    cast(d1.price_date as string) as price_date,
	    d2.product_code as goods_code,
	    (case when d1.product_status=1 then '促销' else '非促销' end) as product_status,-- 1促销 0正常
	    cast(d1.price as decimal(20,6)) as market_research_price 
	from 
		(select * 
		from csx_dwd.csx_dwd_price_market_customer_research_price_di 
		where substr((case when status=0 and price_end_time>update_time then update_time else price_end_time end),1,10)>='${yes_date}' 
		and market_code  not in ('RW507','RW565','RW789','ZD230','ZD231','YW648','YW8',-- 华北
							'YW500','YS606','ZD512','ZD562','ZD513','ZD374','YS703','ZD581','YS784','ZD374', -- 华南
							'ZD631','RW207','ZD636', -- 华东
							'ZD114','ZD741','ZD701','ZD122','ZD775','ZD5','ZD674','RW391','ZD114','ZD807','ZD116','ZD157','ZD125','ZD371','ZD572','ZD149','ZD408','ZD438','ZD145','ZD152',
							'ZD430','ZD168','ZD286','ZD222','ZD730','ZD731','ZD120','ZD234','RS20','ZD292','ZD706','ZD557','RW790','RW110','RW810'-- 华西
								) 
		) d1  
		left join 
		(select * 
		from csx_ods.csx_ods_csx_price_prod_market_research_product_df 
		where sdt='${yes_sdt}'  
		) d2 
		on d1.product_id=d2.id 
		left join 
		(select 
			* 
		from csx_dim.csx_dim_shop 
		where sdt='current' 
		) d3 
		on d2.location_code=d3.shop_code 
	where (d3.performance_region_name='华南大区' and d1.market_name not like '%综合价%') or d3.performance_region_name<>'华南大区' 
	) t 
;

-- ---------------------------------------
-- ---近1个月有动销商品
drop table if exists csx_analyse_tmp.csx_analyse_tmp_sale_goods;
create table if not exists csx_analyse_tmp.csx_analyse_tmp_sale_goods as 
select 
	t1.*,
	t2.shop_code,
	t2.shop_name 
from 
	(select 
		a.performance_region_name,
		a.performance_province_name,
		a.performance_city_name,
		a.inventory_dc_code,
		h.first_category_name,
		h.second_category_name,
		h.third_category_name,
		b.classify_large_code,
		b.classify_large_name,
		b.classify_middle_code,
		b.classify_middle_name,
		b.classify_small_code,
		b.classify_small_name,
		a.goods_code,
		b.goods_name,
		b.unit_name,
		c.goods_status_name,
		d.suggest_price_mid,
		d.suggest_price_type_name,
		e.dc_code as last_receive_dc_code,
		e.price as last_receive_price,
		-- f.market_research_price,
		-- f.shop_code,
		-- f.shop_name,
		-- f.type,
		-- f.product_status,
		sum(a.sale_amt)/10000 as sale_amt,
		sum(a.profit)/10000 as profit,
		sum(a.sale_qty) as sale_qty,
		sum(a.profit)/abs(sum(a.sale_amt)) as profitlv,
		sum(a.sale_amt)/sum(a.sale_qty) as sale_price,
		sum(a.sale_amt-a.profit)/sum(a.sale_qty) as cost_price 
	from 
		(select 
			* 
		from csx_dws.csx_dws_sale_detail_di 
		where sdt>=regexp_replace(date_add(from_unixtime(unix_timestamp('${yes_sdt}','yyyyMMdd'),'yyyy-MM-dd'),-29),'-','') 
		and sdt<='${yes_sdt}' 
		and business_type_code=1 
		and order_channel_code not in (4,5,6) 
		and refund_order_flag<>1 
		-- and inventory_dc_code in ('W0A3','W0A8','W0A2','W0A7') 
		) a 
		left join 
		(select * 
		from csx_dim.csx_dim_basic_goods 
		where sdt='current'
		) b 
		on a.goods_code=b.goods_code 
		left join 
		-- 商品状态
		(select * 
		from csx_dim.csx_dim_basic_dc_goods 
		where sdt='current'  
		) c 
		on a.inventory_dc_code=c.dc_code and a.goods_code=c.goods_code 
		-- 目前生效的建议售价
		left join 
		(select 
			d1.* 
		from 
			(select 
				warehouse_code as dc_code,
				product_code as goods_code,
				cast(suggest_price_mid as decimal(20,4)) as suggest_price_mid,
				suggest_price_type,
				(case when suggest_price_type='1' then '目标定价法' 
					  when suggest_price_type='2' then '市调价格' 
					  when suggest_price_type='3' then '手动导入' 
					  when suggest_price_type='4' then '固定价' 
					  when suggest_price_type='5' then '上期价格' 
					  when suggest_price_type='6' then '上期价格人工BOM表' 
					  when suggest_price_type='7' then '目标定价法人工BOM表' 
				end) as suggest_price_type_name,
				row_number()over(partition by warehouse_code,product_code order by create_time desc) as pm  
			from csx_dwd.csx_dwd_price_goods_price_guide_di 
			where regexp_replace(date(price_end_time),'-','')>='${today_sdt}' 
			and regexp_replace(date(price_begin_time),'-','')<='${today_sdt}' 
			and is_expired=0 
			and shipper_code='YHCSX' 
-- 			and warehouse_code in ('W0A3','W0A8','W0A2','W0A7') 
			) d1 
		where pm=1 
		) d 
		on a.inventory_dc_code=d.dc_code and a.goods_code=d.goods_code 
		left join 
		-- 产品最后一次入库时间
		(select 
			d1.* 
		from 
			(select 
				t1.*,
				row_number()over(partition by t1.performance_city_name,t1.goods_code order by t1.create_time desc) as pm
			from 
				(select 
					c.performance_city_name,
					a.target_location_code as dc_code,
					a.goods_code,
					cast(a.price_include_tax as decimal(20,6)) as price,
					substr(a.create_time,1,19) as create_time 
				from 
					(select * 
					from csx_dws.csx_dws_scm_order_detail_di t12      
					where sdt<=regexp_replace(date_add(from_unixtime(unix_timestamp('${yes_sdt}','yyyyMMdd'),'yyyy-MM-dd'),-1),'-','') 
					and sdt>=regexp_replace(date_add(from_unixtime(unix_timestamp('${yes_sdt}','yyyyMMdd'),'yyyy-MM-dd'),-60),'-','')  
					and shipper_code='YHCSX' 
					and super_class in (1,3)  
	    			and source_type in ('1','10','13','19','23','9')
					and header_status=4 
					and items_status=4 
					and price_include_tax>0.1 
					and price_remedy_flag<>1 -- 剔除价格补救单，以防计算成本价错误 
					) a 
					-- 关联价格补救订单数据 
				    left join 
				    (select 
				        * 
				     from csx_dws.csx_dws_scm_order_received_di 
				     where sdt>=regexp_replace(date_add(from_unixtime(unix_timestamp('${yes_date}','yyyyMMdd'),'yyyy-MM-dd'),-80),'-','') 
				     and sdt<='${yes_date}' 
				     and price_remedy_flag=1 
				    ) a2 
				    on a.order_code=a2.original_order_code and a.goods_code=a2.goods_code  
					left join 
					(select * 
					from csx_dim.csx_dim_basic_goods 
					where sdt='current' 
					) b 
					on a.goods_code=b.goods_code 
					left join 
					(select * 
					from csx_dim.csx_dim_shop 
					where sdt='current' 
					) c 
					on a.target_location_code=c.shop_code 
				where 
				(
					(b.classify_large_code in ('B01','B02','B03') and b.classify_middle_code<>'B0101' and date(a.create_time)>=date_add(from_unixtime(unix_timestamp('${yes_sdt}','yyyyMMdd'),'yyyy-MM-dd'),-14) and date(a.create_time)<=date_add(from_unixtime(unix_timestamp('${yes_sdt}','yyyyMMdd'),'yyyy-MM-dd'),-1)) -- 生鲜取近7天最后一次入库数据
					or 
					((b.classify_large_code not in ('B01','B02','B03') or b.classify_middle_code='B0101') and date(a.create_time)>=date_add(from_unixtime(unix_timestamp('${yes_sdt}','yyyyMMdd'),'yyyy-MM-dd'),-60) and date(a.create_time)<=date_add(from_unixtime(unix_timestamp('${yes_sdt}','yyyyMMdd'),'yyyy-MM-dd'),-1))
				) -- 食百取近30天最后一次入库数据 
				and a2.original_order_code is null  -- 剔除价格补救原单 
				and c.warehouse_purpose_name in ('大客户物流','工厂') 
				) t1 
			) d1 
		where d1.pm=1 
		) e  
		on a.performance_city_name=e.performance_city_name and a.goods_code=e.goods_code 
		-- left join 
		-- -- 市调价数据
		-- (select * 
		-- from csx_analyse_tmp.csx_analyse_tmp_market_research_price 
		-- where final_pm=1 
		-- ) f 
		-- on a.inventory_dc_code=f.dc_code and a.goods_code=f.goods_code 
		left join 
		(select
		    code as type,
		    max(name) as name,
		    max(extra) as extra 
		from csx_dim.csx_dim_basic_topic_dict_df
		where parent_code = 'direct_delivery_type' 
		group by code 
		) g 
		on cast(a.direct_delivery_type as string)=cast(g.type as string) 
		left join 
		(select 
			* 
		from csx_dim.csx_dim_crm_customer_info 
		where sdt='current' 
		) h 
		on a.customer_code=h.customer_code 
	where g.extra='采购参与' and c.goods_status_name like 'B%'
	group by 
		a.performance_region_name,
		a.performance_province_name,
		a.performance_city_name,
		a.inventory_dc_code,
		h.first_category_name,
		h.second_category_name,
		h.third_category_name,
		b.classify_large_code,
		b.classify_large_name,
		b.classify_middle_code,
		b.classify_middle_name,
		b.classify_small_code,
		b.classify_small_name,
		a.goods_code,
		b.goods_name,
		b.unit_name,
		c.goods_status_name,
		d.suggest_price_mid,
		d.suggest_price_type_name,
		e.dc_code,
		e.price 
-- 		f.market_research_price,
-- 		f.shop_code,
-- 		f.shop_name,
-- 		f.type,
-- 		f.product_status 
	) t1 
	cross join 
	(select 
		dc_code,
		shop_code,
		shop_name 
	from csx_analyse_tmp.csx_analyse_tmp_market_research_price 
	where pm=1 
	group by 
		dc_code,
		shop_code,
		shop_name 
	) t2 
where t1.inventory_dc_code=t2.dc_code 
;

-- ---------------------------------------
-- ---最终数据表
drop table if exists csx_analyse_tmp.csx_analyse_tmp_final_table_market_cost;
create table if not exists csx_analyse_tmp.csx_analyse_tmp_final_table_market_cost as 
select 
    	a1.performance_region_name,
    	a1.performance_province_name,
    	a1.performance_city_name,
    	a1.inventory_dc_code,
    	a1.first_category_name,
    	a1.second_category_name,
    	a1.third_category_name,
    	a1.classify_large_code,
    	a1.classify_large_name,
    	a1.classify_middle_code,
    	a1.classify_middle_name,
    	a1.classify_small_code,
    	a1.classify_small_name,
    	a1.goods_code,
    	a1.goods_name,
    	a1.unit_name,
    	a1.goods_status_name,
    	a1.suggest_price_mid,
    	a1.suggest_price_type_name,
    	a1.last_receive_dc_code,
    	a1.last_receive_price,
    	a1.sale_amt,
    	a1.profit,
    	a1.sale_qty,
    	a1.profitlv,
    	a1.sale_price,
    	a1.cost_price,
    	a1.shop_code,
    	a1.shop_name,
    	(case when a2.shop_code is not null and a1.last_receive_price>0 then '对标' else '非对标' end) as if_db,
    	a2.market_research_price,
    	a2.type as shop_type,
    	a2.product_status,
    	current_date() as update_date  
    from 
    	csx_analyse_tmp.csx_analyse_tmp_sale_goods a1 
    	left join 
    	-- 市调价数据
    	(select * 
    	from csx_analyse_tmp.csx_analyse_tmp_market_research_price 
    	where pm=1 
    	) a2 
    	on a1.inventory_dc_code=a2.dc_code and a1.shop_code=a2.shop_code and a1.goods_code=a2.goods_code 
;

-- ---------------------------------------
-- ---最终数据表汇总数据表
drop table if exists csx_analyse_tmp.csx_analyse_tmp_final_table_market_cost_sum;
create table if not exists csx_analyse_tmp.csx_analyse_tmp_final_table_market_cost_sum as 
select 
    t1.*,
    row_number()over(partition by t1.performance_region_name,t1.performance_province_name,t1.performance_city_name,t1.inventory_dc_code order by t1.db_shop_sale_amt desc) as pm 
from 
    (select 
        performance_region_name,
        performance_province_name,
        performance_city_name,
        inventory_dc_code,
        shop_code,
        sum(sale_amt) as shop_sale_amt,
        sum(case when if_db='对标' then sale_amt end) as db_shop_sale_amt 
    from csx_analyse_tmp.csx_analyse_tmp_final_table_market_cost 
    group by 
        performance_region_name,
        performance_province_name,
        performance_city_name,
        inventory_dc_code,
        shop_code 
    ) t1 
;


insert overwrite table csx_analyse.csx_analyse_market_price_cost_profit_df  
select 
	a1.performance_region_name,
	a1.performance_province_name,
	a1.performance_city_name,
	a1.inventory_dc_code,
	a1.first_category_name,
	a1.second_category_name,
	a1.third_category_name,
	a1.classify_large_code,
	a1.classify_large_name,
	a1.classify_middle_code,
	a1.classify_middle_name,
	a1.classify_small_code,
	a1.classify_small_name,
	a1.goods_code,
	a1.goods_name,
	a1.unit_name,
	a1.goods_status_name,
	a1.suggest_price_mid,
	a1.suggest_price_type_name,
	a1.last_receive_dc_code,
	a1.last_receive_price,
	a1.sale_amt,
	a1.profit,
	a1.sale_qty,
	a1.profitlv,
	a1.sale_price,
	a1.cost_price,
	a1.shop_code,
	a1.shop_name,
	a1.if_db,
	a1.market_research_price,
	a1.shop_type,
	a1.product_status,
	a1.update_date,
	
	a2.pm as db_pm,
	d2.db_shop_sale_amt as dc_shop_all_sale_amt
from 
	csx_analyse_tmp.csx_analyse_tmp_final_table_market_cost a1 
	left join 
	-- 市调价数据
	csx_analyse_tmp.csx_analyse_tmp_final_table_market_cost_sum a2 
	on a1.performance_region_name=a2.performance_region_name and a1.performance_province_name=a2.performance_province_name and a1.performance_city_name=a2.performance_city_name 
	and a1.inventory_dc_code=a2.inventory_dc_code and a1.shop_code=a2.shop_code  
;





select 
	performance_region_name as `大区`,
	performance_province_name as `省区`,
	performance_city_name as `城市`,
	inventory_dc_code as `仓`,
	first_category_name as `一级客户分类`,
	second_category_name as `二级客户分类`,
	classify_large_name as `管理大类名称`,
	classify_middle_name as `管理中类名称`,
	shop_code as `对标地点编码`,
	shop_name as `对标地点名称`,
	(case when shop_code='RW494' then '京东' 
		  when shop_code like 'RW%' then '二批网站' 
		  when shop_code like 'R%' then '二批' 
		  when shop_code like 'YW%' then '一批网站' 
		  when shop_code like 'Y%' then '一批' 
		  when shop_code like 'ZD%' then '终端' 
	end) as `对标地点类型`, 
	db_pm as `对标占比排名`,
	if_db as `是否对标`,
	dc_shop_all_sale_amt/10000 as `对标总销售额（万）`,
	nvl(sum(sale_amt)/10000,0) as `销售额（万）`,
	sum(case when market_research_price>0 and last_receive_price>0 then sale_qty end) as `销量`,
	sum(case when market_research_price>0 and last_receive_price>0 then sale_qty*last_receive_price end)/sum(case when market_research_price>0 and last_receive_price>0 then sale_qty end) as `最近一次入库价`,
	sum(case when market_research_price>0 and last_receive_price>0 then sale_qty*market_research_price end)/sum(case when market_research_price>0 and last_receive_price>0 then sale_qty end) as `市调价`  
from csx_analyse.csx_analyse_market_price_cost_profit_df 
group by 
	performance_region_name,
	performance_province_name,
	performance_city_name,
	inventory_dc_code,
	first_category_name,
	second_category_name,
	classify_large_name,
	classify_middle_name,
	shop_code,
	shop_name,
	(case when shop_code='RW494' then '京东' 
		  when shop_code like 'RW%' then '二批网站' 
		  when shop_code like 'R%' then '二批' 
		  when shop_code like 'YW%' then '一批网站' 
		  when shop_code like 'Y%' then '一批' 
		  when shop_code like 'ZD%' then '终端' 
	end), 
	db_pm,
	if_db,
	dc_shop_all_sale_amt/10000
;