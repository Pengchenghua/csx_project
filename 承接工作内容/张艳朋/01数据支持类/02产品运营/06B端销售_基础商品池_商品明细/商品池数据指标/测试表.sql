
	
-- drop table if exists csx_analyse_tmp.csx_analyse_tmp_fr_product_pool_indicator_mi;
create table csx_analyse_tmp.csx_analyse_tmp_fr_product_pool_indicator_mi
as

with tmp_sale_detail as 
(
select
	sdt,inventory_dc_code,customer_code,goods_code,
	sum(sale_amt) as sale_amt,
	sum(profit) as profit
from
	csx_dws.csx_dws_sale_detail_di
where
	sdt>='20220101'
	and sdt<='20221117'
	and channel_code in ('1','7','9')
	and business_type_code in (1)
	and order_channel_code !=4
group by 
	sdt,inventory_dc_code,customer_code,goods_code
),
tmp_dc_product_pool as 
(
select
	inventory_dc_code,product_code,base_product_tag,base_product_status
from
	csx_ods.csx_ods_b2b_mall_prod_yszx_dc_product_pool_df
where
	sdt='20221117'
)
	

-- insert overwrite table csx_analyse.csx_analyse_fr_product_pool_indicator_mi partition(sdt)

select
	h.performance_region_code, -- 大区编码
	h.performance_region_name, -- 大区名称
	h.performance_province_code, -- 省份编码
	h.performance_province_name, -- 省份名称
	h.performance_city_code, -- 城市编码
	h.performance_city_name, -- 城市名称
	a.inventory_dc_code, -- dc编码
	coalesce(a.create_date,'-') as create_date, -- 换品开始时间
	coalesce(a.base_goods_cnt,0) as base_goods_cnt, -- 基础商品池总数
	coalesce(a.base_purchase_price_goods_cnt,0) as base_purchase_price_goods_cnt, -- 有采购报价的基础商品数
	coalesce(a.base_guide_price_goods_cnt,0) as base_guide_price_goods_cnt, -- 有建议售价的基础商品数
	coalesce(a.base_sale_goods_cnt,0) as base_sale_goods_cnt, -- 有动销的基础商品数
	coalesce(a.base_goods_sale_amt,0) as base_goods_sale_amt, -- 有动销的基础商品日配销售额
	coalesce(b.dc_sale_amt,0) as dc_sale_amt, -- dc日配销售额
	coalesce(c.bind_common_product_customer_cnt,0) as bind_common_product_customer_cnt, -- 落地客户数 绑定基础商品池的客户数
	coalesce(c.on_rule_customer_cnt,0) as on_rule_customer_cnt, -- 配置开启的客户数
	coalesce(c.need_on_rule_customer_cnt,0) as need_on_rule_customer_cnt, -- 需开启总数
	coalesce(c.bind_common_product_customer_sale_amt,0) as bind_common_product_customer_sale_amt, -- 落地客户日配销售额
	coalesce(d.category_cnt,0) as category_cnt, -- 品类总数
	coalesce(d.config_th_rule_category_cnt,0) as config_th_rule_category_cnt, -- 配置汰换规则的品类数
	coalesce(e.base_dth_product_cnt,0) as base_dth_product_cnt,-- 待替换商品为基础商品的数量
	coalesce(e.no_base_dth_product_cnt,0) as no_base_dth_product_cnt, -- 待替换商品不是基础商品的数量
	coalesce(e.need_th_product_cnt,0) as need_th_product_cnt, -- 需换品商品数
	coalesce(e.pending_product_cnt,0) as pending_product_cnt, -- 待处理商品数
	coalesce(e.finish_product_cnt,0) as finish_product_cnt, -- 完成商品数
	coalesce(e.refuse_product_cnt,0) as refuse_product_cnt, -- 拒绝商品数	
	coalesce(f.total_sale_amt,0) as total_sale_amt, -- 总销售额
	coalesce(f.finish_sale_amt,0) as finish_sale_amt, -- 完成商品销售额
	coalesce(f.refuse_sale_amt,0) as refuse_sale_amt, -- 拒绝商品销售额	
	coalesce(g.xd_goods_cnt,0) as xd_goods_cnt, -- 通过下单添加至客户商品池的商品 商品总数
	coalesce(g.base_xd_goods_cnt,0) as base_xd_goods_cnt, -- 基础商品数
	coalesce(f.increase_profit,0) as increase_profit, -- 毛利额增量
	'20221117' as sdt
from
	(
	select
		a.inventory_dc_code, -- dc编码
		e.create_date, -- 换品开始时间
		count(a.product_code) as base_goods_cnt, -- 基础商品池总数
		count(b.product_code) as base_purchase_price_goods_cnt, -- 有采购报价的基础商品数
		count(c.product_code) as base_guide_price_goods_cnt, -- 有建议售价的基础商品数
		count(case when sale_amt>0 then d.goods_code else null end) as base_sale_goods_cnt, -- 有动销的基础商品数
		sum(sale_amt) as base_goods_sale_amt -- 有动销的基础商品日配销售额
	from
		( -- 基础商品
		select
			inventory_dc_code,product_code 
		from
			tmp_dc_product_pool
		where
			base_product_tag=1
			and base_product_status in (0,7)
		group by 
			inventory_dc_code,product_code 
		) a 
		left join -- 有采购报价的基础商品
			(
			select 
				warehouse_code,product_code,purchase_price
			from 
				(
				select 
					*,row_number()over(partition by warehouse_code,product_code order by last_put_time) as pm 
				from 
					csx_ods.csx_ods_csx_price_prod_effective_purchase_prices_df  
				where 
					sdt='20221117' 
					and regexp_replace(to_date(price_begin_time), '-', '')<='20221117' 
					and regexp_replace(to_date(price_end_time), '-', '')>='20221117' 
					and effective='true'
				) a 
			where
				a.pm=1
			group by 
				warehouse_code,product_code,purchase_price
			) b on b.warehouse_code=a.inventory_dc_code and b.product_code=a.product_code
		left join -- 有建议售价的基础商品
			(	
			select 
				warehouse_code,product_code 
			from 
				csx_ods.csx_ods_csx_price_prod_goods_price_guide_df 
			where 
				sdt='20221117'
				and regexp_replace(to_date(price_begin_time),'-','')<='20221117' 
				and regexp_replace(to_date(price_end_time),'-','')>='20221117' 
				and is_expired='false' 
			group by 
				warehouse_code,product_code
			) c on c.warehouse_code=a.inventory_dc_code and c.product_code=a.product_code
		left join
			(
			select
				inventory_dc_code,goods_code,sum(sale_amt) as sale_amt
			from
				tmp_sale_detail
			where
				sdt>=concat(substr('20221117',1,6),'01')
				and sdt<='20221117'
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
					a.inventory_dc_code,to_date(b.create_time) as create_date,count(a.id),row_number()over(partition by a.inventory_dc_code order by count(a.id) desc) as rn
				from
					(
					select
						id,config_id,inventory_dc_code,main_product_code,sap_cus_code,status,create_time
					from
						csx_ods.csx_ods_b2b_mall_prod_yszx_change_product_task_df
					where
						sdt='20221117'
					) a 
					join
						(
						select
							id,inventory_dc_code,main_product_code,update_time,create_time
						from
							csx_ods.csx_ods_b2b_mall_prod_yszx_change_product_config_df -- 换品配置表
						where
							sdt='20221117'
						) b on b.id=a.config_id
				group by 
					a.inventory_dc_code,to_date(b.create_time)
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
			tmp_sale_detail
		where
			sdt>=concat(substr('20221117',1,6),'01')
			and sdt<='20221117'			
		group by 
			inventory_dc_code
		) b on b.inventory_dc_code=a.inventory_dc_code
	left join
		(
		select
			b.dc_code,
			count(case when b.customer_flag in ('Ⅰ类客户','Ⅱ类客户') then b.customer_code else null end) as bind_common_product_customer_cnt, -- 落地客户数 绑定基础商品池的客户数
			sum(case when b.customer_flag in ('Ⅰ类客户','Ⅱ类客户') then (coalesce(a.bind_common_product_flag,0)+coalesce(a.create_order_auto_add_flag,1)
				+coalesce(a.price_auto_add_flag,1)+coalesce(a.remove_customer_product_flag,0))/4 else 0 end) as on_rule_customer_cnt, -- 配置开启的客户数		
			count(case when b.customer_flag in ('Ⅰ类客户','Ⅱ类客户') then b.customer_code else null end) as need_on_rule_customer_cnt, -- 需开启总数
			sum(case when b.customer_flag in ('Ⅰ类客户','Ⅱ类客户') then c.sale_amt else 0 end) as bind_common_product_customer_sale_amt -- 落地客户日配销售额
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
				sdt='20221117'
			) a 
			right join
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
					tmp_sale_detail
				where
					sdt>=concat(substr('20221117',1,6),'01')
					and sdt<='20221117'
				group by 
					inventory_dc_code,customer_code
				) c on c.customer_code=b.customer_code and c.inventory_dc_code=b.dc_code
		group by 
			b.dc_code
		) c on c.dc_code=a.inventory_dc_code
	left join -- 配置汰换规则的品类与品类总数
		(		
		select
			a.dc_code,
			count(a.big_category_code) as category_cnt, -- 品类总数
			count(case when a.status=1 then a.big_category_code else null end) as config_th_rule_category_cnt -- 配置汰换规则的品类数
		from
			(
			select
				dc_code,dc_name,big_category_code,status
			from
				csx_ods.csx_ods_b2b_mall_prod_yszx_cus_product_remove_rule_df
			where
				sdt='20221117'
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
			count(distinct case when b.base_product_tag=1 then a.main_product_code else null end) as base_dth_product_cnt,-- 待替换商品为基础商品的数量
			count(distinct case when b.base_product_tag !=1 or b.base_product_tag is null then a.main_product_code else null end) as no_base_dth_product_cnt, -- 待替换商品不是基础商品的数量
			count(a.main_product_code) as need_th_product_cnt, -- 需换品商品数
			count(case when a.status='1' then a.main_product_code else null end) as pending_product_cnt, -- 待处理商品数
			count(case when a.status='2' then a.main_product_code else null end) as finish_product_cnt, -- 完成商品数
			count(case when a.status='3' then a.main_product_code else null end) as refuse_product_cnt -- 拒绝商品数
		from
			(
			select
				a.inventory_dc_code,a.main_product_code,a.sap_cus_code,a.status
			from
				(
				select
					config_id,inventory_dc_code,main_product_code,sap_cus_code,status
				from
					csx_ods.csx_ods_b2b_mall_prod_yszx_change_product_task_df
				where
					sdt='20221117'
				) a 
				join
					(
					select
						id,inventory_dc_code,main_product_code,update_time,create_time
					from
						csx_ods.csx_ods_b2b_mall_prod_yszx_change_product_config_df
					where
						sdt='20221117'
					) b on b.id=a.config_id		
			) a 
			left join
				(
				select
					inventory_dc_code,product_code,base_product_tag
				from
					tmp_dc_product_pool							
				group by 
					inventory_dc_code,product_code,base_product_tag
				) b on b.inventory_dc_code=a.inventory_dc_code and b.product_code=a.main_product_code
		group by 
			a.inventory_dc_code
		) e on e.inventory_dc_code=a.inventory_dc_code
	left join
		(
		select
			a.inventory_dc_code,
			sum(case when release_time !='' then a.sale_amt else 0 end) as total_sale_amt, -- 总销售额
			sum(case when release_time !='' and a.status=2 then a.sale_amt else 0 end ) as finish_sale_amt, -- 完成商品销售额
			sum(case when release_time !='' and a.status=3 then a.sale_amt else 0 end ) as refuse_sale_amt, -- 拒绝商品销售额
			sum(increase_profit) as increase_profit
		from
			(
			select
				a.inventory_dc_code,a.inventory_dc_name,a.main_product_code,a.main_product_name,a.unit,a.sales_amount,a.profit_margin,a.sap_cus_code,a.sap_cus_name,a.status,a.status_name,a.operator_by,
				a.change_product_code,a.change_product_name,a.change_unit,
				a.release_time, -- 操作时间 发布时间
				a.create_time, -- 映射到客户的时间
				a.update_time, -- 完成时间
				a.sale_amt, -- 往前追一个月 如没有则再往前追一个月 如没有再往前追一个月 还没有就是0
				a.profit, -- 往前追一个月 如没有则再往前追一个月 如没有再往前追一个月 还没有就是0
				coalesce(a.profit_rate,0) as profit_rate,
				coalesce(b.finish_after_sale_amt,0) as finish_after_sale_amt, -- 完成后销售额
				coalesce(b.finish_after_profit,0) as finish_after_profit, -- 完成后毛利额
				coalesce(b.finish_after_profit,0)-coalesce(a.profit_rate,0)*coalesce(b.finish_after_sale_amt,0) as increase_profit
			from
				(
				select
					a.id,a.inventory_dc_code,a.inventory_dc_name,a.main_product_code,a.main_product_name,a.unit,a.sales_amount,a.profit_margin,a.sap_cus_code,a.sap_cus_name,a.status,
					a.update_by,a.change_product_code,a.change_product_name,a.change_unit,a.create_time,a.update_time,
					coalesce(a.month_ago_1_sale_amt,a.month_ago_2_sale_amt,a.month_ago_3_sale_amt,0) as sale_amt,
					coalesce(a.month_ago_1_profit,a.month_ago_2_profit,a.month_ago_3_profit,0) as profit,
					coalesce(a.month_ago_1_profit,a.month_ago_2_profit,a.month_ago_3_profit,0)/abs(coalesce(a.month_ago_1_sale_amt,a.month_ago_2_sale_amt,a.month_ago_3_sale_amt,0)) as profit_rate,
					case status when 1 then '待处理' when 2 then '已完成' when 3 then '已拒绝' end as status_name,
					coalesce(b.create_time,'') as release_time, -- 操作时间 发布时间
					if(a.status=1,coalesce(c.rp_service_user_name_new,c.fl_service_user_name_new,c.bbc_service_user_name_new,c.sales_name_new),a.update_by) as operator_by -- 操作人
				from
					(
					select
						a.id,a.config_id,a.inventory_dc_code,a.inventory_dc_name,a.main_product_code,a.main_product_name,a.unit,a.sales_amount,a.profit_margin,a.sap_cus_code,a.sap_cus_name,a.status,
						a.update_by,a.change_product_code,a.change_product_name,a.change_unit,a.create_time,a.update_time,a.create_date,a.month_ago_1,a.month_ago_2,a.month_ago_3,
						sum(case when b.sdt>=a.month_ago_1 and b.sdt<=a.create_date then b.sale_amt else null end) as month_ago_1_sale_amt,
						sum(case when b.sdt>=a.month_ago_2 and b.sdt<=a.create_date then b.sale_amt else null end) as month_ago_2_sale_amt,
						sum(case when b.sdt>=a.month_ago_3 and b.sdt<=a.create_date then b.sale_amt else null end) as month_ago_3_sale_amt,
						
						sum(case when b.sdt>=a.month_ago_1 and b.sdt<=a.create_date then b.profit else null end) as month_ago_1_profit,
						sum(case when b.sdt>=a.month_ago_2 and b.sdt<=a.create_date then b.profit else null end) as month_ago_2_profit,
						sum(case when b.sdt>=a.month_ago_3 and b.sdt<=a.create_date then b.profit else null end) as month_ago_3_profit
					from	
						(
						select
							id,config_id,inventory_dc_code,inventory_dc_name,main_product_code,main_product_name,unit,sales_amount,profit_margin,sap_cus_code,sap_cus_name,status,
							update_by,change_product_code,change_product_name,change_unit,create_time,update_time,
							regexp_replace(to_date(create_time),'-','') as create_date,
							regexp_replace(add_months(to_date(create_time),-1),'-','') as month_ago_1,
							regexp_replace(add_months(to_date(create_time),-2),'-','') as month_ago_2,
							regexp_replace(add_months(to_date(create_time),-3),'-','') as month_ago_3
							-- row_number()over(partition by inventory_dc_code,main_product_code,sap_cus_code order by update_time desc) as rn
						from
							csx_ods.csx_ods_b2b_mall_prod_yszx_change_product_task_df
						where
							sdt='20221117'
						) a 
						left join
							(
							select
								sdt,inventory_dc_code,customer_code,goods_code,sum(sale_amt) as sale_amt,sum(profit) as profit
							from
								tmp_sale_detail
							group by 
								sdt,inventory_dc_code,customer_code,goods_code
							) b on b.inventory_dc_code=a.inventory_dc_code and b.customer_code=a.sap_cus_code and b.goods_code=a.main_product_code
					group by 
						a.id,a.config_id,a.inventory_dc_code,a.inventory_dc_name,a.main_product_code,a.main_product_name,a.unit,a.sales_amount,a.profit_margin,a.sap_cus_code,a.sap_cus_name,a.status,
						a.update_by,a.change_product_code,a.change_product_name,a.change_unit,a.create_time,a.update_time,a.create_date,a.month_ago_1,a.month_ago_2,a.month_ago_3	
					) a 
					left join
						(
						select
							id,inventory_dc_code,main_product_code,update_time,create_time
						from
							csx_ods.csx_ods_b2b_mall_prod_yszx_change_product_config_df
						where
							sdt='20221117'
						) b on b.id=a.config_id
					left join
						(
						select
							customer_no,work_no_new,sales_name_new,
							rp_service_user_work_no_new,rp_service_user_name_new,
							fl_service_user_work_no_new,fl_service_user_name_new,
							bbc_service_user_work_no_new,bbc_service_user_name_new
						from
							csx_analyse.csx_analyse_report_crm_customer_sale_service_manager_info_df
						where
							sdt='20221117'
						) c on c.customer_no=a.sap_cus_code
				) a 
				left join
					(
					select
						a.id,a.inventory_dc_code,a.main_product_code,a.sap_cus_code,a.status,
						a.change_product_code,a.create_time,a.update_time,					
						sum(case when b.sdt>=a.finish_date then b.sale_amt else 0 end) as finish_after_sale_amt,
						sum(case when b.sdt>=a.finish_date then b.profit else 0 end) as finish_after_profit
					from
						(
						select
							id,inventory_dc_code,main_product_code,sap_cus_code,status,
							change_product_code,create_time,update_time,
							regexp_replace(to_date(update_time),'-','') as finish_date,
							row_number()over(partition by inventory_dc_code,change_product_code,sap_cus_code order by update_time desc) as rn
						from
							csx_ods.csx_ods_b2b_mall_prod_yszx_change_product_task_df
						where
							sdt='20221117'	
							and status=2 -- 已完成
						) a 
						left join
							(
							select
								sdt,inventory_dc_code,customer_code,goods_code,sum(sale_amt) as sale_amt,sum(profit) as profit
							from
								tmp_sale_detail
							group by 
								sdt,inventory_dc_code,customer_code,goods_code
							) b on b.inventory_dc_code=a.inventory_dc_code and b.customer_code=a.sap_cus_code and b.goods_code=a.change_product_code
					where
						-- 1=1
						rn=1
					group by 
						a.id,a.inventory_dc_code,a.main_product_code,a.sap_cus_code,a.status,
						a.change_product_code,a.create_time,a.update_time				
					) b on b.id=a.id
			) a 
		group by 
			a.inventory_dc_code
		) f on f.inventory_dc_code=a.inventory_dc_code
	left join -- 通过下单添加至客户商品池的商品
		(
		select
			a.inventory_dc_code,
			count(a.product_code) as xd_goods_cnt, -- 通过下单添加至客户商品池的商品 商品总数
			count(b.product_code) as base_xd_goods_cnt -- 基础商品数
		from
			(
			select
				customer_code,inventory_dc_code,product_code,data_source
			from
				csx_ods.csx_ods_b2b_mall_prod_yszx_customer_product_df
			where
				data_source=1 -- 数据来源：0-手动添加 1-客户订单 2-报价 3-商品池模板 4-必售商品 5-商品池模板替换 6-新品 7-基础商品池 8-CRM换品 9-销售添加
				and regexp_replace(to_date(updated_time),'-','') between concat(substr('20221117',1,6),'01') and '20221117'
			)a 
			left join
				(
				select
					inventory_dc_code,product_code 
				from
					tmp_dc_product_pool
				where
					base_product_tag=1 -- 基础商品
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
;