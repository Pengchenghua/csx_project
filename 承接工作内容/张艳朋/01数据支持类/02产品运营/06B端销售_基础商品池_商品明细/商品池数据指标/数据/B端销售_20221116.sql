-- B端销售_基础商品池_商品明细
	select
		a.inventory_dc_code,
		a.product_code,
		b.goods_name,
		b.unit_name,
		a.base_product_status,
		case a.base_product_status
			when 0 then '正常'
			when 3 then '停售'
			when 6 then '退场'
			when 7 then '停购'
		end as base_product_status_name,
		a.sync_customer_product_flag,
		case a.sync_customer_product_flag
			when 0 then '否' when 1 then '是'
		end as sync_customer_product_flag_name,
		a.create_time,
		a.created_by
	from
		(
		select
			inventory_dc_code,product_code,base_product_status,sync_customer_product_flag,create_time,created_by
		from
			csx_ods.csx_ods_b2b_mall_prod_yszx_dc_product_pool_df
		where
			sdt='20221116'
			and base_product_tag=1
		) a 
		left join 
			(
			select
				goods_code,goods_name,unit_name
			from
				csx_dim.csx_dim_basic_goods
			where
				sdt='current'
			) b on b.goods_code=a.product_code
;

-- B端销售_客户商品池_规则设置
select
	a.customer_code,
	coalesce(b.customer_name,'') as customer_name,
	-- a.price_auto_add_flag,
	case a.price_auto_add_flag when 0 then '未启动' when 1 then '已启动' end as price_auto_add_flag_name,
	-- a.create_order_auto_add_flag,
	case a.create_order_auto_add_flag when 0 then '未启动' when 1 then '已启动' end as create_order_auto_add_flag_name,
	-- a.bind_common_product_flag,
	case a.bind_common_product_flag when 0 then '否' when 1 then '是' end as bind_common_product_flag_name,
	-- a.lock_customer_product_flag,
	case a.lock_customer_product_flag when 0 then '未锁定' when 1 then '已锁定' end as lock_customer_product_flag_name,
	-- a.lock_mall_product_flag,
	case a.lock_mall_product_flag when 0 then '未锁定' when 1 then '已锁定' end lock_mall_product_flag_name,
	-- a.remove_customer_product_flag,
	case a.remove_customer_product_flag when 0 then '关闭' when 1 then '开启' end as remove_customer_product_flag_name,
	a.create_by,
	a.create_time
from
	(
	select
		customer_code,price_auto_add_flag,create_order_auto_add_flag,bind_common_product_flag,lock_customer_product_flag,lock_mall_product_flag,remove_customer_product_flag,create_by,create_time
	from
		csx_ods.csx_ods_b2b_mall_prod_yszx_cus_product_rule_df
	where
		sdt='20221109'
	) a 
	left join 
		(
		select 
			customer_code,customer_name 
		from 
			csx_dim.csx_dim_crm_customer_info
		where
			sdt='current'
		) b on b.customer_code=a.customer_code
;

-- 销售明细
select
	sdt,inventory_dc_code,customer_code,goods_code,
	sum(sale_amt) as sale_amt,
	sum(profit) as profit
from
	csx_dws.csx_dws_sale_detail_di
where
	sdt>='20220901'
	and sdt<='20221116'
	and channel_code in ('1','7','9')
	and business_type_code in (1)
	and order_channel_code !=4
	and inventory_dc_code in ('W0A3','W0P8','W0Q9','W0A2','W0BR','W0BH','W0N0','W0AS','W0A5','W0R9','W0A7','W0A6','W0Q2','W0A8','W0BK')
group by 
	sdt,inventory_dc_code,customer_code,goods_code
;

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
	data_source=1 -- 数据来源：0-手动添加 1-客户订单 2-报价 3-商品池模板 4-必售商品 5-商品池模板替换 6-新品 7-基础商品池 8-CRM换品 9-销售添加
	and regexp_replace(to_date(updated_time),'-','') between concat(substr('20221109',1,6),'01') and '20221116'
;
-- B端销售_换品配置

select
	a.inventory_dc_code,a.inventory_dc_name,a.main_product_code,a.main_product_name,a.unit,a.sales_amount,a.profit_margin,a.sap_cus_code,a.sap_cus_name,a.status_name,a.operator_by,
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
			a.id,a.inventory_dc_code,a.inventory_dc_name,a.main_product_code,a.main_product_name,a.unit,a.sales_amount,a.profit_margin,a.sap_cus_code,a.sap_cus_name,a.status,
			a.update_by,a.change_product_code,a.change_product_name,a.change_unit,a.create_time,a.update_time,a.create_date,a.month_ago_1,a.month_ago_2,a.month_ago_3,
			sum(case when rn=1 and b.sdt>=a.month_ago_1 and b.sdt<=a.create_date then b.sale_amt else null end) as month_ago_1_sale_amt,
			sum(case when rn=1 and b.sdt>=a.month_ago_2 and b.sdt<=a.create_date then b.sale_amt else null end) as month_ago_2_sale_amt,
			sum(case when rn=1 and b.sdt>=a.month_ago_3 and b.sdt<=a.create_date then b.sale_amt else null end) as month_ago_3_sale_amt,
			
			sum(case when rn=1 and b.sdt>=a.month_ago_1 and b.sdt<=a.create_date then b.profit else null end) as month_ago_1_profit,
			sum(case when rn=1 and b.sdt>=a.month_ago_2 and b.sdt<=a.create_date then b.profit else null end) as month_ago_2_profit,
			sum(case when rn=1 and b.sdt>=a.month_ago_3 and b.sdt<=a.create_date then b.profit else null end) as month_ago_3_profit
		from	
			(
			select
				id,inventory_dc_code,inventory_dc_name,main_product_code,main_product_name,unit,sales_amount,profit_margin,sap_cus_code,sap_cus_name,status,
				update_by,change_product_code,change_product_name,change_unit,create_time,update_time,
				regexp_replace(to_date(create_time),'-','') as create_date,
				regexp_replace(add_months(to_date(create_time),-1),'-','') as month_ago_1,
				regexp_replace(add_months(to_date(create_time),-2),'-','') as month_ago_2,
				regexp_replace(add_months(to_date(create_time),-3),'-','') as month_ago_3,
				row_number()over(partition by inventory_dc_code,main_product_code,sap_cus_code order by update_time desc) as rn
			from
				csx_ods.csx_ods_b2b_mall_prod_yszx_change_product_task_df
			where
				sdt='20221116'
			) a 
			left join
				(
				select
					sdt,inventory_dc_code,customer_code,goods_code,sum(sale_amt) as sale_amt,sum(profit) as profit
				from
					csx_dws.csx_dws_sale_detail_di
				where
					sdt>=(select regexp_replace(add_months(to_date(min(create_time)),-3),'-','') from csx_ods.csx_ods_b2b_mall_prod_yszx_change_product_task_df where sdt='20221116')
					and sdt<='20221116'
					and channel_code in ('1','7','9')
					and business_type_code in (1)
					and order_channel_code !=4
				group by 
					sdt,inventory_dc_code,customer_code,goods_code
				) b on b.inventory_dc_code=a.inventory_dc_code and b.customer_code=a.sap_cus_code and b.goods_code=a.main_product_code
		group by 
			a.id,a.inventory_dc_code,a.inventory_dc_name,a.main_product_code,a.main_product_name,a.unit,a.sales_amount,a.profit_margin,a.sap_cus_code,a.sap_cus_name,a.status,
			a.update_by,a.change_product_code,a.change_product_name,a.change_unit,a.create_time,a.update_time,a.create_date,a.month_ago_1,a.month_ago_2,a.month_ago_3	
		) a 
		left join
			(
			select
				inventory_dc_code,main_product_code,update_time,create_time
			from
				csx_ods.csx_ods_b2b_mall_prod_yszx_change_product_config_df
			where
				sdt='20221116'
			group by 
				inventory_dc_code,main_product_code,update_time,create_time
			) b on b.inventory_dc_code=a.inventory_dc_code and b.main_product_code=a.main_product_code
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
				sdt='20221116'
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
				row_number()over(partition by inventory_dc_code,main_product_code,sap_cus_code order by update_time desc) as rn
			from
				csx_ods.csx_ods_b2b_mall_prod_yszx_change_product_task_df
			where
				sdt='20221116'	
				and status=2 -- 已完成
			) a 
			left join
				(
				select
					sdt,inventory_dc_code,customer_code,goods_code,sum(sale_amt) as sale_amt,sum(profit) as profit
				from
					csx_dws.csx_dws_sale_detail_di
				where
					sdt>=(select regexp_replace(add_months(to_date(min(create_time)),-3),'-','') from csx_ods.csx_ods_b2b_mall_prod_yszx_change_product_task_df where sdt='20221116')
					and sdt<='20221116'
					and channel_code in ('1','7','9')
					and business_type_code in (1)
					and order_channel_code !=4
				group by 
					sdt,inventory_dc_code,customer_code,goods_code
				) b on b.inventory_dc_code=a.inventory_dc_code and b.customer_code=a.sap_cus_code and b.goods_code=a.change_product_code
		where
			rn=1
		group by 
			a.id,a.inventory_dc_code,a.main_product_code,a.sap_cus_code,a.status,
			a.change_product_code,a.create_time,a.update_time				
		) b on b.id=a.id
;

-- 验数
	select
		sdt,sum(sale_amt) as sale_amt,sum(profit) as profit
	from
		csx_dws.csx_dws_sale_detail_di
	where
		sdt>='20221022'
		and sdt<='20221116'
		and channel_code in ('1','7','9')
		and business_type_code in (1)
		and order_channel_code !=4
		and inventory_dc_code='W0A2'
		and customer_code='127717'
		and goods_code='572894'
	group by 
		sdt
;


		select
			a.inventory_dc_code,a.main_product_code,a.sap_cus_code,
			a.change_product_code,a.update_time,a.finish_date,a.next_finish_date,b.sdt,					
			case when b.sdt>=a.finish_date and b.sdt<a.next_finish_date then b.sale_amt else null end as finish_after_sale_amt,
			case when b.sdt>=a.finish_date and b.sdt<a.next_finish_date then b.profit else null end as finish_after_profit
		from
			(
			select
				id,inventory_dc_code,main_product_code,sap_cus_code,status,
				change_product_code,create_time,update_time,
				regexp_replace(to_date(update_time),'-','') as finish_date,
				-- row_number()over(partition by inventory_dc_code,change_product_code,sap_cus_code order by update_time desc) as rn
				regexp_replace(to_date(lead(update_time,1,'9999-12-31')over(partition by inventory_dc_code,change_product_code,sap_cus_code order by update_time)),'-','') as next_finish_date
			from
				csx_ods.csx_ods_b2b_mall_prod_yszx_change_product_task_df
			where
				sdt='20221123'	
				and status=2 -- 已完成
				-- and inventory_dc_code in ('W0A3','W0A5','W0A8','W0A2')
			) a 
			left join
				(
				select
					sdt,inventory_dc_code,customer_code,goods_code,sum(sale_amt) as sale_amt,sum(profit) as profit
				from
					csx_dws.csx_dws_sale_detail_di
				where
					sdt>='20221101'
					and sdt<='20221123'
					and channel_code in ('1','7','9')
					and business_type_code in (1)
					and order_channel_code !=4
				group by 
					sdt,inventory_dc_code,customer_code,goods_code
				) b on b.inventory_dc_code=a.inventory_dc_code and b.customer_code=a.sap_cus_code and b.goods_code=a.change_product_code
		where
			-- rn=1
			b.sdt>=a.finish_date
			and b.sdt<a.next_finish_date
;
-- 采购报价
select 
	warehouse_code,product_code,purchase_price,base_product_status
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
	and warehouse_code in ('W0A3','W0P8','W0Q9','W0A2','W0BR','W0BH','W0N0','W0AS','W0A5','W0R9','W0A7','W0A6','W0Q2','W0A8','W0BK')
group by 
	warehouse_code,product_code,purchase_price,base_product_status
;
-- 建议售价
select 
	warehouse_code,product_code,suggest_price_mid,base_product_status
from 
	csx_ods.csx_ods_csx_price_prod_goods_price_guide_df 
where 
	sdt='20221117'
	and regexp_replace(to_date(price_begin_time),'-','')<='20221117' 
	and regexp_replace(to_date(price_end_time),'-','')>='20221117' 
	and is_expired='false' 
group by 
	warehouse_code,product_code,suggest_price_mid,base_product_status
;
-- 换品明细
select
	a.inventory_dc_code,
	sum(a.sale_amt) as total_sale_amt, -- 总销售额
	sum(case when a.status=2 then a.sale_amt else 0 end ) as finish_sale_amt, -- 完成商品销售额
	sum(case when a.status=3 then a.sale_amt else 0 end ) as refuse_sale_amt, -- 拒绝商品销售额
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
					sdt='20221124'
					-- and inventory_dc_code='W0A3'
				) a 
				left join
					(
					select
						sdt,inventory_dc_code,customer_code,goods_code,sum(sale_amt) as sale_amt,sum(profit) as profit
					from
						csx_dws.csx_dws_sale_detail_di
					where
						sdt>='20220101'
						and sdt<='20221124'
						and channel_code in ('1','7','9')
						and business_type_code in (1)
						and order_channel_code !=4
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
					sdt='20221124'
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
					sdt='20221124'
				) c on c.customer_no=a.sap_cus_code
		) a 
		left join
			(
			select
				a.id,a.inventory_dc_code,a.main_product_code,a.sap_cus_code,a.status,
				a.change_product_code,a.create_time,a.update_time,					
				sum(case when b.sdt>=a.finish_date and b.sdt<a.next_finish_date then b.sale_amt else 0 end) as finish_after_sale_amt,
				sum(case when b.sdt>=a.finish_date and b.sdt<a.next_finish_date then b.profit else 0 end) as finish_after_profit
			from
				(
				select
					id,inventory_dc_code,main_product_code,sap_cus_code,status,
					change_product_code,create_time,update_time,
					regexp_replace(to_date(update_time),'-','') as finish_date,
					-- row_number()over(partition by inventory_dc_code,change_product_code,sap_cus_code order by update_time desc) as rn
					regexp_replace(to_date(lead(update_time,1,'9999-12-31')over(partition by inventory_dc_code,change_product_code,sap_cus_code order by update_time)),'-','') as next_finish_date
				from
					csx_ods.csx_ods_b2b_mall_prod_yszx_change_product_task_df
				where
					sdt='20221124'	
					and status=2 -- 已完成
				) a 
				left join
					(
					select
						sdt,inventory_dc_code,customer_code,goods_code,sum(sale_amt) as sale_amt,sum(profit) as profit
					from
						csx_dws.csx_dws_sale_detail_di
					where
						sdt>='20221101'
						and sdt<='20221124'
						and channel_code in ('1','7','9')
						and business_type_code in (1)
						and order_channel_code !=4
					group by 
						sdt,inventory_dc_code,customer_code,goods_code
					) b on b.inventory_dc_code=a.inventory_dc_code and b.customer_code=a.sap_cus_code and b.goods_code=a.change_product_code
			where
				1=1
				-- rn=1
			group by 
				a.id,a.inventory_dc_code,a.main_product_code,a.sap_cus_code,a.status,
				a.change_product_code,a.create_time,a.update_time				
			) b on b.id=a.id
	) a 
group by 
	a.inventory_dc_code


		
		
	