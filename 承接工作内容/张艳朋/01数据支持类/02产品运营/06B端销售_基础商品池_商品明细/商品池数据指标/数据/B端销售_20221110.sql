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
			sdt='20221109'
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

-- B端销售_换品配置

select
	a.inventory_dc_code,a.inventory_dc_name,a.main_product_code,a.main_product_name,a.unit,a.sales_amount,a.profit_margin,a.sap_cus_code,
	a.sap_cus_name,a.status_name,
	if(a.status=1,coalesce(c.rp_service_user_name_new,c.fl_service_user_name_new,c.bbc_service_user_name_new,c.sales_name_new),a.update_by) as update_by,
	a.change_product_code,a.change_product_name,a.change_unit,
	-- a.create_time,
	coalesce(b.create_time,'') as create_time, -- 操作时间 发布时间
	a.update_time -- 完成时间
from
	(
	select
		inventory_dc_code,inventory_dc_name,main_product_code,
		main_product_name,
		unit,sales_amount,profit_margin,sap_cus_code,sap_cus_name,
		status,
		case status when 1 then '待处理' when 2 then '已完成' when 3 then '已拒绝' end as status_name,
		-- create_by,
		update_by,change_product_code,change_product_name,change_unit,
		create_time,
		update_time
	from
		csx_ods.csx_ods_b2b_mall_prod_yszx_change_product_task_df
	where
		sdt='20221109'
	) a 
	left join
		(
		select
			inventory_dc_code,main_product_code,update_time,create_time
		from
			csx_ods.csx_ods_b2b_mall_prod_yszx_change_product_config_df
		where
			sdt='20221109'
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
			sdt='20221109'
		) c on c.customer_no=a.sap_cus_code
;
		
		
	select
		a.inventory_dc_code,a.main_product_code,a.sap_cus_code,a.status,a.create_date,a.month_ago_1,a.month_ago_2,a.month_ago_3,
		coalesce(a.month_ago_1_sale_amt,a.month_ago_2_sale_amt,a.month_ago_3_sale_amt,0) as sale_amt,
		coalesce(a.month_ago_1_profit,a.month_ago_2_profit,a.month_ago_3_profit,0) as profit
	from
		(
		select
			a.inventory_dc_code,a.main_product_code,a.sap_cus_code,a.status,a.create_date,a.month_ago_1,a.month_ago_2,a.month_ago_3,
			sum(case when b.sdt>=a.month_ago_1 and b.sdt<=a.create_date then b.sale_amt else null end) as month_ago_1_sale_amt,
			sum(case when b.sdt>=a.month_ago_2 and b.sdt<=a.create_date then b.sale_amt else null end) as month_ago_2_sale_amt,
			sum(case when b.sdt>=a.month_ago_3 and b.sdt<=a.create_date then b.sale_amt else null end) as month_ago_3_sale_amt,
			
			sum(case when b.sdt>=a.month_ago_1 and b.sdt<=a.create_date then b.profit else null end) as month_ago_1_profit,
			sum(case when b.sdt>=a.month_ago_2 and b.sdt<=a.create_date then b.profit else null end) as month_ago_2_profit,
			sum(case when b.sdt>=a.month_ago_3 and b.sdt<=a.create_date then b.profit else null end) as month_ago_3_profit
		from
			(
			select
				a.inventory_dc_code,a.main_product_code,a.sap_cus_code,a.status,regexp_replace(to_date(b.create_time),'-','') as create_date,
				regexp_replace(add_months(to_date(b.create_time),-1),'-','') as month_ago_1,
				regexp_replace(add_months(to_date(b.create_time),-2),'-','') as month_ago_2,
				regexp_replace(add_months(to_date(b.create_time),-3),'-','') as month_ago_3
			from
				(
				select
					inventory_dc_code,main_product_code,sap_cus_code,status,create_time
				from
					csx_ods.csx_ods_b2b_mall_prod_yszx_change_product_task_df
				where
					sdt='20221109'
				) a 
				left join
					(
					select
						inventory_dc_code,main_product_code,update_time,create_time
					from
						csx_ods.csx_ods_b2b_mall_prod_yszx_change_product_config_df
					where
						sdt='20221109'
					group by 
						inventory_dc_code,main_product_code,update_time,create_time
					) b on b.inventory_dc_code=a.inventory_dc_code and b.main_product_code=a.main_product_code
			) a 
			join
				(
				select
					sdt,inventory_dc_code,customer_code,goods_code,sum(sale_amt) as sale_amt,sum(profit) as profit
				from
					csx_dws.csx_dws_sale_detail_di
				where
					sdt>=(select regexp_replace(add_months(to_date(min(create_time)),-3),'-','') from csx_ods.csx_ods_b2b_mall_prod_yszx_change_product_task_df where sdt='20221109')
					and sdt<='20221109'
					and channel_code in ('1','7','9')
					and business_type_code in (1)
					and order_channel_code !=4
				group by 
					sdt,inventory_dc_code,customer_code,goods_code
				) b on b.inventory_dc_code=a.inventory_dc_code and b.customer_code=a.sap_cus_code and b.goods_code=a.main_product_code
		group by 
			a.inventory_dc_code,a.main_product_code,a.sap_cus_code,a.status,a.create_date,a.month_ago_1,a.month_ago_2,a.month_ago_3
		) a 
	