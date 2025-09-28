
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
;


-- 换品复盘看板（省区内个人维度）
-- 业务员	
select
	-- a.inventory_dc_code,
	-- a.operator,
	sales_user_number,
	sales_user_name,
	performance_province_name,
	coalesce((finish_product_cnt/(finish_product_cnt+refuse_product_cnt))*0.5,0)+coalesce((finish_sale_amt/(finish_sale_amt+refuse_sale_amt))*0.5,0) as hp_pass_rate,
	(finish_product_cnt+refuse_product_cnt)/(pending_product_cnt+finish_product_cnt+refuse_product_cnt) as processing_progress,
	pending_product_cnt+finish_product_cnt+refuse_product_cnt as need_hp_cnt,
	pending_product_cnt,
	finish_product_cnt,
	refuse_product_cnt,
	hp_sale_goods_cnt,
	finish_sale_amt,
	refuse_sale_amt
from
	(
	select
		-- a.inventory_dc_code,
		-- a.operator,
		b.sales_user_number,
		b.sales_user_name,
		b.performance_province_name,
		count( case when a.status='1' then a.main_product_code else null end) as pending_product_cnt, -- 待处理商品数
		count( case when a.status='2' then a.main_product_code else null end) as finish_product_cnt, -- 完成商品数
		count( case when a.status='3' then a.main_product_code else null end) as refuse_product_cnt, -- 拒绝商品数		
		sum(a.sale_amt) as total_sale_amt, -- 总销售额
		sum(case when a.status=2 then a.sale_amt else 0 end ) as finish_sale_amt, -- 完成商品销售额
		sum(case when a.status=3 then a.sale_amt else 0 end ) as refuse_sale_amt, -- 拒绝商品销售额
		count( case when a.status='2' and a.sale_amt>0 then a.main_product_code else null end) as hp_sale_goods_cnt
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
		left join
			(
			select
				customer_code,customer_name,
				-- 业务员
				sales_user_number,sales_user_name,
				-- 主管
				supervisor_user_number,supervisor_user_name,
				-- 经理
				city_manager_user_number,city_manager_user_name,
				-- 总监
				sales_manager_user_number,sales_manager_user_name,
				-- 省区总
				province_manager_user_number,province_manager_user_name,
				performance_province_name,performance_city_name
			from
				csx_dim.csx_dim_crm_customer_info
			where
				sdt='current'
			) b on b.customer_code=a.sap_cus_code
	group by 
		-- a.inventory_dc_code,a.operator
		b.sales_user_number,b.sales_user_name,b.performance_province_name
	) a 
;

-- 换品复盘看板（省区内个人维度）
-- 主管	
select
	-- a.inventory_dc_code,
	-- a.operator,
	supervisor_user_number,
	supervisor_user_name,
	performance_province_name,
	coalesce((finish_product_cnt/(finish_product_cnt+refuse_product_cnt))*0.5,0)+coalesce((finish_sale_amt/(finish_sale_amt+refuse_sale_amt))*0.5,0) as hp_pass_rate,
	(finish_product_cnt+refuse_product_cnt)/(pending_product_cnt+finish_product_cnt+refuse_product_cnt) as processing_progress,
	pending_product_cnt+finish_product_cnt+refuse_product_cnt as need_hp_cnt,
	pending_product_cnt,
	finish_product_cnt,
	refuse_product_cnt,
	hp_sale_goods_cnt,
	finish_sale_amt,
	refuse_sale_amt
from
	(
	select
		-- a.inventory_dc_code,
		-- a.operator,
		b.supervisor_user_number,
		b.supervisor_user_name,
		b.performance_province_name,
		count( case when a.status='1' then a.main_product_code else null end) as pending_product_cnt, -- 待处理商品数
		count( case when a.status='2' then a.main_product_code else null end) as finish_product_cnt, -- 完成商品数
		count( case when a.status='3' then a.main_product_code else null end) as refuse_product_cnt, -- 拒绝商品数		
		sum(a.sale_amt) as total_sale_amt, -- 总销售额
		sum(case when a.status=2 then a.sale_amt else 0 end ) as finish_sale_amt, -- 完成商品销售额
		sum(case when a.status=3 then a.sale_amt else 0 end ) as refuse_sale_amt, -- 拒绝商品销售额
		count( case when a.status='2' and a.sale_amt>0 then a.main_product_code else null end) as hp_sale_goods_cnt
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
		left join
			(
			select
				customer_code,customer_name,
				-- 业务员
				sales_user_number,sales_user_name,
				-- 主管
				supervisor_user_number,supervisor_user_name,
				-- 经理
				city_manager_user_number,city_manager_user_name,
				-- 总监
				sales_manager_user_number,sales_manager_user_name,
				-- 省区总
				province_manager_user_number,province_manager_user_name,
				performance_province_name,performance_city_name
			from
				csx_dim.csx_dim_crm_customer_info
			where
				sdt='current'
			) b on b.customer_code=a.sap_cus_code
	group by 
		-- a.inventory_dc_code,a.operator
		b.supervisor_user_number,b.supervisor_user_name,b.performance_province_name
	) a 
;

-- 换品复盘看板（省区内个人维度）
-- 经理
select
	-- a.inventory_dc_code,
	-- a.operator,
	city_manager_user_number,city_manager_user_name,
	performance_province_name,
	coalesce((finish_product_cnt/(finish_product_cnt+refuse_product_cnt))*0.5,0)+coalesce((finish_sale_amt/(finish_sale_amt+refuse_sale_amt))*0.5,0) as hp_pass_rate,
	(finish_product_cnt+refuse_product_cnt)/(pending_product_cnt+finish_product_cnt+refuse_product_cnt) as processing_progress,
	pending_product_cnt+finish_product_cnt+refuse_product_cnt as need_hp_cnt,
	pending_product_cnt,
	finish_product_cnt,
	refuse_product_cnt,
	hp_sale_goods_cnt,
	finish_sale_amt,
	refuse_sale_amt
from
	(
	select
		-- a.inventory_dc_code,
		-- a.operator,
		b.city_manager_user_number,b.city_manager_user_name,
		b.performance_province_name,
		count( case when a.status='1' then a.main_product_code else null end) as pending_product_cnt, -- 待处理商品数
		count( case when a.status='2' then a.main_product_code else null end) as finish_product_cnt, -- 完成商品数
		count( case when a.status='3' then a.main_product_code else null end) as refuse_product_cnt, -- 拒绝商品数		
		sum(a.sale_amt) as total_sale_amt, -- 总销售额
		sum(case when a.status=2 then a.sale_amt else 0 end ) as finish_sale_amt, -- 完成商品销售额
		sum(case when a.status=3 then a.sale_amt else 0 end ) as refuse_sale_amt, -- 拒绝商品销售额
		count( case when a.status='2' and a.sale_amt>0 then a.main_product_code else null end) as hp_sale_goods_cnt
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
		left join
			(
			select
				customer_code,customer_name,
				-- 业务员
				sales_user_number,sales_user_name,
				-- 主管
				supervisor_user_number,supervisor_user_name,
				-- 经理
				city_manager_user_number,city_manager_user_name,
				-- 总监
				sales_manager_user_number,sales_manager_user_name,
				-- 省区总
				province_manager_user_number,province_manager_user_name,
				performance_province_name,performance_city_name
			from
				csx_dim.csx_dim_crm_customer_info
			where
				sdt='current'
			) b on b.customer_code=a.sap_cus_code
	group by 
		-- a.inventory_dc_code,a.operator
		b.city_manager_user_number,b.city_manager_user_name,b.performance_province_name
	) a 
;

-- 换品复盘看板（省区内个人维度）
-- 总监
select
	-- a.inventory_dc_code,
	-- a.operator,
	sales_manager_user_number,sales_manager_user_name,
	performance_province_name,
	coalesce((finish_product_cnt/(finish_product_cnt+refuse_product_cnt))*0.5,0)+coalesce((finish_sale_amt/(finish_sale_amt+refuse_sale_amt))*0.5,0) as hp_pass_rate,
	(finish_product_cnt+refuse_product_cnt)/(pending_product_cnt+finish_product_cnt+refuse_product_cnt) as processing_progress,
	pending_product_cnt+finish_product_cnt+refuse_product_cnt as need_hp_cnt,
	pending_product_cnt,
	finish_product_cnt,
	refuse_product_cnt,
	hp_sale_goods_cnt,
	finish_sale_amt,
	refuse_sale_amt
from
	(
	select
		-- a.inventory_dc_code,
		-- a.operator,
		sales_manager_user_number,sales_manager_user_name,
		b.performance_province_name,
		count( case when a.status='1' then a.main_product_code else null end) as pending_product_cnt, -- 待处理商品数
		count( case when a.status='2' then a.main_product_code else null end) as finish_product_cnt, -- 完成商品数
		count( case when a.status='3' then a.main_product_code else null end) as refuse_product_cnt, -- 拒绝商品数		
		sum(a.sale_amt) as total_sale_amt, -- 总销售额
		sum(case when a.status=2 then a.sale_amt else 0 end ) as finish_sale_amt, -- 完成商品销售额
		sum(case when a.status=3 then a.sale_amt else 0 end ) as refuse_sale_amt, -- 拒绝商品销售额
		count( case when a.status='2' and a.sale_amt>0 then a.main_product_code else null end) as hp_sale_goods_cnt
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
		left join
			(
			select
				customer_code,customer_name,
				-- 业务员
				sales_user_number,sales_user_name,
				-- 主管
				supervisor_user_number,supervisor_user_name,
				-- 经理
				city_manager_user_number,city_manager_user_name,
				-- 总监
				sales_manager_user_number,sales_manager_user_name,
				-- 省区总
				province_manager_user_number,province_manager_user_name,
				performance_province_name,performance_city_name
			from
				csx_dim.csx_dim_crm_customer_info
			where
				sdt='current'
			) b on b.customer_code=a.sap_cus_code
	group by 
		-- a.inventory_dc_code,a.operator
		sales_manager_user_number,sales_manager_user_name,b.performance_province_name
	) a 		
;

-- 换品复盘看板（省区内个人维度）
-- 省区总
select
	-- a.inventory_dc_code,
	-- a.operator,
	province_manager_user_number,province_manager_user_name,
	performance_province_name,
	coalesce((finish_product_cnt/(finish_product_cnt+refuse_product_cnt))*0.5,0)+coalesce((finish_sale_amt/(finish_sale_amt+refuse_sale_amt))*0.5,0) as hp_pass_rate,
	(finish_product_cnt+refuse_product_cnt)/(pending_product_cnt+finish_product_cnt+refuse_product_cnt) as processing_progress,
	pending_product_cnt+finish_product_cnt+refuse_product_cnt as need_hp_cnt,
	pending_product_cnt,
	finish_product_cnt,
	refuse_product_cnt,
	hp_sale_goods_cnt,
	finish_sale_amt,
	refuse_sale_amt
from
	(
	select
		-- a.inventory_dc_code,
		-- a.operator,
		province_manager_user_number,province_manager_user_name,
		b.performance_province_name,
		count( case when a.status='1' then a.main_product_code else null end) as pending_product_cnt, -- 待处理商品数
		count( case when a.status='2' then a.main_product_code else null end) as finish_product_cnt, -- 完成商品数
		count( case when a.status='3' then a.main_product_code else null end) as refuse_product_cnt, -- 拒绝商品数		
		sum(a.sale_amt) as total_sale_amt, -- 总销售额
		sum(case when a.status=2 then a.sale_amt else 0 end ) as finish_sale_amt, -- 完成商品销售额
		sum(case when a.status=3 then a.sale_amt else 0 end ) as refuse_sale_amt, -- 拒绝商品销售额
		count( case when a.status='2' and a.sale_amt>0 then a.main_product_code else null end) as hp_sale_goods_cnt
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
		left join
			(
			select
				customer_code,customer_name,
				-- 业务员
				sales_user_number,sales_user_name,
				-- 主管
				supervisor_user_number,supervisor_user_name,
				-- 经理
				city_manager_user_number,city_manager_user_name,
				-- 总监
				sales_manager_user_number,sales_manager_user_name,
				-- 省区总
				province_manager_user_number,province_manager_user_name,
				performance_province_name,performance_city_name
			from
				csx_dim.csx_dim_crm_customer_info
			where
				sdt='current'
			) b on b.customer_code=a.sap_cus_code
	group by 
		-- a.inventory_dc_code,a.operator
		province_manager_user_number,province_manager_user_name,b.performance_province_name
	) a 	