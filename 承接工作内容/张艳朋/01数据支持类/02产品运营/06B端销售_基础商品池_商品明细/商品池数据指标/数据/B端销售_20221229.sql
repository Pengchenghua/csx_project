-- 换品明细
create table csx_analyse_tmp.csx_analyse_tmp_yszx_change_product_task
as 
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
				sdt='20221228'
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
					and sdt<='20221228'
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
				sdt='20221228'
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
				sdt='20221228'
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
				sdt='20221228'	
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
					and sdt<='20221228'
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
;
select * from csx_analyse_tmp.csx_analyse_tmp_yszx_change_product_task
;


-- 换品明细
create table csx_analyse_tmp.csx_analyse_tmp_yszx_change_product_task_02
as 
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
				sdt='20221228'
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
					and sdt<='20221228'
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
				sdt='20221228'
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
				sdt='20221228'
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
				sdt='20221228'	
				and status=2 -- 已完成
			) a 
			left join
				(
				select
					sdt,inventory_dc_code,customer_code,goods_code,sum(sale_amt) as sale_amt,sum(profit) as profit
				from
					csx_dws.csx_dws_sale_detail_di
				where
					sdt>='20221201'
					and sdt<='20221228'
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
;
select * from csx_analyse_tmp.csx_analyse_tmp_yszx_change_product_task_02