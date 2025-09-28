-- ===============================================================================================================================================================================
drop table csx_analyse_tmp.csx_analyse_tmp_cust_business_detail;
create table csx_analyse_tmp.csx_analyse_tmp_cust_business_detail
as
select
	a.stage_1_month,a.cust_flag,a.business_sign_month,a.business_number,a.customer_id,a.customer_code,a.customer_name,a.first_category_name,a.second_category_name,a.third_category_name,
	a.performance_region_name,a.performance_province_name,a.performance_city_name,
	a.owner_user_number,a.owner_user_name,a.business_type_code,a.business_type_name,a.customer_acquisition_type_code,a.customer_acquisition_type_name,
	a.business_stage,a.contract_cycle,a.estimate_contract_amount,a.gross_profit_rate,a.business_sign_date,a.business_sign_date_2,a.first_business_sign_date,a.stage_1_date,
	a.stage_2_date,a.stage_3_date,a.stage_4_date,a.stage_5_date,a.diff_2,a.diff_3,a.diff_4,a.diff_5,a.diff_1_5,a.new_classify_name,a.num,a.next_sign_date,
	sum(case when b.sdt>=a.business_sign_date and b.sdt<a.next_sign_date then b.sale_amt else null end) as sale_amt,
	sum(case when b.sdt>=a.business_sign_date and b.sdt<a.next_sign_date then b.profit else null end) as profit,
	count(distinct case when b.sdt>=a.business_sign_date and b.sdt<a.next_sign_date then b.sdt else null end) as sdt_cnt,
	min(case when b.sdt>=a.business_sign_date and b.sdt<a.next_sign_date then b.sdt else null end) as min_sdt,
	from_unixtime(unix_timestamp(min(case when b.sdt>=a.business_sign_date and b.sdt<a.next_sign_date then b.sdt else null end),'yyyyMMdd'),'yyyy-MM-dd') as min_sdt_date,
	datediff(from_unixtime(unix_timestamp(min(case when b.sdt>=a.business_sign_date and b.sdt<a.next_sign_date then b.sdt else null end),'yyyyMMdd'),'yyyy-MM-dd'),a.stage_5_date) as diff_sale_5,
	datediff(from_unixtime(unix_timestamp(min(case when b.sdt>=a.business_sign_date and b.sdt<a.next_sign_date then b.sdt else null end),'yyyyMMdd'),'yyyy-MM-dd'),a.stage_1_date) as diff_sale_1_5
from
	(
	select
		date_format(f.stage_1_date,'yyyyMM') as stage_1_month,a.cust_flag,
		a.business_sign_month,a.business_number,a.customer_id,a.customer_code,a.customer_name,g.first_category_name,g.second_category_name,g.third_category_name,
		a.performance_region_name,a.performance_province_name,a.performance_city_name,
		a.owner_user_number,a.owner_user_name,a.business_type_code,a.business_type_name,a.customer_acquisition_type_code,a.customer_acquisition_type_name,
		a.business_stage,a.contract_cycle,a.estimate_contract_amount,a.gross_profit_rate,
		a.business_sign_date,a.business_sign_date_2,a.first_business_sign_date,f.stage_1_date,
		b.stage_2_date,c.stage_3_date,d.stage_4_date,e.stage_5_date,
		datediff(b.stage_2_date,f.stage_1_date) as diff_2,
		datediff(c.stage_3_date,b.stage_2_date) as diff_3,
		datediff(d.stage_4_date,c.stage_3_date) as diff_4,
		datediff(e.stage_5_date,d.stage_4_date) as diff_5,
		datediff(e.stage_5_date,f.stage_1_date) as diff_1_5,
		h.new_classify_name,
		row_number() over(partition by a.customer_code order by a.business_sign_time) num,
		regexp_replace(to_date(lead(a.business_sign_time,1,'9999-12-31')over(partition by a.customer_code order by a.business_stage,a.business_sign_time)),'-','') as next_sign_date
		
	from 
		(
		select
			if(to_date(business_sign_time)=to_date(first_business_sign_time),'新客','老客') cust_flag,business_sign_time,
			regexp_replace(substr(to_date(business_sign_time),1,7),'-','') business_sign_month,business_number,customer_id,customer_code,customer_name,
			first_category_name,second_category_name,third_category_name,
			performance_region_name,performance_province_name,performance_city_name,
			owner_user_number,owner_user_name,business_type_code,business_type_name,customer_acquisition_type_code,
			if(customer_acquisition_type_name='','非投标',customer_acquisition_type_name) as customer_acquisition_type_name,
			business_stage,contract_cycle,estimate_contract_amount,gross_profit_rate,
			regexp_replace(to_date(business_sign_time),'-','') business_sign_date,
			to_date(business_sign_time) as business_sign_date_2,
			regexp_replace(to_date(first_business_sign_time),'-','') first_business_sign_date
			-- row_number() over(partition by concat(customer_code) order by business_sign_time) num --商机顺序
		from 
			csx_dim.csx_dim_crm_business_info
		where 
			sdt='current'
			and channel_code in('1','7','9')
			and business_type_code in(1) -- 日配业务
			and status=1  -- 是否有效 0.无效 1.有效 (status=0,'停止跟进')
			and business_stage=5
			-- and regexp_replace(to_date(business_sign_time),'-','')>='20220101'		
		)a
		left join
			(
			select
				business_number,to_date(min(create_time)) as stage_2_date
			from 
				csx_ods.csx_ods_csx_crm_prod_operate_log_df
			where
				sdt='20230331'
				and operate_type='01' 
				and before_data='1'
				and after_data='2'
				and business_number is not null
			group by 
				business_number
			) b on b.business_number=a.business_number
		left join
			(	
			select
				business_number,to_date(min(create_time)) as stage_3_date
			from 
				csx_ods.csx_ods_csx_crm_prod_operate_log_df
			where
				sdt='20230331'
				and operate_type='01' 
				and before_data='2'
				and after_data='3'
				and business_number is not null
			group by 
				business_number
			)c on c.business_number=a.business_number
		left join
			(
			select
				business_number,to_date(min(create_time)) as stage_4_date
			from 
				csx_ods.csx_ods_csx_crm_prod_operate_log_df
			where
				sdt='20230331'
				and operate_type='01' 
				and before_data='3'
				and after_data='4'
				and business_number is not null
			group by 
				business_number
			)d on d.business_number=a.business_number
		left join
			(
			select
				business_number,to_date(min(create_time)) as stage_5_date
			from 
				csx_ods.csx_ods_csx_crm_prod_operate_log_df
			where
				sdt='20230331'
				and operate_type='01' 
				and before_data='4'
				and after_data='5'
				and business_number is not null
			group by 
				business_number
			)e on e.business_number=a.business_number
		left join
			(
			select
				business_number,to_date(min(create_time)) as stage_1_date
			from 
				csx_ods.csx_ods_csx_crm_prod_operate_log_df
			where
				sdt='20230331'
				and operate_type='01' 
				and before_data='0'
				and after_data='1'
				and business_number is not null
			group by 
				business_number
			) f on f.business_number=a.business_number
		left join
			(
			select
				customer_code,customer_name,performance_region_name,performance_province_name,performance_city_name,first_category_name,second_category_name,third_category_name,sign_company_code,
				sales_user_number,sales_user_name,customer_address_full
			from
				csx_dim.csx_dim_crm_customer_info
			where
				sdt='current'
			) g on g.customer_code=a.customer_code
		left join
			(
			select
				second_category_code,second_category_name,new_classify_name
			from
				csx_analyse.csx_analyse_fr_new_customer_classify_mf
			group by 
				second_category_code,second_category_name,new_classify_name
			) h on h.second_category_name=g.second_category_name
	where
		f.stage_1_date>='2022-10-01' and f.stage_1_date<='2023-03-31'
	) a 
	left join 
		(
		select 
			sdt,customer_code,
			sum(sale_amt) as sale_amt,
			sum(profit) as profit,
			sum(profit)/abs(sum(sale_amt)) as profit_rate
		from 	
			csx_dws.csx_dws_sale_detail_di
		where 
			sdt>='20221001' and sdt<='20230331'
			and channel_code in('1','7','9')
			and business_type_code in(1)
			and order_channel_code not in (4,6) -- 1-b端 2-m端 3-bbc 4-客户返利管理 5-价格补救 6-客户调价管理 -1-sap
		group by 
			sdt,customer_code
		)b on a.customer_code=b.customer_code
group by 
	a.stage_1_month,a.cust_flag,a.business_sign_month,a.business_number,a.customer_id,a.customer_code,a.customer_name,a.first_category_name,a.second_category_name,a.third_category_name,
	a.performance_region_name,a.performance_province_name,a.performance_city_name,
	a.owner_user_number,a.owner_user_name,a.business_type_code,a.business_type_name,a.customer_acquisition_type_code,a.customer_acquisition_type_name,
	a.business_stage,a.contract_cycle,a.estimate_contract_amount,a.gross_profit_rate,a.business_sign_date,a.business_sign_date_2,a.first_business_sign_date,a.stage_1_date,
	a.stage_2_date,a.stage_3_date,a.stage_4_date,a.stage_5_date,a.diff_2,a.diff_3,a.diff_4,a.diff_5,a.diff_1_5,a.new_classify_name,a.num,a.next_sign_date	
;
select * from csx_analyse_tmp.csx_analyse_tmp_cust_business_detail

-- ===============================================================================================================================================================================
drop table csx_analyse_tmp.csx_analyse_tmp_cust_business_detail_02;
create table csx_analyse_tmp.csx_analyse_tmp_cust_business_detail_02
as
select
	a.stage_1_month,a.cust_flag,a.business_sign_month,a.business_number,a.customer_id,a.customer_code,a.customer_name,a.first_category_name,a.second_category_name,a.third_category_name,
	a.performance_region_name,a.performance_province_name,a.performance_city_name,
	a.owner_user_number,a.owner_user_name,a.business_type_code,a.business_type_name,a.customer_acquisition_type_code,a.customer_acquisition_type_name,
	a.business_stage,a.contract_cycle,a.estimate_contract_amount,a.gross_profit_rate,a.business_sign_date,a.business_sign_date_2,a.first_business_sign_date,a.stage_1_date,
	a.stage_2_date,a.stage_3_date,a.stage_4_date,a.stage_5_date,a.diff_2,a.diff_3,a.diff_4,a.diff_5,a.diff_1_5,a.new_classify_name,a.num,a.next_sign_date,
	sum(case when b.sdt>=a.business_sign_date and b.sdt<a.next_sign_date then b.sale_amt else null end) as sale_amt,
	sum(case when b.sdt>=a.business_sign_date and b.sdt<a.next_sign_date then b.profit else null end) as profit,
	count(distinct case when b.sdt>=a.business_sign_date and b.sdt<a.next_sign_date then b.sdt else null end) as sdt_cnt,
	min(case when b.sdt>=a.business_sign_date and b.sdt<a.next_sign_date then b.sdt else null end) as min_sdt,
	from_unixtime(unix_timestamp(min(case when b.sdt>=a.business_sign_date and b.sdt<a.next_sign_date then b.sdt else null end),'yyyyMMdd'),'yyyy-MM-dd') as min_sdt_date,
	datediff(from_unixtime(unix_timestamp(min(case when b.sdt>=a.business_sign_date and b.sdt<a.next_sign_date then b.sdt else null end),'yyyyMMdd'),'yyyy-MM-dd'),a.stage_5_date) as diff_sale_5,
	datediff(from_unixtime(unix_timestamp(min(case when b.sdt>=a.business_sign_date and b.sdt<a.next_sign_date then b.sdt else null end),'yyyyMMdd'),'yyyy-MM-dd'),a.stage_1_date) as diff_sale_1_5
from
	(
	select
		date_format(f.stage_1_date,'yyyyMM') as stage_1_month,cust_flag,
		a.business_sign_month,a.business_number,a.customer_id,a.customer_code,a.customer_name,g.first_category_name,g.second_category_name,g.third_category_name,
		a.performance_region_name,a.performance_province_name,a.performance_city_name,
		a.owner_user_number,a.owner_user_name,a.business_type_code,a.business_type_name,a.customer_acquisition_type_code,a.customer_acquisition_type_name,
		a.business_stage,a.contract_cycle,a.estimate_contract_amount,a.gross_profit_rate,
		a.business_sign_date,a.business_sign_date_2,a.first_business_sign_date,f.stage_1_date,
		b.stage_2_date,c.stage_3_date,d.stage_4_date,e.stage_5_date,
		datediff(b.stage_2_date,f.stage_1_date) as diff_2,
		datediff(c.stage_3_date,b.stage_2_date) as diff_3,
		datediff(d.stage_4_date,c.stage_3_date) as diff_4,
		datediff(e.stage_5_date,d.stage_4_date) as diff_5,
		datediff(e.stage_5_date,f.stage_1_date) as diff_1_5,
		h.new_classify_name,
		row_number() over(partition by a.customer_code order by a.business_sign_time) num, --商机顺序
		regexp_replace(to_date(lead(a.business_sign_time,1,'9999-12-31')over(partition by a.customer_code order by a.business_stage,a.business_sign_time)),'-','') as next_sign_date
	from 
		(
		select
			if(to_date(business_sign_time)=to_date(first_business_sign_time),'新客','老客') cust_flag,business_sign_time,
			regexp_replace(substr(to_date(business_sign_time),1,7),'-','') business_sign_month,business_number,customer_id,customer_code,customer_name,
			first_category_name,second_category_name,third_category_name,
			performance_region_name,performance_province_name,performance_city_name,
			owner_user_number,owner_user_name,business_type_code,business_type_name,customer_acquisition_type_code,
			if(customer_acquisition_type_name='','非投标',customer_acquisition_type_name) as customer_acquisition_type_name,
			business_stage,contract_cycle,estimate_contract_amount,gross_profit_rate,
			regexp_replace(to_date(business_sign_time),'-','') business_sign_date,
			to_date(business_sign_time) as business_sign_date_2,
			regexp_replace(to_date(first_business_sign_time),'-','') first_business_sign_date
			-- row_number() over(partition by concat(customer_code) order by business_sign_time) num --商机顺序
		from 
			csx_dim.csx_dim_crm_business_info
		where 
			sdt='current'
			and channel_code in('1','7','9')
			and business_type_code in(1) -- 日配业务
			and status=1  -- 是否有效 0.无效 1.有效 (status=0,'停止跟进')
			-- and business_stage=5
			-- and regexp_replace(to_date(business_sign_time),'-','')>='20220101'		
		)a
		left join
			(
			select
				business_number,to_date(min(create_time)) as stage_2_date
			from 
				csx_ods.csx_ods_csx_crm_prod_operate_log_df
			where
				sdt='20230331'
				and operate_type='01' 
				and before_data='1'
				and after_data='2'
				and business_number is not null
			group by 
				business_number
			) b on b.business_number=a.business_number
		left join
			(	
			select
				business_number,to_date(min(create_time)) as stage_3_date
			from 
				csx_ods.csx_ods_csx_crm_prod_operate_log_df
			where
				sdt='20230331'
				and operate_type='01' 
				and before_data='2'
				and after_data='3'
				and business_number is not null
			group by 
				business_number
			)c on c.business_number=a.business_number
		left join
			(
			select
				business_number,to_date(min(create_time)) as stage_4_date
			from 
				csx_ods.csx_ods_csx_crm_prod_operate_log_df
			where
				sdt='20230331'
				and operate_type='01' 
				and before_data='3'
				and after_data='4'
				and business_number is not null
			group by 
				business_number
			)d on d.business_number=a.business_number
		left join
			(
			select
				business_number,to_date(min(create_time)) as stage_5_date
			from 
				csx_ods.csx_ods_csx_crm_prod_operate_log_df
			where
				sdt='20230331'
				and operate_type='01' 
				and before_data='4'
				and after_data='5'
				and business_number is not null
			group by 
				business_number
			)e on e.business_number=a.business_number
		left join
			(
			select
				business_number,to_date(min(create_time)) as stage_1_date
			from 
				csx_ods.csx_ods_csx_crm_prod_operate_log_df
			where
				sdt='20230331'
				and operate_type='01' 
				and before_data='0'
				and after_data='1'
				and business_number is not null
			group by 
				business_number
			) f on f.business_number=a.business_number
		left join
			(
			select
				customer_code,customer_name,performance_region_name,performance_province_name,performance_city_name,first_category_name,second_category_name,third_category_name,sign_company_code,
				sales_user_number,sales_user_name,customer_address_full
			from
				csx_dim.csx_dim_crm_customer_info
			where
				sdt='current'
			) g on g.customer_code=a.customer_code		
		left join
			(
			select
				second_category_code,second_category_name,new_classify_name
			from
				csx_analyse.csx_analyse_fr_new_customer_classify_mf
			group by 
				second_category_code,second_category_name,new_classify_name
			) h on h.second_category_name=g.second_category_name	
	where
		f.stage_1_date>='2022-10-01' and f.stage_1_date<='2023-03-31'
	) a 
	left join 
		(
		select 
			sdt,customer_code,
			sum(sale_amt) as sale_amt,
			sum(profit) as profit,
			sum(profit)/abs(sum(sale_amt)) as profit_rate
		from 	
			csx_dws.csx_dws_sale_detail_di
		where 
			sdt>='20221001' and sdt<='20230331'
			and channel_code in('1','7','9')
			and business_type_code in(1)
			and order_channel_code not in (4,6) -- 1-b端 2-m端 3-bbc 4-客户返利管理 5-价格补救 6-客户调价管理 -1-sap
		group by 
			sdt,customer_code
		)b on a.customer_code=b.customer_code			
group by 
	a.stage_1_month,a.cust_flag,a.business_sign_month,a.business_number,a.customer_id,a.customer_code,a.customer_name,a.first_category_name,a.second_category_name,a.third_category_name,
	a.performance_region_name,a.performance_province_name,a.performance_city_name,
	a.owner_user_number,a.owner_user_name,a.business_type_code,a.business_type_name,a.customer_acquisition_type_code,a.customer_acquisition_type_name,
	a.business_stage,a.contract_cycle,a.estimate_contract_amount,a.gross_profit_rate,a.business_sign_date,a.business_sign_date_2,a.first_business_sign_date,a.stage_1_date,
	a.stage_2_date,a.stage_3_date,a.stage_4_date,a.stage_5_date,a.diff_2,a.diff_3,a.diff_4,a.diff_5,a.diff_1_5,a.new_classify_name,a.num,a.next_sign_date
;
select * from csx_analyse_tmp.csx_analyse_tmp_cust_business_detail_02
;

-- 验数
		select 
			sdt,customer_code,order_channel_code,
			sum(sale_amt) as sale_amt,
			sum(profit) as profit,
			sum(profit)/abs(sum(sale_amt)) as profit_rate
		from 	
			csx_dws.csx_dws_sale_detail_di
		where 
			sdt>='20220101' and sdt<='20230331'
			and channel_code in('1','7','9')
			and business_type_code in(1)
			and customer_code='127389'
		group by 
			sdt,customer_code,order_channel_code


