
insert overwrite directory '/tmp/zhangyanpeng/20221226_01' row format delimited fields terminated by '\t'

select
	coalesce(b.rp_service_user_work_no_new,'') as rp_service_user_work_no_new,
	coalesce(b.rp_service_user_name_new,'') as rp_service_user_name_new,
	coalesce(b.fl_service_user_work_no_new,'') as fl_service_user_work_no_new,
	coalesce(b.fl_service_user_name_new,'') as fl_service_user_name_new,
	coalesce(b.bbc_service_user_work_no_new,'') as bbc_service_user_work_no_new,
	coalesce(b.bbc_service_user_name_new,'') as bbc_service_user_name_new,
	sum(sales_value) as sales_value,
	sum(profit) as profit,
	sum(rp_sales_value) as rp_sales_value,
	sum(fl_sales_value) as fl_sales_value,
	sum(bbc_sales_value) as bbc_sales_value,
	sum(rp_profit) as rp_profit,
	sum(fl_profit) as fl_profit,
	sum(bbc_profit) as bbc_profit
from
	(
	select
		substr(sdt,1,6) as smonth,customer_no,	
		sum(sales_value) as sales_value,sum(profit) as profit,
		sum(case when business_type_code='1' then sales_value else 0 end) as rp_sales_value,
		sum(case when business_type_code='2' then sales_value else 0 end) as fl_sales_value,
		sum(case when business_type_code='6' then sales_value else 0 end) as bbc_sales_value,
		
		sum(case when business_type_code='1' then profit else 0 end) as rp_profit,
		sum(case when business_type_code='2' then profit else 0 end) as fl_profit,
		sum(case when business_type_code='6' then profit else 0 end) as bbc_profit
	from
		-- csx_dws.csx_dws_sale_detail_di
		csx_dw.dws_sale_r_d_detail
	where 
		sdt>='20220101' and sdt<='20221225'
		and channel_code in('1','7','9')
		and business_type_code in ('1','2','6')
	group by
		substr(sdt,1,6),customer_no	
	) a 
	join
		(
		select distinct 
			month,customer_no,customer_name,region_name,province_name,city_group_name,
			rp_service_user_work_no_new,rp_service_user_name_new,
			fl_service_user_work_no_new,fl_service_user_name_new,
			bbc_service_user_work_no_new,bbc_service_user_name_new,
			work_no_new,sales_name_new
		from
			csx_tmp.report_crm_w_a_customer_service_manager_info_business_new
		where
			month between '202201' and '202212'
		) b on a.customer_no=b.customer_no and a.smonth=b.month
where
	b.rp_service_user_work_no_new is not null
	or b.fl_service_user_work_no_new is not null
	or b.bbc_service_user_work_no_new is not null
group by 
	coalesce(b.rp_service_user_work_no_new,''),
	coalesce(b.rp_service_user_name_new,''),
	coalesce(b.fl_service_user_work_no_new,''),
	coalesce(b.fl_service_user_name_new,''),
	coalesce(b.bbc_service_user_work_no_new,''),
	coalesce(b.bbc_service_user_name_new,'')
order by 
	rp_service_user_work_no_new,
	rp_service_user_name_new,
	fl_service_user_work_no_new,
	fl_service_user_name_new,
	bbc_service_user_work_no_new,
	bbc_service_user_name_new
	
	
	
select
	b.service_user_work_no,
	b.service_user_name,
	a.business_type_name,
	sum(sales_value) as sales_value,
	sum(profit) as profit
from
	(
	select
		substr(sdt,1,6) as smonth,customer_no,business_type_name,
		sum(sales_value) as sales_value,sum(profit) as profit
	from
		-- csx_dws.csx_dws_sale_detail_di
		csx_dw.dws_sale_r_d_detail
	where 
		sdt>='20220101' and sdt<='20221225'
		and channel_code in('1','7','9')
		and business_type_code in ('1','2','6')
	group by
		substr(sdt,1,6),customer_no,business_type_name	
	) a 
	join
		(
		select
			b_tag,month,customer_no,customer_name,service_user_work_no,service_user_name
		from
			(
			-- 日配服务管家
			select distinct 
				'日配业务' as b_tag,month,customer_no,customer_name,
				rp_service_user_work_no_new as service_user_work_no,rp_service_user_name_new as service_user_name
			from
				csx_tmp.report_crm_w_a_customer_service_manager_info_business_new
			where
				month between '202201' and '202212'
				and rp_service_user_work_no_new <>''
			union all
			-- 福利服务管家
			select distinct
				'福利业务' as b_tag,month,customer_no,customer_name,
				fl_service_user_work_no_new as service_user_work_no,fl_service_user_name_new as service_user_name
			from
				csx_tmp.report_crm_w_a_customer_service_manager_info_business_new
			where
				month between '202201' and '202212'
				and fl_service_user_work_no_new <>''
			union all
			-- BBC服务管家
			select distinct
				'BBC' as b_tag,month,customer_no,customer_name,
				bbc_service_user_work_no_new as service_user_work_no,bbc_service_user_name_new as service_user_name
			from
				csx_tmp.report_crm_w_a_customer_service_manager_info_business_new
			where
				month between '202201' and '202212'
				and bbc_service_user_work_no_new <>''
			) a 
		) b on b.customer_no=a.customer_no and b.month=a.smonth and b.b_tag=a.business_type_name
group by 
	b.service_user_work_no,
	b.service_user_name,
	a.business_type_name