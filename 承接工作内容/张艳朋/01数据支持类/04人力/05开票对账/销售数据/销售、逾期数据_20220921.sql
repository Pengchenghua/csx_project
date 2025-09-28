	select 
		a.smonth,b.province_name,a.customer_no,b.customer_name,
		--b.work_no,b.sales_name,
		b.rp_service_user_work_no,b.rp_service_user_name,b.fl_service_user_work_no,b.fl_service_user_name,b.bbc_service_user_work_no,b.bbc_service_user_name,
		a.business_type_name,a.sales_value,a.profit
	from 
		(
		select 
			customer_no,substr(sdt,1,6) as smonth,business_type_name,
			sum(sales_value) as sales_value,
			sum(profit) as profit
		from 
			csx_dw.dws_sale_r_d_detail
		where 
			sdt>='20220601' and sdt<='20220731'
			and channel_code in('1','7','9')
			and goods_code not in ('8718','8708','8649')
			and business_type_code in('1','2','6')
		group by 
			customer_no,substr(sdt,1,6),business_type_name
		)a 
		--join 
		--	(
		--	select 
		--		substr(sdt,1,6) as smonth,customer_id,customer_no,customer_name,sales_id,work_no,sales_name,province_name
		--	from 
		--		csx_dw.dws_crm_w_a_customer 
		--	where 
		--		sdt in ('20220630','20220731')
		--		and work_no in ('81122598')
		--	)b on b.customer_no=a.customer_no and b.smonth=a.smonth 
		join
			(
			select 
				distinct province_name,customer_id,customer_no,customer_name,month,
				sales_id_new as sales_id,
				work_no_new as work_no,
				sales_name_new as sales_name,
				rp_service_user_id_new as rp_service_user_id,
				rp_service_user_work_no_new as rp_service_user_work_no,
				rp_service_user_name_new as rp_service_user_name,
				fl_service_user_id_new as fl_service_user_id,
				fl_service_user_work_no_new as fl_service_user_work_no,
				fl_service_user_name_new as fl_service_user_name,
				bbc_service_user_id_new as bbc_service_user_id,
				bbc_service_user_work_no_new as bbc_service_user_work_no,
				bbc_service_user_name_new as bbc_service_user_name
			from 
				csx_tmp.report_crm_w_a_customer_service_manager_info_business_new
			where 
				month in ('202206','202207')
				and (rp_service_user_work_no_new='81122598' or fl_service_user_work_no_new='81122598' or bbc_service_user_work_no_new='81122598')
			) b on b.customer_no=a.customer_no and b.month=a.smonth
;


select
	a.smonth,
	d.province_name,	-- 省区
	a.channel_name,	-- 渠道
	a.customer_no,	-- 客户编码
	a.customer_name,	-- 客户名称
	d.rp_service_user_work_no,d.rp_service_user_name,
	d.fl_service_user_work_no,d.fl_service_user_name,
	d.bbc_service_user_work_no,d.bbc_service_user_name,
	a.company_code,	-- 公司代码
	a.company_name,	-- 公司名称,
	case when a.receivable_amount>=0 then a.receivable_amount else 0 end as receivable_amount,	-- 应收金额
	case when a.overdue_amount>=0 and a.receivable_amount>0 then a.overdue_amount else 0 end as overdue_amount,	-- 逾期金额
	case when overdue_coefficient_numerator>=0 and receivable_amount>0 
		then overdue_coefficient_numerator else 0 end as overdue_coefficient_numerator, -- 逾期金额*逾期天数 计算因子，用于计算逾期系数分子
	case when overdue_coefficient_denominator>=0 and receivable_amount>0 
		then overdue_coefficient_denominator else 0 end as overdue_coefficient_denominator, -- 应收金额*账期天数 计算因子，用于计算逾期系数分母
	coalesce(round(case when coalesce(case when a.receivable_amount>=0 then a.receivable_amount else 0 end, 0) <= 1 then 0  
		else coalesce(case when overdue_coefficient_numerator>=0 and a.receivable_amount>0 then overdue_coefficient_numerator else 0 end, 0)
		/(case when overdue_coefficient_denominator>=0 and a.receivable_amount>0 then overdue_coefficient_denominator else 0 end) end, 6),0) as over_rate -- 逾期系数		
from
	(
	select
		customer_code as customer_no,substr(sdt,1,6) as smonth,
		customer_name,company_code,company_name,channel_code,channel_name,payment_terms,payment_days,payment_name,receivable_amount,overdue_amount,max_overdue_day,
		overdue_coefficient_numerator, -- 逾期金额*逾期天数 计算因子，用于计算逾期系数分子
		overdue_coefficient_denominator -- 应收金额*账期天数 计算因子，用于计算逾期系数分母
	from 
		csx_dw.dws_sss_r_d_customer_settle_detail
	where 
		sdt in ('20220630','20220731')
	)a
	--关联客户对应销售员与服务管家
	join		
		(  
		select 
			distinct province_name,customer_id,customer_no,customer_name,month,
			sales_id_new as sales_id,
			work_no_new as work_no,
			sales_name_new as sales_name,
			rp_service_user_id_new as rp_service_user_id,
			rp_service_user_work_no_new as rp_service_user_work_no,
			rp_service_user_name_new as rp_service_user_name,
			fl_service_user_id_new as fl_service_user_id,
			fl_service_user_work_no_new as fl_service_user_work_no,
			fl_service_user_name_new as fl_service_user_name,
			bbc_service_user_id_new as bbc_service_user_id,
			bbc_service_user_work_no_new as bbc_service_user_work_no,
			bbc_service_user_name_new as bbc_service_user_name
		from 
			csx_tmp.report_crm_w_a_customer_service_manager_info_business_new
		where 
			month in ('202206','202207')
			and (rp_service_user_work_no_new='81122598' or fl_service_user_work_no_new='81122598' or bbc_service_user_work_no_new='81122598')
		)d on d.customer_no=a.customer_no and d.month =a.smonth
where 
	a.receivable_amount>0 or a.receivable_amount is null