-- 对账
insert overwrite directory '/tmp/zhangyanpeng/20220610_01' row format delimited fields terminated by '\t'

select
	substr(a.sdt,1,6) as smonth,
	b.province_name as province_name,
	a.customer_code,
	b.customer_name customer_name,
	a.company_code,
	a.company_name,
	coalesce(b.rp_service_user_work_no_new,'') as rp_service_user_work_no_new,
	coalesce(b.rp_service_user_name_new,'') as rp_service_user_name_new,
	coalesce(b.fl_service_user_work_no_new,'') as fl_service_user_work_no_new,
	coalesce(b.fl_service_user_name_new,'') as fl_service_user_name_new,
	coalesce(b.bbc_service_user_work_no_new,'') as bbc_service_user_work_no_new,
	coalesce(b.bbc_service_user_name_new,'') as bbc_service_user_name_new,
	a.statement_amount,--对账金额
	--a.unstatement_amount,--未对账金额
	-- a.kp_amount,--开票金额
	a.tax_sale_amount,--财务含税销售额
	a.statement_ratio--对账率
	-- a.kp_ratio --开票率	
from
	(
	select
		customer_code,company_code,company_name,
		statement_amount,--对账金额
		kp_amount,--开票金额
		tax_sale_amount,--财务含税销售额
		statement_amount/tax_sale_amount as statement_ratio,
		kp_amount/tax_sale_amount as kp_ratio,
		--statement_ratio,--对账率
		--kp_ratio,--开票率
		sdt
	from
		csx_dw.dws_sss_r_d_customer_settle_detail
	where
		sdt in ('20221110')
	) a 
	join
		(
		select distinct 
			month,customer_no,customer_name,region_name,province_name,city_group_name,
			rp_service_user_work_no_new,rp_service_user_name_new,
			fl_service_user_work_no_new,fl_service_user_name_new,
			bbc_service_user_work_no_new,bbc_service_user_name_new
		from
			csx_tmp.report_crm_w_a_customer_service_manager_info_business_new
		where
			month in ('202211')
			--and (rp_service_user_work_no_new='80955319' or fl_service_user_work_no_new='80955319' or bbc_service_user_work_no_new='80955319')
		) b on a.customer_code=b.customer_no and substr(a.sdt,1,6)=b.month
;

-- 开票
insert overwrite directory '/tmp/zhangyanpeng/20220610_02' row format delimited fields terminated by '\t'

select
	substr(a.sdt,1,6) as smonth,
	b.province_name as province_name,
	a.customer_code,
	b.customer_name customer_name,
	a.company_code,
	a.company_name,
	coalesce(b.rp_service_user_work_no_new,'') as rp_service_user_work_no_new,
	coalesce(b.rp_service_user_name_new,'') as rp_service_user_name_new,
	coalesce(b.fl_service_user_work_no_new,'') as fl_service_user_work_no_new,
	coalesce(b.fl_service_user_name_new,'') as fl_service_user_name_new,
	coalesce(b.bbc_service_user_work_no_new,'') as bbc_service_user_work_no_new,
	coalesce(b.bbc_service_user_name_new,'') as bbc_service_user_name_new,
	--a.statement_amount,--对账金额
	--a.unstatement_amount,--未对账金额
	a.kp_amount,--开票金额
	a.tax_sale_amount,--财务含税销售额
	--a.statement_ratio,--对账率
	a.kp_ratio --开票率	
from
	(
	select
		customer_code,company_code,company_name,
		statement_amount,--对账金额
		kp_amount,--开票金额
		tax_sale_amount,--财务含税销售额
		statement_amount/tax_sale_amount as statement_ratio,
		kp_amount/tax_sale_amount as kp_ratio,
		--statement_ratio,--对账率
		--kp_ratio,--开票率
		sdt
	from
		csx_dw.dws_sss_r_d_customer_settle_detail
	where
		sdt in ('20221115')
	) a 
	join
		(
		select distinct 
			month,customer_no,customer_name,region_name,province_name,city_group_name,
			rp_service_user_work_no_new,rp_service_user_name_new,
			fl_service_user_work_no_new,fl_service_user_name_new,
			bbc_service_user_work_no_new,bbc_service_user_name_new
		from
			csx_tmp.report_crm_w_a_customer_service_manager_info_business_new
		where
			month in ('202211')
			--and (rp_service_user_work_no_new='80955319' or fl_service_user_work_no_new='80955319' or bbc_service_user_work_no_new='80955319')
		) b on a.customer_code=b.customer_no and substr(a.sdt,1,6)=b.month
;
			
-- 开票
insert overwrite directory '/tmp/zhangyanpeng/20220610_03' row format delimited fields terminated by '\t'

select
	substr(a.sdt,1,6) as smonth,
	b.province_name as province_name,
	a.customer_code,
	b.customer_name customer_name,
	a.company_code,
	a.company_name,
	coalesce(b.rp_service_user_work_no_new,'') as rp_service_user_work_no_new,
	coalesce(b.rp_service_user_name_new,'') as rp_service_user_name_new,
	coalesce(b.fl_service_user_work_no_new,'') as fl_service_user_work_no_new,
	coalesce(b.fl_service_user_name_new,'') as fl_service_user_name_new,
	coalesce(b.bbc_service_user_work_no_new,'') as bbc_service_user_work_no_new,
	coalesce(b.bbc_service_user_name_new,'') as bbc_service_user_name_new,
	--a.statement_amount,--对账金额
	--a.unstatement_amount,--未对账金额
	a.kp_amount,--开票金额
	a.tax_sale_amount,--财务含税销售额
	--a.statement_ratio,--对账率
	a.kp_ratio --开票率	
from
	(
	select
		customer_code,company_code,company_name,
		statement_amount,--对账金额
		kp_amount,--开票金额
		tax_sale_amount,--财务含税销售额
		statement_amount/tax_sale_amount as statement_ratio,
		kp_amount/tax_sale_amount as kp_ratio,
		--statement_ratio,--对账率
		--kp_ratio,--开票率
		sdt
	from
		csx_dw.dws_sss_r_d_customer_settle_detail
	where
		sdt in ('20221029')
	) a 
	join
		(
		select distinct 
			month,customer_no,customer_name,region_name,province_name,city_group_name,
			rp_service_user_work_no_new,rp_service_user_name_new,
			fl_service_user_work_no_new,fl_service_user_name_new,
			bbc_service_user_work_no_new,bbc_service_user_name_new
		from
			csx_tmp.report_crm_w_a_customer_service_manager_info_business_new
		where
			month in ('202210')
			--and (rp_service_user_work_no_new='80955319' or fl_service_user_work_no_new='80955319' or bbc_service_user_work_no_new='80955319')
		) b on a.customer_code=b.customer_no and substr(a.sdt,1,6)=b.month
;	

/*
insert overwrite directory '/tmp/zhangyanpeng/20220610_04' row format delimited fields terminated by '\t'

select
	substr(a.sdt,1,6) as smonth,
	b.province_name,
	a.customer_no,
	b.customer_name,
	a.company_code,
	a.company_name,
	-- a.channel_code,
	a.channel_name,
	coalesce(f.rp_service_user_work_no_new,'') as rp_service_user_work_no_new,
	coalesce(f.rp_service_user_name_new,'') as rp_service_user_name_new,
	coalesce(f.fl_service_user_work_no_new,'') as fl_service_user_work_no_new,
	coalesce(f.fl_service_user_name_new,'') as fl_service_user_name_new,
	coalesce(f.bbc_service_user_work_no_new,'') as bbc_service_user_work_no_new,
	coalesce(f.bbc_service_user_name_new,'') as bbc_service_user_name_new,	
	-- a.payment_terms,
	-- a.payment_days,
	-- a.payment_name,
	a.receivable_amount,
	a.overdue_amount,
	-- a.max_overdue_day,
	a.overdue_coefficient_numerator, -- 逾期金额*逾期天数 计算因子，用于计算逾期系数分子
	a.overdue_coefficient_denominator, -- 应收金额*账期天数 计算因子，用于计算逾期系数分母
	a.over_rate
from	
	(
	select
		sdt,
		customer_code as customer_no,
		customer_name,company_code,company_name,channel_code,channel_name,payment_terms,payment_days,payment_name,receivable_amount,overdue_amount,max_overdue_day,
		overdue_coefficient_numerator, -- 逾期金额*逾期天数 计算因子，用于计算逾期系数分子
		overdue_coefficient_denominator, -- 应收金额*账期天数 计算因子，用于计算逾期系数分母
		coalesce(round(case when coalesce(case when receivable_amount>=0 then receivable_amount else 0 end, 0) <= 1 then 0  
			else coalesce(case when overdue_coefficient_numerator>=0 and receivable_amount>0 then overdue_coefficient_numerator else 0 end, 0)
			/(case when overdue_coefficient_denominator>=0 and receivable_amount>0 then overdue_coefficient_denominator else 0 end) end, 6),0) as over_rate -- 逾期系数	
	from
		csx_dw.dws_sss_r_d_customer_settle_detail
		-- csx_dw.dws_sss_r_a_customer_company_accounts
	where
		sdt='20220831'
	) a 
	--剔除业务代理与内购客户
	join		
		(
		select 
			* 
		from 
			csx_dw.dws_crm_w_a_customer 
		where 
			sdt='20220831'
			and (channel_code in('1','7','8'))  ----渠道编号-1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 7.企业购 8.其他 9.业务代理
			and (customer_name not like '%内%购%' and customer_name not like '%临保%')	
		)b on b.customer_no=a.customer_no  
	--剔除当月有城市服务商与批发内购业绩的客户逾期系数
	left join 
		(
		select 
			distinct customer_no 
		from 
			csx_dw.dws_sale_r_d_detail 
		where 
			sdt>='20220801'
			and sdt<='20220831'
			and business_type_code in('3','4')
		)e on e.customer_no=a.customer_no
	join
		(
		select distinct 
			month,customer_no,customer_name,region_name,province_name,city_group_name,
			rp_service_user_work_no_new,rp_service_user_name_new,
			fl_service_user_work_no_new,fl_service_user_name_new,
			bbc_service_user_work_no_new,bbc_service_user_name_new
		from
			csx_tmp.report_crm_w_a_customer_service_manager_info_business_new
		where
			month in ('202208')
			--and (rp_service_user_work_no_new='80955319' or fl_service_user_work_no_new='80955319' or bbc_service_user_work_no_new='80955319')
		) f on a.customer_no=f.customer_no
where
	e.customer_no is null
	and(a.receivable_amount>0 or a.receivable_amount is null)
*/
			