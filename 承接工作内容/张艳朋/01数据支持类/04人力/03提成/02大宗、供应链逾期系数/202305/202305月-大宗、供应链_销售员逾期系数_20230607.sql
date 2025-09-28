

--客户应收金额、逾期金额
drop table if exists csx_analyse_tmp.csx_analyse_tmp_tc_cust_overdue_0;
create table csx_analyse_tmp.csx_analyse_tmp_tc_cust_overdue_0
as
select
	a.sdt,
	a.customer_code,
	a.customer_name,a.company_code,a.company_name,a.channel_code,a.channel_name,a.account_period_code,a.account_period_value,a.account_period_name,a.receivable_amount,a.overdue_amount,a.max_overdue_day,
	a.overdue_coefficient_numerator, -- 逾期金额*逾期天数 计算因子，用于计算逾期系数分子
	a.overdue_coefficient_denominator -- 应收金额*账期天数 计算因子，用于计算逾期系数分母
from	
	(
	select
		sdt,
		customer_code,
		customer_name,company_code,company_name,channel_code,channel_name,account_period_code,account_period_value,account_period_name,receivable_amount,overdue_amount,max_overdue_day,
		overdue_coefficient_numerator, -- 逾期金额*逾期天数 计算因子，用于计算逾期系数分子
		overdue_coefficient_denominator -- 应收金额*账期天数 计算因子，用于计算逾期系数分母
	from
		--csx_dw.dws_sss_r_d_customer_settle_detail
		-- csx_dw.dws_sss_r_a_customer_company_accounts
		csx_dws.csx_dws_sss_customer_invoice_bill_settle_stat_di
	where
		sdt='20230531'	
	) a 
	left join 
		(
		select 
			customer_code 
		from 
			-- csx_dw.dws_sale_r_d_detail 
			csx_dws.csx_dws_sale_detail_di
		where 
			sdt>='20230501' 
			and sdt<='20230531' 
			and business_type_code in(3,4)
		group by 
			customer_code
		)e on e.customer_code=a.customer_code
where
	e.customer_code is null
;

select 
	a.channel_name,	-- 渠道
	b.performance_province_name,	-- 省区
	a.customer_code,	-- 客户编码
	a.customer_name,	-- 客户名称
	b.sales_user_number,	-- 销售员工号
	b.sales_user_name,	-- 销售员
	a.account_period_code,	-- 账期编码
	a.account_period_value,	-- 帐期天数
	a.account_period_name,	-- 账期名称
	a.company_code,	-- 公司代码
	a.company_name,	-- 公司名称
	a.receivable_amount,	-- 应收金额
	a.overdue_amount,	-- 逾期金额
	overdue_coefficient_numerator, -- 逾期金额*逾期天数 计算因子，用于计算逾期系数分子
	overdue_coefficient_denominator, -- 应收金额*账期天数 计算因子，用于计算逾期系数分母
	if(overdue_coefficient_numerator/overdue_coefficient_denominator<0,0,overdue_coefficient_numerator/overdue_coefficient_denominator) as over_rate -- 逾期系数			    
from
	(
	select
		customer_code,
		customer_name,company_code,company_name,channel_code,channel_name,account_period_code,account_period_value,account_period_name,receivable_amount,overdue_amount,max_overdue_day,
		overdue_coefficient_numerator, -- 逾期金额*逾期天数 计算因子，用于计算逾期系数分子
		overdue_coefficient_denominator -- 应收金额*账期天数 计算因子，用于计算逾期系数分母		
	from 
		csx_analyse_tmp.csx_analyse_tmp_tc_cust_overdue_0
	where 
		(channel_name like '大宗%' or channel_name like '%供应链%')
		and sdt ='20230531' 
	)a
	join		 
		(
		select 
			* 
		from 
			-- csx_dw.dws_crm_w_a_customer 
			csx_dim.csx_dim_crm_customer_info
		where 
			sdt='20230531' 
			and channel_code in('4','5','6') ----渠道编号-1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 7.企业购 8.其他 9.业务代理
		)b on b.customer_code=a.customer_code 
where
	(a.receivable_amount>0 or a.receivable_amount is null)
;

