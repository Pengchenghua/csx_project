
select
	performance_province_name,customer_code,customer_name,business_type_name,inventory_dc_name,sum(sale_amt) sale_amt
from
	-- csx_dw.dws_sale_r_d_detail 
	csx_dws.csx_dws_sale_detail_di
where
	sdt>='20230501' 
	and sdt<='20230531'
	and channel_code in ('1', '7', '9')
	and business_type_code=4
	and inventory_dc_name like '%V2DC%'
	and performance_province_name='福建省'
group by
	performance_province_name,customer_code,customer_name,business_type_name,inventory_dc_name
;

		
-- 处理客户城市	
drop table if exists csx_analyse_tmp.csx_analyse_tmp_tc_tsp_customer_info;
create table csx_analyse_tmp.csx_analyse_tmp_tc_tsp_customer_info
as  
select
	customer_code,customer_name,performance_province_name,sales_person_cityname
from
	(
	select 
		customer_code,customer_name,performance_province_name,
		case when customer_code in ('125739','125763','125736','125547','125741','129722','124803') then '福清'
			when customer_code in ('128845','128818','128799','128840','128759','128836','128835','128849','128841') then '宁德'
			when customer_code in ('129797','131497','128724','131457','123247','123242','130784','129787','130114','123253','122269') then '泉州'
			when customer_code in ('122328','126178','122338','129984','124198','122339','124225','122327','127573','128157','123213','128906','223729') then '长乐'
		end as sales_person_cityname
	from 
		-- csx_dw.dws_crm_w_a_customer
		csx_dim.csx_dim_crm_customer_info
	where 
		sdt = '20230531' 
		and customer_code in ('125739','125763','125736','125547','125741','129722','124803','128845','128818','128799','128840','128759','128836','128835','128849','128841',
		'129797','131497','128724','131457','123247','123242','130784','129787','130114','123253','122269','122328','126178','122338','129984','124198','122339','124225',
		'122327','127573','128157','123213','128906','223729'
		)
	) a 
where
	sales_person_cityname is not null
;

-- 获取最近6个月履约过项目供应商业绩的客户 计算逾期系数用
-- drop table if exists csx_analyse_tmp.csx_analyse_tmp_tc_tsp_customer_last_one_year;
-- create table csx_analyse_tmp.csx_analyse_tmp_tc_tsp_customer_last_one_year
-- as 
-- select
-- 	customer_code
-- from
-- 	-- csx_dw.dws_sale_r_d_detail 
-- 	csx_dws.csx_dws_sale_detail_di
-- where
-- 	sdt>='20220301' -- 最近一年
-- 	and sdt<='20230531'
-- 	and channel_code in ('1', '7', '9')
-- 	and business_type_code=4
-- 	and inventory_dc_name like '%V2DC%'
-- 	and performance_province_name='福建省'
-- group by
-- 	customer_code
-- ;


-- 查询结果集
--计算逾期系数

select 
	--a.channel_name,	-- 渠道
	b.performance_province_name,	-- 省区
	b.sales_person_cityname,
	a.customer_code,	-- 客户编码
	a.customer_name,	-- 客户名称
	a.account_period_code,	-- 账期编码
	a.account_period_value,	-- 帐期天数
	a.account_period_name,	-- 账期名称
	a.company_code,	-- 公司代码
	a.company_name,	-- 公司名称
	a.receivable_amount,	-- 应收金额
	a.overdue_amount,	-- 逾期金额
	a.overdue_coefficient_numerator,	-- 逾期金额*逾期天数
	a.overdue_coefficient_denominator,	-- 应收金额*帐期天数
	a.overdue_rate
from
	(
	select 
		* 
	from 
		-- csx_dw.dws_sss_r_a_customer_company_accounts
		csx_dws.csx_dws_sss_customer_invoice_bill_settle_stat_di
	where 
		sdt = '20230531' --and channel_name = '大客户' 
	)a
	join		 
		(
		select 
			*
		from 
			csx_analyse_tmp.csx_analyse_tmp_tc_tsp_customer_info
		)b on b.customer_code=a.customer_code   
;



--客户逾期系数
drop table if exists csx_analyse_tmp.csx_analyse_tmp_tc_tsp_cust_over_rate;
create table csx_analyse_tmp.csx_analyse_tmp_tc_tsp_cust_over_rate
as
select 
	--a.channel_name,	-- 渠道
	a.customer_code,	-- 客户编码
	a.customer_name,	-- 客户名称
	sum(case when receivable_amount>=0 then receivable_amount else 0 end) receivable_amount,	-- 应收金额
	sum(case when overdue_amount>=0 and receivable_amount>0 then overdue_amount else 0 end) overdue_amount,	-- 逾期金额
	sum(case when overdue_coefficient_numerator>=0 and receivable_amount>0 
		then overdue_coefficient_numerator else 0 end) as overdue_coefficient_numerator,	-- 逾期金额*逾期天数
	sum(case when overdue_coefficient_denominator>=0 and receivable_amount>0 
		then overdue_coefficient_denominator else 0 end) overdue_coefficient_denominator,	-- 应收金额*帐期天数	
	coalesce(round(
		case when coalesce(sum(case when receivable_amount>=0 then receivable_amount else 0 end), 0) <= 1 then 0  
		else coalesce(sum(case when overdue_coefficient_numerator>=0 and receivable_amount>0 then overdue_coefficient_numerator else 0 end), 0)
		/(sum(case when overdue_coefficient_denominator>=0  and receivable_amount>0 then overdue_coefficient_denominator else 0 end)) end, 6),0) as over_rate -- 逾期系数 	    
from
	(
	select 
		* 
	from 
		-- csx_dw.dws_sss_r_a_customer_company_accounts
		csx_dws.csx_dws_sss_customer_invoice_bill_settle_stat_di
	where 
		sdt = '20230531' --and channel_name = '大客户' 
	)a
	join		 
		(
		select 
			* 
		from 
			csx_analyse_tmp.csx_analyse_tmp_tc_tsp_customer_info
		)b on b.customer_code=a.customer_code 
group by
	a.customer_code,	-- 客户编码
	a.customer_name	-- 客户名称			
;

--城市逾期系数
drop table csx_analyse_tmp.csx_analyse_tmp_tc_tsp_town_service_provider_over_rate;
create table csx_analyse_tmp.csx_analyse_tmp_tc_tsp_town_service_provider_over_rate
as
select 
	--a.channel_name,	-- 渠道
	b.sales_person_cityname,	-- 客户编码
	sum(case when receivable_amount>=0 then receivable_amount else 0 end) receivable_amount,	-- 应收金额
	sum(case when overdue_amount>=0 and receivable_amount>0 then overdue_amount else 0 end) overdue_amount,	-- 逾期金额
	sum(case when overdue_coefficient_numerator>=0 and receivable_amount>0 
		then overdue_coefficient_numerator else 0 end) as overdue_coefficient_numerator,	-- 逾期金额*逾期天数
	sum(case when overdue_coefficient_denominator>=0 and receivable_amount>0 
		then overdue_coefficient_denominator else 0 end) overdue_coefficient_denominator,	-- 应收金额*帐期天数	
	coalesce(round(
		case when coalesce(sum(case when receivable_amount>=0 then receivable_amount else 0 end), 0) <= 1 then 0  
		else coalesce(sum(case when overdue_coefficient_numerator>=0 and receivable_amount>0 then overdue_coefficient_numerator else 0 end), 0)
		/(sum(case when overdue_coefficient_denominator>=0  and receivable_amount>0 then overdue_coefficient_denominator else 0 end)) end, 6),0) as over_rate -- 逾期系数 	    
from
	(
	select 
		* 
	from 
		-- csx_dw.dws_sss_r_a_customer_company_accounts
		csx_dws.csx_dws_sss_customer_invoice_bill_settle_stat_di
	where 
		sdt = '20230531' --and channel_name = '大客户' 
	)a
	join		 
		(
		select 
			* 
		from 
			csx_analyse_tmp.csx_analyse_tmp_tc_tsp_customer_info
		)b on b.customer_code=a.customer_code 
group by
	b.sales_person_cityname
;



-- 客户提成

select
	a.s_month,a.performance_region_name,a.performance_province_name,c.sales_person_cityname,a.customer_code,a.customer_name,
	a.sale_amt_no_tax,a.profit_no_tax,a.profit_rate,a.sale_cost,
	coalesce(b.receivable_amount,0) as receivable_amount,
	coalesce(b.overdue_amount,0) as overdue_amount,
	coalesce(b.overdue_coefficient_numerator,0) as overdue_coefficient_numerator,
	coalesce(b.overdue_coefficient_denominator,0) as overdue_coefficient_denominator,
	coalesce(b.over_rate,0) as over_rate
from
	(
	select
		s_month,performance_region_name,performance_province_name,customer_code,customer_name,sign_company_code,
		-- 202303签呈 2121主体客户毛利低于8个点不计算销售额和毛利额，正常计算销售成本。其他主体低于7个点也不计算销售额和毛利额，正常计算销售成本
		if((sign_company_code='2121' and profit_rate<0.0795) or profit_rate<0.0695,0,sale_amt_no_tax) as sale_amt_no_tax, 
		if((sign_company_code='2121' and profit_rate<0.0795) or profit_rate<0.0695,0,profit_no_tax) as profit_no_tax, 
		sale_cost,
		if((sign_company_code='2121' and profit_rate<0.0795) or profit_rate<0.0695,0,profit_rate) as profit_rate
	from
		(
		select 
			substr(sdt,1,6) as s_month,performance_region_name,performance_province_name,customer_code,customer_name,sign_company_code,
			sum(sale_amt_no_tax) sale_amt_no_tax,   
			sum(profit_no_tax) profit_no_tax,  
			sum(sale_cost) as sale_cost,
			sum(profit_no_tax)/abs(sum(sale_amt_no_tax)) as profit_rate
		from 
			csx_dws.csx_dws_sale_detail_di
		where 
			sdt>='20230501' 
			and sdt<='20230531'
			and channel_code in ('1', '7', '9')
			and business_type_code=4
			and inventory_dc_name like '%V2DC%'
		group by 
			substr(sdt,1,6),performance_region_name,performance_province_name,customer_code,customer_name,sign_company_code
		) a 
	) a 
	left join
		(
		select
			customer_code,receivable_amount,overdue_amount,overdue_coefficient_numerator,overdue_coefficient_denominator,over_rate
		from
			csx_analyse_tmp.csx_analyse_tmp_tc_tsp_cust_over_rate
		) b on b.customer_code=a.customer_code	
	--关联城市
	join
		(
		select 
			*
		from 
			csx_analyse_tmp.csx_analyse_tmp_tc_tsp_customer_info
		)c on c.customer_code=a.customer_code  		
;



--====================================================================================================================================================================
-- 城市提成

select
	a.s_month,a.performance_region_name,a.performance_province_name,b.sales_person_cityname,
	a.sale_amt_no_tax,a.profit_no_tax,a.profit_rate,a.sale_cost,
	coalesce(b.overdue_coefficient_numerator,0) as overdue_coefficient_numerator,
	coalesce(b.overdue_coefficient_denominator,0) as overdue_coefficient_denominator,
	coalesce(b.over_rate,0) as over_rate
from
	(
	select 
		a.s_month,a.performance_region_name,a.performance_province_name,b.sales_person_cityname,
		sum(a.sale_amt_no_tax) sale_amt_no_tax,   --
		sum(a.profit_no_tax) profit_no_tax,  --
		sum(sale_cost) as sale_cost,
		sum(a.profit_no_tax)/abs(sum(a.sale_amt_no_tax)) as profit_rate
	from 
		(
		select
			s_month,performance_region_name,performance_province_name,customer_code,customer_name,sign_company_code,
			-- 202303签呈 2121主体客户毛利低于8个点不计算销售额和毛利额，正常计算销售成本。其他主体低于7个点也不计算销售额和毛利额，正常计算销售成本
			if((sign_company_code='2121' and profit_rate<0.0795) or profit_rate<0.0695,0,sale_amt_no_tax) as sale_amt_no_tax, 
			if((sign_company_code='2121' and profit_rate<0.0795) or profit_rate<0.0695,0,profit_no_tax) as profit_no_tax, 
			sale_cost,
			if((sign_company_code='2121' and profit_rate<0.0795) or profit_rate<0.0695,0,profit_rate) as profit_rate
		from
			(
			select 
				substr(sdt,1,6) as s_month,performance_region_name,performance_province_name,customer_code,customer_name,sign_company_code,
				sum(sale_amt_no_tax) sale_amt_no_tax,   
				sum(profit_no_tax) profit_no_tax,  
				sum(sale_cost) as sale_cost,
				sum(profit_no_tax)/abs(sum(sale_amt_no_tax)) as profit_rate
			from 
				csx_dws.csx_dws_sale_detail_di
			where 
				sdt>='20230501' 
				and sdt<='20230531'
				and channel_code in ('1', '7', '9')
				and business_type_code=4
				and inventory_dc_name like '%V2DC%'
			group by 
				substr(sdt,1,6),performance_region_name,performance_province_name,customer_code,customer_name,sign_company_code
			) a 
		)a 
		join
			(
			select
				*
			from
				csx_analyse_tmp.csx_analyse_tmp_tc_tsp_customer_info
			) b on b.customer_code=a.customer_code	
	group by 
		a.s_month,a.performance_region_name,a.performance_province_name,b.sales_person_cityname
	) a
	left join
		(
		select
			sales_person_cityname,overdue_coefficient_numerator,overdue_coefficient_denominator,over_rate
		from
			csx_analyse_tmp.csx_analyse_tmp_tc_tsp_town_service_provider_over_rate
		) b on b.sales_person_cityname=a.sales_person_cityname
;