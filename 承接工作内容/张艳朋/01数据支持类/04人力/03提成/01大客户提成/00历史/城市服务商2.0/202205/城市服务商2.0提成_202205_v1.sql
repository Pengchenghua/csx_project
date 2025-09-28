--查询城市服务商2.0客户,按库存DC


set i_sdate_1 ='2022-05-31';
set i_sdate_11 ='20220531';
set i_sdate_12 ='20220501';

		
-- 处理客户与供应商对应关系		
drop table csx_tmp.tmp_tc_customer_town_service_provider_info;
create table csx_tmp.tmp_tc_customer_town_service_provider_info
as  
select '115352' as customer_no,'20044273' as town_service_provider_no,'东丰县恒达商贸有限公司(彩食鲜)' as town_service_provider_name
    union all   select '117103' as customer_no,'20044273' as town_service_provider_no,'东丰县恒达商贸有限公司(彩食鲜)' as town_service_provider_name
    union all   select '118411' as customer_no,'20044273' as town_service_provider_no,'东丰县恒达商贸有限公司(彩食鲜)' as town_service_provider_name
    union all   select '119695' as customer_no,'20044273' as town_service_provider_no,'东丰县恒达商贸有限公司(彩食鲜)' as town_service_provider_name
    union all   select '125128' as customer_no,'20044273' as town_service_provider_no,'东丰县恒达商贸有限公司(彩食鲜)' as town_service_provider_name
    union all   select '127372' as customer_no,'20044273' as town_service_provider_no,'东丰县恒达商贸有限公司(彩食鲜)' as town_service_provider_name

	
;


--客户逾期系数
drop table csx_tmp.temp_tc_tsp_cust_over_rate;
create table csx_tmp.temp_tc_tsp_cust_over_rate
as
select 
	a.channel_name,	-- 渠道
	a.customer_no,
	a.customer_name,
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
	csx_dw.dws_sss_r_a_customer_company_accounts a 
where 
	sdt = ${hiveconf:i_sdate_11} --and channel_name = '大客户' 
	and customer_no in ('115352','117103','118411','119695','125128','127372')
group by 	
	a.channel_name,	-- 渠道
	a.customer_no,
	a.customer_name
;

--供应商逾期系数
drop table csx_tmp.temp_tc_tsp_town_service_provider_over_rate;
create table csx_tmp.temp_tc_tsp_town_service_provider_over_rate
as
select 
	a.channel_name,	-- 渠道
	b.town_service_provider_no,
	b.town_service_provider_name,
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
		csx_dw.dws_sss_r_a_customer_company_accounts
	where 
		sdt = ${hiveconf:i_sdate_11} --and channel_name = '大客户' 
		and customer_no in ('115352','117103','118411','119695','125128','127372')
	)a	
	--关联供应商
	left join
		(
		select 
			customer_no,town_service_provider_no,town_service_provider_name
		from 
			csx_tmp.tmp_tc_customer_town_service_provider_info
		group by 
			customer_no,town_service_provider_no,town_service_provider_name
		)b on b.customer_no=a.customer_no  
group by 
	a.channel_name,	-- 渠道
	b.town_service_provider_no,
	b.town_service_provider_name
;



-- 客户提成
insert overwrite directory '/tmp/zhangyanpeng/tc_tsp_customer' row format delimited fields terminated by '\t'
select
	a.s_month,business_type_name,a.region_name,a.province_name,a.customer_no,a.customer_name,c.town_service_provider_no,c.town_service_provider_name,
	a.excluding_tax_sales,a.excluding_tax_profit,a.profit_rate,a.sales_cost,
	coalesce(b.receivable_amount,0) as receivable_amount,
	coalesce(b.overdue_amount,0) as overdue_amount,
	coalesce(b.overdue_coefficient_numerator,0) as overdue_coefficient_numerator,
	coalesce(b.overdue_coefficient_denominator,0) as overdue_coefficient_denominator,
	coalesce(b.over_rate,0) as over_rate
from
	(
	select 
		substr(sdt,1,6) as s_month,business_type_name,region_name,province_name,customer_no,customer_name,
		sum(excluding_tax_sales) excluding_tax_sales,   
		sum(excluding_tax_profit) excluding_tax_profit,  
		sum(sales_cost) as sales_cost,
		sum(excluding_tax_profit)/abs(sum(excluding_tax_sales)) as profit_rate
	from 
		csx_dw.dws_sale_r_d_detail 
	where 
		sdt>=${hiveconf:i_sdate_12} 
		and sdt<=${hiveconf:i_sdate_11}
		and channel_code in ('1', '7', '9')
		and business_type_code ='4'
		and customer_no in ('115352','117103','118411','119695','125128','127372')
	group by 
		substr(sdt,1,6),business_type_name,region_name,province_name,customer_no,customer_name
	) a 
	left join
		(
		select
			customer_no,customer_name,receivable_amount,overdue_amount,overdue_coefficient_numerator,overdue_coefficient_denominator,over_rate 
		from
			csx_tmp.temp_tc_tsp_cust_over_rate
		) b on b.customer_no=a.customer_no	
	--关联供应商
	left join
		(
		select 
			customer_no,town_service_provider_no,town_service_provider_name
		from 
			csx_tmp.tmp_tc_customer_town_service_provider_info
		group by 
			customer_no,town_service_provider_no,town_service_provider_name
		)c on c.customer_no=a.customer_no  		
;



--====================================================================================================================================================================
-- 服务商提成

insert overwrite directory '/tmp/zhangyanpeng/tc_tsp_town_service_provider' row format delimited fields terminated by '\t'

select
	a.s_month,a.business_type_name,a.region_name,a.province_name,b.town_service_provider_no,b.town_service_provider_name,
	a.excluding_tax_sales,a.excluding_tax_profit,a.profit_rate,a.sales_cost,b.overdue_coefficient_numerator,b.overdue_coefficient_denominator,b.over_rate
from
	(
	select 
		a.s_month,a.business_type_name,a.region_name,a.province_name,b.town_service_provider_no,b.town_service_provider_name,
		sum(a.excluding_tax_sales) excluding_tax_sales,   --
		sum(a.excluding_tax_profit) excluding_tax_profit,  --
		sum(sales_cost) as sales_cost,
		sum(a.excluding_tax_profit)/abs(sum(a.excluding_tax_sales)) as profit_rate
	from 
		(
		select 
			region_code,region_name,province_code,province_name,city_group_code,city_group_name,business_type_name,customer_no,
			customer_name,excluding_tax_sales,sales_cost,excluding_tax_profit,front_profit,
			substr(sdt,1,6) as s_month
		from 
			csx_dw.dws_sale_r_d_detail 
		where 
			sdt>=${hiveconf:i_sdate_12} 
			and sdt<=${hiveconf:i_sdate_11}
			and channel_code in ('1', '7', '9')
			and business_type_code ='4'
			and customer_no in ('115352','117103','118411','119695','125128','127372')		
		)a 
		left join
			(
			select
				customer_no,town_service_provider_no,town_service_provider_name
			from
				csx_tmp.tmp_tc_customer_town_service_provider_info
			) b on b.customer_no=a.customer_no	
	group by 
		a.s_month,a.business_type_name,a.region_name,a.province_name,b.town_service_provider_no,b.town_service_provider_name
	) a
	left join
		(
		select
			town_service_provider_no,town_service_provider_name,receivable_amount,overdue_amount,overdue_coefficient_numerator,overdue_coefficient_denominator,over_rate -- 逾期系数 
		from
			csx_tmp.temp_tc_tsp_town_service_provider_over_rate
		) b on b.town_service_provider_no=a.town_service_provider_no
;