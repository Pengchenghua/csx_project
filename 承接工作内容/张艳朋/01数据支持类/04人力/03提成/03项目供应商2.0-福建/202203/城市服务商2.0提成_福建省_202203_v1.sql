--查询城市服务商2.0客户,按库存DC


set i_sdate_1 ='2022-03-31';
set i_sdate_11 ='20220331';
set i_sdate_12 ='20220301';

		
-- 处理客户城市	
drop table csx_tmp.tmp_tc_tsp_customer_info;
create table csx_tmp.tmp_tc_tsp_customer_info
as  
select
	customer_no,customer_name,work_no,sales_name,sales_region_code,sales_region_name,province_name,sales_person_cityname
from
	(
	select 
		customer_no,customer_name,work_no,sales_name,sales_region_code,sales_region_name,province_name,
		case when customer_no in ('122267','125739','125547','126056','123133','125763','125688','122697','112291','125741','124803') then '福清'
			when customer_no in ('115721','122286','122106','125508','123813','120100','126267','119703','122125','122375') then '永安'
			when customer_no in ('122339','124225','121146','122338','122327','123213','122328') then '长乐'
		end as sales_person_cityname
	from 
		csx_dw.dws_crm_w_a_customer
	where 
		sdt = ${hiveconf:i_sdate_11} 
		and customer_no in ('122267','125739','125547','126056','123133','125763','125688','122697','112291','125741','124803','115721','122286','122106','125508','123813',
		'120100','126267','119703','122125','122375','122339','124225','121146','122338','122327','123213','122328')
	) a 
where
	sales_person_cityname is not null
;


-- 查询结果集
--计算逾期系数
insert overwrite directory '/tmp/zhangyanpeng/yuqi_town_service_provider' row format delimited fields terminated by '\t'
select 
	--a.channel_name,	-- 渠道
	b.province_name,	-- 省区
	b.sales_person_cityname,
	a.customer_no,	-- 客户编码
	a.customer_name,	-- 客户名称
	a.payment_terms,	-- 账期编码
	a.payment_days,	-- 帐期天数
	a.payment_name,	-- 账期名称
	a.company_code,	-- 公司代码
	a.company_name,	-- 公司名称
	a.receivable_amount,	-- 应收金额
	a.overdue_amount,	-- 逾期金额
	a.overdue_coefficient_numerator,	-- 逾期金额*逾期天数
	a.overdue_coefficient_denominator,	-- 应收金额*帐期天数	
    a.overdue_coefficient		    
from
	(
	select 
		* 
	from 
		csx_dw.dws_sss_r_a_customer_company_accounts
	where 
		sdt = ${hiveconf:i_sdate_11} --and channel_name = '大客户' 
	)a
	join		 
		(
		select 
			* 
		from 
			csx_tmp.tmp_tc_tsp_customer_info
		)b on b.customer_no=a.customer_no   
;



--客户逾期系数
drop table csx_tmp.temp_tc_tsp_cust_over_rate;
create table csx_tmp.temp_tc_tsp_cust_over_rate
as
select 
	--a.channel_name,	-- 渠道
	a.customer_no,	-- 客户编码
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
		csx_dw.dws_sss_r_a_customer_company_accounts
	where 
		sdt = ${hiveconf:i_sdate_11} --and channel_name = '大客户' 
	)a
	join		 
		(
		select 
			* 
		from 
			csx_tmp.tmp_tc_tsp_customer_info
		)b on b.customer_no=a.customer_no 
group by
	a.customer_no,	-- 客户编码
	a.customer_name	-- 客户名称			
;

--城市逾期系数
drop table csx_tmp.temp_tc_tsp_town_service_provider_over_rate;
create table csx_tmp.temp_tc_tsp_town_service_provider_over_rate
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
		csx_dw.dws_sss_r_a_customer_company_accounts
	where 
		sdt = ${hiveconf:i_sdate_11} --and channel_name = '大客户' 
	)a
	join		 
		(
		select 
			* 
		from 
			csx_tmp.tmp_tc_tsp_customer_info
		)b on b.customer_no=a.customer_no 
group by
	b.sales_person_cityname
;



-- 客户提成
insert overwrite directory '/tmp/zhangyanpeng/tc_tsp_customer' row format delimited fields terminated by '\t'
select
	a.s_month,a.region_name,a.province_name,c.sales_person_cityname,a.customer_no,a.customer_name,
	a.excluding_tax_sales,a.excluding_tax_profit,a.profit_rate,a.sales_cost,
	coalesce(b.receivable_amount,0) as receivable_amount,
	coalesce(b.overdue_amount,0) as overdue_amount,
	coalesce(b.overdue_coefficient_numerator,0) as overdue_coefficient_numerator,
	coalesce(b.overdue_coefficient_denominator,0) as overdue_coefficient_denominator,
	coalesce(b.over_rate,0) as over_rate
from
	(
	select 
		substr(sdt,1,6) as s_month,region_name,province_name,customer_no,customer_name,
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
		and dc_name like '%V2DC%'
	group by 
		substr(sdt,1,6),region_name,province_name,customer_no,customer_name
	) a 
	left join
		(
		select
			customer_no,receivable_amount,overdue_amount,overdue_coefficient_numerator,overdue_coefficient_denominator,over_rate
		from
			csx_tmp.temp_tc_tsp_cust_over_rate
		) b on b.customer_no=a.customer_no	
	--关联城市
	join
		(
		select 
			*
		from 
			csx_tmp.tmp_tc_tsp_customer_info
		)c on c.customer_no=a.customer_no  		
;



--====================================================================================================================================================================
-- 城市提成

insert overwrite directory '/tmp/zhangyanpeng/tc_tsp_town_service_provider' row format delimited fields terminated by '\t'

select
	a.s_month,a.region_name,a.province_name,b.sales_person_cityname,
	a.excluding_tax_sales,a.excluding_tax_profit,a.profit_rate,a.sales_cost,
	coalesce(b.overdue_coefficient_numerator,0) as overdue_coefficient_numerator,
	coalesce(b.overdue_coefficient_denominator,0) as overdue_coefficient_denominator,
	coalesce(b.over_rate,0) as over_rate
from
	(
	select 
		a.s_month,a.region_name,a.province_name,b.sales_person_cityname,
		sum(a.excluding_tax_sales) excluding_tax_sales,   --
		sum(a.excluding_tax_profit) excluding_tax_profit,  --
		sum(sales_cost) as sales_cost,
		sum(a.excluding_tax_profit)/abs(sum(a.excluding_tax_sales)) as profit_rate
	from 
		(
		select 
			region_code,region_name,province_code,province_name,city_group_code,city_group_name,customer_no,
			customer_name,excluding_tax_sales,sales_cost,excluding_tax_profit,front_profit,
			substr(sdt,1,6) as s_month
		from 
			csx_dw.dws_sale_r_d_detail 
		where 
			sdt>=${hiveconf:i_sdate_12} 
			and sdt<=${hiveconf:i_sdate_11}
			and channel_code in ('1', '7', '9')
			and dc_name like '%V2DC%'
		)a 
		join
			(
			select
				*
			from
				csx_tmp.tmp_tc_tsp_customer_info
			) b on b.customer_no=a.customer_no	
	group by 
		a.s_month,a.region_name,a.province_name,b.sales_person_cityname
	) a
	left join
		(
		select
			sales_person_cityname,overdue_coefficient_numerator,overdue_coefficient_denominator,over_rate
		from
			csx_tmp.temp_tc_tsp_town_service_provider_over_rate
		) b on b.sales_person_cityname=a.sales_person_cityname
;