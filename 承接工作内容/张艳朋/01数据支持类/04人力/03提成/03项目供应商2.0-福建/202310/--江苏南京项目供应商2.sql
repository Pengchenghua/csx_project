--江苏南京项目供应商2.0

-- 逾期金额

with csx_analyse_tmp_tc_tsp_customer_info as 
(select
	performance_province_name,customer_code,customer_name,business_type_name,inventory_dc_code,inventory_dc_name,sum(sale_amt) sale_amt ,
	case  when city_name='福州市' then substr(town_name ,1,length(town_name)-1) else  substr(city_name ,1,length(city_name)-1)  end  as sales_person_cityname
from
	csx_dws.csx_dws_sale_detail_di a 
	join 
	(select shop_code,performance_city_code,performance_city_name,basic_performance_city_name,city_name,town_name      from    csx_dim.csx_dim_shop where sdt='current') b on a.inventory_dc_code=b.shop_code
where
	sdt>='20231101' 
	and sdt<='20231130'
	and channel_code in ('1', '7', '9')
	and business_type_code=4
	and inventory_dc_name like '%V2DC%'
	and performance_province_name='江苏南京'
group by
	performance_province_name,customer_code,customer_name,business_type_name,inventory_dc_name,inventory_dc_code, 
case  when city_name='福州市' then substr(town_name ,1,length(town_name)-1) else  substr(city_name ,1,length(city_name)-1)  end
)
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
		csx_dws.csx_dws_sss_customer_invoice_bill_settle_stat_di
	where 
		sdt = '20231031' --and channel_name = '大客户' 
	)a
	join		 
		(
		select 
			*
		from 
			csx_analyse_tmp_tc_tsp_customer_info
		)b on b.customer_code=a.customer_code   
;



-- 城市归属处理
drop table  csx_analyse_tmp.csx_analyse_tmp_tc_tsp_customer_info ;
create table csx_analyse_tmp.csx_analyse_tmp_tc_tsp_customer_info as 
select
	performance_province_name,customer_code,customer_name,business_type_name,inventory_dc_code,inventory_dc_name,sum(sale_amt) sale_amt ,
	case  when city_name='福州市' then substr(town_name ,1,length(town_name)-1) else  substr(city_name ,1,length(city_name)-1)  end  as sales_person_cityname
from
	csx_dws.csx_dws_sale_detail_di a 
	join 
	(select shop_code,performance_city_code,performance_city_name,basic_performance_city_name,city_name,town_name      from    csx_dim.csx_dim_shop where sdt='current') b on a.inventory_dc_code=b.shop_code
where
	sdt>='20231101' 
	and sdt<='20231130'
	and channel_code in ('1', '7', '9')
	and business_type_code=4
	and inventory_dc_name like '%V2DC%'
	and performance_province_name !='福建省'
group by
	performance_province_name,customer_code,customer_name,business_type_name,inventory_dc_name,inventory_dc_code, 
case  when city_name='福州市' then substr(town_name ,1,length(town_name)-1) else  substr(city_name ,1,length(city_name)-1)  end
;



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
		csx_dws.csx_dws_sss_customer_invoice_bill_settle_stat_di
	where 
		sdt = '20231130' --and channel_name = '大客户' 
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
		csx_dws.csx_dws_sss_customer_invoice_bill_settle_stat_di
	where 
		sdt = '20231130' --and channel_name = '大客户' 
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
		-- 202303签呈 2121主体客户毛利低于8个点不计算销售额和毛利额，2115 不低于7% 不参与计算，其他正常计算销售成本正常计算销售成本
		case when  (sign_company_code='2121' and profit_rate<0.0795) or (sign_company_code='2121' and profit_rate<0.0695) then 0
			     when  (sign_company_code='2121' and profit_rate>0.0795) or (sign_company_code='2121' and profit_rate>0.0695) then sale_amt_no_tax
			 else  sale_amt_no_tax end  as sale_amt_no_tax, 
			case when  (sign_company_code='2121' and profit_rate<0.0795) or (sign_company_code='2121' and profit_rate<0.0695) then 0
			    when  (sign_company_code='2121' and profit_rate>0.0795) or (sign_company_code='2121' and profit_rate>0.0695) then profit_no_tax
			    else  profit_no_tax end  as profit_no_tax, 
			sale_cost,
			 case when  (sign_company_code='2121' and profit_rate<0.0795) or (sign_company_code='2121' and profit_rate<0.0695) then 0
			      when  (sign_company_code='2121' and profit_rate>0.0795) or (sign_company_code='2121' and profit_rate>0.0695) then profit_rate
			  else  profit_rate end  as profit_rate
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
			sdt>='20231101' 
			and sdt<='20231130'
			and channel_code in ('1', '7', '9')
			and business_type_code=4
		--	and customer_code='225541'
		--	and inventory_dc_name like '%V2DC%'
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
			distinct customer_code,sales_person_cityname
		from 
			csx_analyse_tmp.csx_analyse_tmp_tc_tsp_customer_info 
			-- where customer_code='225541'
		)c on c.customer_code=a.customer_code  
		-- and a.inventory_dc_code=c.inventory_dc_code	
;




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
			-- 福建计算方法 202303签呈 2121主体客户毛利低于8个点不计算销售额和毛利额，正常计算销售成本。其他主体低于7个点也不计算销售额和毛利额，正常计算销售成本
			-- 福建按照
			case when  (sign_company_code='2121' and profit_rate<0.0795) or (sign_company_code='2121' and profit_rate<0.0695) then 0
			     when  (sign_company_code='2121' and profit_rate>0.0795) or (sign_company_code='2121' and profit_rate>0.0695) then sale_amt_no_tax
			 else  sale_amt_no_tax end  as sale_amt_no_tax, 
			case when  (sign_company_code='2121' and profit_rate<0.0795) or (sign_company_code='2121' and profit_rate<0.0695) then 0
			    when  (sign_company_code='2121' and profit_rate>0.0795) or (sign_company_code='2121' and profit_rate>0.0695) then profit_no_tax
			    else  profit_no_tax end  as profit_no_tax, 
			sale_cost,
			 case when  (sign_company_code='2121' and profit_rate<0.0795) or (sign_company_code='2121' and profit_rate<0.0695) then 0
			      when  (sign_company_code='2121' and profit_rate>0.0795) or (sign_company_code='2121' and profit_rate>0.0695) then profit_rate
			  else  profit_rate end  as profit_rate
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
				sdt>='20231101' 
				and sdt<='20231130'
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
					distinct customer_code,sales_person_cityname
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
