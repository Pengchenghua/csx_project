drop table if exists csx_analyse_tmp.csx_analyse_tmp_finance_customer_sale_20230130;
create table csx_analyse_tmp.csx_analyse_tmp_finance_customer_sale_20230130
as
select
	b.performance_province_name,a.customer_code,b.customer_name,a.channel_name,c.quarter_of_year,d.first_sale_yearmonth,
	count(distinct a.sdt) as day_frequency,
	if(d.first_sale_quarter_of_year=c.quarter_of_year,'新客','老客') as customer_flag_q,
	if(substr(d.first_sale_quarter_of_year,1,4)=substr(c.quarter_of_year,1,4),'新客','老客') as customer_flag_y,
	a.business_type_name,
	b.first_category_name,
	b.second_category_name,
	b.third_category_name,
	sum(a.sale_amt) as sale_amt,
	sum(a.sale_amt_no_tax) as sale_amt_no_tax,
	sum(a.profit) as profit,
	sum(a.profit_no_tax) as profit_no_tax
from
	(
	select
		sdt,customer_code,channel_name,business_type_name,sale_amt,sale_amt_no_tax,profit,profit_no_tax
	from
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt>='20210101' and sdt<='20221231'
		and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
	) a 
	left join
		(
		select
			customer_code,customer_name,performance_region_name,performance_province_name,performance_city_name,first_category_name,second_category_name,third_category_name,sign_company_code,
			sales_user_number,sales_user_name,customer_address_full
		from
			csx_dim.csx_dim_crm_customer_info
		where
			sdt='current'
		) b on b.customer_code=a.customer_code
	left join
		(
		select
			calday,quarter_of_year
		from
			csx_dim.csx_dim_basic_date
		) c on c.calday=a.sdt
	left join
		(
		select
			t1.customer_code,t1.first_sale_date,t1.first_sale_yearmonth,t2.quarter_of_year as first_sale_quarter_of_year
		from
			(
			select
				customer_code,first_sale_date,substr(first_sale_date,1,6) as first_sale_yearmonth
			from
				csx_dws.csx_dws_crm_customer_active_di
			where
				sdt='current'
			) t1
			left join
				(
				select
					calday,quarter_of_year
				from
					csx_dim.csx_dim_basic_date
				) t2 on t2.calday=t1.first_sale_date				
		) d on d.customer_code=a.customer_code
group by 
	b.performance_province_name,a.customer_code,b.customer_name,a.channel_name,c.quarter_of_year,d.first_sale_yearmonth,
	if(d.first_sale_quarter_of_year=c.quarter_of_year,'新客','老客'),
	if(substr(d.first_sale_quarter_of_year,1,4)=substr(c.quarter_of_year,1,4),'新客','老客'),
	a.business_type_name,
	b.first_category_name,
	b.second_category_name,
	b.third_category_name
;
select * from csx_analyse_tmp.csx_analyse_tmp_finance_customer_sale_20230130

-- 验数
	select
		sum(sale_amt)
	from
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt>='20210101' and sdt<='20221231'
		and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
		
	select
		substr(sdt,1,4) as syear,
		is_factory_goods_flag,	
		sum(sale_amt_no_tax) as sale_amt_no_tax,
		sum(profit_no_tax) as profit_no_tax
	from
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt>='20210101' and sdt<='20221231'
		and channel_code in('1','7','9','2') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
		and business_type_code !=4
	group by  
		substr(sdt,1,4),is_factory_goods_flag
;
-- 月度
drop table if exists csx_analyse_tmp.csx_analyse_tmp_finance_customer_customer_small_level_20230202;
create table csx_analyse_tmp.csx_analyse_tmp_finance_customer_customer_small_level_20230202
as
select
	smonth,customer_code,sale_amt,profit,sale_amt_no_tax,profit_no_tax,profit_rate,
	case when sale_amt<1000  or  sale_amt/cust_sales_cnt<500 then 'E3' 
		when  sale_amt>=300000 and profit_rate>=0.2  then  'A1'
		when  sale_amt>=50000  and sale_amt<300000 and profit_rate>=0.2  then 'A2'
		when  sale_amt>=300000 and profit_rate>=0.12 and profit_rate<0.2  then 'A3'
		when  sale_amt<50000   and profit_rate>=0.2 then  'B1'
		when  sale_amt>=50000  and sale_amt<300000 and profit_rate>=0.12 and profit_rate<0.2  then  'B2'
		when  sale_amt>=300000 and profit_rate>=0.05 and profit_rate<0.12  then  'C1'
		when  sale_amt>=300000 and profit_rate<0.05  then  'C2'
		when  sale_amt<50000   and profit_rate>=0.12 and profit_rate<0.2 then  'D1'
		when  sale_amt>=50000  and sale_amt<300000 and profit_rate>=0.05 and profit_rate<0.12  then  'D2'
		when  sale_amt<50000   and profit_rate>=0.05 and profit_rate<0.12  then  'E1'
		when  sale_amt>=50000  and sale_amt<=300000 and profit_rate<0.05 then  'E2'
		when  sale_amt<50000   and  profit_rate<0.05 then  'E3'
		else 'E3' 
	end customer_small_level
from
	(
	select
		substr(sdt,1,6) as smonth,
		customer_code,
		count(distinct sdt) as cust_sales_cnt,
		sum(sale_amt) as sale_amt,
		sum(profit) as profit,
		sum(sale_amt_no_tax) as sale_amt_no_tax,
		sum(profit_no_tax) as profit_no_tax,
		if(sum(sale_amt)=0,0,sum(profit)/abs(sum(sale_amt))) as profit_rate
	from
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt>='20210101' and sdt<='20221231'
		and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
		and business_type_code not in (4,6) -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.bbc 7.大宗一部 8.大宗二部 9.商超)
	group by  
		substr(sdt,1,6),customer_code
	) a 
;
select * from csx_analyse_tmp.csx_analyse_tmp_finance_customer_customer_small_level_20230202

-- 年度
drop table if exists csx_analyse_tmp.csx_analyse_tmp_finance_customer_customer_small_level_year_20230202;
create table csx_analyse_tmp.csx_analyse_tmp_finance_customer_customer_small_level_year_20230202
as
select
	syear,customer_code,sale_amt,profit,sale_amt_no_tax,profit_no_tax,profit_rate,
	case when sale_amt<1000*12  or  sale_amt/cust_sales_cnt<500 then 'E3' 
		when  sale_amt>=300000*12 and profit_rate>=0.2  then  'A1'
		when  sale_amt>=50000*12  and sale_amt<300000*12 and profit_rate>=0.2  then 'A2'
		when  sale_amt>=300000*12 and profit_rate>=0.12 and profit_rate<0.2  then 'A3'
		when  sale_amt<50000*12   and profit_rate>=0.2 then  'B1'
		when  sale_amt>=50000*12  and sale_amt<300000*12 and profit_rate>=0.12 and profit_rate<0.2  then  'B2'
		when  sale_amt>=300000*12 and profit_rate>=0.05 and profit_rate<0.12  then  'C1'
		when  sale_amt>=300000*12 and profit_rate<0.05  then  'C2'
		when  sale_amt<50000*12   and profit_rate>=0.12 and profit_rate<0.2 then  'D1'
		when  sale_amt>=50000*12  and sale_amt<300000*12 and profit_rate>=0.05 and profit_rate<0.12  then  'D2'
		when  sale_amt<50000*12   and profit_rate>=0.05 and profit_rate<0.12  then  'E1'
		when  sale_amt>=50000*12  and sale_amt<=300000*12 and profit_rate<0.05 then  'E2'
		when  sale_amt<50000*12   and  profit_rate<0.05 then  'E3'
		else 'E3' 
	end customer_small_level
from
	(
	select
		substr(sdt,1,4) as syear,
		customer_code,
		count(distinct sdt) as cust_sales_cnt,
		sum(sale_amt) as sale_amt,
		sum(profit) as profit,
		sum(sale_amt_no_tax) as sale_amt_no_tax,
		sum(profit_no_tax) as profit_no_tax,
		if(sum(sale_amt)=0,0,sum(profit)/abs(sum(sale_amt))) as profit_rate
	from
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt>='20210101' and sdt<='20221231'
		and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
		and business_type_code not in (4,6) -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.bbc 7.大宗一部 8.大宗二部 9.商超)
	group by  
		substr(sdt,1,4),customer_code
	) a 
;
select * from csx_analyse_tmp.csx_analyse_tmp_finance_customer_customer_small_level_year_20230202;

drop table if exists csx_analyse_tmp.csx_analyse_tmp_finance_customer_sale_20230202;
create table csx_analyse_tmp.csx_analyse_tmp_finance_customer_sale_20230202
as
select
	b.performance_province_name,a.customer_code,b.customer_name,b.channel_name,
	case when a.delivery_type_name='一件代发' then '一件代发' else '非一件代发' end as delivery_type_name,
	c.quarter_of_year,
	count(distinct a.sdt) as day_frequency,
	d.first_business_sale_date,
	if(d.first_sale_quarter_of_year=c.quarter_of_year,'新客','老客') as customer_flag_q,
	if(substr(d.first_sale_quarter_of_year,1,4)=substr(c.quarter_of_year,1,4),'新客','老客') as customer_flag_y,
	-- a.business_type_name,
	b.first_category_name,
	b.second_category_name,
	b.third_category_name,
	sum(a.sale_amt) as sale_amt,
	sum(a.sale_amt_no_tax) as sale_amt_no_tax,
	sum(a.profit) as profit,
	sum(a.profit_no_tax) as profit_no_tax
from
	(
	select
		sdt,customer_code,business_type_name,sale_amt,sale_amt_no_tax,profit,profit_no_tax,delivery_type_name,delivery_type_code
	from
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt>='20210101' and sdt<='20221231'
		and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
		and business_type_code=6 -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.bbc 7.大宗一部 8.大宗二部 9.商超)
	) a 
	left join
		(
		select
			customer_code,customer_name,performance_region_name,performance_province_name,performance_city_name,first_category_name,second_category_name,third_category_name,sign_company_code,
			sales_user_number,sales_user_name,customer_address_full,channel_name
		from
			csx_dim.csx_dim_crm_customer_info
		where
			sdt='current'
		) b on b.customer_code=a.customer_code
	left join
		(
		select
			calday,quarter_of_year
		from
			csx_dim.csx_dim_basic_date
		) c on c.calday=a.sdt
	left join
		(
		select
			t1.customer_code,t1.first_business_sale_date,t1.first_sale_yearmonth,t2.quarter_of_year as first_sale_quarter_of_year
		from
			(
			select
				customer_code,first_business_sale_date,substr(first_business_sale_date,1,6) as first_sale_yearmonth
			from
				csx_dws.csx_dws_crm_customer_business_active_di
			where
				sdt='current'
				and business_type_code=6
			) t1
			left join
				(
				select
					calday,quarter_of_year
				from
					csx_dim.csx_dim_basic_date
				) t2 on t2.calday=t1.first_business_sale_date				
		) d on d.customer_code=a.customer_code

group by 
	b.performance_province_name,a.customer_code,b.customer_name,b.channel_name,
	case when a.delivery_type_name='一件代发' then '一件代发' else '非一件代发' end,
	c.quarter_of_year,
	d.first_business_sale_date,
	if(d.first_sale_quarter_of_year=c.quarter_of_year,'新客','老客'),
	if(substr(d.first_sale_quarter_of_year,1,4)=substr(c.quarter_of_year,1,4),'新客','老客'),
	-- a.business_type_name,
	b.first_category_name,
	b.second_category_name,
	b.third_category_name
;
select * from csx_analyse_tmp.csx_analyse_tmp_finance_customer_sale_20230202;

drop table if exists csx_analyse_tmp.csx_analyse_tmp_finance_customer_sale_20230202_02;
create table csx_analyse_tmp.csx_analyse_tmp_finance_customer_sale_20230202_02
as
select
	b.performance_province_name,a.customer_code,b.customer_name,a.channel_name,
	case when a.delivery_type_name='一件代发' then '一件代发' else '非一件代发' end as delivery_type_name,
	c.quarter_of_year,
	count(distinct a.sdt) as day_frequency,
	d.first_business_sale_date,
	if(d.first_sale_quarter_of_year=c.quarter_of_year,'新客','老客') as customer_flag_q,
	if(substr(d.first_sale_quarter_of_year,1,4)=substr(c.quarter_of_year,1,4),'新客','老客') as customer_flag_y,
	-- a.business_type_name,
	b.first_category_name,
	b.second_category_name,
	b.third_category_name,
	sum(a.sale_amt) as sale_amt,
	sum(a.sale_amt_no_tax) as sale_amt_no_tax,
	sum(a.profit) as profit,
	sum(a.profit_no_tax) as profit_no_tax
from
	(
	select
		sdt,customer_code,business_type_name,sale_amt,sale_amt_no_tax,profit,profit_no_tax,delivery_type_name,delivery_type_code,channel_name
	from
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt>='20210101' and sdt<='20221231'
		and channel_code in('7') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
		and business_type_code=6 -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.bbc 7.大宗一部 8.大宗二部 9.商超)
	) a 
	left join
		(
		select
			customer_code,customer_name,performance_region_name,performance_province_name,performance_city_name,first_category_name,second_category_name,third_category_name,sign_company_code,
			sales_user_number,sales_user_name,customer_address_full,channel_name
		from
			csx_dim.csx_dim_crm_customer_info
		where
			sdt='current'
		) b on b.customer_code=a.customer_code
	left join
		(
		select
			calday,quarter_of_year
		from
			csx_dim.csx_dim_basic_date
		) c on c.calday=a.sdt
	left join
		(
		select
			t1.customer_code,t1.first_business_sale_date,t1.first_sale_yearmonth,t2.quarter_of_year as first_sale_quarter_of_year
		from
			(
			select
				customer_code,first_business_sale_date,substr(first_business_sale_date,1,6) as first_sale_yearmonth
			from
				csx_dws.csx_dws_crm_customer_business_active_di
			where
				sdt='current'
				and business_type_code=6
			) t1
			left join
				(
				select
					calday,quarter_of_year
				from
					csx_dim.csx_dim_basic_date
				) t2 on t2.calday=t1.first_business_sale_date				
		) d on d.customer_code=a.customer_code

group by 
	b.performance_province_name,a.customer_code,b.customer_name,a.channel_name,
	case when a.delivery_type_name='一件代发' then '一件代发' else '非一件代发' end,
	c.quarter_of_year,
	d.first_business_sale_date,
	if(d.first_sale_quarter_of_year=c.quarter_of_year,'新客','老客'),
	if(substr(d.first_sale_quarter_of_year,1,4)=substr(c.quarter_of_year,1,4),'新客','老客'),
	-- a.business_type_name,
	b.first_category_name,
	b.second_category_name,
	b.third_category_name
;
select * from csx_analyse_tmp.csx_analyse_tmp_finance_customer_sale_20230202_02



