-- 华南——直送仓影响
-- 华北——非直送非异常数据影响，需要待分析
-- 华西——异常数据影响(调价、退单)
-- 华东——直送仓+异常数据影响(退单)
-- 华中——直送仓+异常数据+待分析的原因

-- 总结关键点 大区省区问题 哪些客户

-- ===============================================================================================================================================================================

drop table if exists csx_analyse_tmp.csx_analyse_tmp_dl_jr_profit_decline;
create table csx_analyse_tmp.csx_analyse_tmp_dl_jr_profit_decline
as
select 
	a.performance_region_name,a.performance_province_name,a.sale_month,
	if(a.sale_month=c.first_sale_month,'新客','老客') as customer_flag,d.second_category_name,d.third_category_name,
	count(distinct a.customer_code) as customer_cnt,
	sum(a.sale_amt) as sale_amt,
	sum(a.profit) as profit
from
	(
	select
		substr(sdt,1,6) as sale_month,sdt,customer_code,goods_code,sale_amt,profit,performance_region_name,performance_province_name
	from
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt>='20220401' and sdt<='20230430'
		and channel_code in('1','7','9')
		and business_type_code not in(4)
		and performance_province_name !='平台-B'
	) a 
	left join
		(
		select
			customer_code,min(first_business_sale_date) as first_sale_date,substr(min(first_business_sale_date),1,6) as first_sale_month
		from
			csx_dws.csx_dws_crm_customer_business_active_di
		where 
			sdt='current'
			and business_type_code in(1,2,3,5,6) -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.bbc 7.大宗一部 8.大宗二部 9.商超)
		group by 
			customer_code
		) c on c.customer_code=a.customer_code
	left join
		(
		select
			customer_code,customer_name,performance_region_name,performance_province_name,performance_city_name,first_category_name,second_category_name,third_category_name,sign_company_code,
			sales_user_number,sales_user_name,customer_address_full
		from
			csx_dim.csx_dim_crm_customer_info
		where
			sdt='current'
			-- and second_category_code='305' -- 客户二级分类：教育
		) d on d.customer_code=a.customer_code	
group by 
	a.performance_region_name,a.performance_province_name,a.sale_month,
	if(a.sale_month=c.first_sale_month,'新客','老客'),d.second_category_name,d.third_category_name
;
select * from csx_analyse_tmp.csx_analyse_tmp_dl_jr_profit_decline;

-- 业务类型
select
	substr(sdt,1,6) as sale_month,performance_region_name,performance_province_name,business_type_name,sum(sale_amt) as sale_amt,sum(profit) as profit,sum(profit)/abs(sum(sale_amt)) as profit_rate
from
	csx_dws.csx_dws_sale_detail_di
where 
	sdt>='20220401' and sdt<='20230430'
	and channel_code in('1','7','9')
	and business_type_code not in(4)
	and performance_province_name !='平台-B'
	and second_category_name in ('金融业','电力燃气水供应')
group by 
	substr(sdt,1,6),business_type_name,performance_region_name,performance_province_name
	
-- 日配 直送仓非直送仓
select
	substr(a.sdt,1,6) as sale_month,performance_region_name,performance_province_name,case when b.shop_code is not null then '是' else '否' end as if_zs_dc,
	sum(sale_amt) as sale_amt,sum(profit) as profit,sum(profit)/abs(sum(sale_amt)) as profit_rate
from
	(
	select
		sdt,business_type_name,customer_code,goods_code,inventory_dc_code,sale_amt,profit,performance_region_name,performance_province_name
	from
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt>='20220401' and sdt<='20230430'
		and channel_code in('1','7','9')
		and business_type_code in(1)
		and performance_province_name !='平台-B'
		and second_category_name in ('金融业','电力燃气水供应')
	) a 
	left join
		(
		select
			shop_code
		from 
			csx_dim.csx_dim_shop 
		where 
			sdt='current' 
			and shop_low_profit_flag=1 
		) b on b.shop_code=a.inventory_dc_code
group by 
	substr(a.sdt,1,6),performance_region_name,performance_province_name,case when b.shop_code is not null then '是' else '否' end
;
-- 
drop table if exists csx_analyse_tmp.csx_analyse_tmp_dl_jr_profit_decline_customer;
create table csx_analyse_tmp.csx_analyse_tmp_dl_jr_profit_decline_customer
as
select 
	substr(a.sdt,1,6) as month,
	a.performance_region_code,
	a.performance_region_name,
	a.performance_province_code,
	a.performance_province_name,
	a.performance_city_code,
	a.performance_city_name,
	a.business_type_code,
	a.business_type_name,
	c.classify_large_code,
	c.classify_large_name,
	c.classify_middle_code,
	c.classify_middle_name,
	a.customer_code,
	a.customer_name,
	d.first_business_sale_date,
	e.first_category_name,
	e.second_category_name,
	e.third_category_name,
	a.inventory_dc_code,
	(case when b.shop_code is not null then '是' else '否' end) as if_zs_dc,
	nvl(sum(a.profit),0) as all_profit,
	nvl(sum(a.sale_amt),0) as all_sale_amt,
	nvl(sum(case when a.delivery_type_code=2 then a.profit end),0) as zs_profit,
	nvl(sum(case when a.delivery_type_code=2 then a.sale_amt end),0) as zs_sale_amt,
	nvl(sum(case when a.order_channel_code=6 then a.profit end),0) as tj_profit,
	nvl(sum(case when a.order_channel_code=6 then a.sale_amt end),0) as tj_sale_amt,
	nvl(sum(case when a.order_channel_code=4 then a.profit end),0) as fl_profit,
	nvl(sum(case when a.order_channel_code=4 then a.sale_amt end),0) as fl_sale_amt,
	nvl(sum(case when a.refund_order_flag=1 then a.profit end),0) as td_profit,
	nvl(sum(case when a.refund_order_flag=1 then a.sale_amt end),0) as td_sale_amt,
	nvl(sum(case when a.channel_code in (1,7,9) and a.business_type_code=3 then a.profit end),0) as cq_profit,
	nvl(sum(case when a.channel_code in (1,7,9) and a.business_type_code=3 then a.sale_amt end),0) as cq_sale_amt 
from 
	(
	select 
		* 
	from 
		csx_dws.csx_dws_sale_detail_di 
	where 
		sdt>='20230301' and sdt<='20230430'
		and channel_code in('1','7','9')
		-- and business_type_code in(1)
		and performance_province_name !='平台-B'
		and second_category_name in ('金融业','电力燃气水供应')
	) a 
	left join (select * from csx_dim.csx_dim_shop where sdt='current' and shop_low_profit_flag=1 ) b on a.inventory_dc_code=b.shop_code 
	left join (select * from csx_dim.csx_dim_basic_goods where sdt='current') c on a.goods_code=c.goods_code 
	left join (select * from csx_dws.csx_dws_crm_customer_business_active_di where sdt='current') d on a.customer_code=d.customer_code and a.business_type_code=d.business_type_code 
	left join (select * from csx_dim.csx_dim_crm_customer_info where sdt='current') e on a.customer_code=e.customer_code 
group by 
	substr(a.sdt,1,6),
	a.performance_region_code,
	a.performance_region_name,
	a.performance_province_code,
	a.performance_province_name,
	a.performance_city_code,
	a.performance_city_name,
	a.business_type_code,
	a.business_type_name,
	c.classify_large_code,
	c.classify_large_name,
	c.classify_middle_code,
	c.classify_middle_name,
	a.customer_code,
	a.customer_name,
	e.first_category_name,
	e.second_category_name,
	e.third_category_name,
	a.inventory_dc_code,
	(case when b.shop_code is not null then '是' else '否' end),
	d.first_business_sale_date 
;
select * from csx_analyse_tmp.csx_analyse_tmp_dl_jr_profit_decline_customer;


drop table if exists csx_analyse_tmp.csx_analyse_tmp_dl_jr_profit_decline_goods;
create table csx_analyse_tmp.csx_analyse_tmp_dl_jr_profit_decline_goods
as
select 
	substr(sdt,1,6) as smonth,performance_region_name,performance_province_name,performance_city_name,business_type_name,customer_code,customer_name,first_category_name,
	second_category_name,classify_large_name,classify_middle_name,business_division_name,goods_code,goods_name,
	sum(sale_amt) sale_amt,
	sum(profit) profit,
	sum(sale_qty) sale_qty,
	sum(sale_cost) sale_cost,
	sum(sale_amt)/sum(sale_qty) sale_sj,
	sum(sale_cost)/sum(sale_qty) sale_cbj  
from 
	csx_dws.csx_dws_sale_detail_di
where 
	sdt>='20230301' and sdt<='20230430'
	and channel_code in('1','7','9')
	and business_type_code=1
	and second_category_name in ('金融业','电力燃气水供应')
	-- and performance_province_name='北京市'
group by 
	substr(sdt,1,6),performance_region_name,performance_province_name,performance_city_name,business_type_name,customer_code,customer_name,first_category_name,
	second_category_name,classify_large_name,classify_middle_name,business_division_name,goods_code,goods_name;

select * from csx_analyse_tmp.csx_analyse_tmp_dl_jr_profit_decline_goods
-- ===============================================================================================================================================================================
-- 202303下单在202304未下单	

select
	a.performance_region_name,a.performance_province_name,
	sum(a.sale_amt) as sale_amt,sum(profit) as profit,sum(profit)/abs(sum(a.sale_amt)) as profit
from
	(
	select
		customer_code,performance_region_name,performance_province_name,sale_amt,profit
	from
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt>='20230301' and sdt<='20230331'
		and channel_code in('1','7','9')
		and business_type_code in(1)
		and second_category_name in ('金融业','电力燃气水供应')
	) a 
	left join
		(
		select
			customer_code
		from
			csx_dws.csx_dws_sale_detail_di
		where 
			sdt>='20230401' and sdt<='20230430'
			and channel_code in('1','7','9')
			and business_type_code in(1)
		group by 
			customer_code
		) b on b.customer_code=a.customer_code
where
	b.customer_code is null
group by  
	a.performance_region_name,a.performance_province_name
;

-- ===============================================================================================================================================================================
-- 20224下单在20231未下单明细

select
	d.performance_region_name,d.performance_province_name,d.performance_city_name,a.customer_code,d.customer_name,first_category_name,second_category_name,third_category_name,
	a.sale_amt,a.profit,a.profit_rate
from
	(
	select
		customer_code,sum(sale_amt) as sale_amt,sum(profit) as profit,if(sum(sale_amt)=0,0,sum(profit)/abs(sum(sale_amt))) as profit_rate
	from
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt>='20221001' and sdt<='20221231'
		and channel_code in('1','7','9')
		and business_type_code in(1)
		and order_channel_code not in (4,6) -- 1-b端 2-m端 3-bbc 4-客户返利管理 5-价格补救 6-客户调价管理 -1-sap
	group by 
		customer_code
	) a 
	left join
		(
		select
			customer_code
		from
			csx_dws.csx_dws_sale_detail_di
		where 
			sdt>='20230101' and sdt<='20230331'
			and channel_code in('1','7','9')
			and business_type_code in(1)
			and order_channel_code not in (4,6) -- 1-b端 2-m端 3-bbc 4-客户返利管理 5-价格补救 6-客户调价管理 -1-sap
		group by 
			customer_code
		) b on b.customer_code=a.customer_code
	join
		(
		select
			customer_code,customer_name,performance_region_name,performance_province_name,performance_city_name,first_category_name,second_category_name,third_category_name,sign_company_code,
			sales_user_number,sales_user_name,customer_address_full
		from
			csx_dim.csx_dim_crm_customer_info
		where
			sdt='current'
			and second_category_code='305' -- 客户二级分类：教育
		) d on d.customer_code=a.customer_code	
where
	b.customer_code is null
;
-- ===============================================================================================================================================================================
-- 商机
select
	a.quarter_of_year,a.business_sign_month,a.business_number,a.customer_id,a.customer_code,a.customer_name,
	a.performance_region_name,a.performance_province_name,a.performance_city_name,
	a.business_stage,a.contract_cycle,a.estimate_contract_amount,a.gross_profit_rate,a.business_sign_date,a.business_sign_date_2,a.first_business_sign_date,
	a.num,a.next_sign_date,
	sum(case when b.sdt>=a.business_sign_date and b.sdt<a.next_sign_date then b.sale_amt else null end) as sale_amt,
	sum(case when b.sdt>=a.business_sign_date and b.sdt<a.next_sign_date then b.profit else null end) as profit,
	count(distinct case when b.sdt>=a.business_sign_date and b.sdt<a.next_sign_date then b.sdt else null end) as sdt_cnt,
	min(case when b.sdt>=a.business_sign_date and b.sdt<a.next_sign_date then b.sdt else null end) as min_sdt
from
	(
	select
		a.business_sign_month,a.business_number,a.customer_id,a.customer_code,a.customer_name,
		d.performance_region_name,d.performance_province_name,d.performance_city_name,
		a.business_stage,a.contract_cycle,a.estimate_contract_amount,a.gross_profit_rate,
		a.business_sign_date,a.business_sign_date_2,a.first_business_sign_date,
		row_number() over(partition by a.customer_code order by a.business_sign_time) num,
		regexp_replace(to_date(lead(a.business_sign_time,1,'9999-12-31')over(partition by a.customer_code order by a.business_sign_time)),'-','') as next_sign_date,
		b.quarter_of_year
	from 
		(
		select
			business_sign_time,
			regexp_replace(substr(to_date(business_sign_time),1,7),'-','') business_sign_month,business_number,customer_id,customer_code,customer_name,
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
			and regexp_replace(to_date(business_sign_time),'-','') between '20220101' and '20230331'
		)a
		left join
			(
			select
				calday,quarter_of_year
			from
				csx_dim.csx_dim_basic_date
			) b on b.calday=a.business_sign_date
		join
			(
			select
				customer_code,customer_name,performance_region_name,performance_province_name,performance_city_name,first_category_name,second_category_name,third_category_name,sign_company_code,
				sales_user_number,sales_user_name,customer_address_full
			from
				csx_dim.csx_dim_crm_customer_info
			where
				sdt='current'
				and second_category_code='305' -- 客户二级分类：教育
			) d on d.customer_code=a.customer_code	
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
			sdt>='20220101' and sdt<='20230331'
			and channel_code in('1','7','9')
			and business_type_code in(1)
			and order_channel_code not in (4,6) -- 1-b端 2-m端 3-bbc 4-客户返利管理 5-价格补救 6-客户调价管理 -1-sap
		group by 
			sdt,customer_code
		)b on a.customer_code=b.customer_code
group by 
	a.quarter_of_year,a.business_sign_month,a.business_number,a.customer_id,a.customer_code,a.customer_name,
	a.performance_region_name,a.performance_province_name,a.performance_city_name,
	a.business_stage,a.contract_cycle,a.estimate_contract_amount,a.gross_profit_rate,a.business_sign_date,a.business_sign_date_2,a.first_business_sign_date,
	a.num,a.next_sign_date
;

