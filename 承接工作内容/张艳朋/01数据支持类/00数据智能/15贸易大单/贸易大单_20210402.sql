--============================================================================================================================================================
-- 省区
select
	a.s_sdt,
	--a.province_code,
	a.province_name,
	a.business_type_name,
	a.sales_value,
	a.profit,
	a.profit_rate,
	a.customer_cnt,
	a.sales_value/b.sales_value as sales_value_prorate,
	a.sales_value/b.sales_value_2 as sales_value_prorate_2
from
	(
	select
		substr(sdt,1,6) as s_sdt,
		--province_code,
		province_name,
		business_type_name,
		sum(sales_value) as sales_value,
		sum(profit) as profit,
		sum(profit)/abs(sum(sales_value)) as profit_rate,
		count(distinct customer_no) as customer_cnt
	from 
		csx_dw.dws_sale_r_d_detail
	where 
		sdt >= '20201101' 
		and sdt <= '20210331'
		and business_type_code in ('3','5') --业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
	group by 
		substr(sdt,1,6),
		--province_code,
		province_name,
		business_type_name
	) as a
	left join
		(
		select
			substr(sdt,1,6) as s_sdt,
			--province_code,
			province_name,
			sum(case when channel_code in ('1','7','9') then sales_value else 0 end) as sales_value,
			sum(sales_value) as sales_value_2
		from 
			csx_dw.dws_sale_r_d_detail
		where 
			sdt >= '20201101' 
			and sdt <= '20210331'
			--and business_type_code in ('3','5') --业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
		group by 
			substr(sdt,1,6),
			--province_code,
			province_name
		) as b on b.s_sdt=a.s_sdt and b.province_name=a.province_name
		
		
--===================================================================================================================================================================
-- W0K4
select
	a.s_sdt,
	--a.province_code,
	a.province_name,
	a.business_type_name,
	a.sales_value,
	a.profit,
	a.profit_rate,
	a.customer_cnt,
	a.sales_value/b.sales_value as sales_value_prorate,
	a.sales_value/b.sales_value_2 as sales_value_prorate_2
from
	(
	select
		substr(sdt,1,6) as s_sdt,
		--province_code,
		'W0K4' as province_name,
		'W0K4' as business_type_name,
		sum(sales_value) as sales_value,
		sum(profit) as profit,
		sum(profit)/abs(sum(sales_value)) as profit_rate,
		count(distinct customer_no) as customer_cnt
	from 
		csx_dw.dws_sale_r_d_detail
	where 
		sdt >= '20201101' 
		and sdt <= '20210331'
		--and business_type_code in ('3','5') --业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
		and dc_code='W0K4'
	group by 
		substr(sdt,1,6)
		--province_code,
		--province_name,
		--business_type_name
	) as a
	left join
		(
		select
			substr(sdt,1,6) as s_sdt,
			--province_code,
			'W0K4' as province_name,
			sum(case when channel_code in ('1','7','9') then sales_value else 0 end) as sales_value,
			sum(sales_value) as sales_value_2
		from 
			csx_dw.dws_sale_r_d_detail
		where 
			sdt >= '20201101' 
			and sdt <= '20210331'
			--and business_type_code in ('3','5') --业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
			and dc_code='W0K4'
		group by 
			substr(sdt,1,6)
			--province_code,
			--province_name
		) as b on b.s_sdt=a.s_sdt and b.province_name=a.province_name
		

--===================================================================================================================================================================
-- 全国
select
	a.s_sdt,
	--a.province_code,
	a.province_name,
	a.business_type_name,
	a.sales_value,
	a.profit,
	a.profit_rate,
	a.customer_cnt,
	a.sales_value/b.sales_value as sales_value_prorate,
	a.sales_value/b.sales_value_2 as sales_value_prorate_2
from
	(
	select
		substr(sdt,1,6) as s_sdt,
		--province_code,
		'全国' as province_name,
		business_type_name,
		sum(sales_value) as sales_value,
		sum(profit) as profit,
		sum(profit)/abs(sum(sales_value)) as profit_rate,
		count(distinct customer_no) as customer_cnt
	from 
		csx_dw.dws_sale_r_d_detail
	where 
		sdt >= '20201101' 
		and sdt <= '20210331'
		and business_type_code in ('3','5') --业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
	group by 
		substr(sdt,1,6),
		--province_code,
		--province_name,
		business_type_name
	) as a
	left join
		(
		select
			substr(sdt,1,6) as s_sdt,
			--province_code,
			'全国' as province_name,
			sum(case when channel_code in ('1','7','9') then sales_value else 0 end) as sales_value,
			sum(sales_value) as sales_value_2
		from 
			csx_dw.dws_sale_r_d_detail
		where 
			sdt >= '20201101' 
			and sdt <= '20210331'
			--and business_type_code in ('3','5') --业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
		group by 
			substr(sdt,1,6)
			--province_code,
			--province_name
		) as b on b.s_sdt=a.s_sdt and b.province_name=a.province_name
		
		
--===================================================================================================================================================================
-- 贸易大单 省区
--规则：订单金额>=3万，毛利率<=5%，当月下单天数<=2 的
-- insert overwrite directory '/tmp/zhangyanpeng/20210402_data_1' row format delimited fields terminated by '\t'
select
	a.s_sdt,
	a.province_name,
	a.type,
	a.sales_value,
	a.profit,
	a.profit_rate,
	a.customer_cnt,
	a.sales_value/b.sales_value as sales_value_prorate,
	a.sales_value/b.sales_value_2 as sales_value_prorate_2
from
	(
	select 
		a.s_sdt,
		a.province_name,
		'贸易大单' as type,
		sum(a.sales_value) as sales_value,   
		sum(a.profit) as profit,
		sum(a.profit)/abs(sum(a.sales_value)) as profit_rate,
		count(distinct a.customer_no) as customer_cnt
	from 
		(
		select 
			substr(sdt,1,6) as s_sdt,customer_no,channel_name,province_name,dc_code,dc_name,goods_code,goods_name,origin_order_no,order_no,sales_value,profit
		from 
			csx_dw.dws_sale_r_d_detail
		where 
			sdt>='20201101'
			and sdt<='20210331'
			and channel_code in('1','7','9')
			and business_type_code in ('1','2')
		)a		 
		left join 
			(
			select 
				customer_no,customer_name,sales_province_code,sales_province_name,attribute,attribute_desc,attribute_code,attribute_name,
				first_category_name,second_category_name,third_category_name,work_no,sales_name,regexp_replace(split(sign_time, ' ')[0], '-', '') as sign_date
			from 
				csx_dw.dws_crm_w_a_customer
			where 
				sdt=regexp_replace(date_sub(current_date,1),'-','') 
			) b on a.customer_no=b.customer_no
		join  --------------------------------------------------------------------------------------------------------------------订单金额>=3万。
			(
			select 
				substr(sdt,1,6) as s_sdt,origin_order_no,
				count(distinct goods_code) SKU,
				sum(sales_value) sales_value,	--含税销售额
				sum(sales_cost) sales_cost,	--含税销售成本
				sum(profit) profit,	--含税毛利
				sum(profit)/sum(sales_value) order_profit_rate
			from 
				csx_dw.dws_sale_r_d_detail
			where 
				sdt>='20201101'
				and sdt<='20210331'
				and channel_code in('1','7','9')
				and business_type_code in ('1','2')
			group by  
				substr(sdt,1,6),origin_order_no
			having 
				sum(sales_value)>=30000 
				and (sum(profit)/sum(sales_value))<=0.05
			) c on c.origin_order_no=a.origin_order_no and c.s_sdt =a.s_sdt
		left join --客户本月下单次数、月整体毛利率
			(
			select	
				substr(sdt,1,6)as s_sdt,province_code,province_name,city_group_code,city_group_name,customer_no,	
				count(distinct sdt) count_days,
				sum(profit)/sum(sales_value) month_profit_rate
			from 
				csx_dw.dws_sale_r_d_detail
			where 
				sdt>='20201101'
				and sdt<='20210331'
				and channel_code in('1','7','9')
				and business_type_code in ('1','2')
			group by 
				substr(sdt,1,6),province_code,province_name,city_group_code,city_group_name,customer_no
			) d on d.customer_no=a.customer_no and d.s_sdt =a.s_sdt
	where  
		d.count_days<=2
	group by 
		a.s_sdt,
		a.province_name	
	) as a 
	left join
		(
		select
			substr(sdt,1,6) as s_sdt,
			--province_code,
			province_name,
			sum(case when channel_code in ('1','7','9') then sales_value else 0 end) as sales_value,
			sum(sales_value) as sales_value_2
		from 
			csx_dw.dws_sale_r_d_detail
		where 
			sdt >= '20201101' 
			and sdt <= '20210331'
		group by 
			substr(sdt,1,6),
			--province_code,
			province_name
		) as b on b.s_sdt=a.s_sdt and b.province_name=a.province_name
		
		
		
--===================================================================================================================================================================
-- 贸易大单 全国
--规则：订单金额>=3万，毛利率<=5%，当月下单天数<=2 的

select
	a.s_sdt,
	a.province_name,
	a.type,
	a.sales_value,
	a.profit,
	a.profit_rate,
	a.customer_cnt,
	a.sales_value/b.sales_value as sales_value_prorate,
	a.sales_value/b.sales_value_2 as sales_value_prorate_2
from
	(
	select 
		a.s_sdt,
		'全国' as province_name,
		'贸易大单' as type,
		sum(a.sales_value) as sales_value,   
		sum(a.profit) as profit,
		sum(a.profit)/abs(sum(a.sales_value)) as profit_rate,
		count(distinct a.customer_no) as customer_cnt
	from 
		(
		select 
			substr(sdt,1,6) as s_sdt,customer_no,channel_name,province_name,dc_code,dc_name,goods_code,goods_name,origin_order_no,order_no,sales_value,profit
		from 
			csx_dw.dws_sale_r_d_detail
		where 
			sdt>='20201101'
			and sdt<='20210331'
			and channel_code in('1','7','9')
			and business_type_code in ('1','2')
		)a		 
		left join 
			(
			select 
				customer_no,customer_name,sales_province_code,sales_province_name,attribute,attribute_desc,attribute_code,attribute_name,
				first_category_name,second_category_name,third_category_name,work_no,sales_name,regexp_replace(split(sign_time, ' ')[0], '-', '') as sign_date
			from 
				csx_dw.dws_crm_w_a_customer
			where 
				sdt=regexp_replace(date_sub(current_date,1),'-','') 
			) b on a.customer_no=b.customer_no
		join  --------------------------------------------------------------------------------------------------------------------订单金额>=3万。
			(
			select 
				substr(sdt,1,6) as s_sdt,origin_order_no,
				count(distinct goods_code) SKU,
				sum(sales_value) sales_value,	--含税销售额
				sum(sales_cost) sales_cost,	--含税销售成本
				sum(profit) profit,	--含税毛利
				sum(profit)/sum(sales_value) order_profit_rate
			from 
				csx_dw.dws_sale_r_d_detail
			where 
				sdt>='20201101'
				and sdt<='20210331'
				and channel_code in('1','7','9')
				and business_type_code in ('1','2')
			group by  
				substr(sdt,1,6),origin_order_no
			having 
				sum(sales_value)>=30000 
				and (sum(profit)/sum(sales_value))<=0.05
			) c on c.origin_order_no=a.origin_order_no and c.s_sdt =a.s_sdt
		left join --客户本月下单次数、月整体毛利率
			(
			select	
				substr(sdt,1,6)as s_sdt,province_code,province_name,city_group_code,city_group_name,customer_no,	
				count(distinct sdt) count_days,
				sum(profit)/sum(sales_value) month_profit_rate
			from 
				csx_dw.dws_sale_r_d_detail
			where 
				sdt>='20201101'
				and sdt<='20210331'
				and channel_code in('1','7','9')
				and business_type_code in ('1','2')
			group by 
				substr(sdt,1,6),province_code,province_name,city_group_code,city_group_name,customer_no
			) d on d.customer_no=a.customer_no and d.s_sdt =a.s_sdt
	where  
		d.count_days<=2
	group by 
		a.s_sdt
		-- a.province_name	
	) as a 
	left join
		(
		select
			substr(sdt,1,6) as s_sdt,
			--province_code,
			'全国' as province_name,
			sum(case when channel_code in ('1','7','9') then sales_value else 0 end) as sales_value,
			sum(sales_value) as sales_value_2
		from 
			csx_dw.dws_sale_r_d_detail
		where 
			sdt >= '20201101' 
			and sdt <= '20210331'
		group by 
			substr(sdt,1,6)
			--province_code,
			--province_name
		) as b on b.s_sdt=a.s_sdt and b.province_name=a.province_name
		
		
--===================================================================================================================================================================
--商品明细 省区大宗+批发内购+W0K4
	
insert overwrite directory '/tmp/zhangyanpeng/20210402_data_1' row format delimited fields terminated by '\t'	
select 
	province_name,
	business_type_name,
	goods_code,
	goods_name,
	unit,
	department_code,
	department_name,
	classify_large_code,
	classify_large_name,
	classify_middle_code,
	classify_middle_name,
	classify_small_code,
	classify_small_name,
	sum(sales_value)sales,
	sum(profit) profit,
	sum(profit)/sum(sales_value)as  profit_rate,
	count(distinct case when sales_value>0 then  customer_no end ) as sale_cust,
	count(distinct case when sales_value>0 then  sdt end) as sale_sdt
from 
	csx_dw.dws_sale_r_d_detail 
where 
	sdt>='20201101' 
	and sdt<='20210331' 
	and business_type_code in ('3','5') -- or dc_code='W0K4') --业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
group by 
	province_name,
	business_type_name,
	goods_code,
	goods_name,
	unit,
	department_code,
	department_name,
	classify_large_code,
	classify_large_name,
	classify_middle_code,
	classify_middle_name,
	classify_small_code,
	classify_small_name
union all
select 
	province_name,
	'W0K4' as business_type_name,
	goods_code,
	goods_name,
	unit,
	department_code,
	department_name,
	classify_large_code,
	classify_large_name,
	classify_middle_code,
	classify_middle_name,
	classify_small_code,
	classify_small_name,
	sum(sales_value)sales,
	sum(profit) profit,
	sum(profit)/sum(sales_value)as  profit_rate,
	count(distinct case when sales_value>0 then  customer_no end ) as sale_cust,
	count(distinct case when sales_value>0 then  sdt end) as sale_sdt
from 
	csx_dw.dws_sale_r_d_detail 
where 
	sdt>='20201101' 
	and sdt<='20210331' 
	and dc_code='W0K4'
group by 
	province_name,
	goods_code,
	goods_name,
	unit,
	department_code,
	department_name,
	classify_large_code,
	classify_large_name,
	classify_middle_code,
	classify_middle_name,
	classify_small_code,
	classify_small_name
	
	
	
--===================================================================================================================================================================
--商品明细 贸易大单
	
insert overwrite directory '/tmp/zhangyanpeng/20210402_data_1_1' row format delimited fields terminated by '\t'	

select 
	a.province_name,
	'贸易大单' as type,
	goods_code,
	goods_name,
	unit,
	department_code,
	department_name,
	classify_large_code,
	classify_large_name,
	classify_middle_code,
	classify_middle_name,
	classify_small_code,
	classify_small_name,
	sum(a.sales_value)sales,
	sum(a.profit) profit,
	sum(a.profit)/sum(a.sales_value)as  profit_rate,
	count(distinct case when a.sales_value>0 then  a.customer_no end ) as sale_cust,
	count(distinct case when a.sales_value>0 then  a.s_sdt end) as sale_sdt
from 
	(
	select 
		substr(sdt,1,6) as s_sdt,customer_no,channel_name,province_name,dc_code,dc_name,goods_code,goods_name,unit,department_code,department_name,
		origin_order_no,order_no,sales_value,profit,classify_large_code,classify_large_name,classify_middle_code,classify_middle_name,classify_small_code,classify_small_name
	from 
		csx_dw.dws_sale_r_d_detail
	where 
		sdt>='20201101'
		and sdt<='20210331'
		and channel_code in('1','7','9')
		and business_type_code in ('1','2')
	)a		 
	left join 
		(
		select 
			customer_no,customer_name,sales_province_code,sales_province_name,attribute,attribute_desc,attribute_code,attribute_name,
			first_category_name,second_category_name,third_category_name,work_no,sales_name,regexp_replace(split(sign_time, ' ')[0], '-', '') as sign_date
		from 
			csx_dw.dws_crm_w_a_customer
		where 
			sdt=regexp_replace(date_sub(current_date,1),'-','') 
		) b on a.customer_no=b.customer_no
	join  --------------------------------------------------------------------------------------------------------------------订单金额>=3万。
		(
		select 
			substr(sdt,1,6) as s_sdt,origin_order_no,
			count(distinct goods_code) SKU,
			sum(sales_value) sales_value,	--含税销售额
			sum(sales_cost) sales_cost,	--含税销售成本
			sum(profit) profit,	--含税毛利
			sum(profit)/sum(sales_value) order_profit_rate
		from 
			csx_dw.dws_sale_r_d_detail
		where 
			sdt>='20201101'
			and sdt<='20210331'
			and channel_code in('1','7','9')
			and business_type_code in ('1','2')
		group by  
			substr(sdt,1,6),origin_order_no
		having 
			sum(sales_value)>=30000 
			and (sum(profit)/sum(sales_value))<=0.05
		) c on c.origin_order_no=a.origin_order_no and c.s_sdt =a.s_sdt
	left join --客户本月下单次数、月整体毛利率
		(
		select	
			substr(sdt,1,6)as s_sdt,province_code,province_name,city_group_code,city_group_name,customer_no,	
			count(distinct sdt) count_days,
			sum(profit)/sum(sales_value) month_profit_rate
		from 
			csx_dw.dws_sale_r_d_detail
		where 
			sdt>='20201101'
			and sdt<='20210331'
			and channel_code in('1','7','9')
			and business_type_code in ('1','2')
		group by 
			substr(sdt,1,6),province_code,province_name,city_group_code,city_group_name,customer_no
		) d on d.customer_no=a.customer_no and d.s_sdt =a.s_sdt
where  
	d.count_days<=2
group by
	a.province_name,
	goods_code,
	goods_name,
	unit,
	department_code,
	department_name,
	classify_large_code,
	classify_large_name,
	classify_middle_code,
	classify_middle_name,
	classify_small_code,
	classify_small_name	





--===================================================================================================================================================================
--客户明细	省区大宗+批发内购+批发内购
insert overwrite directory '/tmp/zhangyanpeng/20210402_data_2' row format delimited fields terminated by '\t'	
select 
	province_name,
	business_type_name,
	customer_no,
	customer_name,
	second_category_name,
	sum(sales_value)sales,
	sum(profit) profit,
	sum(profit)/sum(sales_value)as  profit_rate,
	count(distinct goods_code) as sale_sku,
	count(distinct sdt) as sale_sdt
from 
	csx_dw.dws_sale_r_d_detail 
where 
	sdt>='20201101' 
	and sdt<='20210331' 
	and business_type_code in ('3','5') -- or dc_code='W0K4')
group by 
	province_name,
	business_type_name,
	second_category_name,
	customer_no,
	customer_name
union all
select 
	province_name,
	'W0K4' as business_type_name,
	customer_no,
	customer_name,
	second_category_name,
	sum(sales_value)sales,
	sum(profit) profit,
	sum(profit)/sum(sales_value)as  profit_rate,
	count(distinct goods_code) as sale_sku,
	count(distinct sdt) as sale_sdt
from 
	csx_dw.dws_sale_r_d_detail 
where 
	sdt>='20201101' 
	and sdt<='20210331' 
	and dc_code='W0K4'
group by 
	province_name,
	second_category_name,
	customer_no,
	customer_name
	
	
--===================================================================================================================================================================
--客户明细	贸易大单
insert overwrite directory '/tmp/zhangyanpeng/20210402_data_2_2' row format delimited fields terminated by '\t'	

select 
	a.province_name,
	'贸易大单' as business_type_name,
	a.customer_no,
	a.customer_name,
	b.second_category_name,
	sum(a.sales_value)sales,
	sum(a.profit) profit,
	sum(a.profit)/abs(sum(a.sales_value)) as  profit_rate,
	count(distinct a.goods_code) as sale_sku,
	count(distinct a.s_sdt) as sale_sdt
from 
	(
	select 
		substr(sdt,1,6) as s_sdt,customer_no,channel_name,province_name,dc_code,dc_name,goods_code,goods_name,unit,department_code,department_name,customer_name,
		origin_order_no,order_no,sales_value,profit,classify_large_code,classify_large_name,classify_middle_code,classify_middle_name,classify_small_code,classify_small_name
	from 
		csx_dw.dws_sale_r_d_detail
	where 
		sdt>='20201101'
		and sdt<='20210331'
		and channel_code in('1','7','9')
		and business_type_code in ('1','2')
	)a		 
	left join 
		(
		select 
			customer_no,customer_name,sales_province_code,sales_province_name,attribute,attribute_desc,attribute_code,attribute_name,
			first_category_name,second_category_name,third_category_name,work_no,sales_name,regexp_replace(split(sign_time, ' ')[0], '-', '') as sign_date
		from 
			csx_dw.dws_crm_w_a_customer
		where 
			sdt=regexp_replace(date_sub(current_date,1),'-','') 
		) b on a.customer_no=b.customer_no
	join  --------------------------------------------------------------------------------------------------------------------订单金额>=3万。
		(
		select 
			substr(sdt,1,6) as s_sdt,origin_order_no,
			count(distinct goods_code) SKU,
			sum(sales_value) sales_value,	--含税销售额
			sum(sales_cost) sales_cost,	--含税销售成本
			sum(profit) profit,	--含税毛利
			sum(profit)/sum(sales_value) order_profit_rate
		from 
			csx_dw.dws_sale_r_d_detail
		where 
			sdt>='20201101'
			and sdt<='20210331'
			and channel_code in('1','7','9')
			and business_type_code in ('1','2')
		group by  
			substr(sdt,1,6),origin_order_no
		having 
			sum(sales_value)>=30000 
			and (sum(profit)/sum(sales_value))<=0.05
		) c on c.origin_order_no=a.origin_order_no and c.s_sdt =a.s_sdt
	left join --客户本月下单次数、月整体毛利率
		(
		select	
			substr(sdt,1,6)as s_sdt,province_code,province_name,city_group_code,city_group_name,customer_no,	
			count(distinct sdt) count_days,
			sum(profit)/sum(sales_value) month_profit_rate
		from 
			csx_dw.dws_sale_r_d_detail
		where 
			sdt>='20201101'
			and sdt<='20210331'
			and channel_code in('1','7','9')
			and business_type_code in ('1','2')
		group by 
			substr(sdt,1,6),province_code,province_name,city_group_code,city_group_name,customer_no
		) d on d.customer_no=a.customer_no and d.s_sdt =a.s_sdt
where  
	d.count_days<=2
group by
	a.province_name,
	a.customer_no,
	a.customer_name,
	b.second_category_name