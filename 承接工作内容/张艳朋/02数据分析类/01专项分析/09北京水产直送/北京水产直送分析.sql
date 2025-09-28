北京水产直送比例高，毛利率低的原因

W21直送客户TOP10商品销售额VS同商品非直送成本和售价

水产是什么问题，直送主要原因是什么？补货临采？zz? 临采什么问题？为什么临采？采购问题还是客户问题？
如果是采购不满足为什么？采购要同步
成本是否合理？组成是什么？
客户为什么临采？新品？为什么新增？是否可以前置管理？如何定价？
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
		sdt>='20230101' and sdt<='20230531'
		and channel_code in('1','7','9')
		and business_type_code in (1)
		and performance_province_name ='北京市'
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
	f.csx_week,
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
		sdt>='20230101' and sdt<='20230531'
		and channel_code in('1','7','9')
		and business_type_code in(1)
		and performance_province_name ='北京市'
		and classify_middle_name in ('水产')
		and inventory_dc_code !='WB26'
	) a 
	left join (select * from csx_dim.csx_dim_shop where sdt='current' and shop_low_profit_flag=1 ) b on a.inventory_dc_code=b.shop_code 
	left join (select * from csx_dim.csx_dim_basic_goods where sdt='current') c on a.goods_code=c.goods_code 
	left join (select * from csx_dws.csx_dws_crm_customer_business_active_di where sdt='current') d on a.customer_code=d.customer_code and a.business_type_code=d.business_type_code 
	left join (select * from csx_dim.csx_dim_crm_customer_info where sdt='current') e on a.customer_code=e.customer_code
	left join (select * from csx_dim.csx_dim_basic_date) f on f.calday=a.sdt
group by 
	substr(a.sdt,1,6),
	f.csx_week,
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

-- 成本 售价
drop table if exists csx_analyse_tmp.csx_analyse_tmp_dl_jr_profit_decline_goods;
create table csx_analyse_tmp.csx_analyse_tmp_dl_jr_profit_decline_goods
as

	select 
		substr(a.sdt,1,6) as smonth,f.csx_week,customer_code,customer_name,
		a.goods_code,c.goods_name,delivery_type_name,
		sum(sale_amt) sale_amt,
		sum(profit) profit,
		sum(sale_qty) sale_qty,
		sum(sale_cost) sale_cost,
		sum(sale_amt)/sum(sale_qty) sale_sj,
		sum(sale_cost)/sum(sale_qty) sale_cbj  
	from 
		(
		select
			*
		from
			csx_dws.csx_dws_sale_detail_di
		where 
			sdt>='20230101' and sdt<='20230531'
			and channel_code in('1','7','9')
			and business_type_code in(1)
			and performance_province_name ='北京市'
			and classify_middle_name in ('水产')
			and inventory_dc_code !='WB26'
			-- and 
		) a 
		left join (select * from csx_dim.csx_dim_basic_goods where sdt='current') c on a.goods_code=c.goods_code 
		left join (select * from csx_dim.csx_dim_basic_date) f on f.calday=a.sdt
	group by 
		substr(a.sdt,1,6),f.csx_week,customer_code,customer_name,
		a.goods_code,c.goods_name,delivery_type_name;

select * from csx_analyse_tmp.csx_analyse_tmp_dl_jr_profit_decline_goods;


	select 
		substr(sdt,1,6) as smonth,
		customer_code,
		customer_name,
		classify_middle_name,
		delivery_type_name,
		sum(sale_amt) as sale_amt,
		sum(profit) as profit
	from 
		csx_dws.csx_dws_sale_detail_di 
	where 
		sdt>='20230101' and sdt<='20230531'
		and channel_code in('1','7','9')
		and business_type_code in(1)
		and performance_province_name ='北京市'
		-- and classify_middle_name in ('水产')
		and inventory_dc_code !='WB26'
		and customer_code='223402'
	group by 
		substr(sdt,1,6),customer_code,customer_name,classify_middle_name,delivery_type_name
		
		
	select 
		substr(sdt,1,6),delivery_type_name,sum(sale_amt)
	from 
		csx_dws.csx_dws_sale_detail_di 
	where 
		sdt>='20230101' and sdt<='20230531'
		and channel_code in('1','7','9')
		and business_type_code in(1)
		and performance_province_name ='北京市'
		-- and classify_middle_name in ('水产')
		and inventory_dc_code !='WB26'
		and customer_code='223402'
		and goods_code='1466091'
	group by 
	    substr(sdt,1,6),delivery_type_name
		
		
	select 
		substr(a.sdt,1,6) as smonth,f.csx_week,
		a.goods_code,c.goods_name,delivery_type_name,
		sum(sale_amt) sale_amt,
		sum(profit) profit,
		sum(sale_qty) sale_qty,
		sum(sale_cost) sale_cost,
		sum(sale_amt)/sum(sale_qty) sale_sj,
		sum(sale_cost)/sum(sale_qty) sale_cbj  
	from 
		(
		select
			*
		from
			csx_dws.csx_dws_sale_detail_di
		where 
			sdt>='20230101' and sdt<='20230531'
			and channel_code in('1','7','9')
			and business_type_code in(1)
			and performance_province_name ='北京市'
			and classify_middle_name in ('水产')
			and inventory_dc_code !='WB26'
			and delivery_type_name='直送'
			and goods_code='1491318'
		) a 
		left join (select * from csx_dim.csx_dim_basic_goods where sdt='current') c on a.goods_code=c.goods_code 
		left join (select * from csx_dim.csx_dim_basic_date) f on f.calday=a.sdt
	group by 
		substr(a.sdt,1,6),f.csx_week,customer_code,customer_name,
		a.goods_code,c.goods_name,delivery_type_name;
		
		
	select 
		substr(a.sdt,1,6) as smonth,f.csx_week,
		a.goods_code,c.goods_name,delivery_type_name,
		sum(sale_amt) sale_amt,
		sum(profit) profit,
		sum(sale_qty) sale_qty,
		sum(sale_cost) sale_cost,
		if(sum(sale_qty)=0,0,sum(sale_amt)/sum(sale_qty)) sale_sj,
		if(sum(sale_qty)=0,0,sum(sale_cost)/sum(sale_qty)) sale_cbj  
	from 
		(
		select
			*
		from
			csx_dws.csx_dws_sale_detail_di
		where 
			sdt>='20230422' and sdt<='20230531'
			and channel_code in('1','7','9')
			and business_type_code in(1)
			and performance_province_name ='北京市'
			and classify_middle_name in ('水产')
			and inventory_dc_code !='WB26'
			-- and delivery_type_name='配送'
			and goods_code='1491318'
			and customer_code='106775'
		) a 
		left join (select * from csx_dim.csx_dim_basic_goods where sdt='current') c on a.goods_code=c.goods_code 
		left join (select * from csx_dim.csx_dim_basic_date) f on f.calday=a.sdt
	group by 
		substr(a.sdt,1,6),f.csx_week,customer_code,customer_name,
		a.goods_code,c.goods_name,delivery_type_name;
		
		
select
	substr(sdt,1,6) as smonth,customer_code,customer_name,delivery_type_name,inventory_dc_code,
	sum(sale_amt) sale_amt,sum(profit) profit
from
	csx_dws.csx_dws_sale_detail_di
where
	sdt>='20230401' and sdt<='20230531'
	and channel_code in('1','7','9')
	and business_type_code in(1)
	and performance_province_name ='北京市'
	-- and classify_middle_name in ('水产')
	and inventory_dc_code !='WB26'
	and customer_code='223402'
	and delivery_type_name='直送'
group by 
	substr(sdt,1,6),customer_code,customer_name,delivery_type_name,inventory_dc_code
	
select
	substr(sdt,1,6) as smonth,customer_code,customer_name,delivery_type_name,inventory_dc_code,
	sum(sale_amt) sale_amt,sum(profit) profit
from
	csx_dws.csx_dws_sale_detail_di
where
	sdt>='20230401' and sdt<='20230429'
	and channel_code in('1','7','9')
	and business_type_code in(1)
	and performance_province_name ='北京市'
	-- and classify_middle_name in ('水产')
	and inventory_dc_code !='WB26'
	and customer_code='223402'
	and delivery_type_name='直送'
group by 
	substr(sdt,1,6),customer_code,customer_name,delivery_type_name,inventory_dc_code
	
	
	
select
	substr(sdt,1,6) as smonth,customer_code,customer_name,delivery_type_name,inventory_dc_code,
	sum(sale_amt) sale_amt,sum(profit) profit
from
	csx_dws.csx_dws_sale_detail_di
where
	sdt>='20190101' and sdt<='20230607'
	and channel_code in('1','7','9')
	and performance_province_name ='陕西省'
	and customer_code='130364'
	and refund_order_flag=1
group by 
	substr(sdt,1,6),customer_code,customer_name,delivery_type_name,inventory_dc_code
	

drop table if exists csx_analyse_tmp.csx_analyse_tmp_shanxi_th_tj_detail;
create table csx_analyse_tmp.csx_analyse_tmp_shanxi_th_tj_detail
as 
select
	-- substr(sdt,1,6) as smonth,customer_code,customer_name,delivery_type_name,inventory_dc_code,
    *
from
	csx_dws.csx_dws_sale_detail_di
where
	sdt>='20190101' and sdt<='20230607'
	and channel_code in('1','7','9')
	-- and business_type_code in(1)
	and performance_province_name ='陕西省'
	-- and classify_middle_name in ('水产')
	and customer_code='130364'
	and (refund_order_flag=1 or order_channel_code=6)