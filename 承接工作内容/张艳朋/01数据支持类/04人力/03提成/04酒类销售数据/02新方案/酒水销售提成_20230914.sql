-- 酒水销售提成
drop table if exists csx_analyse_tmp.csx_analyse_tmp_js_tc_00;
create table csx_analyse_tmp.csx_analyse_tmp_js_tc_00 
as 
select 
	* 
from 
	csx_dws.csx_dws_sale_detail_di 
where 
	sdt between '${start_day}' and '${end_day}' and channel_code in('1','7','9') 
	and goods_code in ('8718','8708','8649','840509','800682','909970','1017653','1316198','1316196','1316197','1386902','1502064','1565060',
		'1565062','1615101','1612617','1549320','1572400','8621','8623','48985','8624','8625','8613','14631','8619','226574','227048','1474840','1474814','1474831',
		'1474830','1275847','1131104','1474769','1339644')
;

-- 四种茅台酒客户清单
drop table if exists csx_analyse_tmp.csx_analyse_tmp_js_tc_01;
create table csx_analyse_tmp.csx_analyse_tmp_js_tc_01 
as 
select distinct customer_code from csx_analyse_tmp.csx_analyse_tmp_js_tc_00 where goods_code in ('8718','8708','8649','840509')
;

-- 搭售酒的客户清单
drop table if exists csx_analyse_tmp.csx_analyse_tmp_js_tc_02;
create table csx_analyse_tmp.csx_analyse_tmp_js_tc_02 
as 
select distinct customer_code from csx_analyse_tmp.csx_analyse_tmp_js_tc_00 where goods_code in ('800682','909970','1017653','1316198','1316196','1316197','1386902','1502064','1565060',
	'1565062','1615101','1612617','1549320','1572400','8621','8623','48985','8624','8625','8613','14631','8619','226574','227048','1474840','1474814','1474831',
	'1474830','1275847','1131104','1474769','1339644')
;

-- 都有销售的客户清单
drop table if exists csx_analyse_tmp.csx_analyse_tmp_js_tc_03;
create table csx_analyse_tmp.csx_analyse_tmp_js_tc_03 
as 
select distinct a.customer_code from csx_analyse_tmp.csx_analyse_tmp_js_tc_01 a join csx_analyse_tmp.csx_analyse_tmp_js_tc_02 b on b.customer_code=a.customer_code
;

-- 客户+商品维度
drop table if exists csx_analyse_tmp.csx_analyse_tmp_js_tc_04;
create table csx_analyse_tmp.csx_analyse_tmp_js_tc_04 
as 
select
	concat('${start_day}','-','${end_day}') as qj,
	a.performance_province_name,b.sales_user_number,b.sales_user_name,
	a.customer_code,b.customer_name,a.goods_code,c.goods_name,a.classify_middle_name,
	sum(a.sale_amt) as sale_amt,
	sum(a.profit) as profit,
	sum(a.profit)/abs(sum(a.sale_amt)) as profit_rate
from 
	(
	select
		performance_province_name,goods_code,classify_large_name,classify_middle_name,classify_small_name,sdt,customer_code,sale_amt,profit
	from
		csx_analyse_tmp.csx_analyse_tmp_js_tc_00
	) a 
	left join
		(
		select
			customer_code,customer_name,performance_region_name,performance_province_name,performance_city_name,first_category_name,second_category_name,third_category_name,sales_user_number,sales_user_name
		from
			csx_dim.csx_dim_crm_customer_info
		where
			sdt='${end_day}'
		) b on b.customer_code=a.customer_code
	left join
		(
		select
			goods_code,goods_name,classify_large_name,classify_middle_name,classify_small_name
		from
			csx_dim.csx_dim_basic_goods
		where
			sdt='current'
		) c on c.goods_code=a.goods_code
	-- 取两种酒都有销售的客户
	join csx_analyse_tmp.csx_analyse_tmp_js_tc_03 d on d.customer_code=a.customer_code
group by 
	a.performance_province_name,b.sales_user_number,b.sales_user_name,
	a.customer_code,b.customer_name,a.goods_code,c.goods_name,a.classify_middle_name
;
select * from csx_analyse_tmp.csx_analyse_tmp_js_tc_04;

-- 客户维度汇总
select
	qj,performance_province_name,sales_user_number,sales_user_name,customer_code,customer_name,sale_amt,profit,profit_rate,
	case when profit_rate>0.12 then profit*0.08
		when profit_rate>0 then sale_amt*0.01
		when profit_rate<=0 then sale_amt*0.007
	end as tc
from
	(
	select
		qj,
		performance_province_name,sales_user_number,sales_user_name,
		customer_code,customer_name,
		sum(a.sale_amt) as sale_amt,
		sum(a.profit) as profit,
		sum(a.profit)/abs(sum(a.sale_amt)) as profit_rate
	from 
		csx_analyse_tmp.csx_analyse_tmp_js_tc_04 a 
	group by 
		qj,
		performance_province_name,sales_user_number,sales_user_name,
		customer_code,customer_name
	) a 
;
-- =========================================================================================================================================================================
-- 绩效
select
	concat('${start_day}','-','${end_day}') as qj,
	a.performance_province_name,b.sales_user_number,b.sales_user_name,
	a.customer_code,b.customer_name,
	c.classify_middle_name,c.classify_small_name,a.goods_code,c.goods_name,
	sum(a.sale_amt) as sale_amt,
	sum(a.profit) as profit
from 
	(
	select
		performance_province_name,goods_code,goods_name,classify_large_name,classify_middle_name,classify_small_name,sdt,customer_code,sale_amt,profit
	from
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt between '${start_day}' and '${end_day}'
		and channel_code in('1','7','9')
		and goods_code in ('8649','8708','8718','800682','909970','1017653','1316198','1316196','1316197','1386902','1128274','232454','1288902','13532','6939','13523',
		'478192','451849','1502064','1565060','1565062','1615101','1612617','1092910','1092359','825041','259561','1418636','1436800','825971','1422935','1549320',
		'1572400','8621','8623','48985','8624','8625','8613','14631','8619','226574','227048','1474814','1479155','1474830','1131103','1131104','1474769','1339644',
		'1474840','1474831','1275847','1565070','1565061','840509','1576411','1360226','1275847','1565070','1565061','1576411','1360226','1479313','1479155','1131103')
	) a 
	left join
		(
		select
			customer_code,customer_name,performance_region_name,performance_province_name,performance_city_name,first_category_name,second_category_name,third_category_name,sign_company_code,
			sales_user_number,sales_user_name,customer_address_full
		from
			csx_dim.csx_dim_crm_customer_info
		where
			sdt='${end_day}'
		) b on b.customer_code=a.customer_code
	left join
		(
		select
			goods_code,goods_name,classify_large_name,classify_middle_name,classify_small_name
		from
			csx_dim.csx_dim_basic_goods
		where
			sdt='current'
		) c on c.goods_code=a.goods_code
group by 
	a.performance_province_name,b.sales_user_number,b.sales_user_name,
	a.customer_code,b.customer_name,
	c.classify_middle_name,c.classify_small_name,a.goods_code,c.goods_name