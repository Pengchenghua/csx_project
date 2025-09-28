-- 1-8月份TOP商品 全国TOP20 省区TOP20

-- 大单品 省区TOP20
--  大单品 省区TOP20
drop table if exists csx_analyse_tmp.csx_analyse_tmp_finance_shenji_01;
create table csx_analyse_tmp.csx_analyse_tmp_finance_shenji_01
as
select
	a.performance_province_name,a.performance_city_name,a.customer_code,b.customer_name,a.smonth,a.channel_name,a.business_type_name,
	b.first_category_name,b.second_category_name,b.third_category_name,c.business_division_name,a.goods_code,c.goods_name,
	c.purchase_group_code,c.purchase_group_name,c.classify_large_name,c.classify_middle_name,c.classify_small_name,
	a.delivery_type_name,c.unit_name,
	a.sale_qty,a.avg_price,a.sale_amt,a.sale_amt_no_tax,a.profit_no_tax,
	a.rn
from
	(
	select
		a.performance_province_name,a.performance_city_name,a.customer_code,a.smonth,a.channel_name,a.business_type_name,a.goods_code,a.delivery_type_name,
		a.sale_qty,a.avg_price,a.sale_amt,a.sale_amt_no_tax,a.profit_no_tax,b.rn
	from
		(
		select
			performance_province_name,performance_city_name,customer_code,substr(sdt,1,6) as smonth,channel_name,business_type_name,goods_code,delivery_type_name,
			sum(sale_qty) as sale_qty,sum(sale_amt)/sum(sale_qty) as avg_price,sum(sale_amt) as sale_amt,sum(sale_amt_no_tax)as sale_amt_no_tax,sum(profit_no_tax)as profit_no_tax
		from
			(
			select
				*
			from
				csx_dws.csx_dws_sale_detail_di
			where 
				sdt>='20221001' and sdt<='20231130'
				and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
				and business_type_code not in (4) -- 不含项目供应商
			) a 
		group by 
			performance_province_name,performance_city_name,customer_code,substr(sdt,1,6),channel_name,business_type_name,goods_code,delivery_type_name
		) a
		join
			(
			select
				performance_province_name,goods_code,sale_amt,rn,performance_city_name
			from
				(
				select
					performance_province_name,performance_city_name,goods_code,sum(sale_amt) as sale_amt,
					row_number()over(partition by performance_province_name,performance_city_name order by sum(sale_amt) desc) as rn
				from
					(
					select
						*
					from
						csx_dws.csx_dws_sale_detail_di
					where 
						sdt>='20221001' and sdt<='20231130'
						and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
						and business_type_code not in (4) -- 不含项目供应商
					) a 
				group by 
					performance_province_name,goods_code,performance_city_name	
				) a 
			where
				rn<=20
			) b on b.goods_code=a.goods_code and b.performance_province_name=a.performance_province_name and a.performance_city_name=b.performance_city_name
	) a 
	left join
		(
		select
			customer_code,customer_name,performance_region_name,performance_province_name,performance_city_name,first_category_name,second_category_name,third_category_name
		from
			csx_dim.csx_dim_crm_customer_info
		where
			sdt='current'
		) b on b.customer_code=a.customer_code
	left join
		(
		select
			goods_code,goods_name,classify_large_name,classify_middle_name,classify_small_name,business_division_name,purchase_group_code,purchase_group_name,unit_name
		from
			csx_dim.csx_dim_basic_goods
		where
			sdt='current'
		) c on c.goods_code=a.goods_code
order by 
	a.rn,a.sale_amt desc		
;
select * from csx_analyse_tmp.csx_analyse_tmp_finance_shenji_01 where performance_province_name='福建省'


-- 大单品 全国TOP20
drop table if exists csx_analyse_tmp.csx_analyse_tmp_finance_shenji_02;
create table csx_analyse_tmp.csx_analyse_tmp_finance_shenji_02
as
select
	a.performance_province_name,a.performance_city_name,a.customer_code,b.customer_name,a.smonth,a.channel_name,a.business_type_name,
	b.first_category_name,b.second_category_name,b.third_category_name,c.business_division_name,a.goods_code,c.goods_name,
	c.purchase_group_code,c.purchase_group_name,c.classify_large_name,c.classify_middle_name,c.classify_small_name,
	a.delivery_type_name,c.unit_name,
	a.sale_qty,a.avg_price,a.sale_amt,a.sale_amt_no_tax,a.profit_no_tax,
	a.rn
from
	(
	select
		a.performance_province_name,a.performance_city_name,a.customer_code,a.smonth,a.channel_name,a.business_type_name,a.goods_code,a.delivery_type_name,
		a.sale_qty,a.avg_price,a.sale_amt,a.sale_amt_no_tax,a.profit_no_tax,b.rn
	from
		(
		select
			performance_province_name,performance_city_name,customer_code,substr(sdt,1,6) as smonth,channel_name,business_type_name,goods_code,delivery_type_name,
			sum(sale_qty) as sale_qty,sum(sale_amt)/sum(sale_qty) as avg_price,sum(sale_amt) as sale_amt,sum(sale_amt_no_tax)as sale_amt_no_tax,sum(profit_no_tax)as profit_no_tax
		from
			(
			select
				*
			from
				csx_dws.csx_dws_sale_detail_di
			where 
				sdt>='20230101' and sdt<='20230831'
				and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
				and business_type_code not in (4) -- 不含项目供应商
			) a 
		group by 
			performance_province_name,performance_city_name,customer_code,substr(sdt,1,6),channel_name,business_type_name,goods_code,delivery_type_name
		) a
		join
			(
			select
				goods_code,sale_amt,rn
			from
				(
				select
					goods_code,sum(sale_amt) as sale_amt,
					row_number()over(order by sum(sale_amt) desc) as rn
				from
					(
					select
						*
					from
						csx_dws.csx_dws_sale_detail_di
					where 
						sdt>='20230101' and sdt<='20230831'
						and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
						and business_type_code not in (4) -- 不含项目供应商
					) a 
				group by 
					goods_code	
				) a 
			where
				rn<=20
			) b on b.goods_code=a.goods_code
	) a 
	left join
		(
		select
			customer_code,customer_name,performance_region_name,performance_province_name,performance_city_name,first_category_name,second_category_name,third_category_name
		from
			csx_dim.csx_dim_crm_customer_info
		where
			sdt='current'
		) b on b.customer_code=a.customer_code
	left join
		(
		select
			goods_code,goods_name,classify_large_name,classify_middle_name,classify_small_name,business_division_name,purchase_group_code,purchase_group_name,unit_name
		from
			csx_dim.csx_dim_basic_goods
		where
			sdt='current'
		) c on c.goods_code=a.goods_code
order by 
	a.rn,a.sale_amt desc		
;
select * from csx_analyse_tmp.csx_analyse_tmp_finance_shenji_02;

-- ===================================================================================================================================================================

-- 大单品 省区TOP20
drop table if exists csx_analyse_tmp.csx_analyse_tmp_finance_shenji_03;
create table csx_analyse_tmp.csx_analyse_tmp_finance_shenji_03
as
select
	a.performance_province_name,a.performance_city_name,a.customer_code,b.customer_name,a.smonth,a.channel_name,a.business_type_name,
	b.first_category_name,b.second_category_name,b.third_category_name,c.business_division_name,a.goods_code,c.goods_name,
	c.purchase_group_code,c.purchase_group_name,c.classify_large_name,c.classify_middle_name,c.classify_small_name,
	a.delivery_type_name,c.unit_name,
	a.sale_qty,a.avg_price,a.sale_amt,a.sale_amt_no_tax,a.profit_no_tax,
	a.rn
from
	(
	select
		a.performance_province_name,a.performance_city_name,a.customer_code,a.smonth,a.channel_name,a.business_type_name,a.goods_code,a.delivery_type_name,
		a.sale_qty,a.avg_price,a.sale_amt,a.sale_amt_no_tax,a.profit_no_tax,b.rn
	from
		(
		select
			performance_province_name,performance_city_name,customer_code,substr(sdt,1,6) as smonth,channel_name,business_type_name,goods_code,delivery_type_name,
			sum(sale_qty) as sale_qty,sum(sale_amt)/sum(sale_qty) as avg_price,sum(sale_amt) as sale_amt,sum(sale_amt_no_tax)as sale_amt_no_tax,sum(profit_no_tax)as profit_no_tax
		from
			(
			select
				*
			from
				csx_dws.csx_dws_sale_detail_di
			where 
				sdt>='20230101' and sdt<='20230831'
				and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
				and business_type_code not in (4) -- 不含项目供应商
			) a 
		group by 
			performance_province_name,performance_city_name,customer_code,substr(sdt,1,6),channel_name,business_type_name,goods_code,delivery_type_name
		) a
		join
			(
			select
				smonth,performance_province_name,goods_code,sale_amt,rn
			from
				(
				select
					substr(sdt,1,6) as smonth,performance_province_name,goods_code,sum(sale_amt) as sale_amt,
					row_number()over(partition by substr(sdt,1,6),performance_province_name order by sum(sale_amt) desc) as rn
				from
					(
					select
						*
					from
						csx_dws.csx_dws_sale_detail_di
					where 
						sdt>='20230101' and sdt<='20230831'
						and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
						and business_type_code not in (4) -- 不含项目供应商
					) a 
				group by 
					substr(sdt,1,6),performance_province_name,goods_code	
				) a 
			where
				rn<=5
			) b on b.goods_code=a.goods_code and b.performance_province_name=a.performance_province_name and b.smonth=a.smonth
	) a 
	left join
		(
		select
			customer_code,customer_name,performance_region_name,performance_province_name,performance_city_name,first_category_name,second_category_name,third_category_name
		from
			csx_dim.csx_dim_crm_customer_info
		where
			sdt='current'
		) b on b.customer_code=a.customer_code
	left join
		(
		select
			goods_code,goods_name,classify_large_name,classify_middle_name,classify_small_name,business_division_name,purchase_group_code,purchase_group_name,unit_name
		from
			csx_dim.csx_dim_basic_goods
		where
			sdt='current'
		) c on c.goods_code=a.goods_code
order by 
	a.rn,a.sale_amt desc		
;
select * from csx_analyse_tmp.csx_analyse_tmp_finance_shenji_03


-- 大单品 全国TOP20
drop table if exists csx_analyse_tmp.csx_analyse_tmp_finance_shenji_04;
create table csx_analyse_tmp.csx_analyse_tmp_finance_shenji_04
as
select
	a.performance_province_name,a.performance_city_name,a.customer_code,b.customer_name,a.smonth,a.channel_name,a.business_type_name,
	b.first_category_name,b.second_category_name,b.third_category_name,c.business_division_name,a.goods_code,c.goods_name,
	c.purchase_group_code,c.purchase_group_name,c.classify_large_name,c.classify_middle_name,c.classify_small_name,
	a.delivery_type_name,c.unit_name,
	a.sale_qty,a.avg_price,a.sale_amt,a.sale_amt_no_tax,a.profit_no_tax,
	a.rn
from
	(
	select
		a.performance_province_name,a.performance_city_name,a.customer_code,a.smonth,a.channel_name,a.business_type_name,a.goods_code,a.delivery_type_name,
		a.sale_qty,a.avg_price,a.sale_amt,a.sale_amt_no_tax,a.profit_no_tax,b.rn
	from
		(
		select
			performance_province_name,performance_city_name,customer_code,substr(sdt,1,6) as smonth,channel_name,business_type_name,goods_code,delivery_type_name,
			sum(sale_qty) as sale_qty,sum(sale_amt)/sum(sale_qty) as avg_price,sum(sale_amt) as sale_amt,sum(sale_amt_no_tax)as sale_amt_no_tax,sum(profit_no_tax)as profit_no_tax
		from
			(
			select
				*
			from
				csx_dws.csx_dws_sale_detail_di
			where 
				sdt>='20230101' and sdt<='20230831'
				and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
				and business_type_code not in (4) -- 不含项目供应商
			) a 
		group by 
			performance_province_name,performance_city_name,customer_code,substr(sdt,1,6),channel_name,business_type_name,goods_code,delivery_type_name
		) a
		join
			(
			select
				smonth,goods_code,sale_amt,rn
			from
				(
				select
					substr(sdt,1,6) as smonth,goods_code,sum(sale_amt) as sale_amt,
					row_number()over(partition by substr(sdt,1,6) order by sum(sale_amt) desc) as rn
				from
					(
					select
						*
					from
						csx_dws.csx_dws_sale_detail_di
					where 
						sdt>='20230101' and sdt<='20230831'
						and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
						and business_type_code not in (4) -- 不含项目供应商
					) a 
				group by 
					substr(sdt,1,6),goods_code	
				) a 
			where
				rn<=5
			) b on b.goods_code=a.goods_code and b.smonth=a.smonth
	) a 
	left join
		(
		select
			customer_code,customer_name,performance_region_name,performance_province_name,performance_city_name,first_category_name,second_category_name,third_category_name
		from
			csx_dim.csx_dim_crm_customer_info
		where
			sdt='current'
		) b on b.customer_code=a.customer_code
	left join
		(
		select
			goods_code,goods_name,classify_large_name,classify_middle_name,classify_small_name,business_division_name,purchase_group_code,purchase_group_name,unit_name
		from
			csx_dim.csx_dim_basic_goods
		where
			sdt='current'
		) c on c.goods_code=a.goods_code
order by 
	a.rn,a.sale_amt desc		
;
select * from csx_analyse_tmp.csx_analyse_tmp_finance_shenji_04;

