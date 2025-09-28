select   belong_region_code  ,
  belong_region_name  ,
  basic_performance_province_code ,
  basic_performance_province_name ,
  basic_performance_city_name ,
  a.goods_code , 
  bar_code   ,
  goods_name , 
  unit_name , 
  brand_name , 
  a.classify_large_code , 
  a.classify_large_name , 
  a.classify_middle_code , 
  a.classify_middle_name ,
  supplier_code, 
  supplier_name, 
  sum(receive_qty ) receive_qty , 
  sum(receive_amt) receive_amt ,
  if(sum(receive_qty)=0,0,sum(receive_amt)/sum(receive_qty)) avg_cost,
  sum(case when a.order_business_type_name='是' then receive_qty end ) as jd_qty,
  sum(case when a.order_business_type_name='是' then receive_amt end ) jd_amt,
  sum(case when a.is_central_tag='1' and a. source_type_name not in('临时地采','临时加单','客户直送','紧急采购' )  then receive_qty end ) as jc_qty,
  sum(case when a.is_central_tag='1' and a. source_type_name not in('临时地采','临时加单','客户直送','紧急采购' )  then receive_amt end ) jc_amt,
  sum(case when a.is_central_tag='1' and a. source_type_name not in('临时地采','临时加单','客户直送','紧急采购' )  then receive_qty end ) as jc_qty,
  if(sum(case when a.is_central_tag='1' and a. source_type_name not in('临时地采','临时加单','客户直送','紧急采购' )  then receive_qty end )=0,0,sum(case when a.is_central_tag='1' and a. source_type_name not in('临时地采','临时加单','客户直送','紧急采购' )  then receive_amt end )/sum(case when a.is_central_tag='1' and a. source_type_name not in('临时地采','临时加单','客户直送','紧急采购' )  then receive_qty end )) jc_avg_cost,
  sum(case when a. source_type_name  in('临时地采','临时加单','客户直送','紧急采购' ) and a.is_central_tag !='1' then receive_qty end ) as lc_qty,
  sum(case when a. source_type_name  in('临时地采','临时加单','客户直送','紧急采购' ) and a.is_central_tag !='1' then receive_amt end ) lc_amt,
  if(sum(case when a. source_type_name  in('临时地采','临时加单','客户直送','紧急采购' ) and a.is_central_tag !='1' then receive_qty end )=0,0, sum(case when a. source_type_name  in('临时地采','临时加单','客户直送','紧急采购' ) and a.is_central_tag !='1' then receive_amt end )/sum(case when a. source_type_name  in('临时地采','临时加单','客户直送','紧急采购' ) and a.is_central_tag !='1' then receive_qty end )) as lc_avg_cost,
  sum(case when a. source_type_name not in('临时地采','临时加单','客户直送','紧急采购' )and a.is_central_tag !='1' and a.order_business_type_name !='是' then receive_qty end ) as qt_qty,
  sum(case when a. source_type_name not in('临时地采','临时加单','客户直送','紧急采购' )and a.is_central_tag !='1' and a.order_business_type_name!='是' then receive_amt end ) qt_amt,
  if(sum(case when a. source_type_name not in('临时地采','临时加单','客户直送','紧急采购' )and a.is_central_tag !='1' and a.order_business_type_name !='是' then receive_qty end )=0,0,sum(case when a. source_type_name not in('临时地采','临时加单','客户直送','紧急采购' )and a.is_central_tag !='1' and a.order_business_type_name!='是' then receive_amt end )/sum(case when a. source_type_name not in('临时地采','临时加单','客户直送','紧急采购' )and a.is_central_tag !='1' and a.order_business_type_name !='是' then receive_qty end )) as qt_avg_cost,
  if(b.goods_code is null ,'否','是') as is_jd,
  if(c.goods_code is null ,'否','是') as is_jc,
  if(d.goods_code is null ,'否','是') as is_lc
  --  rank_aa
 from   csx_analyse_tmp.csx_analyse_tmp_entry_goods  a 
left  join (select distinct goods_code  from   csx_analyse_tmp.csx_analyse_tmp_entry_goods where order_business_type_name='是' ) b on a.goods_code=b.goods_code  -- 基地标识
left  join (select distinct goods_code  from   csx_analyse_tmp.csx_analyse_tmp_entry_goods where is_central_tag='1' ) c on a.goods_code=c.goods_code  -- 集采标识 
left  join (select distinct goods_code  from   csx_analyse_tmp.csx_analyse_tmp_entry_goods where source_type_name  in('临时地采','临时加单','客户直送','紧急采购' )) d on a.goods_code=d.goods_code  -- 临采标识 
-- join csx_analyse_tmp.csx_analyse_tmp_goods_top_20 b on a.goods_code=b.goods_code
where source_type_name not in ('城市服务商','联营直送','项目合伙人')
    and is_supplier_dc='是'
    AND receive_sdt>='20230501'
	and a.classify_middle_name='蔬菜'
	and b.goods_code is not null
group by 
-- rank_aa,
  belong_region_code  ,
  belong_region_name  ,
  basic_performance_province_code ,
  basic_performance_province_name ,
  a.goods_code , 
  bar_code   ,
  goods_name , 
  unit_name , 
  brand_name , 
  a.classify_large_code , 
  a.classify_large_name , 
  a.classify_middle_code , 
  a.classify_middle_name,
  supplier_code, 
  supplier_name, 
  basic_performance_city_name,
  if(b.goods_code is null ,'否','是'),
if(c.goods_code is null ,'否','是'),
if(d.goods_code is null ,'否','是');
-- ===============================================================================================================================================================================
select   
	belong_region_code,belong_region_name,basic_performance_province_code,basic_performance_province_name,
	a.classify_large_code,a.classify_large_name,a.classify_middle_code,a.classify_middle_name,
	sum(receive_qty ) receive_qty , 
	sum(receive_amt) receive_amt
from   
	csx_analyse_tmp.csx_analyse_tmp_entry_goods  a 
	left join (select distinct goods_code from csx_analyse_tmp.csx_analyse_tmp_entry_goods where order_business_type_name='是') b on a.goods_code=b.goods_code  -- 基地标识
where 
	source_type_name not in ('城市服务商','联营直送','项目合伙人')
    and is_supplier_dc='是'
    AND receive_sdt>='20230501'
	and a.classify_middle_name='蔬菜'
	and b.goods_code is not null
	and basic_performance_province_name='重庆'
group by 
	belong_region_code,belong_region_name,basic_performance_province_code,basic_performance_province_name,
	a.classify_large_code,a.classify_large_name,a.classify_middle_code,a.classify_middle_name
;

select 
	b.belong_region_code,
	b.belong_region_name,
	b.basic_performance_province_code,
	b.basic_performance_province_name,
	a.classify_large_code,
	a.classify_large_name,
	a.classify_middle_code,
	a.classify_middle_name,
	sum(a.sale_amt) as sale_amt,
	sum(a.profit) as profit
from 
	(
	select 
		* 
	from 
		csx_dws.csx_dws_sale_detail_di 
	where 
		sdt>='20230501' and sdt<='20230514'
		and channel_code in('1','7','9')
		and business_type_code in(1)
		and classify_middle_name='蔬菜'
	) a 
	join 
		(
		select 
			shop_code,basic_performance_province_code,basic_performance_province_name,basic_performance_city_code,basic_performance_city_name ,belong_region_code,belong_region_name
		from
			(
			select
				*
			from
				csx_dim.csx_dim_shop
			where
				sdt='current' and shop_low_profit_flag !=1
			)a 
			left join 
				(
				select 
					distinct belong_region_code,belong_region_name,performance_province_code,performance_province_name,performance_city_code,performance_city_name
				from 
					csx_dim.csx_dim_basic_performance_attribution
				) b on a.basic_performance_city_code= b.performance_city_code
		) b on b.shop_code=a.inventory_dc_code
group by 
	b.belong_region_code,
	b.belong_region_name,
	b.basic_performance_province_code,
	b.basic_performance_province_name,
	a.classify_large_code,
	a.classify_large_name,
	a.classify_middle_code,
	a.classify_middle_name
-- ===============================================================================================================================================================================
select 
	b.belong_region_code,
	b.belong_region_name,
	b.basic_performance_province_code,
	b.basic_performance_province_name,
	a.classify_large_code,
	a.classify_large_name,
	a.classify_middle_code,
	a.classify_middle_name,
	a.goods_code,
	c.goods_name,
	sum(a.sale_amt) as sale_amt,
	sum(a.profit) as profit
from 
	(
	select 
		* 
	from 
		csx_dws.csx_dws_sale_detail_di 
	where 
		sdt>='20230501' and sdt<='20230514'
		and channel_code in('1','7','9')
		and business_type_code in(1)
		and classify_middle_name='蔬菜'
	) a 
	join 
		(
		select 
			shop_code,basic_performance_province_code,basic_performance_province_name,basic_performance_city_code,basic_performance_city_name ,belong_region_code,belong_region_name
		from
			(
			select
				*
			from
				csx_dim.csx_dim_shop
			where
				sdt='current' and shop_low_profit_flag !=1
			)a 
			left join 
				(
				select 
					distinct belong_region_code,belong_region_name,performance_province_code,performance_province_name,performance_city_code,performance_city_name
				from 
					csx_dim.csx_dim_basic_performance_attribution
				) b on a.basic_performance_city_code= b.performance_city_code
		) b on b.shop_code=a.inventory_dc_code
	left join
		(
		select 
			* 
		from 
			csx_dim.csx_dim_basic_goods 
		where sdt='current'
		) c on a.goods_code=c.goods_code 
group by 
	b.belong_region_code,
	b.belong_region_name,
	b.basic_performance_province_code,
	b.basic_performance_province_name,
	a.classify_large_code,
	a.classify_large_name,
	a.classify_middle_code,
	a.classify_middle_name,
	a.goods_code,
	c.goods_name
;

-- 基地采购分析
select   
	belong_region_code,belong_region_name,basic_performance_province_code,basic_performance_province_name,
	a.classify_large_code,a.classify_large_name,a.classify_middle_code,a.classify_middle_name,
	sum(receive_amt) receive_amt,
	sum(case when a.order_business_type_name='是' then receive_amt end ) jd_amt
from   
	csx_analyse_tmp.csx_analyse_tmp_entry_goods  a 
	left join (select distinct goods_code from csx_analyse_tmp.csx_analyse_tmp_entry_goods where order_business_type_name='是') b on a.goods_code=b.goods_code  -- 基地标识
where 
	source_type_name not in ('城市服务商','联营直送','项目合伙人')
    and is_supplier_dc='是'
    AND receive_sdt>='20230501'
	and a.classify_middle_name='蔬菜'
group by 
	belong_region_code,belong_region_name,basic_performance_province_code,basic_performance_province_name,
	a.classify_large_code,a.classify_large_name,a.classify_middle_code,a.classify_middle_name
;

-- 商品top分析
select
	a.belong_region_code,a.belong_region_name,a.basic_performance_province_code,a.basic_performance_province_name,
	a.classify_large_code,a.classify_large_name,a.classify_middle_code,a.classify_middle_name,a.goods_code,a.goods_name,
	a.receive_amt,a.avg_cost,a.jd_amt,a.jd_rate,a.jd_avg_cost,a.lc_avg_cost,a.lc_amt,a.lc_qty,b.sale_amt,b.profit
from	
	(
	select   
		belong_region_code,belong_region_name,basic_performance_province_code,basic_performance_province_name,
		a.classify_large_code,a.classify_large_name,a.classify_middle_code,a.classify_middle_name,a.goods_code,a.goods_name,
		sum(receive_amt) receive_amt,
		if(sum(receive_qty)=0,0,sum(receive_amt)/sum(receive_qty)) avg_cost,
		sum(case when a.order_business_type_name='是' then receive_amt end ) jd_amt,
		sum(case when a.order_business_type_name='是' then receive_amt end )/sum(receive_amt) as jd_rate,
		if(sum(case when a.order_business_type_name='是' then receive_qty end )=0,0,
			sum(case when a.order_business_type_name='是' then receive_amt end )/sum(case when a.order_business_type_name='是' then receive_qty end )) jd_avg_cost,
		if(sum(case when a. source_type_name  in('临时地采','临时加单','客户直送','紧急采购' ) and a.is_central_tag !='1' then receive_qty end )=0,0, sum(case when a. source_type_name  in('临时地采','临时加单','客户直送','紧急采购' ) and a.is_central_tag !='1' then receive_amt end )/sum(case when a. source_type_name  in('临时地采','临时加单','客户直送','紧急采购' ) and a.is_central_tag !='1' then receive_qty end )) as lc_avg_cost,
		sum(case when a. source_type_name  in('临时地采','临时加单','客户直送','紧急采购' ) and a.is_central_tag !='1' then receive_amt end ) as lc_amt,
		sum(case when a. source_type_name  in('临时地采','临时加单','客户直送','紧急采购' ) and a.is_central_tag !='1' then receive_qty end ) as lc_qty
	from   
		csx_analyse_tmp.csx_analyse_tmp_entry_goods  a 
		left join (select distinct goods_code from csx_analyse_tmp.csx_analyse_tmp_entry_goods where order_business_type_name='是') b on a.goods_code=b.goods_code  -- 基地标识
	where 
		source_type_name not in ('城市服务商','联营直送','项目合伙人')
		and is_supplier_dc='是'
		AND receive_sdt>='20230501'
		and a.classify_middle_name='蔬菜'
	group by 
		belong_region_code,belong_region_name,basic_performance_province_code,basic_performance_province_name,
		a.classify_large_code,a.classify_large_name,a.classify_middle_code,a.classify_middle_name,a.goods_code,a.goods_name
	having
		sum(receive_amt)>=10000
		and sum(case when a.order_business_type_name='是' then receive_amt end )>0
	) a 
	left join
		(
		select 
			b.belong_region_code,b.belong_region_name,b.basic_performance_province_code,b.basic_performance_province_name,
			a.classify_large_code,a.classify_large_name,a.classify_middle_code,a.classify_middle_name,a.goods_code,
			sum(a.sale_amt) as sale_amt,
			sum(a.profit) as profit
		from 
			(
			select 
				* 
			from 
				csx_dws.csx_dws_sale_detail_di 
			where 
				sdt>='20230501' and sdt<='20230514'
				and channel_code in('1','7','9')
				and business_type_code in(1)
				and classify_middle_name='蔬菜'
			) a 
			join 
				(
				select 
					shop_code,basic_performance_province_code,basic_performance_province_name,basic_performance_city_code,basic_performance_city_name ,belong_region_code,belong_region_name
				from
					(
					select
						*
					from
						csx_dim.csx_dim_shop
					where
						sdt='current' and shop_low_profit_flag !=1
					)a 
					left join 
						(
						select 
							distinct belong_region_code,belong_region_name,performance_province_code,performance_province_name,performance_city_code,performance_city_name
						from 
							csx_dim.csx_dim_basic_performance_attribution
						) b on a.basic_performance_city_code= b.performance_city_code
				) b on b.shop_code=a.inventory_dc_code
		group by 
			b.belong_region_code,b.belong_region_name,b.basic_performance_province_code,b.basic_performance_province_name,
			a.classify_large_code,a.classify_large_name,a.classify_middle_code,a.classify_middle_name,a.goods_code
		) b on b.basic_performance_province_code=a.basic_performance_province_code and b.goods_code=a.goods_code
;

