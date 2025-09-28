
-- 客户商品池 
drop table if exists csx_analyse_tmp.csx_analyse_tmp_customer_product;
create table if not exists  csx_analyse_tmp.csx_analyse_tmp_customer_product as 
select
  		*,
		case 
		when data_source=0 then '手动添加'
		when data_source=1 then '客户订单'
		when data_source=2 then '报价'
		when data_source=3 then '商品池模板'
		when data_source=4 then '必售商品'
		when data_source=5 then '商品池模板替换'
		when data_source=6 then '新品'
		when data_source=7 then '基础商品池'	
		when data_source=8 then 'CRM换品'
		when data_source=9 then '9'
		when data_source=10 then '运营换品'
		when data_source=11 then '核单换品'	
		when data_source=12 then '无清单商品匹配'	
		when data_source=13 then '临时码替换'
		else data_source end as data_source_name 	-- 数据来源	
  		-- row_number() over(partition by customer_code, product_code order by update_time desc) r_num
  	from csx_ods.csx_ods_b2b_mall_prod_yszx_customer_product_df
  	where create_time>='2025-01-01'
  	and shipper_code='YHCSX'
  	and base_product_status=0  -- 0正常 3停售 6退场 7停购
	
;

-- 

with tmp_sale_sku as 
	(select substr (sdt,1,6) smonth, 
		performance_region_name,
		performance_province_name,
		performance_city_name,
		customer_code,
		customer_name,
		goods_code,
		goods_name,
		sum(sale_qty) sale_qty,
		sum(sale_amt) sale_amt,
		sum(profit) profit
	from 
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt>= '20250501'and sdt <='20250509'
		and business_type_code=1  
		and shipper_code='YHCSX' 
		and performance_province_name='广东广州'
	    and order_channel_code not in (4,6,5) -- 剔除所有异常
	    and refund_order_flag<>1 
	    -- and delivery_type_code<>2 
	group by substr (sdt,1,6),performance_province_name,performance_city_name,customer_code,customer_name,goods_code,goods_name
	
	),
tmp_product_goods AS 
(select
  		*,
		case 
		when data_source=0 then '手动添加'
		when data_source=1 then '客户订单'
		when data_source=2 then '报价'
		when data_source=3 then '商品池模板'
		when data_source=4 then '必售商品'
		when data_source=5 then '商品池模板替换'
		when data_source=6 then '新品'
		when data_source=7 then '基础商品池'	
		when data_source=8 then 'CRM换品'
		when data_source=9 then '9'
		when data_source=10 then '运营换品'
		when data_source=11 then '核单换品'	
		when data_source=12 then '无清单商品匹配'	
		when data_source=13 then '临时码替换'
		else data_source end as data_source_name 	-- 数据来源	
  		-- row_number() over(partition by customer_code, product_code order by update_time desc) r_num
  	from csx_ods.csx_ods_b2b_mall_prod_yszx_customer_product_df
  	where create_time>='2025-01-01'
  	and shipper_code='YHCSX'
  	and base_product_status=0  -- 0正常 3停售 6退场 7停购
	)
tmp_product_sku as 
( select customer_code,
    count(1) as sku
    -- count(case when data_source_name in('手动添加','报价') then 1 end) as sdtj_bj,
    from  tmp_product_goods
    group by customer_code
     
)
select a.*,b.sku from 
(select performance_province_name,
	customer_code,
	customer_name,
	count(distinct goods_code ) sale_sku 
    from tmp_sale_sku 
    where smonth='202505'
    group by  performance_province_name,
	customer_code,
	customer_name
    ) a 
left join 
(select customer_code,
    sku 
from tmp_product_sku 
) b on a.customer_code=b.customer_code
 


-- 客户商品池SKU
with tmp_sale_sku as 
	(select substr (sdt,1,6) smonth, *
	from 
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt>= '20250501'and sdt <='20250509'
		and business_type_code=1  
		and shipper_code='YHCSX' 
		and performance_province_name='广东广州'
	    and order_channel_code not in (4,6,5) -- 剔除所有异常
	    and refund_order_flag<>1 
	    and delivery_type_code<>2 
	),
tmp_product_sku as 
(select 
    a.customer_code,
    a.goods_code ,	
    d.customer_name,
    d.performance_region_name,
	d.performance_province_name,
	d.performance_city_name
from 
    (select customer_code,
    product_code as goods_code
    -- count(case when data_source_name in('手动添加','报价') then 1 end) as sdtj_bj,
    from  csx_analyse_tmp.csx_analyse_tmp_customer_product
    group by customer_code,
    product_code
    )a
	left join 
		(select 
			customer_code,
			customer_name,
			performance_region_name,
			performance_province_name,
			performance_city_name 
		from csx_dim.csx_dim_crm_customer_info 
		where sdt='current' 
		and shipper_code='YHCSX'
		) d
		on a.customer_code=d.customer_code 
)
select performance_province_name,customer_code,customer_name,sum(if(flag=1,1,0)) product_sku,sum(if(flag=0,1,0)) as no_product_sku
    from 
(
select a.*, coalesce(flag,0 ) flag 
from 
(select  performance_province_name,
    customer_code,customer_name,
    goods_code 
    from tmp_sale_sku 
    where smonth='202505'
    group by  performance_province_name,customer_code,customer_name,goods_code
    ) a 
left join 
(select customer_code,
    goods_code ,
    1 as flag 
from tmp_product_sku 
) b on a.customer_code=b.customer_code and a.goods_code=b.goods_code
 ) a 
 group by performance_province_name,customer_code,customer_name



-- 
with tmp_sale_sku as 
	(select substr (sdt,1,6) smonth, *
	from 
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt>= '20250501'and sdt <='20250509'
		and business_type_code=1  
		and shipper_code='YHCSX' 
		and performance_province_name='广东广州'
	    and order_channel_code not in (4,6,5) -- 剔除所有异常
	    and refund_order_flag<>1 
	    and delivery_type_code<>2 
	),
tmp_product_sku as 
(select 
    a.customer_code,
    a.goods_code ,	
    d.customer_name,
    d.performance_region_name,
	d.performance_province_name,
	d.performance_city_name
from 
    (select customer_code,
    product_code as goods_code
    -- count(case when data_source_name in('手动添加','报价') then 1 end) as sdtj_bj,
    from  csx_analyse_tmp.csx_analyse_tmp_customer_product
    group by customer_code,
    product_code
    )a
	left join 
		(select 
			customer_code,
			customer_name,
			performance_region_name,
			performance_province_name,
			performance_city_name 
		from csx_dim.csx_dim_crm_customer_info 
		where sdt='current' 
		and shipper_code='YHCSX'
		) d
		on a.customer_code=d.customer_code 
)
select performance_province_name,customer_code,customer_name,a.goods_code,goods_name,classify_large_name,classify_middle_name,sale_amt,profit,profit/sale_amt profit_rate,flag 
-- sum(if(flag=1,1,0)) product_sku,sum(if(flag=0,1,0)) as no_product_sku
    from 
(
select a.*, coalesce(flag,0 ) flag 
from 
(select  performance_province_name,customer_code,customer_name,goods_code,sum(sale_amt) sale_amt,sum(profit)profit
    from tmp_sale_sku 
    where smonth='202505'
    group by  performance_province_name,customer_code,customer_name,goods_code
    ) a 
left join 
(select customer_code,
    goods_code ,
    1 as flag 
from tmp_product_sku 
) b on a.customer_code=b.customer_code and a.goods_code=b.goods_code
 ) a 
left join 
(select goods_code,goods_name,classify_large_name,classify_middle_name from   csx_dim.csx_dim_basic_goods where sdt='current') b on a.goods_code=b.goods_code
;




-- 客户商品池SKU
with tmp_sale_sku as 
	(select substr (sdt,1,6) smonth, *
	from 
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt>= '20250501'and sdt <='20250512'
		and business_type_code=1  
		and shipper_code='YHCSX' 
		and performance_province_name='广东广州'
	    and order_channel_code not in (4,6,5) -- 剔除所有异常
	    and refund_order_flag<>1 
	    and delivery_type_code<>2 
	),
tmp_product_sku as 
(select 
    a.customer_code,
    a.goods_code ,	
    d.customer_name,
    d.performance_region_name,
	d.performance_province_name,
	d.performance_city_name
from 
    (select customer_code,
    product_code as goods_code
    -- count(case when data_source_name in('手动添加','报价') then 1 end) as sdtj_bj,
    from  csx_analyse_tmp.csx_analyse_tmp_gz_customer_product
    group by customer_code,
    product_code
    )a
	left join 
		(select 
			customer_code,
			customer_name,
			performance_region_name,
			performance_province_name,
			performance_city_name 
		from csx_dim.csx_dim_crm_customer_info 
		where sdt='current' 
		and shipper_code='YHCSX'
		) d
		on a.customer_code=d.customer_code 
)
select sdt, performance_province_name,a.customer_code,customer_name,sum(if(flag=1,1,0)) product_sku,sum(if(flag=0,1,0)) as no_product_sku,c.sku
    from 
(
select a.*, coalesce(flag,0 ) flag 
from 
(select sdt, performance_province_name,
    customer_code,customer_name,
    goods_code 
    from tmp_sale_sku 
    where smonth='202505'
    group by sdt, performance_province_name,customer_code,customer_name,goods_code
    ) a 
left join 
(select customer_code,
    goods_code ,
    1 as flag 
from tmp_product_sku 
) b on a.customer_code=b.customer_code and a.goods_code=b.goods_code
 ) a 
 left join 
 (select customer_code,
    count(1) as sku
    -- count(case when data_source_name in('手动添加','报价') then 1 end) as sdtj_bj,
    from  csx_analyse_tmp.csx_analyse_tmp_gz_customer_product
    group by customer_code
    )c on a.customer_code=c.customer_code
 group by sdt, performance_province_name,a.customer_code,customer_name,c.sku

  
  