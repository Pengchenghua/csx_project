-- csx_dwd_sss_kp_apply_goods_group_detail_di 发票明细开票金额根据开票号与对账单号关联销售单号

-- 发票明细开票金额
select
  a.order_code,
  company_code,
  company_name,
  goods_code,	
  goods_name,
  total_amount,
  a.tax_amt as head_tax_amt,
  b.total_amt,
  b.tax_amt,
  sdt
from
  csx_dwd.csx_dwd_sss_invoice_di a 
  join 
  (select order_code,
    d.goods_code,	
    e.goods_name,
    classify_large_code,
	classify_large_name,
	-- 管理大类
	classify_middle_code,
	classify_middle_name,
	-- 管理中类
	classify_small_code,
	classify_small_name ,-- 管理小类
	total_amt,
	tax_amt 
  from csx_dwd.csx_dwd_sss_kp_apply_goods_group_di d 
  join (
			select goods_code,
				goods_name,
				purchase_group_code,
				purchase_group_name,
				classify_large_code,
				classify_large_name,
				-- 管理大类
				classify_middle_code,
				classify_middle_name,
				-- 管理中类
				classify_small_code,
				classify_small_name -- 管理小类
			from csx_dim.csx_dim_basic_goods
			where sdt = 'current'
			and goods_name like '%鸡蛋%'
			and classify_middle_code='B0103'
		) e on d.goods_code = e.goods_code
  where sdt>='20210101'
    	and is_delete = '0'
    	
  ) b on a.order_code=b.order_code
where
  company_code = '2115'
  and sdt >= '20210101'

  select
  a.order_code,
  company_code,
  company_name,
  goods_code,	
  goods_name,
  total_amount,
  a.tax_amt as head_tax_amt,
  b.total_amt,
  b.tax_amt,
  sdt
from
  csx_dwd.csx_dwd_sss_invoice_di a 
  join 
  (select order_code,
    d.goods_code,	
    e.goods_name,
    classify_large_code,
	classify_large_name,
	-- 管理大类
	classify_middle_code,
	classify_middle_name,
	-- 管理中类
	classify_small_code,
	classify_small_name ,-- 管理小类
	total_amt,
	tax_amt 
  from csx_dwd.csx_dwd_sss_kp_apply_goods_group_di d 
  join (
			select goods_code,
				goods_name,
				purchase_group_code,
				purchase_group_name,
				classify_large_code,
				classify_large_name,
				-- 管理大类
				classify_middle_code,
				classify_middle_name,
				-- 管理中类
				classify_small_code,
				classify_small_name -- 管理小类
			from csx_dim.csx_dim_basic_goods
			where sdt = 'current'
			and goods_name like '%鸡蛋%'
			and classify_middle_code='B0103'
		) e on d.goods_code = e.goods_code
  where sdt>='20210101'
    	and is_delete = '0'
    	
  ) b on a.order_code=b.order_code
where
  company_code = '2115'
  and sdt >= '20210101'
  ;

-- 商品销售明细鸡蛋
-- BBC渠道

with a as (
  select 
        substr(sdt,1,4) as syear,
        substr(sdt,1,6) as smonth,
        performance_region_name,
        performance_province_name,
        customer_code,
        goods_code,
        company_code,
        sum(sale_qty)sale_qty,
        sum(sale_amt)/10000 as sale_amt, 
        sum(profit)/10000 as profit 
    from csx_dws.csx_dws_sale_detail_di  
    where company_code='2115'
        and goods_name like '%鸡蛋%'
		and classify_middle_code='B0103'
		and sdt>='20210101'
    group by 
        substr(sdt,1,4) ,
        substr(sdt,1,6) ,
        performance_region_name,
        performance_province_name,
        customer_code,
        goods_code,
        company_code
        ) 
    select performance_province_name,
        company_code,
        a.goods_code,
       	goods_name,
       	unit_name,
        brand_name,
        classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name,
        classify_small_code	,
        classify_small_name,
        sum(sale_qty) sale_qty,
        sum(sale_amt)sale_amt ,
        sum(profit)profit,
        sum(profit)/sum(sale_amt) as profit_rate
    from a 
    join 
    (select goods_code,
        goods_name,
        unit_name,
        brand_name,
        classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name,
        classify_small_code	,
        classify_small_name
    from 
        csx_dim.csx_dim_basic_goods
    where sdt='current')
        b on a.goods_code=b.goods_code
    group by performance_province_name,
        a.goods_code,
        goods_name,unit_name,
        brand_name,classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name,
        classify_small_code	,
        classify_small_name,
        company_code;

		
-- 发票商品明细
(
	select d.order_code as order_no,
		d.total_amt as total_amount,
		e.purchase_group_code,
		e.purchase_group_name,
		e.classify_large_code,
		e.classify_large_name,
		-- 管理大类
		e.classify_middle_code,
		e.classify_middle_name,
		-- 管理中类
		e.classify_small_code,
		e.classify_small_name -- 管理小类		
	from (
			select *,
				--  id排名取最新一条
				row_number() over(
					partition by id
					order by update_time desc
				) as id_rank -- from csx_ods.csx_ods_csx_b2b_sss_sss_kp_apply_goods_group_di
				-- where (sdt>='20200101' or sdt='19990101')
			from csx_dwd.csx_dwd_sss_kp_apply_goods_group_di
			where sdt >= '20200101'
				and is_delete = '0'
		) d
		left join (
			select goods_code,
				goods_name,
				purchase_group_code,
				purchase_group_name,
				classify_large_code,
				classify_large_name,
				-- 管理大类
				classify_middle_code,
				classify_middle_name,
				-- 管理中类
				classify_small_code,
				classify_small_name -- 管理小类
			from csx_dim.csx_dim_basic_goods
			where sdt = 'current'
		) e on d.goods_code = e.goods_code
	where d.id_rank = 1
),