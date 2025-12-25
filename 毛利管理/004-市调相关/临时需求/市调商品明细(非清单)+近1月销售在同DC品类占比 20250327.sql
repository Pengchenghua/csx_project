
-- 市调商品明细(非清单)+近1月销售在同DC品类占比 20250327

with dc_goods_sale_bf1m as 
(
select 
	a.inventory_dc_code,a.goods_code,
	b.goods_name,
	b.classify_large_code,b.classify_large_name, -- 管理大类
	b.classify_middle_code,b.classify_middle_name,-- 管理中类
	b.classify_small_code,b.classify_small_name,-- 管理小类
	sum(sale_amt) as sale_amt,
	sum(a.sale_qty) sale_qty,
	sum(a.profit)/abs(sum(a.sale_amt)) as profit_rate,
	count(distinct a.customer_code) customer_cnt,
	count(distinct a.sdt) as day_cnt,  
	count(a.goods_code) as goods_cnt
from 
  (
  select *
  from csx_dws.csx_dws_sale_detail_di
  -- where sdt>='20250227' and sdt<='20250326'
  where sdt between regexp_replace(add_months('${sdt_date}',-1),'-','') and regexp_replace(date_sub('${sdt_date}',1),'-','')
  -- and channel_code in ('1','7','9')
  and business_type_code =1 -- 仅日配 业务类型编码(1.日配单 2.福利单 3.批发内购 4.城市服务商 5.省区大宗 6.BBC)
  and delivery_type_code in (1) -- 剔除直送和自提 配送类型编码：1-配送 2-直送 3-自提 4-直通 11-同城配送 12-快递配送 13-一件代发
  and shipper_code = 'YHCSX'
  )a
left join -- -- -- 商品信息
  (
    select
    goods_code,
    regexp_replace(regexp_replace(goods_name,'\n',''),'\r','') as goods_name,
    purchase_group_code as department_id,purchase_group_name as department_name,    
    classify_large_code,classify_large_name, -- 管理大类
    classify_middle_code,classify_middle_name,-- 管理中类
    classify_small_code,classify_small_name-- 管理小类
    from csx_dim.csx_dim_basic_goods
    where sdt = 'current'
  )b on a.goods_code = b.goods_code  
  group by 
  a.inventory_dc_code,a.goods_code,
  b.goods_name,
  b.classify_large_code,b.classify_large_name, -- 管理大类
  b.classify_middle_code,b.classify_middle_name,-- 管理中类
  b.classify_small_code,b.classify_small_name
),

dc_goods_sale_bf1m_rno as
(
select 
  inventory_dc_code,goods_code,
  goods_name,
  classify_large_code,classify_large_name, -- 管理大类
  classify_middle_code,classify_middle_name,-- 管理中类
  classify_small_code,classify_small_name,-- 管理小类
  sale_amt,goods_cnt,dc_mclass_sale_rno,
 sum(sale_amt) over(partition by inventory_dc_code,classify_middle_name order by dc_mclass_sale_rno rows between unbounded preceding and 1 preceding ) dc_mclass_sale_lj_lp,
 sum(sale_amt) over(partition by inventory_dc_code,classify_middle_name order by dc_mclass_sale_rno rows between unbounded preceding and 0 preceding ) dc_mclass_sale_lj 
from 
(
select 
  inventory_dc_code,goods_code,
  goods_name,
  classify_large_code,classify_large_name, -- 管理大类
  classify_middle_code,classify_middle_name,-- 管理中类
  classify_small_code,classify_small_name,-- 管理小类
  sale_amt,goods_cnt,
  row_number() over(partition by inventory_dc_code,classify_middle_name order by sale_amt desc) as dc_mclass_sale_rno
from dc_goods_sale_bf1m
)a
),

dc_goods_sale_bf1m_zb as
(
select a.*,
 b.dc_mclass_sale_all,
 a.dc_mclass_sale_lj/b.dc_mclass_sale_all as dc_mclass_sale_zb,
 a.dc_mclass_sale_lj_lp/b.dc_mclass_sale_all as dc_mclass_sale_lp_zb
from dc_goods_sale_bf1m_rno a 
left join 
(
select inventory_dc_code,classify_middle_name,
sum(sale_amt) as dc_mclass_sale_all
from dc_goods_sale_bf1m_rno
group by inventory_dc_code,classify_middle_name
)b on a.inventory_dc_code=b.inventory_dc_code and a.classify_middle_name=b.classify_middle_name
)

select 
a.flag as `类别`,
a.performance_region_name as `大区`,
a.performance_province_name as `省区`,
a.performance_city_name as `城市`,
a.location_code as `DC`,
a.classify_large_name as `一级分类`,
a.classify_middle_name as `二级分类`,
a.goods_code as `商品编码`,
a.goods_name as `商品名称`,
a.regionalized_goods_name as `区域化名称`,
a.unit_name as `单位`,
a.standard as `规格`,
a.product_code as `原料编码`,
regexp_replace(regexp_replace(a.product_name,'\n',''),'\r','') as `原料名称`,
a.product_source as `原料来源`,
b.sale_amt as `销售额`,
b.goods_cnt as `下单次数`,
a.is_market_research_price_ty as `是否有通用市调价格`,
concat(nvl(a.shop_code_name_ty,''),nvl(a.shop_code_name_bom,'')) as `通用市调地点`,
a.is_market_research_price_yc as `是否有云超价格`,
a.is_market_research_price_all as `是否有市调价格`,
a.is_qd_list as `是否清单商品`,
case 
when (a.classify_large_name in('调味杂货','非食品','其他','日配食品','休闲食品') or a.classify_middle_name='熟食烘焙') then '非加工品'
when (a.goods_name like '%cm%' and a.goods_name not like '%饺子%' and a.goods_name not like '%馄饨%')
or a.goods_name like '%mm%'
or a.goods_name like '%去骨%'
or a.goods_name like '%杀净%'
or a.goods_name like '%净杀%'
or a.goods_name like '%活杀%'
or a.goods_name like '%鲜杀%'
or a.goods_name like '%净膛%'
or (a.goods_name like '%净%' and a.classify_middle_name='蔬菜')
or a.goods_name like '%去皮%'
or a.goods_name like '%去头尾%'
or a.goods_name like '%去把%'
or a.goods_name like '%去蒂%'
or a.goods_name like '%去根%'
or a.goods_name like '%去内脏%'
or a.goods_name like '%三去%'
or a.goods_name like '%切块%'
or a.goods_name like '%切片%'
or a.goods_name like '%切丝%'
or a.goods_name like '%肉馅%'
or a.goods_name like '%切片%'
or (a.goods_name like '%丁%' and a.classify_large_name in('肉禽水产','蔬菜水果'))
then '加工品' else '非加工品' end as `是否加工商品`,
a.base_product_status_name as `商品状态`,
a.swt as `swt`,
-- b.sale_amt as sale_amt_new,
b.dc_mclass_sale_all,
b.dc_mclass_sale_lj,
b.dc_mclass_sale_zb,
b.dc_mclass_sale_lj_lp,
b.dc_mclass_sale_lp_zb
from 
(
select *
from csx_analyse.csx_analyse_price_market_research_cover_rate_nqd_wi
where swt='202517'
)a 
left join dc_goods_sale_bf1m_zb b on a.location_code=b.inventory_dc_code and a.goods_code=b.goods_code
;



