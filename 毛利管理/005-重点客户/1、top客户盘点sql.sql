drop table if exists csx_analyse_tmp.csx_analyse_tmp_customer_goods_tmp; 
create table if not exists csx_analyse_tmp.csx_analyse_tmp_customer_goods_tmp as 
with csx_analyse_tmp_link_customer_info as 
(select
  a.*,
  b.target_rate,
  price_name,
  if(a.bloc_code='297509',210,sale_base) sale_base,
  target_profit
from
  csx_analyse_tmp.csx_analyse_tmp_link_customer_info a
  left join csx_analyse_tmp.csx_analyse_tmp_link_customer_profit_target b on a.bloc_code = b.bloc_code
)
select 
	t1.*,
	(
		cus_cla_last_week_sale_amt*(cus_cla_this_week_profit/abs(cus_cla_this_week_sale_amt)-cus_cla_last_week_profit/abs(cus_cla_last_week_sale_amt))
		+
		(cus_cla_this_week_sale_amt-cus_cla_last_week_sale_amt)*(cus_cla_this_week_profit/abs(cus_cla_this_week_sale_amt)-cus_this_week_profit/abs(cus_this_week_sale_amt))
	)/cus_last_week_sale_amt as cus_cla_profitlv_eff,-- 客户品类毛利影响

	(
		nvl(last_week_sale_amt,0)*(nvl(this_week_profitlv,last_week_profitlv)-nvl(last_week_profitlv,this_week_profitlv))
		+
		(nvl(this_week_sale_amt,0)-nvl(last_week_sale_amt,0))*(nvl(this_week_profitlv,last_week_profitlv)-cus_cla_this_week_profit/abs(cus_cla_this_week_sale_amt))
	)/cus_cla_last_week_sale_amt as cus_cla_good_profitlv_eff,-- 客户品类商品毛利影响

	(
		nvl(last_week_sale_amt,0)*(nvl(this_week_profitlv,last_week_profitlv)-nvl(last_week_profitlv,this_week_profitlv))
		+
		(nvl(this_week_sale_amt,0)-nvl(last_week_sale_amt,0))*(nvl(this_week_profitlv,last_week_profitlv)-cus_this_week_profit/abs(cus_this_week_sale_amt))
	)/cus_last_week_sale_amt as cus_goods_profitlv_eff,-- 客户品类毛利影响,

	t2.this_week_receive_qty,
	t2.this_week_receive_price,

	t2.last_week_receive_qty,
	t2.last_week_receive_price,

	t3.min_this_week_receive_price 
from 
(select 
	t.*,

	sum(t.last_week_sale_amt)over(partition by t.performance_city_name,t.customer_code) as cus_last_week_sale_amt,
	sum(t.last_week_profit)over(partition by t.performance_city_name,t.customer_code) as cus_last_week_profit,

	sum(t.this_week_sale_amt)over(partition by t.performance_city_name,t.customer_code) as cus_this_week_sale_amt,
	sum(t.this_week_profit)over(partition by t.performance_city_name,t.customer_code) as cus_this_week_profit, 

	sum(t.last_week_sale_amt)over(partition by t.performance_city_name,t.customer_code,t.classify_middle_name) as cus_cla_last_week_sale_amt,
	sum(t.last_week_profit)over(partition by t.performance_city_name,t.customer_code,t.classify_middle_name) as cus_cla_last_week_profit,

	sum(t.this_week_sale_amt)over(partition by t.performance_city_name,t.customer_code,t.classify_middle_name) as cus_cla_this_week_sale_amt,
	sum(t.this_week_profit)over(partition by t.performance_city_name,t.customer_code,t.classify_middle_name) as cus_cla_this_week_profit 
from 
	(select 
		a.performance_region_name,
		a.performance_province_name,
		a.performance_city_name,
		bloc_code,
		bloc_name,
		a.customer_code,
		a.customer_name,
		b.classify_middle_name,
		a.goods_code,
		b.goods_name ,
		sum(case when a.sdt>='20251206' and a.sdt<='20251212' then sale_amt end) as last_week_sale_amt,
		sum(case when a.sdt>='20251206' and a.sdt<='20251212' then profit end) as last_week_profit,
		sum(case when a.sdt>='20251206' and a.sdt<='20251212' then sale_qty end) as last_week_sale_qty,
		sum(case when a.sdt>='20251206' and a.sdt<='20251212' then profit end)/abs(sum(case when a.sdt>='20251206' and a.sdt<='20251212' then sale_amt end)) as last_week_profitlv,
		sum(case when a.sdt>='20251206' and a.sdt<='20251212' then sale_amt end)/sum(case when a.sdt>='20251206' and a.sdt<='20251212' then sale_qty end) as last_week_sale_price,
		sum(case when a.sdt>='20251206' and a.sdt<='20251212' then sale_amt-profit end)/sum(case when a.sdt>='20251206' and a.sdt<='20251212' then sale_qty end) as last_week_cost_price,

		sum(case when a.sdt>='20251213' and a.sdt<='20251217' then sale_amt end) as this_week_sale_amt,
		sum(case when a.sdt>='20251213' and a.sdt<='20251217' then profit end) as this_week_profit,
		sum(case when a.sdt>='20251213' and a.sdt<='20251217' then sale_qty end) as this_week_sale_qty,
		sum(case when a.sdt>='20251213' and a.sdt<='20251217' then profit end)/abs(sum(case when a.sdt>='20251213' and a.sdt<='20251217' then sale_amt end)) as this_week_profitlv,
		sum(case when a.sdt>='20251213' and a.sdt<='20251217' then sale_amt end)/sum(case when a.sdt>='20251213' and a.sdt<='20251217' then sale_qty end) as this_week_sale_price,
		sum(case when a.sdt>='20251213' and a.sdt<='20251217' then sale_amt-profit end)/sum(case when a.sdt>='20251213' and a.sdt<='20251217' then sale_qty end) as this_week_cost_price 
	from 
		(select a.*, b.bloc_code,b.bloc_name
		from csx_dws.csx_dws_sale_detail_di a 
		join csx_analyse_tmp_link_customer_info b  on a.customer_code=b.customer_code
		where sdt>='20251206' 
		and sdt<='20251217' 
		and business_type_code=1 
		and order_channel_code not in (4,5,6) 
		and refund_order_flag<>1 
		-- and customer_code in ('243348','252038','265077','250926','183893','276632','276636','276605','278490','276637','233646','252028','249799','258144','259930',
		-- '115080','223402','225238','226207','106775','112554','220106','223283','241458','217596','131129','131187','249548','250879','255475','124524','255101','254013','258134','222798','160081','117412','156736','257658','249962','122129',
		-- '126387','263986','275371','211834','254068','258261','269286','281122','164512','256667','189765','235752') 
		) a 
		left join 
		(select * 
		from csx_dim.csx_dim_basic_goods 
		where sdt='current'
		) b 
		on a.goods_code=b.goods_code 
		left join 
		(select
		    code as type,
		    max(name) as name,
		    max(extra) as extra 
		from csx_dim.csx_dim_basic_topic_dict_df
		where parent_code = 'direct_delivery_type' 
		group by code 
		) g 
		on a.direct_delivery_type=g.type 
	where g.extra='采购参与' 
	group by 
		a.performance_region_name,
		a.performance_province_name,
		a.performance_city_name,
		a.customer_code,
		a.customer_name,
		b.classify_middle_name,
		a.goods_code,
		b.goods_name ,
		bloc_code,
		bloc_name
	) t 
) t1 
left join 
-- 本周平均入库价
csx_analyse_tmp.tmp_dc_goods_received_city_ky t2 
on t1.performance_city_name=t2.performance_city_name and t1.goods_code=t2.goods_code   
left join 
-- 大区最低入库价
(select 
	performance_region_name,
	goods_code,
	min(this_week_receive_price) as min_this_week_receive_price 
from csx_analyse_tmp.tmp_dc_goods_received_city_ky   
group by performance_region_name,goods_code 
) t3 
on t1.performance_region_name=t3.performance_region_name and t1.goods_code=t3.goods_code
;


drop table if exists csx_analyse_tmp.csx_analyse_tmp_final_table; 
create table if not exists csx_analyse_tmp.csx_analyse_tmp_final_table as 
select 
	a.performance_region_name,
	a.performance_province_name,
	a.performance_city_name,
	bloc_code,
	bloc_name,
	a.customer_code,
	a.customer_name,

	a.cus_last_week_sale_amt,
	a.cus_last_week_profit,
	a.cus_last_week_profit/abs(a.cus_last_week_sale_amt) as cus_last_week_profitlv,

	a.cus_this_week_sale_amt,
	a.cus_this_week_profit,
	a.cus_this_week_profit/abs(a.cus_this_week_sale_amt) as cus_this_week_profitlv, 
	a.cus_this_week_profit/abs(a.cus_this_week_sale_amt)-a.cus_last_week_profit/abs(a.cus_last_week_sale_amt) as cus_week_profitlv_diff,

	a.cus_cla_last_week_sale_amt,
	a.cus_cla_last_week_profit,
	a.cus_cla_last_week_profit/abs(a.cus_cla_last_week_sale_amt) as cus_cla_last_week_profitlv,

	a.cus_cla_this_week_sale_amt,
	a.cus_cla_this_week_profit,
	a.cus_cla_this_week_profit/abs(a.cus_cla_this_week_sale_amt) as cus_cla_this_week_profitlv, 
	a.cus_cla_this_week_profit/abs(a.cus_cla_this_week_sale_amt)-a.cus_cla_last_week_profit/abs(a.cus_cla_last_week_sale_amt) as cus_cla_week_profitlv_diff,

	a.classify_middle_name,
	a.goods_code,
	a.goods_name,

	a.last_week_sale_amt,
	a.last_week_profit,
	a.last_week_sale_qty,
	a.last_week_sale_price,
	a.last_week_cost_price,
	a.last_week_profit/abs(a.last_week_sale_amt) as last_week_profitlv,
	a.this_week_sale_amt,
	a.this_week_profit,
	a.this_week_sale_qty,
	a.this_week_sale_price,
	a.this_week_cost_price,
	a.this_week_profit/abs(a.this_week_sale_amt) as this_week_profitlv, 
	a.this_week_profit/abs(a.this_week_sale_amt)-a.last_week_profit/abs(a.last_week_sale_amt) as week_profitlv_diff,

	dense_rank()over(partition by a.performance_city_name,a.customer_code order by a.cus_cla_profitlv_eff) as cus_cla_pm,
	dense_rank()over(partition by a.performance_city_name,a.customer_code,a.classify_middle_name order by a.cus_cla_good_profitlv_eff) as cus_cla_goods_pm,
	dense_rank()over(partition by a.performance_city_name,a.customer_code order by a.cus_goods_profitlv_eff) as cus_goods_pm,

	a.cus_cla_profitlv_eff,-- 客户品类毛利影响
	a.cus_cla_good_profitlv_eff,-- 客户品类商品毛利影响
	a.cus_goods_profitlv_eff,-- 客户品类毛利影响,

	a.last_week_receive_qty*a.last_week_receive_price as last_week_receive_amt,
	a.last_week_receive_price,

	a.this_week_receive_qty*a.this_week_receive_price as this_week_receive_amt,
	a.this_week_receive_price,

	(a.this_week_receive_price-a.last_week_receive_price)/a.last_week_receive_price as week_receive_price_diff,

	a.min_this_week_receive_price, 
	(case when a.this_week_receive_price>a.min_this_week_receive_price then '是' else '否' end) as if_di, 
	(case when a.this_week_receive_price>a.min_this_week_receive_price then (a.this_week_receive_price-a.min_this_week_receive_price)*a.this_week_sale_qty end) as ts_profit,
	
	-- 各城市入库价
	b.this_week_receive_price_fz,
	b.this_week_receive_price_sz,
	b.this_week_receive_price_gz,
	b.this_week_receive_price_bj,
	b.this_week_receive_price_sjz,
	b.this_week_receive_price_xa,
	b.this_week_receive_price_zz,

	b.this_week_receive_price_cq,
	b.this_week_receive_price_wz,
	b.this_week_receive_price_qj,
	b.this_week_receive_price_szx,
	b.this_week_receive_price_cd,
	b.this_week_receive_price_yb,
	b.this_week_receive_price_gy,

	b.this_week_receive_price_sj,
	b.this_week_receive_price_shsz,
	b.this_week_receive_price_nj,
	b.this_week_receive_price_yc,
	b.this_week_receive_price_hz,
	b.this_week_receive_price_nb,
	b.this_week_receive_price_tz,
	b.this_week_receive_price_hf,
	b.this_week_receive_price_fy,
	b.this_week_receive_price_wh  
from 
csx_analyse_tmp.csx_analyse_tmp_customer_goods_tmp a 
left join 
(select * from csx_analyse.csx_analyse_scm_order_market_price_wf where if_jgbj_order='是') b 
on a.performance_city_name=b.performance_city_name and a.goods_code=b.goods_code 
;


select 
	a.performance_region_name as `大区`,
	a.performance_province_name as `省区`,
	a.performance_city_name as `城市`,
	bloc_code as `集团编码`,
	bloc_name as `集团名称`,
	a.customer_code as `客户编码`,
	a.customer_name as `客户名称`,

	a.cus_last_week_sale_amt as `客户上周销售额`,
	a.cus_last_week_profit as `客户上周毛利额`,
	a.cus_last_week_profitlv as `客户上周毛利率`,

	a.cus_this_week_sale_amt as `客户本周销售额`,
	a.cus_this_week_profit as `客户本周毛利额`,
	a.cus_this_week_profitlv as `客户本周毛利率`, 
	a.cus_week_profitlv_diff as `客户毛利率周环比`,
	
	cus_cla_pm as `客户品类影响排名`,
	cus_cla_goods_pm as `客户品类商品影响排名（用于挑客户品类重点商品）`,
	cus_goods_pm as `客户商品影响排名（用于挑客户重点商品）`,

	a.cus_cla_profitlv_eff as `客户品类毛利影响`,-- 客户品类毛利影响
	a.cus_cla_good_profitlv_eff as `客户品类商品毛利影响`,-- 客户品类商品毛利影响
	a.cus_goods_profitlv_eff as `客户商品毛利影响`,-- 客户品类毛利影响,

	a.cus_cla_last_week_sale_amt as `客户品类上周销售额`,
	a.cus_cla_last_week_profit as `客户品类上周毛利额`,
	a.cus_cla_last_week_profitlv as `客户品类上周毛利率`,

	a.cus_cla_this_week_sale_amt as `客户品类本周销售额`,
	a.cus_cla_this_week_profit as `客户品类本周毛利额`,
	a.cus_cla_this_week_profitlv as `客户品类本周毛利率`, 
	a.cus_cla_week_profitlv_diff as `客户品类毛利率周环比`,

	a.classify_middle_name as `管理中类`,
	a.goods_code as `商品编码`,
	a.goods_name as `商品名称`,

	a.last_week_sale_amt as `上周正常销售额`,
	a.last_week_profit as `上周正常毛利额`,
	a.last_week_sale_qty as `上周正常销量`,
	a.last_week_sale_price as `上周正常售价`,
	a.last_week_cost_price as `上周正常成本`,
	a.last_week_profitlv as `上周正常毛利率`,
	a.this_week_sale_amt as `本周正常销售额`,
	a.this_week_profit as `本周正常毛利额`,
	a.this_week_sale_qty as `本周正常销量`,
	a.this_week_sale_price as `本周正常售价`,
	a.this_week_cost_price as `本周正常成本`,
	a.this_week_profitlv as `本周正常毛利率`, 
	a.week_profitlv_diff as `毛利率周环比`,

	a.last_week_receive_amt as `上周入库额`,
	a.last_week_receive_price as `上周入库价`,

	a.this_week_receive_amt as `本周入库额`,
	a.this_week_receive_price as `本周入库价`,

	a.week_receive_price_diff as `入库价周环比`,

	a.min_this_week_receive_price as `本周大区最低入库价`, 
	if_di as `城市入库价是否高于大区最低入库价`, 
	ts_profit as `降本后提升毛利额`,

	this_week_receive_price_fz as  `福州入库价`,
	this_week_receive_price_sz as  `深圳入库价`,
	this_week_receive_price_gz as  `广州入库价`,

	this_week_receive_price_bj as `北京入库价`,
	this_week_receive_price_sjz as `石家庄入库价`,
	this_week_receive_price_xa as `西安入库价`,
	this_week_receive_price_zz as `郑州入库价`,

	this_week_receive_price_cq as `重庆主城入库价`,
	this_week_receive_price_wz as `万州入库价`,
	this_week_receive_price_qj as `黔江入库价`,
	this_week_receive_price_szx as `石柱县入库价`,
	this_week_receive_price_cd as `成都入库价`,
	this_week_receive_price_yb as `宜宾入库价`,
	this_week_receive_price_gy as `贵阳入库价`,

	this_week_receive_price_sj as `松江入库价`,
	this_week_receive_price_shsz as `苏州入库价`,
	this_week_receive_price_nj as `南京主城入库价`,
	this_week_receive_price_yc as `盐城入库价`,
	this_week_receive_price_hz as `杭州入库价`,
	this_week_receive_price_nb as `宁波入库价`,
	this_week_receive_price_tz as `台州入库价`,
	this_week_receive_price_hf as `合肥入库价`,
	this_week_receive_price_fy as `阜阳入库价`,
	this_week_receive_price_wh as `武汉入库价` 
from  csx_analyse_tmp.csx_analyse_tmp_final_table a 