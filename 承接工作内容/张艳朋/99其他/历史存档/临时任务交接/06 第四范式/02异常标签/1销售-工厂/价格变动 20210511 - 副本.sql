-- 统计日期
set current_day = regexp_replace(date_sub(current_date, 1), '-', '');
-- 当月月初
set current_start_day = regexp_replace(trunc(date_sub(current_date, 1), 'MM'),'-','');
-- 库存操作起始日期
set wms_start_day = regexp_replace(add_months(trunc(date_sub(current_date, 1), 'MM'), -11),'-','');
-- 当前月
set currnet_month = substr(${hiveconf:current_day}, 1, 6);

--临时表1：明细数据 销售表中取各维度数据+成本、采购报价、中台报价、售价、销售额、毛利等，关联批次取批次库存成本价、关联工厂取原料价
drop table csx_tmp.tmp_goods_salezp;
create temporary table csx_tmp.tmp_goods_salezp
as
select 
  a.sdt,--日期
  a.credential_no,--凭证号
  a.region_code,--大区编码
  a.region_name,--大区
  a.province_code,--省区编码
  a.province_name,--省区
  a.city_group_code,--城市组编码
  a.city_group_name,--城市组
  a.dc_code, --DC编码
  f.shop_name as dc_name,  --DC名称
  a.customer_no,--客户编码
  d.customer_name,--客户名称
  a.goods_code,--商品编码
  regexp_replace(regexp_replace(e.goods_name,'\n',''),'\r','') as goods_name,--商品名称
  e.unit,--单位
  e.unit_name,--单位名称
  e.department_id dept_id,--课组编码
  e.department_name dept_name,--课组名称
  e.classify_middle_code,--管理中类编码
  e.classify_middle_name,--管理中类名称
  case when e.division_code in ('10','11') then '11'
  	   when e.division_code in ('12','13','14','15') then '12'
  	   else '' end as division_code, --部类编码 
  case when e.division_code in ('10','11') then '生鲜'
  	   when e.division_code in ('12','13','14','15') then '食百'
  	   else '' end as division_name,--部类名称
  a.is_factory_goods_desc,--是否工厂加工商品
  case when c.fact_price is not null then '是' end as is_fact, --是否有原料价
  case when purchase_price_flag='1' then '是' end as is_purchase,  --是否有采购报价
  sum(a.sales_qty) sales_qty,--销售数量
  sum(coalesce(c.fact_price,0)*a.sales_qty) fact_value, --原料金额
  sum(coalesce(c.cost_price_0,0)*a.sales_qty) cost_value_0,  --批次库存成本
  sum(coalesce(a.cost_price,0)*a.sales_qty) cost_value,--成本金额
  sum(coalesce(a.purchase_price,0)*a.sales_qty) purchase_value,--采购成本
  sum(coalesce(a.middle_office_price,0)*a.sales_qty) middle_office_value,  --中台成本
  sum(a.sales_value) sales_value,--销售额
  sum(a.sales_cost) sales_cost,--销售成本
  sum(a.profit) profit--毛利
from 
  (
    select 
      sdt,
	  split(id, '&')[0] as credential_no,
      region_code,
      region_name,
      province_code,
      province_name,
	  city_group_code,
	  city_group_name,
	  dc_code, 
      customer_no,
      customer_name,
      goods_code,
      goods_name,
	  is_factory_goods_desc,
      sales_qty,
      sales_value,
      sales_cost,
      profit,
	  purchase_price_flag,
      cost_price,
      case when purchase_price_flag='1' then purchase_price end as purchase_price,
      middle_office_price,
      sales_price
    from csx_dw.dws_sale_r_d_detail 
    where sdt >= ${hiveconf:current_start_day} and sdt <= ${hiveconf:current_day} 
	and channel_code in ('1', '7', '9')
	and business_type_code ='1'
	and sales_type<>'fanli'
	and return_flag<>'X'
	and province_name='重庆市'
  )a 
  left outer join 
  (
    select
	  b.goods_code,
	  b.credential_no,
	  sum(b.qty) as qty,
	  sum(b.price*b.qty)/sum(b.qty) cost_price_0,  --多批次平均库存成本价
	  sum(c.fact_price*b.qty)/sum(case when c.fact_price is not null then b.qty end) fact_price --原料价
	from 
	--批次操作明细表
	(
	  select
	  	goods_code,
	  	credential_no,
	  	source_order_no,
	  	sum(qty) as qty,
		sum(amt)/sum(qty) price
	  from csx_dw.dws_wms_r_d_batch_detail
	  where sdt >= ${hiveconf:wms_start_day} 
	  and move_type in ('107A', '108A')
	  group by goods_code, credential_no, source_order_no
    )b 
	--工厂加工表
    left outer join 
    (
      select 
      	goods_code,
      	order_code,
        sum(fact_values)/sum(goods_reality_receive_qty) as fact_price --原料价
      from csx_dw.dws_mms_r_a_factory_order
      where sdt >= ${hiveconf:wms_start_day} and mrp_prop_key in('3061','3010')
      group by goods_code, order_code
    )c on b.source_order_no = c.order_code and b.goods_code = c.goods_code
	group by b.goods_code,b.credential_no
  )c on a.goods_code = c.goods_code and a.credential_no = c.credential_no
  --客户信息表
  left outer join 
  (
    select 
		customer_no,
		customer_name
    from csx_dw.dws_crm_w_a_customer
    where sdt = 'current' 
  )d on d.customer_no = a.customer_no
  --商品维表
  left outer join 
  (
    select *
    from csx_dw.dws_basic_w_a_csx_product_m 
    where sdt = 'current'
  )e on e.goods_id = a.goods_code
  --DC门店维表
  left outer join 
  (
    select *
    from csx_dw.dws_basic_w_a_csx_shop_m 
    where sdt = 'current'
  )f on f.shop_id = a.dc_code
group by 
  a.sdt,
  a.credential_no,
  a.region_code,
  a.region_name,
  a.province_code,
  a.province_name,
  a.city_group_code,
  a.city_group_name,
  a.dc_code, 
  f.shop_name,  
  a.customer_no,
  d.customer_name,
  a.goods_code,
  regexp_replace(regexp_replace(e.goods_name,'\n',''),'\r',''),
  e.unit,
  e.unit_name,
  e.department_id,
  e.department_name,
  e.classify_middle_code,
  e.classify_middle_name,
  case when e.division_code in ('10','11') then '11'
  	   when e.division_code in ('12','13','14','15') then '12'
  	   else '' end,  
  case when e.division_code in ('10','11') then '生鲜'
  	   when e.division_code in ('12','13','14','15') then '食百'
  	   else '' end,
  a.is_factory_goods_desc,
  case when c.fact_price is not null then '是' end,
  case when purchase_price_flag='1' then '是' end;


--临时表2：明细数据 计算省区部类的商品销售额排名、省区部类商品累计销售额--仅关注重点商品
drop table csx_tmp.tmp_goods_salezp02;
create temporary table csx_tmp.tmp_goods_salezp02
as
select a.division_code,a.goods_code,a.province_name,sales_value,
  row_number() over(partition by a.division_code,a.province_name order by sales_value desc)rno,
  sum(sales_value) over(partition by a.division_code,a.province_name order by sales_value desc)/sales_t zb_sale
from 
  (
  select division_code,goods_code,province_name,sum(sales_value)sales_value 
  from csx_tmp.tmp_goods_salezp 
  group by division_code,goods_code,province_name
  )a 
join 
  (
  select province_name,division_code,sum(sales_value)sales_t 
  from csx_tmp.tmp_goods_salezp 
  group by province_name,division_code
  )b on (a.province_name=b.province_name and a.division_code=b.division_code);

--临时表3 筛选省区分部类的销售额的前80%或者top50的商品
drop table csx_tmp.tmp_goods_detail;
create temporary table csx_tmp.tmp_goods_detail
as
select a.sdt,a.division_code,a.division_name,
  a.dept_id,a.dept_name,a.classify_middle_code,a.classify_middle_name,
  a.goods_code,a.goods_name,a.unit_name,a.is_factory_goods_desc,a.is_fact,a.is_purchase,
  a.customer_no,a.customer_name,a.dc_code,a.dc_name,a.province_name,
  sales_qty,fact_value,cost_value_0,cost_value,purchase_value,middle_office_value,a.sales_value,
  fact_value/sales_qty fact_price,
  cost_value_0/sales_qty cost_price_0,
  cost_value/sales_qty cost_price,
  purchase_value/sales_qty purchase_price,
  middle_office_value/sales_qty middle_office_price,
  a.sales_value/sales_qty sales_price
from csx_tmp.tmp_goods_salezp a 
--部类的销售额的前80%或者top50的商品
join 
  (
  select * from csx_tmp.tmp_goods_salezp02 where rno<=50 or zb_sale<0.8
  )b on (a.goods_code=b.goods_code and a.province_name=b.province_name)
where a.sales_value<>0;

  
--结果表1：明细数据 日期+商品+客户维度
insert overwrite directory '/tmp/raoyanhua/jiage1' row format delimited fields terminated by '\t' 
select * from  csx_tmp.tmp_goods_detail a
order by a.province_name,a.dc_code,a.customer_no,a.dept_id,a.goods_code,sdt;


---临时表4：昨日销售价格与该省区月至今平均价格
drop table csx_tmp.tmp_goods_res; 
create temporary table csx_tmp.tmp_goods_res
as
select a.sdt,a.division_code,a.division_name,
  a.dept_id,a.dept_name,a.classify_middle_code,a.classify_middle_name,
  a.goods_code,a.goods_name,a.unit_name,a.is_factory_goods_desc,a.is_fact,a.is_purchase,
  a.customer_no,a.customer_name,a.dc_code,a.dc_name,a.province_name,
  a.sales_qty,a.fact_value,a.cost_value_0,a.cost_value,a.purchase_value,a.middle_office_value,a.sales_value,
  a.fact_value/a.sales_qty fact_price,
  a.cost_value_0/a.sales_qty cost_price_0,
  a.cost_value/a.sales_qty cost_price,
  purchase_value/a.sales_qty purchase_price,
  middle_office_value/a.sales_qty middle_office_price,
  a.sales_value/a.sales_qty sales_price,
  fact_price_std,cost_std_0,cost_std,sale_std,
  1-a.cost_value/a.sales_value tprorate,
  1-a.middle_office_value/a.sales_value front_prorate,
  a.sales_value-b.sale_std*a.sales_qty diff_sale,--当前销售额与核准销售额的差异
  a.sales_value/a.sales_qty-sale_std diff_sale_price,--销售价格偏差较大
  a.middle_office_value/a.sales_qty-cost_std diff_zt_price --中台报价提价
from 
  (
  select * from csx_tmp.tmp_goods_salezp where sdt=${hiveconf:current_day}
  )a 
--一段时间内（月至今）各平均价格
join 
  (
  select province_name,goods_code,sum(sales_qty) sales_qty,sum(cost_value) cost_value,
    sum(sales_value) sales_value,
	--平均原料价
    sum(if(is_fact='是',fact_value,0))/sum(if(is_fact='是',sales_qty,0)) fact_price_std,
	--平均库存成本价
    sum(cost_value_0)/sum(sales_qty) cost_std_0,
	--平均成本价
    sum(cost_value)/sum(sales_qty) cost_std,
	--平均售价
    sum(sales_value)/sum(sales_qty) sale_std
  from csx_tmp.tmp_goods_salezp where sdt<${hiveconf:current_day}
  group by province_name,goods_code
  )b on (a.province_name=b.province_name and a.goods_code=b.goods_code);

--临时表5：计算各环节加价率
drop table csx_tmp.tmp_goods_res01; 
create temporary table csx_tmp.tmp_goods_res01
as
select a.sdt,a.division_code,a.division_name,
  a.dept_id,a.dept_name,a.classify_middle_code,a.classify_middle_name,
  a.goods_code,a.goods_name,a.unit_name,a.is_factory_goods_desc,a.is_fact,a.is_purchase,
  a.customer_no,a.customer_name,a.dc_code,a.dc_name,a.province_name,
  a.sales_qty,
  a.fact_value,
  a.cost_value_0,
  a.cost_value,
  a.purchase_value,
  a.middle_office_value,
  a.sales_value,
  fact_price,
  cost_price_0,
  cost_price,
  purchase_price,
  middle_office_price,
  sales_price,
  cost_std,
  sale_std,
  tprorate,
  front_prorate,
  diff_sale,
  diff_sale_price,
  --销售价格差%
  case when sale_std  is null or sale_std<0 then 0 else diff_sale_price/sale_std end add_sale_rate,
  diff_zt_price,
  --工厂加价率 
  case when fact_price_std is not null and is_fact='是' then cost_price_0/fact_price_std else 0 end add_gc_rate,
  --中台加价率 
  case when cost_std is null then 0 else diff_zt_price/cost_std end add_zt_rate,
  --成本价对比
  case when cost_std is null then 1 else cost_price/cost_std end diff_cost,
  --历史毛利率
  case when sale_std is null or sale_std<0 then 0 else 1-cost_std/sale_std end prorate_std,
  --总毛利率-前台毛利率=中台毛利率
  tprorate-front_prorate zt_prorate
from csx_tmp.tmp_goods_res a 
where sales_value<>0;

--临时表6：异常标签
drop table csx_tmp.tmp_goods_res02; 
create temporary table csx_tmp.tmp_goods_res02
as
select a.*,
  --销售价格异常高:销售价格比历史均价高10个百分点，同时毛利率高于0.3，前台毛利高于0.1; 或者总销售额差异大于5千元，总毛利率大于0
  case when (add_sale_rate>0.1 and tprorate>0.3 and front_prorate>0.1)or (diff_sale>5000 and tprorate>0) then 1 else 0 end high_sale_price,
  --中台报价异常高:总毛利率-前台毛利率高于0.2，工厂加价率大于0
  case when zt_prorate>0.2 and (is_fact is null or add_gc_rate>0) then 1 else 0 end high_zt_price,
  --成本价异常高:成本价是历史成本价的1.2及以上，且历史的毛利率低于30%
  case when diff_cost>1.2 and prorate_std<0.3 then 1 else 0 end high_cost_pice,
  --销售价格异常低:销售价格比历史均价低10个百分点，同时总毛利率低于0，前台毛利率低于0；或者销售总额差异小于-5千元，毛利率小于0
  case when (add_sale_rate<-0.1 and tprorate<0 and front_prorate<0) or (diff_sale<-5000 and tprorate<0) then 1 else 0 end low_sale_price,
  --中台报价异常低:总毛利率-前台毛利率低于-0.1，且中台加价率小于0
  case when zt_prorate<-0.1 and add_zt_rate<0 then 1 else 0 end low_zt_price,
  --成本价异常低:工厂加价率小于0，毛利率高于0.5；或总毛利率高于0.5，成本价低于历史平均成本价80%
  case when (add_gc_rate<0 and tprorate>0.5) or (tprorate>0.5 and diff_cost<=0.8) then 1 else 0 end low_cost_price,
  --工厂加价异常高:工厂加价率大于30%
  case when add_gc_rate>0.3 then 1 else 0 end high_gc_price
from csx_tmp.tmp_goods_res01 a;

--结果表2：昨日明细数据（日期+商品+客户维度）的异常标签情况
insert overwrite directory '/tmp/raoyanhua/jiage2' row format delimited fields terminated by '\t' 
select * from csx_tmp.tmp_goods_res02 a order by a.province_name,a.dc_code,a.dept_id,a.goods_code;

--结果表3：各异常标签汇总数据
insert overwrite directory '/tmp/raoyanhua/jiage3' row format delimited fields terminated by '\t' 
select province_name,
  count(*) sum_yc,
  sum(high_sale_price) high_sale_price,
  sum(high_zt_price) high_zt_price,
  sum(high_cost_pice) high_cost_pice,
  sum(low_sale_price) low_sale_price,
  sum(low_zt_price) low_zt_price,
  sum(low_cost_price) low_cost_price,
  sum(high_gc_price) high_gc_price
from csx_tmp.tmp_goods_res02 a 
where high_sale_price=1 or high_zt_price=1 or high_cost_pice=1 or low_sale_price=1 or low_zt_price=1 or low_cost_price=1 or high_gc_price=1
group by 'AA',province_name
grouping sets ('AA',('AA',province_name));

--结果表4：异常标签汇总数据的生鲜食百统计
insert overwrite directory '/tmp/raoyanhua/jiage4' row format delimited fields terminated by '\t' 
select province_name,
  count(case when division_name='生鲜' then 1 else null end)sx_yc,
  count(case when division_name='食百' then 1 else null end)sb_yc,
  count(*)sum_yc
from csx_tmp.tmp_goods_res02 a 
where high_sale_price=1 or high_zt_price=1 or high_cost_pice=1 or low_sale_price=1 or low_zt_price=1 or low_cost_price=1 or high_gc_price=1
group by 'AA',province_name
grouping sets ('AA',('AA',province_name));

--结果表5：总毛利率高于30% 生鲜食百的商品数汇总
insert overwrite directory '/tmp/raoyanhua/jiage5' row format delimited fields terminated by '\t'
select province_name,
  count(case when division_name='生鲜' then 1 else null end)sx_yc,
  count(case when division_name='食百' then 1 else null end)sb_yc,
  count(*)sum_yc
from csx_tmp.tmp_goods_res02 a 
where tprorate>0.3
group by 'AA',province_name
grouping sets ('AA',('AA',province_name));

--结果表6：负毛利率 生鲜食百的商品数汇总
insert overwrite directory '/tmp/raoyanhua/jiage6' row format delimited fields terminated by '\t'
select province_name,
  count(case when division_name='生鲜' and tprorate<0 then 1 else null end)sx_fml,
  count(case when division_name='食百' and tprorate<0 then 1 else null end)sb_fml,
  count(case when tprorate<0 then 1 else null end)sum_fml,

  count(case when division_name='生鲜' then 1 else null end)sx_yc,
  count(case when division_name='食百' then 1 else null end)sb_yc,
  count(*)sum_yc
from csx_tmp.tmp_goods_res02 a 
group by 'AA',province_name
grouping sets ('AA',('AA',province_name));



















