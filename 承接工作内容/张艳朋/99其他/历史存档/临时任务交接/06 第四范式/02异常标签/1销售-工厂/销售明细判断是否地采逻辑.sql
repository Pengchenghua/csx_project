-- 统计日期
set current_day = regexp_replace(date_sub(current_date, 1), '-', '');
-- 当月月初
set current_start_day = regexp_replace(trunc(date_sub(current_date, 1), 'MM'),'-','');
-- 库存操作起始日期
set wms_start_day = regexp_replace(add_months(trunc(date_sub(current_date, 1), 'MM'), -11),'-','');
-- 当前月
set currnet_month = substr(${hiveconf:current_day}, 1, 6);



from csx_tmp.tmp_goods_salezp


--临时表1：明细数据 销售表中取各维度数据+成本、采购报价、中台报价、售价、销售额、毛利等，关联批次取批次库存成本价、关联工厂取原料价
drop table csx_tmp.tmp_goods_salezp;
create temporary table csx_tmp.tmp_goods_salezp
as
select 
  --g.source_type,   
  ----来源类型(1-采购导入、2-直送客户、3-一键代发、4-项目合伙人、5-无单入库、6-寄售调拨、7-自营调拨、8-云超采购、9-工厂采购、10-智能补货、11-商超直送、12-WMS调拨、13-云超门店采购、14-临时地采)
  --case when g.source_type='1' then '采购导入'
  --     when g.source_type='2' then '直送客户'
  --     when g.source_type='3' then '一键代发'
  --     when g.source_type='4' then '项目合伙人'
  --     when g.source_type='5' then '无单入库'
  --     when g.source_type='6' then '寄售调拨'
  --     when g.source_type='7' then '自营调拨'
  --     when g.source_type='8' then '云超采购'
  --     when g.source_type='9' then '工厂采购'
  --     when g.source_type='10' then '智能补货'
  --     when g.source_type='11' then '商超直送'
  --     when g.source_type='12' then 'WMS调拨'
  --     when g.source_type='13' then '云超门店采购'
  --     when g.source_type='14' then '临时地采'   --地采
  --     else '其他' end source_type_name,
  --case when g.source_type='14' then '地采' else '非地采' end is_dicai,
  a.sdt,--日期
  a.credential_no,--凭证号
  a.order_no,
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
  sum(coalesce(c.qty_dicai,0)) qty_dicai, --地采数量
  sum(coalesce(c.cost_price_0_dicai,0)*coalesce(c.qty_dicai,0)) cost_value_0_dicai,   --地采金额
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
	  order_no,
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
	  sum(case when g.source_type='14' then b.qty end) qty_dicai,--地采数量
	  sum(case when g.source_type='14' then b.price*b.qty end)/sum(case when g.source_type='14' then b.qty end) cost_price_0_dicai, --地采库存成本价
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
  --判断是否地采  source_type='14'为地采
  left join 
	(select order_code,source_type
	from csx_ods.source_scm_r_d_scm_order_header where sdt='19990101' )g on b.source_order_no=g.order_code	
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
  a.order_no,
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