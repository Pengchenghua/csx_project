-- 统计日期
set current_day = regexp_replace(date_sub(current_date, 1), '-', '');
set current_day1 = date_sub(current_date, 1);
--前3天
set current_day_3bf = regexp_replace(date_sub(current_date, 1+3),'-','');
-- 14天前
set current_start_day = regexp_replace(date_sub(current_date, 1+14),'-','');
set current_start_day1 = date_sub(current_date, 1+14);
-- 库存操作起始日期
set wms_start_day = regexp_replace(add_months(trunc(date_sub(current_date, 1), 'MM'), -11),'-','');





--临时表1：明细数据 采购入库表中取到采购单商品维度数据+采购量 采购单价，关联凭证号获取销售相关数据，为最细粒度明细数据
drop table csx_tmp.tmp_goods_received;
create temporary table csx_tmp.tmp_goods_received
as
select 
  aa.source_type,
  super_class,
  aa.sdt csdt,
  aa.credential_no c_credential_no,--凭证号
  aa.order_code,
  ff.province_code DC_province_code,--省区编码
  ff.province_name DC_province_name,--省区
  ff.city_group_code DC_city_group_code,--城市组编码
  ff.city_group_name DC_city_group_name,--城市组
  aa.target_location_code DC_DC_code, --DC编码
  ff.shop_name DC_DC_name,  --DC名称
  aa.goods_code,--商品编码
  regexp_replace(regexp_replace(e.goods_name,'\n',''),'\r','') as goods_name,--商品名称
  e.unit,--单位
  e.unit_name,--单位名称
  e.department_id,--课组编码
  e.department_name,--课组名称
  e.classify_middle_code,--管理中类编码
  e.classify_middle_name,--管理中类名称
  case when e.division_code in ('10','11') then '11'
  	   when e.division_code in ('12','13','14','15') then '12'
  	   else '' end as division_code, --部类编码 
  case when e.division_code in ('10','11') then '生鲜'
  	   when e.division_code in ('12','13','14','15') then '食百'
  	   else '' end as division_name,--部类名称
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
 -- a.is_factory_goods_desc,--是否工厂加工商品
 -- case when c.fact_price is not null then '是' end as is_fact, --是否有原料价
  case when purchase_price_flag='1' then '是' end as is_purchase,  --是否有采购报价
  channel_code,
  channel_name,
  business_type_code,
  business_type_name,
  sum(received_qty) received_qty,
  sum(coalesce(aa.received_price,0)*aa.received_qty) received_value, --采购入库金额
  sum(coalesce(c.qty_dicai,0)) qty_dicai, --地采数量
  sum(coalesce(c.cost_price_0_dicai,0)*coalesce(c.qty_dicai,0)) cost_value_0_dicai,   --地采金额

 -- sum(coalesce(c.fact_price,0)*a.sales_qty) fact_value, --原料金额
  sum(coalesce(c.cost_price_0,0)*a.sales_qty) cost_value_0,  --批次库存成本
  
  sum(a.sales_qty) sales_qty,--销售数量
  sum(coalesce(a.cost_price,0)*a.sales_qty) cost_value,--成本金额
  sum(coalesce(a.purchase_price,0)*a.sales_qty) purchase_value,--采购成本
  sum(coalesce(a.middle_office_price,0)*a.sales_qty) middle_office_value,  --中台成本
  sum(a.sales_value) sales_value,--销售额
  sum(a.sales_cost) sales_cost,--销售成本
  sum(a.profit) profit--毛利
from 
(
    select 
     if(a.sdt='19990101',regexp_replace(substr(order_time,1,10),'-',''),a.sdt) sdt,
     a.goods_code,
	 a.order_code,
	  b.credential_no,
	  a.target_location_code,
	  a.source_type,
	  super_class,
	  	sum(received_qty) as received_qty,
		sum(received_price1*received_qty)/sum(received_qty) received_price	
	from csx_dw.dws_scm_r_d_order_received a
	left join 
	 (
      select distinct  source_order_no,credential_no,goods_code 
	  from csx_dw.dws_wms_r_d_batch_detail  
	  where  move_type in ('107A', '108A')
     )b on a.order_code=b.source_order_no and a.goods_code=b.goods_code
    where header_status in (3,4)  --3入库中4已完成
    --and ((a.sdt>='20210401' and a.sdt<='20210410' ) or (a.sdt='19990101' and order_time>'2021-04-01' and  order_time<'2021-04-11'))
	and ((a.sdt>=${hiveconf:current_start_day} and a.sdt<=${hiveconf:current_day}) 
	  or (a.sdt='19990101' and order_time>${hiveconf:current_start_day1} and  order_time<${hiveconf:current_day1}))	
    and super_class in (1,3)   --1 供应商订单 
	and a.source_type<>4 --剔除项目合伙人
   group by   if(a.sdt='19990101',regexp_replace(substr(order_time,1,10),'-',''),a.sdt),a.goods_code,
	 a.order_code,
	  b.credential_no,super_class,
	  a.target_location_code, a.source_type
  )aa
  left outer join 
  (
    select *
    from csx_dw.dws_basic_w_a_csx_shop_m 
    where sdt = 'current'
  )ff on ff.shop_id = aa.target_location_code	  
 left join  (
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
	  channel_code,
	  channel_name,
	  business_type_code,
	  business_type_name,
	  sales_type,
	  return_flag,
      case when purchase_price_flag='1' then purchase_price end as purchase_price,
      middle_office_price,
      sales_price
    from csx_dw.dws_sale_r_d_detail 
    --where sdt >= '20210401'
	where sdt >= ${hiveconf:current_start_day}
	and channel_code in ('1', '7', '9')
	--and business_type_code ='1'
	and sales_type<>'fanli'
	and return_flag<>'X'
--	and province_name='重庆市'
  )a on aa.credential_no=a.credential_no and aa.goods_code=a.goods_code
  left outer join 
  (
    select
	  b.goods_code,
	  b.credential_no,
	  sum(case when g.source_type='14' then b.qty end) qty_dicai,--地采数量
	  sum(case when g.source_type='14' then b.price*b.qty end)/sum(case when g.source_type='14' then b.qty end) cost_price_0_dicai, --地采库存成本价
	  sum(b.qty) as qty,
	  sum(b.price*b.qty)/sum(b.qty) cost_price_0  --多批次平均库存成本价
	  --sum(c.fact_price*b.qty)/sum(case when c.fact_price is not null then b.qty end) fact_price --原料价
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
  left join 
	(select order_code,source_type
	from csx_ods.source_scm_r_d_scm_order_header where sdt='19990101' )g on b.source_order_no=g.order_code	
  	group by b.goods_code,b.credential_no	
  )c on aa.goods_code = c.goods_code and aa.credential_no = c.credential_no
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
  )e on e.goods_id = aa.goods_code
  --DC门店维表
  left outer join 
  (
    select *
    from csx_dw.dws_basic_w_a_csx_shop_m 
    where sdt = 'current'
  )f on f.shop_id = a.dc_code	 
where  ff.province_name='重庆市' 
group by 
aa.source_type,
aa.super_class,
  aa.sdt,
  aa.credential_no,--凭证号
  aa.order_code,
  ff.province_code,--省区编码
  ff.province_name,--省区
  ff.city_group_code,--城市组编码
  ff.city_group_name,--城市组
  aa.target_location_code, --DC编码
  ff.shop_name ,  --DC名称
  aa.goods_code,--商品编码
  e.goods_name,--商品名称
  e.unit,--单位
  e.unit_name,--单位名称
  e.department_id ,--课组编码
  e.department_name ,--课组名称
  e.classify_middle_code,--管理中类编码
  e.classify_middle_name,--管理中类名称
  case when e.division_code in ('10','11') then '11'
  	   when e.division_code in ('12','13','14','15') then '12'
  	   else '' end, --部类编码 
  case when e.division_code in ('10','11') then '生鲜'
  	   when e.division_code in ('12','13','14','15') then '食百'
  	   else '' end ,--部类名称
  	    channel_code,
  channel_name,
  business_type_code,
  business_type_name,
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
  f.shop_name ,  --DC名称
  a.customer_no,--客户编码
  d.customer_name,--客户名称
  --case when c.fact_price is not null then '是' end, --是否有原料价
  case when purchase_price_flag='1' then '是' end 
having qty_dicai=0; --地采数量  剔除地采数据  


  
    
--临时表2：当日采购入库订单明细+采购入库异常标签
drop table csx_tmp.tmp_goods_received_d;
create temporary table csx_tmp.tmp_goods_received_d
as
select a.*,
  b.received_qty_ls,b.received_value_ls,b.received_price_ls,
  c.received_qty_last,c.received_value_last,c.received_price_last,
  d.received_qty_yc,d.received_value_yc,d.received_price_yc,
 --入库价异常高:入库价是历史入库价的1.3倍以上，或是竞争对手入库价的1.2倍以上 
  case when b.received_price_ls is not null and a.received_price/b.received_price_ls>1.3 then 1
       when d.received_price_yc is not null and a.received_price/d.received_price_yc>1.2 then 1
	   else 0 end as received_price_hight,
 --入库价异常低：入库价是历史入库价的0.7倍以下，或是竞争对手入库价的0.7倍以下	   
  case when b.received_price_ls is not null and a.received_price/b.received_price_ls<0.7 then 1
       when d.received_price_yc is not null and a.received_price/d.received_price_yc<0.7 then 1
	   else 0 end as received_price_low,
 --入库价突涨：入库价/前一日入库价>1.3，且入库价/历史入库价>1.1
  case when c.received_price_last is not null and a.received_price/c.received_price_last>1.3 
         and b.received_price_ls is not null and a.received_price/b.received_price_ls>1.1 then 1
	   else 0 end as received_price_up,
 --入库价突降：入库价/前一日入库价<0.7，且入库价/历史入库价<0.9		   
  case when c.received_price_last is not null and a.received_price/c.received_price_last<0.7
         and b.received_price_ls is not null and a.received_price/b.received_price_ls<0.9 then 1
	   else 0 end as received_price_down	   
from
( 
select distinct source_type,super_class,csdt,c_credential_no,order_code,
  DC_province_code,DC_province_name,DC_city_group_code,DC_city_group_name,DC_DC_code,DC_DC_name,
  goods_code,goods_name,unit,department_id,department_name,classify_middle_code,classify_middle_name,division_code,division_name,
  received_qty,received_value,
  received_value/received_qty as received_price
from csx_tmp.tmp_goods_received 
where csdt=${hiveconf:current_day}
)a 
--历史各仓库每个商品的采购入库单价
left join 
(
select 
  goods_code,
  DC_DC_code,
  sum(received_qty) as received_qty_ls,
  sum(received_value) received_value_ls,
  sum(received_value)/sum(received_qty) received_price_ls	
from csx_tmp.tmp_goods_received
where csdt<${hiveconf:current_day}
group by goods_code,DC_DC_code
)b on b.goods_code=a.goods_code and b.DC_DC_code=a.DC_DC_code
--最近一次入库价
left join 
(
  select 
    goods_code,
    DC_DC_code,
    sum(received_qty) as received_qty_last,
    sum(received_value) received_value_last,
    sum(received_value)/sum(received_qty) received_price_last
  from 
    (
    select *,
    rank() over (partition by goods_code,DC_DC_code order by csdt desc ) as cn1  
    from csx_tmp.tmp_goods_received
    where csdt<${hiveconf:current_day}
	)a 
	where a.cn1=1
  group by goods_code,DC_DC_code
)c on c.goods_code=a.goods_code and b.DC_DC_code=a.DC_DC_code
--近3天永辉入库价
left join 
  (
  select
    a.goods_code,
    sum(a.received_qty_yc) received_qty_yc,
    sum(a.received_value_yc) received_value_yc,
    sum(a.received_value_yc)/sum(a.received_qty_yc) received_price_yc	 
  from
  (
    select
    	shop_id_in,
    	goodsid goods_code,
    	pur_doc_id,
  	sum(pur_qty_in) received_qty_yc,
  	sum(tax_pur_val_in) received_value_yc,
  	sum(tax_pur_val_in)/sum(pur_qty_in) received_price_yc
    from b2b.ord_orderflow_t 
    where sdt>=${hiveconf:current_day_3bf} and sdt<${hiveconf:current_day}
    	and pur_qty_in>0 
    	and tax_pur_val_in >0  --剔除入库金额为 0 的商品		
    	and ordertype not in ('返配','退货') 
    	and regexp_replace(vendor_id,'(0|^)([^0].*)',2) not like '75%'
  	group by shop_id_in,goodsid,pur_doc_id
  )a
  join
  (
    select distinct
    	shop_id as shop_id, 
    	city_name as city_name,
    	province_name as province_name,
    	dept_id_channel,
    	case when shop_channel ='csx' then '彩食鲜' else '云超' end stype
    from 
    	csx_dw.ads_sale_r_d_purprice_globaleye_shop
    where 
    	sdt='current' 
    	and  province_name in ('重庆市')
  ) as d on a.shop_id_in=d.shop_id--拿到云超的数据
  group by a.goods_code
 )d on d.goods_code=a.goods_code;




--临时表2：当日采购入库订单明细+采购入库异常标签
drop table csx_tmp.tmp_goods_received_d;
create temporary table csx_tmp.tmp_goods_received_d
as
from
( 
select distinct source_type,super_class,csdt,c_credential_no,order_code,
  DC_province_code,DC_province_name,DC_city_group_code,DC_city_group_name,DC_DC_code,DC_DC_name,
  goods_code,goods_name,unit,department_id,department_name,classify_middle_code,classify_middle_name,division_code,division_name,
  received_qty,received_value,
  received_value/received_qty as received_price
from csx_tmp.tmp_goods_received_d a 
  --采购报价
  left outer join 
  (
    select warehouse_code,product_code,price_begin_time,price_end_time
    from csx_ods.source_price_r_d_effective_purchase_prices 
    where sdt =regexp_replace(date_sub(current_date, 1), '-', '') --用最近分区，每个分区全量数据
  )f on f.shop_id = a.dc_code 






  
  




  
  
  
  
  
  
  