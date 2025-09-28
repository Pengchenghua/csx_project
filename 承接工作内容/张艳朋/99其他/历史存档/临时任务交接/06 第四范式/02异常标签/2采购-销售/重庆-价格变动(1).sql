
--临时表1：明细数据 采购入库表中取到采购单商品维度数据+采购量 采购单价，关联凭证号获取销售相关数据，为最细粒度明细数据
drop table csx_tmp.tmp_goods_received;
create temporary table csx_tmp.tmp_goods_received
as
select 
aa.source_type,
super_class,
  aa.sdt csdt,
  aa.credential_no ccredential_no,--凭证号
  aa.order_code,
  ff.province_code ffprovince_code,--省区编码
  ff.province_name ffprovince_name,--省区
  ff.city_group_code ffcity_group_code,--城市组编码
  ff.city_group_name ffcity_group_name,--城市组
  aa.target_location_code, --DC编码
  ff.shop_name ,  --DC名称
  aa.goods_code,--商品编码
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
  sum(coalesce(aa.received_price,0)*aa.received_qty) received_price, --采购入库金额
 -- sum(coalesce(c.qty_dicai,0)) qty_dicai, --地采数量
 -- sum(coalesce(c.cost_price_0_dicai,0)*coalesce(c.qty_dicai,0)) cost_value_0_dicai,   --地采金额

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
    and ((a.sdt>='20210401' and a.sdt<='20210410' ) or (a.sdt='19990101' and order_time>'2021-04-01' and  order_time<'2021-04-11'))
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
    where sdt >= '20210401'
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
	  where sdt >= '20210401'
	  and move_type in ('107A', '108A')
	  group by goods_code, credential_no, source_order_no
    )b 
/*	--工厂加工表
    left outer join 
    (
      select 
      	goods_code,
      	order_code,
        sum(fact_values)/sum(goods_reality_receive_qty) as fact_price --原料价
      from csx_dw.dws_mms_r_a_factory_order
      where sdt >= '20210401' and mrp_prop_key in('3061','3010')
      group by goods_code, order_code
    )c on b.source_order_no = c.order_code and b.goods_code = c.goods_code
  --判断是否地采  source_type='14'为地采*/
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
  case when purchase_price_flag='1' then '是' end ;
  

 --临时表2：汇总维度到日期商品客户
drop table csx_tmp.tmp_goods_received03; 
create temporary table csx_tmp.tmp_goods_received03
--insert overwrite directory '/tmp/gaoxuefang/kehusy' row format delimited fields terminated by '\t' 
select
source_type,
super_class,
csdt,
   ffprovince_code,--省区编码
   ffprovince_name,--省区
   ffcity_group_code,--城市组编码
   ffcity_group_name,--城市组
  target_location_code, --DC编码
  shop_name ,  --DC名称
  goods_code,--商品编码
  goods_name,--商品名称
  unit,--单位
  unit_name,--单位名称
   dept_id,--课组编码
   dept_name,--课组名称
  classify_middle_code,--管理中类编码
  classify_middle_name,--管理中类名称
  division_code, --部类编码 
  division_name,--部类名称
 -- is_fact, --是否有原料价
  is_purchase,  --是否有采购报价
  channel_code,
  channel_name,
  business_type_code,
  business_type_name,

  sum(received_qty) received_qty,
  sum(received_price*received_qty) received_value, --采购入库金额
  sum(received_price*received_qty)/sum(received_qty) received_price, --采购入库单价
  sum(qty_dicai) qty_dicai, --地采数量
  sum(cost_value_0_dicai) cost_value_0_dicai,   --地采金额
 -- sum(fact_value) fact_value, --原料金额
  sum(cost_value_0) cost_value_0,  --批次库存成本  
  sum(sales_qty) sales_qty,--销售数量
  sum(cost_value) cost_value,--成本金额
  sum(purchase_value) purchase_value,--采购成本
  sum(middle_office_value) middle_office_value,  --中台成本
  sum(sales_value) sales_value,--销售额
  
  sum(sales_cost) sales_cost,--销售成本
  sum(profit) profit,--毛利
  
   sum(cost_value_0)/ sum(sales_qty) cost_value_price,  --批次库存成本单价 
  sum(cost_value)/ sum(sales_qty) cost_pricee,--成本金额单价
  sum(purchase_value)/ sum(sales_qty) purchase_price,--采购成本单价
  sum(middle_office_value)/ sum(sales_qty) middle_office_price,  --中台成本单价
  sum(sales_value)/ sum(sales_qty) sales_price, --销售额单价
   sum(sales_cost)/ sum(sales_qty) sales_cost_price  --销售成本单价
from (select 
aa.source_type,
super_class,
  aa.sdt csdt,
  aa.order_code,
  ff.province_code ffprovince_code,--省区编码
  ff.province_name ffprovince_name,--省区
  ff.city_group_code ffcity_group_code,--城市组编码
  ff.city_group_name ffcity_group_name,--城市组
  aa.target_location_code, --DC编码
  ff.shop_name ,  --DC名称
  aa.goods_code,--商品编码
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

  --case when c.fact_price is not null then '是' end as is_fact, --是否有原料价
  case when purchase_price_flag='1' then '是' end as is_purchase,  --是否有采购报价
  channel_code,
  channel_name,
  business_type_code,
  business_type_name,
  sales_type,
  return_flag,
  max(received_qty) received_qty,
  max(aa.received_price) received_price, --采购入库金额
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
    and ((a.sdt>='20210401' and a.sdt<='20210410' ) or (a.sdt='19990101' and order_time>'2021-04-01' and  order_time<'2021-04-11'))
    and super_class in (1,3)   --1 供应商订单 
	and a.source_type<>4
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
    where sdt >= '20210401'
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
	  where sdt >= '20210401'
	  and move_type in ('107A', '108A')
	  group by goods_code, credential_no, source_order_no
    )b 
	/*--工厂加工表
    left outer join 
    (
      select 
      	goods_code,
      	order_code,
        sum(fact_values)/sum(goods_reality_receive_qty) as fact_price --原料价
      from csx_dw.dws_mms_r_a_factory_order
      where sdt >= '20210401' and mrp_prop_key in('3061','3010')
      group by goods_code, order_code
    )c on b.source_order_no = c.order_code and b.goods_code = c.goods_code*/
  --判断是否地采  source_type='14'为地采
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
  sales_type,
  return_flag,

  case when c.fact_price is not null then '是' end, --是否有原料价
  case when purchase_price_flag='1' then '是' end )a 
 group by  source_type,
super_class,
  csdt,
   ffprovince_code,--省区编码
   ffprovince_name,--省区
   ffcity_group_code,--城市组编码
   ffcity_group_name,--城市组
  target_location_code, --DC编码
  shop_name ,  --DC名称
  goods_code,--商品编码
  goods_name,--商品名称
  unit,--单位
  unit_name,--单位名称
   dept_id,--课组编码
   dept_name,--课组名称
  classify_middle_code,--管理中类编码
  classify_middle_name,--管理中类名称
  division_code, --部类编码 
  division_name,--部类名称
  --is_fact, --是否有原料价
  is_purchase,  --是否有采购报价
  channel_code,
  channel_name,
  business_type_code,
  business_type_name;

  
--临时表3：昨日各仓库每个商品的采购入库单价
drop table csx_tmp.tmp_goods_res04; 
create temporary table csx_tmp.tmp_goods_res04
as
   select 
     a.goods_code,
	 a.target_location_code,
	  	sum(received_qty) as received_qty,
		sum(received_price1*received_qty)/sum(received_qty) received_price	
	from csx_dw.dws_scm_r_d_order_received a
  left outer join 
  (
    select *
    from csx_dw.dws_basic_w_a_csx_shop_m 
    where sdt = 'current'
  )ff on ff.shop_id = a.target_location_code	 
    where header_status in (3,4)  --3入库中4已完成
    and (( a.sdt='20210407' ) or (a.sdt='19990101' and order_time>'2021-04-07' and  order_time<'2021-04-08'))
    and super_class in (1,3)   --1 供应商订单 
	and a.source_type<>4
    AND ff.province_name='重庆市'
   group by   a.goods_code,a.target_location_code
   

--临时表4：历史各仓库每个商品的采购入库单价
drop table csx_tmp.tmp_goods_res05; 
create temporary table csx_tmp.tmp_goods_res05
as
   select 
     a.goods_code,
	 a.target_location_code,
	  	sum(received_qty) as received_qty,
		sum(received_price1*received_qty)/sum(received_qty) received_price	
	from csx_dw.dws_scm_r_d_order_received a
  left outer join 
  (
    select *
    from csx_dw.dws_basic_w_a_csx_shop_m 
    where sdt = 'current'
  )ff on ff.shop_id = a.target_location_code	 
    where header_status in (3,4)  --3入库中4已完成
    and (( a.sdt>='20210401' and a.sdt='20210408' ) or (a.sdt='19990101' and order_time>'2021-04-01' and  order_time<'2021-04-09'))
    and super_class in (1,3)   --1 供应商订单 
	and a.source_type<>4 
    AND ff.province_name='重庆市'
   group by   a.goods_code,a.target_location_code