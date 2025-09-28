--采购关联工厂、再关联入库批次表，获得采购入库到入库批次环节，包含采购入库异常、原料价异常、工厂加价异常、采购报价异常
--满足可关联单号+批次+商品标记上述环节是否异常目的
--唯一id  concat_ws('&', scm_order_code,scm_goods_code,order_code,batch_no,goods_code,credential_no,sdt_end) as id



--使用工厂生产数据通过工单加原料关联成本核算凭证明细表的原单号和商品，在凭证中原料和成品都会产生凭证，并且凭证号是一致的。
--通过获取的凭证号和商品再去关联物流的入库批次表，可以得到工厂加工商品的原料的采购订单号。
--这里取能关联出采购订单号的工厂生产数据，存在部分获取不到采购单号的数据。
--临时表1：工厂-凭证明细-入库批次明细
drop table csx_tmp.tmp_factory_order_to_scm_11;
create temporary table csx_tmp.tmp_factory_order_to_scm_11
as
select 
  factory_order_code,
  a.goods_code,
  product_code,
  fact_values,
  product_price,
  goods_reality_receive_qty,
  fact_qty,
  source_order_no as scm_order_code
from 
(
  select 
    order_code as factory_order_code,
	a.goods_code,
	a.product_code,
	fact_values,
	product_price,
	goods_reality_receive_qty,
	fact_qty,
    credential_no as factory_credential_no
  from 
  --工厂加工
  (
    select 
	  product_code,--原料编号
	  goods_code,--成品商品编号
	  order_code,
	  sum(fact_values) as fact_values,--原料金额
	  max(product_price) as product_price,
	  sum(goods_reality_receive_qty) as goods_reality_receive_qty,
	  sum(fact_qty) as fact_qty
    from csx_dw.dws_mms_r_a_factory_order
	where sdt >= '20210101' and mrp_prop_key in('3061','3010') 
	group by product_code,goods_code,order_code
  )a 
  --凭证明细底表,通过工单和原料关联出凭证号
  left join 
  (
    select 
	  distinct
	  source_order_no,
	  credential_no,
	  product_code
    from csx_ods.source_cas_r_d_accounting_credential_item
	where sdt = '19990101' and move_type = '119A'  --119A 原料转成品
  )b on a.order_code = b.source_order_no and a.product_code = b.product_code
)a 
--入库批次明细表,通过关联凭证号和原料编码关联获得采购单号 能关联到的为工厂加工
left join 
(
  select 
    distinct 
    source_order_no,
    credential_no,
    goods_code
  from csx_dw.dws_wms_r_d_batch_detail  
--119A-原料转成品，PO-采购单
  where move_type = '119A' and source_order_no like 'PO%'
)b on a.factory_credential_no = b.credential_no and a.product_code = b.goods_code
where b.source_order_no is not null;


--在工厂加工关联到采购单号的基础上，用采购入库表去做关联，关联上的用工厂工单号做订单编码，工厂成品做商品编码，关联不上的
--直接用采购的采购订单做订单编码，采购商品编码做商品编码。
--临时表2：采购入库表区分是否工厂加工得到最终可关联的订单编码及商品编码
drop table csx_tmp.tmp_scm_factory_order_to_batch_1;
create temporary table csx_tmp.tmp_scm_factory_order_to_batch_1
as
select 
  a.scm_sdt,
  c.province_code DC_province_code,--省区编码
  c.province_name DC_province_name,--省区
  c.city_group_code DC_city_group_code,--城市组编码
  c.city_group_name DC_city_group_name,--城市组
  a.target_location_code DC_DC_code, --DC编码
  c.shop_name DC_DC_name,  --DC名称
  a.order_code as scm_order_code,
  a.goods_code as scm_goods_code,
  a.order_qty,
  a.received_qty,
  a.received_price,
  a.received_amount,
  coalesce(b.factory_order_code,'') as factory_order_code,
  coalesce(b.goods_code,'') as factory_goods_code,
  coalesce(b.product_code,'') as product_code,
  coalesce(b.fact_values,'') as fact_values,
  coalesce(b.product_price,'') as product_price,
  coalesce(b.goods_reality_receive_qty,'') as goods_reality_receive_qty,
  coalesce(b.fact_qty,'') as fact_qty,
  case when b.fact_qty is not null then '是' end as is_fact, --是否有原料价
  --关联上的用工厂工单号做订单编码，否则用采购的采购订单做订单编码
  case when b.factory_order_code is null then a.order_code 
    else b.factory_order_code end as order_code,
  --关联上的工厂成品做商品编码，否则用采购商品编码做商品编码
  case when b.factory_order_code is null then a.goods_code 
    else b.goods_code end as goods_code
from 
  --采购入库表
  (
    select 
      if(sdt='19990101',regexp_replace(substr(order_time,1,10),'-',''),sdt) scm_sdt,
	  target_location_code,
	  order_code,
  	  goods_code,
  	  sum(order_qty) as order_qty,
  	  sum(received_qty) as received_qty,
  	  max(received_price1) as received_price,
  	  sum(received_amount) as received_amount
    from csx_dw.dws_scm_r_d_order_received
    where header_status in (3,4)  --3入库中4已完成
	  and ((sdt >= '20210101' and sdt <= '20210601')
      or (sdt = '19990101' and substr(order_time,1,10) >= '20210101' 
  	  and substr(order_time,1,10) <= '20210601'))
	  and super_class in (1,3)   --1 供应商订单 
	  --and source_type<>4 --剔除项目合伙人
	  and source_type not in (2,3,4,11,15,16) 
	  and local_purchase_flag='0'--剔除地采，是否地采(0-否、1-是)
    group by if(sdt='19990101',regexp_replace(substr(order_time,1,10),'-',''),sdt),target_location_code,order_code,goods_code
  )a 
--第一步结果表 工厂工厂-凭证明细-入库批次明细
left join csx_tmp.tmp_factory_order_to_scm_11 b on a.order_code = b.scm_order_code and a.goods_code = b.product_code
left join 
  (select * from csx_dw.dws_basic_w_a_csx_shop_m where sdt = 'current')c on c.shop_id = a.target_location_code	
where c.province_name in('重庆市','安徽省');



--在第二步基础上，用选择好的订单编码，商品编码再次关联物流入库批次表，获取销售出库数据，拿到销售出库凭证号，
--再利用商品加凭证号，即可关联出销售数据
--临时表3：采购入库到最终入库批次明细（可根据商品于凭证号关联到销售表）
drop table csx_tmp.tmp_scm_factory_order_to_batch_2;
create temporary table csx_tmp.tmp_scm_factory_order_to_batch_2
as
select 
  a.scm_sdt,
  a.DC_province_code,--省区编码
  a.DC_province_name,--省区
  a.DC_city_group_code,--城市组编码
  a.DC_city_group_name,--城市组
  a.DC_DC_code, --DC编码
  a.DC_DC_name,  --DC名称  
  a.scm_order_code,  --采购入库单号
  a.scm_goods_code,  --采购商品或原料编号
  a.order_qty,
  a.received_qty,
  a.received_price,
  a.received_amount,
  a.factory_order_code,
  a.factory_goods_code,
  a.product_code,  --工厂原料品编号
  e.goods_name product_name,  --工厂原料品名称
  a.fact_values,  --原料金额
  a.product_price,
  a.goods_reality_receive_qty,
  a.fact_qty,
  a.fact_values/a.fact_qty as fact_price, --原料价
  a.is_fact, --是否有原料价
  a.order_code, --批次入库单号或工单号
  a.goods_code,  --商品编号
  c.goods_name,  --商品名称
  c.unit,  --单位
  c.unit_name, --单位名称
  c.division_code, --部类编码
  c.division_name,--部类名称
  c.department_id ,--课组编码
  c.department_name ,--课组名称
  c.classify_middle_code,--管理中类编码
  c.classify_middle_name, --管理中类名称   
  coalesce(b.batch_sdt,a.scm_sdt) as sdt_end, --加工后或采购单到批次日期
  b.credential_no,
  b.batch_no,
  b.batch_price, --批次成本价
  --d.received_qty_ls,d.received_value_ls,d.received_price_ls,
  --d.received_qty_last,d.received_value_last,d.received_price_last,
  --d.received_qty_yc,d.received_value_yc,d.received_price_yc,
  d.received_price_hight, --入库价异常高
  d.received_price_low, --入库价异常低
  d.received_price_up, --入库价突涨
  d.received_price_down  --入库价突降
from csx_tmp.tmp_scm_factory_order_to_batch_1 a 
--入库批次明细表
left join 
  (
    select
      goods_code,
      credential_no,
	  batch_no,
      source_order_no,
	  max(sdt) batch_sdt,
      sum(qty) as batch_qty,
	  sum(amt)/sum(qty) batch_price
    from csx_dw.dws_wms_r_d_batch_detail
    --where move_type in ('107A', '108A')
	where move_type in ('101A', '120A') --101A 收货入库,120A 原料转成品
    group by goods_code,credential_no,batch_no,source_order_no
  )b on a.order_code = b.source_order_no and a.goods_code = b.goods_code
--商品维表
left join 
  (select * from csx_dw.dws_basic_w_a_csx_product_m where sdt='current')c on c.goods_id = a.goods_code	
--采购入库异常标签表  
left join 
  (select * from csx_tmp.tmp_goods_received_d where scm_sdt>='20201001'
  )d on d.order_code = a.scm_order_code and d.goods_code = a.scm_goods_code
--商品维表
left join 
  (select * from csx_dw.dws_basic_w_a_csx_product_m where sdt='current')e on e.goods_id = a.product_code ;


--临时表4：采购入库到最终入库批次明细+关联采购报价
drop table csx_tmp.tmp_scm_factory_order_to_batch_3;
create table csx_tmp.tmp_scm_factory_order_to_batch_3
as
select a.*,b.purchase_price
from csx_tmp.tmp_scm_factory_order_to_batch_2 a
left join 
(
  select dc_code,product_code,calday,
    max(purchase_price) purchase_price
  from 
  (
    select b.*,d.calday
    from 
    (
      select warehouse_code dc_code,
        regexp_replace(split(price_begin_time, ' ')[0], '-', '') as price_begin_date,
        regexp_replace(split(price_end_time, ' ')[0], '-', '') as price_end_date,
        product_code,
        purchase_price
      from csx_ods.source_price_r_d_effective_purchase_prices
      where sdt=regexp_replace(date_sub(current_date, 1), '-', '')
      and effective='1'
    )b
    left join 
    (
      select 
        calday
      from csx_dw.dws_basic_w_a_date
      where calday >='20210101'
      --where calday >= regexp_replace(add_months(trunc(current_date,'MM'),-1),'-','')  --刷新上月至今的数据
        and calday <= regexp_replace(date_sub(current_date,1),'-','')  --关联时间维表,获取有效日期维度
    ) d on 1 = 1
    where b.price_begin_date <= d.calday and d.calday <= b.price_end_date
  )a
  group by dc_code,product_code,calday
)b on b.dc_code=a.DC_DC_code and b.product_code=coalesce(a.goods_code,a.scm_goods_code) and b.calday=a.sdt_end;


--临时表5：历史采购报价 原料价
drop table csx_tmp.tmp_scm_factory_order_to_batch_ls;
create table csx_tmp.tmp_scm_factory_order_to_batch_ls
as
select distinct
  goods_code,
  DC_DC_code,
  sdt_end,
  fact_qty_ls,
  fact_values_ls,
  fact_values_ls/fact_qty_ls fact_price_ls,
  purchase_price_ls  
from 
  (
  select 
    goods_code,
    DC_DC_code,
    sdt_end,
    sum(fact_qty) over(partition by goods_code,DC_DC_code order by unix_timestamp(sdt_end,'yyyyMMdd') asc range between 1209600 preceding and 0 preceding) as fact_qty_ls,
    sum(fact_values) over(partition by goods_code,DC_DC_code order by unix_timestamp(sdt_end,'yyyyMMdd') asc range between 1209600 preceding and 0 preceding) as fact_values_ls,
	avg(purchase_price) over(partition by goods_code,DC_DC_code order by unix_timestamp(sdt_end,'yyyyMMdd') asc range between 1209600 preceding and 0 preceding) as purchase_price_ls
  from csx_tmp.tmp_scm_factory_order_to_batch_3 )a;
  

--结果表：采购入库到工厂到批次各环节异常标签
--drop table csx_tmp.tmp_scm_factory_order_to_batch_4;
--create table csx_tmp.tmp_scm_factory_order_to_batch_4
--as
set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table csx_tmp.tmp_received_batch_detail_abnormal_label partition(sdt)
select 
  concat_ws('&', scm_order_code,scm_goods_code,order_code,batch_no,goods_code,credential_no,sdt_end) as id,
  scm_sdt,
  DC_province_code,--省区编码
  DC_province_name,--省区
  DC_city_group_code,--城市组编码
  DC_city_group_name,--城市组
  DC_DC_code, --DC编码
  DC_DC_name,  --DC名称  
  scm_order_code,  --采购入库单号
  scm_goods_code,  --采购商品或原料编号
  order_qty,
  received_qty,
  received_price,
  received_amount,
  factory_order_code,
  factory_goods_code,
  product_code,  --工厂原料品编号
  product_name,  --工厂原料品名称
  fact_values,  --原料金额
  product_price,
  goods_reality_receive_qty,
  fact_qty,
  fact_price, --原料价
  is_fact, --是否有原料价
  order_code, --批次入库单号或工单号
  goods_code,  --商品编号
  goods_name,  --商品名称
  unit,  --单位
  unit_name, --单位名称
  division_code, --部类编码
  division_name,--部类名称
  department_id ,--课组编码
  department_name ,--课组名称
  classify_middle_code,--管理中类编码
  classify_middle_name, --管理中类名称   
  --sdt_end, --加工后或采购单到批次日期
  credential_no,
  batch_no,
  batch_price, --批次成本价
  purchase_price,
  received_price_hight, --入库价异常高
  received_price_low, --入库价异常低
  received_price_up, --入库价突涨
  received_price_down,  --入库价突降
  high_gc_price,  --工厂加价异常高
  low_gc_price,  --工厂加价异常低
  high_fact_price,  --工厂原料价异常高
  low_fact_price,  --工厂原料价异常低
  high_purchase_price,  --采购报价异常高
  low_purchase_price,  --采购报价异常低
  
  if(received_price_hight='1' or received_price_low='1' or received_price_up='1',1,0) as received_price_abnormal,  --采购入库价异常
  if(high_fact_price='1' or low_fact_price='1',1,0) as fact_price_abnormal,  --原料价异常
  if(high_gc_price='1' or low_gc_price='1',1,0) as gc_price_abnormal,  --工厂加价异常
  if(high_purchase_price='1' or low_purchase_price='1',1,0) as purchase_price_abnormal,   --采购报价异常
  sdt_end sdt --加工后或采购单到批次日期
from 
(
  select a.*,
    --工厂加价异常高:工厂加价率大于30%
    case when a.is_fact='是' and (a.batch_price-b.fact_price_ls)/b.fact_price_ls>0.3 then 1 else 0 end high_gc_price,
    --工厂加价异常低:工厂加价率小于5%
    case when a.is_fact='是' and (a.batch_price-b.fact_price_ls)/b.fact_price_ls<0.05 then 1 else 0 end low_gc_price,
    --工厂原料价异常高:原料价/历史原料价>1.2
    case when b.fact_price_ls is not null and a.is_fact='是' and a.fact_price/b.fact_price_ls>1.2 then 1 else 0 end high_fact_price,
    --工厂原料价异常低:原料价/历史原料价<0.8
    case when b.fact_price_ls is not null and a.is_fact='是' and a.fact_price/b.fact_price_ls<0.8 then 1 else 0 end low_fact_price,
    --采购报价异常高:采购报价/历史采购报价>1.2
    case when a.purchase_price is not null and a.purchase_price/b.purchase_price_ls>1.2 then 1 else 0 end high_purchase_price,
    --采购报价异常低:采购报价/历史采购报价<0.8
    case when a.purchase_price is not null and a.purchase_price/b.purchase_price_ls<0.8 then 1 else 0 end low_purchase_price
  from csx_tmp.tmp_scm_factory_order_to_batch_3 a 
  left join csx_tmp.tmp_scm_factory_order_to_batch_ls b on b.goods_code=a.goods_code and b.DC_DC_code=a.DC_DC_code and b.sdt_end=a.sdt_end
)a ;






/*
---------------------------------------------------------------------------------------------------------
---------------------------------------------hive 建表语句-----------------------------------------------
--采购入库到工厂到批次各环节异常标签 csx_tmp.tmp_received_batch_detail_abnormal_label

drop table if exists csx_tmp.tmp_received_batch_detail_abnormal_label;
CREATE TABLE `csx_tmp.tmp_received_batch_detail_abnormal_label`(
  `id` string COMMENT '唯一id',
  `scm_sdt` string COMMENT '采购入库日期',
  `DC_province_code` string COMMENT '省区编码',
  `DC_province_name` string COMMENT '省区',
  `DC_city_group_code` string COMMENT '城市组编码',
  `DC_city_group_name` string COMMENT '城市组',
  `DC_DC_code` string COMMENT 'DC编码',
  `DC_DC_name` string COMMENT 'DC名称',
  `scm_order_code` string COMMENT '采购入库单号',
  `scm_goods_code` string COMMENT '采购商品或原料编号',
  `order_qty` decimal(20,6) COMMENT '采购单数量',
  `received_qty` decimal(20,6) COMMENT '入库数量',
  `received_price` decimal(20,6) COMMENT '入库价格',
  `received_amount` decimal(20,6) COMMENT '入库金额',
  `factory_order_code` string COMMENT '工单号',
  `factory_goods_code` string COMMENT '工厂商品编号',
  `product_code` string COMMENT '工厂原料品编号',
  `product_name` string COMMENT '工厂原料品名称',
  `fact_values` decimal(20,6) COMMENT '原料金额',
  `product_price` decimal(20,6) COMMENT '物料价格',
  `goods_reality_receive_qty` decimal(20,6) COMMENT '工厂商品实际产量',
  `fact_qty` decimal(20,6) COMMENT '原料数量',
  `fact_price` decimal(20,6) COMMENT '原料价',
  `is_fact` string COMMENT '是否有原料价',
  `order_code` string COMMENT '批次入库单号或工单号',
  `goods_code` string COMMENT '商品编号',
  `goods_name` string COMMENT '商品名称',
  `unit` string COMMENT '单位',
  `unit_name` string COMMENT '单位名称',
  `division_code` string COMMENT '部类编码',
  `division_name` string COMMENT '部类名称',
  `department_id` string COMMENT '课组编码',
  `department_name` string COMMENT '课组名称',
  `classify_middle_code` string COMMENT '管理中类编码',
  `classify_middle_name` string COMMENT '管理中类名称',
  `credential_no` string COMMENT '凭证号',
  `batch_no` string COMMENT '批次号',
  `batch_price` decimal(20,6) COMMENT '批次成本价',
  `purchase_price` decimal(20,6) COMMENT '采购报价',
  `received_price_hight` string COMMENT '入库价异常高',
  `received_price_low` string COMMENT '入库价异常低',
  `received_price_up` string COMMENT '入库价突涨',
  `received_price_down` string COMMENT '入库价突降',
  `high_gc_price` string COMMENT '工厂加价异常高',
  `low_gc_price` string COMMENT '工厂加价异常低',
  `high_fact_price` string COMMENT '工厂原料价异常高',
  `low_fact_price` string COMMENT '工厂原料价异常低',
  `high_purchase_price` string COMMENT '采购报价异常高',
  `low_purchase_price` string COMMENT '采购报价异常低',
  `received_price_abnormal` string COMMENT '采购入库价异常',
  `fact_price_abnormal` string COMMENT '原料价异常',
  `gc_price_abnormal` string COMMENT '工厂加价异常',
  `purchase_price_abnormal` string COMMENT '采购报价异常'
) COMMENT '采购入库到工厂到批次各环节异常标签'
PARTITIONED BY (sdt string COMMENT '日期分区')
STORED AS TEXTFILE;










