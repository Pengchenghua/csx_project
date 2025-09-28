set hive.support.quoted.identifiers=none;
set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
-- 作业负责人
set create_by='zhaoxiaomin';
--当前时间
set i_sdate_16 = from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss');

--昨日，昨日的上上月1日
set current_sdate =regexp_replace(date_sub(current_date,1),'-','');
set before_0mon =regexp_replace(trunc(date_sub(current_date,1),'MM'),'-','');
set before_2mon =regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-2),'-','');

--订单批次明细
insert overwrite table csx_dw.report_sale_r_d_negative_profit_batch  partition(sdt)
select
  concat_ws('&', a.id,cast(b.id as string),a.sdt) as id,
  a.business_type_code,
  a.business_type_name, 
  a.customer_no,
  a.customer_name,
  a.goods_code,
  a.goods_name, 
  a.origin_order_no,
  a.order_no,
  b.batch_no,  --入库批次号
  b.move_name, --批次入库类型
  b.link_wms_entry_qty as qty_caigou,    -- '采购数量'
  b.link_wms_entry_amt_no_tax as amt_no_tax_caigou,    -- '采购不含税金额'
  b.link_wms_entry_price_no_tax as price_no_tax_caigou,    -- '采购不含税单价'
  b.qty,  --订单批次出库 数量、金额，入库单价
  b.amt,
  b.amt/b.qty entry_price,
  a.purchase_price,
  a.middle_office_price,
  a.sales_price,
  (a.sales_price-b.price)*b.qty profit,
  round(((a.sales_price-b.price)/b.price),6) prorate,
  --concat(round(((a.sales_price-b.price)/b.price)*100,2),'%') prorate,
  a.sdt
from 
(
  select 
    id,
    split(id, '&')[0] as credential_no,
    business_type_code,
    business_type_name,  
    sdt,
    customer_no,
    customer_name,
    goods_code,
    goods_name,
    origin_order_no,
    order_no,
    sales_qty,
    sales_value,
    profit,
    cost_price,
    purchase_price,
    middle_office_price,
    sales_price
  from csx_dw.dws_sale_r_d_detail 
  where sdt>=${hiveconf:before_0mon}
  and sdt<=${hiveconf:current_sdate}
  --and channel_code in('1','7','9')
  and profit<0
)a
left join
(
  select
    id,
    goods_code,     -- '商品编码',
    out_order,     -- '出库顺序',
    qty,     -- '操作数量',
    amt,     -- '操作金额',
    price,     -- '操作单价',
    credential_no,     -- '操作明细的凭证号',
    credential_item_id,     -- '凭证明细id',
    move_type,     -- '移动类型'
    move_name,
    batch_no,     -- '成本批次号',
    source_order_no,     -- '对应凭证的来源单号',
    link_wms_entry_qty,    -- '采购数量'
    link_wms_entry_amt_no_tax,    -- '采购不含税金额'
    link_wms_entry_price_no_tax,    -- '采购不含税单价'
    link_wms_entry_price_no_tax *(1+tax_rate/100) batch_price    -- '含税单价'
  from csx_dw.dws_wms_r_d_batch_detail
  where (in_or_out='1' or(in_or_out='0' and move_type in('108A')))
)b
on a.credential_no=b.credential_no and a.goods_code=b.goods_code;




drop table if exists csx_tmp.report_sale_r_d_negative_profit_batch;
CREATE TABLE csx_tmp.report_sale_r_d_negative_profit_batch(
  id bigint COMMENT '唯一id',
  business_type_code string COMMENT '业务类型编码',
  business_type_name string COMMENT '业务类型',
  customer_no string COMMENT '客户编码',
  customer_name string COMMENT '客户名称',
  goods_code string COMMENT '商品编码',
  goods_name string COMMENT '商品名称',
  origin_order_no string COMMENT '原始单号',
  order_no string COMMENT '订单号',
  batch_no string COMMENT '入库批次号',
  move_name string COMMENT '入库类型',
  qty_caigou decimal(20,6) COMMENT '采购数量',
  amt_no_tax_caigou decimal(20,6) COMMENT '采购不含税金额',
  price_no_tax_caigou decimal(20,6) COMMENT '采购不含税单价',
  qty decimal(20,6) COMMENT '出库数量',
  amt decimal(20,6) COMMENT '出库金额',
  entry_price decimal(20,6) COMMENT '库存平均价',
  purchase_price decimal(20,6) COMMENT '采购报价',
  middle_office_price decimal(20,6) COMMENT '中台报价',
  sales_price decimal(20,6) COMMENT '销售订单含税单价',
  profit decimal(20,6) COMMENT '含税定价毛利额',
  prorate decimal(20,6) COMMENT '含税定价毛利率',
  create_by string COMMENT '创建人' ,
  create_time string COMMENT '创建时间' ,
  update_time string COMMENT '更新时间'  
) COMMENT '负毛利批次明细'
PARTITIONED BY (sdt string COMMENT '日期分区')
STORED AS TEXTFILE;


insert overwrite table csx_tmp.report_sale_r_d_negative_profit_batch partition (sdt)
select 
  `(sdt)?+.+`,
  ${hiveconf:create_by} as create_by,
  ${hiveconf:i_sdate_16} as create_time,
  ${hiveconf:i_sdate_16} as update_time ,
  sdt
from csx_dw.report_sale_r_d_negative_profit_batch 
where sdt>=${hiveconf:before_0mon};


