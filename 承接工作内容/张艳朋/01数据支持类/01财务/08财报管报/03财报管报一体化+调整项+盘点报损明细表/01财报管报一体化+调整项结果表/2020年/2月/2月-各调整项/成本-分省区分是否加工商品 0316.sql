
-- 成本端发货维度数据关系调整单 - 按原单凭证号和商品汇总
drop table b2b_tmp.tmp_data_relation_cas_sale_adjustment_1;
create temporary table b2b_tmp.tmp_data_relation_cas_sale_adjustment_1 
as 
select 
  concat_ws(',',collect_set(cast(id as string))) as id,
  concat_ws(',',collect_set(adjustment_no)) as adjustment_no,
  adjustment_reason,
  adjustment_type,
  item_credential_no,
  item_source_order_no,
  product_code,
  product_name,
  company_code,
  regexp_replace(split(posting_time, ' ')[0], '-', '') as posting_time,
  sum(qty) as qty,
  sum(adjustment_amt) as adjustment_amt,
  sum(adjustment_amt_no_tax) as adjustment_amt_no_tax,
  item_wms_biz_type,
  reservoir_area_code,
  location_code
from csx_ods.source_sync_r_d_data_relation_cas_sale_adjustment
where sdt = '19990101'
and posting_time >= '2020-02-01 00:00:00' 
--and  posting_time < '2020-03-01 00:00:00'
group by adjustment_reason, adjustment_type, item_credential_no, item_source_order_no, product_code, product_name, company_code,
  item_wms_biz_type, reservoir_area_code,location_code,regexp_replace(split(posting_time, ' ')[0], '-', '');

-- 成本核算-报损、盘盈盘亏 - 按原单凭证号和商品汇总
drop table b2b_tmp.tmp_accounting_credential_detail_1;
create temporary table b2b_tmp.tmp_accounting_credential_detail_1
as 
select
credential_no,product_code,location_code,wms_biz_type,reservoir_area_code,move_type,sdt,
sum(amt_no_tax)amt_no_tax,sum(amt)amt
from csx_dw.dwd_cas_r_d_accounting_credential_detail
--where sdt >= '20200201'
--and sdt < '20200301'
group by credential_no,product_code,location_code,wms_biz_type,reservoir_area_code,move_type,sdt;




--与凭证表关联不上的，空  adjustment_no=CA20200306032383
--item_credential_no=,成本表中
select a.*
from b2b_tmp.tmp_data_relation_cas_sale_adjustment_1 a
left join b2b_tmp.cust_sales_m2 b on a.item_credential_no=b.credential_no and a.product_code=b.goods_code
where posting_time >= '20200201' 
and  posting_time < '20200301'
and b.credential_no is null;


--1 2 3 4 5 7  adjustment_amt_no_tax,adjustment_amt
select province_name,
sum(adj_cost_fkc)/10000 adj_cost_fkc,
sum(adj_cost_cgth)/10000 adj_cost_cgth,
sum(adj_cost_gc_xs)/10000 adj_cost_gc_xs,
sum(adj_cost_gc_db)/10000 adj_cost_gc_db,
sum(adj_cost_gc_qt)/10000 adj_cost_gc_qt,
sum(adj_cost_sg)/10000 adj_cost_sg
from(
select b.province_name,
--对抵负库存的成本调整
case when adjustment_reason='in_remark' then adjustment_amt_no_tax end adj_cost_fkc,
--采购退货金额差异的成本调整
case when adjustment_reason='out_remark' then adjustment_amt_no_tax end adj_cost_cgth,
--工厂月末分摊-调整销售订单
case when (adjustment_reason='fac_remark_sale' and item_wms_biz_type in('18','19','20','21','22','23','24','25','26','27','55','58','68','72','73') )
		or (adjustment_reason='fac_remark' and item_wms_biz_type in('18','19','20','21','22','23','24','25','26','27','55','58','68','72','73') )
		then adjustment_amt_no_tax end adj_cost_gc_xs,
--工厂月末分摊-调整跨公司调拨订单
case when (adjustment_reason='fac_remark_sale' and item_wms_biz_type in('06','07','08','09','15','17') )
		or (adjustment_reason='fac_remark' and item_wms_biz_type in('06','07','08','09','15','17') )
		then adjustment_amt_no_tax end adj_cost_gc_db,
--工厂月末分摊-调整其他单据或无原单
case when adjustment_reason='fac_remark_sale' and item_wms_biz_type not in('18','19','20','21','22','23','24','25','26','27','55','58','68','72','73','06','07','08','09','15','17')
		then adjustment_amt_no_tax end adj_cost_gc_qt,
--手工调整销售成本
case when adjustment_reason='manual_remark' then adjustment_amt_no_tax end adj_cost_sg
from csx_ods.source_sync_r_d_data_relation_cas_sale_adjustment a
left join 
(select if(shop_id like '9%',concat('E',substr(shop_id,2,3)),shop_id)shop_id,shop_name,province_name
from csx_dw.shop_m where sdt = 'current') b on b.shop_id=a.location_code
where posting_time >= '2020-02-01 00:00:00' 
and  posting_time < '2020-03-01 00:00:00')a
group by province_name;


--6 价量差工厂未使用的商品 
select b.province_name,sum(amount)/10000 amount
from csx_ods.source_mms_r_a_factory_report_no_share_product a 
left join 
(select if(shop_id like '9%',concat('E',substr(shop_id,2,3)),shop_id)shop_id,shop_name,province_name
from csx_dw.shop_m where sdt = 'current') b on b.shop_id=a.location_code
where sdt='20200315'
and period='2020-02'
group by b.province_name;


-- 8 报损	amt_no_tax,amt
select b.province_name,
sum(item.amt_no_tax)/10000 amt_no_tax
from csx_dw.dwd_cas_r_d_accounting_credential_detail item
left join csx_ods.wms_reservoir_area_ods area on area.warehouse_code = item.location_code and area.reservoir_area_code = item.reservoir_area_code 
left join 
(select if(shop_id like '9%',concat('E',substr(shop_id,2,3)),shop_id)shop_id,shop_name,province_name
from csx_dw.shop_m where sdt = 'current') b on b.shop_id=item.location_code
where item.sdt >= '20200201'
and item.sdt < '20200301'
and item.move_type in( '117A','117B')
and concat(item.wms_biz_type,area.reservoir_area_attribute) in ('35C','36C','37C','38C','38Y','39C','40C','41C','64C','64Y','66C','66Y','76C','76Y','77C','78C')
group by b.province_name;


-- 9 10 盘盈 盘亏 amt_no_tax,amt
select b.province_name,
sum(case when move_type in ( '115A', '116B' ) then -amt_no_tax end )/10000  inventory_p, --盘盈  取移动类型为115A或116B，且库区是PD01的金额
sum(case when move_type in ( '115B', '116A' ) then amt_no_tax end )/10000  inventory_l --盘亏
from csx_dw.dwd_cas_r_d_accounting_credential_detail a 
left join 
(select if(shop_id like '9%',concat('E',substr(shop_id,2,3)),shop_id)shop_id,shop_name,province_name
from csx_dw.shop_m where sdt = 'current') b on b.shop_id=a.location_code
where sdt >= '20200201'
and sdt < '20200301'
and reservoir_area_code = 'PD01'
group by b.province_name;





-- 11  采购后台收入	net_value，value_tax_total
select case when a.cost_name like '目标返利%' then '目标返利' else a.cost_name end cost_name ,
	b.province_name,sum( a.net_value ) 
from csx_ods.settle_settle_bill_ods a
left join (select shop_id,shop_name,province_name
from csx_dw.shop_m where sdt = 'current') b on b.shop_id=a.settle_place_code
where a.attribution_date >= '2020-02-01' 
and a.attribution_date < '2020-03-01'
and a.sdt='19990101'
group by case when a.cost_name like '目标返利%' then '目标返利' else a.cost_name end,b.province_name;

/*
--后台收入拆分到供应商
select case when a.cost_name like '目标返利%' then '目标返利' else a.cost_name end cost_name ,
a.settle_place_code,b.province_name,a.supplier_code,a.supplier_name,sum( a.net_value) net_value,sum( a.value_tax_total) value_tax_total
from csx_ods.settle_settle_bill_ods a
left join (select shop_id,shop_name,province_name
from csx_dw.shop_m where sdt = 'current') b on b.shop_id=a.settle_place_code
where a.attribution_date >= '2020-02-01' 
and a.attribution_date < '2020-03-01'
and a.sdt='19990101'
group by case when a.cost_name like '目标返利%' then '目标返利' else a.cost_name end,
a.settle_place_code,b.province_name,a.supplier_code,a.supplier_name;
*/


-----0316拆分到渠道
--工厂月末分摊-调整销售订单 拆分到渠道
select  
b.province_name,c.channel_name,
sum(a.adjustment_amt)adjustment_amt,
sum(a.adjustment_amt_no_tax) adjustment_amt_no_tax
from 
(select * from b2b_tmp.tmp_data_relation_cas_sale_adjustment_1
where posting_time >= '20200201' 
and  posting_time < '20200301'
and item_source_order_no<>'' and item_source_order_no is not null
and ((adjustment_reason='fac_remark_sale' and item_wms_biz_type in('18','19','20','21','22','23','24','25','26','27','55','58','68','72','73') )
		or (adjustment_reason='fac_remark' and item_wms_biz_type in('18','19','20','21','22','23','24','25','26','27','55','58','68','72','73') ))
)a
left join (
select AA,channel_name,order_no,goods_code,sdt,
sum(sales_value)sales_value,
sum(excluding_tax_sales)excluding_tax_sales
from b2b_tmp.cust_sales_m2
--where AA='成本'
group by AA,channel_name,order_no,goods_code,sdt
)c on a.item_source_order_no=c.order_no and a.product_code=c.goods_code
left join 
(select if(shop_id like '9%',concat('E',substr(shop_id,2,3)),shop_id)shop_id,shop_name,province_name
from csx_dw.shop_m where sdt = 'current') b on b.shop_id=a.location_code
group by b.province_name,c.channel_name;

--------------------------------------------------------------------------------------------------------------------------

-----0316数据明细 汇总 不含税额

--1 2 3 4 5 7  adjustment_amt_no_tax,adjustment_amt
select a.province_name,
if(d.workshop_code is null,'不是工厂商品','是工厂商品') as is_factory_goods_name,
sum(adj_cost_fkc)/10000 adj_cost_fkc,
sum(adj_cost_cgth)/10000 adj_cost_cgth,
sum(adj_cost_gc_xs)/10000 adj_cost_gc_xs,
sum(adj_cost_gc_db)/10000 adj_cost_gc_db,
sum(adj_cost_gc_qt)/10000 adj_cost_gc_qt,
sum(adj_cost_sg)/10000 adj_cost_sg
from(
select b.province_code,b.province_name,a.product_code,a.product_name,
--对抵负库存的成本调整
case when adjustment_reason='in_remark' then adjustment_amt_no_tax end adj_cost_fkc,
--采购退货金额差异的成本调整
case when adjustment_reason='out_remark' then adjustment_amt_no_tax end adj_cost_cgth,
--工厂月末分摊-调整销售订单
case when (adjustment_reason='fac_remark_sale' and item_wms_biz_type in('18','19','20','21','22','23','24','25','26','27','55','58','68','72','73') )
		or (adjustment_reason='fac_remark' and item_wms_biz_type in('18','19','20','21','22','23','24','25','26','27','55','58','68','72','73') )
		then adjustment_amt_no_tax end adj_cost_gc_xs,
--工厂月末分摊-调整跨公司调拨订单
case when (adjustment_reason='fac_remark_sale' and item_wms_biz_type in('06','07','08','09','15','17') )
		or (adjustment_reason='fac_remark' and item_wms_biz_type in('06','07','08','09','15','17') )
		then adjustment_amt_no_tax end adj_cost_gc_db,
--工厂月末分摊-调整其他单据或无原单
case when adjustment_reason='fac_remark_sale' and item_wms_biz_type not in('18','19','20','21','22','23','24','25','26','27','55','58','68','72','73','06','07','08','09','15','17')
		then adjustment_amt_no_tax end adj_cost_gc_qt,
--手工调整销售成本
case when adjustment_reason='manual_remark' then adjustment_amt_no_tax end adj_cost_sg
from 
(select * from csx_ods.source_sync_r_d_data_relation_cas_sale_adjustment
where posting_time >= '2020-02-01 00:00:00' 
and  posting_time < '2020-03-01 00:00:00') a
left join 
(select if(shop_id like '9%',concat('E',substr(shop_id,2,3)),shop_id)shop_id,shop_name,province_code,province_name
from csx_dw.shop_m where sdt = 'current') b on b.shop_id=a.location_code
)a
left join
(select
    workshop_code, province_code, goods_code
  from csx_dw.dws_mms_w_a_factory_setting_craft_once_all
  where sdt='current' and new_or_old=1
)d on a.province_code=d.province_code and a.product_code=d.goods_code
group by a.province_name,
if(d.workshop_code is null,'不是工厂商品','是工厂商品');



--工厂月末分摊-调整销售订单
insert overwrite directory '/tmp/raoyanhua/linshi01' row format delimited fields terminated by '\t' 
select  
substr(a.posting_time,1,6)posting_time,a.province_name,a.channel_name,
if(d.workshop_code is null,'不是工厂商品','是工厂商品') as is_factory_goods_name,
sum(a.adjustment_amt)/10000 adjustment_amt,
sum(a.adjustment_amt_no_tax)/10000 adjustment_amt_no_tax
from 
(select a.*,c.channel_name,b.province_code,b.province_name
from
(select * from b2b_tmp.tmp_data_relation_cas_sale_adjustment_1
where posting_time >= '20200201' 
and  posting_time < '20200301'
and item_source_order_no<>'' and item_source_order_no is not null
and ((adjustment_reason='fac_remark_sale' and item_wms_biz_type in('18','19','20','21','22','23','24','25','26','27','55','58','68','72','73') )
		or (adjustment_reason='fac_remark' and item_wms_biz_type in('18','19','20','21','22','23','24','25','26','27','55','58','68','72','73') ))
)a
left join (
select AA,channel_name,order_no,goods_code,sdt,
sum(sales_value)sales_value,
sum(excluding_tax_sales)excluding_tax_sales
from b2b_tmp.cust_sales_m2
--where AA='成本'
group by AA,channel_name,order_no,goods_code,sdt
)c on a.item_source_order_no=c.order_no and a.product_code=c.goods_code
left join 
(select if(shop_id like '9%',concat('E',substr(shop_id,2,3)),shop_id)shop_id,shop_name,province_code,province_name
from csx_dw.shop_m where sdt = 'current') b on b.shop_id=a.location_code
) a
left join
(select
    workshop_code, province_code, goods_code
  from csx_dw.dws_mms_w_a_factory_setting_craft_once_all
  where sdt='current' and new_or_old=1
)d on a.province_code=d.province_code and a.product_code=d.goods_code
group by substr(a.posting_time,1,6),a.province_name,a.channel_name,
if(d.workshop_code is null,'不是工厂商品','是工厂商品');




--6 价量差工厂未使用的商品 
select a.period,a.province_name,
if(d.workshop_code is null,'不是工厂商品','是工厂商品') as is_factory_goods_name,
sum(amount)/10000 amount
from
(select a.*,b.province_code,b.province_name
from (select * from csx_ods.source_mms_r_a_factory_report_no_share_product
where sdt='20200315'
and period in('2020-02'))a 
left join 
(select if(shop_id like '9%',concat('E',substr(shop_id,2,3)),shop_id)shop_id,shop_name,province_code,province_name
from csx_dw.shop_m where sdt = 'current') b on b.shop_id=a.location_code
left join 
(select goods_id,department_id,department_name
	from csx_dw.goods_m where sdt = 'current' )c on a.product_code=c.goods_id
) a
left join
(select
    workshop_code, province_code, goods_code
  from csx_dw.dws_mms_w_a_factory_setting_craft_once_all
  where sdt='current' and new_or_old=1
)d on a.province_code=d.province_code and a.product_code=d.goods_code
group by a.period,a.province_name,
if(d.workshop_code is null,'不是工厂商品','是工厂商品');




-- 9 10 盘盈 盘亏 amt_no_tax,amt
select substr(a.sdt,1,6)sdt,a.province_name,
if(d.workshop_code is null,'不是工厂商品','是工厂商品') as is_factory_goods_name,
sum(case when move_type in ( '115A', '116B' ) then -amt end )/10000  inventory_p, --盘盈  取移动类型为115A或116B，且库区是PD01的金额
sum(case when move_type in ( '115A', '116B' ) then -amt_no_tax end )/10000  inventory_p_notax, --盘盈  
sum(case when move_type in ( '115B', '116A' ) then amt end )/10000  inventory_l, --盘亏
sum(case when move_type in ( '115B', '116A' ) then amt_no_tax end )/10000  inventory_l_notax --盘亏
from
(select a.*,b.province_code,b.province_name
from (select * from csx_dw.dwd_cas_r_d_accounting_credential_detail
where sdt >= '20200201'
and sdt < '20200301'
and reservoir_area_code = 'PD01') a 
left join 
(select if(shop_id like '9%',concat('E',substr(shop_id,2,3)),shop_id)shop_id,shop_name,province_code,province_name
from csx_dw.shop_m where sdt = 'current') b on b.shop_id=a.location_code
left join 
(select goods_id,department_id,department_name
	from csx_dw.goods_m where sdt = 'current' )c on a.product_code=c.goods_id
) a
left join
(select
    workshop_code, province_code, goods_code
  from csx_dw.dws_mms_w_a_factory_setting_craft_once_all
  where sdt='current' and new_or_old=1
)d on a.province_code=d.province_code and a.product_code=d.goods_code
group by substr(a.sdt,1,6),a.province_name,
if(d.workshop_code is null,'不是工厂商品','是工厂商品');


-- 8 报损	amt_no_tax,amt
select 
substr(a.sdt,1,6)sdt,a.province_name,
if(d.workshop_code is null,'不是工厂商品','是工厂商品') as is_factory_goods_name,
sum(item.amt_no_tax)/10000 amt_no_tax
from
(select a.*,b.province_code,b.province_name
from (select * from csx_dw.dwd_cas_r_d_accounting_credential_detail
where sdt >= '20200201'
and sdt < '20200301'
and item.move_type in( '117A','117B')) item
left join csx_ods.wms_reservoir_area_ods area on area.warehouse_code = item.location_code and area.reservoir_area_code = item.reservoir_area_code 
left join 
(select if(shop_id like '9%',concat('E',substr(shop_id,2,3)),shop_id)shop_id,shop_name,province_code,province_name
from csx_dw.shop_m where sdt = 'current') b on b.shop_id=item.location_code
where concat(item.wms_biz_type,area.reservoir_area_attribute) in ('35C','36C','37C','38C','38Y','39C','40C','41C','64C','64Y','66C','66Y','76C','76Y','77C','78C')
) a
left join
(select
    workshop_code, province_code, goods_code
  from csx_dw.dws_mms_w_a_factory_setting_craft_once_all
  where sdt='current' and new_or_old=1
)d on a.province_code=d.province_code and a.product_code=d.goods_code
group by substr(a.sdt,1,6),a.province_name,
if(d.workshop_code is null,'不是工厂商品','是工厂商品');




--------------------------------------------------------------------------------------------------------------------------
--北京明细
-----0316数据明细 汇总 不含税额   

--1 2 3 4 5 7  adjustment_amt_no_tax,adjustment_amt
insert overwrite directory '/tmp/raoyanhua/linshi01' row format delimited fields terminated by '\t' 
select a.province_code,a.province_name,a.product_code,a.product_name,b.department_id,b.department_name,
if(d.workshop_code is null,'不是工厂商品','是工厂商品') as is_factory_goods_name,
sum(adj_cost_fkc_tax) adj_cost_fkc_tax,
sum(adj_cost_fkc) adj_cost_fkc,
sum(adj_cost_cgth_tax) adj_cost_cgth_tax,
sum(adj_cost_cgth) adj_cost_cgth,
--sum(adj_cost_gc_xs_tax) adj_cost_gc_xs_tax,
--sum(adj_cost_gc_xs) adj_cost_gc_xs,
sum(adj_cost_gc_db_tax) adj_cost_gc_db_tax,
sum(adj_cost_gc_db) adj_cost_gc_db,
sum(adj_cost_gc_qt_tax) adj_cost_gc_qt_tax,
sum(adj_cost_gc_qt) adj_cost_gc_qt,
sum(adj_cost_sg_tax) adj_cost_sg_tax,
sum(adj_cost_sg) adj_cost_sg
from(
select b.province_code,b.province_name,a.product_code,a.product_name,a.item_source_order_no,

--对抵负库存的成本调整
case when adjustment_reason='in_remark' then adjustment_amt end adj_cost_fkc_tax,
--采购退货金额差异的成本调整
case when adjustment_reason='out_remark' then adjustment_amt end adj_cost_cgth_tax,
--工厂月末分摊-调整销售订单
--case when (adjustment_reason='fac_remark_sale' and item_wms_biz_type in('18','19','20','21','22','23','24','25','26','27','55','58','68','72','73') )
--		or (adjustment_reason='fac_remark' and item_wms_biz_type in('18','19','20','21','22','23','24','25','26','27','55','58','68','72','73') )
--		then adjustment_amt end adj_cost_gc_xs_tax,
--工厂月末分摊-调整跨公司调拨订单
case when (adjustment_reason='fac_remark_sale' and item_wms_biz_type in('06','07','08','09','15','17') )
		or (adjustment_reason='fac_remark' and item_wms_biz_type in('06','07','08','09','15','17') )
		then adjustment_amt end adj_cost_gc_db_tax,
--工厂月末分摊-调整其他单据或无原单
case when adjustment_reason='fac_remark_sale' and item_wms_biz_type not in('18','19','20','21','22','23','24','25','26','27','55','58','68','72','73','06','07','08','09','15','17')
		then adjustment_amt end adj_cost_gc_qt_tax,
--手工调整销售成本
case when adjustment_reason='manual_remark' then adjustment_amt end adj_cost_sg_tax,

--对抵负库存的成本调整
case when adjustment_reason='in_remark' then adjustment_amt_no_tax end adj_cost_fkc,
--采购退货金额差异的成本调整
case when adjustment_reason='out_remark' then adjustment_amt_no_tax end adj_cost_cgth,
--工厂月末分摊-调整销售订单
--case when (adjustment_reason='fac_remark_sale' and item_wms_biz_type in('18','19','20','21','22','23','24','25','26','27','55','58','68','72','73') )
--		or (adjustment_reason='fac_remark' and item_wms_biz_type in('18','19','20','21','22','23','24','25','26','27','55','58','68','72','73') )
--		then adjustment_amt_no_tax end adj_cost_gc_xs,
--工厂月末分摊-调整跨公司调拨订单
case when (adjustment_reason='fac_remark_sale' and item_wms_biz_type in('06','07','08','09','15','17') )
		or (adjustment_reason='fac_remark' and item_wms_biz_type in('06','07','08','09','15','17') )
		then adjustment_amt_no_tax end adj_cost_gc_db,
--工厂月末分摊-调整其他单据或无原单
case when adjustment_reason='fac_remark_sale' and item_wms_biz_type not in('18','19','20','21','22','23','24','25','26','27','55','58','68','72','73','06','07','08','09','15','17')
		then adjustment_amt_no_tax end adj_cost_gc_qt,
--手工调整销售成本
case when adjustment_reason='manual_remark' then adjustment_amt_no_tax end adj_cost_sg
from 
(select * from csx_ods.source_sync_r_d_data_relation_cas_sale_adjustment
where posting_time >= '2020-02-01 00:00:00' 
and  posting_time < '2020-03-01 00:00:00') a
left join 
(select if(shop_id like '9%',concat('E',substr(shop_id,2,3)),shop_id)shop_id,shop_name,province_code,province_name
from csx_dw.shop_m where sdt = 'current') b on b.shop_id=a.location_code
)a
left join 
(select goods_id,department_id,department_name
	from csx_dw.goods_m where sdt = 'current' )b on a.product_code=b.goods_id
left join
(select
    workshop_code, province_code, goods_code
  from csx_dw.dws_mms_w_a_factory_setting_craft_once_all
  where sdt='current' and new_or_old=1
)d on a.province_code=d.province_code and a.product_code=d.goods_code
where a.province_code='110000'
and (adj_cost_fkc is not null 
or adj_cost_cgth is not null 
--or adj_cost_gc_xs is not null 
or adj_cost_gc_db is not null 
or adj_cost_gc_qt is not null 
or adj_cost_sg is not null )
group by a.province_code,a.province_name,a.product_code,a.product_name,b.department_id,b.department_name,
if(d.workshop_code is null,'不是工厂商品','是工厂商品');



--工厂月末分摊-调整销售订单
insert overwrite directory '/tmp/raoyanhua/linshi02' row format delimited fields terminated by '\t' 
select  a.province_code,a.province_name,a.product_code,a.product_name,b.department_id,b.department_name,
a.channel_name,
if(d.workshop_code is null,'不是工厂商品','是工厂商品') as is_factory_goods_name,
sum(adjustment_amt)adjustment_amt,
sum(adjustment_amt_no_tax) adjustment_amt_no_tax
from 
(select a.*,c.channel_name,b.province_code,b.province_name,b.shop_id,b.shop_name
from
(select * from b2b_tmp.tmp_data_relation_cas_sale_adjustment_1
where posting_time >= '20200201' 
and  posting_time < '20200301'
and item_source_order_no<>'' and item_source_order_no is not null
and ((adjustment_reason='fac_remark_sale' and item_wms_biz_type in('18','19','20','21','22','23','24','25','26','27','55','58','68','72','73') )
		or (adjustment_reason='fac_remark' and item_wms_biz_type in('18','19','20','21','22','23','24','25','26','27','55','58','68','72','73') ))
)a
left join (
select AA,channel_name,order_no,goods_code,sdt,
sum(sales_value)sales_value,
sum(excluding_tax_sales)excluding_tax_sales
from b2b_tmp.cust_sales_m2
--where AA='成本'
group by AA,channel_name,order_no,goods_code,sdt
)c on a.item_source_order_no=c.order_no and a.product_code=c.goods_code
left join 
(select if(shop_id like '9%',concat('E',substr(shop_id,2,3)),shop_id)shop_id,shop_name,province_code,province_name
from csx_dw.shop_m where sdt = 'current') b on b.shop_id=a.location_code
) a
left join 
(select goods_id,department_id,department_name
	from csx_dw.goods_m where sdt = 'current' )b on a.product_code=b.goods_id
left join
(select
    workshop_code, province_code, goods_code
  from csx_dw.dws_mms_w_a_factory_setting_craft_once_all
  where sdt='current' and new_or_old=1
)d on a.province_code=d.province_code and a.product_code=d.goods_code
where a.province_code='110000'
group by a.province_code,a.province_name,a.product_code,a.product_name,b.department_id,b.department_name,
a.channel_name,
if(d.workshop_code is null,'不是工厂商品','是工厂商品');




--6 价量差工厂未使用的商品 
insert overwrite directory '/tmp/raoyanhua/linshi03' row format delimited fields terminated by '\t'
select a.province_code,a.province_name,a.product_code,b.goods_name,b.department_id,b.department_name,
if(d.workshop_code is null,'不是工厂商品','是工厂商品') as is_factory_goods_name,
 sum(amount)amount
from
(select a.*,b.province_code,b.province_name,b.shop_id,b.shop_name
from (select * from csx_ods.source_mms_r_a_factory_report_no_share_product
where sdt='20200315'
and period in('2020-02'))a 
left join 
(select if(shop_id like '9%',concat('E',substr(shop_id,2,3)),shop_id)shop_id,shop_name,province_code,province_name
from csx_dw.shop_m where sdt = 'current') b on b.shop_id=a.location_code
) a
left join 
(select goods_id,goods_name,department_id,department_name
	from csx_dw.goods_m where sdt = 'current' )b on a.product_code=b.goods_id
left join
(select
    workshop_code, province_code, goods_code
  from csx_dw.dws_mms_w_a_factory_setting_craft_once_all
  where sdt='current' and new_or_old=1
)d on a.province_code=d.province_code and a.product_code=d.goods_code
where a.province_code='110000'
group by a.province_code,a.province_name,a.product_code,b.goods_name,b.department_id,b.department_name,
if(d.workshop_code is null,'不是工厂商品','是工厂商品');




-- 9 10 盘盈 盘亏 amt_no_tax,amt
insert overwrite directory '/tmp/raoyanhua/linshi04' row format delimited fields terminated by '\t'
select a.province_code,a.province_name,a.product_code,a.product_name,b.department_id,b.department_name,
if(d.workshop_code is null,'不是工厂商品','是工厂商品') as is_factory_goods_name,
sum(case when move_type in ( '115A', '116B' ) then -amt end )/10000  inventory_p, --盘盈  取移动类型为115A或116B，且库区是PD01的金额
sum(case when move_type in ( '115A', '116B' ) then -amt_no_tax end )/10000  inventory_p_notax, --盘盈  
sum(case when move_type in ( '115B', '116A' ) then amt end )/10000  inventory_l, --盘亏
sum(case when move_type in ( '115B', '116A' ) then amt_no_tax end )/10000  inventory_l_notax --盘亏
from
(select a.*,b.province_code,b.province_name,b.shop_id,b.shop_name
from (select * from csx_dw.dwd_cas_r_d_accounting_credential_detail
where sdt >= '20200201'
and sdt < '20200301'
and reservoir_area_code = 'PD01') a 
left join 
(select if(shop_id like '9%',concat('E',substr(shop_id,2,3)),shop_id)shop_id,shop_name,province_code,province_name
from csx_dw.shop_m where sdt = 'current') b on b.shop_id=a.location_code
) a
left join 
(select goods_id,goods_name,department_id,department_name
	from csx_dw.goods_m where sdt = 'current' )b on a.product_code=b.goods_id
left join
(select
    workshop_code, province_code, goods_code
  from csx_dw.dws_mms_w_a_factory_setting_craft_once_all
  where sdt='current' and new_or_old=1
)d on a.province_code=d.province_code and a.product_code=d.goods_code
where a.province_code='110000'
group by a.province_code,a.province_name,a.product_code,a.product_name,b.department_id,b.department_name,
if(d.workshop_code is null,'不是工厂商品','是工厂商品');


-- 8 报损	amt_no_tax,amt
insert overwrite directory '/tmp/raoyanhua/linshi05' row format delimited fields terminated by '\t'
select 
a.province_code,a.province_name,a.product_code,a.product_name,b.department_id,b.department_name,
if(d.workshop_code is null,'不是工厂商品','是工厂商品') as is_factory_goods_name,
sum(a.amt) amt,
sum(a.amt_no_tax) amt_no_tax
from
(select item.*,b.province_code,b.province_name,b.shop_id,b.shop_name
from (select * from csx_dw.dwd_cas_r_d_accounting_credential_detail
where sdt >= '20200201'
and sdt < '20200301'
and move_type in( '117A','117B')) item
left join csx_ods.wms_reservoir_area_ods area on area.warehouse_code = item.location_code and area.reservoir_area_code = item.reservoir_area_code 
left join 
(select if(shop_id like '9%',concat('E',substr(shop_id,2,3)),shop_id)shop_id,shop_name,province_code,province_name
from csx_dw.shop_m where sdt = 'current') b on b.shop_id=item.location_code
where concat(item.wms_biz_type,area.reservoir_area_attribute) in ('35C','36C','37C','38C','38Y','39C','40C','41C','64C','64Y','66C','66Y','76C','76Y','77C','78C')
) a
left join 
(select goods_id,department_id,department_name
	from csx_dw.goods_m where sdt = 'current' )b on a.product_code=b.goods_id
left join
(select
    workshop_code, province_code, goods_code
  from csx_dw.dws_mms_w_a_factory_setting_craft_once_all
  where sdt='current' and new_or_old=1
)d on a.province_code=d.province_code and a.product_code=d.goods_code
where a.province_code='110000'
group by a.province_code,a.province_name,a.product_code,a.product_name,b.department_id,b.department_name,
if(d.workshop_code is null,'不是工厂商品','是工厂商品');




