--销售订单所在省区渠道
drop table b2b_tmp.tmp_sale_order_flag;
create table b2b_tmp.tmp_sale_order_flag 
as 
select channel_name,province_code,province_name,order_no,goods_code,
sum(sales_value)sales_value,
sum(excluding_tax_sales)excluding_tax_sales
from csx_dw.dws_sale_r_d_customer_sale
where sdt>='20200101'
and sales_type in ('sapqyg','sapgc','qyg','sc','bbc')
group by channel_name,province_code,province_name,order_no,goods_code;


left join b2b_tmp.tmp_sale_order_flag c on a.item_source_order_no=c.order_no and a.product_code=c.goods_code






-- 成本端发货维度数据关系调整单 - 按原单凭证号和商品汇总
drop table b2b_tmp.tmp_data_relation_cas_sale_adjustment_1;
create temporary table b2b_tmp.tmp_data_relation_cas_sale_adjustment_1 
as 
select 
  concat_ws(',',collect_set(cast(id as string))) as id,
  concat_ws(',',collect_set(adjustment_no)) as adjustment_no,
  adjustment_reason,
  adjustment_type,
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
and posting_time >= '2020-03-01 00:00:00' 
and  posting_time < '2020-04-01 00:00:00'
group by adjustment_reason, adjustment_type, item_source_order_no, product_code, product_name, company_code,
  item_wms_biz_type, reservoir_area_code,location_code,regexp_replace(split(posting_time, ' ')[0], '-', '');



--与凭证表关联不上的，空  adjustment_no=CA20200306032383
--item_source_order_no=,成本表中
select a.*
from b2b_tmp.tmp_data_relation_cas_sale_adjustment_1 a
left join b2b_tmp.tmp_sale_order_flag b on a.item_source_order_no=b.order_no and a.product_code=b.goods_code
where a.posting_time >= '20200301' 
and  a.posting_time < '20200401'
and b.order_no is not null;

--测试工厂调整成本数据的省区是否可以用订单与商品编号匹配销售数据
/*
select b.province_name,c.province_name province_name1,a.item_source_order_no,a.product_code,
--对抵负库存的成本调整
case when adjustment_reason='in_remark' then adjustment_amt_no_tax end adj_cost_fkc,
--采购退货金额差异的成本调整
case when adjustment_reason='out_remark' then adjustment_amt_no_tax end adj_cost_cgth,
--工厂月末分摊-调整销售订单
case when (adjustment_reason in('fac_remark_sale','fac_remark') 
			and item_wms_biz_type in('18','19','20','21','22','23','24','25','26','27','55','58','68','72','73',
									'A18','A20','A21','A22','A23','A24','A25','A55') )
		then adjustment_amt_no_tax end adj_cost_gc_xs,
--工厂月末分摊-调整跨公司调拨订单
case when (adjustment_reason in('fac_remark_sale','fac_remark') and item_wms_biz_type in('06','07','08','09','15','17') )
		then adjustment_amt_no_tax end adj_cost_gc_db,
--工厂月末分摊-调整其他
case when adjustment_reason='fac_remark_sale' 
		and item_wms_biz_type not in('18','19','20','21','22','23','24','25','26','27','55','58','68','72','73','06','07','08','09','15','17',
									'A18','A20','A21','A22','A23','A24','A25','A55')
		then adjustment_amt_no_tax end adj_cost_gc_qt,
--手工调整销售成本
case when adjustment_reason='manual_remark' then if(adjustment_type='stock',-1*adjustment_amt_no_tax,adjustment_amt_no_tax) end adj_cost_sg
from (select * from csx_ods.source_sync_r_d_data_relation_cas_sale_adjustment
where sdt = '19990101'
and posting_time >= '2020-03-01 00:00:00' 
and  posting_time < '2020-04-01 00:00:00') a
left join 
(select if(shop_id like '9%',concat('E',substr(shop_id,2,3)),shop_id)shop_id,shop_name,province_name
from csx_dw.shop_m where sdt = 'current') b on b.shop_id=a.location_code
left join b2b_tmp.tmp_sale_order_flag c on a.item_source_order_no=c.order_no and a.product_code=c.goods_code
where c.province_name is null;


insert overwrite directory '/tmp/raoyanhua/linshi01' row format delimited fields terminated by '\t' 
select b.province_name,c.province_name province_name1,c.channel_name,a.item_source_order_no,a.product_code,
--工厂月末分摊-调整销售订单
case when (adjustment_reason in('fac_remark_sale','fac_remark') 
			and item_wms_biz_type in('18','19','20','21','22','23','24','25','26','27','55','58','68','72','73',
									'A18','A20','A21','A22','A23','A24','A25','A55') )
		then adjustment_amt_no_tax end adj_cost_gc_xs
from (select * from csx_ods.source_sync_r_d_data_relation_cas_sale_adjustment
where sdt = '19990101'
and posting_time >= '2020-03-01 00:00:00' 
and  posting_time < '2020-04-01 00:00:00'
and adjustment_reason in('fac_remark_sale','fac_remark') 
and item_wms_biz_type in('18','19','20','21','22','23','24','25','26','27','55','58','68','72','73',
									'A18','A20','A21','A22','A23','A24','A25','A55') ) a
left join 
(select if(shop_id like '9%',concat('E',substr(shop_id,2,3)),shop_id)shop_id,shop_name,province_name
from csx_dw.shop_m where sdt = 'current') b on b.shop_id=a.location_code
left join b2b_tmp.tmp_sale_order_flag c on a.item_source_order_no=c.order_no and a.product_code=c.goods_code
where c.province_name is null;

--成本调整订单在销售表找不到 4月9日找到了出库日期4月7-8日
select 
dc_code,dc_name,customer_no,customer_name,channel,
first_category,sales_province,sales_name,work_no,
order_time,sales_date,goods_code,goods_name,
origin_order_no,order_no,sap_doc_number,
report_price,cost_price,purchase_price,middle_office_price,
sales_price,promotion_cost_price,promotion_price,supplier_cost_rate,tax_rate,
order_qty,sales_qty,sales_value,sales_cost,profit,front_profit,return_flag,order_mode,order_kind,sdt,sales_type
from csx_dw.dws_sale_r_d_customer_sale
where sdt>='20191201'
and sales_type in ('qyg','sapqyg','sapgc','sc','bbc')
and( (order_no='OY200407000779' and goods_code='978740')
or (order_no='OY200408000089' and goods_code='903413'));
*/

--1 2 3 4 5 7  adjustment_amt_no_tax,adjustment_amt
select province_name,
sum(adj_cost_fkc)/10000 adj_cost_fkc,
sum(adj_cost_cgth)/10000 adj_cost_cgth,
sum(adj_cost_gc_xs)/10000 adj_cost_gc_xs,
sum(adj_cost_gc_db)/10000 adj_cost_gc_db,
sum(adj_cost_gc_qt)/10000 adj_cost_gc_qt,
sum(adj_cost_sg)/10000 adj_cost_sg
from(
select case when a.location_code='W0H4' then '供应链' else b.province_name end province_name,
--对抵负库存的成本调整
case when adjustment_reason='in_remark' then adjustment_amt_no_tax end adj_cost_fkc,
--采购退货金额差异的成本调整
case when adjustment_reason='out_remark' then adjustment_amt_no_tax end adj_cost_cgth,
--工厂月末分摊-调整销售订单
case when (adjustment_reason in('fac_remark_sale','fac_remark') 
			and item_wms_biz_type in('18','19','20','21','22','23','24','25','26','27','55','58','68','72','73',
									'A18','A20','A21','A22','A23','A24','A25','A55') )
		then adjustment_amt_no_tax end adj_cost_gc_xs,
--工厂月末分摊-调整跨公司调拨订单
case when (adjustment_reason in('fac_remark_sale','fac_remark') and item_wms_biz_type in('06','07','08','09','15','17','A06','A07','A08','A09','A15') )
		then adjustment_amt_no_tax end adj_cost_gc_db,
--工厂月末分摊-调整其他
case when adjustment_reason='fac_remark_sale' 
		and item_wms_biz_type not in('18','19','20','21','22','23','24','25','26','27','55','58','68','72','73','06','07','08','09','15','17',
									'A18','A20','A21','A22','A23','A24','A25','A55')
		then adjustment_amt_no_tax end adj_cost_gc_qt,
--手工调整销售成本
case when adjustment_reason='manual_remark' then if(adjustment_type='stock',-1*adjustment_amt_no_tax,adjustment_amt_no_tax) end adj_cost_sg
from (select * from csx_ods.source_sync_r_d_data_relation_cas_sale_adjustment
where sdt = '19990101'
and posting_time >= '2020-03-01 00:00:00' 
and  posting_time < '2020-04-01 00:00:00') a
left join 
(select if(shop_id like '9%',concat('E',substr(shop_id,2,3)),shop_id)shop_id,shop_name,province_name
from csx_dw.shop_m where sdt = 'current') b on b.shop_id=a.location_code
)a
group by province_name;


--6 价量差工厂未使用的商品  不区分是否含税金额
select case when a.location_code='W0H4' then '供应链' else b.province_name end province_name,
sum(amount)/10000 amount
from csx_ods.source_mms_r_a_factory_report_no_share_product a 
left join 
(select if(shop_id like '9%',concat('E',substr(shop_id,2,3)),shop_id)shop_id,shop_name,province_name
from csx_dw.shop_m where sdt = 'current') b on b.shop_id=a.location_code
where sdt='20200408' 
and period='2020-03'
group by case when a.location_code='W0H4' then '供应链' else b.province_name end;


--8 报损	不含税 amt_no_tax,---含税 amt
select case when a.location_code='W0H4' then '供应链' else b.province_name end province_name,
sum(amt_no_tax)/10000 amt_no_tax,
sum(amt)/10000 amt
from
(select a.*
from csx_ods.source_sync_r_d_data_sync_broken_item a
where a.sdt = '19990101'
and (( a.wms_biz_type <>'64' and a.reservoir_area_prop = 'C' and ( a.purchase_group_code like 'H%' or a.purchase_group_code like 'U%' ) ) 
or a.wms_biz_type = '64' )
and a.posting_time >= '2020-03-01 00:00:00' 
and a.posting_time < '2020-04-01 00:00:00')a
left join 
(select if(shop_id like '9%',concat('E',substr(shop_id,2,3)),shop_id)shop_id,shop_name,province_name
from csx_dw.shop_m where sdt = 'current') b on b.shop_id=a.location_code
group by case when a.location_code='W0H4' then '供应链' else b.province_name end;


-- 9 10 盘盈 盘亏 不含税 amt_no_tax,---含税 amt
select case when a.location_code='W0H4' then '供应链' else b.province_name end province_name,
sum(case when amt_no_tax<0 then amt_no_tax end )/10000  inventory_p_no, --盘盈  
sum(case when amt_no_tax>=0 then amt_no_tax end )/10000  inventory_l_no, --盘亏
sum(case when amt<0 then amt end )/10000  inventory_p, --盘盈  
sum(case when amt>=0 then amt end )/10000  inventory_l --盘亏
from
(select a.*
from csx_ods.source_sync_r_d_data_sync_inventory_item a
where a.sdt = '19990101'
and a.reservoir_area_code = 'PD01' 
and ( a.purchase_group_code like 'H%' or a.purchase_group_code like 'U%' ) 
and a.posting_time >= '2020-03-01 00:00:00' 
and a.posting_time < '2020-04-01 00:00:00')a
left join 
(select if(shop_id like '9%',concat('E',substr(shop_id,2,3)),shop_id)shop_id,shop_name,province_name
from csx_dw.shop_m where sdt = 'current') b on b.shop_id=a.location_code
group by case when a.location_code='W0H4' then '供应链' else b.province_name end;


-- 11  采购后台收入	net_value，value_tax_total
select 
case when a.cost_name like '目标返利%' then '目标返利'
			when a.cost_name like '仓储服务费%' then '仓储服务费'  
			else a.cost_name end cost_name ,
case when a.settle_place_code='W0H4' then '供应链' else b.province_name end province_name,
sum(net_value)/10000 net_value,
sum(value_tax_total)/10000 value_tax_total
from 
( select * from csx_ods.settle_settle_bill_ods 
where sdt='19990101'
and attribution_date >= '2020-03-01' 
and attribution_date < '2020-04-01' )a
left join (select shop_id,shop_name,province_name
from csx_dw.shop_m where sdt = 'current') b on b.shop_id=a.settle_place_code
group by case when a.cost_name like '目标返利%' then '目标返利'
			when a.cost_name like '仓储服务费%' then '仓储服务费'  
			else a.cost_name end,
case when a.settle_place_code='W0H4' then '供应链' else b.province_name end;

--工厂分摊后成本小于0，未分摊金额 不区分是否含税
select case when a.location_code='W0H4' then '供应链' else b.province_name end province_name,
sum(d_cost_subtotal)/10000 d_cost_subtotal
from
(select *
from csx_ods.source_mms_r_a_factory_report_diff_apportion_header
where sdt='20200408'
and period = '2020-03' 
and notice_status = '3')a
left join 
(select if(shop_id like '9%',concat('E',substr(shop_id,2,3)),shop_id)shop_id,shop_name,province_name
from csx_dw.shop_m where sdt = 'current') b on b.shop_id=a.location_code
group by case when a.location_code='W0H4' then '供应链' else b.province_name end;

--销售后台支出-调价、返利  Z68是返利Z69是调价 --hive中返利表
select a.adjust_reason,
case when a.inventory_dc_code='W0H4' then '供应链' else b.province_name end province_name,
sum(total_price/(1+tax_rate/100))/10000 amt_no_tax,
sum(total_price)/10000 amt
from
(select *
from csx_dw.dwd_csms_r_d_yszx_customer_rebate_detail_new 
where type in ('0','1')
and commit_time>='2020-03-01 00:00:00'
and commit_time<'2020-04-01 00:00:00')a
left join 
(select if(shop_id like '9%',concat('E',substr(shop_id,2,3)),shop_id)shop_id,shop_name,province_name
from csx_dw.shop_m where sdt = 'current') b on b.shop_id=a.inventory_dc_code
group by a.adjust_reason,
	case when a.inventory_dc_code='W0H4' then '供应链' else b.province_name end
order by a.adjust_reason;





/*
--后台收入拆分到供应商
select case when a.cost_name like '目标返利%' then '目标返利' else a.cost_name end cost_name ,
a.settle_place_code,b.province_name,a.supplier_code,a.supplier_name,sum( a.net_value) net_value,sum( a.value_tax_total) value_tax_total
from csx_ods.settle_settle_bill_ods a
left join (select shop_id,shop_name,province_name
from csx_dw.shop_m where sdt = 'current') b on b.shop_id=a.settle_place_code
where a.attribution_date >= '2020-03-01' 
and a.attribution_date < '2020-04-01'
and a.sdt='19990101'
group by case when a.cost_name like '目标返利%' then '目标返利' else a.cost_name end,
a.settle_place_code,b.province_name,a.supplier_code,a.supplier_name;
*/


		
-----0316拆分到渠道
--工厂月末分摊-调整销售订单 拆分到渠道
select  
case when a.location_code='W0H4' then '供应链' else b.province_name end province_name,
c.channel_name,
sum(a.adjustment_amt)/10000 adjustment_amt,
sum(a.adjustment_amt_no_tax)/10000 adjustment_amt_no_tax
from 
(select * from csx_ods.source_sync_r_d_data_relation_cas_sale_adjustment
where sdt = '19990101'
and posting_time >= '2020-03-01 00:00:00' 
and  posting_time < '2020-04-01 00:00:00'
and adjustment_reason in('fac_remark_sale','fac_remark') 
and item_wms_biz_type in('18','19','20','21','22','23','24','25','26','27','55','58','68','72','73',
						'A18','A20','A21','A22','A23','A24','A25','A55')
)a
left join b2b_tmp.tmp_sale_order_flag c on a.item_source_order_no=c.order_no and a.product_code=c.goods_code
left join 
(select if(shop_id like '9%',concat('E',substr(shop_id,2,3)),shop_id)shop_id,shop_name,province_name
from csx_dw.shop_m where sdt = 'current') b on b.shop_id=a.location_code
group by case when a.location_code='W0H4' then '供应链' else b.province_name end,
c.channel_name;

/*
-----数据明细 到课组 含税额与不含税额都要
--工厂月末分摊-调整销售订单
insert overwrite directory '/tmp/raoyanhua/linshi01' row format delimited fields terminated by '\t' 
select  
case when a.location_code='W0H4' then '供应链' else b.province_name end province_name,
c.channel_name,d.department_id,d.department_name,
sum(a.adjustment_amt)/10000 adjustment_amt,
sum(a.adjustment_amt_no_tax)/10000 adjustment_amt_no_tax
from 
(select * from csx_ods.source_sync_r_d_data_relation_cas_sale_adjustment
where sdt = '19990101'
and posting_time >= '2020-03-01 00:00:00' 
and posting_time < '2020-04-01 00:00:00'
and adjustment_reason in('fac_remark_sale','fac_remark') 
and item_wms_biz_type in('18','19','20','21','22','23','24','25','26','27','55','58','68','72','73',
						'A18','A20','A21','A22','A23','A24','A25','A55')
)a
left join b2b_tmp.tmp_sale_order_flag c on a.item_source_order_no=c.order_no and a.product_code=c.goods_code
left join 
(select if(shop_id like '9%',concat('E',substr(shop_id,2,3)),shop_id)shop_id,shop_name,province_name
from csx_dw.shop_m where sdt = 'current') b on b.shop_id=a.location_code
left join 
(select goods_id,department_id,department_name
	from csx_dw.goods_m where sdt = 'current' )d on a.product_code=d.goods_id
group by case when a.location_code='W0H4' then '供应链' else b.province_name end,
c.channel_name,d.department_id,d.department_name;


--工厂月末分摊-调整其他
insert overwrite directory '/tmp/raoyanhua/linshi02' row format delimited fields terminated by '\t' 
select a.province_name,b.department_id,b.department_name,
sum(adj_cost_gc_qt_tax)/10000 adj_cost_gc_qt_tax,
sum(adj_cost_gc_qt)/10000 adj_cost_gc_qt
from(
select b.province_code,
case when a.location_code='W0H4' then '供应链' else b.province_name end province_name,
a.product_code,a.product_name,a.item_source_order_no,
case when adjustment_reason='fac_remark_sale' 
		and item_wms_biz_type not in('18','19','20','21','22','23','24','25','26','27','55','58','68','72','73','06','07','08','09','15','17',
									'A18','A20','A21','A22','A23','A24','A25','A55')
		then adjustment_amt end adj_cost_gc_qt_tax,
case when adjustment_reason='fac_remark_sale' 
		and item_wms_biz_type not in('18','19','20','21','22','23','24','25','26','27','55','58','68','72','73','06','07','08','09','15','17',
									'A18','A20','A21','A22','A23','A24','A25','A55')
		then adjustment_amt_no_tax end adj_cost_gc_qt
from 
(select * from csx_ods.source_sync_r_d_data_relation_cas_sale_adjustment
where sdt = '19990101'
and posting_time >= '2020-03-01 00:00:00' 
and  posting_time < '2020-04-01 00:00:00') a
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
where adj_cost_gc_qt_tax is not null
group by a.province_code,a.province_name,b.department_id,b.department_name;


--6 价量差工厂未使用的商品 
insert overwrite directory '/tmp/raoyanhua/linshi03' row format delimited fields terminated by '\t' 
select case when a.location_code='W0H4' then '供应链' else b.province_name end province_name,
c.department_id,c.department_name,
sum(amount)/10000 amount
from csx_ods.source_mms_r_a_factory_report_no_share_product a 
left join 
(select if(shop_id like '9%',concat('E',substr(shop_id,2,3)),shop_id)shop_id,shop_name,province_name
from csx_dw.shop_m where sdt = 'current') b on b.shop_id=a.location_code
left join 
(select goods_id,department_id,department_name
	from csx_dw.goods_m where sdt = 'current' )c on a.product_code=c.goods_id
where sdt='20200408'
and period='2020-03'
group by case when a.location_code='W0H4' then '供应链' else b.province_name end,
c.department_id,c.department_name;


-- 9 10 盘盈 盘亏 amt_no_tax,amt
insert overwrite directory '/tmp/raoyanhua/linshi04' row format delimited fields terminated by '\t' 
select case when a.location_code='W0H4' then '供应链' else b.province_name end province_name,
c.department_id,c.department_name,
sum(case when amt_no_tax<0 then amt_no_tax end )/10000  inventory_p_no, --盘盈  
sum(case when amt_no_tax>=0 then amt_no_tax end )/10000  inventory_l_no, --盘亏
sum(case when amt<0 then amt end )/10000  inventory_p, --盘盈  
sum(case when amt>=0 then amt end )/10000  inventory_l --盘亏
from
(select a.*
from csx_ods.source_sync_r_d_data_sync_inventory_item a
where a.sdt = '19990101'
and a.reservoir_area_code = 'PD01' 
and ( a.purchase_group_code like 'H%' or a.purchase_group_code like 'U%' ) 
and a.posting_time >= '2020-03-01 00:00:00' 
and a.posting_time < '2020-04-01 00:00:00')a
left join 
(select if(shop_id like '9%',concat('E',substr(shop_id,2,3)),shop_id)shop_id,shop_name,province_name
from csx_dw.shop_m where sdt = 'current') b on b.shop_id=a.location_code
left join 
(select goods_id,department_id,department_name
	from csx_dw.goods_m where sdt = 'current' )c on a.product_code=c.goods_id
group by case when a.location_code='W0H4' then '供应链' else b.province_name end,
c.department_id,c.department_name;

*/




