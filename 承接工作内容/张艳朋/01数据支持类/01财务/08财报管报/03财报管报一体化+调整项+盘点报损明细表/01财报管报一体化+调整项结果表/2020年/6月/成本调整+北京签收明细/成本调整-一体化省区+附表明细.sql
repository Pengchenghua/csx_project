--销售订单所在省区渠道
drop table csx_tmp.tmp_sale_order_flag;
create table csx_tmp.tmp_sale_order_flag 
as 
select channel_name,province_code,province_name,origin_order_no,order_no,goods_code,
sum(sales_value)sales_value,
sum(excluding_tax_sales)excluding_tax_sales
from csx_dw.dws_sale_r_d_customer_sale
where sdt>='20200101'
and sales_type in ('sapqyg','sapgc','qyg','sc','bbc')
and (order_no not in ('OC200529000043','OC200529000044','OC200529000045','OC200529000046') or order_no is null)
group by channel_name,province_code,province_name,origin_order_no,order_no,goods_code;


left join csx_tmp.tmp_sale_order_flag c on a.item_source_order_no=c.order_no and a.product_code=c.goods_code



------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------
--供应链用公司代码2126判断  或地点编码W0H4
--第一部分 一体化表：各省区-调整
--1 2 3 4 5 6  adjustment_amt_no_tax,adjustment_amt
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
case when ((adjustment_reason in('fac_remark_sale','fac_remark') and item_wms_biz_type in('06','07','08','09','15','17','A06','A07','A08','A09','A15') )
				or (adjustment_reason = 'fac_remark_sale' AND item_wms_biz_type='12'))
		then adjustment_amt_no_tax end adj_cost_gc_db,
--工厂月末分摊-调整其他
case when adjustment_reason='fac_remark_sale' 
		and item_wms_biz_type not in('18','19','20','21','22','23','24','25','26','27','55','58','68','72','73','06','07','08','09','15','17',
									'A18','A20','A21','A22','A23','A24','A25','A55','12')
		then adjustment_amt_no_tax end adj_cost_gc_qt,
--手工调整销售成本
case when adjustment_reason='manual_remark' then if(adjustment_type='stock',-1*adjustment_amt_no_tax,adjustment_amt_no_tax) end adj_cost_sg
from (select * from csx_ods.source_sync_r_d_data_relation_cas_sale_adjustment
where sdt = '19990101'
and posting_time >= '2020-05-01 00:00:00' 
and  posting_time < '2020-06-01 00:00:00') a
left join 
(select if(shop_id like '9%',concat('E',substr(shop_id,2,3)),shop_id)shop_id,shop_name,province_name
from csx_dw.dws_basic_w_a_csx_shop_m where sdt = 'current') b on b.shop_id=a.location_code
)a
group by province_name;

--7 价量差工厂未使用的商品  不区分是否含税金额
select case when a.location_code='W0H4' then '供应链' else b.province_name end province_name,
sum(amount)/10000 amount
from csx_ods.source_mms_r_a_factory_report_no_share_product a 
left join 
(select if(shop_id like '9%',concat('E',substr(shop_id,2,3)),shop_id)shop_id,shop_name,province_name
from csx_dw.dws_basic_w_a_csx_shop_m where sdt = 'current') b on b.shop_id=a.location_code
where sdt='20200606' 
and period='2020-05'
group by case when a.location_code='W0H4' then '供应链' else b.province_name end;

--8 工厂分摊后成本小于0，未分摊金额 不区分是否含税
select case when a.location_code='W0H4' then '供应链' else b.province_name end province_name,
sum(d_cost_subtotal)/10000 d_cost_subtotal
from
(select *
from csx_ods.source_mms_r_a_factory_report_diff_apportion_header
where sdt='20200606'
and period = '2020-05' 
and notice_status = '3')a
left join 
(select if(shop_id like '9%',concat('E',substr(shop_id,2,3)),shop_id)shop_id,shop_name,province_name
from csx_dw.dws_basic_w_a_csx_shop_m where sdt = 'current') b on b.shop_id=a.location_code
group by case when a.location_code='W0H4' then '供应链' else b.province_name end;

--10 报损	不含税 amt_no_tax,---含税 amt
select case when a.location_code='W0H4' then '供应链' else b.province_name end province_name,
sum(amt_no_tax)/10000 amt_no_tax,
sum(amt)/10000 amt
from
(select a.*
from csx_ods.source_sync_r_d_data_sync_broken_item a
where a.sdt = '19990101'
and (( a.wms_biz_type <>'64' and a.reservoir_area_prop = 'C' and ( a.purchase_group_code like 'H%' or a.purchase_group_code like 'U%' ) ) 
or a.wms_biz_type = '64' )
and a.posting_time >= '2020-05-01 00:00:00' 
and a.posting_time < '2020-06-01 00:00:00')a
left join 
(select if(shop_id like '9%',concat('E',substr(shop_id,2,3)),shop_id)shop_id,shop_name,province_name
from csx_dw.dws_basic_w_a_csx_shop_m where sdt = 'current') b on b.shop_id=a.location_code
group by case when a.location_code='W0H4' then '供应链' else b.province_name end;


--11 12 盘盈 盘亏 不含税 amt_no_tax,---含税 amt
select case when a.location_code='W0H4' then '供应链' else b.province_name end province_name,
sum(case when amt_no_tax>=0 then -amt_no_tax end )/10000  inventory_p_no, --盘盈  
sum(case when amt_no_tax<0 then -amt_no_tax end )/10000  inventory_l_no, --盘亏

sum(case when amt>=0 then -amt end )/10000  inventory_p, --盘盈  
sum(case when amt<0 then -amt end )/10000  inventory_l --盘亏
from
(select a.*
from csx_ods.source_sync_r_d_data_sync_inventory_item a
where a.sdt = '19990101'
and a.reservoir_area_code = 'PD01' 
and ( a.purchase_group_code like 'H%' or a.purchase_group_code like 'U%' ) 
and a.posting_time >= '2020-05-01 00:00:00' 
and a.posting_time < '2020-06-01 00:00:00')a
left join 
(select if(shop_id like '9%',concat('E',substr(shop_id,2,3)),shop_id)shop_id,shop_name,province_name
from csx_dw.dws_basic_w_a_csx_shop_m where sdt = 'current') b on b.shop_id=a.location_code
group by case when a.location_code='W0H4' then '供应链' else b.province_name end;


--13  采购后台收入	net_value，value_tax_total
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
and attribution_date >= '2020-05-01' 
and attribution_date < '2020-06-01' )a
left join (select shop_id,shop_name,province_name
from csx_dw.dws_basic_w_a_csx_shop_m where sdt = 'current') b on b.shop_id=a.settle_place_code
group by case when a.cost_name like '目标返利%' then '目标返利'
			when a.cost_name like '仓储服务费%' then '仓储服务费'  
			else a.cost_name end,
case when a.settle_place_code='W0H4' then '供应链' else b.province_name end;

--14 销售后台支出-调价、返利  Z68是返利Z69是调价 --hive中返利表
select a.adjust_reason,
case when a.inventory_dc_code='W0H4' then '供应链' else b.province_name end province_name,
sum(total_price/(1+tax_rate/100))/10000 amt_no_tax,
sum(total_price)/10000 amt
from
(select *
from csx_dw.dwd_csms_r_d_yszx_customer_rebate_detail_new 
where type in ('0','1')
and commit_time>='2020-05-01 00:00:00'
and commit_time<'2020-06-01 00:00:00')a
left join 
(select if(shop_id like '9%',concat('E',substr(shop_id,2,3)),shop_id)shop_id,shop_name,province_name
from csx_dw.dws_basic_w_a_csx_shop_m where sdt = 'current') b on b.shop_id=a.inventory_dc_code
group by a.adjust_reason,
	case when a.inventory_dc_code='W0H4' then '供应链' else b.province_name end
order by a.adjust_reason;

		
-----调整销售--省区、渠道
--工厂月末分摊-调整销售订单 拆分到渠道
select  
case when a.location_code='W0H4' then '供应链' else b.province_name end province_name,
c.channel_name,
sum(a.adjustment_amt)/10000 adjustment_amt,
sum(a.adjustment_amt_no_tax)/10000 adjustment_amt_no_tax
from 
(select * from csx_ods.source_sync_r_d_data_relation_cas_sale_adjustment
where sdt = '19990101'
and posting_time >= '2020-05-01 00:00:00' 
and  posting_time < '2020-06-01 00:00:00'
and adjustment_reason in('fac_remark_sale','fac_remark') 
and item_wms_biz_type in('18','19','20','21','22','23','24','25','26','27','55','58','68','72','73',
						'A18','A20','A21','A22','A23','A24','A25','A55')
)a
left join csx_tmp.tmp_sale_order_flag c on a.item_source_order_no=c.order_no and a.product_code=c.goods_code
left join 
(select if(shop_id like '9%',concat('E',substr(shop_id,2,3)),shop_id)shop_id,shop_name,province_name
from csx_dw.dws_basic_w_a_csx_shop_m where sdt = 'current') b on b.shop_id=a.location_code
group by case when a.location_code='W0H4' then '供应链' else b.province_name end,
c.channel_name;

------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------
--第2部分 成本调整金额课组明细  调整项对应的各明细附表 到课组 或到课组商品
--其中前两项对应附表6-附表7有王威出的4张表
--1、工厂月末分摊-调整销售订单--课组
insert overwrite directory '/tmp/raoyanhua/linshi01' row format delimited fields terminated by '\t' 
select  
case when a.location_code='W0H4' then '供应链' else b.province_name end province_name,a.location_code,a.location_name,
c.channel_name,d.department_id,d.department_name,
sum(a.adjustment_amt) adjustment_amt,
sum(a.adjustment_amt_no_tax) adjustment_amt_no_tax
from 
(select * from csx_ods.source_sync_r_d_data_relation_cas_sale_adjustment
where sdt = '19990101'
and posting_time >= '2020-05-01 00:00:00' 
and posting_time < '2020-06-01 00:00:00'
and adjustment_reason in('fac_remark_sale','fac_remark') 
and item_wms_biz_type in('18','19','20','21','22','23','24','25','26','27','55','58','68','72','73',
						'A18','A20','A21','A22','A23','A24','A25','A55')
)a
left join csx_tmp.tmp_sale_order_flag c on a.item_source_order_no=c.order_no and a.product_code=c.goods_code
left join 
(select if(shop_id like '9%',concat('E',substr(shop_id,2,3)),shop_id)shop_id,shop_name,province_name
from csx_dw.dws_basic_w_a_csx_shop_m where sdt = 'current') b on b.shop_id=a.location_code
left join 
(select goods_id,department_id,department_name
	from csx_dw.dws_basic_w_a_csx_product_m where sdt = 'current' )d on a.product_code=d.goods_id
group by case when a.location_code='W0H4' then '供应链' else b.province_name end,a.location_code,a.location_name,
c.channel_name,d.department_id,d.department_name;


--2、工厂月末分摊-调整其他--课组
insert overwrite directory '/tmp/raoyanhua/linshi02' row format delimited fields terminated by '\t' 
select a.province_name,a.location_code,a.location_name,b.department_id,b.department_name,
sum(adj_cost_gc_qt_tax) adj_cost_gc_qt_tax,
sum(adj_cost_gc_qt) adj_cost_gc_qt
from(
select b.province_code,
case when a.location_code='W0H4' then '供应链' else b.province_name end province_name,location_code,location_name,
a.product_code,a.product_name,a.item_source_order_no,
case when adjustment_reason='fac_remark_sale' 
		and item_wms_biz_type not in('18','19','20','21','22','23','24','25','26','27','55','58','68','72','73','06','07','08','09','15','17',
									'A18','A20','A21','A22','A23','A24','A25','A55','12')
		then adjustment_amt end adj_cost_gc_qt_tax,
case when adjustment_reason='fac_remark_sale' 
		and item_wms_biz_type not in('18','19','20','21','22','23','24','25','26','27','55','58','68','72','73','06','07','08','09','15','17',
									'A18','A20','A21','A22','A23','A24','A25','A55','12')
		then adjustment_amt_no_tax end adj_cost_gc_qt
from 
(select * from csx_ods.source_sync_r_d_data_relation_cas_sale_adjustment
where sdt = '19990101'
and posting_time >= '2020-05-01 00:00:00' 
and  posting_time < '2020-06-01 00:00:00') a
left join 
(select if(shop_id like '9%',concat('E',substr(shop_id,2,3)),shop_id)shop_id,shop_name,province_code,province_name
from csx_dw.dws_basic_w_a_csx_shop_m where sdt = 'current') b on b.shop_id=a.location_code
)a
left join 
(select goods_id,department_id,department_name
	from csx_dw.dws_basic_w_a_csx_product_m where sdt = 'current' )b on a.product_code=b.goods_id
left join
(select
    workshop_code, province_code, goods_code
  from csx_dw.dws_mms_w_a_factory_setting_craft_once_all
  where sdt='current' and new_or_old=1
)d on a.province_code=d.province_code and a.product_code=d.goods_code
where adj_cost_gc_qt_tax is not null
group by a.province_code,a.province_name,a.location_code,a.location_name,b.department_id,b.department_name;


--3、价量差工厂未使用的商品 到课组
insert overwrite directory '/tmp/raoyanhua/linshi03' row format delimited fields terminated by '\t'
select a.province_name,a.location_code,b.department_id,b.department_name,
 sum(amount)amount
from
(select a.*,b.province_code,b.province_name,b.shop_id,b.shop_name
from (select * from csx_ods.source_mms_r_a_factory_report_no_share_product
where sdt='20200606'
and period in('2020-05'))a 
left join 
(select if(shop_id like '9%',concat('E',substr(shop_id,2,3)),shop_id)shop_id,shop_name,province_code,province_name
from csx_dw.dws_basic_w_a_csx_shop_m where sdt = 'current') b on b.shop_id=a.location_code
) a
left join 
(select regexp_replace(regexp_replace(goods_name,'\n',''),'\r','') goods_name,
goods_id,department_id,department_name
	from csx_dw.dws_basic_w_a_csx_product_m where sdt = 'current' )b on a.product_code=b.goods_id
group by a.province_name,a.location_code,b.department_id,b.department_name;

--4、附表9 盘盈 盘亏 amt_no_tax,amt 到课组
insert overwrite directory '/tmp/raoyanhua/linshi04' row format delimited fields terminated by '\t' 
select a.province_name,a.location_code,a.location_name,b.department_id,b.department_name,
sum(case when amt_no_tax>=0 then -amt_no_tax end )  inventory_p_no, --盘盈  
sum(case when amt_no_tax<0 then -amt_no_tax end )  inventory_l_no, --盘亏

sum(case when amt>=0 then -amt end )  inventory_p, --盘盈  
sum(case when amt<0 then -amt end ) inventory_l --盘亏
from
(select a.*,b.province_code,
case when a.location_code='W0H4' then '供应链' else b.province_name end province_name,
b.shop_id,b.shop_name
from (select a.*
from csx_ods.source_sync_r_d_data_sync_inventory_item a
where a.sdt = '19990101'
and a.reservoir_area_code = 'PD01' 
and ( a.purchase_group_code like 'H%' or a.purchase_group_code like 'U%' ) 
and a.posting_time >= '2020-05-01 00:00:00' 
and a.posting_time < '2020-06-01 00:00:00') a 
left join 
(select if(shop_id like '9%',concat('E',substr(shop_id,2,3)),shop_id)shop_id,shop_name,province_code,province_name
from csx_dw.dws_basic_w_a_csx_shop_m where sdt = 'current') b on b.shop_id=a.location_code
) a
left join 
(select goods_id,goods_name,department_id,department_name
	from csx_dw.dws_basic_w_a_csx_product_m where sdt = 'current' )b on a.product_code=b.goods_id
group by a.province_name,a.location_code,a.location_name,b.department_id,b.department_name;


--第2部分增加 20200512 剩余调整项到课组明细表

--1、对抵负库存成本调整--课组
insert overwrite directory '/tmp/raoyanhua/linshi05' row format delimited fields terminated by '\t' 
select  
case when a.location_code='W0H4' then '供应链' else b.province_name end province_name,a.location_code,a.location_name,
d.department_id,d.department_name,
sum(a.adjustment_amt) adjustment_amt,
sum(a.adjustment_amt_no_tax) adjustment_amt_no_tax
from 
(select * from csx_ods.source_sync_r_d_data_relation_cas_sale_adjustment
where sdt = '19990101'
and posting_time >= '2020-05-01 00:00:00' 
and posting_time < '2020-06-01 00:00:00'
and adjustment_reason='in_remark'
)a
left join 
(select if(shop_id like '9%',concat('E',substr(shop_id,2,3)),shop_id)shop_id,shop_name,province_name
from csx_dw.dws_basic_w_a_csx_shop_m where sdt = 'current') b on b.shop_id=a.location_code
left join 
(select goods_id,department_id,department_name
	from csx_dw.dws_basic_w_a_csx_product_m where sdt = 'current' )d on a.product_code=d.goods_id
group by case when a.location_code='W0H4' then '供应链' else b.province_name end,a.location_code,a.location_name,
d.department_id,d.department_name;

--2、采购退货金额差异调整--课组
insert overwrite directory '/tmp/raoyanhua/linshi06' row format delimited fields terminated by '\t' 
select  
case when a.location_code='W0H4' then '供应链' else b.province_name end province_name,a.location_code,a.location_name,
d.department_id,d.department_name,
sum(a.adjustment_amt) adjustment_amt,
sum(a.adjustment_amt_no_tax) adjustment_amt_no_tax
from 
(select * from csx_ods.source_sync_r_d_data_relation_cas_sale_adjustment
where sdt = '19990101'
and posting_time >= '2020-05-01 00:00:00' 
and posting_time < '2020-06-01 00:00:00'
and adjustment_reason='out_remark'
)a
left join 
(select if(shop_id like '9%',concat('E',substr(shop_id,2,3)),shop_id)shop_id,shop_name,province_name
from csx_dw.dws_basic_w_a_csx_shop_m where sdt = 'current') b on b.shop_id=a.location_code
left join 
(select goods_id,department_id,department_name
	from csx_dw.dws_basic_w_a_csx_product_m where sdt = 'current' )d on a.product_code=d.goods_id
group by case when a.location_code='W0H4' then '供应链' else b.province_name end,a.location_code,a.location_name,
d.department_id,d.department_name;


--3、工厂分摊后成本小于0，未分摊金额--课组
insert overwrite directory '/tmp/raoyanhua/linshi07' row format delimited fields terminated by '\t' 
select  
case when a.location_code='W0H4' then '供应链' else b.province_name end province_name,a.location_code,--a.location_name,
d.department_id,d.department_name,
sum(a.d_cost_subtotal) d_cost_subtotal
from 
(select * from csx_ods.source_mms_r_a_factory_report_diff_apportion_header
where sdt='20200606'
and period = '2020-05' 
and notice_status = '3'
)a
left join 
(select if(shop_id like '9%',concat('E',substr(shop_id,2,3)),shop_id)shop_id,shop_name,province_name
from csx_dw.dws_basic_w_a_csx_shop_m where sdt = 'current') b on b.shop_id=a.location_code
left join 
(select goods_id,department_id,department_name
	from csx_dw.dws_basic_w_a_csx_product_m where sdt = 'current' )d on a.product_code=d.goods_id
group by case when a.location_code='W0H4' then '供应链' else b.province_name end,a.location_code,--a.location_name,
d.department_id,d.department_name;

--4 报损--课组	不含税 amt_no_tax,---含税 amt
insert overwrite directory '/tmp/raoyanhua/linshi08' row format delimited fields terminated by '\t' 
select a.province_name,a.location_code,a.location_name,b.department_id,b.department_name,
sum(amt_no_tax) amt_no_tax,
sum(amt) amt
from
(select a.*,b.province_code,
case when a.location_code='W0H4' then '供应链' else b.province_name end province_name,
b.shop_id,b.shop_name
from (select a.*
from csx_ods.source_sync_r_d_data_sync_broken_item a
where a.sdt = '19990101'
and (( a.wms_biz_type <>'64' and a.reservoir_area_prop = 'C' and ( a.purchase_group_code like 'H%' or a.purchase_group_code like 'U%' ) ) 
or a.wms_biz_type = '64' )
and a.posting_time >= '2020-05-01 00:00:00' 
and a.posting_time < '2020-06-01 00:00:00') a 
left join 
(select if(shop_id like '9%',concat('E',substr(shop_id,2,3)),shop_id)shop_id,shop_name,province_code,province_name
from csx_dw.dws_basic_w_a_csx_shop_m where sdt = 'current') b on b.shop_id=a.location_code
) a
left join 
(select goods_id,goods_name,department_id,department_name
	from csx_dw.dws_basic_w_a_csx_product_m where sdt = 'current' )b on a.product_code=b.goods_id
group by a.province_name,a.location_code,a.location_name,b.department_id,b.department_name;


------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------
--第3部分 附表8-10、销售后台支出明细表  调整项对应的各明细附表 到课组 或到课组商品

--1、附表8 价量差工厂未使用的商品 到商品课组
insert overwrite directory '/tmp/raoyanhua/linshi01' row format delimited fields terminated by '\t'
select a.province_code,a.province_name,a.location_code,a.cost_center_code,a.product_code,b.goods_name,b.department_id,b.department_name,
if(d.workshop_code is null,'不是工厂商品','是工厂商品') as is_factory_goods_name,
 sum(amount)amount
from
(select a.*,b.province_code,b.province_name,b.shop_id,b.shop_name
from (select * from csx_ods.source_mms_r_a_factory_report_no_share_product
where sdt='20200606'
and period in('2020-05'))a 
left join 
(select if(shop_id like '9%',concat('E',substr(shop_id,2,3)),shop_id)shop_id,shop_name,province_code,province_name
from csx_dw.dws_basic_w_a_csx_shop_m where sdt = 'current') b on b.shop_id=a.location_code
) a
left join 
(select regexp_replace(regexp_replace(goods_name,'\n',''),'\r','') goods_name,
goods_id,department_id,department_name
	from csx_dw.dws_basic_w_a_csx_product_m where sdt = 'current' )b on a.product_code=b.goods_id
left join
(select
    workshop_code, province_code, goods_code
  from csx_dw.dws_mms_w_a_factory_setting_craft_once_all
  where sdt='current' and new_or_old=1
)d on a.province_code=d.province_code and a.product_code=d.goods_code
group by a.province_code,a.province_name,a.location_code,a.cost_center_code,a.product_code,b.goods_name,b.department_id,b.department_name,
if(d.workshop_code is null,'不是工厂商品','是工厂商品');

--2、附表9 盘盈 盘亏 amt_no_tax,amt 到商品课组
insert overwrite directory '/tmp/raoyanhua/linshi02' row format delimited fields terminated by '\t' 
select a.province_code,a.province_name,
--c.channel_name,a.cost_center_code,a.cost_center_name,
a.location_code,a.location_name,a.company_code,a.company_name,a.product_code,
regexp_replace(regexp_replace(a.product_name,'\n',''),'\r','') product_name,
b.department_id,b.department_name,
if(d.workshop_code is null,'不是工厂商品','是工厂商品') as is_factory_goods_name,
sum(case when amt_no_tax>=0 then -amt_no_tax end )  inventory_p_no, --盘盈  
sum(case when amt_no_tax<0 then -amt_no_tax end )  inventory_l_no, --盘亏

sum(case when amt>=0 then -amt end )  inventory_p, --盘盈  
sum(case when amt<0 then -amt end ) inventory_l --盘亏
from
(select a.*,b.province_code,
case when a.location_code='W0H4' then '供应链' else b.province_name end province_name,
b.shop_id,b.shop_name
from (select a.*
from csx_ods.source_sync_r_d_data_sync_inventory_item a
where a.sdt = '19990101'
and a.reservoir_area_code = 'PD01' 
and ( a.purchase_group_code like 'H%' or a.purchase_group_code like 'U%' ) 
and a.posting_time >= '2020-05-01 00:00:00' 
and a.posting_time < '2020-06-01 00:00:00') a 
left join 
(select if(shop_id like '9%',concat('E',substr(shop_id,2,3)),shop_id)shop_id,shop_name,province_code,province_name
from csx_dw.dws_basic_w_a_csx_shop_m where sdt = 'current') b on b.shop_id=a.location_code
) a
left join 
(select goods_id,goods_name,department_id,department_name
	from csx_dw.dws_basic_w_a_csx_product_m where sdt = 'current' )b on a.product_code=b.goods_id
left join
(select
    workshop_code, province_code, goods_code
  from csx_dw.dws_mms_w_a_factory_setting_craft_once_all
  where sdt='current' and new_or_old=1
)d on a.province_code=d.province_code and a.product_code=d.goods_code
left join csx_tmp.tmp_sale_order_flag c on a.wms_order_no=c.order_no and a.product_code=c.goods_code
--where a.province_code='110000'
group by a.province_code,a.province_name,c.channel_name,a.location_code,a.location_name,
a.company_code,a.company_name,a.cost_center_code,a.cost_center_name,a.product_code,
regexp_replace(regexp_replace(a.product_name,'\n',''),'\r',''),
b.department_id,b.department_name,
if(d.workshop_code is null,'不是工厂商品','是工厂商品');


--3、附表10 后台收入拆分到供应商
insert overwrite directory '/tmp/raoyanhua/linshi03' row format delimited fields terminated by '\t' 
select case when a.cost_name like '目标返利%' then '目标返利'
			when a.cost_name like '仓储服务费%' then '仓储服务费'  
			else a.cost_name end cost_name ,
a.settle_place_code,b.province_name,a.supplier_code,a.supplier_name,sum( a.net_value) net_value,sum( a.value_tax_total) value_tax_total
from 
( select * from csx_ods.settle_settle_bill_ods 
where sdt='19990101'
and attribution_date >= '2020-05-01' 
and attribution_date < '2020-06-01' )a
left join (select shop_id,shop_name,province_name
from csx_dw.dws_basic_w_a_csx_shop_m where sdt = 'current') b on b.shop_id=a.settle_place_code
group by case when a.cost_name like '目标返利%' then '目标返利'
			when a.cost_name like '仓储服务费%' then '仓储服务费'  
			else a.cost_name end,
a.settle_place_code,b.province_name,a.supplier_code,a.supplier_name;


--4、销售后台支出-调价、返利  Z68是返利Z69是调价
insert overwrite directory '/tmp/raoyanhua/linshi04' row format delimited fields terminated by '\t' 
select a.adjust_reason,a.inventory_dc_code,a.inventory_dc_name,
case when a.inventory_dc_code='W0H4' then '供应链' else a.province_name end province_name,
c.channel_name,a.product_code,a.product_name,e.department_id,e.department_name,
if(d.workshop_code is null,'不是工厂商品','是工厂商品') as is_factory_goods_name,
sum(total_price/(1+tax_rate/100)) amt_no_tax,
sum(total_price) amt
from
(select a.*,b.province_code,b.province_name
from csx_dw.dwd_csms_r_d_yszx_customer_rebate_detail_new a
left join --省区
(select 
if(shop_id like '9%',concat('E',substr(shop_id,2,3)),shop_id)shop_id,shop_name,province_code,province_name
from csx_dw.dws_basic_w_a_csx_shop_m where sdt = 'current') b on b.shop_id=a.inventory_dc_code
where a.type in ('0','1')
and a.commit_time>='2020-05-01 00:00:00'
and a.commit_time<'2020-06-01 00:00:00')a
left join csx_tmp.tmp_sale_order_flag c on a.original_order_no=c.origin_order_no and a.product_code=c.goods_code --渠道
left join --是否工厂商品
(select
    workshop_code, province_code, goods_code
  from csx_dw.dws_mms_w_a_factory_setting_craft_once_all
  where sdt='current' and new_or_old=1
)d on a.province_code=d.province_code and a.product_code=d.goods_code
left join --课组
(select goods_id,goods_name,department_id,department_name
	from csx_dw.dws_basic_w_a_csx_product_m where sdt = 'current' )e on a.product_code=e.goods_id
group by a.adjust_reason,a.inventory_dc_code,a.inventory_dc_name,
case when a.inventory_dc_code='W0H4' then '供应链' else a.province_name end,
c.channel_name,a.product_code,a.product_name,e.department_id,e.department_name,
if(d.workshop_code is null,'不是工厂商品','是工厂商品') ;




----------------------------------------------------------------------------------------------------------------------------------------
--1、对抵负库存成本调整--商品明细
insert overwrite directory '/tmp/raoyanhua/linshi05' row format delimited fields terminated by '\t' 
select  
case when a.location_code='W0H4' then '供应链' else b.province_name end province_name,
d.department_id,d.department_name,
a.adjustment_no        ,
a.adjustment_reason    ,
a.adjustment_type      ,
a.credential_no        ,
a.item_credential_no   ,
a.item_source_order_no ,
a.product_code         ,
a.product_name         ,
a.posting_time         ,
a.location_code,
a.location_name,
a.qty                  ,
a.purchase_group_code  ,
a.adjustment_amt       ,
a.adjustment_amt_no_tax,
a.adjustment_order_no  ,
a.item_wms_biz_type    ,
a.reservoir_area_code  
from 
(select * from csx_ods.source_sync_r_d_data_relation_cas_sale_adjustment
where sdt = '19990101'
and posting_time >= '2020-05-01 00:00:00' 
and posting_time < '2020-06-01 00:00:00'
and adjustment_reason='in_remark'
)a
left join 
(select if(shop_id like '9%',concat('E',substr(shop_id,2,3)),shop_id)shop_id,shop_name,province_name
from csx_dw.dws_basic_w_a_csx_shop_m where sdt = 'current') b on b.shop_id=a.location_code
left join 
(select goods_id,department_id,department_name
	from csx_dw.dws_basic_w_a_csx_product_m where sdt = 'current' )d on a.product_code=d.goods_id;


--4 报损--商品明细	不含税 amt_no_tax,---含税 amt
insert overwrite directory '/tmp/raoyanhua/linshi08' row format delimited fields terminated by '\t' 
select a.province_name,a.location_code,a.location_name,b.department_id,b.department_name,
a.posting_time               ,
a.wms_order_no               ,
a.wms_biz_type               ,
a.wms_biz_type_name          ,
a.credential_no              ,
--a.purchase_group_code        ,
--a.purchase_group_name        ,
a.product_code               ,
a.product_name               ,
a.unit                       ,
a.qty                        ,
a.price_no_tax               ,
a.amt_no_tax                 ,
a.amt                        ,
a.fac_adjust_amt_no_tax      ,
a.negative_adjust_amt_no_tax ,
a.remedy_adjust_amt_no_tax   ,
a.manual_adjust_amt_no_tax   ,
a.cost_amt_no_tax            ,
a.company_code               ,
a.company_name               ,
a.cost_center_code           ,
a.cost_center_name           ,
a.small_category_code        ,
a.small_category_name        ,
a.reservoir_area_code        ,
a.reservoir_area_name        ,
a.reservoir_area_prop        
from
(select a.*,b.province_code,
case when a.location_code='W0H4' then '供应链' else b.province_name end province_name,
b.shop_id,b.shop_name
from (select a.*
from csx_ods.source_sync_r_d_data_sync_broken_item a
where a.sdt = '19990101'
and (( a.wms_biz_type <>'64' and a.reservoir_area_prop = 'C' and ( a.purchase_group_code like 'H%' or a.purchase_group_code like 'U%' ) ) 
or a.wms_biz_type = '64' )
and a.posting_time >= '2020-05-01 00:00:00' 
and a.posting_time < '2020-06-01 00:00:00') a 
left join 
(select if(shop_id like '9%',concat('E',substr(shop_id,2,3)),shop_id)shop_id,shop_name,province_code,province_name
from csx_dw.dws_basic_w_a_csx_shop_m where sdt = 'current') b on b.shop_id=a.location_code
) a
left join 
(select goods_id,goods_name,department_id,department_name
	from csx_dw.dws_basic_w_a_csx_product_m where sdt = 'current' )b on a.product_code=b.goods_id;



