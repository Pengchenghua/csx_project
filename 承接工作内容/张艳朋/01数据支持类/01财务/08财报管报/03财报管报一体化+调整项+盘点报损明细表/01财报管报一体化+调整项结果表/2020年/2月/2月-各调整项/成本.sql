
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

/*
-----0316数据明细 到课组 含税额与不含税额都要
--工厂月末分摊-调整销售订单
insert overwrite directory '/tmp/raoyanhua/linshi01' row format delimited fields terminated by '\t' 
select  
b.province_name,c.channel_name,d.department_id,d.department_name,
sum(a.adjustment_amt)/10000adjustment_amt,
sum(a.adjustment_amt_no_tax)/10000 adjustment_amt_no_tax
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
left join 
(select goods_id,department_id,department_name
	from csx_dw.goods_m where sdt = 'current' )d on a.product_code=d.goods_id
group by b.province_name,c.channel_name,d.department_id,d.department_name;


--工厂月末分摊-调整其他
insert overwrite directory '/tmp/raoyanhua/linshi01' row format delimited fields terminated by '\t' 
select a.province_code,a.province_name,b.department_id,b.department_name,
sum(adj_cost_gc_qt_tax)/10000 adj_cost_gc_qt_tax,
sum(adj_cost_gc_qt)/10000 adj_cost_gc_qt
from(
select b.province_code,b.province_name,a.product_code,a.product_name,a.item_source_order_no,
--工厂月末分摊-调整其他
case when adjustment_reason='fac_remark_sale' and item_wms_biz_type not in('18','19','20','21','22','23','24','25','26','27','55','58','68','72','73','06','07','08','09','15','17')
		then adjustment_amt end adj_cost_gc_qt_tax,
case when adjustment_reason='fac_remark_sale' and item_wms_biz_type not in('18','19','20','21','22','23','24','25','26','27','55','58','68','72','73','06','07','08','09','15','17')
		then adjustment_amt_no_tax end adj_cost_gc_qt
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
group by a.province_code,a.province_name,b.department_id,b.department_name;


--6 价量差工厂未使用的商品 
select b.province_name,c.department_id,c.department_name,
sum(amount)/10000 amount
from csx_ods.source_mms_r_a_factory_report_no_share_product a 
left join 
(select if(shop_id like '9%',concat('E',substr(shop_id,2,3)),shop_id)shop_id,shop_name,province_name
from csx_dw.shop_m where sdt = 'current') b on b.shop_id=a.location_code
left join 
(select goods_id,department_id,department_name
	from csx_dw.goods_m where sdt = 'current' )c on a.product_code=c.goods_id
where sdt='20200315'
and period='2020-02'
group by b.province_name,c.department_id,c.department_name;


-- 9 10 盘盈 盘亏 amt_no_tax,amt
select b.province_name,c.department_id,c.department_name,
sum(case when move_type in ( '115A', '116B' ) then -amt end )/10000  inventory_p, --盘盈  取移动类型为115A或116B，且库区是PD01的金额
sum(case when move_type in ( '115A', '116B' ) then -amt_no_tax end )/10000  inventory_p_notax, --盘盈  
sum(case when move_type in ( '115B', '116A' ) then amt end )/10000  inventory_l, --盘亏
sum(case when move_type in ( '115B', '116A' ) then amt_no_tax end )/10000  inventory_l_notax --盘亏
from csx_dw.dwd_cas_r_d_accounting_credential_detail a 
left join 
(select if(shop_id like '9%',concat('E',substr(shop_id,2,3)),shop_id)shop_id,shop_name,province_name
from csx_dw.shop_m where sdt = 'current') b on b.shop_id=a.location_code
left join 
(select goods_id,department_id,department_name
	from csx_dw.goods_m where sdt = 'current' )c on a.product_code=c.goods_id
where sdt >= '20200201'
and sdt < '20200301'
and reservoir_area_code = 'PD01'
group by b.province_name,c.department_id,c.department_name;

*/









--销售端明细数据
insert overwrite directory '/tmp/raoyanhua/linshi01' row format delimited fields terminated by '\t' 
select a.AA,a.sdt,a.customer_no,a.customer_name,a.channel_name,a.province_code,a.province_name,a.shop_id,
	a.attribute,a.first_category,a.second_category,a.sales_name,a.work_no,
	a.department_code,a.department_name,
	sum(a.sales_value) sales_value,sum(a.sales_cost) sales_cost,sum(a.profit) profit	
from 
(select 
a.AA,a.sdt,a.customer_no,b.customer_name,a.channel_name,a.province_code,a.province_name,a.shop_id,
	a.attribute,a.first_category,a.second_category,a.sales_name,a.work_no,
	a.order_no,a.credential_no,a.goods_code,a.goods_name,a.department_code,a.department_name,
    a.sales_value,a.sales_cost,a.profit	
from b2b_tmp.cust_sales_m2 a 
left join 
( select
a.customer_no,
a.customer_name,
a.attribute,
a.channel,
a.sales_id,
a.sales_name,
a.work_no,
a.first_supervisor_name,
a.second_supervisor_name,
a.third_supervisor_name,
a.fourth_supervisor_name,
a.sales_province,
a.sales_city,
a.first_category,
a.second_category,
a.third_category,
regexp_replace(split(a.sign_time, ' ')[0], '-', '') as sign_date
from csx_dw.customer_m a
where a.sdt=regexp_replace(date_sub(current_date,1),'-','')
and length(a.customer_no) > 0 )b on b.customer_no=a.customer_no) a
group by a.AA,a.sdt,a.customer_no,a.customer_name,a.channel_name,a.province_code,a.province_name,a.shop_id,
	a.attribute,a.first_category,a.second_category,a.sales_name,a.work_no,
	a.department_code,a.department_name;
	
	
	
	



--数据源，处理省区、渠道信息
drop table b2b_tmp.cust_sales_mmm;
create table b2b_tmp.cust_sales_mmm
as
select a.AA,a.sdt,coalesce(b.channel,a.channel) channel,a.dc_code,a.origin_shop_id,a.division_code,a.customer_no,a.sales_value,a.sales_cost,a.profit,b.sales_province,
b.attribute,b.first_category,b.second_category,b.sales_name,b.work_no,
a.credential_no,a.goods_code,a.goods_name,a.order_no,a.department_code,a.department_name
from
(select '成本'AA,credential_no,product_code goods_code,product_name goods_name,source_order_no order_no,purchase_group_code department_code,purchase_group_name department_name,
regexp_replace(split(create_time, ' ')[0], '-', '') as sdt,
case when source_sys='1' then '大客户'
	when source_sys='2' then '企业购'
	when source_sys='3' then '商超'
	else '其他' end channel,
location_code dc_code,  
'' origin_shop_id,
root_category	division_code, 
customer_no,
sale_amt sales_value,
cost_amt sales_cost,
profit_amt profit
from csx_dw.dwd_sync_r_d_data_relation_cas_sale_detail
where create_time>='2020-01-01'
and create_time<'2020-03-11'
 )a
left join 
( select
a.customer_no,
a.customer_name,
a.attribute,
a.channel,
a.sales_id,
a.sales_name,
a.work_no,
a.first_supervisor_name,
a.second_supervisor_name,
a.third_supervisor_name,
a.fourth_supervisor_name,
a.sales_province,
a.sales_city,
a.first_category,
a.second_category,
a.third_category,
regexp_replace(split(a.sign_time, ' ')[0], '-', '') as sign_date
from csx_dw.customer_m a
where a.sdt=regexp_replace(date_sub(current_date,2),'-','')
and length(a.customer_no) > 0 )b on b.customer_no=a.customer_no;


drop table b2b_tmp.cust_sales_mmm1;
create table b2b_tmp.cust_sales_mmm1
as
select
AA,sdt,shop_id,customer_no,sales_value,sales_cost,profit,channel,
attribute,first_category,second_category,sales_name,work_no,
credential_no,goods_code,goods_name,order_no,department_code,department_name,
  case
    when channel = '1'  OR channel = '' then '大客户'
    when channel = '2' then '商超'
    when channel = '3' then '商超(对外)'
    when channel = '4' then '大宗'
    when channel = '5' then '供应链(食百)'
    when channel = '6' then '供应链(生鲜)'
    when channel = '7' then '企业购 '
    when channel = '8' then '其他'
  end as channel_name,


  case
    when channel = '5' then '平台-食百采购'
    when channel = '6' then '平台-生鲜采购'
    when channel = '4' then '平台-大宗'
    else a.province_name
  end as province_name

from
(
select
a.AA,a.sdt,a.shop_id,a.customer_no,a.sales_value,a.sales_cost,a.profit, 
a.attribute,a.first_category,a.second_category,a.sales_name,a.work_no,
a.credential_no,a.goods_code,a.goods_name,a.order_no,a.department_code,a.department_name,
  case 
    when  a.shop_id like 'E%' then '2'
    --when a.origin_shop_id = 'W0B6' or a.channel like '%企业购%' or a.sales_type='bbc' then '7'
    when a.origin_shop_id = 'W0B6' or a.channel like '%企业购%' then '7'	
    when (a.shop_id = 'W0H4' and a.customer_no like 'S%' and a.category_code in ('12','13','14') ) 
      or (a.channel like '供应链%' and a.category_code in ('12','13','14'))then '5'
    when (a.shop_id = 'W0H4' and a.customer_no like 'S%' and a.category_code in ('10','11'))
      or (a.channel like '供应链%' and a.category_code in ('10', '11'))then '6'  
    when a.channel = '大客户' or a.channel = 'B端' then '1'
    when a.channel ='M端'  or a.channel like '%对内%' or a.channel='商超' then '2'
    when a.channel like '%对外%' then '3'
    when a.channel = '大宗' then '4'  
    when a.channel='其他' then '8'
    else ''
    end as channel,

   case
     when a.shop_id in ('W0M1','W0M4','W0J6','W0M6') then '商超平台' 
     when a.customer_no is not null and a.sales_province='BBC' then '福建省'
     when a.sales_province is not null and a.channel <> 'M端' and a.channel not like '商超%'  then a.sales_province
     when a.customer_no like 'S%' and substr(c.province_name, 1, 2) 
       in ('重庆','四川','北京','福建','上海','浙江','江苏','安徽','广东','贵州') 
       then c.province_name
     else d.province_name end as province_name
from 
(select AA,sdt,channel,dc_code shop_id,origin_shop_id,division_code as category_code,customer_no,sales_value,sales_cost,profit,sales_province,
attribute,first_category,second_category,sales_name,work_no,
credential_no,goods_code,goods_name,order_no,department_code,department_name,
case when dc_code like 'E%' then concat('9',substr(dc_code,2,3)) else dc_code end shop_no 
from b2b_tmp.cust_sales_mmm )a
left outer join 
(
  select
    shop_id,
    case
      when shop_id in ('W055') then '上海市'  else province_name
      end province_name,
    case
      when province_name like '%市' then province_name
      else city_name
      end city_name     
  from csx_dw.shop_m 
  where sdt = 'current'
)c 
on a.customer_no = concat('S',c.shop_id)
left outer join
(
  select
    shop_id,
    shop_name,
    province_name
  from
    csx_dw.shop_m
  where
    sdt = 'current' 
)d 
on a.shop_no = d.shop_id )a;


drop table b2b_tmp.cust_sales_mmm2;
create table b2b_tmp.cust_sales_mmm2
as
select a.AA,a.sdt,a.shop_id,a.customer_no,a.sales_value,a.sales_cost,a.profit,
	a.attribute,a.first_category,a.second_category,a.sales_name,a.work_no,
	a.credential_no,a.goods_code,a.goods_name,a.order_no,a.department_code,a.department_name,
    a.channel,
    a.channel_name,
    case when a.province_name='商超平台'  then '-100' else g.province_code end province_code,
    case when a.province_name='平台-B' then '大客户平台' else a.province_name end province_name
from
	(select
	AA,sdt,shop_id,customer_no,sales_value,sales_cost,profit,
	attribute,first_category,second_category,sales_name,work_no,
	credential_no,goods_code,goods_name,order_no,department_code,department_name,
		case when channel is null or channel='' then '1' when province_name='平台-B' and channel='1' then '1' else channel end channel,
		case when channel is null or channel='' then '大客户' when province_name='平台-B' and channel='1' then '大客户' else channel_name end channel_name,
		case when province_name ='成都省' then '四川省'   else province_name end province_name
	from b2b_tmp.cust_sales_mmm1	)a	
  left outer join 
  (
    select
      province_code,
      province
    from csx_ods.sys_province_ods
  )g on a.province_name=g.province;

