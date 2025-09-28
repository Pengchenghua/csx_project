1、日常进价/售价变动趋势，出现异常价格及时预警；
2、
 不匹配的 报价异常 
进售价，总进价额比预算进价额超过1万元；趋势  入库 采购报价/中台报价  库存平均价 售价 -销售毛利低时受那部分影响
进售价：日常进价和异常进价   进售价 不匹配的 报价异常 
进售价，总进价额比预算进价额超过1万元；趋势  入库 采购报价/中台报价  库存平均价 售价 -销售毛利低时受那部分影响
1、这份的分析模型里加一下云超的平均进价，另外后续具体影响到的订单明细要有一个.
2、报告形式增加一个。比如：按一个月计算，目前按你的算法各个异常类型价格有多少SKU，就统计成PPT报告类似的维度
3、负毛利的订单占比，负毛利商品占比.
4、整体进货价格在上升，或者呈现上升趋势的SKU有多少，占比多少.--以各个课组或者部分销售额占比90%的商品--(全球眼模式，但是
取当天所有商品的进货单，不只是取量最大的那一笔).--云超有的这些商品比例是多少，它那边是什么情况.
5、进货价格在上升，但是售价在下滑的商品.
7、负毛利订单里面，有多少是报价问题，或者是售价问题.

--商品价格每日变动情况
set i_sdate = '${START_DATE}';
set i_sdate_l35 =date_sub(${hiveconf:i_sdate},35);

drop table b2b_tmp.temp_salezp;
drop table b2b_tmp.temp_salezp01;
drop table b2b_tmp.temp_salezp02;
drop table b2b_tmp.temp_detail;
drop table b2b_tmp.temp_res;

--去掉项目合伙人数据
drop table b2b_tmp.temp_salezp;
CREATE temporary table b2b_tmp.temp_salezp
as
select sdt,goods_code,dc_code,
sum(sales_qty)sales_qty,
sum(coalesce(cost_price,0)*sales_qty)cost_value,
sum(coalesce(purchase_price,0)*sales_qty)purchase_value,
sum(coalesce(middle_office_price,0)*sales_qty)middle_office_value,
sum(sales_value)sales_value,
sum(profit)profit
from 
(select * from csx_dw.sale_item_m
where sdt>=regexp_replace(${hiveconf:i_sdate_l35},'-','')
and sales_type in ('qyg','gc','anhui','sc','bbc') and customer_no not like 'S%')a 
left join (select distinct substr(sdt,1,6)smonth,customer_no from csx_dw.csx_partner_list where sdt>=regexp_replace(${hiveconf:i_sdate_l35},'-','')) b 
on (lpad(a.customer_no,10,'0')=lpad(b.customer_no,10,'0') and substr(a.sdt,1,6)=b.smonth)
where b.customer_no is null 
group by sdt,goods_code,dc_code;






--每个DC价格变动明细表，成本价/销售价
drop table b2b_tmp.temp_salezp01;
CREATE temporary table b2b_tmp.temp_salezp01
as
select a.sdt,bd_id,bd_name,
b.dept_id,b.dept_name,a.goods_code,b.goodsname,unit_name,case when d.goodsid is null then '非加工' else '加工' end mat_type,
dc_code,shop_name,province_name,sales_qty,cost_value,purchase_value,middle_office_value,sales_value
from 
b2b_tmp.temp_salezp a 
join 
(select goodsid,regexp_replace(regexp_replace(goodsname,'\n',''),'\r','') as goodsname,unit,unit_name,
dept_id,dept_name,case when bd_id='' then '20' else bd_id end bd_id,
case when bd_id='' then '其他' else bd_name end bd_name
from dim.dim_goods where edate='9999-12-31')b on a.goods_code=b.goodsid 
join (select * from csx_dw.shop_m where sdt='current')c on a.dc_code=c.shop_id
left join (select distinct goodsid from b2b.csx_ecc_marc where mat_type='成品' and goodsid<>'5990')d 
on (a.goods_code=d.goodsid);

--明细数据筛选各省区分部类的销售额的前80%或者top50的商品
drop table b2b_tmp.temp_salezp02;
CREATE temporary table b2b_tmp.temp_salezp02
as
select a.bd_id,a.goods_code,a.province_name,sales_value,
row_number() OVER(PARTITION BY a.bd_id,a.province_name ORDER BY sales_value desc)rno,
sum(sales_value)over(PARTITION BY a.bd_id,a.province_name order by sales_value desc)/sales_t zb_sale
 from (select bd_id,goods_code,province_name,sum(sales_value)sales_value 
 from b2b_tmp.temp_salezp01 group by bd_id,goods_code,province_name) a 
join (select province_name,bd_id,sum(sales_value)sales_t from 
b2b_tmp.temp_salezp01 group by province_name,bd_id)b on (a.province_name=b.province_name and a.bd_id=b.bd_id);

drop table b2b_tmp.temp_detail;
CREATE temporary table b2b_tmp.temp_detail
as
select a.sdt,a.bd_id,a.bd_name,
a.dept_id,a.dept_name,a.goods_code,a.goodsname,a.unit_name,mat_type,
a.dc_code,a.shop_name,a.province_name,
sales_qty,cost_value,purchase_value,middle_office_value,a.sales_value,
cost_value/sales_qty cost_price,
purchase_value/sales_qty purchase_price,
middle_office_value/sales_qty middle_office_price,
a.sales_value/sales_qty sales_price
 from b2b_tmp.temp_salezp01 a 
join (select * from b2b_tmp.temp_salezp02 where rno<=50 or zb_sale<0.8)b 
on (a.goods_code=b.goods_code and a.province_name=b.province_name)
where a.sales_value<>0;


--提取
select * from  b2b_tmp.temp_detail a
order by a.province_name,a.dc_code,a.dept_id,a.goods_code,sdt;

--入库异常，剔除异常单，报价的点，
---昨日销售价格与该省区近35天平均价格
drop table b2b_tmp.temp_res; 
CREATE temporary table b2b_tmp.temp_res
as
select a.sdt,a.bd_id,a.bd_name,
a.dept_id,a.dept_name,a.goods_code,a.goodsname,a.unit_name,mat_type,
a.dc_code,a.shop_name,a.province_name,
a.sales_qty,a.cost_value,a.purchase_value,a.middle_office_value,a.sales_value,
a.cost_value/a.sales_qty cost_price,
purchase_value/a.sales_qty purchase_price,
middle_office_value/a.sales_qty middle_office_price,
a.sales_value/a.sales_qty sales_price,cost_std,sale_std,
1-a.cost_value/a.sales_value tprorate,
1-a.middle_office_value/a.sales_value front_prorate,
a.sales_value-b.sale_std*a.sales_qty diff_sale,--当前销售额与核准销售额的差异
a.sales_value/a.sales_qty-sale_std diff_sale_price,--销售价格偏差较大
a.middle_office_value/a.sales_qty-cost_std diff_zt_price --中台报价提价
from (select * from b2b_tmp.temp_salezp01 where sdt=regexp_replace(${hiveconf:i_sdate},'-',''))a 
join (select province_name,goods_code,sum(sales_qty)sales_qty,sum(cost_value)cost_value,sum(sales_value)sales_value,
sum(cost_value)/sum(sales_qty)cost_std,
sum(sales_value)/sum(sales_qty)sale_std
from b2b_tmp.temp_salezp01 where sdt<regexp_replace(${hiveconf:i_sdate},'-','')
group by province_name,goods_code)b on (a.province_name=b.province_name and a.goods_code=b.goods_code);


drop table b2b_tmp.temp_res01; 
CREATE temporary table b2b_tmp.temp_res01
as
select a.sdt,a.bd_id,a.bd_name,
a.dept_id,a.dept_name,a.goods_code,a.goodsname,a.unit_name,mat_type,
a.dc_code,a.shop_name,a.province_name,
a.sales_qty,
a.cost_value,
a.purchase_value,
a.middle_office_value,
a.sales_value,
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
case when sale_std  is null or sale_std<0 then 0 else diff_sale_price/sale_std end add_sale_rate, --销售价格差%
diff_zt_price,
case when cost_std is null then 0 else diff_zt_price/cost_std end add_zt_rate, --中台加价率 
case when cost_std is null then 1 else cost_price/cost_std end diff_cost, --成本价对比
case when sale_std is null or sale_std<0 then 0 else 1.0-cost_std/sale_std end prorate_std,--历史毛利率
tprorate-front_prorate zt_prorate --总毛利率-前台毛利率
from b2b_tmp.temp_res a 
where dept_id<>'999' and sales_value<>0;


drop table b2b_tmp.temp_res02; 
CREATE temporary table b2b_tmp.temp_res02
as
select a.*,
case when (add_sale_rate>0.1 and tprorate>0.3 and front_prorate>0.1)or (diff_sale>5000 and tprorate>0) then 1 else 0 end high_sale_price,
case when zt_prorate>0.2 and cost_price>1 then 1 else 0 end high_zt_price,
case when diff_cost>1.2 and prorate_std<0.3then 1 else 0 end high_cost_pice,
case when (add_sale_rate<-0.1 and tprorate<0 and front_prorate<0) or (diff_sale<-5000 and tprorate<0) then 1 else 0 end low_sale_price,
case when add_zt_rate<0 and  zt_prorate<-0.1 then 1 else 0 end low_zt_price,
case when (cost_price<=1 and tprorate>0.5)or (tprorate>0.5 and diff_cost<=0.8) then 1 else 0 end low_cost_price
from b2b_tmp.temp_res01 a;

--报表
select * from b2b_tmp.temp_res02 a order by a.province_name,a.dc_code,a.dept_id,a.goods_code;

--异常汇总数据
select province_name,count(*)sum_yc,sum(high_sale_price)high_sale_price,sum(high_zt_price)high_zt_price,
sum(high_cost_pice)high_cost_pice,sum(low_sale_price)low_sale_price,sum(low_zt_price)low_zt_price,sum(low_cost_price)low_cost_price
from b2b_tmp.temp_res02 a 
where high_sale_price=1 or high_zt_price=1 or high_cost_pice=1 or low_sale_price=1 or low_zt_price=1 or low_cost_price=1
group by province_name
union all 
select '总计'province_name,count(*)sum_yc,sum(high_sale_price)high_sale_price,sum(high_zt_price)high_zt_price,
sum(high_cost_pice)high_cost_pice,sum(low_sale_price)low_sale_price,sum(low_zt_price)low_zt_price,sum(low_cost_price)low_cost_price
from b2b_tmp.temp_res02 a 
where high_sale_price=1 or high_zt_price=1 or high_cost_pice=1 or low_sale_price=1 or low_zt_price=1 or low_cost_price=1;


select province_name,count(case when bd_name='其他' then 1 else null end)qt_yc,
count(case when bd_name='生鲜事业部' then 1 else null end)sx_yc,
count(case when bd_name='食百事业部' then 1 else null end)sb_yc,
count(*)sum_yc
from b2b_tmp.temp_res02 a 
where high_sale_price=1 or high_zt_price=1 or high_cost_pice=1 or low_sale_price=1 or low_zt_price=1 or low_cost_price=1
group by province_name
union all 
select '总计'province_name,
count(case when bd_name='其他' then 1 else null end)qt_yc,
count(case when bd_name='生鲜事业部' then 1 else null end)sx_yc,
count(case when bd_name='食百事业部' then 1 else null end)sb_yc,
count(*)sum_yc
from b2b_tmp.temp_res02 a 
where high_sale_price=1 or high_zt_price=1 or high_cost_pice=1 or low_sale_price=1 or low_zt_price=1 or low_cost_price=1;

--总毛利率高于0.3
select province_name,count(case when bd_name='其他' then 1 else null end)qt_yc,
count(case when bd_name='生鲜事业部' then 1 else null end)sx_yc,
count(case when bd_name='食百事业部' then 1 else null end)sb_yc,
count(*)sum_yc
from b2b_tmp.temp_res02 a 
where tprorate>0.3
group by province_name
union all 
select '总计'province_name,
count(case when bd_name='其他' then 1 else null end)qt_yc,
count(case when bd_name='生鲜事业部' then 1 else null end)sx_yc,
count(case when bd_name='食百事业部' then 1 else null end)sb_yc,
count(*)sum_yc
from b2b_tmp.temp_res02 a 
where tprorate>0.3;

--负毛利率
select province_name,count(case when bd_name='其他' and tprorate<0  then 1 else null end)qt_fml,
count(case when bd_name='生鲜事业部' and tprorate<0 then 1 else null end)sx_fml,
count(case when bd_name='食百事业部' and tprorate<0 then 1 else null end)sb_fml,
count(case when tprorate<0 then 1 else null end)sum_fml,

count(case when bd_name='其他' then 1 else null end)qt_yc,
count(case when bd_name='生鲜事业部' then 1 else null end)sx_yc,
count(case when bd_name='食百事业部' then 1 else null end)sb_yc,
count(*)sum_yc
from b2b_tmp.temp_res02 a 
group by province_name
union all 
select '总计'province_name,
count(case when bd_name='其他' and tprorate<0  then 1 else null end)qt_fml,
count(case when bd_name='生鲜事业部' and tprorate<0 then 1 else null end)sx_fml,
count(case when bd_name='食百事业部' and tprorate<0 then 1 else null end)sb_fml,
count(case when tprorate<0 then 1 else null end)sum_fml,

count(case when bd_name='其他' then 1 else null end)qt_yc,
count(case when bd_name='生鲜事业部' then 1 else null end)sx_yc,
count(case when bd_name='食百事业部' then 1 else null end)sb_yc,
count(*)sum_yc
from b2b_tmp.temp_res02 a;


---进货数据
drop table b2b_tmp.temp_purprice;
CREATE temporary table b2b_tmp.temp_purprice
as
select stype,province_name,shop_id_in,shop_name,
a.goodsid,goodsname,b.unit_name,b.dept_id,b.dept_name,b.bd_id,b.bd_name,
substr(sdt,1,6)smm,sum(pur_qty_in)pur_qty,sum(tax_pur_val_in)pur_amt,
sum(tax_pur_val_in)/sum(pur_qty_in) pur_price
from 
(
--旧系统物流入库数据
select shop_id_in,goodsid,pur_doc_id,sdt,pur_qty_in,tax_pur_val_in from b2b.ord_orderflow_t 
where sdt>='20191001'
and shop_id_in like 'W%' 
 and  pur_qty_in>0 and ordertype not in ('返配','退货') and regexp_replace(vendor_id,'(0|^)([^0].*)',2) not like '75%'
union all --旧系统门店入库(只挑选直送)
select shop_id_in,goodsid,pur_doc_id,sdt,pur_qty_in,tax_pur_val_in from b2b.ord_orderflow_t 
where sdt>='20191001'
and shop_id_in not like 'W%' and pur_doc_id like '40%' and regexp_replace(vendor_id,'(0|^)([^0].*)',2) not like '75%'
 and  pur_qty_in>0
 union all --新系统入库数据
 select location_code shop_id_in,product_code goodsid,a.order_code pur_doc_id,a.sdate sdt,receive_qty pur_qty_in,amount tax_pur_val_in
from 
(select distinct order_code,regexp_replace(to_date(receive_time),'-','')sdate from csx_ods.wms_entry_order_header_ods a 
where sdt>='20191015' and entry_type LIKE 'P%'
and to_date(receive_time)>='2019-10-01' and return_flag<>'Y' and receive_status<>0)a 
join 
(select distinct order_code,product_code,location_code,receive_qty,price,amount from csx_ods.wms_entry_order_item_ods 
where sdt>='20191015' and receive_qty>0
and to_date(update_time)>='2019-10-01')b 
on a.order_code=b.order_code )a 
join (select shop_id,shop_name,province_name,case when sales_belong_flag='1_云超' then '云超' else '彩食鲜'end stype
from csx_dw.shop_m 
where sdt='current' and sales_belong_flag in ('1_云超','4_企业购','5_彩食鲜') 
and shop_id not in('W0H3','W0H7','W0H8','W0H6','W0H9','W0K2','W0G7','W0H4','W0G1','W0K4'))c on a.shop_id_in=c.shop_id
join 
(select goodsid,regexp_replace(regexp_replace(goodsname,'\n',''),'\r','') as goodsname,unit,unit_name,
dept_id,dept_name,case when bd_id='' then '20' else bd_id end bd_id,
case when bd_id='' then '其他' else bd_name end bd_name
from dim.dim_goods where edate='9999-12-31')b on a.goodsid=b.goodsid
group by stype,province_name,shop_id_in,shop_name,
a.goodsid,goodsname,b.unit_name,b.dept_id,b.dept_name,b.bd_id,b.bd_name,substr(sdt,1,6);

set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table csx_dw.purprice_globaleye01 partition (smm) 
select a.stype,a.province_name,a.goodsid,goodsname,a.dept_id,dept_name,
sum(pur_amt)/sum(pur_qty) pur_price,smm
from 
(select * from b2b_tmp.temp_purprice 
where province_name in ('上海市','北京市','四川省','安徽省',
'广东省','江苏省','河北省','浙江省',
'福建省','重庆市'))a 
join (select distinct goodsid from b2b_tmp.temp_purprice where province_name in ('上海市','北京市','四川省','安徽省',
'广东省','江苏省','河北省','浙江省',
'福建省','重庆市') and stype='彩食鲜')b on a.goodsid=b.goodsid 
group by a.stype,a.province_name,a.goodsid,goodsname,a.dept_id,dept_name,smm
order by a.dept_id;


INVALIDATE METADATA csx_dw.purprice_globaleye01;


select a.province_name,a.goodsid,goodsname,dept_id,dept_name,b.pur_price price10,c.pur_price price11,d.pur_price price12
from 
(SELECT distinct province_name,goodsid,goodsname,dept_id,dept_name 
from csx_dw.purprice_globaleye01 where stype='彩食鲜')a 
left join 
(select province_name,goodsid,pur_price from  csx_dw.purprice_globaleye01 where stype='彩食鲜' and smm='201910')b 
on (a.province_name=b.province_name and a.goodsid=b.goodsid)
left join 
(select province_name,goodsid,pur_price from  csx_dw.purprice_globaleye01 where stype='彩食鲜' and smm='201911')c
on (a.province_name=c.province_name and a.goodsid=c.goodsid)
left join 
(select province_name,goodsid,pur_price from  csx_dw.purprice_globaleye01 where stype='彩食鲜' and smm='201912')d
on (a.province_name=d.province_name and a.goodsid=d.goodsid)
order by dept_id,a.goodsid,a.province_name;


create table csx_dw.purprice_globaleye01
(
stype string comment  '物流类型',
province_name string comment  '省份',
goodsid string comment  '商品编码',
goodsname string comment  '商品名称',
dept_id string comment  '课组编码',
dept_name string comment  '课组名称',
pur_price decimal(26,4) comment '进货价格'
)
comment '彩食鲜进价全球眼'
partitioned by (smm string comment '过账日期分区')
row format delimited
stored as parquet;




