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
set i_sdate =regexp_replace(date_sub(current_date,1),'-','');
set i_sdate_2 =regexp_replace(trunc(date_sub(current_date,1),'MM'),'-','');

drop table b2b_tmp.tmp_goods_salezp;
drop table b2b_tmp.tmp_goods_salezp01;
drop table b2b_tmp.tmp_goods_salezp02;
drop table b2b_tmp.tmp_goods_detail;
drop table b2b_tmp.tmp_goods_res;


--去掉项目合伙人数据、冲销退货单
drop table b2b_tmp.tmp_goods_salezp;
CREATE temporary table b2b_tmp.tmp_goods_salezp
as
-- select coalesce(c.sdt,a.sdt)sdt,
select case when a.order_no like 'RH%' then c.sdt else a.sdt end sdt,
goods_code,dc_code,a.customer_no,d.customer_name,d.sales_province,
sum(sales_qty)sales_qty,
sum(coalesce(cost_price,0)*sales_qty)cost_value,
sum(coalesce(purchase_price,0)*sales_qty)purchase_value,
sum(coalesce(middle_office_price,0)*sales_qty)middle_office_value,
sum(sales_value)sales_value,
sum(profit)profit
from 
(select * from csx_dw.sale_item_m
where sdt>=${hiveconf:i_sdate_2}
and sales_type in ('qyg','gc','anhui','sc','bbc') and customer_no not like 'S%')a 
left join (select distinct substr(sdt,1,6)smonth,customer_no from csx_dw.csx_partner_list1 where sdt>=${hiveconf:i_sdate_2}) b 
on (lpad(a.customer_no,10,'0')=lpad(b.customer_no,10,'0') and substr(a.sdt,1,6)=b.smonth)
left join 
(select origin_order_no,min(sdt)sdt
from csx_dw.sale_item_m 
where sdt>=${hiveconf:i_sdate_2}
and order_no not like 'RH%' and origin_order_no=order_no 
group by origin_order_no) c on c.origin_order_no=a.origin_order_no
left join 
(select *
from csx_dw.customer_m
where sdt= regexp_replace(date_sub(current_date,1),'-','')) d on d.customer_no=a.customer_no
where b.customer_no is null 
and c.origin_order_no is not null
group by case when a.order_no like 'RH%' then c.sdt else a.sdt end,goods_code,dc_code,a.customer_no,d.customer_name,d.sales_province;


--每个DC、客户价格变动明细表，成本价/销售价
drop table b2b_tmp.tmp_goods_salezp01;
CREATE temporary table b2b_tmp.tmp_goods_salezp01
as
select a.sdt,bd_id,bd_name,
b.dept_id,b.dept_name,a.goods_code,b.goodsname,unit_name,case when d.goodsid is null then '非加工' else '加工' end mat_type,
customer_no,customer_name,sales_province,dc_code,shop_name,province_name,sales_qty,cost_value,purchase_value,middle_office_value,sales_value
from 
b2b_tmp.tmp_goods_salezp a 
join 
(select goodsid,regexp_replace(regexp_replace(goodsname,'\n',''),'\r','') as goodsname,unit,unit_name,
dept_id,dept_name,case when bd_id='' then '20' else bd_id end bd_id,
case when bd_id='' then '其他' else bd_name end bd_name
from dim.dim_goods where edate='9999-12-31')b on a.goods_code=b.goodsid 
join (select * from csx_dw.shop_m where sdt='current')c on a.dc_code=c.shop_id
left join (select distinct goodsid from b2b.csx_ecc_marc where mat_type='成品' and goodsid<>'5990')d 
on (a.goods_code=d.goodsid);

--明细数据筛选各省区分部类的销售额的前80%或者top50的商品
drop table b2b_tmp.tmp_goods_salezp02;
CREATE temporary table b2b_tmp.tmp_goods_salezp02
as
select a.bd_id,a.goods_code,a.sales_province,sales_value,
row_number() OVER(PARTITION BY a.bd_id,a.sales_province ORDER BY sales_value desc)rno,
sum(sales_value)over(PARTITION BY a.bd_id,a.sales_province order by sales_value desc)/sales_t zb_sale
 from (select bd_id,goods_code,sales_province,sum(sales_value)sales_value 
 from b2b_tmp.tmp_goods_salezp01 group by bd_id,goods_code,sales_province) a 
join (select sales_province,bd_id,sum(sales_value)sales_t from 
b2b_tmp.tmp_goods_salezp01 group by sales_province,bd_id)b on (a.sales_province=b.sales_province and a.bd_id=b.bd_id);

drop table b2b_tmp.tmp_goods_detail;
CREATE temporary table b2b_tmp.tmp_goods_detail
as
select a.sdt,a.bd_id,a.bd_name,
a.dept_id,a.dept_name,a.goods_code,a.goodsname,a.unit_name,mat_type,
a.customer_no,a.customer_name,a.dc_code,a.shop_name,a.sales_province,
sales_qty,cost_value,purchase_value,middle_office_value,a.sales_value,
cost_value/sales_qty cost_price,
purchase_value/sales_qty purchase_price,
middle_office_value/sales_qty middle_office_price,
a.sales_value/sales_qty sales_price
 from b2b_tmp.tmp_goods_salezp01 a 
join (select * from b2b_tmp.tmp_goods_salezp02 where rno<=50 or zb_sale<0.8)b 
on (a.goods_code=b.goods_code and a.sales_province=b.sales_province)
where a.sales_value<>0;


--提取
insert overwrite directory '/user/raoyanhua/jiage1' row format delimited fields terminated by '\t' 
select * from  b2b_tmp.tmp_goods_detail a
order by a.sales_province,a.dc_code,a.customer_no,a.dept_id,a.goods_code,sdt;

--入库异常，剔除异常单，报价的点，
---昨日销售价格与该省区月至今平均价格
drop table b2b_tmp.tmp_goods_res; 
CREATE temporary table b2b_tmp.tmp_goods_res
as
select a.sdt,a.bd_id,a.bd_name,
a.dept_id,a.dept_name,a.goods_code,a.goodsname,a.unit_name,mat_type,
a.customer_no,a.customer_name,a.dc_code,a.shop_name,a.sales_province,
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
from (select * from b2b_tmp.tmp_goods_salezp01 where sdt=${hiveconf:i_sdate})a 
join (select sales_province,goods_code,sum(sales_qty)sales_qty,sum(cost_value)cost_value,sum(sales_value)sales_value,
sum(cost_value)/sum(sales_qty)cost_std,
sum(sales_value)/sum(sales_qty)sale_std
from b2b_tmp.tmp_goods_salezp01 where sdt<${hiveconf:i_sdate}
group by sales_province,goods_code)b on (a.sales_province=b.sales_province and a.goods_code=b.goods_code);


drop table b2b_tmp.tmp_goods_res01; 
CREATE temporary table b2b_tmp.tmp_goods_res01
as
select a.sdt,a.bd_id,a.bd_name,
a.dept_id,a.dept_name,a.goods_code,a.goodsname,a.unit_name,mat_type,
a.customer_no,a.customer_name,a.dc_code,a.shop_name,a.sales_province,
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
from b2b_tmp.tmp_goods_res a 
where dept_id<>'999' and sales_value<>0;


drop table b2b_tmp.tmp_goods_res02; 
CREATE temporary table b2b_tmp.tmp_goods_res02
as
select a.*,
case when (add_sale_rate>0.1 and tprorate>0.3 and front_prorate>0.1)or (diff_sale>5000 and tprorate>0) then 1 else 0 end high_sale_price,
case when zt_prorate>0.2 and cost_price>1 then 1 else 0 end high_zt_price,
case when diff_cost>1.2 and prorate_std<0.3then 1 else 0 end high_cost_pice,
case when (add_sale_rate<-0.1 and tprorate<0 and front_prorate<0) or (diff_sale<-5000 and tprorate<0) then 1 else 0 end low_sale_price,
case when add_zt_rate<0 and  zt_prorate<-0.1 then 1 else 0 end low_zt_price,
case when (cost_price<=1 and tprorate>0.5)or (tprorate>0.5 and diff_cost<=0.8) then 1 else 0 end low_cost_price
from b2b_tmp.tmp_goods_res01 a;

--报表
insert overwrite directory '/user/raoyanhua/jiage2' row format delimited fields terminated by '\t' 
select * from b2b_tmp.tmp_goods_res02 a order by a.sales_province,a.dc_code,a.dept_id,a.goods_code;

--异常汇总数据
insert overwrite directory '/user/raoyanhua/jiage3' row format delimited fields terminated by '\t' 
select sales_province,count(*)sum_yc,sum(high_sale_price)high_sale_price,sum(high_zt_price)high_zt_price,
sum(high_cost_pice)high_cost_pice,sum(low_sale_price)low_sale_price,sum(low_zt_price)low_zt_price,sum(low_cost_price)low_cost_price
from b2b_tmp.tmp_goods_res02 a 
where high_sale_price=1 or high_zt_price=1 or high_cost_pice=1 or low_sale_price=1 or low_zt_price=1 or low_cost_price=1
group by sales_province
union all 
select '总计'sales_province,count(*)sum_yc,sum(high_sale_price)high_sale_price,sum(high_zt_price)high_zt_price,
sum(high_cost_pice)high_cost_pice,sum(low_sale_price)low_sale_price,sum(low_zt_price)low_zt_price,sum(low_cost_price)low_cost_price
from b2b_tmp.tmp_goods_res02 a 
where high_sale_price=1 or high_zt_price=1 or high_cost_pice=1 or low_sale_price=1 or low_zt_price=1 or low_cost_price=1;

insert overwrite directory '/user/raoyanhua/jiage4' row format delimited fields terminated by '\t' 
select sales_province,count(case when bd_name='其他' then 1 else null end)qt_yc,
count(case when bd_name='生鲜事业部' then 1 else null end)sx_yc,
count(case when bd_name='食百事业部' then 1 else null end)sb_yc,
count(*)sum_yc
from b2b_tmp.tmp_goods_res02 a 
where high_sale_price=1 or high_zt_price=1 or high_cost_pice=1 or low_sale_price=1 or low_zt_price=1 or low_cost_price=1
group by sales_province
union all 
select '总计'sales_province,
count(case when bd_name='其他' then 1 else null end)qt_yc,
count(case when bd_name='生鲜事业部' then 1 else null end)sx_yc,
count(case when bd_name='食百事业部' then 1 else null end)sb_yc,
count(*)sum_yc
from b2b_tmp.tmp_goods_res02 a 
where high_sale_price=1 or high_zt_price=1 or high_cost_pice=1 or low_sale_price=1 or low_zt_price=1 or low_cost_price=1;

--总毛利率高于0.3
insert overwrite directory '/user/raoyanhua/jiage5' row format delimited fields terminated by '\t'
select sales_province,count(case when bd_name='其他' then 1 else null end)qt_yc,
count(case when bd_name='生鲜事业部' then 1 else null end)sx_yc,
count(case when bd_name='食百事业部' then 1 else null end)sb_yc,
count(*)sum_yc
from b2b_tmp.tmp_goods_res02 a 
where tprorate>0.3
group by sales_province
union all 
select '总计'sales_province,
count(case when bd_name='其他' then 1 else null end)qt_yc,
count(case when bd_name='生鲜事业部' then 1 else null end)sx_yc,
count(case when bd_name='食百事业部' then 1 else null end)sb_yc,
count(*)sum_yc
from b2b_tmp.tmp_goods_res02 a 
where tprorate>0.3;

--负毛利率
insert overwrite directory '/user/raoyanhua/jiage6' row format delimited fields terminated by '\t'
select sales_province,count(case when bd_name='其他' and tprorate<0  then 1 else null end)qt_fml,
count(case when bd_name='生鲜事业部' and tprorate<0 then 1 else null end)sx_fml,
count(case when bd_name='食百事业部' and tprorate<0 then 1 else null end)sb_fml,
count(case when tprorate<0 then 1 else null end)sum_fml,

count(case when bd_name='其他' then 1 else null end)qt_yc,
count(case when bd_name='生鲜事业部' then 1 else null end)sx_yc,
count(case when bd_name='食百事业部' then 1 else null end)sb_yc,
count(*)sum_yc
from b2b_tmp.tmp_goods_res02 a 
group by sales_province
union all 
select '总计'sales_province,
count(case when bd_name='其他' and tprorate<0  then 1 else null end)qt_fml,
count(case when bd_name='生鲜事业部' and tprorate<0 then 1 else null end)sx_fml,
count(case when bd_name='食百事业部' and tprorate<0 then 1 else null end)sb_fml,
count(case when tprorate<0 then 1 else null end)sum_fml,

count(case when bd_name='其他' then 1 else null end)qt_yc,
count(case when bd_name='生鲜事业部' then 1 else null end)sx_yc,
count(case when bd_name='食百事业部' then 1 else null end)sb_yc,
count(*)sum_yc
from b2b_tmp.tmp_goods_res02 a;

