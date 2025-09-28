--select ${hiveconf:i_sdate},${hiveconf:i_sdate_1},${hiveconf:i_sdate_2},${hiveconf:i_sdate_3},${hiveconf:i_sdate_4};
--1、取省区大类中销售额占比前80%或入库金额前80%商品满足任一条件
--2、采购进价包含直送地采数据，取当天采购数量最大单的价格、当天所有采购综合价格、数量、金额。
--3、销售取当天所有销售商品综合采购报价、中台报价、售价；当天价格=价格*数量/数量
--20210810 销售表剔除条件返利数据


 --昨日,本月第1天,上月最后一天,前7天,本月第1天与前7天比较的较早日期
set i_sdate =regexp_replace(date_sub(current_date,1),'-','');
set i_sdate_1 =regexp_replace(trunc(date_sub(current_date,1),'MM'),'-','');
set i_sdate_2 =regexp_replace(add_months(last_day(date_sub(current_date,1)),-1),'-','');
set i_sdate_3=regexp_replace(date_sub(current_date,7),'-','');
set i_sdate_4=if(regexp_replace(date_sub(current_date,7),'-','')>=regexp_replace(trunc(date_sub(current_date,1),'MM'),'-',''),
				regexp_replace(trunc(date_sub(current_date,1),'MM'),'-',''), 
				regexp_replace(date_sub(current_date,7),'-',''));


 --临时表1：月至今销售数量、金额（7号前去最近7天，7号后取月至今）
drop table csx_tmp.temp_price_trend_sale0;
create temporary table csx_tmp.temp_price_trend_sale0 as
select 'sale'STYPE,
             a.province_code,
             a.province_name,
             a.city_name,
             a.location_code,
             a.location_name,
             a.goods_code,
             a.sdt,
             d.goods_name,
             d.division_name,
             d.category_large_code catg_l_id,
             d.category_large_name catg_l_name,
             d.category_middle_code catg_m_id,
             d.category_middle_name catg_m_name,
             d.department_id dept_id,
             d.department_name dept_name,
             sum(a.qty)qty,
             sum(a.untax_sale)untax_sale,
             sum(a.sale)sale
from --销售表中大客户生鲜部类
  (select dc_province_code province_code,
          dc_province_name province_name,
          dc_city_name city_name,
          dc_code location_code,
          dc_name location_name,
          goods_code,
          customer_no,
          sdt,
          sum(sales_qty)qty,
          sum(excluding_tax_sales)untax_sale,
          sum(sales_value)sale
   from csx_dw.dws_sale_r_d_detail
   where sdt>=${hiveconf:i_sdate_4}
     and sdt<=${hiveconf:i_sdate}
     and department_code like 'H%'
     and channel_name like'大客户%' 
	 and business_type_code<>'4' --不含城市服务商业绩
	 and sales_type<>'fanli'
     --and (order_no not in ('OC200529000043','OC200529000044','OC200529000045','OC200529000046') or order_no is null)
   group by dc_province_code,
            dc_province_name,
            dc_city_name,
            dc_code,
            dc_name,
            goods_code,
            customer_no,
            sdt)a 
left join
  (select *
   from csx_dw.dws_basic_w_a_csx_product_m
   where sdt='current') d on d.goods_id=a.goods_code 
where d.division_code='11' --部类为11的
group by a.province_code,
         a.province_name,
         a.city_name,
         a.location_code,
         a.location_name,
         a.goods_code,
         a.sdt,
         d.goods_name,
         d.division_name,
         d.category_large_code,
         d.category_large_name,
         d.category_middle_code,
         d.category_middle_name,
         d.department_id,
         d.department_name;


 --临时表2：销售占比 各商品在该城市大类销售额占比
drop table csx_tmp.temp_price_trend_sale1;
create temporary table csx_tmp.temp_price_trend_sale1 as
select a.goods_code,
       a.goods_name,
       a.province_code,
       a.province_name,
       a.city_name,
       a.catg_l_id,
       a.catg_l_name,
       x.dept_id,
       x.dept_name,
       x.catg_m_id,
       x.catg_m_name,
       a.qty,
       a.untax_sale,
       a.sale,
       row_number() over(partition by a.province_code,a.province_name,a.city_name,a.catg_l_id
                         order by sale desc)rno,
                                            sum(sale)over(partition by a.province_code,a.province_name,a.city_name,a.catg_l_id
                                                          order by sale desc rows between UNBOUNDED PRECEDING and 0 PRECEDING)/sale_t zb_sale
from
  (select goods_code,
          goods_name,
          province_code,
          province_name,
          city_name,
          catg_l_id,
          catg_l_name,
          sum(qty)qty,
          sum(untax_sale)untax_sale,
          sum(sale)sale
   from csx_tmp.temp_price_trend_sale0
   group by goods_code,
            goods_name,
            province_code,
            province_name,
            city_name,
            catg_l_id,
            catg_l_name having sale>0) a
join
  (select goods_id goods_code,
          goods_name,
          department_id dept_id,
          department_name dept_name,
          category_middle_code catg_m_id,
          category_middle_name catg_m_name
   from csx_dw.dws_basic_w_a_csx_product_m
   where sdt='current')x on a.goods_code=x.goods_code
join
  (select province_code,
          province_name,
          city_name,
          catg_l_id,
          sum(sale)sale_t
   from csx_tmp.temp_price_trend_sale0
   group by province_code,
            province_name,
            city_name,
            catg_l_id )b on b.province_name=a.province_name
and b.city_name=a.city_name
and b.catg_l_id=a.catg_l_id;


 --临时表3：采购入库的数量、单价、金额
drop table csx_tmp.temp_price_trend_pur0;
create temporary table csx_tmp.temp_price_trend_pur0 as
select '彩食鲜' STYPE,
             c.province_code,
             c.province_name,
             c.city_name,
             location_code,
             shop_name,
             a.goods_code,
             pur_doc_id,
             a.sdt,
             d.goods_name,
             d.division_name,
             d.category_large_code catg_l_id,
             d.category_large_name catg_l_name,
             d.category_middle_code catg_m_id,
             d.category_middle_name catg_m_name,
             d.department_id dept_id,
             d.department_name dept_name,
             pur_qty_in,
             pur_price,
             tax_pur_val_in
from
  ( select location_code location_code,product_code goods_code,a.order_code pur_doc_id,a.sdate sdt,receive_qty pur_qty_in,price pur_price,(receive_qty*price) tax_pur_val_in
   from
     (select distinct order_code,
                      regexp_replace(to_date(receive_time),'-','')sdate
      from csx_ods.source_wms_r_d_entry_order_header a
      where sdt>=regexp_replace(date_sub(current_date,90),'-','')
        and sdt<=${hiveconf:i_sdate}
        and entry_type like 'P%' --采购入库
and to_date(receive_time)>=to_date(concat(substr(${hiveconf:i_sdate_4},1,4),'-',substr(${hiveconf:i_sdate_4},5,2),'-',substr(${hiveconf:i_sdate_4},7,2)))
        and return_flag<>'Y'
        and receive_status<>0)a
   join
     (select distinct order_code,
                      product_code,
                      location_code,
                      receive_qty,
                      price,
                      amount
      from csx_ods.source_wms_r_d_entry_order_item
      where sdt>=regexp_replace(date_sub(current_date,90),'-','')
        and sdt<=${hiveconf:i_sdate}
        and receive_qty>0)b on a.order_code=b.order_code )a
join
  (select shop_id,
          shop_name,
          province_code,
          province_name,
          city_name
   from csx_dw.dws_basic_w_a_csx_shop_m
   where sdt='current')c on a.location_code=c.shop_id
left join
  (select *
   from csx_dw.dws_basic_w_a_csx_product_m
   where sdt='current') d on d.goods_id=a.goods_code
where d.department_id like 'H%' ;


 --临时表4：采购入库占比 各商品在城市大类入库金额占比
drop table csx_tmp.temp_price_trend_pur1;
create temporary table csx_tmp.temp_price_trend_pur1 as
select a.goods_code,
       a.goods_name,
       a.province_code,
       a.province_name,
       a.city_name,
       a.catg_l_id,
       a.catg_l_name,
       x.dept_id,
       x.dept_name,
       x.catg_m_id,
       x.catg_m_name,
       a.pur_qty_in,
       a.tax_pur_val_in,
       row_number() over(partition by a.province_code,a.province_name,a.city_name,a.catg_l_id
                         order by tax_pur_val_in desc)rno,
                                                      sum(tax_pur_val_in)over(partition by a.province_code,a.province_name,a.city_name,a.catg_l_id
                                                                              order by tax_pur_val_in desc rows between UNBOUNDED PRECEDING and 0 PRECEDING)/tax_pur_val_in_t zb_tax_pur_val_in
from
  (select goods_code,
          goods_name,
          province_code,
          province_name,
          city_name,
          catg_l_id,
          catg_l_name,
          sum(pur_qty_in)pur_qty_in,
          sum(tax_pur_val_in)tax_pur_val_in
   from csx_tmp.temp_price_trend_pur0
   group by goods_code,
            goods_name,
            province_code,
            province_name,
            city_name,
            catg_l_id,
            catg_l_name ) a
join
  (select goods_id goods_code,
          goods_name,
          department_id dept_id,
          department_name dept_name,
          category_middle_code catg_m_id,
          category_middle_name catg_m_name
   from csx_dw.dws_basic_w_a_csx_product_m
   where sdt='current')x on a.goods_code=x.goods_code
join
  (select province_name,
          city_name,
          catg_l_id,
          sum(tax_pur_val_in)tax_pur_val_in_t
   from csx_tmp.temp_price_trend_pur0
   group by province_name,
            city_name,
            catg_l_id )b on b.province_name=a.province_name
and b.city_name=a.city_name
and b.catg_l_id=a.catg_l_id;


 --临时表5：销售前80%或采购入库前80%的商品
drop table csx_tmp.temp_price_trend_pur_sale;
create temporary table csx_tmp.temp_price_trend_pur_sale as
select goods_code,
       goods_name,
       province_code,
       province_name,
       city_name,
       catg_l_id,
       catg_l_name,
       dept_id,
       dept_name,
       catg_m_id,
       catg_m_name,
       sum(sale)sale,
       sum(rno)rno,
       sum(zb_sale)zb_sale,
       sum(tax_pur_val_in)tax_pur_val_in,
       sum(rno_pur)rno_pur,
       sum(zb_tax_pur_val_in)zb_tax_pur_val_in
from
  (select '销售' types,
          goods_code,
          goods_name,
          province_code,
          province_name,
          city_name,
          catg_l_id,
          catg_l_name,
          dept_id,
          dept_name,
          catg_m_id,
          catg_m_name,
          qty,
          sale,
          rno,
          zb_sale,
          0 pur_qty_in,
          0 tax_pur_val_in,
          0 rno_pur,
          0 zb_tax_pur_val_in
   from csx_tmp.temp_price_trend_sale1
   where zb_sale<=0.8 --and (rno<=80 or rno is null)
   union all select '采购入库' types,
                    goods_code,
                    goods_name,
                    province_code,
                    province_name,
                    city_name,
                    catg_l_id,
                    catg_l_name,
                    dept_id,
                    dept_name,
                    catg_m_id,
                    catg_m_name,
                    0 qty,
                    0 sale,
                    0 rno,
                    0 zb_sale,
                    pur_qty_in,
                    tax_pur_val_in,
                    rno rno_pur,
                    zb_tax_pur_val_in
   from csx_tmp.temp_price_trend_pur1
   where zb_tax_pur_val_in<=0.8 --and (rno<=80 or rno is null)
 )a
group by goods_code,
         goods_name,
         province_code,
         province_name,
         city_name,
         catg_l_id,
         catg_l_name,
         dept_id,
         dept_name,
         catg_m_id,
         catg_m_name;


 --临时表6：筛选销售或采购入库前80%的商品--入库数量、单价、金额
drop table csx_tmp.temp_price_trend_pur2;
create temporary table csx_tmp.temp_price_trend_pur2 as
select a.province_code,
       a.province_name,
       a.city_name,
       a.location_code,
       a.shop_name location_name,
       a.goods_code,
       b.goods_name,
       b.catg_l_id,
       b.catg_l_name,
       b.dept_id,
       b.dept_name,
       b.catg_m_id,
       b.catg_m_name,
       pur_doc_id order_no,
       sdt,
       a.pur_qty_in,
       a.pur_price,
       a.tax_pur_val_in,
       '' sales_qty,
       '' cost_price,
       '' purchase_price,
       '' middle_office_price,
       '' promotion_price,
       '' sales_value,
       datediff(date_sub(current_date,0), concat(substr(a.sdt,1,4),'-',substr(a.sdt,5,2),'-',substr(a.sdt,7,2))) days
from
  (select c.province_code,
          c.province_name,
          c.city_name,
          location_code,
          shop_name,
          goods_code,
          pur_doc_id,
          sdt,
          a.pur_qty_in,
          a.pur_price,
          a.tax_pur_val_in
   from
     ( select location_code,product_code goods_code,a.order_code pur_doc_id,a.sdate sdt,receive_qty pur_qty_in,price pur_price,(receive_qty*price) tax_pur_val_in
      from
        (select distinct order_code,
                         regexp_replace(to_date(receive_time),'-','')sdate
         from csx_ods.source_wms_r_d_entry_order_header a
         where sdt>=regexp_replace(date_sub(current_date,90),'-','')
           and sdt<=${hiveconf:i_sdate}
           and entry_type like 'P%' --采购入库
		   and to_date(receive_time)>=to_date(concat(substr(${hiveconf:i_sdate_4},1,4),'-',substr(${hiveconf:i_sdate_4},5,2),'-',substr(${hiveconf:i_sdate_4},7,2)))
           and return_flag<>'Y'
           and receive_status<>0)a
      join
        (select distinct order_code,
                         product_code,
                         location_code,
                         receive_qty,
                         price,
                         amount
         from csx_ods.source_wms_r_d_entry_order_item
         where sdt>=regexp_replace(date_sub(current_date,90),'-','')
           and sdt<=${hiveconf:i_sdate}
           and receive_qty>0)b on a.order_code=b.order_code )a
   join
     (select shop_id,
             shop_name,
             province_code,
             province_name,
             city_name
      from csx_dw.dws_basic_w_a_csx_shop_m
      where sdt='current')c on a.location_code=c.shop_id)a
join csx_tmp.temp_price_trend_pur_sale b on b.province_name=a.province_name
and a.city_name=b.city_name
and a.goods_code=b.goods_code
where b.dept_id like 'H%';


 --临时表7：各天各商品按采购数量降序排列，用于后续取当天采购最大的一笔价格
drop table csx_tmp.temp_price_trend_pur3;
create temporary table csx_tmp.temp_price_trend_pur3 as
select province_code,
       province_name,
       city_name,
       location_code,
       location_name,
       goods_code,
       goods_name,
       catg_l_id,
       catg_l_name,
       dept_id,
       dept_name,
       catg_m_id,
       catg_m_name,
       order_no,
       sdt,
       pur_qty_in,
       pur_price,
       tax_pur_val_in,
       days,
       row_number() over(partition by province_code,province_name,city_name,location_code,location_name,goods_code,goods_name,sdt
                         order by pur_qty_in desc)pur_rno
from csx_tmp.temp_price_trend_pur2;


 --临时表8：筛选销售或采购入库前80%的商品--销售数量、单价、金额
drop table csx_tmp.temp_price_trend_sale2;
create temporary table csx_tmp.temp_price_trend_sale2 as
select a.province_code,
       a.province_name,
       a.city_name,
       a.location_code,
       a.location_name,
       a.goods_code,
       b.goods_name,
       b.catg_l_id,
       b.catg_l_name,
       b.dept_id,
       b.dept_name,
       b.catg_m_id,
       b.catg_m_name,
       order_no,
       sdt,
       '' pur_qty_in,
       '' pur_price,
       '' tax_pur_val_in,
       a.sales_qty,
       a.cost_price,
       a.purchase_price,
       a.middle_office_price,
       a.promotion_price,
       a.sales_value,
       datediff(date_sub(current_date,0), concat(substr(a.sdt,1,4),'-',substr(a.sdt,5,2),'-',substr(a.sdt,7,2))) days
from
  (select dc_province_code province_code,
          dc_province_name province_name,
          dc_city_name city_name,
          dc_code location_code,
          dc_name location_name,
          goods_code,
          customer_no,
          order_no,
          sdt,
          sales_qty,
          cost_price,
          purchase_price,
          middle_office_price,
          sales_price as promotion_price,
          sales_value
   from csx_dw.dws_sale_r_d_detail
   where sdt>=${hiveconf:i_sdate_4}
     --and is_factory_goods_code='1' --不含成品
	 and department_code like 'H%'
	 and sales_type<>'fanli'
     and channel_name like'大客户%' --and (order_no not in ('OC200529000043','OC200529000044','OC200529000045','OC200529000046') or order_no is null)
)a
join csx_tmp.temp_price_trend_pur_sale b on b.province_name=a.province_name
and a.city_name=b.city_name
and a.goods_code=b.goods_code
where b.dept_id like 'H%';


 --临时表9：销售前80%或采购入库前80%的商品--采购入库价格、数量金额，销售价格、数量、金额，当天采购数量最大一笔的采购价格
drop table csx_tmp.temp_price_trend_pur_sale1;
create temporary table csx_tmp.temp_price_trend_pur_sale1 as
select a.*,
       b.pur_price_A
from
  (select a.province_code,
          a.province_name,
          a.city_name,
          a.location_code,
          a.location_name,
          a.goods_code,
          a.goods_name,
          a.catg_l_id,
          a.catg_l_name,
          a.dept_id,
          a.dept_name,
          a.catg_m_id,
          a.catg_m_name,
          a.sdt,
          a.days,
          sum(a.pur_qty_in) pur_qty_in,
          sum(a.tax_pur_val_in)/sum(a.pur_qty_in) pur_price,
          sum(a.tax_pur_val_in) tax_pur_val_in,
          sum(a.sales_qty) sales_qty,
          sum(a.sales_qty*cost_price)/sum(a.sales_qty) cost_price, --成本价
          sum(a.sales_qty*purchase_price)/sum(a.sales_qty) purchase_price, --采购价
          sum(a.sales_qty*middle_office_price)/sum(a.sales_qty) middle_office_price, --中台报价
          sum(a.sales_qty*promotion_price)/sum(a.sales_qty) promotion_price, --销售价
          sum(a.sales_value) sales_value
   from --销售价格、数量、金额
     (select province_code,
             province_name,
             city_name,
             location_code,
             location_name,
             goods_code,
             goods_name,
             catg_l_id,
             catg_l_name,
             dept_id,
             dept_name,
             catg_m_id,
             catg_m_name,
             order_no,
             sdt,
             '' pur_qty_in,
             '' pur_price,
             '' tax_pur_val_in,
             sales_qty,
             cost_price,
             purchase_price,
             middle_office_price,
             promotion_price,
             sales_value,
             days
      from csx_tmp.temp_price_trend_sale2
      union all --采购价格、数量、金额
 select province_code,
        province_name,
        city_name,
        location_code,
        location_name,
        goods_code,
        goods_name,
        catg_l_id,
        catg_l_name,
        dept_id,
        dept_name,
        catg_m_id,
        catg_m_name,
        order_no,
        sdt,
        pur_qty_in,
        pur_price,
        tax_pur_val_in,
        '' sales_qty,
        '' cost_price,
        '' purchase_price,
        '' middle_office_price,
        '' promotion_price,
        '' sales_value,
        days
      from csx_tmp.temp_price_trend_pur2)a
   group by a.province_code,
            a.province_name,
            a.city_name,
            a.location_code,
            a.location_name,
            a.goods_code,
            a.goods_name,
            a.catg_l_id,
            a.catg_l_name,
            a.dept_id,
            a.dept_name,
            a.catg_m_id,
            a.catg_m_name,
            a.sdt,
            a.days )a --当天采购数量最大的一笔采购价

left join
  (select province_name,
          city_name,
          location_code,
          location_name,
          goods_code,
          goods_name,
          sdt,
          pur_price pur_price_A
   from csx_tmp.temp_price_trend_pur3
   where pur_rno=1 )b on a.province_name=b.province_name
and a.city_name=b.city_name
and a.location_code=b.location_code
and a.goods_code=b.goods_code
and a.sdt=b.sdt ;


 --结果表1：商品明细（颗粒度到天非订单）
--insert overwrite directory '/tmp/raoyanhua/linshi01' row format delimited fields terminated by '\t'
set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table csx_dw.dws_sale_r_d_price_pur_sale_detail partition (sdt)
select province_code,
       province_name,
       city_name,
       location_code,
       location_name,
       goods_code,
       goods_name,
       catg_l_id,
       catg_l_name,
       dept_id,
       dept_name,
       catg_m_id,
       catg_m_name,
       pur_price_A,
       pur_qty_in,
       pur_price,
       tax_pur_val_in,
       sales_qty,
       cost_price,
       purchase_price,
       middle_office_price,
       promotion_price,
       sales_value,
       days,
       sdt date1,
	   regexp_replace(date_sub(current_date,1),'-','') sdt
from csx_tmp.temp_price_trend_pur_sale1 ;


insert overwrite table csx_dw.dws_sale_r_a_price_trend_pur_sale partition (sdt)
select province_code,
	province_name,
	city_name,
	goods_code,
	goods_name,
	dept_id,
	dept_name,
	catg_l_id,
	catg_l_name,
	catg_m_id,
	catg_m_name,
	sum(pur_qty_in)sum_pur_qty_in,  --总采购数量
	sum(sales_qty)sum_sales_qty,  --总销售数量
	sum(tax_pur_val_in) sum_tax_pur_val_in,--总采购金额
	sum(sales_value) sum_sales_value,--总销售金额
	avg(pur_price_A)pur_price_A,  --当日最大单采购价
	sum(tax_pur_val_in)/sum(pur_qty_in) avg_pur_price,  --采购价
	sum(coalesce(sales_qty*purchase_price,0))/sum(sales_qty) avg_purchase_price,  --采购报价
	sum(coalesce(sales_qty*middle_office_price,0))/sum(sales_qty) avg_middle_office_price,  --中台报价
	sum(coalesce(sales_qty*promotion_price,0))/sum(sales_qty) avg_promotion_price,  --销售价

	avg(case when days=7 then pur_price_A end) pur_price_A_7,
	avg(case when days=6 then pur_price_A end) pur_price_A_6,
	avg(case when days=5 then pur_price_A end) pur_price_A_5,
	avg(case when days=4 then pur_price_A end) pur_price_A_4,
	avg(case when days=3 then pur_price_A end) pur_price_A_3,
	avg(case when days=2 then pur_price_A end) pur_price_A_2,
	avg(case when days=1 then pur_price_A end) pur_price_A_1,

	sum(case when days=7 then coalesce(sales_qty*purchase_price,0) end)
		/sum(case when days=7 then sales_qty end) purchase_price_7,
	sum(case when days=6 then coalesce(sales_qty*purchase_price,0) end)
		/sum(case when days=6 then sales_qty end) purchase_price_6,
	sum(case when days=5 then coalesce(sales_qty*purchase_price,0) end)
		/sum(case when days=5 then sales_qty end) purchase_price_5,
	sum(case when days=4 then coalesce(sales_qty*purchase_price,0) end)
		/sum(case when days=4 then sales_qty end) purchase_price_4,
	sum(case when days=3 then coalesce(sales_qty*purchase_price,0) end)
		/sum(case when days=3 then sales_qty end) purchase_price_3,
	sum(case when days=2 then coalesce(sales_qty*purchase_price,0) end)
		/sum(case when days=2 then sales_qty end) purchase_price_2,
	sum(case when days=1 then coalesce(sales_qty*purchase_price,0) end)
		/sum(case when days=1 then sales_qty end) purchase_price_1,	
			
	sum(case when days=7 then coalesce(sales_qty*middle_office_price,0) end)
		/sum(case when days=7 then sales_qty end) middle_office_price_7,
	sum(case when days=6 then coalesce(sales_qty*middle_office_price,0) end)
		/sum(case when days=6 then sales_qty end) middle_office_price_6,
	sum(case when days=5 then coalesce(sales_qty*middle_office_price,0) end)
		/sum(case when days=5 then sales_qty end) middle_office_price_5,
	sum(case when days=4 then coalesce(sales_qty*middle_office_price,0) end)
		/sum(case when days=4 then sales_qty end) middle_office_price_4,
	sum(case when days=3 then coalesce(sales_qty*middle_office_price,0) end)
		/sum(case when days=3 then sales_qty end) middle_office_price_3,
	sum(case when days=2 then coalesce(sales_qty*middle_office_price,0) end)
		/sum(case when days=2 then sales_qty end) middle_office_price_2,
	sum(case when days=1 then coalesce(sales_qty*middle_office_price,0) end)
		/sum(case when days=1 then sales_qty end) middle_office_price_1,
	
	sum(case when days=7 then coalesce(sales_qty*promotion_price,0) end)
		/sum(case when days=7 then sales_qty end) promotion_price_7,
	sum(case when days=6 then coalesce(sales_qty*promotion_price,0) end)
		/sum(case when days=6 then sales_qty end) promotion_price_6,
	sum(case when days=5 then coalesce(sales_qty*promotion_price,0) end)
		/sum(case when days=5 then sales_qty end) promotion_price_5,
	sum(case when days=4 then coalesce(sales_qty*promotion_price,0) end)
		/sum(case when days=4 then sales_qty end) promotion_price_4,
	sum(case when days=3 then coalesce(sales_qty*promotion_price,0) end)
		/sum(case when days=3 then sales_qty end) promotion_price_3,
	sum(case when days=2 then coalesce(sales_qty*promotion_price,0) end)
		/sum(case when days=2 then sales_qty end) promotion_price_2,
	sum(case when days=1 then coalesce(sales_qty*promotion_price,0) end)
		/sum(case when days=1 then sales_qty end) promotion_price_1,
	regexp_replace(date_sub(current_date,1),'-','') sdt
from csx_tmp.temp_price_trend_pur_sale1
group by province_code,province_name,city_name,goods_code,goods_name,dept_id,dept_name,catg_l_id,catg_l_name,catg_m_id,catg_m_name,
regexp_replace(date_sub(current_date,1),'-','')
;
