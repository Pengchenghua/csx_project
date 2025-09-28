

insert overwrite directory '/tmp/hukaimin/price_202005' row format delimited fields terminated by '\t' 
SELECT 
* 
FROM 
(
select
 substr(sdt,1,6)smonth,
 customer_no AAA,
 attribute,
 channel_name,
 customer_name,
 child_customer_no,
 regexp_replace(child_customer_name, '\n|\t|\r', ''),
 sales_name,
 supervisor_name,
 sales_province,
 sales_city,
 order_no,
 sales_date,
 perform_dc_code,
 perform_dc_name,
 dc_code,
 dc_name,
 case  when order_kind='NORMAL' then '普通单'
  when order_kind='WELFARE' then '福利单'
  end as order_kind,  --订单类型
 division_code,
 division_name,
 department_code,
 department_name,
 category_middle_code,
 category_middle_name,
 goods_code,
 goods_name,
 if (self_product_name is not null and self_product_name<>'','是','否') as self_product_name, --是否自建商品
 promotion_price as promotion_price,  --促销价格
 sales_qty as sales_qty, --销售数量 
 case when order_mode ='0' then '配送' 
  when  order_mode ='1' then '直送' 
  when  order_mode ='2' then '自提' 
  when  order_mode ='3' then '直通' 
  end  as order_mode,  --配送类型
 purchase_price,
 middle_office_price,
 cost_price as price
-- h1.promotion_cost_price
from 
  csx_dw.dws_sale_r_d_customer_sale  
where sdt>='20200530' and sdt<='20200605'
 and report_price= '1' 
 AND attribute_code != '5'
 AND channel_name LIKE '大客户%'
 AND sales_type in ('qyg','sapqyg','sapgc','sc','bbc')
 ) A left join 
(select 
     distinct customer_no,
     substr(sdt,1,6)smonth 
from csx_dw.csx_partner_list 
) d on d.customer_no=A.AAA and d.smonth=A.smonth
where d.customer_no is null


;
