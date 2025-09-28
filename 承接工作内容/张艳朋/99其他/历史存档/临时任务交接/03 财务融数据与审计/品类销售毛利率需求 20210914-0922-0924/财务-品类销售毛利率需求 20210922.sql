--二级猪肉销售毛利率202001-202108、一级蔬菜水果毛利率202001-202108、一级蔬菜水果每月前10SKU毛利率202001-202108 20210922
insert overwrite directory '/tmp/raoyanhua/linshi01' row format delimited fields terminated by '\t'
select
  a.province_name,a.city_group_name,b.classify_large_code,b.classify_large_name,
  b.classify_middle_code,b.classify_middle_name,b.classify_small_code,b.classify_small_name,a.smonth,
  sum(a.excluding_tax_sales) excluding_tax_sales,
  sum(a.excluding_tax_profit) excluding_tax_profit  
from
  (
    select 
      province_name,city_group_name,goods_code,substr(sdt,1,6) smonth,
      sum(excluding_tax_sales) excluding_tax_sales,
      sum(excluding_tax_profit) excluding_tax_profit
    from csx_dw.dws_sale_r_d_detail
    where sdt>='20200101'
	and sdt<'20210901'
	and channel_code in('1','7','9')
    and business_type_code ='1'
    group by province_name,city_group_name,goods_code,substr(sdt,1,6)
  )a
 join 
  (
    select goods_id,goods_name,classify_large_code,classify_large_name,classify_middle_code,classify_middle_name,classify_small_code,classify_small_name
    from csx_dw.dws_basic_w_a_csx_product_m where sdt = 'current'
  )b on a.goods_code=b.goods_id
where classify_middle_name='猪肉'
group by 
  a.province_name,a.city_group_name,b.classify_large_code,b.classify_large_name,
  b.classify_middle_code,b.classify_middle_name,b.classify_small_code,b.classify_small_name,a.smonth;

insert overwrite directory '/tmp/raoyanhua/linshi02' row format delimited fields terminated by '\t'
select
  a.province_name,a.city_group_name,b.classify_large_code,b.classify_large_name,
  b.classify_middle_code,b.classify_middle_name,b.classify_small_code,b.classify_small_name,a.smonth,
  sum(a.excluding_tax_sales) excluding_tax_sales,
  sum(a.excluding_tax_profit) excluding_tax_profit 
from
  (
    select 
      province_name,city_group_name,goods_code,substr(sdt,1,6) smonth,
      sum(excluding_tax_sales) excluding_tax_sales,
      sum(excluding_tax_profit) excluding_tax_profit
    from csx_dw.dws_sale_r_d_detail
    where sdt>='20200101'
	and sdt<'20210901'
	and channel_code in('1','7','9')
    and business_type_code ='1'
    group by province_name,city_group_name,goods_code,substr(sdt,1,6)
  )a
 join 
  (
    select goods_id,goods_name,classify_large_code,classify_large_name,classify_middle_code,classify_middle_name,classify_small_code,classify_small_name
    from csx_dw.dws_basic_w_a_csx_product_m where sdt = 'current'
  )b on a.goods_code=b.goods_id
  --B02 蔬菜水果
where classify_large_code='B02'
group by 
  a.province_name,a.city_group_name,b.classify_large_code,b.classify_large_name,
  b.classify_middle_code,b.classify_middle_name,b.classify_small_code,b.classify_small_name,a.smonth;


insert overwrite directory '/tmp/raoyanhua/linshi03' row format delimited fields terminated by '\t'
select b.*,a.excluding_tax_sales_all,a.excluding_tax_profit_all
from 
(
select
  a.province_name,a.city_group_name,b.classify_large_code,b.classify_large_name,
  --b.classify_middle_code,b.classify_middle_name,b.classify_small_code,b.classify_small_name,
  a.smonth,
  sum(a.excluding_tax_sales) excluding_tax_sales_all,
  sum(a.excluding_tax_profit) excluding_tax_profit_all
from
  (
    select 
      province_name,city_group_name,goods_code,substr(sdt,1,6) smonth,
      sum(excluding_tax_sales) excluding_tax_sales,
      sum(excluding_tax_profit) excluding_tax_profit
    from csx_dw.dws_sale_r_d_detail
    where sdt>='20200101'
	and sdt<'20210901'
	and channel_code in('1','7','9')
    and business_type_code ='1'
    group by province_name,city_group_name,goods_code,substr(sdt,1,6)
  )a
 join 
  (
    select goods_id,goods_name,classify_large_code,classify_large_name,classify_middle_code,classify_middle_name,classify_small_code,classify_small_name
    from csx_dw.dws_basic_w_a_csx_product_m where sdt = 'current'
  )b on a.goods_code=b.goods_id  
  --B02 蔬菜水果
where classify_large_code='B02'
group by 
  a.province_name,a.city_group_name,b.classify_large_code,b.classify_large_name,
  --b.classify_middle_code,b.classify_middle_name,b.classify_small_code,b.classify_small_name,
  a.smonth
)a 
left join
(
select
  a.province_name,a.city_group_name,b.classify_large_code,b.classify_large_name,
  b.classify_middle_code,b.classify_middle_name,b.classify_small_code,b.classify_small_name,
  a.goods_code,b.goods_name,a.smonth,a.excluding_tax_sales,a.excluding_tax_profit,a.rno
from
  (
    select 
      province_name,city_group_name,classify_large_code,goods_code,substr(sdt,1,6) smonth,
      sum(excluding_tax_sales) excluding_tax_sales,
      sum(excluding_tax_profit) excluding_tax_profit,
	  row_number() over(partition by province_name,city_group_name,classify_large_code,substr(sdt,1,6) order by sum(excluding_tax_sales) desc) rno
    from csx_dw.dws_sale_r_d_detail
    where sdt>='20200101'
	and sdt<'20210901'
	and channel_code in('1','7','9')
    and business_type_code ='1'
    group by province_name,city_group_name,classify_large_code,goods_code,substr(sdt,1,6)
  )a
 join 
  (
    select goods_id,goods_name,classify_large_code,classify_large_name,classify_middle_code,classify_middle_name,classify_small_code,classify_small_name
    from csx_dw.dws_basic_w_a_csx_product_m where sdt = 'current'
  )b on a.goods_code=b.goods_id  
  --B02 蔬菜水果
where b.classify_large_code='B02'
)b on a.province_name=b.province_name and a.city_group_name=b.city_group_name and a.smonth=b.smonth and a.classify_large_code=b.classify_large_code
where b.rno<=10;