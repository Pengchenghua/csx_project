--1、指定客户首次成交时间
select customer_no,first_order_date,substr(first_order_date,1,6) first_order_month
from csx_dw.dws_crm_w_a_customer_active
where sdt = 'current'
and customer_no in('104402','104885','102534','PF1205','102633','104086','116401','106287','104493','104901','113758',
                   '106898','113439','115732','105186','111137','107428','105446','105164','105156','102755');




--2、省区TOPsku（指定）的20年、21年1-6月不含税销售额与毛利 自营日配
select 
  province_name,city_group_name,goods_code,
  sum(case when substr(sdt,1,4)='2020' then excluding_tax_sales end) 2020_excluding_tax_sales,
  sum(case when substr(sdt,1,4)='2020' then excluding_tax_profit end) 2020_excluding_tax_profit,
  sum(case when substr(sdt,1,4)='2021' then excluding_tax_sales end) 2021_excluding_tax_sales,
  sum(case when substr(sdt,1,4)='2021' then excluding_tax_profit end) 2021_excluding_tax_profit  
from csx_dw.dws_sale_r_d_detail
where sdt>='20200101'
and sdt<'20210701'
and channel_code in('1','7','9')
and business_type_code ='1'
and city_group_name in('合肥市','福州市','成都市')
and goods_code in(
    '816','1057928','1057910','1273735','1135271','317160','576','1057922','919062','531',
    '860131','1279463','977399','1278956','873565','1278946','1279120','1279289','977468','1279277',
    '1286220','852426','1294856','852410','1278941','852436','852443','837472','858325','1279445')
group by province_name,city_group_name,goods_code;

