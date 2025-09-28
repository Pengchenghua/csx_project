select case when channel_code in('1','9') then 'B端' 
            when channel_code ='2' then 'M端' 
			when channel_code ='7' then 'BBC' 
			when channel_code ='4' then '大宗' 
			when channel_code in('5','6') then '供应链' end channel_name,
  region_name,province_name,city_group_name,business_type_name, 
  sum(sales_value_1) sales_value_1,
  sum(sales_cost_1) sales_cost_1,
  sum(adjust_cost) adjust_cost,
  sum(sales_value) sales_value,
  sum(sales_cost) sales_cost 
from 
(
  select a.*,b.business_type_name,b.channel_code,b.channel_name,b.region_name,b.province_name,b.city_group_name,
    b.sales_value,b.sales_cost
  from
  (
  select order_no,goods_code,sales_value sales_value_1,sales_cost sales_cost_1,adjust_cost
  from csx_tmp.tmp_mms_r_a_abnormal_order
  )a
  left join
  (
  select distinct split(id, '&')[0] as credential_no,order_no,goods_code,
  business_type_name,channel_code,channel_name,region_name,province_name,city_group_name,
  dc_code,dc_name,customer_no,customer_name,
  sales_value,sales_cost
  from csx_dw.dws_sale_r_d_detail
  where sdt>='20210701'
  )b on a.order_no=b.order_no and a.goods_code=b.goods_code
)a
group by case when channel_code in('1','9') then 'B端' 
            when channel_code ='2' then 'M端' 
			when channel_code ='7' then 'BBC' 
			when channel_code ='4' then '大宗' 
			when channel_code in('5','6') then '供应链' end,
  region_name,province_name,city_group_name,business_type_name; 
  
  
select sum(adjust_cost) adjust_cost
from csx_tmp.tmp_mms_r_a_abnormal_order; 
  