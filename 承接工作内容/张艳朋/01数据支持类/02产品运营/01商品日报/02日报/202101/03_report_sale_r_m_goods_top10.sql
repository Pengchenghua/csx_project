-- 商品简报 销售额top10信息
-- 核心逻辑： 统计省区城市top10商品信息

with goods_sales as 
(
  select 
    concat(city_group_name, row_number() over(partition by city_group_name order by sum(sales_value) desc)) as id,
    province_name,
    city_group_name,
    goods_code,
    goods_name,
    sum(sales_value) as sales_value,
    sum(profit) as profit,
    count(distinct customer_no) as customer_amount,
    -- count(distinct customer_no) / count(distinct customer_no) over(partition by city_group_name) as customer_amount_prorate,
    row_number() over(partition by city_group_name order by sum(sales_value) desc) as goods_sale_no
  from csx_dw.dws_sale_r_d_detail
  where sdt >= regexp_replace(trunc(date_sub(current_date, 1), 'MM'), '-', '') and sdt <= regexp_replace(date_sub(current_date, 1), '-', '') 
    and channel_code in ('1','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
	and business_type_code !=4 -- 业务类型编码(1.日配单 2.福利单 3.批发内购 4.城市服务商 5.省区大宗 6.BBC)
  group by province_name, city_group_name, goods_code, goods_name
)
select 
  *
from goods_sales 
where goods_sale_no <= 10;