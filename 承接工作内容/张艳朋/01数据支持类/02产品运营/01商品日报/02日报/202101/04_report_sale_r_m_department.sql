-- 商品简报 课组统计
-- 核心逻辑： 商品课组销售统计

with goods_sale_department as
(
    select 
      city_group_name,
      department_name,										-- 课组
      sum(sales_value) as total_sales_value,					-- 总销售额
      sum(profit) total_profit,	-- 总毛利额
	  count(distinct customer_no) as customer_cnt,
	  count(distinct sdt) as days_cnt,
	  count(distinct goods_code) as goods_cnt
    from csx_dw.dws_sale_r_d_detail
    where sdt >= regexp_replace(trunc(date_sub(current_date, 1), 'MM'), '-', '') and sdt <= regexp_replace(date_sub(current_date, 1), '-', '')
      and channel_code in ('1','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
      and business_type_code !=4 -- 业务类型编码(1.日配单 2.福利单 3.批发内购 4.城市服务商 5.省区大宗 6.BBC)
    group by city_group_name, 
    	department_name
)

select 
  *
from goods_sale_department 