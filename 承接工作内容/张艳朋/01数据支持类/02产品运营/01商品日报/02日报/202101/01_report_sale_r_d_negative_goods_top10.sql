-- 商品简报 负毛利top10商品
-- 核心逻辑： 统计省区城市负毛利top10商品


-- 筛选负毛利商品
with negative_sale as 
(
  select 
  	province_name, 
  	city_group_name,
  	case when division_code in ('10', '11') then '生鲜'
  	  when division_code in ('12') then '食品'
  	  else '非食品' end as division_type,					-- 部类分组
  	department_name,										-- 课组
    goods_code,
    regexp_replace(goods_name, '\n|\t|\r', '') as goods_name,
    sum(sales_value) as total_sales_value,					-- 总销售额
    sum(profit) as total_profit,							-- 总毛利
    sum(profit) / abs(sum(sales_value)) as total_profit_prorate,		-- 总毛利率
    sum(sales_qty) as total_sales_qty,							-- 总销售数量
    count(distinct customer_no) as customer_amount,			-- 总客户数
    row_number() over(partition by city_group_name order by sum(profit) asc) as city_profit_no  -- 城市商品负毛利排名
  from csx_dw.dws_sale_r_d_detail
  where sdt >= regexp_replace(date_sub(current_date,if(pmod(datediff(current_date, '2020-04-06'), 7) = 0, 3, 1) ),'-','') -- 周一跑三天数据 其它时间跑前一天数据
    and channel_code in ('1','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
	and business_type_code !=4 -- 业务类型编码(1.日配单 2.福利单 3.批发内购 4.城市服务商 5.省区大宗 6.BBC)
  group by province_name, city_group_name, 
    case when division_code in ('10', '11') then '生鲜'
  	  when division_code in ('12') then '食品'
  	  else '非食品' end,
  	department_name,								
    goods_code,
    regexp_replace(goods_name, '\n|\t|\r', '')
  having sum(sales_value) > 0 and sum(profit) < 0
),
goods_negative_days as 
(
  select 
    city_group_name,
    goods_code,
    count(distinct sdt) as sale_days -- 负毛利销售天数
  from 
  (
    select 
      city_group_name,
      goods_code,
      sdt,
      sum(profit) as total_profit
    from csx_dw.dws_sale_r_d_detail
    where sdt >= regexp_replace(trunc(date_sub(current_date, 1), 'MM'), '-', '') and sdt <= regexp_replace(date_sub(current_date, 1), '-', '')
    and channel_code in ('1','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
	and business_type_code !=4 -- 业务类型编码(1.日配单 2.福利单 3.批发内购 4.城市服务商 5.省区大宗 6.BBC)
    group by city_group_name, goods_code, sdt
    having sum(profit) < 0
  )t1 
  group by city_group_name, goods_code
)

select 
  t1.province_name,
  t1.city_group_name,
  t1.division_type,
  t1.department_name,
  t1.goods_code,
  t1.goods_name,
  t1.total_sales_value,
  t1.total_profit,
  t1.total_profit_prorate,
  t1.total_sales_qty,
  t1.customer_amount,
  t1.city_profit_no,
  t2.sale_days
from negative_sale t1 left outer join goods_negative_days t2 
on t1.city_group_name = t2.city_group_name and t1.goods_code = t2.goods_code
where t1.city_profit_no <= 10;