-- 商品简报 销售额top10信息
-- 核心逻辑： 统计省区城市top10商品信息

-- 计算日期
-- 周一跑三天数据 其它时间跑前一天数据

with goods_sale_rank as 
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
    row_number() over(partition by city_group_name order by sum(sales_value) desc) as city_sale_no  -- 城市商品负毛利排名
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
)

select 
  *
from goods_sale_rank 
where city_sale_no <= 10;