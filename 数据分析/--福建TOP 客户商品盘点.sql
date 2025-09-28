--福建TOP 客户商品盘点
-- 新增 236479	深圳市育新学校 236481	深圳市第二职业技术学校 236484	深圳大学附属实验中学 '236479','236481','236484'
--  ----------------------------------------------------------------
-- 近4个月客户商品数据
drop table if exists csx_analyse_tmp.tmp_hn;
create table if not exists csx_analyse_tmp.tmp_hn as 
select 
	substr(a.sdt,1,6) as sdt_month,
	a.performance_region_name,
	a.performance_province_name,
	a.performance_city_name,
	c.classify_large_name,
	c.classify_middle_name,
	c.classify_small_name,
	a.customer_code,
	d.customer_name,
	a.goods_code,
	c.goods_name,
    sum(sale_amt) as month_sale_amt,
    sum(profit) as month_profit,
    sum(sale_qty) as month_sale_qty,
    sum(case when order_channel_code not in ('4','6') and delivery_type_code<>2 and refund_order_flag<>1 and inventory_dc_code not in ('W0J2','W0AJ','W0G6','WB71') then sale_amt end) as normal_month_sale_amt,
    sum(case when order_channel_code not in ('4','6') and delivery_type_code<>2 and refund_order_flag<>1 and inventory_dc_code not in ('W0J2','W0AJ','W0G6','WB71') then profit end) as normal_month_profit,
    sum(case when order_channel_code not in ('4','6') and delivery_type_code<>2 and refund_order_flag<>1 and inventory_dc_code not in ('W0J2','W0AJ','W0G6','WB71') then sale_qty end) as normal_month_sale_qty  
from 
(select * 
from csx_analyse.csx_analyse_bi_sale_detail_di 
where sdt >= '20231101' and sdt <= regexp_replace(add_months(date_sub(current_date,1),0),'-','') 
    and channel_code <> '2' and substr(customer_code, 1, 1) <> 'S' -- 剔除商超数据 
    and business_type_code in ('1') 
    and performance_region_name='华南大区'
    -- and performance_city_name in ('石家庄市') 
) a
left join 
(select  distinct shop_code 
from csx_dim.csx_dim_shop 
where sdt='current' and shop_low_profit_flag=1  
) b 
on a.inventory_dc_code = b.shop_code 
left join 
(select * 
from csx_dim.csx_dim_basic_goods 
where sdt='current'
) c 
on a.goods_code=c.goods_code 
left join 
(select * 
from csx_dim.csx_dim_crm_customer_info 
where sdt='current'	 
) d 
on a.customer_code=d.customer_code 
where b.shop_code is null 
group by 
	substr(a.sdt,1,6),
	a.performance_region_name,
	a.performance_province_name,
	a.performance_city_name,
	c.classify_large_name,
	c.classify_middle_name,
	c.classify_small_name,
	a.customer_code,
	d.customer_name,
	a.goods_code,
	c.goods_name
;


-- ------------------------------------------------------------------
-- 最终数据
select 
	a.performance_region_name as `大区`,
	a.performance_province_name as `省区`,
	a.performance_city_name as `城市`,
	a.customer_code as `客户编码`,
	a.customer_name as `客户名称`,
	price_set_type_first `定价类型`,
    price_set_type_sec  `定价类型2`,
  --   price_type_name `定价类型`,
     price_period_name `报价周期`,
     price_date_name `报价日期`,
	a.classify_middle_name as `管理中类`,
	a.goods_code as `商品编码`,
	a.goods_name as `商品名称`,

	a.11_month_sale_amt/a.cus_cla_11_month_sale_amt as `销售额占比-品类_M11`,
	a.12_month_sale_amt/a.cus_cla_12_month_sale_amt as `销售额占比-品类_M12`,
	a.1_month_sale_amt/a.cus_cla_1_month_sale_amt as `销售额占比-品类_M1`,
	a.2_month_sale_amt/a.cus_cla_2_month_sale_amt as `销售额占比-品类_M2`,

	a.11_month_profit/a.11_month_sale_amt as `毛利率_M11`,
	a.12_month_profit/a.12_month_sale_amt as `毛利率_M12`,
	a.1_month_profit/a.1_month_sale_amt as `毛利率_M1`,
	a.2_month_profit/a.2_month_sale_amt as `毛利率_M2`,

	b.11_month_profitlv as `重点客户毛利率_M11`,
	b.12_month_profitlv as `重点客户毛利率_M12`,
	b.1_month_profitlv as `重点客户毛利率_M1`,
	b.2_month_profitlv as `重点客户毛利率_M2`,

	c.11_month_profitlv as `省区毛利率_M11`,
	c.12_month_profitlv as `省区毛利率_M12`,
	c.1_month_profitlv as `省区毛利率_M1`,
	c.2_month_profitlv as `省区毛利率_M2`,

	a.11_month_normal_cost as `正常平均成本_M11`,
	a.12_month_normal_cost as `正常平均成本_M12`,
	a.1_month_normal_cost as `正常平均成本_M1`,
	a.2_month_normal_cost as `正常平均成本_M2`,
	a.11_month_normal_price as `正常平均售价_M11`,
	a.12_month_normal_price as `正常平均售价_M12`,
	a.1_month_normal_price as `正常平均售价_M1`,
	a.2_month_normal_price as `正常平均售价_M2`  

from 
(select 
	a1.*,
	sum(11_month_sale_amt)over(partition by customer_code,classify_middle_name) as cus_cla_11_month_sale_amt, -- 求品类销售额占比
	sum(12_month_sale_amt)over(partition by customer_code,classify_middle_name) as cus_cla_12_month_sale_amt, -- 求品类销售额占比
	sum(1_month_sale_amt)over(partition by customer_code,classify_middle_name) as cus_cla_1_month_sale_amt, -- 求品类销售额占比
	sum(2_month_sale_amt)over(partition by customer_code,classify_middle_name) as cus_cla_2_month_sale_amt  
 from 
	(select 
		performance_region_name,
		performance_province_name,
		performance_city_name,
		customer_code,
		customer_name,
		classify_middle_name,
		goods_code,
		goods_name,

		nvl(sum(case when sdt_month='202311' then month_sale_amt end),0) as 11_month_sale_amt,
		nvl(sum(case when sdt_month='202312' then month_sale_amt end),0) as 12_month_sale_amt,
		nvl(sum(case when sdt_month='202401' then month_sale_amt end),0) as 1_month_sale_amt,
		nvl(sum(case when sdt_month='202402' then month_sale_amt end),0) as 2_month_sale_amt,

		nvl(sum(case when sdt_month='202311' then month_profit end),0) as 11_month_profit,
		nvl(sum(case when sdt_month='202312' then month_profit end),0) as 12_month_profit,
		nvl(sum(case when sdt_month='202401' then month_profit end),0) as 1_month_profit,
		nvl(sum(case when sdt_month='202402' then month_profit end),0) as 2_month_profit,


		nvl(sum(case when sdt_month='202311' then month_sale_qty end),0) as 11_month_sale_qty,
		nvl(sum(case when sdt_month='202312' then month_sale_qty end),0) as 12_month_sale_qty,
		nvl(sum(case when sdt_month='202401' then month_sale_qty end),0) as 1_month_sale_qty,
		nvl(sum(case when sdt_month='202402' then month_sale_qty end),0) as 2_month_sale_qty,

		nvl(sum(case when sdt_month='202311' then normal_month_sale_amt-normal_month_profit end),0)/nvl(sum(case when sdt_month='202311' then normal_month_sale_qty end),0) as 11_month_normal_cost,
		nvl(sum(case when sdt_month='202312' then normal_month_sale_amt-normal_month_profit end),0)/nvl(sum(case when sdt_month='202312' then normal_month_sale_qty end),0) as 12_month_normal_cost,
		nvl(sum(case when sdt_month='202401' then normal_month_sale_amt-normal_month_profit end),0)/nvl(sum(case when sdt_month='202401' then normal_month_sale_qty end),0) as 1_month_normal_cost,
		nvl(sum(case when sdt_month='202402' then normal_month_sale_amt-normal_month_profit end),0)/nvl(sum(case when sdt_month='202402' then normal_month_sale_qty end),0) as 2_month_normal_cost ,
        nvl(sum(case when sdt_month='202311' then normal_month_sale_amt end),0)/nvl(sum(case when sdt_month='202311' then normal_month_sale_qty end),0) as 11_month_normal_price,
		nvl(sum(case when sdt_month='202312' then normal_month_sale_amt end),0)/nvl(sum(case when sdt_month='202312' then normal_month_sale_qty end),0) as 12_month_normal_price,
		nvl(sum(case when sdt_month='202401' then normal_month_sale_amt end),0)/nvl(sum(case when sdt_month='202401' then normal_month_sale_qty end),0) as 1_month_normal_price,
		nvl(sum(case when sdt_month='202402' then normal_month_sale_amt end),0)/nvl(sum(case when sdt_month='202402' then normal_month_sale_qty end),0) as 2_month_normal_price
	from csx_analyse_tmp.tmp_hn
	where customer_code in ('110807','155386','128371','235130','195996','236479','236481','236484') -- 后面3个新增教育客户
	group by 
		performance_region_name,
		performance_province_name,
		performance_city_name,
		customer_code,
		customer_name,
		classify_middle_name,
		goods_code,
		goods_name 
	) a1 
) a 
left join 
-- TOP客户整体毛利率
(select 
	performance_region_name,
	performance_province_name,
	performance_city_name,
	classify_middle_name,
	goods_code,
	nvl(sum(case when sdt_month='202311' then month_profit end),0)/nvl(sum(case when sdt_month='202311' then month_sale_amt end),0) as 11_month_profitlv,
	nvl(sum(case when sdt_month='202312' then month_profit end),0)/nvl(sum(case when sdt_month='202312' then month_sale_amt end),0) as 12_month_profitlv,
	nvl(sum(case when sdt_month='202401' then month_profit end),0)/nvl(sum(case when sdt_month='202401' then month_sale_amt end),0) as 1_month_profitlv,
	nvl(sum(case when sdt_month='202402' then month_profit end),0)/nvl(sum(case when sdt_month='202402' then month_sale_amt end),0) as 2_month_profitlv    
from csx_analyse_tmp.tmp_hn
where customer_code in ('110807','155386','128371','235130','195996','236479','236481','236484') 
group by 
	performance_region_name,
	performance_province_name,
	performance_city_name,
	classify_middle_name,
	goods_code 
) b 
on a.performance_region_name=b.performance_region_name and a.performance_province_name=b.performance_province_name and a.performance_city_name=b.performance_city_name and a.classify_middle_name=b.classify_middle_name and a.goods_code=b.goods_code 
left join 
-- 省区整体毛利率 
(select 
	performance_region_name,
	performance_province_name,
	performance_city_name,
	classify_middle_name,
	goods_code,
	nvl(sum(case when sdt_month='202311' then month_profit end),0)/nvl(sum(case when sdt_month='202311' then month_sale_amt end),0) as 11_month_profitlv,
	nvl(sum(case when sdt_month='202312' then month_profit end),0)/nvl(sum(case when sdt_month='202312' then month_sale_amt end),0) as 12_month_profitlv,
	nvl(sum(case when sdt_month='202401' then month_profit end),0)/nvl(sum(case when sdt_month='202401' then month_sale_amt end),0) as 1_month_profitlv,
	nvl(sum(case when sdt_month='202402' then month_profit end),0)/nvl(sum(case when sdt_month='202402' then month_sale_amt end),0) as 2_month_profitlv    
from csx_analyse_tmp.tmp_hn
group by 
	performance_region_name,
	performance_province_name,
	performance_city_name,
	classify_middle_name,
	goods_code  
) c 
on a.performance_region_name=c.performance_region_name and a.performance_province_name=c.performance_province_name and a.performance_city_name=c.performance_city_name and a.classify_middle_name=c.classify_middle_name and a.goods_code=c.goods_code 
left join 
(select * from 
(select 
       customer_code, price_type_name,price_period_name,price_date_name,business_attribute_name
        ,
        split(price_set_type,',')[0],
        case when split(price_set_type,',')[0] ='1' then '对标定价' 
              when split(price_set_type,',')[0]='4' then '客户定价' 
              when split(price_set_type,',')[0]='8' then '自主定价' 
              when split(price_set_type,',')[0]='11'  then '临时报价' 
              when split(price_set_type,',')[0]='2' then '全对标' 
              when split(price_set_type,',')[0]='3' then '半对标' 
              when split(price_set_type,',')[0]='5' then '合同固定价' 
              when split(price_set_type,',')[0]='6' then '多方比价' 
              when split(price_set_type,',')[0]='7' then '单独议价' 
              when split(price_set_type,',')[0]='9' then '采购或车间定价' 
              when split(price_set_type,',')[0]='10'  then '建议售价' 
              when split(price_set_type,',')[0]='12'  then '下单时' 
              when split(price_set_type,',')[0]='13'  then '单品项' 
        end as price_set_type_first,
        (case when split(price_set_type,',')[1]='1' then '对标定价' 
              when split(price_set_type,',')[1]='4' then '客户定价' 
              when split(price_set_type,',')[1]='8' then '自主定价' 
              when split(price_set_type,',')[1]='11' then '临时报价' 
              when split(price_set_type,',')[1]='2' then '全对标' 
              when split(price_set_type,',')[1]='3' then '半对标' 
              when split(price_set_type,',')[1]='5' then '合同固定价' 
              when split(price_set_type,',')[1]='6' then '多方比价' 
              when split(price_set_type,',')[1]='7' then '单独议价' 
              when split(price_set_type,',')[1]='9' then '采购或车间定价' 
              when split(price_set_type,',')[1]='10' then '建议售价' 
              when split(price_set_type,',')[1]='12' then '下单时' 
              when split(price_set_type,',')[1]='13' then '单品项' 
        end) as price_set_type_sec, 
        row_number()over(partition by customer_code order by business_number desc) as pm 
    from csx_dim.csx_dim_crm_business_info 
    where sdt='current' 
)a where pm=1) d on a.customer_code=d.customer_code


-- 以周为单位 
-- 近4个周客户商品数据
drop table if exists csx_analyse_tmp.tmp_hn;
create table if not exists csx_analyse_tmp.tmp_hn as 
select 
	-- substr(a.sdt,1,6) as sdt_month,
	csx_week,
	a.performance_region_name,
	a.performance_province_name,
	a.performance_city_name,
	c.classify_large_name,
	c.classify_middle_name,
	c.classify_small_name,
	a.customer_code,
	d.customer_name,
	a.goods_code,
	c.goods_name,
	 dense_rank()over(order by csx_week desc ) week_rn, 
    sum(sale_amt) as month_sale_amt,
    sum(profit) as month_profit,
    sum(sale_qty) as month_sale_qty,
    sum(case when order_channel_code not in ('4','6') and delivery_type_code<>2 and refund_order_flag<>1 and inventory_dc_code not in ('W0J2','W0AJ','W0G6','WB71') then sale_amt end) as normal_month_sale_amt,
    sum(case when order_channel_code not in ('4','6') and delivery_type_code<>2 and refund_order_flag<>1 and inventory_dc_code not in ('W0J2','W0AJ','W0G6','WB71') then profit end) as normal_month_profit,
    sum(case when order_channel_code not in ('4','6') and delivery_type_code<>2 and refund_order_flag<>1 and inventory_dc_code not in ('W0J2','W0AJ','W0G6','WB71') then sale_qty end) as normal_month_sale_qty  
from 
(select a.* ,b.csx_week
from csx_dws.csx_dws_sale_detail_di a 
left join 
(select calday,csx_week from csx_dim.csx_dim_basic_date) b on a.sdt=b.calday
where sdt >= '20240101' and sdt <= regexp_replace(add_months(date_sub(current_date,1),0),'-','') 
    and channel_code <> '2' and substr(customer_code, 1, 1) <> 'S' -- 剔除商超数据 
    and business_type_code in ('1') 
    and performance_region_name='华南大区'
   -- and customer_code='103719'
    -- and performance_city_name in ('石家庄市')
    ) a
left join 
(select  distinct shop_code 
from csx_dim.csx_dim_shop 
where sdt='current' and shop_low_profit_flag=1  
) b 
on a.inventory_dc_code = b.shop_code 
left join 
(select * 
from csx_dim.csx_dim_basic_goods 
where sdt='current'
) c 
on a.goods_code=c.goods_code 
left join 
(select * 
from csx_dim.csx_dim_crm_customer_info 
where sdt='current'	 
) d 
on a.customer_code=d.customer_code 
where b.shop_code is null 
group by 
	csx_week,
	a.performance_region_name,
	a.performance_province_name,
	a.performance_city_name,
	c.classify_large_name,
	c.classify_middle_name,
	c.classify_small_name,
	a.customer_code,
	d.customer_name,
	a.goods_code,
	c.goods_name
;


-- 客户侧 
-- 最终数据
select 
	a.performance_region_name as `大区`,
	a.performance_province_name as `省区`,
	a.performance_city_name as `城市`,
	a.customer_code as `客户编码`,
	a.customer_name as `客户名称`,
	price_set_type_first `定价类型`,
    price_set_type_sec  `定价类型2`,
  --   price_type_name `定价类型`,
     price_period_name `报价周期`,
     price_date_name `报价日期`,
	a.classify_middle_name as `管理中类`,
	a.goods_code as `商品编码`,
	a.goods_name as `商品名称`,

	a.11_month_sale_amt/a.cus_cla_11_month_sale_amt as `销售额占比-品类_M11`,
	a.12_month_sale_amt/a.cus_cla_12_month_sale_amt as `销售额占比-品类_M12`,
	a.1_month_sale_amt/a.cus_cla_1_month_sale_amt as `销售额占比-品类_M1`,
	a.2_month_sale_amt/a.cus_cla_2_month_sale_amt as `销售额占比-品类_M2`,

	a.11_month_profit/a.11_month_sale_amt as `毛利率_M11`,
	a.12_month_profit/a.12_month_sale_amt as `毛利率_M12`,
	a.1_month_profit/a.1_month_sale_amt as `毛利率_M1`,
	a.2_month_profit/a.2_month_sale_amt as `毛利率_M2`,

	b.11_month_profitlv as `重点客户毛利率_M11`,
	b.12_month_profitlv as `重点客户毛利率_M12`,
	b.1_month_profitlv as `重点客户毛利率_M1`,
	b.2_month_profitlv as `重点客户毛利率_M2`,

	c.11_month_profitlv as `省区毛利率_M11`,
	c.12_month_profitlv as `省区毛利率_M12`,
	c.1_month_profitlv as `省区毛利率_M1`,
	c.2_month_profitlv as `省区毛利率_M2`,

	a.11_month_normal_cost as `正常平均成本_M11`,
	a.12_month_normal_cost as `正常平均成本_M12`,
	a.1_month_normal_cost as `正常平均成本_M1`,
	a.2_month_normal_cost as `正常平均成本_M2`,
	a.11_month_normal_price as `正常平均售价_M11`,
	a.12_month_normal_price as `正常平均售价_M12`,
	a.1_month_normal_price as `正常平均售价_M1`,
	a.2_month_normal_price as `正常平均售价_M2`  

from 
(select 
	a1.*,
	sum(11_month_sale_amt)over(partition by customer_code,classify_middle_name) as cus_cla_11_month_sale_amt, -- 求品类销售额占比
	sum(12_month_sale_amt)over(partition by customer_code,classify_middle_name) as cus_cla_12_month_sale_amt, -- 求品类销售额占比
	sum(1_month_sale_amt)over(partition by customer_code,classify_middle_name) as cus_cla_1_month_sale_amt, -- 求品类销售额占比
	sum(2_month_sale_amt)over(partition by customer_code,classify_middle_name) as cus_cla_2_month_sale_amt  
 from 
	(select 
		performance_region_name,
		performance_province_name,
		performance_city_name,
		customer_code,
		customer_name,
		classify_middle_name,
		goods_code,
		goods_name,

		nvl(sum(case when week_rn='4' then month_sale_amt end),0) as 11_month_sale_amt,
		nvl(sum(case when week_rn='3' then month_sale_amt end),0) as 12_month_sale_amt,
		nvl(sum(case when week_rn='2' then month_sale_amt end),0) as 1_month_sale_amt,
		nvl(sum(case when week_rn='1' then month_sale_amt end),0) as 2_month_sale_amt,

		nvl(sum(case when week_rn='4' then month_profit end),0) as 11_month_profit,
		nvl(sum(case when week_rn='3' then month_profit end),0) as 12_month_profit,
		nvl(sum(case when week_rn='2' then month_profit end),0) as 1_month_profit,
		nvl(sum(case when week_rn='1' then month_profit end),0) as 2_month_profit,


		nvl(sum(case when week_rn='4' then month_sale_qty end),0) as 11_month_sale_qty,
		nvl(sum(case when week_rn='3' then month_sale_qty end),0) as 12_month_sale_qty,
		nvl(sum(case when week_rn='2' then month_sale_qty end),0) as 1_month_sale_qty,
		nvl(sum(case when week_rn='1' then month_sale_qty end),0) as 2_month_sale_qty,

		nvl(sum(case when week_rn='4' then normal_month_sale_amt-normal_month_profit end),0)/nvl(sum(case when week_rn='4' then normal_month_sale_qty end),0) as 11_month_normal_cost,
		nvl(sum(case when week_rn='3' then normal_month_sale_amt-normal_month_profit end),0)/nvl(sum(case when week_rn='3' then normal_month_sale_qty end),0) as 12_month_normal_cost,
		nvl(sum(case when week_rn='2' then normal_month_sale_amt-normal_month_profit end),0)/nvl(sum(case when week_rn='2' then normal_month_sale_qty end),0) as 1_month_normal_cost,
		nvl(sum(case when week_rn='1' then normal_month_sale_amt-normal_month_profit end),0)/nvl(sum(case when week_rn='1' then normal_month_sale_qty end),0) as 2_month_normal_cost ,
        nvl(sum(case when week_rn='4' then normal_month_sale_amt end),0)/nvl(sum(case when week_rn='4' then normal_month_sale_qty end),0) as 11_month_normal_price,
		nvl(sum(case when week_rn='3' then normal_month_sale_amt end),0)/nvl(sum(case when week_rn='3' then normal_month_sale_qty end),0) as 12_month_normal_price,
		nvl(sum(case when week_rn='2' then normal_month_sale_amt end),0)/nvl(sum(case when week_rn='2' then normal_month_sale_qty end),0) as 1_month_normal_price,
		nvl(sum(case when week_rn='1' then normal_month_sale_amt end),0)/nvl(sum(case when week_rn='1' then normal_month_sale_qty end),0) as 2_month_normal_price
	from csx_analyse_tmp.tmp_hn
	where customer_code in ('110807','155386','128371','235130','195996','236479','236481','236484') -- 后面3个新增教育客户
	 and week_rn<5
	group by 
		performance_region_name,
		performance_province_name,
		performance_city_name,
		customer_code,
		customer_name,
		classify_middle_name,
		goods_code,
		goods_name 
	) a1 
) a 
left join 
-- TOP客户整体毛利率
(select 
	performance_region_name,
	performance_province_name,
	performance_city_name,
	classify_middle_name,
	goods_code,
	nvl(sum(case when week_rn='4' then month_profit end),0)/nvl(sum(case when week_rn='4' then month_sale_amt end),0) as 11_month_profitlv,
	nvl(sum(case when week_rn='3' then month_profit end),0)/nvl(sum(case when week_rn='3' then month_sale_amt end),0) as 12_month_profitlv,
	nvl(sum(case when week_rn='2' then month_profit end),0)/nvl(sum(case when week_rn='2' then month_sale_amt end),0) as 1_month_profitlv,
	nvl(sum(case when week_rn='1' then month_profit end),0)/nvl(sum(case when week_rn='1' then month_sale_amt end),0) as 2_month_profitlv    
from csx_analyse_tmp.tmp_hn
where customer_code in ('110807','155386','128371','235130','195996','236479','236481','236484') 
group by 
	performance_region_name,
	performance_province_name,
	performance_city_name,
	classify_middle_name,
	goods_code 
) b 
on a.performance_region_name=b.performance_region_name and a.performance_province_name=b.performance_province_name and a.performance_city_name=b.performance_city_name and a.classify_middle_name=b.classify_middle_name and a.goods_code=b.goods_code 
left join 
-- 省区整体毛利率 
(select 
	performance_region_name,
	performance_province_name,
	performance_city_name,
	classify_middle_name,
	goods_code,
	nvl(sum(case when week_rn='4' then month_profit end),0)/nvl(sum(case when week_rn='4' then month_sale_amt end),0) as 11_month_profitlv,
	nvl(sum(case when week_rn='3' then month_profit end),0)/nvl(sum(case when week_rn='3' then month_sale_amt end),0) as 12_month_profitlv,
	nvl(sum(case when week_rn='2' then month_profit end),0)/nvl(sum(case when week_rn='2' then month_sale_amt end),0) as 1_month_profitlv,
	nvl(sum(case when week_rn='1' then month_profit end),0)/nvl(sum(case when week_rn='1' then month_sale_amt end),0) as 2_month_profitlv    
from csx_analyse_tmp.tmp_hn
group by 
	performance_region_name,
	performance_province_name,
	performance_city_name,
	classify_middle_name,
	goods_code  
) c 
on a.performance_region_name=c.performance_region_name and a.performance_province_name=c.performance_province_name and a.performance_city_name=c.performance_city_name and a.classify_middle_name=c.classify_middle_name and a.goods_code=c.goods_code 
left join 
(select * from 
(select 
       customer_code, price_type_name,price_period_name,price_date_name,business_attribute_name
        ,
        split(price_set_type,',')[0],
        case when split(price_set_type,',')[0] ='1' then '对标定价' 
              when split(price_set_type,',')[0]='4' then '客户定价' 
              when split(price_set_type,',')[0]='8' then '自主定价' 
              when split(price_set_type,',')[0]='11'  then '临时报价' 
              when split(price_set_type,',')[0]='2' then '全对标' 
              when split(price_set_type,',')[0]='3' then '半对标' 
              when split(price_set_type,',')[0]='5' then '合同固定价' 
              when split(price_set_type,',')[0]='6' then '多方比价' 
              when split(price_set_type,',')[0]='7' then '单独议价' 
              when split(price_set_type,',')[0]='9' then '采购或车间定价' 
              when split(price_set_type,',')[0]='10'  then '建议售价' 
              when split(price_set_type,',')[0]='12'  then '下单时' 
              when split(price_set_type,',')[0]='13'  then '单品项' 
        end as price_set_type_first,
        (case when split(price_set_type,',')[1]='1' then '对标定价' 
              when split(price_set_type,',')[1]='4' then '客户定价' 
              when split(price_set_type,',')[1]='8' then '自主定价' 
              when split(price_set_type,',')[1]='11' then '临时报价' 
              when split(price_set_type,',')[1]='2' then '全对标' 
              when split(price_set_type,',')[1]='3' then '半对标' 
              when split(price_set_type,',')[1]='5' then '合同固定价' 
              when split(price_set_type,',')[1]='6' then '多方比价' 
              when split(price_set_type,',')[1]='7' then '单独议价' 
              when split(price_set_type,',')[1]='9' then '采购或车间定价' 
              when split(price_set_type,',')[1]='10' then '建议售价' 
              when split(price_set_type,',')[1]='12' then '下单时' 
              when split(price_set_type,',')[1]='13' then '单品项' 
        end) as price_set_type_sec, 
        row_number()over(partition by customer_code order by business_number desc) as pm 
    from csx_dim.csx_dim_crm_business_info 
    where sdt='current' 
)a where pm=1) d on a.customer_code=d.customer_code
;
-- 成本侧
-- 最终数据
select 
	a.performance_region_name as `大区`,
	a.performance_province_name as `省区`,
	a.performance_city_name as `城市`,
-- 	a.customer_code as `客户编码`,
-- 	a.customer_name as `客户名称`,
-- 	price_set_type_first `定价类型`,
--     price_set_type_sec  `定价类型2`,
--   --   price_type_name `定价类型`,
--      price_period_name `报价周期`,
--      price_date_name `报价日期`,
	a.classify_middle_name as `管理中类`,
	a.goods_code as `商品编码`,
	a.goods_name as `商品名称`,

	a.11_month_sale_amt/a.cus_cla_11_month_sale_amt as `销售额占比-品类_M11`,
	a.12_month_sale_amt/a.cus_cla_12_month_sale_amt as `销售额占比-品类_M12`,
	a.1_month_sale_amt/a.cus_cla_1_month_sale_amt as `销售额占比-品类_M1`,
	a.2_month_sale_amt/a.cus_cla_2_month_sale_amt as `销售额占比-品类_M2`,

	a.11_month_profit/a.11_month_sale_amt as `毛利率_M11`,
	a.12_month_profit/a.12_month_sale_amt as `毛利率_M12`,
	a.1_month_profit/a.1_month_sale_amt as `毛利率_M1`,
	a.2_month_profit/a.2_month_sale_amt as `毛利率_M2`,

	b.11_month_profitlv as `重点客户毛利率_M11`,
	b.12_month_profitlv as `重点客户毛利率_M12`,
	b.1_month_profitlv as `重点客户毛利率_M1`,
	b.2_month_profitlv as `重点客户毛利率_M2`,

	c.11_month_profitlv as `省区毛利率_M11`,
	c.12_month_profitlv as `省区毛利率_M12`,
	c.1_month_profitlv as `省区毛利率_M1`,
	c.2_month_profitlv as `省区毛利率_M2`,

	a.11_month_normal_cost as `正常平均成本_M11`,
	a.12_month_normal_cost as `正常平均成本_M12`,
	a.1_month_normal_cost as `正常平均成本_M1`,
	a.2_month_normal_cost as `正常平均成本_M2`,
	a.11_month_normal_price as `正常平均售价_M11`,
	a.12_month_normal_price as `正常平均售价_M12`,
	a.1_month_normal_price as `正常平均售价_M1`,
	a.2_month_normal_price as `正常平均售价_M2`  
--   case when (coalesce(2_month_normal_price,0)!=0 and coalesce(1_month_normal_price,0 )!=0 and coalesce(12_month_normal_price,0)!=0 and  coalesce(11_month_normal_price,0)!=0)
--         then least(2_month_normal_price,1_month_normal_price,12_month_normal_price,11_month_normal_price) end  as min_price,
--   if(sort_array(array(coalesce(2_month_normal_price,0),coalesce(1_month_normal_price,0),coalesce(12_month_normal_price,0),coalesce(11_month_normal_price,0)))[0]<=0 ,sort_array(array(2_month_normal_price,1_month_normal_price,12_month_normal_price,11_month_normal_price))[1],sort_array(array(2_month_normal_price,1_month_normal_price,12_month_normal_price,11_month_normal_price))[0]) as  min_val

from 
(select a1.*,
    a1.performance_region_name as `大区`,
	a1.performance_province_name as `省区`,
	a1.performance_city_name as `城市`,
-- 	a.customer_code as `客户编码`,
-- 	a.customer_name as `客户名称`,
-- 	price_set_type_first `定价类型`,
--     price_set_type_sec  `定价类型2`,
--   --   price_type_name `定价类型`,
--      price_period_name `报价周期`,
--      price_date_name `报价日期`,
	a1.classify_middle_name as `管理中类`,
	a1.goods_code as `商品编码`,
	a1.goods_name as `商品名称`,
	a1.11_month_sale_amt,
	a1.12_month_sale_amt,
	a1.1_month_sale_amt ,
	a1.2_month_sale_amt ,
	a1.11_month_profit,
	a1.12_month_profit,
	a1.1_month_profit ,
	a1.2_month_profit ,
	coalesce(a1.11_month_normal_cost,0) 11_month_normal_cost ,
	coalesce(a1.12_month_normal_cost,0 )12_month_normal_cost ,
	coalesce(a1.1_month_normal_cost ,0) 1_month_normal_cost,
	coalesce(a1.2_month_normal_cost ,0 ) 2_month_normal_cost,
	coalesce(a1.11_month_normal_price,0)  11_month_normal_price,
	coalesce(a1.12_month_normal_price,0)  12_month_normal_price,
	coalesce(a1.1_month_normal_price ,0)  1_month_normal_price,
	coalesce(a1.2_month_normal_price ,0) 2_month_normal_price ,
	sum(11_month_sale_amt)over(partition by classify_middle_name) as cus_cla_11_month_sale_amt, -- 求品类销售额占比
	sum(12_month_sale_amt)over(partition by classify_middle_name) as cus_cla_12_month_sale_amt, -- 求品类销售额占比
	sum(1_month_sale_amt)over(partition by  classify_middle_name) as cus_cla_1_month_sale_amt, -- 求品类销售额占比
	sum(2_month_sale_amt)over(partition by  classify_middle_name) as cus_cla_2_month_sale_amt  
 from 
	(select 
		performance_region_name,
		performance_province_name,
		performance_city_name,
-- 		customer_code,
-- 		customer_name,
		classify_middle_name,
		goods_code,
		goods_name,

		coalesce(sum(case when week_rn='4' then month_sale_amt end),0) as 11_month_sale_amt,
		coalesce(sum(case when week_rn='3' then month_sale_amt end),0) as 12_month_sale_amt,
		coalesce(sum(case when week_rn='2' then month_sale_amt end),0) as 1_month_sale_amt,
		coalesce(sum(case when week_rn='1' then month_sale_amt end),0) as 2_month_sale_amt,

		coalesce(sum(case when week_rn='4' then month_profit end),0) as 11_month_profit,
		coalesce(sum(case when week_rn='3' then month_profit end),0) as 12_month_profit,
		coalesce(sum(case when week_rn='2' then month_profit end),0) as 1_month_profit,
		coalesce(sum(case when week_rn='1' then month_profit end),0) as 2_month_profit,


		coalesce(sum(case when week_rn='4' then month_sale_qty end),0) as 11_month_sale_qty,
		coalesce(sum(case when week_rn='3' then month_sale_qty end),0) as 12_month_sale_qty,
		coalesce(sum(case when week_rn='2' then month_sale_qty end),0) as 1_month_sale_qty,
		coalesce(sum(case when week_rn='1' then month_sale_qty end),0) as 2_month_sale_qty,

		coalesce(sum(case when week_rn='4' then normal_month_sale_amt-normal_month_profit end),0)/nvl(sum(case when week_rn='4' then normal_month_sale_qty end),0) as 11_month_normal_cost,
		coalesce(sum(case when week_rn='3' then normal_month_sale_amt-normal_month_profit end),0)/nvl(sum(case when week_rn='3' then normal_month_sale_qty end),0) as 12_month_normal_cost,
		coalesce(sum(case when week_rn='2' then normal_month_sale_amt-normal_month_profit end),0)/nvl(sum(case when week_rn='2' then normal_month_sale_qty end),0) as 1_month_normal_cost,
		coalesce(sum(case when week_rn='1' then normal_month_sale_amt-normal_month_profit end),0)/nvl(sum(case when week_rn='1' then normal_month_sale_qty end),0) as 2_month_normal_cost ,
        coalesce(sum(case when week_rn='4' then normal_month_sale_amt end),0)/nvl(sum(case when week_rn='4' then normal_month_sale_qty end),0) as 11_month_normal_price,
		coalesce(sum(case when week_rn='3' then normal_month_sale_amt end),0)/nvl(sum(case when week_rn='3' then normal_month_sale_qty end),0) as 12_month_normal_price,
		coalesce(sum(case when week_rn='2' then normal_month_sale_amt end),0)/nvl(sum(case when week_rn='2' then normal_month_sale_qty end),0) as 1_month_normal_price,
		coalesce(sum(case when week_rn='1' then normal_month_sale_amt end),0)/nvl(sum(case when week_rn='1' then normal_month_sale_qty end),0) as 2_month_normal_price
	from csx_analyse_tmp.tmp_hn
	where customer_code in ('110807','155386','128371','235130','195996','236479','236481','236484') -- 后面3个新增教育客户
	 and week_rn<5
	group by 
		performance_region_name,
		performance_province_name,
		performance_city_name,
-- 		customer_code,
-- 		customer_name,
		classify_middle_name,
		goods_code,
		goods_name 
	) a1 
) a 
left join 
-- TOP客户整体毛利率
(select 
	performance_region_name,
	performance_province_name,
	performance_city_name,
	classify_middle_name,
	goods_code,
	nvl(sum(case when week_rn='4' then month_profit end),0)/nvl(sum(case when week_rn='4' then month_sale_amt end),0) as 11_month_profitlv,
	nvl(sum(case when week_rn='3' then month_profit end),0)/nvl(sum(case when week_rn='3' then month_sale_amt end),0) as 12_month_profitlv,
	nvl(sum(case when week_rn='2' then month_profit end),0)/nvl(sum(case when week_rn='2' then month_sale_amt end),0) as 1_month_profitlv,
	nvl(sum(case when week_rn='1' then month_profit end),0)/nvl(sum(case when week_rn='1' then month_sale_amt end),0) as 2_month_profitlv    
from csx_analyse_tmp.tmp_hn
where customer_code in ('110807','155386','128371','235130','195996','236479','236481','236484') 
group by 
	performance_region_name,
	performance_province_name,
	performance_city_name,
	classify_middle_name,
	goods_code 
) b 
on a.performance_region_name=b.performance_region_name and a.performance_province_name=b.performance_province_name and a.performance_city_name=b.performance_city_name and a.classify_middle_name=b.classify_middle_name and a.goods_code=b.goods_code 
left join 
-- 省区整体毛利率 
(select 
	performance_region_name,
	performance_province_name,
	performance_city_name,
	classify_middle_name,
	goods_code,
	nvl(sum(case when week_rn='4' then month_profit end),0)/nvl(sum(case when week_rn='4' then month_sale_amt end),0) as 11_month_profitlv,
	nvl(sum(case when week_rn='3' then month_profit end),0)/nvl(sum(case when week_rn='3' then month_sale_amt end),0) as 12_month_profitlv,
	nvl(sum(case when week_rn='2' then month_profit end),0)/nvl(sum(case when week_rn='2' then month_sale_amt end),0) as 1_month_profitlv,
	nvl(sum(case when week_rn='1' then month_profit end),0)/nvl(sum(case when week_rn='1' then month_sale_amt end),0) as 2_month_profitlv    
from csx_analyse_tmp.tmp_hn
group by 
	performance_region_name,
	performance_province_name,
	performance_city_name,
	classify_middle_name,
	goods_code  
) c 
on a.performance_region_name=c.performance_region_name and a.performance_province_name=c.performance_province_name and a.performance_city_name=c.performance_city_name and a.classify_middle_name=c.classify_middle_name and a.goods_code=c.goods_code 
