-- -----------------------------------------------------------------------------------
-- ---------------客户定价类型数据
drop table if exists csx_analyse_tmp.csx_analyse_tmp_customer_price_type_01;
create table if not exists csx_analyse_tmp.csx_analyse_tmp_customer_price_type_01 as 
select 
	t1.*,
	case t1.price_type
		when '客户定价->合同固定价' then '客户定价'
		when '客户定价->单独议价' then '客户定价'
		when '对标定价->全对标' then '对标定价'
		when '' then '自主定价'
		when null then '自主定价'
		when '空' then '自主定价'
		when '自主定价->采购或车间定价' then '自主定价'
		when '临时报价->单品项' then '临时定价'
		when '临时报价->下单时' then '临时定价'
		when '客户定价->多方比价' then '客户定价'
		when '对标定价->半对标' then '对标定价'
		when '自主定价->建议售价' then '自主定价'
		end as price_type1,	
	case t1.price_type
		when '客户定价->合同固定价' then '合同固定价'
		when '客户定价->单独议价' then '单独议价'
		when '对标定价->全对标' then '全对标'
		when '' then '建议售价'
		when null then '建议售价'
		when '空' then '建议售价'
		when '自主定价->采购或车间定价' then '建议售价'   
		when '临时报价->单品项' then '临时定价'
		when '临时报价->下单时' then '临时定价'
		when '客户定价->多方比价' then '多方比价'
		when '对标定价->半对标' then '半对标'
		when '自主定价->建议售价' then '建议售价'
	end as price_type2 
from 
(select 
    customer_code,
    (case when a.price_period_code=1 then '每日' 
          when a.price_period_code=2 then '每周' 
          when a.price_period_code=3 then '每半月' 
          when a.price_period_code=4 then '每月' end) as price_period_name,-- 报价周期 
    price_date_name,
	concat(a.price_set_type_first,'->',a.price_set_type_sec) as price_type -- 定价类型 
from 
    (select 
        *,
        (case when split(price_set_type,',')[0]='1' then '对标定价' 
              when split(price_set_type,',')[0]='4' then '客户定价' 
              when split(price_set_type,',')[0]='8' then '自主定价' 
              when split(price_set_type,',')[0]='11' then '临时报价' 
              when split(price_set_type,',')[0]='2' then '全对标' 
              when split(price_set_type,',')[0]='3' then '半对标' 
              when split(price_set_type,',')[0]='5' then '合同固定价' 
              when split(price_set_type,',')[0]='6' then '多方比价' 
              when split(price_set_type,',')[0]='7' then '单独议价' 
              when split(price_set_type,',')[0]='9' then '采购或车间定价' 
              when split(price_set_type,',')[0]='10' then '建议售价' 
              when split(price_set_type,',')[0]='12' then '下单时' 
              when split(price_set_type,',')[0]='13' then '单品项' 
        end) as price_set_type_first,
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
        row_number()over(partition by customer_code order by business_number desc) as ranks 
    from csx_dim.csx_dim_crm_business_info 
    where sdt='current' 
    and business_attribute_code=1 
    and status=1 
    -- and sign_type_code=1 
    )a 
where a.ranks=1
) t1  
;
-- -----------------------------------------------------------------------------------
-- -----------销售数据--- 客户品类数据
drop table if exists csx_analyse_tmp.csx_analyse_tmp_sale_detail_di_01;
create table if not exists csx_analyse_tmp.csx_analyse_tmp_sale_detail_di_01 as 
select 
	a.performance_region_name,
	a.performance_province_name,
	a.performance_city_name,
	d.first_category_name,
	d.second_category_name,
	d.third_category_name,
	a.customer_code,
	d.customer_name,
	nvl(f.fir_price_type,e.price_type1) as price_type1, -- 定价类型1
	nvl(f.sec_price_type,e.price_type2) as price_type2, -- 定价类型2
	e.price_period_name, -- 报价周期
	e.price_date_name, -- 报价日
	c.classify_middle_code,
	c.classify_middle_name,
	(case when a.delivery_type_code=2 then '直送单'
		  when a.order_channel_code=6 then '调价单' 
		  when a.order_channel_code=4 then '返利单' 
		  when a.refund_order_flag=1 then '退货单' 
		  -- when a.order_channel_detail_code=26 then '价格补救单' 
	else '其他' end) as order_type,
	if(a.delivery_type_code=2 ,'是','否') zhisong_type,
	if(a.order_channel_code=6 ,'是','否') tiaojia_type,
	if(a.order_channel_code=4 ,'是','否') fanli_type,
	if(a.refund_order_flag=1,'是','否') tuihuo_type,
	sum(a.sale_amt) as sale_amt,
	sum(a.profit) as profit
from 
	(select *   -- 销售额
	from csx_dws.csx_dws_sale_detail_di 
	where sdt>='20231207' and sdt<='20231213'
	-- and channel_code in('1','7','9')
	and business_type_code in ('1') 
	and inventory_dc_code not in ('W0J2','W0AJ','W0G6','WB71')
	) a 
	left join  -- 直送仓数据dc
	(select * 
	from csx_dim.csx_dim_shop  
	where sdt='current' 
	and shop_low_profit_flag=1 
	) b 
	on a.inventory_dc_code=b.shop_code 
	left join  -- 商品信息
	(select * 
	from csx_dim.csx_dim_basic_goods 
	where sdt='current' 
	) c 
	on a.goods_code=c.goods_code 
	left join  -- 客户表
	(select * 
	from csx_dim.csx_dim_crm_customer_info 
	where sdt='current'
	) d 
	on a.customer_code=d.customer_code 
	left join  -- 线上客户定价类型
	csx_analyse_tmp.csx_analyse_tmp_customer_price_type_ky e 
	on a.customer_code=e.customer_code 
	left join  -- 线下客户定价类型
	dev.csx_ods_data_analysis_prd_cus_price_type_231206_df f  
	on a.customer_code=f.customer_code 
where b.shop_code is null 
group by 
	a.performance_region_name,
	a.performance_province_name,
	a.performance_city_name,
	d.first_category_name,
	d.second_category_name,
	d.third_category_name,
	a.customer_code,
	d.customer_name,
	nvl(f.fir_price_type,e.price_type1), -- 定价类型1
	nvl(f.sec_price_type,e.price_type2), -- 定价类型2
	e.price_period_name, -- 报价周期
	e.price_date_name, -- 报价日
	c.classify_middle_code,c.classify_middle_name,
	(case when a.delivery_type_code=2 then '直送单'
		  when a.order_channel_code=6 then '调价单' 
		  when a.order_channel_code=4 then '返利单' 
		  when a.refund_order_flag=1 then '退货单' 
		  -- when a.order_channel_detail_code=26 then '价格补救单' 
	else '其他' end),
	if(a.delivery_type_code=2 ,'是','否'),
	if(a.order_channel_code=6 ,'是','否'),
	if(a.order_channel_code=4 ,'是','否'),
	if(a.refund_order_flag=1,'是','否');
	
/*	
-- 销售数据(客户数据)-- 剔除异常因素
-- 客户品类数据 
select 
	customer_ranks,
	performance_region_name,performance_province_name,
	customer_code,customer_name,
	second_category_name,	
	price_type1,price_type2,price_period_name,price_date_name,
	customer_sale_amt,customer_profit,customer_profit_rate,
	province_sale_amt,province_profit,province_profit_rate,customer_impact,
	classify_middle_name,sale_amt,profit,profit_rate,middle_impact,
    province_middle_sale_amt,province_middle_profit,province_middle_profit_rate,
	profit_rate - province_middle_profit_rate as middle_rate_cha
from
	(select 
		a.*,
		row_number() over(partition by customer_code order by  middle_impact  asc) as middle_ranks,-- 1234 客户下品类影响排名
		dense_rank() over(partition by performance_province_name order by  customer_impact  asc) as customer_ranks -- 11112222 省区下客户影响排名
	from 
		(select 
			a.*,
			customer_profit_rate-((customer_profit-profit)/abs(customer_sale_amt-sale_amt)) middle_impact,	-- 品类对客户的毛利影响
			province_profit_rate-((province_profit-customer_profit)/abs(province_sale_amt-customer_sale_amt)) customer_impact -- 客户对省区毛利影响
		from 
			(
			select a.*,
				sum(sale_amt)over(partition by customer_code) customer_sale_amt, -- 客户业绩
				sum(profit)over(partition by customer_code) customer_profit,
				sum(profit)over(partition by customer_code) /abs(sum(sale_amt)over(partition by customer_code)) customer_profit_rate,		
				sum(sale_amt)over(partition by performance_province_name) province_sale_amt, -- 省区业绩
				sum(profit)over(partition by performance_province_name) province_profit,	
				sum(profit)over(partition by performance_province_name)/abs(sum(sale_amt)over(partition by performance_province_name))province_profit_rate, sum(sale_amt)over(partition by performance_province_name,classify_middle_code) province_middle_sale_amt, --省区品类业绩
				sum(profit)over(partition by performance_province_name,classify_middle_code) province_middle_profit,	
				sum(profit)over(partition by performance_province_name,classify_middle_code)/abs(sum(sale_amt)over(partition by performance_province_name,classify_middle_code)) province_middle_profit_rate
			from 
				(
				select 
					performance_region_name,
					performance_province_name,			
					customer_code,customer_name,
					classify_middle_code,classify_middle_name,
					second_category_name,
					price_type1,price_type2,
					price_period_name,price_date_name,
					sum(sale_amt) sale_amt,
					sum(profit) profit,
					sum(profit)/abs(sum(sale_amt)) profit_rate		
				from csx_analyse_tmp.csx_analyse_tmp_sale_detail_di_01
				where order_type = '其他'
				group by 
					performance_region_name,
					performance_province_name,			
					customer_code,customer_name,
					classify_middle_code,classify_middle_name,
					second_category_name,
					price_type1,price_type2,
					price_period_name,price_date_name
				
				)a
			)a
		)a	

	)a
where middle_ranks = 1 and customer_ranks <=10 and customer_impact<0
*/
---销售数据(客户数据)--- 异常因素 TOP5
-- 客户品类数据 
select middle_ranks_asc,middle_ranks_desc,customer_ranks_asc,customer_ranks_desc,
	order_type,
	performance_region_name,
	performance_province_name,			
	customer_code,
    customer_name,
	second_category_name,
	price_type1,price_type2,
	price_period_name,
    price_date_name,
	customer_sale_amt,
    customer_profit,
    customer_profit_rate,
	province_sale_amt,
    province_profit,
    province_profit_rate,
	classify_middle_name,
	sale_amt,profit,profit_rate	
from 
(select 
	a.*,	
	row_number() over(partition by order_type,customer_code order by  sale_amt  asc) as middle_ranks_asc,-- 1234 客户下品类影响排名
	row_number() over(partition by order_type,customer_code order by  sale_amt  desc) as middle_ranks_desc,
	dense_rank() over(partition by order_type,performance_province_name order by  customer_sale_amt  asc) as customer_ranks_asc,
	dense_rank() over(partition by order_type,performance_province_name order by  customer_sale_amt  desc) as customer_ranks_desc -- 1122 省区下客户影响排名
from
	(select 
		a.*,
		sum(sale_amt)over(partition by order_type,customer_code) customer_sale_amt, -- 客户异常业绩合计
		sum(profit)over(partition by order_type,customer_code) customer_profit,
		sum(profit)over(partition by order_type,customer_code) /abs(sum(sale_amt)over(partition by order_type,customer_code)) customer_profit_rate,sum(sale_amt)over(partition by order_type,performance_province_name) province_sale_amt, -- 省区异常业绩合计
		sum(profit)over(partition by order_type,performance_province_name) province_profit,	
		sum(profit)over(partition by order_type,performance_province_name)/abs(sum(sale_amt)over(partition by order_type,performance_province_name))province_profit_rate
	from 
		(select 
			order_type,
			performance_region_name,
			performance_province_name,			
			customer_code,customer_name,
			classify_middle_code,classify_middle_name,
			second_category_name,
			price_type1,price_type2,
			price_period_name,price_date_name,
			sum(sale_amt) sale_amt,
			sum(profit) profit,
			sum(profit)/abs(sum(sale_amt)) profit_rate		
		from csx_analyse_tmp.csx_analyse_tmp_sale_detail_di_01
		where order_type != '其他'
		group by 
			order_type,
			performance_region_name,
			performance_province_name,			
			customer_code,customer_name,
			classify_middle_code,classify_middle_name,
			second_category_name,
			price_type1,price_type2,
			price_period_name,price_date_name
		
		)a	
	)a
)a
 where if (order_type = '直送单',middle_ranks_desc=1 and customer_ranks_desc<=5,middle_ranks_asc=1 and customer_ranks_asc<=5)

 