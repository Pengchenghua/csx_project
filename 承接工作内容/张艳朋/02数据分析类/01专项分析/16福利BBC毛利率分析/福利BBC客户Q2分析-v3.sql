

drop table csx_analyse_tmp.tmp_flbbc_customer_goods_sale;
create  table csx_analyse_tmp.tmp_flbbc_customer_goods_sale
as
select a.group_flag,a.type_flag,a.operation_mode_name,	 
	case when concat(substr(first_business_sale_date,1,4),'Q',floor(substr(first_business_sale_date,5,2)/3.1) + 1)
			=sale_quarter then '新客' else '老客' end cust_new_old,
	a.sale_quarter,
	a.performance_region_name,
	a.performance_province_name,
	a.performance_city_name,
	a.inventory_dc_code,
	g.shop_name as inventory_dc_name,
	a.delivery_type_name,   -- 配送类型名称
	a.customer_code,
	d.customer_name,
	e.first_business_sale_date,
	e.last_business_sale_date,
	f.first_sale_date,f.last_sale_date,
	d.first_category_name,
	d.second_category_name,
	d.third_category_name,
	a.business_type_name,
	h.classify_large_name,
	h.classify_middle_name,
	h.classify_small_name,	
	-- a.goods_code,
	-- h.goods_name,
	count_day,
	sale_qty,
	sale_cost,
	sale_amt,
	profit
from 	 
	(
	select '福利' group_flag,'福利' type_flag,'福利' as operation_mode_name,
		concat(substr(sdt,1,4),'Q',floor(substr(sdt,5,2)/3.1) + 1) as sale_quarter,
		-- substr(sdt,1, 6) month,
		performance_region_code,performance_region_name,
		performance_province_code,performance_province_name,
		performance_city_code,performance_city_name,
		business_type_code,business_type_name,
		customer_code,-- customer_name,
		inventory_dc_code,-- inventory_dc_name,
		delivery_type_name,   -- 配送类型名称
		classify_large_code,
		classify_middle_code,
		classify_small_code,
		-- goods_code,  -- goods_name,
		count(distinct sdt) count_day,
		sum(sale_qty) sale_qty,
		sum(sale_cost)/10000 sale_cost,
		sum(sale_amt)/10000 sale_amt,
		sum(profit)/10000 profit	
	from csx_dws.csx_dws_sale_detail_di 
	where ((sdt>='20220401' and sdt<'20220701')
			or(sdt>='20230401' and sdt<'20230701')) 
		and channel_code  in ('1','7','9')
		and business_type_code in ('2')
		and performance_region_name in ('华南大区','华北大区','华西大区','华东大区','华中大区')
	group by 
		concat(substr(sdt,1,4),'Q',floor(substr(sdt,5,2)/3.1) + 1) ,
		-- substr(sdt,1,6),
		performance_region_code,performance_region_name,
		performance_province_code,performance_province_name,
		performance_city_code,performance_city_name,customer_code,
		business_type_code,business_type_name,
		inventory_dc_code,delivery_type_name,-- goods_code
		classify_large_code,classify_middle_code,classify_small_code
		
	union all
	select 'BBC' group_flag,
		if(credit_pay_type_name='餐卡' or credit_pay_type_code='F11','餐卡','福利') type_flag,operation_mode_name,
		concat(substr(sdt,1,4),'Q',floor(substr(sdt,5,2)/3.1) + 1) as sale_quarter,
		-- substr(sdt,1, 6) month,
		performance_region_code,performance_region_name,
		performance_province_code,performance_province_name,
		performance_city_code,performance_city_name,
		6 as business_type_code,'BBC' as business_type_name,
		customer_code,-- customer_name,
		inventory_dc_code,-- inventory_dc_name,
		delivery_type_name,   -- 配送类型名称
		classify_large_code,
		classify_middle_code,
		classify_small_code,		
		-- goods_code,  -- goods_name,
		count(distinct sdt) count_day,
		sum(sale_qty) sale_qty,
		sum(sale_cost)/10000 sale_cost,
		sum(sale_amt)/10000 sale_amt,
		sum(profit)/10000 profit
	from csx_dws.csx_dws_bbc_sale_detail_di 
	where ((sdt>='20220401' and sdt<'20220701')
			or(sdt>='20230401' and sdt<'20230701')) 
		and channel_code  in ('1','7','9')
		-- and business_type_code in ('2','6')
		and performance_region_name in ('华南大区','华北大区','华西大区','华东大区','华中大区')
	group by if(credit_pay_type_name='餐卡' or credit_pay_type_code='F11','餐卡','福利'),operation_mode_name,
		concat(substr(sdt,1,4),'Q',floor(substr(sdt,5,2)/3.1) + 1) ,
		-- substr(sdt,1,6),
		performance_region_code,performance_region_name,
		performance_province_code,performance_province_name,
		performance_city_code,performance_city_name,customer_code,
		-- business_type_code,business_type_name,
		inventory_dc_code,delivery_type_name,-- goods_code
		classify_large_code,classify_middle_code,classify_small_code	
	)a 	
	left join
		(select * 
		 from csx_dim.csx_dim_crm_customer_info
		 where sdt = 'current'	
		)d on d.customer_code=a.customer_code
	left join  -- 业务类型首单日期
	(
	select 
		customer_code,
		business_type_code,
		min(first_business_sale_date) first_business_sale_date,
		max(last_business_sale_date) last_business_sale_date
	from csx_dws.csx_dws_crm_customer_business_active_di
	where sdt ='current' 
	group by customer_code,business_type_code
	)e on e.customer_code=a.customer_code and e.business_type_code=a.business_type_code
	left join  -- 客户首单日期
	(
	select 
		customer_code,first_sale_date,last_sale_date
	from csx_dws.csx_dws_crm_customer_active_di
	where sdt ='current' 
	)f on f.customer_code=a.customer_code 
	left join 
		( 
            select shop_code,shop_name 
			from csx_dim.csx_dim_shop 
			where sdt='current' 
		)g on a.inventory_dc_code = g.shop_code	
left join 
 (
   select distinct  
	classify_large_code,classify_large_name,
	classify_middle_code,classify_middle_name,
	classify_small_code,classify_small_name  
   from  csx_dim.csx_dim_basic_goods 
   where sdt = 'current'
 -- ) h on h.goods_code = a.goods_code 	
 ) h on h.classify_large_code = a.classify_large_code 
	and h.classify_middle_code = a.classify_middle_code
  and h.classify_small_code = a.classify_small_code
;



select *
from csx_analyse_tmp.tmp_flbbc_customer_sale;



-- 城市汇总-餐卡福利
select group_flag,type_flag,	 
	sale_quarter,
	performance_region_name,
	performance_province_name,
	performance_city_name,
	count(distinct customer_code) count_cust,
	count(distinct case when concat(substr(first_business_sale_date,1,4),'Q',floor(substr(first_business_sale_date,5,2)/3.1) + 1)
			=sale_quarter then customer_code end) count_cust_new,
	sum(sale_cost) sale_cost,
	sum(sale_amt) sale_amt,
	sum(profit) profit,
	sum(profit)/abs(sum(sale_amt)) profit_rate
from csx_analyse_tmp.tmp_flbbc_customer_sale
group by group_flag,type_flag, 
	sale_quarter,
	performance_region_name,
	performance_province_name,
	performance_city_name;

--- 城市汇总-经营方式
select group_flag,operation_mode_name,	 
	sale_quarter,
	performance_region_name,
	performance_province_name,
	performance_city_name,
	count(distinct customer_code) count_cust,
	sum(sale_cost) sale_cost,
	sum(sale_amt) sale_amt,
	sum(profit) profit,
	sum(profit)/abs(sum(sale_amt)) profit_rate
from csx_analyse_tmp.tmp_flbbc_customer_sale
group by group_flag,operation_mode_name,	 
	sale_quarter,
	performance_region_name,
	performance_province_name,
	performance_city_name	
	;

--- 城市汇总	
select '总体' groups,group_flag,
	sale_quarter,
	performance_region_name,
	performance_province_name,
	performance_city_name,
	count(distinct customer_code) count_cust,
	sum(sale_cost) sale_cost,
	sum(sale_amt) sale_amt,
	sum(profit) profit,
	sum(profit)/abs(sum(sale_amt)) profit_rate
from csx_analyse_tmp.tmp_flbbc_customer_sale
group by  group_flag,
	sale_quarter,
	performance_region_name,
	performance_province_name,
	performance_city_name
	
union all
select 
	case when concat(substr(first_business_sale_date,1,4),'Q',floor(substr(first_business_sale_date,5,2)/3.1) + 1)
			=sale_quarter then '新客' else '老客' end groups,
	group_flag,sale_quarter,
	performance_region_name,
	performance_province_name,
	performance_city_name,
	count(distinct customer_code) count_cust,
	sum(sale_cost) sale_cost,
	sum(sale_amt) sale_amt,
	sum(profit) profit,
	sum(profit)/abs(sum(sale_amt)) profit_rate
from csx_analyse_tmp.tmp_flbbc_customer_sale
group by 
	case when concat(substr(first_business_sale_date,1,4),'Q',floor(substr(first_business_sale_date,5,2)/3.1) + 1)
			=sale_quarter then '新客' else '老客' end,	
	group_flag,sale_quarter,
	performance_region_name,
	performance_province_name,
	performance_city_name	
	;
	
	
	


'江西省','广东省','北京市','河北省','陕西省','东北','重庆市','四川省','贵州省','上海松江','江苏南京','江苏苏州','安徽省','河南省','湖北省'

case 
when 'F13' then '高温补贴'
when 'F12' then '劳保'
when 'F14' then '信用付'
when 'F15' then '运动鞋专享'
when 'F22' then '企业专享'
when 'F18' then '奖品'
when 'F11' then '餐卡'
when 'F10' then '福利'
when 'F23' then '消费帮扶'
when 'NA' 	then '非授信'
when 'F21' then '点数'
when 'F19' then '其他'
end 

















