-- 福利新老口径数据需求

-- 1、老口径省区
with tmp_sale as 
(select 
    substr(sdt,1,6) s_month,
	performance_region_code,
	performance_region_name,
	performance_province_code,
	performance_province_name,
	performance_city_code,
	performance_city_name,
	c.classify_large_code,
	c.classify_large_name,
	c.classify_middle_code,
	c.classify_middle_name,
--   business_type_code,
    business_type_name,
	sum(sale_amt) curr_sale_amt,
	sum(profit) curr_profit
from   csx_dws.csx_dws_sale_detail_di a 
    left 	join 
	(select goods_code,
	        classify_large_code,
            classify_large_name,
            classify_middle_code,
            classify_middle_name
	from csx_dim.csx_dim_basic_goods 
	where sdt='current'
	) c on a.goods_code=c.goods_code
where 1=1 	
	and (business_type_code IN (2,6) 
        )
	    
	and ( (sdt>=   '20250101'
    	and sdt<=  '20250429'
    	)  
    	)
	group by performance_region_code,
	performance_region_name,
	performance_province_code,
	performance_province_name,
	performance_city_code,
	performance_city_name,
  c.classify_large_code,
	c.classify_large_name,
	c.classify_middle_code,
	c.classify_middle_name,
	business_type_name,
	 substr(sdt,1,6) 
)
select 
    s_month,
    performance_region_code,
	performance_region_name,
	performance_province_code,
	performance_province_name,
	performance_city_code,
	performance_city_name,
	business_type_name,
	-- 本期
	sum(curr_sale_amt)/10000 curr_sale_amt,
	sum(curr_profit)/10000 curr_profit
from tmp_sale 
where performance_city_name!='东北'
group by performance_region_code,
	performance_region_name,
	performance_province_code,
	performance_province_name,
	performance_city_code,
	performance_city_name,
	business_type_name,
	s_month
order by s_month,  case when performance_region_code ='2' then 1 
 when performance_region_code ='4' then 2
 when performance_region_code ='3' then 3
 when performance_region_code ='1' then 4 else 5 end ,
 case when performance_province_code in ('15','1','32','902') then 1 
when performance_province_code in ('20','6','24','13') then 2
when performance_province_code in ('906','26','23','11') then 3
when performance_province_code in ('14','19','11') then 4
when performance_province_code in ('905','18') then 5
else 6 end ,
 case when performance_city_code in ('5','10','12','41') then 1 
when performance_city_code in ('4','18','22','6') then 2
when performance_city_code in ('1','17','23','8') then 3
when performance_city_code in ('2','28','43','9') then 4
when performance_city_code in ('3','38','46','36') then 5
when performance_city_code in ('26','44','13','14') then 6
when performance_city_code in ('24','33','42','45') then 7
when performance_city_code in ('25','16','32') then 8
when performance_city_code in ('35') then 9
else 10 end
 ;
 -- 老口径品类

 
    
    
    -- case when channel_code in ('1','7','9') and business_type_code = '1'  
--   and  business_type_code = 1 and direct_delivery_type in('1','2','13','14','15') 
--   then '日配-销售管理'
--   when channel_code in ('1','7','9') and business_type_code = '1'  
--   and  direct_delivery_type in ('0','11','12','16','17','18')
--   then '日配-采购参与' 
-- end

with tmp_sale as 
(select 
    substr(sdt,1,6) s_month,
	performance_region_code,
	performance_region_name,
	performance_province_code,
	performance_province_name,
	performance_city_code,
	performance_city_name,
	c.classify_large_code,
	c.classify_large_name,
	c.classify_middle_code,
	c.classify_middle_name,
--   business_type_code,
    business_type_name,
	sum(sale_amt) curr_sale_amt,
	sum(profit) curr_profit
from   csx_dws.csx_dws_sale_detail_di a 
   left join 
	(select shop_code,shop_low_profit_flag from csx_dim.csx_dim_shop 
	  where sdt='current'
	  	and shop_low_profit_flag=0) b on a.inventory_dc_code=shop_code
    left 	join 
	(select goods_code,
	        classify_large_code,
            classify_large_name,
            classify_middle_code,
            classify_middle_name
	from csx_dim.csx_dim_basic_goods 
	where sdt='current'
	) c on a.goods_code=c.goods_code
where 1=1 	
	and (business_type_code IN (2,6)
        )
	    
	and ( (sdt>=   '20250101'
    	and sdt<=  '20250429'
    	)  
    	)
	group by performance_region_code,
	performance_region_name,
	performance_province_code,
	performance_province_name,
	performance_city_code,
	performance_city_name,
   c.classify_large_code,
	c.classify_large_name,
	c.classify_middle_code,
	c.classify_middle_name,
 business_type_name,
	 substr(sdt,1,6) 
)
select 
    s_month,
    performance_province_name,
    performance_city_name,
	    case when classify_middle_code in ('B0102','B0603') then '米面粮油'   -- 米、食用油类
      when classify_middle_code in ('B0101','B0103','B0601','B0602','B0604','B0605') then '调味干货'   -- 干货\蛋\面类/米粉类\调味品类\罐头小菜\早餐冲调
      when classify_middle_code in ('B0401','B0402','B0501','B0701') then '休闲饮品'  -- 酒\香烟饮料\休闲食品\常温乳品饮料
      -- 熟食烘焙\水果\蔬菜\家禽\猪肉\水产\预制菜\牛羊\冷藏冷冻食品
      when classify_middle_code in ('B0104','B0201','B0202','B0301','B0302','B0303','B0305','B0306','B0702') then '生鲜冻品'
      when classify_middle_code in ('B0801') then '清洁用品'
      when classify_middle_code in ('B0801','B0802','B0803','B0804','B0805','B0902','B0901') then '家电家纺'
    else '其他' end classify_name,
    classify_middle_code,
    classify_middle_name,
	business_type_name,
	-- 本期
	sum(curr_sale_amt)/10000 curr_sale_amt,
	sum(curr_profit)/10000 curr_profit
from tmp_sale 
where performance_city_name!='东北'
group by  case when classify_middle_code in ('B0102','B0603') then '米面粮油'   -- 米、食用油类
      when classify_middle_code in ('B0101','B0103','B0601','B0602','B0604','B0605') then '调味干货'   -- 干货\蛋\面类/米粉类\调味品类\罐头小菜\早餐冲调
      when classify_middle_code in ('B0401','B0402','B0501','B0701') then '休闲饮品'  -- 酒\香烟饮料\休闲食品\常温乳品饮料
      -- 熟食烘焙\水果\蔬菜\家禽\猪肉\水产\预制菜\牛羊\冷藏冷冻食品
      when classify_middle_code in ('B0104','B0201','B0202','B0301','B0302','B0303','B0305','B0306','B0702') then '生鲜冻品'
      when classify_middle_code in ('B0801') then '清洁用品'
      when classify_middle_code in ('B0801','B0802','B0803','B0804','B0805','B0902','B0901') then '家电家纺'
    else '其他' end ,
    classify_middle_code,
    classify_middle_name,
	business_type_name,
	
	 performance_province_name,
    performance_city_name,
    s_month
order by s_month,  
case when classify_middle_code in ('B0102','B0603') then 1  -- 米、食用油类
      when classify_middle_code in ('B0101','B0103','B0601','B0602','B0604','B0605') then 2   -- 干货\蛋\面类/米粉类\调味品类\罐头小菜\早餐冲调
      when classify_middle_code in ('B0401','B0402','B0501','B0701') then 3  -- 酒\香烟饮料\休闲食品\常温乳品饮料
      -- 熟食烘焙\水果\蔬菜\家禽\猪肉\水产\预制菜\牛羊\冷藏冷冻食品
      when classify_middle_code in ('B0104','B0201','B0202','B0301','B0302','B0303','B0305','B0306','B0702') then 4
      when classify_middle_code in ('B0801') then 5
      when classify_middle_code in ('B0801','B0802','B0803','B0804','B0805','B0902','B0901') then 6
    else 7 end,
 
classify_middle_code,
classify_middle_name,
 performance_province_name,
    performance_city_name
 ;


 -- 新口径品类

 
    
    
    -- case when channel_code in ('1','7','9') and business_type_code = '1'  
--   and  business_type_code = 1 and direct_delivery_type in('1','2','13','14','15') 
--   then '日配-销售管理'
--   when channel_code in ('1','7','9') and business_type_code = '1'  
--   and  direct_delivery_type in ('0','11','12','16','17','18')
--   then '日配-采购参与' 
-- end

with tmp_sale as 
(select 
    substr(sdt,1,6) s_month,
	performance_region_code,
	performance_region_name,
	performance_province_code,
	performance_province_name,
	performance_city_code,
	performance_city_name,
	c.classify_large_code,
	c.classify_large_name,
	c.classify_middle_code,
	c.classify_middle_name,
--   business_type_code,
    a.customer_code,
    case when b.order_code is not null then new_business_type_name
        when a.business_type_code = 4   and order_business_type_code = 2 then '福利业务'
        when welfare_type_code =2 then '福利小店'
        when a.business_type_code = 4
        and order_business_type_code <> 2 then '日配业务'
        else a.business_type_name
      end as business_type_name,
	sum(sale_amt) curr_sale_amt,
	sum(profit) curr_profit
from   csx_dws.csx_dws_sale_detail_di a 
left join 
(select
  customer_code ,
  order_code  ,
  business_type_code ,
  business_type_name ,
  new_business_type_code,
  new_business_type_name
from
  csx_report.csx_report_sale_fujian_prison_business_type_adjust_df) b on a.customer_code=b.customer_code and a.order_code=b.order_code
    left 	join 
	(select goods_code,
	        classify_large_code,
            classify_large_name,
            classify_middle_code,
            classify_middle_name
	from csx_dim.csx_dim_basic_goods 
	where sdt='current'
	) c on a.goods_code=c.goods_code
where 1=1 	
	and (a.business_type_code IN (2,6,1) or (a.business_type_code = 4
        and order_business_type_code = 2)
        )
	    
	and ( (sdt>=   '20250101'
    	and sdt<=  '20250429'
    	)  
    	)
	group by performance_region_code,
	performance_region_name,
	performance_province_code,
	performance_province_name,
	performance_city_code,
	performance_city_name,
  c.classify_large_code,
	c.classify_large_name,
	c.classify_middle_code,
	c.classify_middle_name,
	a.customer_code,
case when b.order_code is not null then new_business_type_name
        when a.business_type_code = 4   and order_business_type_code = 2 then '福利业务'
        when welfare_type_code =2 then '福利小店'
        when a.business_type_code = 4
        and order_business_type_code <> 2 then '日配业务'
        else a.business_type_name
      end ,
	 substr(sdt,1,6) 
) 
select 
    s_month,
	    case when classify_middle_code in ('B0102','B0603') then '米面粮油'   -- 米、食用油类
      when classify_middle_code in ('B0101','B0103','B0601','B0602','B0604','B0605') then '调味干货'   -- 干货\蛋\面类/米粉类\调味品类\罐头小菜\早餐冲调
      when classify_middle_code in ('B0401','B0402','B0501','B0701') then '休闲饮品'  -- 酒\香烟饮料\休闲食品\常温乳品饮料
      -- 熟食烘焙\水果\蔬菜\家禽\猪肉\水产\预制菜\牛羊\冷藏冷冻食品
      when classify_middle_code in ('B0104','B0201','B0202','B0301','B0302','B0303','B0305','B0306','B0702') then '生鲜冻品'
      when classify_middle_code in ('B0801') then '清洁用品'
      when classify_middle_code in ('B0801','B0802','B0803','B0804','B0805','B0902','B0901') then '家电家纺'
    else '其他' end classify_name,
    classify_middle_code,
    classify_middle_name,
	business_type_name,
	-- 本期
	sum(curr_sale_amt)/10000 curr_sale_amt,
	sum(curr_profit)/10000 curr_profit
from tmp_sale 
where performance_city_name!='东北'
 and business_type_name!='日配业务'
group by  case when classify_middle_code in ('B0102','B0603') then '米面粮油'   -- 米、食用油类
      when classify_middle_code in ('B0101','B0103','B0601','B0602','B0604','B0605') then '调味干货'   -- 干货\蛋\面类/米粉类\调味品类\罐头小菜\早餐冲调
      when classify_middle_code in ('B0401','B0402','B0501','B0701') then '休闲饮品'  -- 酒\香烟饮料\休闲食品\常温乳品饮料
      -- 熟食烘焙\水果\蔬菜\家禽\猪肉\水产\预制菜\牛羊\冷藏冷冻食品
      when classify_middle_code in ('B0104','B0201','B0202','B0301','B0302','B0303','B0305','B0306','B0702') then '生鲜冻品'
      when classify_middle_code in ('B0801') then '清洁用品'
      when classify_middle_code in ('B0801','B0802','B0803','B0804','B0805','B0902','B0901') then '家电家纺'
    else '其他' end ,
    classify_middle_code,
    classify_middle_name,
	business_type_name,
	s_month
order by s_month,  
case when classify_middle_code in ('B0102','B0603') then 1  -- 米、食用油类
      when classify_middle_code in ('B0101','B0103','B0601','B0602','B0604','B0605') then 2   -- 干货\蛋\面类/米粉类\调味品类\罐头小菜\早餐冲调
      when classify_middle_code in ('B0401','B0402','B0501','B0701') then 3  -- 酒\香烟饮料\休闲食品\常温乳品饮料
      -- 熟食烘焙\水果\蔬菜\家禽\猪肉\水产\预制菜\牛羊\冷藏冷冻食品
      when classify_middle_code in ('B0104','B0201','B0202','B0301','B0302','B0303','B0305','B0306','B0702') then 4
      when classify_middle_code in ('B0801') then 5
      when classify_middle_code in ('B0801','B0802','B0803','B0804','B0805','B0902','B0901') then 6
    else 7 end,
 
classify_middle_code,
classify_middle_name
 ;

 -- 新口径省区
 -- case when channel_code in ('1','7','9') and business_type_code = '1'  
--   and  business_type_code = 1 and direct_delivery_type in('1','2','13','14','15') 
--   then '日配-销售管理'
--   when channel_code in ('1','7','9') and business_type_code = '1'  
--   and  direct_delivery_type in ('0','11','12','16','17','18')
--   then '日配-采购参与' 
-- end

with tmp_sale as 
(select 
    substr(sdt,1,6) s_month,
	performance_region_code,
	performance_region_name,
	performance_province_code,
	performance_province_name,
	performance_city_code,
	performance_city_name,
	c.classify_large_code,
	c.classify_large_name,
	c.classify_middle_code,
	c.classify_middle_name,
--   business_type_code,
    a.customer_code,
    case when b.order_code is not null then new_business_type_name
        when a.business_type_code = 4   and order_business_type_code = 2 then '福利业务'
        when welfare_type_code =2 then '福利小店'
        when a.business_type_code = 4
        and order_business_type_code <> 2 then '日配业务'
        else a.business_type_name
      end as business_type_name,
	sum(sale_amt) curr_sale_amt,
	sum(profit) curr_profit
from   csx_dws.csx_dws_sale_detail_di a 
left join 
(select
  customer_code ,
  order_code  ,
  business_type_code ,
  business_type_name ,
  new_business_type_code,
  new_business_type_name
from
  csx_report.csx_report_sale_fujian_prison_business_type_adjust_df) b on a.customer_code=b.customer_code and a.order_code=b.order_code
    left 	join 
	(select goods_code,
	        classify_large_code,
            classify_large_name,
            classify_middle_code,
            classify_middle_name
	from csx_dim.csx_dim_basic_goods 
	where sdt='current'
	) c on a.goods_code=c.goods_code
where 1=1 	
	and (a.business_type_code IN (2,6,1) or (a.business_type_code = 4
        and order_business_type_code = 2)
        )
	    
	and ( (sdt>=   '20250101'
    	and sdt<=  '20250429'
    	)  
    	)
	group by performance_region_code,
	performance_region_name,
	performance_province_code,
	performance_province_name,
	performance_city_code,
	performance_city_name,
  c.classify_large_code,
	c.classify_large_name,
	c.classify_middle_code,
	c.classify_middle_name,
	a.customer_code,
case when b.order_code is not null then new_business_type_name
        when a.business_type_code = 4   and order_business_type_code = 2 then '福利业务'
        when welfare_type_code =2 then '福利小店'
        when a.business_type_code = 4
        and order_business_type_code <> 2 then '日配业务'
        else a.business_type_name
      end ,
	 substr(sdt,1,6) 
) 
select 
    s_month,
    performance_region_code,
	performance_region_name,
	performance_province_code,
	performance_province_name,
	performance_city_code,
	performance_city_name,
	business_type_name,
	-- 本期
	sum(curr_sale_amt)/10000 curr_sale_amt,
	sum(curr_profit)/10000 curr_profit
from tmp_sale 
where
-- performance_city_name!='东北'
business_type_name !='日配业务'
group by performance_region_code,
	performance_region_name,
	performance_province_code,
	performance_province_name,
	performance_city_code,
	performance_city_name,
	business_type_name,
	s_month
order by s_month,  case when performance_region_code ='2' then 1 
 when performance_region_code ='4' then 2
 when performance_region_code ='3' then 3
 when performance_region_code ='1' then 4 else 5 end ,
 case when performance_province_code in ('15','1','32','902') then 1 
when performance_province_code in ('20','6','24','13') then 2
when performance_province_code in ('906','26','23','11') then 3
when performance_province_code in ('14','19','11') then 4
when performance_province_code in ('905','18') then 5
else 6 end ,
 case when performance_city_code in ('5','10','12','41') then 1 
when performance_city_code in ('4','18','22','6') then 2
when performance_city_code in ('1','17','23','8') then 3
when performance_city_code in ('2','28','43','9') then 4
when performance_city_code in ('3','38','46','36') then 5
when performance_city_code in ('26','44','13','14') then 6
when performance_city_code in ('24','33','42','45') then 7
when performance_city_code in ('25','16','32') then 8
when performance_city_code in ('35') then 9
else 10 end
 ;