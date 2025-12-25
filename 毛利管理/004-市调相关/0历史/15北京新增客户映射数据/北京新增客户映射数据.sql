with customer_product as (
select 
	b.*,a.market_code,a.rank as ranks,e.market_name
from 
	(select * from csx_ods.csx_ods_data_analysis_prd_report_csx_analyse_customer_market_site_df)a	
	left join 	-- 客户商品池新增
	(select * from csx_ods.csx_ods_b2b_mall_prod_yszx_customer_product_df 
	where regexp_replace(substr(create_time,1,10),'-','') ='${yesterday}'
	)b on a.customer_code =b.customer_code 
	left join (select * from csx_dim.csx_dim_shop where sdt='current') c on c.shop_code=b.inventory_dc_code
	left join (select * from csx_dim.csx_dim_basic_goods where sdt='current')d on d.goods_code=b.product_code
	left join csx_ods.csx_ods_csx_price_prod_market_research_location_config_df e on a.market_code=e.market_code
	
	where c.performance_province_name ='北京市'
 ),

market_common as (
select e.*
from 
	(select * from csx_ods.csx_ods_csx_price_prod_market_common_web_ref_config_df where sdt ='${yesterday}') e
	left join 
	(select performance_province_name, shop_code from csx_dim.csx_dim_shop where sdt='current'
	) c on c.shop_code=e.warehouse_code
where c.performance_province_name ='北京市'
),

market_customer as (
select *
from 
	(select 
		f.*,
		row_number()over(partition by market_code,product_code order by create_time desc) as rn
	from 
		(select * from  csx_ods.csx_ods_csx_price_prod_market_customer_web_ref_config_df 
		where sdt ='${yesterday}' and customer_code in ('243605','115303','237905')
		) f
		left join 
		(select performance_province_name, shop_code from csx_dim.csx_dim_shop where sdt='current'
		) c on c.shop_code=f.warehouse_code
	where c.performance_province_name ='北京市'
	)f
	where rn=1
)

select 
	regexp_replace(substr(a.create_time,1,10),'-','') create_date 
	,a.customer_code
	,a.customer_name
	,a.product_code
	,a.product_name
	,d.category_large_name
	,d.category_middle_name
	,a.data_source -- 数据来源：0-手动添加 1-客户订单 2-报价 3-商品池模板 4-必售商品 5-商品池模板替换 6-新品 7-基础商品池 8-CRM换品 9-销售添加
	,case 
		when a.data_source = 0 then '手动添加'
		when a.data_source = 1 then '客户订单'
		when a.data_source = 2 then '报价'
		when a.data_source = 3 then '商品池模板'
		when a.data_source = 4 then '必售商品'
		when a.data_source = 5 then '商品池模板替换'	
		when a.data_source = 6 then '新品'	
		when a.data_source = 7 then '基础商品池'	
		when a.data_source = 8 then 'CRM换品'	
		when a.data_source = 9 then '销售添加'
    end data_source_name		
	,d.unit_name
	,d.standard
	,a.ranks
	,a.market_code
	,a.market_name  -- 市调地点
    ,g.price
	
	,e.market_unique_key  as common_market_unique_key -- 唯一值
	,e.market_unit  as common_market_unit
	,e.market_spec  as common_market_spec
	,e.market_third_classify_name  as common_market_third_classify_name -- 网站品类
	,e.change_rate  as common_change_rate  -- 系数
	,e.float_money  as common_float_money  -- 浮动金额
	
	,f.market_unique_key  as customer_market_unique_key -- 唯一值
	,f.market_unit  as customer_market_unit
	,f.market_spec  as customer_market_spec
	,f.market_third_classify_name  as customer_market_third_classify_name  -- 网站品类
	,f.change_rate  as customer_change_rate  -- 系数
	,f.float_money  as customer_float_money    -- 浮动金额
from 
	customer_product a
	left join (select * from csx_dim.csx_dim_shop where sdt='current') c on c.shop_code=a.inventory_dc_code
	left join (select * from csx_dim.csx_dim_basic_goods where sdt='current')d on d.goods_code=a.product_code	
	left join market_common   e on e.market_code = a.market_code and e.product_code=a.product_code
	left join market_customer f on f.market_code = a.market_code and f.product_code=a.product_code	
	left join -- 客户市调生效区
	(select * from csx_ods.csx_ods_csx_price_prod_market_customer_research_price_effective_di
	where sdt>='${sdt_7dago}' and regexp_replace(substr(price_begin_time,1,10),'-','') ='${yesterday}'
	) g on g.customer_code = a.customer_code and g.product_code = a.product_code  

	
	
CREATE TABLE `csx_analyse.csx_analyse_fr_market_common_customer_web_bj_df`(
  `create_date` string COMMENT '日期', 
  `customer_code` string COMMENT '客户编码', 
  `customer_name` string COMMENT '客户名称', 
  `product_code` string COMMENT '商品编码', 
  `product_name` string COMMENT '商品名称', 
  `category_large_name` string COMMENT '大类名称',   
  `category_middle_name` string COMMENT '中类名称', 
  `data_source` int COMMENT '数据来源', 
  `data_source_name` string COMMENT '数据来源名称', 
  `unit_name` string COMMENT '单位',    
  `standard` string COMMENT '规格', 
  `ranks` string COMMENT '优先级', 
  `market_code` string COMMENT '市调地点编码', 
  `market_name` string COMMENT '市调地点名称',     
  `price` decimal(20,6) COMMENT '市调价',  
  `common_market_unique_key` string COMMENT '通用_市调唯一标识', 
  `common_market_unit` string COMMENT '通用_市调商品单位',    
  `common_market_spec` string COMMENT '通用_市调商品规格',  
  `common_market_third_classify_name` string COMMENT '通用_市调品类外部系统名称',  
  `common_change_rate` string COMMENT '通用_转化系数',  
  `common_float_money` string COMMENT '通用_浮动金额',   
  `customer_market_unique_key` string COMMENT '客户_市调唯一标识', 
  `customer_market_unit` string COMMENT '客户_市调商品单位',    
  `customer_market_spec` string COMMENT '客户_市调商品规格',  
  `customer_market_third_classify_name` string COMMENT '客户_市调品类外部系统名称',  
  `customer_change_rate` string COMMENT '客户_转化系数',  
  `customer_float_money` string COMMENT '客户_浮动金额'  
 ) COMMENT '北京新增客户商品的映射数据';
 
 
	
	
