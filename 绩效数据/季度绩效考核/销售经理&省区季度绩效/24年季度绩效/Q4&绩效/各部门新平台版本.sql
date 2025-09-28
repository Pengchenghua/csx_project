	select 
		performance_province_name,
        performance_city_name,
	   substr(sdt,1,6) smonth,
		if( c.shop_code is null,'否','是') types,
		sum(profit)as profit		
	from (
	       select 
		     * 
	       from csx_dws.csx_dws_sale_detail_di 
	       where 
	          ((sdt >= '20221001' and sdt <= '20221231') or (sdt >= '20230901' and sdt <= '20230930' ))
	          and channel_code in('1','9')
	          and business_type_code in ('1') 
		  ) a
    left join ( 
	            select
                   distinct shop_code 
				from csx_dim.csx_dim_shop 
				where sdt='current' and shop_low_profit_flag=1  
			  )c
               on a.inventory_dc_code = c.shop_code
    group by performance_province_name,
        performance_city_name,
	    smonth,
		types;
    
--  -满足率 来自供应商评价表
select 
    substr(a.sdt,1,6) smonths,
c.performance_province_name,
c.performance_city_name,
b.classify_middle_code,
b.classify_middle_name,
sum(line_num),
count(if(is_qty_satisfy=1,a.goods_code,null))
from  csx_ads.csx_ads_scm_supplier_evaluation_detail_1d a
left join 
(
 select goods_code,
  classify_middle_code,classify_middle_name
 from csx_dim.csx_dim_basic_goods 
 where sdt='current'
 )b on a.goods_code=b.goods_code 
 left join (select * 
				from csx_dim.csx_dim_shop 
				where sdt='current' 
				)c
on a.location_code = c.shop_code
where a.sdt >='20230701' and  a.sdt<='20230930'
group by 
    substr(a.sdt,1,6),
	c.performance_province_name,c.performance_city_name,b.classify_middle_code,b.classify_middle_name;

--  --  --  --  --  --  --  --  --  品类B端销售额毛利率

SELECT 
    substr(a.sdt,1,6) smonths,
     a.performance_province_name,
     a.performance_city_name,
     b.classify_middle_name,
     business_type_name,
	 a.inventory_dc_code,
	 a.inventory_dc_name,
	 a.customer_code,
 	 g.customer_name,
	 sum(sale_amt) sale_amt,
	 sum(a.profit) profit,
     sum(if(c.shop_code is null,sale_amt,0)) sale_amt1,
	 sum(if(c.shop_code is null,a.profit,0)) profit
from ( 
        select * from csx_dws.csx_dws_sale_detail_di 
		where channel_code  in ('1','7','9') 
           and business_type_name in ('日配业务','BBC','福利业务')
           and sdt<='20230930' 
           and sdt>='20230701' )a
left join (select distinct shop_code 
				from csx_dim.csx_dim_shop 
				where sdt='current' and shop_low_profit_flag=1 --    低毛利DC标识(1-是,0-否)
				)c
on a.inventory_dc_code = c.shop_code
  LEFT join
          (
              select *
              from csx_dim.csx_dim_crm_customer_info
              where sdt= 'current'
             and channel_code  in ('1','7','9')
           ) g on a.customer_code=g.customer_code 
left join (select goods_code,
  classify_middle_code,classify_middle_name
 from csx_dim.csx_dim_basic_goods 
 where sdt='current')b on a.goods_code=b.goods_code 
group by substr(a.sdt,1,6),
a.performance_province_name,
     a.performance_city_name,
     b.classify_middle_name,
	 a.customer_code,
	 g.customer_name,
	 a.inventory_dc_code,
	 a.inventory_dc_name,
     business_type_name
;


--  -战略客户new

SELECT 
  a.customer_code,
  b.customer_name,
  a.business_type_name,
  sum(sale_amt) sale_amt,
  sum(a.profit) profit,
  sum(a.profit)/abs(sum(a.sale_amt)) maolilv
from csx_dws.csx_dws_sale_detail_di a
left join
--    客户信息
(
  select 
    customer_code,
	customer_name  
  from csx_dim.csx_dim_crm_customer_info 
  where sdt='20230930'
  and customer_code<>''
  and channel_code in('1','7','9')
)b on b.customer_code=a.customer_code
where  a.channel_code in ('1','7','9')--  and  a.business_type_name  in ('城市服务商','BBC','日配业务','福利业务')  
and a.customer_code in 
('120416','123755','125201','123706','124579','117217','130536','130983','164047','228532','130971','114982','235304',
'235502','235446','234928','195022','234959','235291','235165','235408','235463','235874','234958','127861','124524',
'130087','130369','130625','131129','131162','131187','131146','131041','130750','130928','128661','130899','224480',
'225477','129307','201599','224120','155386','130145','232646','225902','125089','125662','123559','231095','228749',
'228401','231320','234827','123561','117927','226569','127123','123395','129908','117346','131294','223559','224016',
'232023','230158','226389','225776','230272','233599','115210','224759','201599','230931','229557','228747','235929',
'232479','115210','236416','130941','228446','230760','114477','227579','228190','228748','229231','228516','158226',
'159226','228541','212969','232846','234827','236564')
and a.sdt<='20230930' 
and a.sdt>='20230701'
group by a.customer_code,b.customer_name,a.business_type_name;


-- 季度准时保量交货SKU数

select
performance_province_name
,performance_city_name
,count(if(goods_reality_receive_qty>=goods_plan_receive_qty,goods_code,null)) sku  -- 季度准时保量交货SKU数
,count(goods_code)  skuall -- 季度准时保量交货SKU数
from 
(
SELECT 
 c.performance_province_name,
 c.performance_city_name,
 order_code
 ,goods_code
 ,sum(goods_plan_receive_qty) goods_plan_receive_qty  -- 商品计划数量
 ,sum(goods_reality_receive_qty)  goods_reality_receive_qty  -- 商品实际产量
from
csx_dws.csx_dws_mms_factory_order_df a
     left join (select  shop_code,performance_province_name performance_province_name,performance_city_name performance_city_name
				from csx_dim.csx_dim_shop 
				where sdt='20230930'
				)c
      on a.location_code=c.shop_code
where produce_date>='20230701' and  produce_date<='20230930' 
  and sdt<='20230930' 
and sdt>='20230701'
GROUP BY performance_province_name,order_code,performance_city_name
,goods_code
) a
group by performance_province_name,performance_city_name; 

--  人均接单SKU达成率

select
  a.performance_province_name,
  a.sku,
  a.ren,
  b.ysku,
  b.reny
--   季度人均接单SKU数
from 
( 
SELECT
performance_province_name,
count(goods_code) sku,
count(distinct recep_order_user_number) ren
from --  csx_dw.dws_csms_r_d_yszx_order_m_new A
csx_dwd.csx_dwd_csms_yszx_order_detail_di a
 LEFT JOIN
    ( --  外部城市服务商DC，需要过滤
      SELECT
        shop_code
      FROM csx_dim.csx_dim_shop
      WHERE sdt = 'current' AND purpose = '09'
    ) t2 ON a.inventory_dc_code = t2.shop_code	
left join
--  客户信息
(
  select 
performance_province_name performance_province_name,customer_code
  from csx_dim.csx_dim_crm_customer_info  
  where sdt='20230930'
)b on b.customer_code=a.customer_code
where sdt>='20230701' and sdt<='20230930'
    and  t2.shop_code IS NULL
and is_return = 0
group by 
performance_province_name)a
left join 
(
SELECT
performance_province_name,
lebie,
sum(ysku) over(PARTITION BY lebie) ysku,
sum(reny) over(PARTITION BY lebie) reny
from 
(select
performance_province_name,
case when zongsku<20000 then '2万以下'
     when zongsku>=20000 and zongsku<50000 then '2-5万'
	 when zongsku>=50000 and zongsku<80000 then '5-8万'
	 when zongsku>=80000 and zongsku<150000 then '8-15万'
	 when zongsku>=150000 then '15万以上' end as lebie,
avg(sku) ysku,
avg(ren) reny
from (
select
smonth,
performance_province_name,sku,ren,
sum(sku) over(PARTITION BY performance_province_name) zongsku
from (
SELECT
substr(sdt,1,6) smonth,
performance_province_name,
count(goods_code) sku,
count(distinct recep_order_user_number) ren
from --  csx_dw.dws_csms_r_d_yszx_order_m_new A
csx_dwd.csx_dwd_csms_yszx_order_detail_di a
 LEFT JOIN
    ( --  外部城市服务商DC，需要过滤
      SELECT
        shop_code
      FROM csx_dim.csx_dim_shop
      WHERE sdt = 'current' AND purpose = '09'
    ) t2 ON a.inventory_dc_code = t2.shop_code	
left join
--  客户信息
(
  select 
performance_province_name performance_province_name,customer_code
  from csx_dim.csx_dim_crm_customer_info  
  where sdt='20230930'
)b on b.customer_code=a.customer_code
where sdt>='20230701' and sdt<='20230930'
  and  t2.shop_code IS NULL
and is_return = 0
group by substr(sdt,1,6),
performance_province_name
)a
) a
group by performance_province_name,
case when zongsku<20000 then '2万以下'
     when zongsku>=20000 and zongsku<50000 then '2-5万'
	 when zongsku>=50000 and zongsku<80000 then '5-8万'
	 when zongsku>=80000 and zongsku<150000 then '8-15万'
	 when zongsku>=150000 then '15万以上' end )a
	 )b on a.performance_province_name=b.performance_province_name

;


--  -酒水
SELECT 
    sum(sale_amt) sale_amt,
	sum(a.profit) profit
from csx_dws.csx_dws_sale_detail_di a
where  channel_code  in ('1','7','9') 
and performance_province_name like '平台%'
and a.sdt<='20230930' 
and a.sdt>='20230701'
;



--城市服务商通道费
SELECT
 performance_province_name,
  if(t2.shop_name like '%V2DC%','2.0','1.0') ff,
  sum(a.sale_amt), -- 总销售额
sum(profit) profit
FROM csx_dws.csx_dws_sale_detail_di  a
 LEFT JOIN
    ( --  外部城市服务商DC，需要过滤
      SELECT
        shop_code,shop_name
      FROM csx_dim.csx_dim_shop
      WHERE sdt = 'current' 
    ) t2 ON a.inventory_dc_code = t2.shop_code	

where 
	sdt between '20230701' and '20230930'
	and business_type_code in (4) -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
	and channel_code in('1','7','9')
group by  performance_province_name,
  if(t2.shop_name like '%V2DC%','2.0','1.0');
  
  /*
--  -餐饮
SELECT 
    sum(sale_amt) sale_amt,
	sum(a.profit) profit
from csx_dws.csx_dws_sale_detail_di a
where  channel_code  in ('1','7','9') 
--  and business_type_name='省区大宗'
and customer_name like '%牛约堡%'
and a.sdt<='20230930' 
and a.sdt>='20230701'
;*/
--  --  --  --  BBC 

SELECT 
    performance_province_name,
	sum(sale_amt) sale_amt,
	sum(a.profit) profit,
	count(distinct customer_code) cnt
from csx_dws.csx_dws_sale_detail_di a
where  channel_code in ('7') 
and a.sdt<='20230930' 
and a.sdt>='20230701'
group by performance_province_name;

--  -季度B端配送销售收入（不含税），剔除 BBC 合伙人）
SELECT 
    performance_province_name,performance_city_name,
	sum(a.sale_amt_no_tax) excluding_tax_sales
from csx_dws.csx_dws_sale_detail_di a
where  channel_code  in ('1','9') and business_type_name<>'城市服务商'
and delivery_type_code<>'2'   --  直送
--  and order_no not in ('OC20111000000022','OC20111000000023','OC20111000000021','OC20111000000024','OC20111000000025')
and a.sdt<='20230930' 
and a.sdt>='20230701'
group by performance_city_name,performance_province_name;
/*
--  -冻品、调理类
SELECT substr(sdt,1,6) smonth,
    performance_province_name,
	sum(a.sale_amt) sale_amt,
	sum(a.profit) profit
from csx_dws.csx_dws_sale_detail_di a
where  channel_code in ('1','9','7') 
and classify_middle_code in('B0304','B0305')
and a.sdt<='20230930' 
and a.sdt>='20230701'
group by smonth,performance_province_name;

--  -平台B
SELECT 
    goods_code,goods_name,
	sum(a.excluding_tax_sales) excluding_tax_sales
from csx_dws.csx_dws_sale_detail_di a
where  channel_code  in ('1','7','9') 
and business_type_name='省区大宗'
and performance_province_name like '平台-B%'
and a.sdt<='20230930' 
and a.sdt>='20230701'
group by goods_code,goods_name;

--  -自营定价毛利率
SELECT 
 a.performance_province_name,
	performance_city_name,
	sum(a.excluding_tax_sales) excluding_tax_sales,
	sum(a.excluding_tax_profit) excluding_tax_profit,
	sum(a.excluding_tax_profit)/abs(sum(a.excluding_tax_sales)) profitlv
	--  count(distinct a.order_no) cnt
from csx_dws.csx_dws_sale_detail_di a
where  a.channel_code in ('1','9') and a.business_type_name in ('福利业务','日配业务')
and a.sdt<='20230930' 
and a.sdt>='20230701'
group by performance_city_name,a.performance_province_name
*/

--  品类结构健康度=基础商品池销售额/总销售额*100%（含税金额，不含平台销售、合伙人销售）
SELECT 
 a.performance_province_name,performance_city_name,
   sum(a.sale_amt)--  含税销售额--  总销售额
  ,sum(if(purchase_price>0.01,a.sale_amt,0))--  基础商品池销售额
from 
 csx_dws.csx_dws_sale_detail_di a
where  a.channel_code in ('1','9') and a.business_type_code='1'
  and inventory_dc_code in 
  ('W0A8',-- 
'W0F7',-- 
'W0G7'  ,-- 
'W0F4'	,-- 
'W0H3',-- 
'W0H7',-- 
'W0K1'	,-- 
'W0K6'	,-- 
'W0L3'	,-- 
'W0L4',-- 
'W0A2'	,-- 
'W0D4'	,-- 
'W0A3'	,-- 
'W0A5'	,-- 
'W0W7',
'W0A6'	,-- 
'W0N1'	,-- 
'W0A7'	,-- 
'W0Q9'	,-- 
'W0P8'	,-- 
'W0N0'	,-- 
'W0P5'	,-- 
'W0Q2'	,-- 
'W0BH',
'W0BR',
'W0BK',
'W0R9'	)
and a.sdt<='20230930' 
and a.sdt>='20230701'
group by a.performance_province_name,performance_city_name;
/*
--  季度准时保量交货SKU数
select 
count(if(goods_reality_receive_qty>=goods_plan_receive_qty,goods_code,null))  --   季度准时保量交货SKU数
,count(goods_code)   
from (SELECT performance_province_name,
order_code
,goods_code
,sum(goods_plan_receive_qty) goods_plan_receive_qty --   商品计划数量
,sum(goods_reality_receive_qty)  goods_reality_receive_qty --   商品实际产量
from csx_dw.dws_mms_r_a_factory_order
where produce_date>='20230701' and  produce_date<='20230930' 
  and sdt<='20230930' 
and sdt>='20230701'
GROUP BY performance_province_name,order_code
,goods_code
) a

--  chengshi
select
performance_province_name
,performance_city_name
,count(if(goods_reality_receive_qty>=goods_plan_receive_qty,goods_code,null))  --   季度准时保量交货SKU数
,count(goods_code)  --   季度准时保量交货SKU数
from (SELECT performance_province_name,performance_city_name,
order_code
,goods_code
,sum(goods_plan_receive_qty) goods_plan_receive_qty --   商品计划数量
,sum(goods_reality_receive_qty)  goods_reality_receive_qty --   商品实际产量
from
csx_dw.dws_mms_r_a_factory_order a
 join (select shop_id,shop_name,performance_city_name
from csx_dim.csx_dim_shop
where sdt='20230930'--  and performance_province_name='江苏省'
)c on a.location_code=c.shop_id
where produce_date>='20230701' and  produce_date<='20230930' 
  and sdt<='20230930' 
and sdt>='20230701'
GROUP BY performance_province_name,order_code,performance_city_name
,goods_code
) a
group by performance_province_name,performance_city_name;



--  --  --  --  - 季度工厂加工品类产量
SELECT performance_province_name,-- city_name,
concat(classify_middle_name,channel)
,sum(goods_reality_receive_qty)  goods_reality_receive_qty --   商品实际产量
from
csx_dw.dws_mms_r_a_factory_order a
left join (select goods_id,classify_middle_name
            from csx_dw.dws_basic_w_a_csx_product_m 
            where sdt='20230930'
            )c on a.goods_code=c.goods_id
where  a.sdt<='20230930' 
and a.sdt>='20230701'
GROUP BY performance_province_name,concat(classify_middle_name,channel)--  ,city_name


SELECT performance_province_name,performance_city_name
,sum(sale_amt)--  总销售额
,sum(if(is_factory_goods=1,sale_amt,0))--  是否工厂商品1是0否
from 
 csx_dws.csx_dws_sale_detail_di a
where  a.sdt<='20230930' 
and a.sdt>='20230701'
group by performance_province_name,performance_city_name;
*/
