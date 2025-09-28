--Q4采购季度绩效

-- 客诉省区汇总  SKU 更改为省区整体SKU，DC更改为城市整体SKU，任静   不含 客诉20240715
-- 需要使用union all 
with aa as (
select
  a.smonth,
  -- a.performance_region_code,
  a.performance_region_name,
  -- a.performance_province_code,
  a.performance_province_name,
  a.performance_city_name,
  -- a.inventory_dc_code,
  -- a.inventory_dc_name,
    if(a.classify_large_code in ('B01', 'B02', 'B03'),'生鲜','食百') as  dept_name,
   case when a.classify_large_code in ('B04','B05','B06','B07','B08','B09') THEN '食百' 
        when a.classify_large_code='B01' then classify_large_name
   else classify_middle_name    end as classify_name, 
   respon_dept,
--  a.classify_middle_name,
  sum(nvl(b.kesu,0)) kesu,
  sum(nvl(a.sku,0)) sku,
  all_months_sku,
  all_sku,
  sum(nvl(b.kesu,0))/sum(nvl(a.sku,0))  kesu_lv
from 
(
select
  smonth,
  -- performance_region_code,
  performance_region_name,
  performance_province_name,
  performance_city_name,
  -- IF(performance_province_name='浙江省',performance_city_name,performance_province_name) performance_province_name,
  inventory_dc_code,
  inventory_dc_name,
  classify_middle_code,
  classify_middle_name,
  a.classify_large_code,
  classify_large_name,
  sum(sku) sku,
  sum(sum(sku))over(partition by smonth,performance_province_name) all_months_sku,
  sum(sum(sku))over(partition by performance_province_name) all_sku

from (
select 
  substr(sdt,1,6) smonth,
  performance_region_code,
  performance_region_name,
  performance_province_code,
  performance_province_name,
  performance_city_code,
  performance_city_name,
  inventory_dc_code,
  inventory_dc_name,
  c.classify_large_code,
  c.classify_large_name,
  c.classify_middle_code,
  c.classify_middle_name,
  a.order_code,
  count(distinct a.goods_code) sku
from csx_dws.csx_dws_sale_detail_di a
left  join
  (
    select
      goods_code,
      classify_large_code,
      classify_large_name,
      classify_middle_code,
      classify_middle_name,
      classify_small_code,
      classify_small_name
    from csx_dim.csx_dim_basic_goods
    where sdt = 'current'
   ) c on c.goods_code = a.goods_code
where  sdt>='20240701' and sdt<='20240930'
    and channel_code in('1','7','9')
		and business_type_code not in(4,6) -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.bbc 7.大宗一部 8.大宗二部 9.商超)
		and order_channel_code =1 -- 1-b端 2-m端 3-bbc 4-客户返利管理 5-价格补救 6-客户调价管理 -1-sap
		and refund_order_flag=0 -- 退货订单标识(0-正向单 1-逆向单)
		and performance_province_name !='平台-B'
group by substr(sdt,1,6) ,
  performance_region_code,
  performance_region_name,
  performance_province_code,
  performance_province_name,
  performance_city_code,
  performance_city_name,
  inventory_dc_code,
  inventory_dc_name,
  c.classify_middle_code,
  c.classify_middle_name,
  c.classify_large_code,
  c.classify_large_name,
  a.order_code
  )a 
group by smonth,
 -- performance_region_code,
   performance_region_name,
 -- performance_province_code,
  performance_province_name,
  performance_city_name,
  classify_large_code,
  classify_large_name,
   -- IF(performance_province_name='浙江省',performance_city_name,performance_province_name),
  inventory_dc_code,
  inventory_dc_name,
  classify_middle_code,
  classify_middle_name	
	) a
left join 
(
  select concat(first_level_department_name,'-', second_level_department_name) respon_dept,
    substr(sdt,1,6) smonth,        -- 客诉日期
	  performance_province_name,
    performance_city_name,
    inventory_dc_code,
    classify_large_code,
    if(second_level_department_name in ('交易支持','非食用品'),second_level_department_name,classify_middle_code) classify_middle_code,
    count(distinct complaint_code)  kesu --- 客诉单量
  from csx_analyse.csx_analyse_fr_oms_complaint_detail_new_di a
  where  sdt>='20240701' and sdt<='20240930'
    and complaint_status_code in (20,30)  -- 客诉状态: 10-待处理 20-已处理待确认 21-驳回待确认  30-已处理 -1-已取消
		and complaint_deal_status in (10,40) -- 责任环节状态 10'待处理' 20'待修改' 30'已处理待审' 31'已驳回待审核' 40'已完成' -1'已取消'
		and second_level_department_name !=''
		and first_level_department_name='采购'
    and complaint_level !='3'
		and complaint_amt <> 0
    -- 调整包含所有客诉
    -- and complaint_source_code<>2    --	客诉来源:1-单独发起客诉单 2-客退单生成 3-补货单生成
	  -- and second_level_department_name not like '%地采%'
  group by  concat(first_level_department_name,'-', second_level_department_name) ,substr(sdt,1,6),        -- 客诉日期
          inventory_dc_code,
          classify_large_code,
          performance_city_name,
          if(second_level_department_name in ('交易支持','非食用品'),second_level_department_name,classify_middle_code),performance_province_name
  )b on a.inventory_dc_code=b.inventory_dc_code 
       and a.smonth=b.smonth 
       and a.classify_middle_code=b.classify_middle_code
       and a.performance_city_name=b.performance_city_name
       and a.performance_province_name=b.performance_province_name 
group by  a.smonth,
  -- a.performance_region_code,
  a.performance_region_name,
  -- a.performance_province_code,
  a.performance_province_name,
  a.performance_city_name,
  -- a.inventory_dc_code,
  -- a.inventory_dc_name,
  if(a.classify_large_code in ('B01', 'B02', 'B03'),'生鲜','食百'),
   case when a.classify_large_code in ('B04','B05','B06','B07','B08','B09') THEN '食百' 
        when a.classify_large_code='B01' then classify_large_name
        else classify_middle_name end,
  respon_dept,
  all_months_sku,
  all_sku
) 
select *from (
select
  a.smonth,
  -- a.performance_region_code,
  a.performance_region_name,
  -- a.performance_province_code,
  a.performance_province_name,
  a.performance_city_name,
  respon_dept,
  -- a.inventory_dc_code,
  -- a.inventory_dc_name,
  dept_name,
  classify_name,
--  a.classify_middle_name,
  kesu,
  all_months_sku  as all_sku,  --更改为省区整体SKU
  kesu/all_months_sku kesu_lv,
  kesu/all_sku as all_kesu_lv
from  aa a
) a where respon_dept is not null ;





--客诉DC明细 
 
-- 客诉省区汇总  SKU 更改为省区整体SKU，DC更改为城市整体SKU，任静   不含 客诉20240715
with aa as (
select
  a.smonth,
  -- a.performance_region_code,
  a.performance_region_name,
  -- a.performance_province_code,
  a.performance_province_name,
  a.performance_city_name,
   a.inventory_dc_code,
   a.inventory_dc_name,
    if(a.classify_large_code in ('B01', 'B02', 'B03'),'生鲜','食百') as  dept_name,
   case when a.classify_large_code in ('B04','B05','B06','B07','B08','B09') THEN '食百' 
        when a.classify_large_code='B01' then classify_large_name
   else classify_middle_name    end as classify_name, 
   respon_dept,
--  a.classify_middle_name,
  sum(nvl(b.kesu,0)) kesu,
  sum(nvl(a.sku,0)) sku,
  all_months_sku,
  all_months_dc_sku,
  all_sku,
  sum(nvl(b.kesu,0))/sum(nvl(a.sku,0))  kesu_lv
from 
(
select
  smonth,
  -- performance_region_code,
  performance_region_name,
  performance_province_name,
  performance_city_name,
  -- IF(performance_province_name='浙江省',performance_city_name,performance_province_name) performance_province_name,
  inventory_dc_code,
  inventory_dc_name,
  classify_middle_code,
  classify_middle_name,
  a.classify_large_code,
  classify_large_name,
  sum(sku) sku,
  sum(sum(sku))over(partition by smonth,performance_province_name,performance_city_name) all_months_sku,
  sum(sum(sku))over(partition by smonth,performance_province_name,inventory_dc_code) all_months_dc_sku,
  sum(sum(sku))over(partition by performance_province_name,performance_city_name) all_sku
from (
select 
  substr(sdt,1,6) smonth,
  performance_region_code,
  performance_region_name,
  performance_province_code,
  performance_province_name,
  performance_city_code,
  performance_city_name,
  inventory_dc_code,
  inventory_dc_name,
  c.classify_large_code,
  c.classify_large_name,
  c.classify_middle_code,
  c.classify_middle_name,
  a.order_code,
  count(distinct a.goods_code) sku
from csx_dws.csx_dws_sale_detail_di a
left  join
  (
    select
      goods_code,
      classify_large_code,
      classify_large_name,
      classify_middle_code,
      classify_middle_name,
      classify_small_code,
      classify_small_name
    from csx_dim.csx_dim_basic_goods
    where sdt = 'current'
   ) c on c.goods_code = a.goods_code
where  sdt>='20240701' and sdt<='20240930'
    and channel_code in('1','7','9')
    -- and performance_province_name='上海'
		and business_type_code not in(4,6) -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.bbc 7.大宗一部 8.大宗二部 9.商超)
		and order_channel_code =1 -- 1-b端 2-m端 3-bbc 4-客户返利管理 5-价格补救 6-客户调价管理 -1-sap
		and refund_order_flag=0 -- 退货订单标识(0-正向单 1-逆向单)
		and performance_province_name !='平台-B'
group by substr(sdt,1,6) ,
  performance_region_code,
  performance_region_name,
  performance_province_code,
  performance_province_name,
  performance_city_code,
  performance_city_name,
  inventory_dc_code,
  inventory_dc_name,
  c.classify_middle_code,
  c.classify_middle_name,
  c.classify_large_code,
  c.classify_large_name,
  a.order_code
  )a 
group by smonth,
 -- performance_region_code,
   performance_region_name,
 -- performance_province_code,
  performance_province_name,
  performance_city_name,
  classify_large_code,
  classify_large_name,
   -- IF(performance_province_name='浙江省',performance_city_name,performance_province_name),
  inventory_dc_code,
  inventory_dc_name,
  classify_middle_code,
  classify_middle_name	
	) a
left join 
(
  select concat(first_level_department_name,'-', second_level_department_name) respon_dept,
    substr(sdt,1,6) smonth,        -- 客诉日期
	  performance_province_name,
    performance_city_name,
    inventory_dc_code,
    classify_large_code,
    if(second_level_department_name in ('交易支持','非食用品'),second_level_department_name,classify_middle_code) classify_middle_code,
    count(distinct complaint_code)  kesu --- 客诉单量
  from csx_analyse.csx_analyse_fr_oms_complaint_detail_new_di a
  where  sdt>='20240701' and sdt<='20240930'
    and complaint_status_code in (20,30)  -- 客诉状态: 10-待处理 20-已处理待确认 21-驳回待确认  30-已处理 -1-已取消
		and complaint_deal_status in (10,40) -- 责任环节状态 10'待处理' 20'待修改' 30'已处理待审' 31'已驳回待审核' 40'已完成' -1'已取消'
		and second_level_department_name !=''
		and first_level_department_name='采购'
    and complaint_level !='3'
		and complaint_amt <> 0
  --  and complaint_source_code<>2    --	客诉来源:1-单独发起客诉单 2-客退单生成 3-补货单生成
	--  and second_level_department_name not like '%地采%'
  group by  concat(first_level_department_name,'-', second_level_department_name) ,substr(sdt,1,6),        -- 客诉日期
          inventory_dc_code,
          classify_large_code,
          performance_city_name,
          if(second_level_department_name in ('交易支持','非食用品'),second_level_department_name,classify_middle_code),performance_province_name
  )b on a.inventory_dc_code=b.inventory_dc_code 
       and a.smonth=b.smonth 
       and a.classify_middle_code=b.classify_middle_code
       and a.performance_city_name=b.performance_city_name
       and a.performance_province_name=b.performance_province_name 
group by  a.smonth,
  -- a.performance_region_code,
  a.performance_region_name,
--   a.performance_province_code,
  a.performance_province_name,
  a.performance_city_name,
   a.inventory_dc_code,
   a.inventory_dc_name,
  if(a.classify_large_code in ('B01', 'B02', 'B03'),'生鲜','食百'),
   case when a.classify_large_code in ('B04','B05','B06','B07','B08','B09') THEN '食百' 
        when a.classify_large_code='B01' then classify_large_name
        else classify_middle_name end,
  respon_dept,
  all_months_sku,
  all_months_dc_sku,
  all_sku
) 
select *from (
select
  a.smonth,
  -- a.performance_region_code,
  a.performance_region_name,
  -- a.performance_province_code,
  a.performance_province_name,
  a.performance_city_name,
  respon_dept,
   a.inventory_dc_code,
   a.inventory_dc_name,
  dept_name,
  classify_name,
--  a.classify_middle_name,
  kesu,
  all_months_sku  as all_sku,  --更改为城市SKU
  kesu/all_months_sku kesu_lv,
  kesu/all_sku as all_kesu_lv,
  all_months_dc_sku   -- DCSKU
from  aa a
) a where respon_dept is not null ;



-- 采购占比 汇总

select
   basic_performance_region_name,
  -- basic_performance_province_code,	
   basic_performance_province_name	,
  -- basic_performance_city_code,	
   basic_performance_city_name,
   a.classify_middle_name,
  -- if(receive_amt=0,0,nvl(amtfenzi,0)/receive_amt) zhanbi,
  sum(amtfenzi) amtfenzi,
  sum(receive_amt) receive_amt,
  sum(amtfenzi)/sum(receive_amt) receive_ratio
   -- b.target_tax,
   --if(receive_amt=0 or b.target_tax=0,0,(nvl(amtfenzi,0)/receive_amt)/b.target_tax)   wancheng_lv,
  -- 0.1 as quanz,
 --  if(receive_amt=0 or b.target_tax=0,0,(nvl(amtfenzi,0)/receive_amt)/b.target_tax*10)
from (
select
   a.performance_region_name,
   a.performance_province_name,
    a.dc_code,
   -- a.classify_middle_code,
   a.classify_middle_name,
    sum(if(is_central_tag='1' or order_business_type='1' or csx_purchase_level_code='03',receive_amt,0)) amtfenzi,   
    sum(receive_amt) receive_amt
from 
(
 select 
   performance_region_name,
   performance_province_name,
   dc_code,
  case
  when classify_middle_code in ('B0305','B0302','B0301','B0201','B0202','B0303','B0306' ) then classify_middle_name 
  -- when classify_middle_name in ('调理预制品','猪肉','家禽','水果','蔬菜','水产','牛羊') then classify_middle_name
  when classify_large_code in ('B01') then '干货加工'
	 else '食百' end  as classify_middle_name,
  csx_purchase_level_code,  -- 01-全国商品,02-一般商品,03-oem商品
   order_business_type, -- 业务类型 1 基地订单
   is_central_tag, --品类+供应商集采
    sum(receive_amt) receive_amt
 from 
 csx_analyse.csx_analyse_scm_purchase_order_flow_di
 where sdt>='20240701' and sdt<='20240930'
  AND order_code like 'IN%'
group by performance_region_name,
   performance_province_name,
   dc_code,
   -- classify_middle_code,
   case when classify_middle_code in ('B0305', 'B0302', 'B0301', 'B0201', 'B0202', 'B0303', 'B0306') then classify_middle_name
   -- when classify_middle_name in ('调理预制品','猪肉','家禽','水果','蔬菜','水产','牛羊') then classify_middle_name
        when classify_large_code in ('B01') then '干货加工'
	-- when classify_middle_name in ('米','蛋','熟食烘焙','干货加工') then  '干货加工'
	 else '食百' end,
   csx_purchase_level_code,
   order_business_type,
   is_central_tag
)a 
  join 
(
 select distinct dc_code
from
csx_ods.csx_ods_csx_data_market_conf_supplychain_location_df
)c  on a.dc_code=c.dc_code
where a.classify_middle_name<>'食百'
group by a.performance_region_name,
   a.performance_province_name,
  a.dc_code,
   a.classify_middle_name
union all   
 -------------  食百
 -- 1.采购占比食百修改
 select
   a.performance_region_name,
   a.performance_province_name,
   a.dc_code,
 --  a.classify_middle_code,
   a.classify_middle_name,
    sum(if(is_central_tag='1' or order_business_type='1' or csx_purchase_level_code='03',receive_amt,0)) amtfenzi,   
    sum(receive_amt) receive_amt
from 
(
 select 
   performance_region_name,
   performance_province_name,
   dc_code,
   'B00'  as classify_middle_code,
   '食百'  as classify_middle_name,
   csx_purchase_level_code,  -- 01-全国商品,02-一般商品,03-oem商品
   order_business_type, -- 业务类型 1 基地订单
    is_central_tag, -- 品类+供应商集采
     sum(receive_amt) receive_amt
 from 
 csx_analyse.csx_analyse_scm_purchase_order_flow_di
 where sdt>='20240701' and sdt<='20240930'
  AND order_code like 'IN%'
 and classify_large_code in ('B04', 'B05', 'B06', 'B07', 'B08', 'B09')
  --  ('酒','香烟饮料','休闲食品','面类/米粉类','调味品类','食用油类','罐头小菜','早餐冲调','常温乳品饮料','冷藏冷冻食品','清洁用品','纺织用品','家电','文体用品','家庭用品','易耗品','服装')
group by performance_region_name,
   performance_province_name,
   dc_code,
   classify_middle_code,
   classify_middle_name,
   csx_purchase_level_code,
   order_business_type,
   is_central_tag
)a 
 join 
(
 select distinct dc_code
from
csx_ods.csx_ods_csx_data_market_conf_supplychain_location_df
)c  on a.dc_code=c.dc_code
group by a.performance_region_name,
   a.performance_province_name,
   a.dc_code,
 --  a.classify_middle_code,
   a.classify_middle_name
 ) a 
 left join 
 (select shop_code,basic_performance_region_name,basic_performance_province_code,	basic_performance_province_name	,basic_performance_city_code,	basic_performance_city_name 
 from csx_dim.csx_dim_shop where sdt='current') d on a.dc_code=d.shop_code
--  left join 
--  (select * 
--  from csx_analyse.csx_analyse_purchase_classify_target1_df
-- where months='202311') b on a.performance_province_name=b.province_name  
and a.classify_middle_name=a.classify_middle_name 
group by  basic_performance_region_name,
  -- basic_performance_province_code,	
   basic_performance_province_name	,
  -- basic_performance_city_code,	
   basic_performance_city_name,
   a.classify_middle_name;
  


-- 采购DC占比 汇总
 
select
   basic_performance_region_name,
  -- basic_performance_province_code,	
   basic_performance_province_name	,
  -- basic_performance_city_code,	
   basic_performance_city_name,
   a.classify_middle_name,
   a.dc_code,
  -- if(receive_amt=0,0,nvl(amtfenzi,0)/receive_amt) zhanbi,
  sum(amtfenzi) amtfenzi,
  sum(receive_amt) receive_amt,
  sum(amtfenzi)/sum(receive_amt) receive_ratio
   -- b.target_tax,
   --if(receive_amt=0 or b.target_tax=0,0,(nvl(amtfenzi,0)/receive_amt)/b.target_tax)   wancheng_lv,
  -- 0.1 as quanz,
 --  if(receive_amt=0 or b.target_tax=0,0,(nvl(amtfenzi,0)/receive_amt)/b.target_tax*10)
from (
select
   a.performance_region_name,
   a.performance_province_name,
    a.dc_code,
   -- a.classify_middle_code,
   a.classify_middle_name,
    sum(if(is_central_tag='1' or order_business_type='1' or csx_purchase_level_code='03',receive_amt,0)) amtfenzi,   
    sum(receive_amt) receive_amt
from 
(
 select 
   performance_region_name,
   performance_province_name,
   dc_code,
   case when classify_middle_code in ('B0305', 'B0302', 'B0301', 'B0201', 'B0202', 'B0303', 'B0306') then classify_middle_name
   -- when classify_middle_name in ('调理预制品','猪肉','家禽','水果','蔬菜','水产','牛羊') then classify_middle_name
        when classify_large_code in ('B01') then '干货加工'
	-- when classify_middle_name in ('米','蛋','熟食烘焙','干货加工') then  '干货加工'
	 else '食百' end  as classify_middle_name,
  csx_purchase_level_code,  -- 01-全国商品,02-一般商品,03-oem商品
   order_business_type, -- 业务类型 1 基地订单
   is_central_tag, --品类+供应商集采
    sum(receive_amt) receive_amt
 from 
 csx_analyse.csx_analyse_scm_purchase_order_flow_di
 where sdt>='20240701' and sdt<='20240930'
  AND order_code like 'IN%'
group by performance_region_name,
   performance_province_name,
   dc_code,
   -- classify_middle_code,
    case when classify_middle_code in ('B0305', 'B0302', 'B0301', 'B0201', 'B0202', 'B0303', 'B0306') then classify_middle_name
   -- when classify_middle_name in ('调理预制品','猪肉','家禽','水果','蔬菜','水产','牛羊') then classify_middle_name
        when classify_large_code in ('B01') then '干货加工'
	-- when classify_middle_name in ('米','蛋','熟食烘焙','干货加工') then  '干货加工'
	 else '食百' end,
   csx_purchase_level_code,
   order_business_type,
   is_central_tag
)a 
  join 
(
 select distinct dc_code
from
csx_ods.csx_ods_csx_data_market_conf_supplychain_location_df
)c  on a.dc_code=c.dc_code
where a.classify_middle_name<>'食百'
group by a.performance_region_name,
   a.performance_province_name,
  a.dc_code,
   a.classify_middle_name
union all   
 -------------  食百
 -- 1.采购占比食百修改
 select
   a.performance_region_name,
   a.performance_province_name,
   a.dc_code,
 --  a.classify_middle_code,
   a.classify_middle_name,
    sum(if(is_central_tag='1' or order_business_type='1' or csx_purchase_level_code='03',receive_amt,0)) amtfenzi,   
    sum(receive_amt) receive_amt
from 
(
 select 
   performance_region_name,
   performance_province_name,
   dc_code,
   'B00'  as classify_middle_code,
   '食百'  as classify_middle_name,
   csx_purchase_level_code,  -- 01-全国商品,02-一般商品,03-oem商品
   order_business_type, -- 业务类型 1 基地订单
    is_central_tag, -- 品类+供应商集采
     sum(receive_amt) receive_amt
 from 
 csx_analyse.csx_analyse_scm_purchase_order_flow_di
 where sdt>='20240701' and sdt<='20240930'
   AND order_code like 'IN%'
   and classify_large_code in ('B04', 'B05', 'B06', 'B07', 'B08', 'B09')
 group by performance_region_name,
   performance_province_name,
   dc_code,
   classify_middle_code,
   classify_middle_name,
   csx_purchase_level_code,
   order_business_type,
   is_central_tag
)a 
 join 
(
 select distinct dc_code
from
csx_ods.csx_ods_csx_data_market_conf_supplychain_location_df
)c  on a.dc_code=c.dc_code
group by a.performance_region_name,
   a.performance_province_name,
   a.dc_code,
 --  a.classify_middle_code,
   a.classify_middle_name
 ) a 
 left join 
 (select shop_code,basic_performance_region_name,basic_performance_province_code,	basic_performance_province_name	,basic_performance_city_code,	basic_performance_city_name 
 from csx_dim.csx_dim_shop where sdt='current') d on a.dc_code=d.shop_code
--  left join 
--  (select * 
--  from csx_analyse.csx_analyse_purchase_classify_target1_df
-- where months='202311') b on a.performance_province_name=b.province_name  
and a.classify_middle_name=a.classify_middle_name 
group by  a.dc_code,basic_performance_region_name,
  -- basic_performance_province_code,	
   basic_performance_province_name	,
  -- basic_performance_city_code,	
   basic_performance_city_name,
   a.classify_middle_name;
  
  -- 日配+商超销售汇总
   
  select
  performance_province_name,
  performance_city_name,
--  IF(performance_province_name='浙江省',concat(performance_province_name,performance_city_name),performance_province_name) performance_province_name,
  dept_name,
  classify_name,
  a.classify_middle_name,
  business_type_name,
 sales_value,
 profit,
 profit/sales_value as profit_rate,
 no_zs_sale_amt,
 no_zs_profit,
 no_zs_profit/no_zs_sale_amt as no_zs_profit_rate
 from
 (
 select
  performance_province_name,
  performance_city_name,
  business_type_name,
  if(a.classify_large_code in ('B01', 'B02', 'B03'),'生鲜','食百')  as  dept_name,
  case when classify_large_code in ('B04','B05','B06','B07','B08','B09') THEN '食百' 
        when classify_large_code='B01' then classify_large_name
        else classify_middle_name 
        end as classify_name, 
--  IF(performance_province_name='浙江省',concat(performance_province_name,performance_city_name),performance_province_name) performance_province_name,

  a.classify_middle_name,
 sum(sale_amt)as sales_value,
 sum(profit)as profit,
 sum(case when shop_code is not null then sale_amt end) no_zs_sale_amt,
 sum(case when shop_code is not null then profit end) no_zs_profit
 from csx_dws.csx_dws_sale_detail_di a
 left join 
 (select shop_code from csx_dim.csx_dim_shop where sdt='current' and shop_low_profit_flag=0) b on a.inventory_dc_code=b.shop_code
        where   sdt >= '20240701'
                and sdt <='20240930'
                and ( channel_code in ('2') or business_type_code=1 )
                and inventory_dc_code not in ('W0J2')
group by     performance_province_name,
  performance_city_name, 
  business_type_name,
   if(a.classify_large_code in ('B01', 'B02', 'B03'),'生鲜','食百') ,
  a.classify_middle_name,
  case when classify_large_code in ('B04','B05','B06','B07','B08','B09') THEN '食百' 
        when classify_large_code='B01' then classify_large_name
        else classify_middle_name 
        end 
  )  a ;

 -- 销售明细
   
  select month,
  performance_province_name,
  performance_city_name,
  dept_name,
    classify_name,
  a.classify_middle_name,
  business_type_name,
  inventory_dc_code,
  shop_name,
  customer_code,
  customer_name,
--  IF(performance_province_name='浙江省',concat(performance_province_name,performance_city_name),performance_province_name) performance_province_name,
 sales_value,
 profit,
 profit/sales_value as profit_rate,
 no_zs_sale_amt,
 no_zs_profit,
 no_zs_profit/no_zs_sale_amt as no_zs_profit_rate
 from
 (
 select
 substr(sdt,1,6) month,
  performance_province_name,
  performance_city_name,
  inventory_dc_code,
  shop_name,
  business_type_name,
  customer_code,
  customer_name,
--  IF(performance_province_name='浙江省',concat(performance_province_name,performance_city_name),performance_province_name) performance_province_name,
   if(a.classify_large_code in ('B01', 'B02', 'B03'),'生鲜','食百')  as  dept_name,
  case when classify_large_code in ('B04','B05','B06','B07','B08','B09') THEN '食百' 
        when classify_large_code='B01' then classify_large_name
        else classify_middle_name 
        end as classify_name, 
        a.classify_middle_name,
 sum(sale_amt)as sales_value,
 sum(profit)as profit,
 sum(case when shop_low_profit_flag=0 then sale_amt end) no_zs_sale_amt,
 sum(case when shop_low_profit_flag=0 then profit end) no_zs_profit
 from csx_dws.csx_dws_sale_detail_di a
 left join 
 (select shop_code,shop_name,shop_low_profit_flag from csx_dim.csx_dim_shop 
    where sdt='current' 
    --and shop_low_profit_flag=0
    ) b on a.inventory_dc_code=b.shop_code
        where   sdt >= '20240701'
                and sdt <='20240930'
                and ( channel_code in ('2') or business_type_code=1 )
             --   and inventory_dc_code not in ('W0J2')
group by   substr(sdt,1,6),  customer_code,
  customer_name,
  performance_province_name,
  performance_city_name, 
  case when classify_large_code in ('B04','B05','B06','B07','B08','B09') THEN '食百' 
        when classify_large_code='B01' then classify_large_name
        else classify_middle_name 
        end,
   if(a.classify_large_code in ('B01', 'B02', 'B03'),'生鲜','食百') ,
  a.classify_middle_name,
  inventory_dc_code,
  business_type_name,
  shop_name)
  a ;
  
-- 客诉明细
 
  select
    a.complaint_code,
    complaint_source_name,
    sale_order_code,
    substr(sdt, 1, 6) smonth,
    -- 客诉日期
    performance_region_name,
    performance_province_name,
    performance_city_name,
    inventory_dc_code,
    customer_code,
    customer_name,
    require_delivery_date,
    first_level_department_name,
    second_level_department_name,
    if(
      second_level_department_name in ('交易支持', '非食用品'),
      second_level_department_name,
      classify_middle_code
    ) new_classify_middle_code,
    complaint_level_name,
    goods_code,
    goods_name,
    generate_reason,
    main_category_name,
    case when a.classify_large_code in ('B04','B05','B06','B07','B08','B09') THEN '食百' 
        when a.classify_large_code='B01' then classify_large_name
    else classify_middle_name    end as classify_name, 
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    complaint_amt --- 客诉金额
  from
    csx_analyse.csx_analyse_fr_oms_complaint_detail_new_di a
    join csx_analyse_tmp.complaint_code_list_use b on a.complaint_code = b.complaint_code
  where
    sdt >= '20240701'
    and sdt <= '20240930'
    and complaint_status_code in (20, 30) -- 客诉状态: 10-待处理 20-已处理待确认 21-驳回待确认  30-已处理 -1-已取消
    and complaint_deal_status in (10, 40) -- 责任环节状态 10'待处理' 20'待修改' 30'已处理待审' 31'已驳回待审核' 40'已完成' -1'已取消'
    and second_level_department_name != ''
    and first_level_department_name = '采购'
    and complaint_level != '3'
    and complaint_amt <> 0
    and second_level_department_name not in ('交易支持', '非食用品')
  --  and complaint_source_code <> 2 --	客诉来源:1-单独发起客诉单 2-客退单生成 3-补货单生成
    --    and second_level_department_name not like '%地采%'
 ;
 -- 采购占比明细

 
 select 
   performance_region_name,
   performance_province_name,
   dc_code,
   supplier_code,
   supplier_name,
    case when classify_middle_code in ('B0305', 'B0302', 'B0301', 'B0201', 'B0202', 'B0303', 'B0306') then classify_middle_name
   -- when classify_middle_name in ('调理预制品','猪肉','家禽','水果','蔬菜','水产','牛羊') then classify_middle_name
        when classify_large_code in ('B01') then '干货加工'
	-- when classify_middle_name in ('米','蛋','熟食烘焙','干货加工') then  '干货加工'
	 else '食百' end  as classify_middle_name,
   if(csx_purchase_level_code='03','OEM','一般') csx_purchase_level_name, -- 01-全国商品,02-一般商品,03-oem商品
   if(order_business_type='1','基地订单','普通')order_business_type_name, -- 业务类型 1 基地订单
   if(is_central_tag='1','集采','') is_central_tag, --品类+供应商集采
    sum(receive_amt) receive_amt
 from 
   csx_analyse.csx_analyse_scm_purchase_order_flow_di
 where sdt>='20240701' and sdt<='20240930'
  AND order_code like 'IN%'
 -- and classify_large_name!='其他'
  and is_supply_stock_tag=1
group by performance_region_name,
   performance_province_name,
   dc_code,
   -- classify_middle_code,
    case when classify_middle_code in ('B0305', 'B0302', 'B0301', 'B0201', 'B0202', 'B0303', 'B0306') then classify_middle_name
   -- when classify_middle_name in ('调理预制品','猪肉','家禽','水果','蔬菜','水产','牛羊') then classify_middle_name
        when classify_large_code in ('B01') then '干货加工'
	-- when classify_middle_name in ('米','蛋','熟食烘焙','干货加工') then  '干货加工'
	 else '食百' end ,
   if(csx_purchase_level_code='03','OEM','一般') ,
    if(order_business_type='1','基地订单','普通'),
   if(is_central_tag='1','集采',''),supplier_code,
   supplier_name
;


-- 采购占比增加直供20240731


select months,
   a.performance_region_name,
   a.performance_province_name,
   a.dc_code,
   classify_large_name,
   classify_middle_name,
   is_central_tag,
   order_business_type,
   csx_purchase_level_code,
   is_direct,
   a.supplier_code,
   supplier_name,
   sum(if(is_central_tag='1' or order_business_type='1' or csx_purchase_level_code='03' or is_direct='是',receive_amt,0)) amtfenzi,   
   sum(receive_amt) receive_amt
from 
(
 select 
   substr(sdt,1,6) months,
   performance_region_name,
   performance_province_name,
   dc_code,
   classify_large_name,
  case
  when classify_middle_code in ('B0305','B0302','B0301','B0201','B0202','B0303','B0306' ) then classify_middle_name 
  -- when classify_middle_name in ('调理预制品','猪肉','家禽','水果','蔬菜','水产','牛羊') then classify_middle_name
  when classify_large_code in ('B01') then '干货加工'
	 else '食百' end  as classify_middle_name,
  csx_purchase_level_code,  -- 01-全国商品,02-一般商品,03-oem商品
   order_business_type, -- 业务类型 1 基地订单
   is_central_tag, --品类+供应商集采
   a.supplier_code,
   supplier_name,
   is_direct,
   sum(receive_amt) receive_amt
 from 
 csx_analyse.csx_analyse_scm_purchase_order_flow_di a 
 left join 
 (select * from csx_analyse_tmp.csx_analyse_tmp_direct_supplier_info_20240731)b on a.supplier_code=b.supplier_code
 where sdt>='20240701' and sdt<='20240930'
  AND order_code like 'IN%'
group by substr(sdt,1,6),
    performance_region_name,
   performance_province_name,
   dc_code,
   classify_large_name,
   -- classify_middle_code,
   case when classify_middle_code in ('B0305', 'B0302', 'B0301', 'B0201', 'B0202', 'B0303', 'B0306') then classify_middle_name
   -- when classify_middle_name in ('调理预制品','猪肉','家禽','水果','蔬菜','水产','牛羊') then classify_middle_name
        when classify_large_code in ('B01') then '干货加工'
	-- when classify_middle_name in ('米','蛋','熟食烘焙','干货加工') then  '干货加工'
	 else '食百' end,
   csx_purchase_level_code,
   order_business_type,
   is_central_tag,
   a.supplier_code,
   supplier_name,
   is_direct
)a 
  join 
(
 select distinct dc_code
from
csx_ods.csx_ods_csx_data_market_conf_supplychain_location_df
)c  on a.dc_code=c.dc_code
group by months,
   a.performance_region_name,
   a.performance_province_name,
   a.dc_code,
   classify_large_name,
   classify_middle_name,
   is_central_tag,
   order_business_type,
   csx_purchase_level_code,
   is_direct,
   a.supplier_code,
   supplier_name