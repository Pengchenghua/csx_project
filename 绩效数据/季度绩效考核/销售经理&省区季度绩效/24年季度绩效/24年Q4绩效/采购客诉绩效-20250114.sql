 
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
where  sdt>='20241001' and sdt<='20241231'
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
  where  sdt>='20241001' and sdt<='20241231'
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
where  sdt>='20241001' and sdt<='20241231'
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
  where  sdt>='20241001' and sdt<='20241231'
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
    -- join csx_analyse_tmp.complaint_code_list_use b on a.complaint_code = b.complaint_code
  where
    sdt >= '20241001'
    and sdt <= '20241231'
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