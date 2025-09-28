-- 全国TOP商品成本监控预警 
-- drop table csx_analyse_tmp.csx_analyse_tmp_entry_top_cost;
-- 全国TOP商品成本监控预警 
-- drop table csx_analyse_tmp.csx_analyse_tmp_entry_top_cost;
create table csx_analyse_tmp.csx_analyse_tmp_entry_top_cost as
select csx_week,
  week_rn,
  csx_week_begin,
  csx_week_end,
  sdt,
  performance_region_code,
  performance_region_name,
  performance_province_code,
  performance_province_name,
  performance_city_code,
  performance_city_name,
  dc_code,
  dc_name,
  receive_dc_code,
  settle_dc_code,
  purchase_order_code,
  order_code,
  supplier_code,
  supplier_name,
  case
    when a.goods_code in ('620', '846778') then '620'
    when a.goods_code in ('1267377', '846807', '1350237') then '1267377'
    when a.goods_code in ('537', '1065627') then '537'
    when a.goods_code in ('3695', '576') then '3695'
    when a.goods_code in ('441', '1140572') then '441'
    when a.goods_code in ('2112', '1065514') then '2112'
    when a.goods_code in ('621', '1161767') then '621'
    when a.goods_code in ('644', '846746') then '644'
    when a.goods_code in ('607', '317208') then '607'
    when a.goods_code in ('474', '846732') then '474'
    when a.goods_code in ('471', '846501') then '471'
    when a.goods_code in ('631', '846759') then '631'
    when a.goods_code in ('472', '450') then '472'
    when a.goods_code in ('531', '1236384') then '531'
    when a.goods_code in ('438', '1065529') then '438'
    when a.goods_code in ('608', '1065502') then '608'
    when a.goods_code in ('619', '846570') then '619'
    when a.goods_code in ('262352', '1325230') then '262352'
    else a.goods_code
  end goods_code,
  classify_middle_name,
  case
    WHEN classify_large_code in ('B01', 'B02', 'B03') THEN '生鲜'
    else '食百'
  end div_name,
  purchase_order_code,
  order_code,
  if (order_price2 = 0, order_price1, order_price2) as cost,
  receive_qty,
  receive_amt
from   csx_analyse.csx_analyse_scm_purchase_order_flow_di a
  join (
    select calday,
      csx_week,
      csx_week_begin,
      csx_week_end,
      dense_rank() over (
        order by csx_week desc
      ) as week_rn
    from csx_dim.csx_dim_basic_date
    where calday =regexp_replace(date_sub(current_date(),1),'-','') 
  ) b on a.sdt = b.calday
where sdt =regexp_replace(date_sub(current_date(),1),'-','') 
  and remedy_flag <> '1'
  and is_supply_stock_tag = '1'
  and super_class_code = '1'
  and source_type_code in ('1','3','9','10','13','17','19','22','23'
  ) -- and goods_code in ('846778', '620') --  
  and a.goods_code in (
    '620',
    '1267377',
    '537',
    '3695',
    '441',
    '618',
    '2112',
    '621',
    '644',
    '607',
    '474',
    '471',
    '631',
    '472',
    '531',
    '438',
    '608',
    '619',
    '262352',
    '566',
    '846778',
    '846807',
    '1065627',
    '576',
    '1140572',
    '1065514',
    '1161767',
    '846746',
    '317208',
    '846732',
    '846501',
    '846759',
    '450',
    '1236384',
    '1065529',
    '1065502',
    '846570',
    '1325230',
    '1350237',
    '1359002',
    '1588624',
    '1532629',
    '1533133',
    '1359914',
    '1359004',
    '1588624',
    '1493716',
    '1728536',
    '1359006',
    '1533153',
    '1358979',
    '1358981',
    '1359886',
    '1532629',
    '1532629',
    '1601165',
    '1533153',
    '1359000',
    '1456612',
    '8778',
    '1456631',
    '1554919',
    '1747107',
    '1015421',
    '1',
    '1186656',
    '266',
    '260832',
    '6',
    '1182675',
    '1005762',
    '2624',
    '264135',
    '1015423',
    '2567',
    '5'
  );





-- 计算生鲜周平均价 
drop table csx_analyse_tmp.csx_analyse_tmp_entry_top_cost_01;
create table csx_analyse_tmp.csx_analyse_tmp_entry_top_cost_01 as
select  csx_week,
        csx_week_begin,
        csx_week_end,
        performance_region_code,
        performance_region_name,
        performance_province_code,
        performance_province_name,
        performance_city_code,
        performance_city_name,
        dc_code,
        dc_name,
        a.goods_code,
        goods_name,
        classify_middle_name,
        sum(receive_amt) / sum(receive_qty) avg_cost,
        sum(receive_amt) receive_amt,
        sum(receive_qty) receive_qty,
        avg(receive_qty) avg_receive_qty,
        sum(sum(receive_amt)) over ( partition by csx_week ,dc_code ) qg_receive_amt,
        sum(sum(receive_qty)) over ( partition by csx_week ,dc_code ) qg_receive_qty,
        sum(avg(receive_qty)) over ( partition by csx_week ,dc_code ) qg_avg_receive_qty,
        dense_rank() over ( partition by csx_week order by  sum(receive_amt) desc ) rn
from csx_analyse_tmp.csx_analyse_tmp_entry_top_cost a
left join
(
	select  goods_code,
	        goods_name
	from csx_dim.csx_dim_basic_goods
	where sdt = 'current' 
) b
on a.goods_code = b.goods_code
group by  csx_week,
          csx_week_begin,
          csx_week_end,
          performance_region_code,
          performance_region_name,
          performance_province_code,
          performance_province_name,
          performance_city_code,
          performance_city_name,
          dc_code,
          dc_name,
          a.goods_code,
          goods_name,
          classify_middle_name;



--关联生鲜取近两周的数据周无值取上一周数据 
--drop table csx_analyse_tmp.csx_analyse_tmp_entry_top_cost_03; 

create table csx_analyse_tmp.csx_analyse_tmp_entry_top_cost_03 as
select  a.csx_week,
        a.week_rn,
-- csx_week_begin 
-- csx_week_end 
        ,,a.performance_region_code,
        a.performance_region_name,
        a.performance_province_code,
        a.performance_province_name,
        a.performance_city_code,
        a.performance_city_name,
        a.shop_code,
        a.shop_name,
        a.goods_code,
        a.goods_name,
        a.classify_middle_name,
        a.div_name,
        avg_cost,
        receive_amt,
        receive_qty,
        avg_receive_qty,
-- 当本周没有值时取上一周数据 
        lag(avg_cost ,1,0) over ( partition by shop_code ,a.goods_code order by  a.csx_week ) as last_avg_cost,
        lag(avg_receive_qty ,1,0) over ( partition by shop_code ,a.goods_code order by a.csx_week ) as last_avg_receive_qty,
        lag(receive_amt ,1,0) over ( partition by shop_code ,a.goods_code order by a.csx_week ) as last_receive_amt,
        lag(receive_qty ,1,0) over ( partition by shop_code ,a.goods_code order by a.csx_week ) as last_receive_qty,
        sum(receive_amt) over ( partition by shop_code ,a.classify_middle_name ) as shop_amt
from
(
	select  distinct csx_week,
	        week_rn ,
          -- csx_week_begin,
          -- csx_week_end ,
	        performance_region_code,
	        performance_region_name,
	        performance_province_code,
	        performance_province_name,
	        performance_city_code,
	        performance_city_name,
	        shop_code,
	        shop_name,
	        a.goods_code,
	        classify_middle_name,
	        div_name,
	        a.goods_name
	from csx_analyse_tmp.csx_analyse_tmp_entry_top_cost_00 a
	where week_rn < 3 -- 
	and goods_code in ('1267377') 
) a
left join -- 关联省区 
(
	select  csx_week,
	        dc_code,
	        dc_name,
	        a.goods_code,
	        goods_name,
	        classify_middle_name,
	        avg_cost,
	        receive_amt,
	        receive_qty,
	        avg_receive_qty,
	        rn
	from csx_analyse_tmp.csx_analyse_tmp_entry_top_cost_01 a 
  ) b
on a.csx_week = b.csx_week and a.goods_code = b.goods_code and a.shop_code = b.dc_code; 




-- 生鲜结果值 
select  a.csx_week,
        a.week_rn,
        a.performance_region_code,
        a.performance_region_name,
        a.performance_province_code,
        a.performance_province_name,
        a.performance_city_code,
        a.performance_city_name,
        a.shop_code,
        a.shop_name,
        a.goods_code,
        a.goods_name,
        a.classify_middle_name,
        a.div_name,
        avg_cost,
        receive_amt,
        receive_qty,
        avg_receive_qty,
        last_avg_cost,
        last_avg_receive_qty,
        last_receive_amt,
        last_receive_qty,
        shop_amt,
        rel_avg_cost,
        rel_avg_receive_qty,
        qg_avg_cost,
        qg_receive_qty,
        qg_receive_amt,
        qg_avg_receive_qty,
        case when rel_avg_cost / qg_avg_cost >= 0.05 then 1  else 0 end price_flag
from
(
	select  a.csx_week,
	        a.week_rn -- csx_week_begin
,-- csx_week_end 
	        ,,a.performance_region_code,
	        a.performance_region_name,
	        a.performance_province_code,
	        a.performance_province_name,
	        a.performance_city_code,
	        a.performance_city_name,
	        a.shop_code,
	        a.shop_name,
	        a.goods_code,
	        a.goods_name,
	        a.classify_middle_name,
	        a.div_name,
	        avg_cost,
	        receive_amt,
	        receive_qty,
	        avg_receive_qty,
	        last_avg_cost,
	        last_avg_receive_qty,
	        last_receive_amt,
	        last_receive_qty,
	        shop_amt,
	        if( avg_cost is null or coalesce(avg_cost ,0) = 0 ,last_avg_cost ,avg_cost ) as rel_avg_cost,
	        if( avg_receive_qty is null or coalesce(avg_receive_qty ,0) = 0 ,last_avg_receive_qty ,avg_receive_qty ) as rel_avg_receive_qty,
	        qg_avg_cost,
	        qg_receive_qty,
	        qg_receive_amt,
	        qg_avg_receive_qty
	from csx_analyse_tmp.csx_analyse_tmp_entry_top_cost_03 a
	left join
	(
		select  csx_week,
		        a.goods_code,
		        sum(receive_amt) / sum(receive_qty) qg_avg_cost,
		        sum(receive_amt) qg_receive_amt,
		        sum(receive_qty) qg_receive_qty,
		        avg(receive_qty) qg_avg_receive_qty,
		        dense_rank() over ( partition by csx_week order by  sum(receive_amt) desc ) rn
		from csx_analyse_tmp.csx_analyse_tmp_entry_top_cost a
		group by  csx_week,
		          a.goods_code
	) b
	on a.goods_code = b.goods_code and a.csx_week = b.csx_week
	where 1 = 1 -- week_rn = 1 
	and shop_amt is not null -- 过滤门店入库为null 
 
) a 
;


-- 食百 
-- 计算省区平均价 
-- drop table csx_analyse_tmp.csx_analyse_tmp_entry_12top_cost_01; 
-- 

create table csx_analyse_tmp.csx_analyse_tmp_entry_12top_cost_01 as
select  performance_region_code,
        performance_region_name,
        performance_province_code,
        performance_province_name,
        performance_city_code,
        performance_city_name,
        dc_code,
        dc_name,
        a.goods_code,
        goods_name,
        classify_middle_name,
        avg_cost,
        receive_amt,
        receive_qty,
        avg_receive_qty,
        qg_receive_amt,
        qg_receive_qty,
        qg_avg_receive_qty,
        qg_avg_cost,
        rn,
        if( (avg_cost - qg_avg_cost) / qg_avg_cost >= 0.05,1 ,0 ) cost_flag
from
(
	select  performance_region_code,
	        performance_region_name,
	        performance_province_code,
	        performance_province_name,
	        performance_city_code,
	        performance_city_name,
	        dc_code,
	        dc_name,
	        a.goods_code,
	        goods_name,
	        classify_middle_name,
	        sum(receive_amt) / sum(receive_qty) avg_cost,
	        sum(receive_amt) receive_amt,
	        sum(receive_qty) receive_qty,
	        avg(receive_qty) avg_receive_qty,
	        sum(sum(receive_amt)) over ( partition by goods_code ) qg_receive_amt,
	        sum(sum(receive_qty)) over ( partition by goods_code ) qg_receive_qty,
	        sum(avg(receive_qty)) over ( partition by goods_code ) qg_avg_receive_qty,
	        sum(sum(receive_amt)) over ( partition by goods_code ) / sum(sum(receive_qty)) over ( partition by goods_code ) qg_avg_cost,
	        dense_rank() over ( partition by 1 order by  sum(receive_amt) desc ) rn
	from csx_analyse_tmp.csx_analyse_tmp_entry_top_cost a
	left join
	(
		select  goods_code,
		        goods_name
		from csx_dim.csx_dim_basic_goods
		where sdt = 'current' 
	) b
	on a.goods_code = b.goods_code
	where div_name = '食百'
	group by  performance_region_code,
	          performance_region_name,
	          performance_province_code,
	          performance_province_name,
	          performance_city_code,
	          performance_city_name,
	          dc_code,
	          dc_name,
	          a.goods_code,
	          goods_name,
	          classify_middle_name
) a
;


-- 异常稽核 

with entry as (select  
  sdt,
  performance_region_code,
  performance_region_name,
  performance_province_code,
  performance_province_name,
  performance_city_code,
  performance_city_name,
  purchase_order_code,
  order_code,
  supplier_code,
  supplier_name,
  dc_code,
  dc_name,
  a.goods_code,
  goods_name,
  classify_middle_name,
  sum(receive_amt) / sum(receive_qty) avg_cost,
  sum(receive_amt) receive_amt,
  sum(receive_qty) receive_qty,
  avg(receive_qty) avg_receive_qty,
  SUM(SUM(receive_amt)) over (partition by performance_region_name ,goods_code ) area_receive_amt,
  SUM(SUM(receive_qty)) over (partition by performance_region_name ,goods_code ) area_receive_qty
from csx_analyse_tmp.csx_analyse_tmp_entry_top_cost a
  left join (
    select goods_code,
      goods_name
    from csx_dim.csx_dim_basic_goods
    where sdt = 'current'
  ) b on a.goods_code = b.goods_code
group by   purchase_order_code,
  order_code,
  supplier_code,
  supplier_name,
  sdt,
  performance_region_code,
  performance_region_name,
  performance_province_code,
  performance_province_name,
  performance_city_code,
  performance_city_name,
  dc_code,
  dc_name,
  a.goods_code,
  goods_name,
  classify_middle_name
  ) 
  select  sdt,
  performance_region_code,
  performance_region_name,
  performance_province_code,
  performance_province_name,
  performance_city_code,
  performance_city_name,
  purchase_order_code,
  order_code,
  supplier_code,
  supplier_name,
  dc_code,
  dc_name,
  a.goods_code,
  goods_name,
  classify_middle_name,
  avg_cost,
  receive_amt,
  receive_qty,
  area_receive_amt,
  area_receive_qty,
  area_cost,
  top_cost_flag
  from
  (
  select  sdt,
  performance_region_code,
  performance_region_name,
  performance_province_code,
  performance_province_name,
  performance_city_code,
  performance_city_name,
  purchase_order_code,
  order_code,
  supplier_code,
  supplier_name,
  dc_code,
  dc_name,
  a.goods_code,
  goods_name,
  classify_middle_name,
  avg_cost,
  receive_amt,
  receive_qty,
  area_receive_amt,
  area_receive_qty,
  coalesce(area_receive_amt/area_receive_qty,0) area_cost,
  if(avg_cost>coalesce(area_receive_amt/area_receive_qty,0),1,0) top_cost_flag
  from entry a 
  ) a 
  where top_cost_flag=1

;

 -- 肉禽水产牛羊 进价 对比
with entry as (select csx_week,
  week_rn,
  csx_week_begin,
  csx_week_end,
  sdt,
  performance_region_code,
  performance_region_name,
  performance_province_code,
  performance_province_name,
  performance_city_code,
  performance_city_name,
  dc_code,
  dc_name,
  receive_dc_code,
  settle_dc_code,
  purchase_order_code,
  order_code,
  supplier_code,
  supplier_name,
  goods_code,
  goods_name,
case when goods_name like '%前腿肉%'	then	'前腿肉'
when goods_name  like  	'%后腿肉%'	then	'后腿肉'
when goods_name  like  	'%五花肉%'	then	'五花肉'
when goods_name  like  	'%肋排%'	then	'肋排'
when goods_name  like  	'%前排%'	then	'前排'
when goods_name  like  	'%猪蹄%'	then	'猪蹄'
when goods_name  like  	'%猪肝%'	then	'猪肝'
when goods_name  like  	'%精瘦肉%'	then	'精瘦肉'
when goods_name  like  	'%肉馅%'	then	'肉馅'
when goods_name  like  	'%整鸡%'	then	'整鸡'
when goods_name  like  	'%鸡腿%'	then	'鸡腿'
when goods_name  like  	'%琵琶腿%'	then	'琵琶腿'
when goods_name  like  	'%鸡翅中%'	then	'鸡翅中'
when goods_name  like  	'%鸡全翅%'	then	'鸡全翅'
when goods_name  like  	'%鸭腿%'	then	'鸭腿'
when goods_name  like  	'%牛腩%'	then	'牛腩'
when goods_name  like  	'%牛腿肉%'	then	'牛腿肉'
when goods_name  like  	'%牛腱子%'	then	'牛腱子'
when goods_name  like  	'%海白虾%'	then	'海白虾'
end sku_name,
  classify_middle_name,
  case
    WHEN classify_large_code in ('B01', 'B02', 'B03') THEN '生鲜'
    else '食百'
  end div_name,
  purchase_order_code,
  order_code,
  if (order_price2 = 0, order_price1, order_price2) as cost,
  receive_qty,
  receive_amt
from   csx_analyse.csx_analyse_scm_purchase_order_flow_di a
  join (
    select calday,
      csx_week,
      csx_week_begin,
      csx_week_end,
      dense_rank() over (
        order by csx_week desc
      ) as week_rn
    from csx_dim.csx_dim_basic_date
    where calday =regexp_replace(date_sub(current_date(),1),'-','') 
  ) b on a.sdt = b.calday
where sdt =regexp_replace(date_sub(current_date(),1),'-','') 
  and remedy_flag <> '1'            --剔除补救
  and is_supply_stock_tag = '1'     -- 供应商仓
  and super_class_code = '1'        -- 供应商订单
  and source_type_code in ('1','3','9','10','13','17','19','22','23' )
  )
  select * from entry a 
  join 
(select goods_code,   
       goods_name,
       spu_goods_name,
       classify_middle_code,
       classify_middle_name
from   csx_dim.csx_dim_basic_goods
    where sdt = 'current'
    and classify_middle_code in ('B0302','B0306','B0301','B0303')  -- 猪肉、牛羊、家禽、水产
and (
goods_name like '%前腿肉%' or goods_name like '%后腿肉%' or goods_name like '%五花肉%' or goods_name like '%肋排%' or goods_name like '%前排%'
or goods_name like '%猪蹄%' or goods_name like '%猪肝%' or goods_name like '%精瘦肉%' or goods_name like '%肉馅%'
or goods_name like '%整鸡%' or goods_name like '%鸡腿%' or goods_name like '%琵琶腿%' or goods_name like '%鸡翅中%' or goods_name like '%鸡全翅%'
or goods_name like '%鸭腿%' or goods_name like '%牛腩%' or goods_name like '%牛腿肉%' or goods_name like '%牛腱子%'
or goods_name like '%海白虾%')
) b on a.goods_code=b.goods_code
;


-- SPU肉禽水产 横向对比 
-- drop table csx_analyse_tmp.csx_analyse_tmp_entry_b03_top_cost;
create table csx_analyse_tmp.csx_analyse_tmp_entry_b03_top_cost as
select csx_week,
  week_rn,
  csx_week_begin,
  csx_week_end,
  sdt,
  performance_region_code,
  performance_region_name,
  performance_province_code,
  performance_province_name,
  performance_city_code,
  performance_city_name,
  dc_code,
  dc_name,
  receive_dc_code,
  settle_dc_code,
  purchase_order_code,
  order_code,
  supplier_code,
  supplier_name,
  g.spu_goods_code,
  g.spu_goods_name,
  a.goods_code,
  g.goods_name,
  classify_middle_name,
  case
    WHEN classify_large_code in ('B01', 'B02', 'B03') THEN '生鲜'
    else '食百'
  end div_name,
  purchase_order_code,
  order_code,
  if (order_price2 = 0, order_price1, order_price2) as cost,
  receive_qty,
  receive_amt
from   csx_analyse.csx_analyse_scm_purchase_order_flow_di a
join 
(select goods_code,goods_name,spu_goods_code,spu_goods_name
from csx_dim.csx_dim_basic_goods
where sdt='current'
and spu_goods_code in (
                        '1',
                        '24',
                        '56',
                        '12',
                        '26',
                        '34',
                        '36',
                        '21',
                        '88',
                        '4',
                        '48',
                        '1074',
                        '5',
                        '250',
                        '1106',
                        '166',
                        '171',
                        '1158',
                        '177',
                        '1195',
                        '184',
                        '1179',
                        '186',
                        '212',
                        '189',
                        '198',
                        '183',
                        '1200',
                        '1204',
                        '301',
                        '1222',
                        '293',
                        '537',
                        '1411',
                        '1321',
                        '326',
                        '1299',
                        '516',
                        '494',
                        '1251',
                        '1375',
                        '1629',
                        '625',
                        '578',
                        '640',
                        '652',
                        '615',
                        '579',
                        '646',
                        '138',
                        '577',
                        '616',
                        '611')
)g on a.goods_code=g.goods_code
join
(
    select calday,
      csx_week,
      csx_week_begin,
      csx_week_end,
      dense_rank() over (
        order by csx_week desc
      ) as week_rn
    from csx_dim.csx_dim_basic_date
    where calday =regexp_replace(date_sub(current_date(),1),'-','') 
) b on a.sdt = b.calday
where sdt =regexp_replace(date_sub(current_date(),1),'-','') 
  and remedy_flag <> '1'
  and is_supply_stock_tag = '1'
  and super_class_code = '1'
  and source_type_code in (
    '1',
    '3',
    '9',
    '10',
    '13',
    '17',
    '19',
    '22',
    '23'
  ) -- and goods_code in ('846778', '620') --  
 

 -- 抓出高于全国均价的异常
 -- spu统计
with entry as (select  
  sdt,
  performance_region_code,
  performance_region_name,
  performance_province_code,
  performance_province_name,
  performance_city_code,
  performance_city_name,
--   purchase_order_code,
--   order_code,
--   supplier_code,
--   supplier_name,
  dc_code,
  dc_name,
  a.spu_goods_code,
  spu_goods_name,
  classify_middle_name,
  sum(receive_amt) / sum(receive_qty) avg_cost,
  sum(receive_amt) receive_amt,
  sum(receive_qty) receive_qty,
  avg(receive_qty) avg_receive_qty,
  SUM(SUM(receive_amt)) over (partition by performance_region_name ,spu_goods_code ) area_receive_amt,
  SUM(SUM(receive_qty)) over (partition by performance_region_name ,spu_goods_code ) area_receive_qty,
  SUM(SUM(receive_amt)) over (partition by spu_goods_code ) all_receive_amt,
  SUM(SUM(receive_qty)) over (partition by spu_goods_code ) all_receive_qty
from csx_analyse_tmp.csx_analyse_tmp_entry_b03_top_cost a
group by   
  sdt,
  performance_region_code,
  performance_region_name,
  performance_province_code,
  performance_province_name,
  performance_city_code,
  performance_city_name,
  dc_code,
  dc_name,
  a.spu_goods_code,
  spu_goods_name,
  classify_middle_name
  ) 
  select  sdt,
  performance_region_code,
  performance_region_name,
  performance_province_code,
  performance_province_name,
  performance_city_code,
  performance_city_name,
--  purchase_order_code,
--   order_code,
--   supplier_code,
--   supplier_name,
  dc_code,
  dc_name,
  a.spu_goods_code,
  spu_goods_name,
  classify_middle_name,
  avg_cost,
  receive_amt,
  receive_qty,
  area_receive_amt,
  area_receive_qty,
  area_cost,
  all_receive_amt,
  all_receive_qty,
  all_cost,
  top_cost_flag
  from
  (
  select  sdt,
  performance_region_code,
  performance_region_name,
  performance_province_code,
  performance_province_name,
  performance_city_code,
  performance_city_name,
--   purchase_order_code,
--   order_code,
--   supplier_code,
--   supplier_name,
  dc_code,
  dc_name,
  a.spu_goods_code,
  spu_goods_name,
  classify_middle_name,
  avg_cost,
  receive_amt,
  receive_qty,
  area_receive_amt,
  area_receive_qty,
  all_receive_amt,
  all_receive_qty,
  coalesce(area_receive_amt/area_receive_qty,0) area_cost,
  coalesce(all_receive_amt/all_receive_qty,0) all_cost,
  if(avg_cost/(coalesce(all_receive_amt/all_receive_qty,0))>0.05,1,0) top_cost_flag
  from entry a 
  ) a 
  where top_cost_flag=1

;