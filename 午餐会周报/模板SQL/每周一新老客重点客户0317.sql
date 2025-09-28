------------------------- 全国新老客毛利（月至今）----------------------

select
  a.region_code,
  a.region_name,
  a.province_code,
  a.province_name,
  a.city_group_code,
  a.city_group_name,
    sum(case when a.sdt >= regexp_replace(trunc('${i_sdate}','MM'),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',0),'-','') then a.sales_value end) sales_value,
	sum(case when  a.sdt >= regexp_replace(trunc('${i_sdate}','MM'),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',0),'-','') then a.profit end) profit,	
    sum(case when substr(c.first_sales_date,1,6)=substr(a.sdt,1,6)  and a.sdt >= regexp_replace(trunc('${i_sdate}','MM'),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',0),'-','') then a.sales_value end) xby_sales_value,
	sum(case when substr(c.first_sales_date,1,6)=substr(a.sdt,1,6)  and a.sdt >= regexp_replace(trunc('${i_sdate}','MM'),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',0),'-','') then a.profit end) xby_profit,	
	sum(case when substr(c.first_sales_date,1,6)=substr(a.sdt,1,6)  and a.sdt >= regexp_replace(add_months(trunc('${i_sdate}','MM'),-1),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',-1),'-','') then a.sales_value end) xsy_sales_value,
	sum(case when substr(c.first_sales_date,1,6)=substr(a.sdt,1,6)  and a.sdt >= regexp_replace(add_months(trunc('${i_sdate}','MM'),-1),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',-1),'-','') then a.profit end) xsy_profit,		
	sum(case when substr(c.first_sales_date,1,6)<substr(a.sdt,1,6)  and a.sdt >= regexp_replace(trunc('${i_sdate}','MM'),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',0),'-','') then a.sales_value end) by_sales_value,
	sum(case when substr(c.first_sales_date,1,6)<substr(a.sdt,1,6)  and a.sdt >= regexp_replace(trunc('${i_sdate}','MM'),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',0),'-','') then a.profit end) by_profit,	
	sum(case when substr(c.first_sales_date,1,6)<substr(a.sdt,1,6)  and a.sdt >= regexp_replace(add_months(trunc('${i_sdate}','MM'),-1),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',-1),'-','') then a.sales_value end) sy_sales_value,
	sum(case when substr(c.first_sales_date,1,6)<substr(a.sdt,1,6)  and a.sdt >= regexp_replace(add_months(trunc('${i_sdate}','MM'),-1),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',-1),'-','') then a.profit end) sy_profit	
from
(
  select
   sdt,
    performance_region_code        region_code
        ,performance_region_name        region_name
		,performance_province_code      province_code
		,performance_province_name      province_name
	    ,performance_city_code     city_group_code
		,performance_city_name     city_group_name,
	customer_code,
    sum(sale_amt)as sales_value,
    sum(profit)as profit
  from
  (
    select * from csx_dws.csx_dws_sale_detail_di
  where   sdt >= regexp_replace(add_months(trunc('${i_sdate}','MM'),-1),'-','') 
  and sdt <= regexp_replace(add_months('${i_sdate}',0),'-','') 
  and business_type_code='1'
  ) a 
 left join (select distinct shop_code 
				from csx_dim.csx_dim_shop 
				where sdt='current'  and  shop_low_profit_flag=1   --  低毛利DC标识(1-是,0-否)
				)c
        on a.inventory_dc_code = c.shop_code
 where c.shop_code is null
 and inventory_dc_code not in ('W0AJ','W0G6','WB71','W0J2') 
                    -- 3海军仓 和 监狱仓W0J2       
  -- and performance_city_name not in  	  ('南平市','三明市','宁德市','龙岩市','东北','黔江区','宁波市','台州市')   
  group by  sdt,
         performance_region_code 
        ,performance_region_name   
		,performance_province_code 
		,performance_province_name 
	    ,performance_city_code     
		,performance_city_name     
		,customer_code
)a
left join  -- 首单日期
(
  select customer_code,min(first_business_sale_date) first_sales_date
  from csx_dws.csx_dws_crm_customer_business_active_di
  where sdt ='current' and 	business_type_code=1
  group by customer_code
)c on c.customer_code=a.customer_code 
group by  a.region_code,
  a.region_name,
  a.province_code,
  a.province_name,
  a.city_group_code,
  a.city_group_name

 ;
  

 -------------------------- 
 -------------------------- 月至今老客省区top20客户毛利数据汇总
select
 a.region_code
,a.region_name
,a.province_code
,a.province_name
,a.city_group_code
,a.city_group_name
,count(c.customer_code) ,
count(if(c.customer_code is not null and by_profit/abs(by_sales_value)-sy_profit/abs(sy_sales_value)<=-0.02 ,c.customer_code,null) ),
sum(by_sales_value),
sum(if(c.customer_code is not null,by_sales_value,0)),
sum(if(c.customer_code is not null,by_profit,0)),	
sum(if(c.customer_code is not null,by_profit,0))/abs(sum(if(c.customer_code is not null,by_sales_value,0))),
sum(if(c.customer_code is not null,by_sales_value,0))/sum(by_sales_value),
sum(sy_sales_value),
sum(if(c.customer_code is not null,sy_sales_value,0)),
sum(if(c.customer_code is not null,sy_profit,0)),		 
sum(if(c.customer_code is not null,sy_profit,0))/abs(sum(if(c.customer_code is not null,sy_sales_value,0))),
sum(if(c.customer_code is not null,sy_sales_value,0))/sum(sy_sales_value)
from
(
  select
    performance_region_code        region_code
        ,performance_region_name        region_name
		,performance_province_code      province_code
		,performance_province_name      province_name
	    ,performance_city_code     city_group_code
		,performance_city_name     city_group_name,
	customer_code,
    sum(case when  a.sdt >= regexp_replace(trunc('${i_sdate}','MM'),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',0),'-','') then a.sale_amt end) by_sales_value,
	sum(case when a.sdt >= regexp_replace(trunc('${i_sdate}','MM'),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',0),'-','') then a.profit end) by_profit,	
	sum(case when a.sdt >= regexp_replace(add_months(trunc('${i_sdate}','MM'),-1),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',-1),'-','') then a.sale_amt end) sy_sales_value,
	sum(case when a.sdt >= regexp_replace(add_months(trunc('${i_sdate}','MM'),-1),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',-1),'-','') then a.profit end) sy_profit	
  
  from
  (
    select * from csx_dws.csx_dws_sale_detail_di
  where   sdt >= regexp_replace(add_months(trunc('${i_sdate}','MM'),-1),'-','') 
  and sdt <= regexp_replace(add_months('${i_sdate}',0),'-','') 
  and business_type_code='1'
  ) a 
 left join (select distinct shop_code 
				from csx_dim.csx_dim_shop 
				where sdt='current'  and  shop_low_profit_flag=1   --  低毛利DC标识(1-是,0-否)
				)c
        on a.inventory_dc_code = c.shop_code
 where c.shop_code is null
  group by  
         performance_region_code 
        ,performance_region_name   
		,performance_province_code 
		,performance_province_name 
	    ,performance_city_code     
		,performance_city_name     
		,customer_code
)a  
left join
 (select * from (select
province_code,
province_name,
customer_code,
row_number() over(partition by province_name order by sales_value desc) ook
from ( select
		performance_province_code      province_code
		,performance_province_name      province_name,

	a.customer_code,
    sum(sale_amt)as sales_value
  from
  (
    select * from csx_dws.csx_dws_sale_detail_di
  where  sdt >= regexp_replace(trunc('${i_sdate}','MM'),'-','') 
  and business_type_code='1'
  ) a 
 left join (select distinct shop_code 
				from csx_dim.csx_dim_shop 
				where sdt='current'  and  shop_low_profit_flag=1   --  低毛利DC标识(1-是,0-否)
				)c
        on a.inventory_dc_code = c.shop_code
left join  -- 首单日期
(
  select customer_code,substr(first_business_sale_date,1,6) first_sales_date
  from csx_dws.csx_dws_crm_customer_business_active_di
  where sdt ='current' and 	business_type_code=1
)d on d.customer_code=a.customer_code 
 where c.shop_code is null and d.first_sales_date<>substr(regexp_replace(trunc('${i_sdate}','MM'),'-',''),1,6)
group by performance_province_code 
		,performance_province_name   
		,a.customer_code)a
		)c
where 	ook<=20)c	on a.customer_code=c.customer_code
group by  a.region_code
,a.region_name
,a.province_code
,a.province_name
,a.city_group_code
,a.city_group_name;

------------------------- 明细老客top20

select
  a.city_group_name  as `城市`,
  a.customer_code  as `客户编码`,
  a.customer_name as `客户名称`,
  by_sales_value/10000 as `月至今销售额(万元)`,	
  by_profit/abs(by_sales_value)  as `月至今毛利率`,
  sy_profit/abs(sy_sales_value),
  by_profit/abs(by_sales_value)-sy_profit/abs(sy_sales_value)  as `毛利率环比`
from
(
select
a.*,
row_number() over(order by by_sales_value desc) ook
from ( select
		    performance_region_code        region_code
        ,performance_region_name        region_name
		,performance_province_code      province_code
		,performance_province_name      province_name
	    ,performance_city_code     city_group_code
		,performance_city_name     city_group_name,
	a.customer_code,d.customer_name,
    sum(case when  a.sdt >= regexp_replace(trunc('${i_sdate}','MM'),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',0),'-','') then a.sale_amt end) by_sales_value,
	sum(case when a.sdt >= regexp_replace(trunc('${i_sdate}','MM'),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',0),'-','') then a.profit end) by_profit,	
	sum(case when a.sdt >= regexp_replace(add_months(trunc('${i_sdate}','MM'),-1),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',-1),'-','') then a.sale_amt end) sy_sales_value,
	sum(case when a.sdt >= regexp_replace(add_months(trunc('${i_sdate}','MM'),-1),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',-1),'-','') then a.profit end) sy_profit	
  
  from
  (
    select * 
	from csx_dws.csx_dws_sale_detail_di
    where  sdt >=regexp_replace(add_months(trunc('${i_sdate}','MM'),-1),'-','')  
	  and sdt<= regexp_replace(add_months('${i_sdate}',0),'-','')
      and business_type_code='1'
      and inventory_dc_code<>'W0J2'
      and performance_city_name not in 
	  ('南平市','三明市','宁德市','龙岩市','东北','黔江区','宁波市','台州市')
   ) a 
 left join (
             select distinct shop_code 
			 from csx_dim.csx_dim_shop 
			 where sdt='current'  and  shop_low_profit_flag=1   --  低毛利DC标识(1-是,0-否)
			)c
        on a.inventory_dc_code = c.shop_code
left join  -- 首单日期
(
  select customer_code,customer_name,substr(first_business_sale_date,1,6) first_sales_date
  from csx_dws.csx_dws_crm_customer_business_active_di
  where sdt ='current' and 	business_type_code=1
)d on d.customer_code=a.customer_code 
 where c.shop_code is null and d.first_sales_date<>substr(regexp_replace(trunc('${i_sdate}','MM'),'-',''),1,6)
 
group by performance_region_code   
        ,performance_region_name   
		,performance_province_code 
		,performance_province_name 
	    ,performance_city_code     
		,performance_city_name     
	,a.customer_code,d.customer_name)a
WHERE    by_profit/abs(by_sales_value)-sy_profit/abs(sy_sales_value)<=-0.02 
		) a		
WHERE ook<=20
;

-------------------------明细新客top20

select
a.city_group_name  as `城市`,
a.customer_code  as `客户编码`,
a.customer_name as `客户名称`,
sales_value/10000 as `月至今销售额(万元)`,	 
sy_profit/abs(sy_sales_value) as `第一周`,
by_profit/abs(by_sales_value)  as `第二周`
-- xz_profit/abs(xz_sales_value) as `第三周`,
-- xxz_profit/abs(xxz_sales_value) as `第四周`,
-- xxxz_profit/abs(xxxz_sales_value) as `第五周`
from
(select
a.*,
row_number() over(order by sales_value desc) ook
from ( 
select
	performance_region_code        region_code
    ,performance_region_name        region_name
	,performance_province_code      province_code
	,performance_province_name      province_name
	,performance_city_code     city_group_code
	,performance_city_name     city_group_name,
	a.customer_code,
	d.customer_name,
	sum(a.sale_amt) sales_value,
	sum(case when a.sdt >='20230701'  and a.sdt <= '20230707'    then a.sale_amt end) sy_sales_value,
	sum(case when a.sdt >='20230701'  and a.sdt <= '20230707'   then a.profit end) sy_profit,	
    sum(case when  a.sdt>='20230708'  and a.sdt <='20230714'    then a.sale_amt end) by_sales_value,
	sum(case when a.sdt >='20230708'  and a.sdt <='20230714'    then a.profit end) by_profit
    -- sum(case when a.sdt >='20230610'  and a.sdt <='20230616'    then a.sale_amt end) xz_sales_value,
    -- sum(case when a.sdt >='20230610'  and a.sdt <= '20230616'   then a.profit end) xz_profit,
    -- sum(case when a.sdt >='20230617'  and a.sdt <='20230623'    then a.sale_amt end) xxz_sales_value,
	-- sum(case when a.sdt >='20230617'  and a.sdt <='20230623'   then a.profit end) xxz_profit,
    -- sum(case when a.sdt >='20230624'  and a.sdt <='20230630'    then a.sale_amt end) xxxz_sales_value,
	-- sum(case when a.sdt >='20230624'  and a.sdt <='20230630'   then a.profit end) xxxz_profit	
  
  from
  (
    select * from csx_dws.csx_dws_sale_detail_di
  where  sdt >='20230701' and sdt<='20230714'
  and business_type_code='1'
  and inventory_dc_code<>'W0J2'
  and performance_city_name not in ('南平市',
'三明市',
'宁德市',
'龙岩市',
'东北',
'黔江区',
'宁波市',
'台州市')
  ) a 
 left join (select distinct shop_code 
				from csx_dim.csx_dim_shop 
				where sdt='current'  and  shop_low_profit_flag=1   --  低毛利DC标识(1-是,0-否)
				)c
        on a.inventory_dc_code = c.shop_code
left join  -- 首单日期
(
  select customer_code,customer_name,substr(first_business_sale_date,1,6) first_sales_date
  from csx_dws.csx_dws_crm_customer_business_active_di
  where sdt ='current' and 	business_type_code=1
)d on d.customer_code=a.customer_code 
 where c.shop_code is null and d.first_sales_date='202307'

group by performance_region_code   
        ,performance_region_name   
		,performance_province_code 
		,performance_province_name 
	    ,performance_city_code     
		,performance_city_name     
	,a.customer_code,d.customer_name)a
		) a		
WHERE ook<=20
order by  sales_value desc;


---------------------------------------------------------

-------毛利整体监控报表临时数据获取
select
  a.region_name
  ,a.province_name
  ,if(a.city_group_name in ('杭州市','宁波市'),a.city_group_name,'-') city_group_name
  sum(case when a.sdt >= '20230301'   then a.sales_value end) by_sales_value,
  sum(case when a.sdt >= '20230301'   then a.profit end) by_profit,

  sum(case when a.sdt >= '20230201'  and a.sdt <= '20230213'  then a.sales_value end) sy_sales_value,
  sum(case when a.sdt >= '20230201'  and a.sdt <= '20230213'  then a.profit end) sy_profit,

  sum(case when a.week=10 then a.sales_value end) bz_sales_value,
  sum(case when a.week=10 then a.profit end) bz_profit,

  sum(case when a.week=9 then a.sales_value end) sz_sales_value,
  sum(case when a.week=9 then a.profit end) sz_profit

from 
(
  select
   performance_region_name region_name
	,performance_province_name province_name
    ,performance_city_name city_group_name
	,sdt
	,substr(sdt,1,6) smonth
    weekofyear(date_sub(to_date(from_unixtime(unix_timestamp(sdt,'yyyyMMdd'))),-2)) week, -- 周（周六-周五）常规周一-周日
	,sale_amt as sales_value
	,profit
  from csx_dws.csx_dws_sale_detail_di
  where sdt >='20230201' 
       and channel_code in('1','7','9')
)
group by a.region_name
  ,a.province_name
  ,if(a.city_group_name in ('杭州市','宁波市'),a.city_group_name,'-');
  
------------------------------------------------------  

select
*
from (
select
*,
row_number() over(partition by city_group_name order by abs(current_sales_valuefan) desc)  as sales_no
from (select 
    performance_province_name  province_name,
    performance_city_name city_group_name,
    customer_code customer_no,
    customer_name,
    sum(sale_amt) as current_sales_value,
     sum(profit) as current_profit,
      sum(if(order_channel_code=6 ,sale_amt,0)) as current_sales_valuefan,
      sum(if(order_channel_code=6 ,profit,0)) as current_profitfan
    from csx_dws.csx_dws_sale_detail_di a 
	left join (select distinct shop_code 
				from csx_dim.csx_dim_shop 
				where sdt='current' and shop_low_profit_flag=1  -- 低毛利DC标识(1-是,0-否)
				)c
on a.inventory_dc_code = c.shop_code
where c.shop_code is null
and 
concat(substr(sdt,1,4),lpad(cast(weekofyear(date_sub(to_date(from_unixtime(unix_timestamp(sdt,'yyyyMMdd'))),-2)) as string),2,'0'))= '${week}'
      and channel_code in ('1','7','9')
    and business_type_code=1
    and performance_city_name ='${city}'
group by performance_province_name,
    performance_city_name,
    customer_code,
    customer_name)a
where current_sales_valuefan<>0)a
where sales_no<=5 order by current_sales_value desc;
--------------------------------------------------------  

	

-- 确定排线
with csx_tmp_tms_order_route as
(
  select
    a.dc_code,
    entrucking_code,
    shipped_order_code,
    a.send_date,
    a.route_id,
    max_create_time
  from
    (
      select
        dc_code,
        send_date,
        entrucking_code,
        shipped_order_code,
        route_id
      from
        csx_dwd.csx_dwd_tms_sign_shipped_order_detail_di
      where
        sdt >= ${s_100days_ago}
        and shipped_type_code = '1'
      group by
        dc_code,
        send_date,
        entrucking_code,
        shipped_order_code,
        route_id
    ) a
    left join (
      select
        warehouse_code,
        route_id,
        max(create_time) max_create_time
      from
        csx_dwd.csx_dwd_tms_route_carrier_log_di
      where
        sdt >= ${s_100days_ago}
        and operator = '排线确认'
      group by
        route_id,
        warehouse_code
    ) b on a.dc_code = b.warehouse_code
    and a.route_id = b.route_id
),


-- 按规则找出销售单
csx_tmp_sale_order as
(
  SELECT
    inventory_dc_code,
    inventory_dc_name,
    customer_code,
    order_code,
    goods_code,
    sale_unit,
    basic_unit,
    purchase_unit_rate,
    sum(sale_unit_purchase_qty) AS sale_unit_purchase_qty,
    sum(basic_unit_purchase_qty) AS basic_unit_purchase_qty,
    max(is_unit_conversion) AS is_unit_conversion,
    sum(sale_unit_send_qty) sale_unit_send_qty,
    sum(send_qty) send_qty
  FROM
    (
      SELECT
        inventory_dc_code,
        inventory_dc_name,
        customer_code,
        order_code,
        item_code,
        goods_code,
        goods_name,
        -- 销售单位
        purchase_unit_name AS sale_unit,
        -- 销售单位下单数量
        purchase_qty AS sale_unit_purchase_qty,
        -- 单位换算比例
        purchase_unit_rate,
        -- 是否单位换算
        if(purchase_unit_rate <> 1, 1, 0) AS is_unit_conversion,
        -- 基础单位
        unit_name AS basic_unit,
        -- 基础单位下单数量
        purchase_qty * purchase_unit_rate AS basic_unit_purchase_qty,
        --  基础单位发货数量
        send_qty,
        -- 销售单位数量
        sale_unit_send_qty,
        if(unit_name = purchase_unit_name, 1, 0) as unit_conversion_flag
      FROM
        csx_dwd.csx_dwd_oms_sale_order_detail_di
      WHERE
        sdt >= ${s_100days_ago}
        AND order_channel_code = 1 -- B端
        AND order_channel_detail_code <> 13 -- 非红旗
        AND order_type_code = 1 -- 正常销售单
        AND order_business_type_code = 1 -- 日配
        AND delivery_type_code = 1 -- 配送
        AND partner_type_code = 0 -- 非合伙人
        AND order_status_code IN (30, 40, 50, 60, 70) -- 30-部分发货  40-配送中  50-待确认 60-已签收  70-已完成
    ) a
    JOIN (
      -- 外部城市服务商DC，需要过滤
      SELECT
        shop_code
      FROM
        csx_dim.csx_dim_shop
      WHERE
        sdt = 'current'
        AND purpose <> '09'
    ) b ON a.inventory_dc_code = b.shop_code
  GROUP BY
    inventory_dc_code,
    inventory_dc_name,
    customer_code,
    order_code,
    goods_code,
    sale_unit,
    basic_unit,
    purchase_unit_rate
),


-- 按规则找出包裹单
csx_tmp_pakcage as
(
  SELECT
    a.delivery_date,
    a.sale_order_code,
    a.package_code,
    a.entrucking_code,
    a.goods_code,
    a.send_quantity,
    a.basic_unit,
    a.send_qty,
    cn,
    aa,
    case
      when cn = 1
      and aa >= 1 then 1
      else 0 end AS is_first_entrucking_code -- 是否首次发车单
  FROM
    (
      SELECT
        sale_order_code,
        package_code,
        entrucking_code,
        regexp_replace(substr(shipped_time, 1, 10), '-', '') AS delivery_date,
        goods_code,
        unit_name AS basic_unit,
        -- 基础单位
        sum(sale_unit_send_qty) AS send_quantity,
        -- 销售单位出库数量
        sum(send_qty) AS send_qty -- 基础单位出库数量
      FROM
        csx_dwd.csx_dwd_oms_package_order_detail_di
      WHERE
        sdt >= ${s_100days_ago}
        AND delivery_type_code = 1 -- 配送
      group by
        sale_order_code,
        package_code,
        entrucking_code,
        regexp_replace(substr(shipped_time, 1, 10), '-', ''),
        goods_code,
        unit_name
    ) a
    LEFT JOIN (
      -- 取TMS销售出库单号根据排线ID，按照更新时间，如果同一时间，未缺货 cn=1且aa>1 属于同单不同车，cn=1且aa=1属于同车同单
      select
        sale_order_code,
        entrucking_code,
        delivery_date,
        aa,
        cn,
        -- 下面union all有重复数据，做一下过滤
        row_number() over(partition by sale_order_code, delivery_date, entrucking_code order by tab) as rank
      from
        (
          select
            shipped_order_code as sale_order_code,
            entrucking_code,
            regexp_replace(send_date, '-', '') delivery_date,
            count(max_create_time) as cn,
            count(a.route_id) aa,
            0 as tab
          from
            csx_tmp_tms_order_route a
          group by
            entrucking_code,
            shipped_order_code,
            regexp_replace(send_date, '-', '')
          union all
          SELECT
            sale_order_code,
            entrucking_code,
            delivery_date,
            1 cn,
            1 aa,
            1 as tab
          FROM
            (
              SELECT
                sale_order_code,
                entrucking_code,
                row_number() OVER(
                  PARTITION BY sale_order_code
                  ORDER BY
                    shipped_time
                ) AS rank,
                min(regexp_replace(substr(shipped_time, 1, 10), '-', '')) OVER(PARTITION BY sale_order_code) AS delivery_date
              FROM
                csx_dwd.csx_dwd_oms_package_order_detail_di -- 这个表只有一个包裹单号，缺省多条数据
              WHERE
                sdt >= ${s_100days_ago}
                AND delivery_type_code = 1 -- 配送
            ) tmp
          WHERE
            rank = 1
        ) a
    ) c ON a.sale_order_code = c.sale_order_code
    AND a.delivery_date = c.delivery_date
    AND a.entrucking_code = c.entrucking_code
    AND c.rank = 1
),


csx_tmp_normal_sale_order_join_pakcage AS
(
  SELECT
    t1.inventory_dc_code,
    t1.inventory_dc_name,
    t1.customer_code,
    t1.order_code,
    t1.goods_code,
    t1.sale_unit_purchase_qty,
    t1.basic_unit_purchase_qty,
    t1.is_unit_conversion,
    t2.delivery_date,
    t2.package_code,
    t2.entrucking_code,
    t1.sale_unit,
    -- 销售单位出库数量
    t2.send_quantity,
    t2.basic_unit,
    -- 基础单位出库数量
    t2.send_qty,
    -- 是否首次发车单
    t2.is_first_entrucking_code
  FROM csx_tmp_sale_order t1
    left join (
      select
        delivery_date,
        sale_order_code,
        package_code,
        entrucking_code,
        goods_code,
        send_qty,
        send_quantity,
        basic_unit,
        is_first_entrucking_code,
        row_number() over(partition by sale_order_code, goods_code order by entrucking_code) as rank
      from
        csx_tmp_pakcage
    ) t2 ON t1.order_code = t2.sale_order_code
    AND t1.goods_code = t2.goods_code
  where
    t2.delivery_date is not null and t2.rank = 1
)


INSERT OVERWRITE TABLE csx_report.csx_report_oms_out_of_stock_goods_1d PARTITION(sdt)
SELECT
  concat_ws('&', delivery_date, package_code, a.goods_code) AS biz_id,
  performance_region_code,
  performance_region_name,
  performance_province_code,
  performance_province_name,
  performance_city_code,
  performance_city_name,
  order_code,
  package_code,
  a.customer_code,
  customer_name,
  delivery_date,
  a.inventory_dc_code,
  inventory_dc_name,
  business_division_name,
  purchase_group_code,
  purchase_group_name,
  classify_large_code,
  classify_large_name,
  classify_middle_code,
  classify_middle_name,
  classify_small_code,
  classify_small_name,
  a.goods_code,
  goods_name,
  basic_unit,
  sale_unit,
  purchase_qty,
  send_qty,
  send_qty - purchase_qty as out_of_stock_qty,
  spec,
  case
    when sale_unit in (
      '市斤',
      '公斤',
      '斤',
      'kg',
      'kG',
      'Kg',
      'KG',
      'g',
      'G',
      '千克',
      '克',
      '两'
    )
    and (send_qty - purchase_qty) / purchase_qty < -0.05 then 1
    when sale_unit not in (
      '市斤',
      '公斤',
      '斤',
      'kg',
      'kG',
      'Kg',
      'KG',
      'g',
      'G',
      '千克',
      '克',
      '两'
    )
    and send_qty < purchase_qty then 1
    else 0 end as is_out_of_stock,
  if(d.inventory_dc_code is not null, 1, 0) as is_base_goods,
  entrucking_code,
  delivery_date AS sdt
FROM
  (
    SELECT
      inventory_dc_code,
      inventory_dc_name,
      delivery_date,
      customer_code,
      order_code,
      package_code,
      entrucking_code,
      goods_code,
      sale_unit,
      basic_unit,
      is_unit_conversion,
      is_first_entrucking_code,
      sale_unit_purchase_qty,
      basic_unit_purchase_qty,
      send_quantity,
      send_qty AS send_qty_1,
      -- case when is_unit_conversion = 1 then sale_unit_purchase_qty
      --     when is_unit_conversion = 0 and basic_unit!=unit_name then sale_unit_purchase_qty
      --     else basic_unit_purchase_qty end  AS purchase_qty,
      case
        when basic_unit in (
          '市斤',
          '公斤',
          '斤',
          'kg',
          'kG',
          'Kg',
          'KG',
          'g',
          'G',
          '千克',
          '克',
          '两'
        ) then sale_unit_purchase_qty
        when is_unit_conversion = 1
        and send_quantity <> 0 then sale_unit_purchase_qty
        else basic_unit_purchase_qty
      end AS purchase_qty,
      -- 单位转换 取销售单位订单数量否则取基础单位订单数量
      -- if(is_first_entrucking_code = 1, if(is_unit_conversion = 1  AND send_quantity <> 0, send_quantity, send_qty   ),  0 ) AS send_qty
      --   case when is_unit_conversion = 1 and send_quantity<>0 then send_quantity
      --         when  is_unit_conversion = 0 and basic_unit!=sale_unit and send_quantity<>0 then send_quantity
      --         else send_qty end send_qty
      case
        when basic_unit in (
          '市斤',
          '公斤',
          '斤',
          'kg',
          'kG',
          'Kg',
          'KG',
          'g',
          'G',
          '千克',
          '克',
          '两'
        )
        and send_quantity <> 0 then send_quantity
        when is_unit_conversion = 1
        and send_quantity <> 0 then send_quantity
        when is_unit_conversion = 0
        and basic_unit != sale_unit
        and send_quantity <> 0 then send_quantity
        else send_qty
      end send_qty -- basic_unit_purchase_qty AS purchase_qty,
      -- if(is_first_entrucking_code = 1, send_qty, 0) AS send_qty
    FROM
      csx_tmp_normal_sale_order_join_pakcage
    where delivery_date >= ${s_last_month_start}
  ) a
  JOIN (
    -- 只取大客户
    SELECT
      customer_code,
      customer_name,
      performance_region_code,
      performance_region_name,
      performance_province_code,
      performance_province_name,
      performance_city_code,
      performance_city_name
    FROM
      csx_dim.csx_dim_crm_customer_info
    WHERE
      sdt = 'current'
      AND customer_code <> ''
      AND channel_code = '1' -- 大客户
  ) b ON a.customer_code = b.customer_code
  left join (
    SELECT
      goods_code,
      goods_name,
      standard as spec,
      business_division_name,
      division_code,
      division_name,
      classify_large_code,
      classify_large_name,
      classify_middle_code,
      classify_middle_name,
      classify_small_code,
      classify_small_name,
      purchase_group_code,
      purchase_group_name
    FROM
      csx_dim.csx_dim_basic_goods
    WHERE
      sdt = 'current'
  ) c on a.goods_code = c.goods_code
  left join (
    select
      inventory_dc_code,
      product_code
    from
      csx_ods.csx_ods_b2b_mall_prod_yszx_dc_product_pool_df
    where
      sdt = regexp_replace(date_sub(current_date(), 1), '-', '')
      and base_product_tag = 1
  ) d on a.inventory_dc_code = d.inventory_dc_code
  and a.goods_code = d.product_code;  
	  
	  
	  
	  
	  
	  
	  
	  
	  
	  
