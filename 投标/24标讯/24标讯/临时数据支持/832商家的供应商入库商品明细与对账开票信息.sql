-- 20240613确认 供应商对账开票信息没有到品类商品颗粒度的

-- 832 20251110更新
select a.company_code,
  c.company_name,
  a.supplier_code,
  b.supplier_name,
  substr(a.smonth, 1, 4) syear,
  substr(a.account_period, 1, 4) smonth,
  count(distinct a.smonth) count_m,
  sum(a.statement_amount) statement_amount
from (
    select company_code,
      company_name,
      supplier_code,
      supplier_name,
      bill_amt statement_amount,
      check_ticket_order_code check_ticket_no,
      merge_account_order_code merge_account_no,
      bill_code statement_no,
      payment_status_code payment_status,
      payment_order_code payment_no,
      bill_type_code statement_type,
      regexp_replace(substr(bill_time, 1, 7), '-', '') as smonth ,-- 采购结算对账单	 
      -- audit_time,     -- 票核时间
      regexp_replace(substr(account_period, 1, 7),'-','') account_period  -- 付款过账时间
    from csx_dwd.csx_dwd_pss_statement_statement_account_di
    where bill_code is not null
      and bill_code != ''
    union all
    select company_code,
      company_name,
      supplier_code,
      supplier_name,
      bill_amt statement_amount,
      check_ticket_order_code check_ticket_no,
      merge_account_order_code merge_account_no,
      merge_account_order_code as statement_no,
      payment_status_code payment_status,
      payment_order_code payment_no,
      bill_type_code statement_type,
      regexp_replace(substr(bill_time, 1, 7), '-', '') as smonth,
      -- audit_time,     -- 票核时间
      regexp_replace(substr(account_period, 1, 7),'-','')  account_period  -- 付款过账时间
    from csx_dwd.csx_dwd_pss_statement_statement_account_di
    where (
        bill_code is null
        or bill_code = ''
      )
      and merge_account_order_code is not null
      and merge_account_order_code != ''
  ) a
  left join (
    select supplier_code,
      -- 供应商编号
      supplier_name -- 供应商名称
    from csx_dim.csx_dim_basic_supplier
    where sdt = 'current'
  ) b on a.supplier_code = b.supplier_code
  left join (
    select company_code,
      company_name
    from csx_dim.csx_dim_basic_company
    where sdt = 'current'
  ) c on a.company_code = c.company_code
where a.supplier_code in('20037948',
'20041275',
'20043215',
'20043707',
'20044655',
'20044869',
'20044903',
'20044906',
'20045052',
'20045062',
'20045077',
'20045204',
'20046104',
'20051865',
'20054736',
'20054957',
'20055454',
'20058764',
'20060220',
'20060261',
'20063932',
'20063951',
'20066719',
'20067525',
'20068210',
'20068487',
'20072454',
'20072890',
'20074705',
'20076647',
'20076919'
)
group by a.company_code,
  c.company_name,
  a.supplier_code,
  b.supplier_name,
  substr(a.smonth, 1, 4),
  substr(a.account_period, 1, 4)
;

-- 832商家的供应商销售数据 20231108  从销售表取可能不准，因此从财务侧对账开票数据取
-- 财务对账数据
select a.company_code,
  c.company_name,
  a.supplier_code,
  b.supplier_name,
  substr(a.smonth, 1, 4) syear,
  substr(a.account_period, 1, 4) smonth,
  count(distinct a.smonth) count_m,
  sum(a.statement_amount) statement_amount
from (
    select company_code,
      company_name,
      supplier_code,
      supplier_name,
      bill_amt statement_amount,
      check_ticket_order_code check_ticket_no,
      merge_account_order_code merge_account_no,
      bill_code statement_no,
      payment_status_code payment_status,
      payment_order_code payment_no,
      bill_type_code statement_type,
      regexp_replace(substr(bill_time, 1, 7), '-', '') as smonth ,-- 采购结算对账单	 
      -- audit_time,     -- 票核时间
      regexp_replace(substr(account_period, 1, 7),'-','') account_period  -- 付款过账时间
    from csx_dwd.csx_dwd_pss_statement_statement_account_di
    where bill_code is not null
      and bill_code != ''
    union all
    select company_code,
      company_name,
      supplier_code,
      supplier_name,
      bill_amt statement_amount,
      check_ticket_order_code check_ticket_no,
      merge_account_order_code merge_account_no,
      merge_account_order_code as statement_no,
      payment_status_code payment_status,
      payment_order_code payment_no,
      bill_type_code statement_type,
      regexp_replace(substr(bill_time, 1, 7), '-', '') as smonth,
      -- audit_time,     -- 票核时间
      regexp_replace(substr(account_period, 1, 7),'-','')  account_period  -- 付款过账时间
    from csx_dwd.csx_dwd_pss_statement_statement_account_di
    where (
        bill_code is null
        or bill_code = ''
      )
      and merge_account_order_code is not null
      and merge_account_order_code != ''
  ) a
  left join (
    select supplier_code,
      -- 供应商编号
      supplier_name -- 供应商名称
    from csx_dim.csx_dim_basic_supplier
    where sdt = 'current'
  ) b on a.supplier_code = b.supplier_code
  left join (
    select company_code,
      company_name
    from csx_dim.csx_dim_basic_company
    where sdt = 'current'
  ) c on a.company_code = c.company_code
where a.supplier_code in(
    '20044906',
    '20044869',
    '20044903',
    '20046040',
    '20045081',
    '20044895',
    '20045077',
    '20044689',
    '20045062',
    '20045052',
    '20045179',
    '20045204',
    '20046104',
    '20044660',
    '20044655',
    '20060261',
    '20018798',
    '20044948',
    '20045948',
    '20043707',
    '20058764',
    '20059199',
    '20027725',
    '20064405',
    '20064442',
    '20068165'
  )
group by a.company_code,
  c.company_name,
  a.supplier_code,
  b.supplier_name,
  substr(a.smonth, 1, 4),
  substr(a.account_period, 1, 4)
;

-- 销售数据
/*
select 
from 
(
select substr(smonth,1,4) syear,
coalesce(a.supplier_code,b.supplier_code) supplier_code,
-- coalesce(a.supplier_name,b.supplier_name) supplier_name,
-- a.classify_middle_code,a.classify_middle_name,
a.sign_company_code,
-- a.sign_company_name,
-- a.business_type_name,
sum(sale_amt) as sale_amt,
count(distinct smonth) as count_m
from 
(
select substr(sdt,1,6) smonth,
goods_code,inventory_dc_code,
performance_region_name,
performance_province_name,
supplier_code,supplier_name,
business_type_name,
classify_middle_code,classify_middle_name,
sign_company_code,sign_company_name,
sum(sale_amt) as sale_amt,
sum(profit) as profit
from csx_dws.csx_dws_sale_detail_di
-- where sdt>='20230101'
-- and supplier_name is not null
group by substr(sdt,1,6),
goods_code,inventory_dc_code,
performance_region_name,
performance_province_name,
supplier_code,supplier_name,
business_type_name,
classify_middle_code,classify_middle_name,
sign_company_code,sign_company_name
)a
left join
(
select 
goods_code, 	 -- 商品编码
dc_code, 	 -- 门店编码
goods_name, 	 -- 商品名称
supplier_code, 	 -- 供应商编号
supplier_name 	 -- 供应商名称
from csx_dim.csx_dim_basic_dc_goods
where sdt='current'
)b on a.goods_code=b.goods_code and a.inventory_dc_code=b.dc_code
where coalesce(a.supplier_code,b.supplier_code) in(
'20044906',
'20044869',
'20044903',
'20046040',
'20045081',
'20044895',
'20045077',
'20044689',
'20045062',
'20045052',
'20045179',
'20045204',
'20046104',
'20044660',
'20044655',
'20060261',
'20018798',
'20044948',
'20045948',
'20052755',
'20043707',
'20054957',
'20058764',
'20059199',
'20027725',
'20064405',
'20064442'
)
group by substr(smonth,1,4),
coalesce(a.supplier_code,b.supplier_code),
-- coalesce(a.supplier_name,b.supplier_name),
a.sign_company_code
)a 
left join
(
select 
supplier_code, 	 -- 供应商编号
supplier_name 	 -- 供应商名称
from csx_dim.csx_dim_basic_supplier
where sdt='current'
)b on a.supplier_code=b.supplier_code
left join
(
select 
company_code,
company_name
from csx_dim.csx_dim_basic_company
where sdt='current'
)c on a.sign_company_code=c.company_code
;	
*/


-- 832商家的供应商20230701-20231111入库商品明细 20231112
select 
  a.sdt,
	a.order_code,
	-- c.performance_region_code,
    c.performance_region_name,
    -- c.performance_province_code ,
    c.performance_province_name ,
    -- c.performance_city_code , 
    c.performance_city_name,
	a.location_code,
	c.shop_name,	
    -- g.supplier_classify_code,     -- 供应商类型
    a.supplier_code,   -- 供应商编码
    a.supplier_name,   -- 供应商名称	
    -- g.supplier_classify_name,     -- 供应商类型 
	-- g.direct_trans_flag,     -- 是否直供
	c.purchase_org,
	c.purchase_org_name,
	-- h.navy_order_flag,             -- 是否海军订单 0-否,1-是
	-- h.delivery_to_direct_flag,             -- 是否配送转直送订单 true 是, false 否
	-- h.local_purchase_order_flag,             -- 是否为转直送地采订单 1-是，0-否
	d.classify_middle_code,
	d.classify_middle_name,
	d.classify_small_code,
	d.classify_small_name,
	a.goods_code,
	d.goods_name,
    receive_qty,
	receive_price,
	receive_amt
from
(
  select sdt,
  order_code,
  supplier_code,   -- 供应商编码
  supplier_name,   -- 供应商名称
	goods_code,
	receive_dc_code as location_code,
	regexp_replace(to_date(receive_time),'-','') as receive_date,
  sum(receive_qty) as receive_qty,
	sum(receive_amt)/sum(receive_qty) as receive_price,
	sum(receive_amt) as receive_amt
  from csx_dws.csx_dws_wms_entry_detail_di
  -- from csx_analyse.csx_analyse_scm_purchase_order_flow_di
  where sdt >='20230701'
  and sdt<='20231111'
	-- and return_flag <> 'Y'   -- 不含退货
	and receive_status <> 0     -- 收货状态 0-待收货 1-收货中 2-已关单
	and entry_type like 'P%'  -- 订单类型
	and receive_qty > 0
	and purpose <> '09'  -- 不含城市服务商
	and supplier_code in(
'20044906',
'20044869',
'20044903',
'20046040',
'20045081',
'20044895',
'20045077',
'20044689',
'20045062',
'20045052',
'20045179',
'20045204',
'20046104',
'20044660',
'20044655',
'20060261',
'20018798',
'20044948',
'20045948',
'20043707',
'20058764',
'20059199',
'20027725',
'20064405',
'20064442',
'20068165'
)
	
	-- and business_type_code<>'03'  -- 客户直送
  group by sdt,order_code,supplier_code,supplier_name,goods_code,receive_dc_code,regexp_replace(to_date(receive_time),'-','')
) a
left join
(
  select 
    purchase_org,
    purchase_org_name,
    performance_region_code,
    performance_region_name,
    performance_province_code,
    performance_province_name,
    performance_city_code, 
    performance_city_name, 	
    shop_code,
    shop_name,
    company_code,
    company_name,
    city_code,
    city_name,
    province_code,
    province_name,
    purpose,
    purpose_name
  from csx_dim.csx_dim_shop
  where sdt='current'
) c on a.location_code = c.shop_code
join
(
  select * from csx_dim.csx_dim_basic_goods 
  where sdt='current' 
  -- and classify_middle_code='B0202' -- 蔬菜
) d on d.goods_code = a.goods_code
-- -- 供应链仓
-- join 
--  (select dc_code,
--     regexp_replace(to_date(enable_time),'-','') enable_date ,
--     '1' is_dc_tag
--  from csx_dim.csx_dim_csx_data_market_conf_supplychain_location 
--  where sdt='current'
--  ) e on a.location_code=e.dc_code
 -- 供应商类型
left join
(
	select supplier_code,
        purchase_org_code,
        business_owner_code,        -- 业态归属
        business_owner_name,        -- 业态归属
        special_customer,           -- 专项客户
        supplier_classify_code,     -- 供应商类型
        supplier_classify_name,     -- 供应商类型 
        borrow_flag,                -- 是否借用
        lock_flag,                  -- 是否锁定
        frozen_flag,                -- 是否冻结
        finance_frozen,             -- 是否财务冻结
        direct_trans_flag           -- 是否直供
    from csx_dim.csx_dim_basic_supplier_purchase 
    where sdt='current'
) g on a.supplier_code=g.supplier_code and c.purchase_org=g.purchase_org_code 	 
-- where e.is_dc_tag=1 -- 供应链仓
-- where d.classify_middle_name in('猪肉','牛羊')
;





-- 832商家的供应商20230101-20240612入库商品明细 20240613
-- drop table csx_analyse_tmp.tmp_1;
create table csx_analyse_tmp.tmp_1
as
select 
    a.sdt,
	a.order_code,
	-- c.performance_region_code,
    c.performance_region_name,
    -- c.performance_province_code ,
    c.performance_province_name ,
    -- c.performance_city_code , 
    c.performance_city_name,
	a.location_code,
	c.shop_name,	
    -- g.supplier_classify_code,     -- 供应商类型
    a.supplier_code,   -- 供应商编码
    a.supplier_name,   -- 供应商名称	
    -- g.supplier_classify_name,     -- 供应商类型 
	-- g.direct_trans_flag,     -- 是否直供
	c.purchase_org,
	c.purchase_org_name,
	-- h.navy_order_flag,             -- 是否海军订单 0-否,1-是
	-- h.delivery_to_direct_flag,             -- 是否配送转直送订单 true 是, false 否
	-- h.local_purchase_order_flag,             -- 是否为转直送地采订单 1-是，0-否
	d.classify_middle_code,
	d.classify_middle_name,
	d.classify_small_code,
	d.classify_small_name,
	a.goods_code,
	d.goods_name,
    receive_qty,
	receive_price,
	receive_amt
from
(
  select sdt,
    order_code,
    supplier_code,   -- 供应商编码
    supplier_name,   -- 供应商名称
	goods_code,
	receive_dc_code as location_code,
	regexp_replace(to_date(receive_time),'-','') as receive_date,
  sum(receive_qty) as receive_qty,
	sum(receive_amt)/sum(receive_qty) as receive_price,
	sum(receive_amt) as receive_amt
  from csx_dws.csx_dws_wms_entry_detail_di
  -- from csx_analyse.csx_analyse_scm_purchase_order_flow_di
  where sdt >='20230101'
  and sdt<='20240612'
	-- and return_flag <> 'Y'   -- 不含退货
	and receive_status <> 0     -- 收货状态 0-待收货 1-收货中 2-已关单
	and entry_type like 'P%'  -- 订单类型
	and receive_qty > 0
	and purpose <> '09'  -- 不含城市服务商
	and supplier_code in(
'20044906',
'20044869',
'20044903',
'20046040',
'20045081',
'20044895',
'20045077',
'20044689',
'20045062',
'20045052',
'20045179',
'20045204',
'20046104',
'20044660',
'20044655',
'20060261',
'20018798',
'20044948',
'20045948',
'20043707',
'20058764',
'20059199',
'20027725',
'20064405',
'20064442',
'20068165'
)
	
	-- and business_type_code<>'03'  -- 客户直送
  group by sdt,order_code,supplier_code,supplier_name,goods_code,receive_dc_code,regexp_replace(to_date(receive_time),'-','')
) a
left join
(
  select 
    purchase_org,
    purchase_org_name,
    performance_region_code,
    performance_region_name,
    performance_province_code,
    performance_province_name,
    performance_city_code, 
    performance_city_name, 	
    shop_code,
    shop_name,
    company_code,
    company_name,
    city_code,
    city_name,
    province_code,
    province_name,
    purpose,
    purpose_name
  from csx_dim.csx_dim_shop
  where sdt='current'
) c on a.location_code = c.shop_code
join
(
  select * from csx_dim.csx_dim_basic_goods 
  where sdt='current' 
  -- and classify_middle_code='B0202' -- 蔬菜
) d on d.goods_code = a.goods_code
-- -- 供应链仓
-- join 
--  (select dc_code,
--     regexp_replace(to_date(enable_time),'-','') enable_date ,
--     '1' is_dc_tag
--  from csx_dim.csx_dim_csx_data_market_conf_supplychain_location 
--  where sdt='current'
--  ) e on a.location_code=e.dc_code
 -- 供应商类型
left join
(
	select supplier_code,
        purchase_org_code,
        business_owner_code,        -- 业态归属
        business_owner_name,        -- 业态归属
        special_customer,           -- 专项客户
        supplier_classify_code,     -- 供应商类型
        supplier_classify_name,     -- 供应商类型 
        borrow_flag,                -- 是否借用
        lock_flag,                  -- 是否锁定
        frozen_flag,                -- 是否冻结
        finance_frozen,             -- 是否财务冻结
        direct_trans_flag           -- 是否直供
    from csx_dim.csx_dim_basic_supplier_purchase 
    where sdt='current'
) g on a.supplier_code=g.supplier_code and c.purchase_org=g.purchase_org_code 	 
-- where e.is_dc_tag=1 -- 供应链仓
where d.classify_middle_name in('猪肉','牛羊','水产','家禽','干货','调味品类','食用油类')
;

-- 因销售数据中有签约公司 入库没有，因此用销售表限制2115 且入库中上述商品出一版
select
sdt as `销售日期`,
-- original_order_code as `原销售单号(逆向单对应的正向单单号)`,
-- order_code as `订单编号`,
business_type_name as `业务类型名称`,
delivery_type_name as `配送类型名称`,
order_channel_detail_name as `下单渠道名称细分`,
-- operation_mode_name as `经营方式名称`,
a.customer_code as `客户编码`,
c.customer_name as `客户名称`,
channel_name as `渠道名称`,
second_category_name as `二级客户分类名称`,
performance_region_name as `业绩大区名称`,
performance_province_name as `业绩省区名称`,
performance_city_name as `业绩城市名称`,
sign_company_code as `签约公司编码`,
sign_company_name as `签约公司名称`,
b.classify_large_name as `管理大类名称`,
b.classify_middle_name as `管理中类名称`,
b.classify_small_name as `管理小类名称`,
a.goods_code as `商品编码`,
b.goods_name as `商品名称`,
b.standard as `规格`,
-- spec,
b.unit_name as `计量单位描述`,
sum(if(order_channel_detail_code=26,0,sale_qty)) as `销售数量`,
sum(sale_amt) as `含税销售金额`,
sum(profit) as `含税定价毛利额`
from
(
select *
from csx_dws.csx_dws_sale_detail_di 
where sdt>='20230101'
and sign_company_code ='2115'
-- and classify_middle_name='米'
)a 
left join  -- -- -- 商品信息
  (
    select
    goods_code,
    regexp_replace(regexp_replace(goods_name,'\n',''),'\r','') as goods_name,
    purchase_group_code as department_id,purchase_group_name as department_name,    
    classify_large_code,classify_large_name, -- 管理大类
    classify_middle_code,classify_middle_name,-- 管理中类
    classify_small_code,classify_small_name,-- 管理小类
	standard,unit_name
    from csx_dim.csx_dim_basic_goods
    where sdt = 'current'
  )b on b.goods_code = a.goods_code
left join
(
select customer_code,customer_name
from csx_dim.csx_dim_crm_customer_info
where sdt='current'
)c on c.customer_code = a.customer_code 
join 
(
select distinct goods_code
from csx_analyse_tmp.tmp_1 
)f on a.goods_code=f.goods_code 
group by 
sdt,
business_type_name,
delivery_type_name,
order_channel_detail_name,
-- operation_mode_name,
a.customer_code,
c.customer_name,
channel_name,
second_category_name,
performance_region_name,
performance_province_name,
performance_city_name,
sign_company_code,
sign_company_name,
b.classify_large_name,
b.classify_middle_name,
b.classify_small_name,
a.goods_code,
b.goods_name,
b.standard,
b.unit_name
;








