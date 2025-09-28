-- 采购绩效方案2.0
-- 大区销售额
-- direct_delivery_type int 履约模式: 0-其他配送、1-客户换补、2-客户自购、11-临时加单、12-紧急补货、13-计划直送、【(废弃) 14-委外 15-前置仓】、16-委外直送、17-配转直、18-委外配送、19-发车前缺货；

-- 日配销售额
with tmp_sale as (
select substr(sdt,1,6) as sales_months,
    basic_performance_region_name,
	basic_performance_province_code,
	basic_performance_province_name,
	basic_performance_city_code,
	basic_performance_city_name,
    case when classify_large_code in ('B04','B05','B06','B07','B08','B09') then '食百'else classify_large_name end classify_large_name,
    classify_middle_code,
    classify_middle_name,
    sum(sale_amt)sale_amt,
    sum(profit) profit
from csx_dws.csx_dws_sale_detail_di a
left join 
	(select basic_performance_region_name,
	    basic_performance_province_code,
	    basic_performance_province_name,
	    basic_performance_city_code,
	    basic_performance_city_name,
	    shop_code
	  from csx_dim.csx_dim_shop
	    where sdt='current' 
	 ) c on a.inventory_dc_code=c.shop_code
  where sdt >= '20250101'
    and sdt <=  '20250228'
    -- and channel_code in ('1','9')
    and business_type_code='1'
    -- and channel_code in ('1','7','9') 
    and  direct_delivery_type in ('0','11','12','16','17','18','19')     -- 日配-采购管理
group by  case when classify_large_code in ('B04','B05','B06','B07','B08','B09') then '食百'else classify_large_name end ,
    classify_middle_code,
    classify_middle_name,
    business_type_code,
    business_type_name,
     substr(sdt,1,6),
    basic_performance_region_name,
	    basic_performance_province_code,
	    basic_performance_province_name,
	    basic_performance_city_code,
	    basic_performance_city_name
) ,
tmp_fl as 
(SELECT
  substr(regexp_replace(to_date(belong_date),'-',''),1,6) s_month,
  a.basic_performance_region_name,
  a.basic_performance_province_code,
  a.basic_performance_province_name,
  a.basic_performance_city_code,
  a.basic_performance_city_name,
  m.first_level_code firstLevelCode,
  m.first_level_name firstLevelName,
  m.second_level_code secondLevelCode,
  m.second_level_name secondLevelName,
  sum(m.total_amount) billTotalAmount 
FROM
    csx_dwd.csx_dwd_pss_settle_settle_bill_detail_management_classification_item_di m
  LEFT JOIN (
    select
        belong_date,
        settle_code,
      settlement_dc_code,
      shop_name as settlement_dc_name,
      c.basic_performance_region_name,
	  c.basic_performance_province_code,
	  c.basic_performance_province_name,
	  c.basic_performance_city_code,
	  c.basic_performance_city_name
    from
      csx_dwd.csx_dwd_pss_settle_settle_bill_di a 
    left join 
	(select basic_performance_region_name,
	    basic_performance_province_code,
	    basic_performance_province_name,
	    basic_performance_city_code,
	    basic_performance_city_name,
	    shop_code,
	    shop_name
	  from csx_dim.csx_dim_shop
	    where sdt='current' 
	 ) c on a.settlement_dc_code=c.shop_code
    where
      sdt >= '20250101'
  ) a ON a.settle_code = m.settle_no
where
  1=1
  and 
  (settlement_dc_name not like '%项目供应商%'
  and settlement_dc_name not like '%福利%'
  and settlement_dc_name not like '%BBC%'
  and settlement_dc_name not like '%全国%'
  and settlement_dc_name not like '%合伙人%'
  and settlement_dc_name not like '%服务商%'
  and settlement_dc_name not like '%前置仓%'
 -- and settlement_dc_name not like '%直送%'
  )
--   and a.settle_date BETWEEN '2025-01-01' AND '2025-02-28'
--   and settle_no='FY25020800766'
  and to_date(belong_date) >= trunc('2025-01-31', 'MM')
  and to_date(belong_date) <= '2025-02-28'
 group by substr(regexp_replace(to_date(belong_date),'-',''),1,6) ,
  a.basic_performance_region_name,
  a.basic_performance_province_code,
  a.basic_performance_province_name,
  a.basic_performance_city_code,
  a.basic_performance_city_name,
  m.first_level_code ,
  m.first_level_name ,
  m.second_level_code ,
  m.second_level_name 
)
select sales_months,
    basic_performance_region_name,
	basic_performance_province_code,
	basic_performance_province_name,
	basic_performance_city_code,
	basic_performance_city_name,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    sum(sale_amt)sale_amt,
    sum(profit) profit,
    sum(billTotalAmount) billTotalAmount
from
(select sales_months,
    basic_performance_region_name,
	basic_performance_province_code,
	basic_performance_province_name,
	basic_performance_city_code,
	basic_performance_city_name,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    (sale_amt)sale_amt,
    (profit) profit,
    0 billTotalAmount
from tmp_sale a 
union all 
SELECT
  s_month sales_months,
  basic_performance_region_name,
  basic_performance_province_code,
  basic_performance_province_name,
  basic_performance_city_code,
  basic_performance_city_name,
  case when firstLevelCode in ('B04','B05','B06','B07','B08','B09') then '食百'else firstLevelName end classify_large_name,
  secondLevelCode,
  secondLevelName,
  0 sale_amt,
  0 profit,
  billTotalAmount 
FROM tmp_fl
)a 
group by sales_months,
    basic_performance_region_name,
	basic_performance_province_code,
	basic_performance_province_name,
	basic_performance_city_code,
	basic_performance_city_name,
    classify_large_name,
    classify_middle_code,
    classify_middle_name
  ;

-- 管理分类后台毛利
SELECT
  m.first_level_code firstLevelCode,
  m.first_level_name firstLevelName,
  m.second_level_code secondLevelCode,
  m.second_level_name secondLevelName,
  m.settle_no settle_no,
  a.agreement_code agreement_no,
  a.create_by createBy,
  a.create_time createTime,
  a.settle_date settle_date,
  a.purchase_org_code purchase_org_code,
  a.purchase_org_name purchase_org_name,
  a.fee_code cost_code,
  a.fee_name cost_name,
  a.cost_code_second_level cost_code_second_level,
  a.cost_name_second_level cost_name_second_level,
  a.belong_date,
  a.supplier_code supplier_code,
  a.supplier_name supplier_name,
  a.settle_type_code settle_flag,
  a.settlement_dc_code,
  a.settlement_dc_name,

  a.calculation_flag_code calculation_flag,
  a.share_finish_amt,
  -- 分摊金额
  a.total_amount,
  -- 单据总金额
  a.rebate_level,
  -- 返利级别
  a.posting_status_code,
  -- 过账状态1，已过账 0，未过账
  a.posting_date,
  -- 过账日期
  a.invoice_type_code,
  a.invoice_type_name,
  a.is_write_off ,            -- 是否冲销1，冲销 0，非冲销
  a.write_off_credential_no , --  冲销凭证号
  a.link_share_fee_order_code,      -- 关联分摊费用单号
  -- a.original_settle_no,
  a.original_supplier_code,
  a.original_supplier_name,
  a.receive_dc_code,
  a.receive_dc_name,
  a.income_type,
  (
    CASE
      WHEN a.income_type = 1 THEN '支出'
      WHEN a.income_type = 2 THEN '收入'
      ELSE ''
    END
  ) incomeTypeMess,
  a.is_share_fee,
  -- a.settle_points settle_points,
  a.company_code company_code,
  a.company_name company_name,
  a.purchase_org_code purchase_code,
  a.purchase_org_name purchase_name,
  b.contract_no contract_no,
  b.bill_no bill_no,
  (
    CASE
      WHEN c.bill_status_code = 0
      AND c.bill_code IS NULL THEN '未对账'
      WHEN c.bill_status_code = 0
      AND c.bill_code != '' THEN '供应商已确认财务未确认'
      WHEN c.bill_status_code = 1 THEN '财务确认对账'
      WHEN c.bill_status_code = 3 THEN '通道服务费对账'
      ELSE ''
    END
  ) statement_state,
  (
    CASE
      WHEN c.payment_status_code = 0 THEN '未付款'
      WHEN c.payment_status_code = 1 THEN '已生成付款单待审核'
      WHEN c.payment_status_code = 4 THEN '付款成功'
      WHEN c.payment_status_code = 5 THEN '已推送待付款'
      WHEN c.payment_status_code = 8 THEN '付款失败'
      WHEN c.payment_status_code = 9 THEN '付款失败'
      ELSE ''
    END
  ) payment_status,
  a.kp_state,
  a.apply_state,
  a.kp_amount,
  a.residue_total_amount,
  a.belong_province_code,
  a.belong_province_name,
  a.belong_city_code,
  a.belong_city_name,
  a.expenses_source_code,
  a.expenses_source_name,
  a.purpose,
  a.purpose_desc,
  m.amount netValue,
  m.total_amount valueTaxTotal,
  m.tax_amount taxAmount,
  m.total_amount billTotalAmount,
  m.tax_rate taxRate,
  m.tax_code taxCode
FROM
  csx_dwd.csx_dwd_pss_settle_settle_bill_detail_management_classification_item_di m
  LEFT JOIN (
    select
      *
    from
      csx_dwd.csx_dwd_pss_settle_settle_bill_di
    where
      sdt >= '20250101'
  ) a ON a.settle_code = m.settle_no
  LEFT JOIN (
    select
      *
    from
      csx_ods.csx_ods_csx_b2b_settle_settle_agreement_df
    where
      sdt = '20250318'
  ) b ON a.agreement_code = b.agreement_no
  LEFT JOIN (
    select
      *
    from
        csx_dwd.csx_dwd_pss_statement_source_bill_di
    where
      sdt >= '20250101'
  ) c ON c.bill_code = a.settle_code
where
  m.sdt>='20250101'
--   and a.settle_date BETWEEN '2025-01-01' AND '2025-02-28'
  and settle_no='FY25030100063'
  and to_date(belong_date) >= trunc('2025-02-28', 'MM')
  and to_date(belong_date) <= '2025-02-28'

    		 
----------------------客诉率------------------------------		 
select *,
  nvl(kesu,0)/nvl(a.skuall,0)  kesu_lv
from (select
*,
sum(sku) over() skuall
from 
(  
select
  a.smonth,
  --  a.inventory_dc_name,
  if(a.classify_large_code in ('B01','B02','B03'),classify_large_name,'食百') as  classify_name,
  a.classify_middle_name,
  sum(nvl(b.kesu,0)) kesu,
  sum(nvl(a.sku,0)) sku
from 
(
select
  smonth,
  --  performance_region_code,
  performance_region_name,
  performance_province_name,
  performance_city_name,
  --  IF(performance_province_name='浙江省',performance_city_name,performance_province_name) performance_province_name,
  inventory_dc_code,
  inventory_dc_name,
  classify_large_code,
  classify_large_name,
  classify_middle_code,
  classify_middle_name,
  sum(sku) sku
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
  a.classify_middle_code,
  c.classify_middle_name,
  c.classify_large_name,
  c.classify_large_code,
  a.order_code,
  count(distinct a.goods_code) sku
from csx_dws.csx_dws_sale_detail_di a
left join
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
where  sdt>='20241201' and sdt<='20241231'
      	and channel_code in('1','7','9')
		and business_type_code not in(4,6) --  业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.bbc 7.大宗一部 8.大宗二部 9.商超)
		and order_channel_code =1 --  1-b端 2-m端 3-bbc 4-客户返利管理 5-价格补救 6-客户调价管理 -1-sap
		and refund_order_flag=0 --  退货订单标识(0-正向单 1-逆向单)
		and performance_province_name !='平台-B'
group by substr(sdt,1,6) ,
c.classify_large_name,
c.classify_large_code,
  performance_region_code,
  performance_region_name,
  performance_province_code,
  performance_province_name,
  performance_city_code,
  performance_city_name,
  inventory_dc_code,inventory_dc_name,
  a.classify_middle_code,
  c.classify_middle_name,
  a.order_code
  )a 
group by smonth,
 --  performance_region_code,
  performance_region_name,
 --  performance_province_code,
 performance_province_name,
 performance_city_name,
   --  IF(performance_province_name='浙江省',performance_city_name,performance_province_name),
   inventory_dc_code,inventory_dc_name,
  classify_middle_code,
  classify_middle_name	,
  classify_large_name,
  classify_large_code
	) a
left join 
(
  select
    substr(sdt,1,6) smonth,        --  客诉日期
	  performance_province_name,
    inventory_dc_code,
    if(second_level_department_name in ('交易支持','非食用品'),second_level_department_name,classify_middle_code) classify_middle_code,
    count(distinct complaint_code)  kesu -- - 客诉单量
  from    csx_analyse.csx_analyse_fr_oms_complaint_detail_new_di
  where  sdt>='20241201' and sdt<='20241231'
        and complaint_status_code in (20,30)  --  客诉状态: 10-待处理 20-已处理待确认 21-驳回待确认  30-已处理 -1-已取消
		and complaint_deal_status in (10,40) --  责任环节状态 10'待处理' 20'待修改' 30'已处理待审' 31'已驳回待审核' 40'已完成' -1'已取消'
		and second_level_department_name !=''
		and first_level_department_name='采购'
    and complaint_level != '3'
	  -- and complaint_source_code<>2    --	客诉来源:1-单独发起客诉单 2-客退单生成 3-补货单生成
  group by  substr(sdt,1,6),        --  客诉日期
          inventory_dc_code,
         if(second_level_department_name in ('交易支持','非食用品'),second_level_department_name,classify_middle_code),performance_province_name
  )b on a.inventory_dc_code=b.inventory_dc_code and a.smonth=b.smonth and a.classify_middle_code=b.classify_middle_code 
        and a.performance_province_name=b.performance_province_name
group by  a.smonth,
  if(a.classify_large_code in ('B01','B02','B03'),classify_large_name,'食百'),
  a.classify_middle_name
  )a 
  )a
;



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
    sdt >= '20241201'
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

-- 周转
select 
month_of_year `月份`,
-- performance_region_code	    as	`业绩归属大区编码`,
-- performance_region_name	    as	`业绩归属大区名称`,
-- performance_province_code	as	`绩效归属省区编码`,
-- performance_province_name	as	`绩效归属省区名称`,
-- performance_city_code	    as	`绩效归属城市编码`,
-- performance_city_name	    as	`绩效归属城市名称`,
if(a.classify_large_code in ('B01','B02','B03'),classify_large_name,'食百')     as `管理大类名称`,
classify_middle_code	as	`管理中类`,
classify_middle_name	as	`管理中类名称`,
sum(nearly30days_amt_no_tax)	as	`不含税近30天累计库存额`,
sum(nearly30days_sale_cost_no_tax)	as	`不含税近30天累计销售出库成本`,
--sum(out_nearly30days_province_transfer_cost) `跨省区出库成本`,
sum(nearly30days_amt_no_tax)/ sum(nearly30days_sale_cost_no_tax) as `近30周转`
from 
 csx_report.csx_report_cas_accounting_turnover_stock_cost_goods_detail_df_new a 
join 
(select distinct month_of_year, month_end from csx_dim.csx_dim_basic_date where calday=regexp_replace('${edate})','-','') ) b on a.sdt=b.month_end
 join 
 (select dc_code,regexp_replace(to_date(enable_time),'-','') enable_date 
 from csx_dim.csx_dim_csx_data_market_conf_supplychain_location 
 where sdt='current') c on a.dc_area_code=c.dc_code
 group by 
month_of_year  ,
-- performance_region_code	 ,
-- performance_region_name	 ,
-- performance_province_code,
-- performance_province_name,
-- performance_city_code	 ,
-- performance_city_name	 ,
if(a.classify_large_code in ('B01','B02','B03'),classify_large_name,'食百')     ,
classify_middle_code	,
classify_middle_name	
; 


-- 采购占比 调整基地逻辑

select
   basic_performance_region_name,
   basic_performance_province_name	,
   basic_performance_city_name,
   a.new_classify_middle_name,
   classify_middle_name,
   classify_small_name,
  sum(central_amt) as central_amt,
  sum(jd_amt) as jd_amt,
  sum(oem_amt) as oem_amt,
  sum(amtfenzi) amtfenzi,
  sum(receive_amt) receive_amt,
  sum(amtfenzi)/sum(receive_amt) receive_ratio
from (
select
   a.performance_region_name,
   a.performance_province_name,
   a.dc_code,
   a.new_classify_middle_name,
   classify_middle_name,
   classify_small_name,
   goods_code,
   goods_name,
   supplier_code,
   supplier_name,
   sum(if(is_central_tag='1',receive_amt,0)) central_amt,
   sum(if(new_order_business_type='1',receive_amt,0)) jd_amt,
   sum(if(csx_purchase_level_code='03',receive_amt,0)) oem_amt,
   sum(if(is_central_tag='1' or new_order_business_type='1' or csx_purchase_level_code='03',receive_amt,0)) amtfenzi,   
   sum(receive_amt) receive_amt
from 
(
 select 
  performance_region_name,
  performance_province_name,
  dc_code,
   goods_code,
   goods_name,
  case
    when classify_middle_code in ('B0305','B0302','B0301','B0201','B0202','B0303','B0306' ) then classify_middle_name 
    when classify_large_code in ('B01') then '干货加工'
	else '食百' end  as new_classify_middle_name,
  classify_middle_name,
  classify_small_name,
  csx_purchase_level_code,  -- 01-全国商品,02-一般商品,03-oem商品
  case when b.supplier_type='5' then '1' end new_order_business_type, -- 业务类型 1 基地订单
  is_central_tag, --品类+供应商集采
  a.supplier_code,
  supplier_name,
  sum(receive_amt) receive_amt
 from 
    csx_analyse.csx_analyse_scm_purchase_order_flow_di a 
    left join 
    (select supplier_code,supplier_type  from csx_dim.csx_dim_basic_supplier where sdt='current') b on a.supplier_code=b.supplier_code
 where sdt>=regexp_replace(trunc('${edate}','MM'),'-','') and sdt<=regexp_replace('${edate}','-','')
  AND order_code like 'IN%'
group by performance_region_name,
   performance_province_name,
   dc_code,
   case when classify_middle_code in ('B0305', 'B0302', 'B0301', 'B0201', 'B0202', 'B0303', 'B0306') then classify_middle_name
        when classify_large_code in ('B01') then '干货加工'
	 else '食百' end,
   csx_purchase_level_code,
   case when b.supplier_type='5' then '1' end,
   is_central_tag,
   classify_small_name,
   classify_middle_name,
    goods_code,
   goods_name,
    a.supplier_code,
  supplier_name
)a 
  join 
(
 select distinct dc_code
from
csx_ods.csx_ods_csx_data_market_conf_supplychain_location_df
)c  on a.dc_code=c.dc_code
where a.classify_middle_name<>'食百'
group by  a.performance_region_name,
   a.performance_province_name,
   a.dc_code,
   a.new_classify_middle_name,
   classify_middle_name,
   classify_small_name,
    goods_code,
   goods_name,
   supplier_code,
   supplier_name
)a
left join 
 (select shop_code,basic_performance_region_name,basic_performance_province_code,	basic_performance_province_name	,basic_performance_city_code,	basic_performance_city_name 
 from csx_dim.csx_dim_shop where sdt='current') d on a.dc_code=d.shop_code
GROUP BY
  basic_performance_region_name,
   basic_performance_province_name	,
   basic_performance_city_name,
   a.new_classify_middle_name,
   classify_middle_name,
   classify_small_name
  ;



  -- 采购占比明细
  
  
-- 采购占比 

select
   basic_performance_region_name,
   basic_performance_province_name	,
   basic_performance_city_name,
   a.new_classify_middle_name,
   classify_middle_name,
   classify_small_name,
   goods_code,
   goods_name,
   supplier_code,
   supplier_name,
  sum(central_amt) as central_amt,
  sum(jd_amt) as jd_amt,
  sum(oem_amt) as oem_amt,
  sum(amtfenzi) amtfenzi,
  sum(receive_amt) receive_amt,
  sum(amtfenzi)/sum(receive_amt) receive_ratio
from (
select
   a.performance_region_name,
   a.performance_province_name,
   a.dc_code,
   a.new_classify_middle_name,
   classify_middle_name,
   classify_small_name,
   goods_code,
   goods_name,
   supplier_code,
   supplier_name,
   sum(if(is_central_tag='1',receive_amt,0)) central_amt,
   sum(if(order_business_type='1',receive_amt,0)) jd_amt,
   sum(if(csx_purchase_level_code='03',receive_amt,0)) oem_amt,
   sum(if(is_central_tag='1' or order_business_type='1' or csx_purchase_level_code='03',receive_amt,0)) amtfenzi,   
   sum(receive_amt) receive_amt
from 
(
 select 
  performance_region_name,
  performance_province_name,
  dc_code,
   goods_code,
   goods_name,
  case
    when classify_middle_code in ('B0305','B0302','B0301','B0201','B0202','B0303','B0306' ) then classify_middle_name 
    when classify_large_code in ('B01') then '干货加工'
	else '食百' end  as new_classify_middle_name,
  classify_middle_name,
  classify_small_name,
  csx_purchase_level_code,  -- 01-全国商品,02-一般商品,03-oem商品
  order_business_type, -- 业务类型 1 基地订单
  is_central_tag, --品类+供应商集采
  supplier_code,
  supplier_name,
  sum(receive_amt) receive_amt
 from 
    csx_analyse.csx_analyse_scm_purchase_order_flow_di
 where sdt>='20241201' and sdt<='20241231'
  AND order_code like 'IN%'
group by performance_region_name,
   performance_province_name,
   dc_code,
   case when classify_middle_code in ('B0305', 'B0302', 'B0301', 'B0201', 'B0202', 'B0303', 'B0306') then classify_middle_name
        when classify_large_code in ('B01') then '干货加工'
	 else '食百' end,
   csx_purchase_level_code,
   order_business_type,
   is_central_tag,
   classify_small_name,
   classify_middle_name,
    goods_code,
   goods_name,
    supplier_code,
  supplier_name
)a 
  join 
(
 select distinct dc_code
from
csx_ods.csx_ods_csx_data_market_conf_supplychain_location_df
)c  on a.dc_code=c.dc_code
where a.classify_middle_name<>'食百'
group by  a.performance_region_name,
   a.performance_province_name,
   a.dc_code,
   a.new_classify_middle_name,
   classify_middle_name,
   classify_small_name,
    goods_code,
   goods_name,
   supplier_code,
   supplier_name
)a
left join 
 (select shop_code,basic_performance_region_name,basic_performance_province_code,	basic_performance_province_name	,basic_performance_city_code,	basic_performance_city_name 
 from csx_dim.csx_dim_shop where sdt='current') d on a.dc_code=d.shop_code
GROUP BY
  basic_performance_region_name,
   basic_performance_province_name	,
   basic_performance_city_name,
   a.new_classify_middle_name,
   classify_middle_name,
   classify_small_name,
   goods_code,
   goods_name,
   supplier_code,
   supplier_name
  ;

  -- 基地新规则 
  
select
   basic_performance_region_name,
   basic_performance_province_name	,
   basic_performance_city_name,
   a.new_classify_middle_name,
   classify_middle_name,
   classify_small_name,
   goods_code,
   goods_name,
   supplier_code,
   supplier_name,
  sum(central_amt) as central_amt,
  sum(jd_amt) as jd_amt,
  sum(oem_amt) as oem_amt,
  sum(amtfenzi) amtfenzi,
  sum(receive_amt) receive_amt,
  sum(amtfenzi)/sum(receive_amt) receive_ratio
from (
select
   a.performance_region_name,
   a.performance_province_name,
   a.dc_code,
   a.new_classify_middle_name,
   classify_middle_name,
   classify_small_name,
   goods_code,
   goods_name,
   supplier_code,
   supplier_name,
   sum(if(is_central_tag='1',receive_amt,0)) central_amt,
   sum(if(new_order_business_type='1',receive_amt,0)) jd_amt,
   sum(if(csx_purchase_level_code='03',receive_amt,0)) oem_amt,
   sum(if(is_central_tag='1' or new_order_business_type='1' or csx_purchase_level_code='03',receive_amt,0)) amtfenzi,   
   sum(receive_amt) receive_amt
from 
(
 select 
  performance_region_name,
  performance_province_name,
  dc_code,
   goods_code,
   goods_name,
  case
    when classify_middle_code in ('B0305','B0302','B0301','B0201','B0202','B0303','B0306' ) then classify_middle_name 
    when classify_large_code in ('B01') then '干货加工'
	else '食百' end  as new_classify_middle_name,
  classify_middle_name,
  classify_small_name,
  csx_purchase_level_code,  -- 01-全国商品,02-一般商品,03-oem商品
  case when b.supplier_type='5' then '1' end new_order_business_type, -- 业务类型 1 基地订单
  is_central_tag, --品类+供应商集采
  a.supplier_code,
  supplier_name,
  sum(receive_amt) receive_amt
 from 
    csx_analyse.csx_analyse_scm_purchase_order_flow_di a 
    left join 
    (select supplier_code,supplier_type  from csx_dim.csx_dim_basic_supplier where sdt='current') b on a.supplier_code=b.supplier_code
 where sdt>='20241201' and sdt<='20241231'
  AND order_code like 'IN%'
group by performance_region_name,
   performance_province_name,
   dc_code,
   case when classify_middle_code in ('B0305', 'B0302', 'B0301', 'B0201', 'B0202', 'B0303', 'B0306') then classify_middle_name
        when classify_large_code in ('B01') then '干货加工'
	 else '食百' end,
   csx_purchase_level_code,
   case when b.supplier_type='5' then '1' end,
   is_central_tag,
   classify_small_name,
   classify_middle_name,
    goods_code,
   goods_name,
    a.supplier_code,
  supplier_name
)a 
  join 
(
 select distinct dc_code
from
csx_ods.csx_ods_csx_data_market_conf_supplychain_location_df
)c  on a.dc_code=c.dc_code
where a.classify_middle_name<>'食百'
group by  a.performance_region_name,
   a.performance_province_name,
   a.dc_code,
   a.new_classify_middle_name,
   classify_middle_name,
   classify_small_name,
    goods_code,
   goods_name,
   supplier_code,
   supplier_name
)a
left join 
 (select shop_code,basic_performance_region_name,basic_performance_province_code,	basic_performance_province_name	,basic_performance_city_code,	basic_performance_city_name 
 from csx_dim.csx_dim_shop where sdt='current') d on a.dc_code=d.shop_code
GROUP BY
  basic_performance_region_name,
   basic_performance_province_name	,
   basic_performance_city_name,
   a.new_classify_middle_name,
   classify_middle_name,
   classify_small_name,
   goods_code,
   goods_name,
   supplier_code,
   supplier_name
  ;