-- 福利BBC客诉
 with sale as (select a.credential_no,
  wms_order_no,
  c.original_order_code,
 -- b.link_wms_move_type_code,
  a.order_code,
  a.performance_region_code,
  a.performance_region_name,
  a.performance_province_code,
  a.performance_province_name,
  a.performance_city_code,
  a.performance_city_name,
  a.business_type_name,
  a.inventory_dc_code,
  a.inventory_dc_name,
  a.sign_company_code,
  a.sign_company_name,
  a.customer_code,
  a.customer_name,
  c.supplier_code,
  c.supplier_name,
  a.goods_code,
  coalesce(b.link_wms_move_type_code, '') as link_wms_move_type_code,
  coalesce(b.link_wms_move_type_name, '') as link_wms_move_type_name,
  round((a.sale_qty),2) as sale_qty,
  round((sale_amt),2) as sale_amt,
  round((a.sale_cost),2) as sale_cost,
  b.sale_cost as wms_sale_cost,
  a.sdt
from
( select
    sdt,
    split(id, '&')[0] as credential_no,
    order_code ,
    performance_region_code,
    performance_region_name,
    performance_province_code,
    performance_province_name,
    performance_city_code,
    performance_city_name,
    business_type_name,
    inventory_dc_code,
    inventory_dc_name,
    sign_company_code,
    sign_company_name,
    customer_code,
    customer_name,
    goods_code,
    a.sale_qty,
    a.sale_cost,
    a.sale_price,
    sale_amt
  from csx_dws.csx_dws_sale_detail_di a 
  where sdt >= regexp_replace(add_months(trunc(date_sub('${current_date}', 1), 'MM'), -1), '-', '') 
        and sdt<=regexp_replace('${current_date}', '-', '') 
	  and channel_code in ('1', '7', '9')
    and business_type_code in('6') 
    and order_channel_code = 3 --  1-b端 2-m端 3-bbc 4-客户返利管理 5-价格补救 6-客户调价管理 -1-sap
    and refund_order_flag = 0 --  退货订单标识(0-正向单 1-逆向单)
   -- and performance_province_name != '平台-B'
)a
--批次操作明细表
left join
(
  select
    credential_no,
    wms_order_no, -- wms入库订单号
    goods_code,
    sum(if(in_or_out = 0, -1 * qty, qty)) as sale_qty,
    sum(if(in_or_out = 0, -1 * amt, amt)) as sale_cost,
    link_wms_move_type_code,
    link_wms_move_type_name
  from csx_dws.csx_dws_wms_batch_detail_di
  where sdt >= regexp_replace(add_months(trunc(date_sub('${current_date}', 1), 'MM'), -9), '-', '')
  group by credential_no, 
    wms_order_no, 
    goods_code, 
    link_wms_move_type_code, 
    link_wms_move_type_name
)b on b.credential_no = a.credential_no and b.goods_code = a.goods_code
--入库明细
left join
(
  select 
    supplier_code,
    supplier_name,
    order_code,
    goods_code,
    goods_name,
    original_order_code
  from    csx_dws.csx_dws_wms_entry_detail_di
  where 1=1
  and  sdt >= '20190101'
 -- AND order_code='IN191208000007'
  group by 
    supplier_code,
    supplier_name,
    order_code,
    goods_code,
    original_order_code,
    goods_name
)c on c.order_code = b.wms_order_no and b.goods_code = c.goods_code
),
complaint as ( select substr(sdt, 1, 6) smonth,
                            --  客诉日期
                            performance_province_name,
                            inventory_dc_code,
                            sale_order_code,
                            if(
                                second_level_department_name in ('交易支持', '非食用品'),
                                second_level_department_name,
                                classify_middle_code
                            ) classify_middle_code,
                            complaint_code ,
                            goods_code
                        from csx_analyse.csx_analyse_fr_oms_complaint_detail_new_di
                        where sdt >= '20240301'
                            and sdt <= '20240430'
                            and complaint_status_code in (20, 30) --  客诉状态: 10-待处理 20-已处理待确认 21-驳回待确认  30-已处理 -1-已取消
                            and complaint_deal_status in (10, 40) --  责任环节状态 10'待处理' 20'待修改' 30'已处理待审' 31'已驳回待审核' 40'已完成' -1'已取消'
                            and second_level_department_name != ''
                         --   and first_level_department_name = '采购'
                            and complaint_source_code <> 2 --	客诉来源:1-单独发起客诉单 2-客退单生成 3-补货单生成
                        
                    ) 
select a.performance_region_code,
  a.performance_region_name,
  a.performance_province_code,
  a.performance_province_name,
  a.performance_city_code,
  a.performance_city_name,
  a.business_type_name,
  a.inventory_dc_code,
  a.inventory_dc_name,
  a.sign_company_code,
  a.sign_company_name,
  a.customer_code,
  a.customer_name,
  a.supplier_code,
  a.supplier_name,
  round(sum(a.sale_qty),2) as sale_qty,
  round(sum(sale_amt),2) as sale_amt,
  round(sum(a.sale_cost),2) as sale_cost,
  b.complaint_code 
 from sale a 
left join 
complaint b on a.order_code=b.sale_order_code and a.goods_code=b.goods_code
group by a.performance_region_code,
  a.performance_region_name,
  a.performance_province_code,
  a.performance_province_name,
  a.performance_city_code,
  a.performance_city_name,
  a.business_type_name,
  a.inventory_dc_code,
  a.inventory_dc_name,
  a.sign_company_code,
  a.sign_company_name,
  a.customer_code,
  a.customer_name,
  a.supplier_code,
  a.supplier_name,
  complaint_code
