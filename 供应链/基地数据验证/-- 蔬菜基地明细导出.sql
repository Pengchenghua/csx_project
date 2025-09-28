-- 蔬菜基地明细导出
with tmp_order_sale as (
  select
    sale_sdt as sdt,
    receive_dc_code,
    receive_dc_name,
    region_code,
    region_name,
    province_code,
    province_name,
    city_group_code,
    city_group_name,
    order_code,
    a.goods_code,
    sale_amt,
    sale_qty,
    profit,
    classify_middle_name,
    meta_batch_no,
    -- 原料批次成本单号
    product_code,
    -- 原料商品编码
    b.goods_name product_name,
    -- 原料商品名称
    short_name,
    product_tax_rate,
    product_classify_large_code,
    product_classify_large_name,
    product_classify_middle_code,
    product_classify_middle_name,
    product_classify_small_code,
    product_classify_small_name,
    meta_qty,
    -- 原料消耗数量
    meta_amt,
    -- 原料消耗金额
    meta_amt_no_tax,
    -- 原料消耗金额(未税)
    use_ratio,
    -- 原料使用占比
    product_ratio,
    -- 原料工单占比
    purchase_order_no,
    order_qty,
    order_amt,
    batch_no,
    transfer_crdential_no,
    supplier_code,
    supplier_name,
    purchase_order_type,
    -- 采购订单类型1 集采采购 2 基地采购 3 其他采购
    goods_shipped_type,
    -- 商品出库类型1 A进A出 2工厂加工 3其他
    channel_type_name,
    channel_type_code,
    supplier_type_code,
    supplier_type_name,
    purchase_crdential_flag,
    sale_correlation_flag,
    batch_qty,
    -- 销售批次数量
    batch_cost,
    -- 销售批次成本
    batch_cost_no_tax,
    -- 销售批次未税成本
    batch_sale_amt,
    -- 批次销售额
    batch_sale_amt_no_tax,
    -- 批次未税销售额
    batch_profit,
    -- 批次毛利额
    batch_profit_no_tax,
    -- 批次未税毛利额
    product_profit_rate,
    product_no_tax_profit_rate,
    product_cost_amt,
    -- 原料销售成本根据占比计算product_ratio
    product_cost_amt_no_tax,
    -- 原料未税销售成本根据占比计算product_ratio
    product_profit,
    -- 原料毛利额根据占比计算product_ratio
    product_profit_no_tax,
    -- 原料未税毛利额根据占比计算product_ratio
    product_sale_amt,
    -- 原料销售额根据占比计算product_ratio
    product_sale_amt_no_tax -- 原料未税销售额根据占比计算product_ratio
  from
    csx_analyse.csx_analyse_fr_fina_goods_sale_trace_po_di a
    join 
    (select goods_code,goods_name from csx_dim.csx_dim_basic_goods
    where sdt='current'
    )b on a.product_code=b.goods_code
  where
    sale_sdt >= '${sdt}'
    and sale_sdt <= '${edt}'
--    and province_name = '${prov}'
   ${if(len(city)==0,""," and city_group_name in ('"+SUBSTITUTE(city,",","','")+"') ")}
    and classify_middle_code = 'B0202' -- 蔬菜
    and supplier_type_code in ('4','5', '10', '11', '12', '13')
    and (
      purchase_order_no is not null
      or sale_correlation_flag = '调拨采购商品未关联'
    )
  group by
    sale_sdt,
    receive_dc_code,
    receive_dc_name,
    region_code,
    region_name,
    province_code,
    province_name,
    city_group_code,
    city_group_name,
    order_code,
    a.goods_code,
 
    sale_amt,
    sale_qty,
    profit,
    classify_middle_name,
    meta_batch_no,
    -- 原料批次成本单号
    product_code,
    -- 原料商品编码
    b.goods_name ,
    -- 原料商品名称
    short_name,
    product_tax_rate,
    product_classify_large_code,
    product_classify_large_name,
    product_classify_middle_code,
    product_classify_middle_name,
    product_classify_small_code,
    product_classify_small_name,
    meta_qty,
    -- 原料消耗数量
    meta_amt,
    -- 原料消耗金额
    meta_amt_no_tax,
    -- 原料消耗金额(未税)
    use_ratio,
    -- 原料使用占比
    product_ratio,
    -- 原料工单占比
    purchase_order_no,
    order_qty,
    order_amt,
    batch_no,
    transfer_crdential_no,
    supplier_code,
    supplier_name,
    purchase_order_type,
    -- 采购订单类型1 集采采购 2 基地采购 3 其他采购
    goods_shipped_type,
    -- 商品出库类型1 A进A出 2工厂加工 3其他
    channel_type_name,
    channel_type_code,
    supplier_type_code,
    supplier_type_name,
    purchase_crdential_flag,
    sale_correlation_flag,
    batch_qty,
    -- 销售批次数量
    batch_cost,
    -- 销售批次成本
    batch_cost_no_tax,
    -- 销售批次未税成本
    batch_sale_amt,
    -- 批次销售额
    batch_sale_amt_no_tax,
    -- 批次未税销售额
    batch_profit,
    -- 批次毛利额
    batch_profit_no_tax,
    -- 批次未税毛利额
    product_profit_rate,
    product_no_tax_profit_rate,
    product_cost_amt,
    -- 原料销售成本根据占比计算product_ratio
    product_cost_amt_no_tax,
    -- 原料未税销售成本根据占比计算product_ratio
    product_profit,
    -- 原料毛利额根据占比计算product_ratio
    product_profit_no_tax,
    -- 原料未税毛利额根据占比计算product_ratio
    product_sale_amt,
    -- 原料销售额根据占比计算product_ratio
    product_sale_amt_no_tax -- 原料未税销售额根据占比计算product_ratio
),
tmp_product_sale as (
  select
    sdt,
    receive_dc_code,
    receive_dc_name,
    region_name,
    province_name,
    city_group_name,
    order_code,
    goods_code,
    sale_amt,
    sale_qty,
    profit,
    classify_middle_name,
    product_code,
    product_name,
    short_name,
    product_tax_rate,
    product_classify_middle_name,
    product_classify_small_name,
    purchase_order_no,
    order_qty,
    order_amt,
    batch_no,
    supplier_code,
    supplier_name,
    channel_type_name,
    supplier_type_code,
    supplier_type_name,
    batch_qty,
    product_cost_amt,
    product_cost_amt_no_tax,
    product_profit,
    product_profit_no_tax,
    product_sale_amt,
    product_sale_amt_no_tax
  from
    tmp_order_sale
)
select
  *,
  total_profit/total_amt total_profit_rate,
  if(cash_pursh_sale_amt =0,0 ,cash_pursh_profit/cash_pursh_sale_amt) as cash_pursh_profit_rate,
  if(base_pursh_sale_amt=0,0, base_pursh_profit/base_pursh_sale_amt)  as base_pursh_profit_rate
from
  (
    select
      *,
      dense_rank() over(
        partition by city_group_name
        order by
          total_amt desc
      ) rn
    from
      (
        select
          region_name,
          province_name,
          city_group_name,
          product_code,
          product_name,
          product_classify_middle_name,
          product_classify_small_name, -- ,supplier_code
          -- ,supplier_name
 
          sum(sum(product_sale_amt)) over(partition by product_code, city_group_name) as total_amt,
          sum(sum(product_profit)) over(partition by product_code, city_group_name) as total_profit,
          coalesce( sum(case when supplier_type_code in('4', '10', '11', '12', '13') then batch_qty   end  ), 0 ) cash_pursh_meta_qty,
          -- 原料消耗数量
          coalesce(
            sum(
              case
                when supplier_type_code in('4', '10', '11', '12', '13') then product_cost_amt
              end
            ),
            0
          ) cash_pursh_cost_amt,
          coalesce(
            sum(
              case
                when supplier_type_code in('4', '10', '11', '12', '13') then product_cost_amt_no_tax
              end
            ),
            0
          ) cash_pursh_cost_amt_no_tax,
          coalesce(
            sum(
              case
                when supplier_type_code in('4', '10', '11', '12', '13') then product_profit
              end
            ),
            0
          ) cash_pursh_profit,
          coalesce(
            sum(
              case
                when supplier_type_code in('4', '10', '11', '12', '13') then product_profit_no_tax
              end
            ),
            0
          ) cash_pursh_profit_no_tax,
          coalesce(
            sum(
              case
                when supplier_type_code in('4', '10', '11', '12', '13') then product_sale_amt
              end
            ),
            0
          ) cash_pursh_sale_amt,
          coalesce(
            sum(
              case
                when supplier_type_code in('4', '10', '11', '12', '13') then product_sale_amt_no_tax
              end
            ),
            0
          ) cash_pursh_sale_amt_no_tax,
          coalesce(
            sum(
              case
                when supplier_type_code in('5') then batch_qty 
              end
            ),
            0
          ) base_pursh_meta_qty,
          -- 原料消耗数量
          coalesce(
            sum(
              case
                when supplier_type_code in('5') then product_cost_amt
              end
            ),
            0
          ) base_pursh_cost_amt,
          coalesce(
            sum(
              case
                when supplier_type_code in('5') then product_cost_amt_no_tax 
              end
            ),
            0
          ) base_pursh_cost_amt_no_tax,
          coalesce(
            sum(
              case
                when supplier_type_code in('5') then product_profit  
              end
            ),
            0
          ) base_pursh_profit,
          coalesce(
            sum(
              case
                when supplier_type_code in('5') then product_profit_no_tax 
              end
            ),
            0
          ) base_pursh_profit_no_tax,
          coalesce(
            sum(
              case
                when supplier_type_code in('5') then product_sale_amt  
              end
            ),
            0
          ) base_pursh_sale_amt,
          coalesce(
            sum(
              case
                when supplier_type_code in('5') then product_sale_amt_no_tax  
              end
            ),
            0
          ) base_pursh_sale_amt_no_tax
        from
          tmp_product_sale a
        group by
          -- receive_dc_code
          -- ,receive_dc_name
          region_name,
          province_name,
          city_group_name,
          product_code,
          product_name,
          product_classify_middle_name,
          product_classify_small_name -- ,supplier_code
          -- ,supplier_name
 
      ) a
  ) a
where
1=1 
${if(len(rn)==0,"","and rn <= "+rn+" ")}