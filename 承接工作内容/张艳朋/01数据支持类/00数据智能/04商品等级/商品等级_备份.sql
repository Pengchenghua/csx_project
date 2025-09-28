






set hive.execution.engine=tez; 
set tez.queue.name=caishixian; --设置彩食鲜队列
set hive.exec.max.dynamic.partitions.pernode=1000;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;
set hive.support.quoted.identifiers=none;


select
sum(sales_sku) as sales_sku,
sum(sales_value) as sales_value,
sum(profit) as profit,
sum(remark_sku) as remark_sku,
sum(sales_level_sku) as sales_level_sku,
sum(sales_level_value) as sales_level_value,
sum(sales_level_profit) as sales_level_profit,
sum(remark_level_sku) as remark_level_sku,
sum(goods_level_sku) as goods_level_sku
from
(
select
  count(distinct goods_code) as sales_sku,
  sum(sales_value) as sales_value,
  sum(profit) as profit,
  0 as remark_sku,
  0 as sales_level_sku,
  0 as sales_level_value,
  0 as sales_level_profit,
  0 as remark_level_sku,
  0 as goods_level_sku
from
(
  select
    a.goods_code,
    a.sales_value,
    a.profit
  from
  (
    select
    goods_code,
    sum(sales_value) as sales_value,
    sum(profit) as profit
    from csx_dw.dws_sale_r_d_customer_sale
    where sdt >= '20200901' and sdt <= '20200927'
      and channel in ('1','7','9') and is_self_sale = 1
    group by goods_code
  ) a
  left join
  (
    select
      goods_id,
    division_code
    from csx_dw.dws_basic_w_a_csx_product_m
    where sdt = 'current'
  ) c on a.goods_code = c.goods_id
  where c.division_code in (10,11)
) a


union all


select
  0 as sales_sku,
  0 as sales_value,
  0 as profit,
  sum(is_remark) as remark_sku,
  0 as sales_level_sku,
  0 as sales_level_value,
  0 as sales_level_profit,
  0 as remark_level_sku,
  0 as goods_level_sku
from
(
  select
    goods_code,
    case when sum(is_remark) > 0 then 1
    else 0 end as is_remark
  from
  (
    select
      a.order_no,
      a.goods_code,
      case when b.order_no is null then 0
        else 1 end as is_remark
    from
    (
      select
        order_no,
        goods_code
      from csx_dw.dws_sale_r_d_customer_sale
      where sdt >= '20200901' and sdt <= '20200927'
        and channel in ('1','7','9') and is_self_sale = 1
      group by order_no,goods_code
    ) a
    left join
    (
      SELECT
        order_no, refund_no, goods_code, spec_remarks, buyer_remarks
      FROM csx_dw.dwd_csms_r_d_yszx_order_detail_new
      WHERE (sdt >= '20200901' OR sdt = '19990101')
        AND (spec_remarks <> '' OR buyer_remarks <> '') AND (item_status is null or item_status <> 0)
    ) b on a.order_no = coalesce(b.refund_no, b.order_no) AND a.goods_code = b.goods_code
    left join
    (
      select
        goods_id,
    division_code
      from csx_dw.dws_basic_w_a_csx_product_m
      where sdt = 'current'
    ) c on a.goods_code = c.goods_id
    where c.division_code in (10,11)
  ) a group by goods_code
) a

union all

select
  0 as sales_sku,
  0 as sales_value,
  0 as profit,
  0 as remark_sku,
  count(distinct goods_code) as sales_level_sku,
  sum(sales_value) as sales_level_value,
  sum(profit) as sales_level_profit,
  0 as remark_level_sku,
  0 as goods_level_sku
from
(
    select
      a.goods_code,
      a.sales_value,
      a.profit
  from
    (
      select
        goods_code,
        sum(sales_value) as sales_value,
        sum(profit) as profit
      from csx_dw.dws_sale_r_d_customer_sale
      where sdt >= '20200901' and sdt <= '20200927'
        and channel in ('1','7','9') and is_self_sale = 1
      group by goods_code
    ) a
    left join
    (
      select
        goods_id,
        goods_name,
        product_level,
    division_code
      from csx_dw.dws_basic_w_a_csx_product_m
      where sdt = 'current'
    ) b on a.goods_code = b.goods_id
    where b.product_level <> '-1' and b.division_code in (10,11)
) a

union all

select
  0 as sales_sku,
  0 as sales_value,
  0 as profit,
  0 as remark_sku,
  0 as sales_level_sku,
  0 as sales_level_value,
  0 as sales_level_profit,
  sum(is_remark) as remark_level_sku,
  0 as goods_level_sku
from
(
  select
    goods_code,
    case when sum(is_remark) > 0 then 1
    else 0 end as is_remark
  from
  (
    select
      a.order_no,
      a.goods_code,
      case when c.order_no is null then 0
        else 1 end as is_remark
  from
    (
      select
        order_no,
        goods_code
      from csx_dw.dws_sale_r_d_customer_sale
      where sdt >= '20200901' and sdt <= '20200927'
        and channel in ('1','7','9') and is_self_sale = 1
      group by order_no,goods_code
    ) a
    left join
    (
      select
        goods_id,
        product_level,
    division_code
      from csx_dw.dws_basic_w_a_csx_product_m
      where sdt = 'current'
    ) b on a.goods_code = b.goods_id
    left join
    (
      SELECT
        order_no, refund_no, goods_code, spec_remarks, buyer_remarks
      FROM csx_dw.dwd_csms_r_d_yszx_order_detail_new
      WHERE (sdt >= '20200901' OR sdt = '19990101')
        AND (spec_remarks <> '' OR buyer_remarks <> '') AND (item_status is null or item_status <> 0)
    ) c on a.order_no = coalesce(c.refund_no, c.order_no) AND a.goods_code = c.goods_code
    where b.product_level <> '-1' and b.division_code in (10,11)
  ) a group by goods_code
) a

union all

select
  0 as sales_sku,
  0 as sales_value,
  0 as profit,
  0 as remark_sku,
  0 as sales_level_sku,
  0 as sales_level_value,
  0 as sales_level_profit,
  0 as remark_level_sku,
  count(distinct goods_id) as goods_level_sku
from csx_dw.dws_basic_w_a_csx_product_m
  where sdt = 'current' and product_level <> '-1' and division_code in (10,11)
) a
;








select 
  goods_code,
  goods_name,
  sales_value,
  product_level,
  product_level_name
from 
(
select 
  goods_code,
  sum(sales_value) as sales_value 
from csx_dw.dws_sale_r_d_customer_sale 
where sdt >= '20200901' and sdt <= '20200926'
  and channel in ('1','7','9') and is_self_sale = 1
group by goods_code
) a 
left join 
(
  select 
    goods_id,
    goods_name,
    division_code,
    division_name,
    department_id，
    department_name，
    product_level,
    product_level_name
  from csx_dw.dws_basic_w_a_csx_product_m 
  where sdt = 'current'
) b on a.goods_code = b.goods_id




























