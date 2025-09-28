
-- sku 按小时取值，统计每小时内接单的sku数（不去重）
select
  t1.order_no_header,
  t1.sales_province,
  t1.sales_city,
  count( if(recep_order_hour = '06', goods_code, null) ) as cont_06_07,
  count( if(recep_order_hour = '07', goods_code, null) ) as cont_07_08,
  count( if(recep_order_hour = '08', goods_code, null) ) as cont_08_09,
  count( if(recep_order_hour = '09', goods_code, null) ) as cont_09_10,
  count( if(recep_order_hour = '10', goods_code, null) ) as cont_10_11,
  count( if(recep_order_hour = '11', goods_code, null) ) as cont_11_12,
  count( if(recep_order_hour = '12', goods_code, null) ) as cont_12_13,
  count( if(recep_order_hour = '13', goods_code, null) ) as cont_13_14,
  count( if(recep_order_hour = '14', goods_code, null) ) as cont_14_15,
  count( if(recep_order_hour = '15', goods_code, null) ) as cont_15_16,
  count( if(recep_order_hour = '16', goods_code, null) ) as cont_16_17,
  count( if(recep_order_hour = '17', goods_code, null) ) as cont_17_18,
  count( if(recep_order_hour = '18', goods_code, null) ) as cont_18_19,
  count( if(recep_order_hour = '19', goods_code, null) ) as cont_19_20,
  count( if(recep_order_hour = '20', goods_code, null) ) as cont_20_21,
  count( if(recep_order_hour = '21', goods_code, null) ) as cont_21_22,
  count( if(recep_order_hour = '22', goods_code, null) ) as cont_22_23,
  count( if(recep_order_hour = '23', goods_code, null) ) as cont_23_24
from
(
  select
    a.order_no_header,
    b.sales_province,
    b.sales_city,
    a.goods_code,
    a.recep_order_hour
  from
  (
    select 
      substr(order_no, 1, 2) as order_no_header, customer_no, goods_code,
      hour(cast(if(recep_order_time like '1999%', created_time, recep_order_time) as TIMESTAMP)) as recep_order_hour -- hour()获取接单时区
    from csx_dw.dws_csms_r_d_yszx_order_m_new
    where (sdt >= '20200701' or sdt = '19990101') and refund_no is null
      and if(recep_order_time like '1999%', created_time, recep_order_time) >= '2020-07-01' 
      and if(recep_order_time like '1999%', created_time, recep_order_time) < '2020-08-01'
    -- 关联旧系统订单，2020-02-29福州上线后，旧系统没有新数据产生
    -- union all
    -- select 
    --   substr(order_no, 1, 2) as order_no_header,
    --   sap_cus_code as customer_no,
    --   product_code as goods_code,
    --   hour(cast(if(recep_order_time like '1999%', created_time, recep_order_time) as TIMESTAMP)) as recep_order_hour
    -- from csx_dw.dwd_csms_r_d_yszx_order_detail
    -- where (sdt >= '20200701' or sdt = '19990101') and refund_no is null and parent_order_no = ''
    --   and if(recep_order_time like '1999%', created_time, recep_order_time) >= '2020-07-01' 
    --   and if(recep_order_time like '1999%', created_time, recep_order_time) < '2020-08-01' 
    --   and (item_status is not null or item_status <> 0)
  ) a left join
  (
    select
      customer_no, sales_province, sales_city
    from csx_dw.dws_crm_w_a_customer_m_v1 
    where sdt = 'current'
  ) b on a.customer_no = b.customer_no
) t1 group by t1.order_no_header, t1.sales_province, t1.sales_city;
