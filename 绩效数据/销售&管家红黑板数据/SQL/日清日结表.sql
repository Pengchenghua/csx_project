select
  sdt,
  index_name,
  performance_province_code,
  performance_province_name,
  performance_city_name,
  service_user_name,
  order_code,
  customer_code,
  require_delivery_date,
  second_dept_name,
  first_dept_name
from
  csx_ods.csx_ods_csx_data_market_report_order_control_oneday_settlement_df_ss_df
where
  sdt >= '20250601'
  and require_delivery_date>='20250601' and require_delivery_date<='20250630'
  and is_access_monitor=1
--   index_name in ()
    and second_dept_code='401'
    and service_user_name !=''
 group by  
  service_user_name,
  index_name



select *
    from  csx_analyse.csx_analyse_fr_hr_red_black_sale_info 
        where smt= substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
        and sdt= regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
    
    
    
    ;
    
select
  performance_region_name,
  b.performance_province_name,
  service_user_number,
  service_user_name,
  order_code,
  a.customer_code,
  customer_name,
  require_delivery_date,
  is_access_monitor,
  second_dept_name,
  first_dept_name
from
     csx_analyse.csx_analyse_report_order_control_oneday_settlement_df_ss_mf a 
  left join 
  (select customer_code,performance_region_name,
    performance_province_name
    from csx_dim.csx_dim_crm_customer_info
    where sdt='current') b on a.customer_code=b.customer_code
where
  smt = '202506'
  
  

CREATE EXTERNAL TABLE csx_analyse.csx_analyse_report_order_control_oneday_settlement_df_ss_mf(
  id bigint COMMENT '自增主键', 
  index_code string COMMENT '指标编码', 
  index_name string COMMENT '指标名称', 
  performance_province_code string COMMENT '业绩省区编码', 
  performance_province_name string COMMENT '业绩省区名称', 
  performance_city_code string COMMENT '业绩城市编码', 
  performance_city_name string COMMENT '业绩城市名称', 
  order_code string COMMENT '订单号', 
  refund_code string COMMENT '退货单号', 
  customer_code string COMMENT '客户编码', 
  customer_name string COMMENT '客户名称', 
  sub_customer_code string COMMENT '子客户编码', 
  sub_customer_name string COMMENT '子客户名称', 
  inventory_dc_code string COMMENT '库存地点编码', 
  inventory_dc_name string COMMENT '库存地点名称', 
  require_delivery_date int COMMENT '要求送货日期', 
  posting_date string COMMENT '过账日期', 
  is_access_monitor int COMMENT '是否进入监控：1-是，0-否', 
  second_dept_code string COMMENT '二级责任部门编码', 
  second_dept_name string COMMENT '二级责任部门', 
  first_dept_code string COMMENT '一级责任部门编码', 
  first_dept_name string COMMENT '一级责任部门', 
  sales_user_name string COMMENT '销售员名称', 
  service_user_name string COMMENT '管家名称', 
  create_time timestamp COMMENT '创建时间', 
  create_by string COMMENT '创建人', 
  update_time timestamp COMMENT '更新时间', 
  update_by string COMMENT '更新人', 
  update_date int COMMENT '更新日期',
  sdt string COMMENT '数据日期历史每日分区')
COMMENT '订单履约监控-日清日结日统计'
PARTITIONED BY ( 
  smt string COMMENT '月日期分区{"FORMAT":"yyyymm"}')
STORED AS  parquet;
