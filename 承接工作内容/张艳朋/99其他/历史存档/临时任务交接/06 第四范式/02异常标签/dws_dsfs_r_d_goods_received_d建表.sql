
CREATE TABLE `csx_dw.dws_dsfs_r_d_goods_received_d`(
  `source_type` int, 
  `super_class` int, 
  `order_code` string, 
  `dc_province_code` string, 
  `dc_province_name` string, 
  `dc_city_group_code` string, 
  `dc_city_group_name` string, 
  `dc_dc_code` string, 
  `dc_dc_name` string, 
  `goods_code` string, 
  `goods_name` string, 
  `unit` string, 
  `department_id` string, 
  `department_name` string, 
  `classify_middle_code` string, 
  `classify_middle_name` string, 
  `division_code` string, 
  `division_name` string, 
  `received_qty` decimal(34,5), 
  `received_value` decimal(38,23), 
  `received_price` decimal(38,27), 
  `received_qty_ls` decimal(38,5), 
  `received_value_ls` decimal(38,23), 
  `received_price_ls` decimal(38,27), 
  `received_qty_last` decimal(38,5), 
  `received_value_last` decimal(38,23), 
  `received_price_last` decimal(38,27), 
  `received_qty_yc` decimal(30,6), 
  `received_value_yc` decimal(30,6), 
  `received_price_yc` decimal(38,22), 
  `received_price_hight` int, 
  `received_price_low` int, 
  `received_price_up` int, 
  `received_price_down` int
)
COMMENT '采购入库异常+标签'
PARTITIONED BY (
  scm_sdt string COMMENT '采购时间')
STORED AS textfile;