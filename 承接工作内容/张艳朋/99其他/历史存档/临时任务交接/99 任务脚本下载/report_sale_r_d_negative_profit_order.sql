set hive.support.quoted.identifiers=none;
set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
-- 作业负责人
set create_by='zhaoxiaomin';
--当前时间
set i_sdate_16 = from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss');

--昨日，昨日的上上月1日
set current_sdate =regexp_replace(date_sub(current_date,1),'-','');
set before_0mon =regexp_replace(trunc(date_sub(current_date,1),'MM'),'-','');
set before_2mon =regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-2),'-','');


--订单明细
drop table csx_tmp.tmp_profit_detail_1;
create temporary table csx_tmp.tmp_profit_detail_1
as
select 
  a.id,
  a.credential_no,
  a.business_type_code,
  a.business_type_name,
  a.province_code,
  a.province_name,
  a.city_group_code,
  a.city_group_name,  
  a.channel_code,
  a.channel_name, 
  a.dc_code,
  a.dc_name,
  if(a.customer_no like 'S%','4', b.attribute)  attribute_code,
  if(a.customer_no like 'S%','M端',b.attribute_desc) attribute_name,
  a.customer_no,
  a.customer_name,
  a.first_category_code,
  a.first_category_name,
  a.second_category_code,
  a.second_category_name,
  a.third_category_code,
  a.third_category_name,
  a.work_no,
  a.sales_name,
  b.first_supervisor_work_no,
  b.first_supervisor_name,
  a.goods_code,
  a.bar_code,
  a.goods_name,
  a.origin_order_no,
  a.order_no,
  a.division_code,
  a.division_name,
  a.classify_large_code,
  a.classify_large_name,
  a.classify_middle_code,
  a.classify_middle_name,
  a.classify_small_code,
  a.classify_small_name,  
  a.department_id,
  a.department_name,
  a.category_large_code,
  a.category_large_name,
  a.category_middle_code,
  a.category_middle_name,
  a.category_small_code,
  a.category_small_name,
  --e.qty,e.amt,
  a.sales_qty,
  a.sales_value,
  a.cost_price,
  a.purchase_price,
  a.middle_office_price,
  a.sales_price,  
  a.profit,
  coalesce(round((a.profit/abs(a.sales_value)),6), '0') prorate,
  --concat(coalesce(round((a.profit/abs(a.sales_value))*100,2), '0.00'),'%') prorate,
  a.sdt
from 
  (select 
    id,
    split(id, '&')[0] as credential_no,
    business_type_code,
    business_type_name,  
    sdt,
    province_code,
    province_name,
    city_group_code,
    city_group_name,
    channel_code,
    channel_name,
    dc_code,
    dc_name,
    customer_no,
    customer_name,
    --attribute_name,
    --attribute_code,
    first_category_code,
    first_category_name,
    second_category_code,
    second_category_name,
    third_category_code,
    third_category_name,
    work_no,
    sales_name,
    goods_code,
    goods_name,
    bar_code,
    division_code,
    division_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,  
    department_code department_id,
    department_name,
    category_large_code,
    category_large_name,
    category_middle_code,
    category_middle_name,
    category_small_code,
    category_small_name,
    origin_order_no,
    order_no,
    sales_qty,
    sales_value,
    profit,
    cost_price,
    purchase_price,
    middle_office_price,
    sales_price
  from csx_dw.dws_sale_r_d_detail 
  where sdt>=${hiveconf:before_0mon}
  and sdt<=${hiveconf:current_sdate}
  --and channel_code in('1','7','9')
  and profit<0
  )a
left join  ----客户维度
(select  
    customer_no,
    attribute,
    attribute_desc,
    first_supervisor_work_no,
    first_supervisor_name
  from csx_dw.dws_crm_w_a_customer   
  where sdt='current'
)b on a.customer_no=b.customer_no 
;




--订单明细导入
insert overwrite table csx_dw.report_sale_r_d_negative_profit_order  partition(sdt)
select 
    id,
  credential_no,
  business_type_code,
    business_type_name,
  province_code,
  province_name,
    city_group_code,
    city_group_name,  
  channel_code,
  channel_name, 
  dc_code,
  dc_name,
  attribute_code,
  attribute_name,
  customer_no,
  customer_name,
  first_category_code,
  first_category_name,
  second_category_code,
  second_category_name,
  third_category_code,
  third_category_name,
  work_no,
  sales_name,
  first_supervisor_work_no,
  first_supervisor_name,
  goods_code, 
  goods_name,
  bar_code,
  origin_order_no,
  order_no,
  division_code,
  division_name,
  classify_large_code,
  classify_large_name,
  classify_middle_code,
  classify_middle_name,
  classify_small_code,
  classify_small_name,
  department_id,
  department_name,
  category_large_code,
  category_large_name,
  category_middle_code,
  category_middle_name,
  category_small_code,
  category_small_name,
  sales_qty,
  sales_value,
  cost_price,
  purchase_price,
  middle_office_price,
  sales_price,  
  profit,
  prorate,
  sdt
from csx_tmp.tmp_profit_detail_1;




drop table if exists csx_tmp.report_sale_r_d_negative_profit_order;
CREATE TABLE csx_tmp.report_sale_r_d_negative_profit_order(
  id string COMMENT '唯一id',
  credential_no string COMMENT '凭证号',
  business_type_code string COMMENT '业务类型编码',
  business_type_name string COMMENT '业务类型',
  province_code string COMMENT '省区编码',
  province_name string COMMENT '省区名称',
  city_group_code string COMMENT '城市组编码',
  city_group_name string COMMENT '城市组名称',
  channel_code string COMMENT '渠道编码',
  channel_name string COMMENT '渠道名称',
  dc_code string COMMENT '库存地点编码',
  dc_name string COMMENT '库存地点名称',
  attribute_code string COMMENT '客户属性编码',
  attribute_name string COMMENT '客户属性名称',
  customer_no string COMMENT '客户编码',
  customer_name string COMMENT '客户名称',
  first_category_code string COMMENT '一级客户分类编码',
  first_category_name string COMMENT '一级客户分类',
  second_category_code string COMMENT '二级客户分类编码',
  second_category_name string COMMENT '二级客户分类',
  third_category_code string COMMENT '三级客户分类编码',
  third_category_name string COMMENT '三级客户分类',
  work_no string COMMENT '销售员工号',
  sales_name string COMMENT '销售员名称',
  first_supervisor_work_no string COMMENT '一级主管工号',
  first_supervisor_name string COMMENT '一级主管姓名',
  goods_code string COMMENT '商品编码',
  goods_name string COMMENT '商品名称',
  bar_code string COMMENT '商品条码',
  origin_order_no string COMMENT '原始单号',
  order_no string COMMENT '订单号',
  division_code string COMMENT '部类编号',
  division_name string COMMENT '部类名称',
  classify_large_code string COMMENT '管理大类编号',
  classify_large_name string COMMENT '管理大类名称',
  classify_middle_code string COMMENT '管理中类编号',
  classify_middle_name string COMMENT '管理中类名称',
  classify_small_code string COMMENT '管理小类编号',
  classify_small_name string COMMENT '管理小类名称',
  department_id string COMMENT '课组编码',
  department_name string COMMENT '课组描述',
  category_large_code string COMMENT '大类编号',
  category_large_name string COMMENT '大类名称',
  category_middle_code string COMMENT '中类编号',
  category_middle_name string COMMENT '中类名称',
  category_small_code string COMMENT '小类编号',
  category_small_name string COMMENT '小类名称',
  sales_qty decimal(20,6) COMMENT '销售数量',
  sales_value decimal(20,6) COMMENT '含税销售金额',
  cost_price decimal(20,6) COMMENT '成本含税单价',
  purchase_price decimal(20,6) COMMENT '采购报价',
  middle_office_price decimal(20,6) COMMENT '中台报价',
  sales_price decimal(20,6) COMMENT '销售订单含税单价',
  profit decimal(20,6) COMMENT '含税定价毛利额',
  prorate decimal(20,6) COMMENT '含税定价毛利率',
  create_by string COMMENT '创建人' ,
  create_time string COMMENT '创建时间' ,
  update_time string COMMENT '更新时间' 
) COMMENT '负毛利订单明细'
PARTITIONED BY (sdt string COMMENT '日期分区')
STORED AS TEXTFILE;


insert overwrite table csx_tmp.report_sale_r_d_negative_profit_order partition (sdt)
select 
  `(sdt)?+.+`,
  ${hiveconf:create_by} as create_by,
  ${hiveconf:i_sdate_16} as create_time,
  ${hiveconf:i_sdate_16} as update_time ,
  sdt
from csx_dw.report_sale_r_d_negative_profit_order 
where sdt>=${hiveconf:before_0mon};


