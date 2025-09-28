
-- 成本端发货维度数据关系调整单
day='19990101'
columns='id,adjustment_no,adjustment_reason,adjustment_type,credential_no,item_credential_no,item_source_order_no,product_code,product_name,company_code,posting_time,create_time,update_time,create_by,update_by,location_code,location_name,qty,purchase_group_code,adjustment_amt,adjustment_amt_no_tax,adjustment_order_no,item_wms_biz_type,reservoir_area_code,sync_time'
username="all_select"
password="I&^lshoejfj02934"
sqoop import \
 --connect jdbc:mysql://10.0.74.154:3306/data_sync \
 --username "$username" \
 --password "$password" \
 --table data_relation_cas_sale_adjustment \
 --delete-target-dir \
 --fields-terminated-by '\001' \
 --target-dir hdfs://nameservice1/user/hive/warehouse/csx_ods.db/source_sync_r_d_data_relation_cas_sale_adjustment/sdt=${day} \
 --columns "${columns}" \
 --hive-drop-import-delims \
 --null-string '\\N'  \
 --null-non-string '\\N' \
 --hive-import \
 --hive-database csx_ods \
 --hive-table source_sync_r_d_data_relation_cas_sale_adjustment \
 --hive-partition-key sdt \
 --hive-partition-value "$day"

-- 工厂调整单数据清洗
day='19990101'
columns='id,create_time,update_time,create_by,update_by,posting_time,source_wms_order_no,source_wms_biz_type,source_wms_biz_type_name,source_credential_no,adjustment_no,adjustment_reason,credential_no,credential_source_order_no,company_code,company_name,location_code,location_name,fac_location_code,fac_location_name,reservoir_area_code,reservoir_area_name,adjustment_qty,adjustment_amt_no_tax,adjustment_amt,tax_rate,batch_no,fac_cost_center_code,fac_cost_center_name,fac_line_code,p_main_material_no_tax,f_main_material_no_tax,d_main_material_no_tax,p_package_material_no_tax,f_package_material_no_tax,d_package_material_no_tax,p_support_material_no_tax,f_support_material_no_tax,d_support_material_no_tax,p_work_fee,f_work_fee,d_work_fee,p_machine_fee,f_machine_fee,d_machine_fee,p_cost_subtotal,f_cost_subtotal,d_cost_subtotal,sync_time,product_code,product_name,source_order_no,purchase_group_code,purchase_group_name,fac_order_no'
username="all_select"
password="I&^lshoejfj02934"
sqoop import \
 --connect jdbc:mysql://10.0.74.154:3306/data_sync?tinyInt1isBit=false \
 --username "$username" \
 --password "$password" \
 --table data_sync_fac_adjustment_item \
 --delete-target-dir \
 --fields-terminated-by '\001' \
 --target-dir hdfs://nameservice1/user/hive/warehouse/csx_ods.db/source_sync_r_d_data_sync_fac_adjustment_item/sdt=${day} \
 --columns "${columns}" \
 --hive-drop-import-delims \
 --null-string '\\N'  \
 --null-non-string '\\N' \
 --hive-import \
 --hive-database csx_ods \
 --hive-table source_sync_r_d_data_sync_fac_adjustment_item \
 --hive-partition-key sdt \
 --hive-partition-value "$day"



-- 工厂月末分摊-调整销售订单
select 
  province_name,
  division_code, 
  division_name, 
  department_id, 
  department_name,
  sum(adjustment_amt_no_tax) as adjustment_amt_no_tax,
  sum(p_main_material_no_tax) as p_main_material_no_tax,
  sum(p_package_material_no_tax) as p_package_material_no_tax,
  sum(p_support_material_no_tax) as p_support_material_no_tax,
  sum(p_work_fee) as p_work_fee,
  sum(p_machine_fee) as p_machine_fee,
  sum(d_main_material_no_tax) as d_main_material_no_tax,
  sum(d_package_material_no_tax) as d_package_material_no_tax,
  sum(d_support_material_no_tax) as d_support_material_no_tax,
  sum(d_work_fee) as d_work_fee,
  sum(d_machine_fee) as d_machine_fee,
  sum(f_main_material_no_tax) as f_main_material_no_tax,
  sum(f_package_material_no_tax) as f_package_material_no_tax,
  sum(f_support_material_no_tax) as f_support_material_no_tax,
  sum(f_work_fee) as f_work_fee,
  sum(f_machine_fee) as f_machine_fee
from(
  select 
    b.province_name,
    d.division_code, 
    d.division_name, 
    d.department_id, 
    d.department_name,
    a.adjustment_amt_no_tax, -- 工厂月末分摊-调整销售订单
    coalesce(c.p_main_material_no_tax, 0) as p_main_material_no_tax,
    coalesce(c.f_main_material_no_tax, 0) as f_main_material_no_tax,
    coalesce(c.d_main_material_no_tax, 0) as d_main_material_no_tax,
    coalesce(c.p_package_material_no_tax, 0) as p_package_material_no_tax,
    coalesce(c.f_package_material_no_tax, 0) as f_package_material_no_tax,
    coalesce(c.d_package_material_no_tax, 0) as d_package_material_no_tax,
    coalesce(c.p_support_material_no_tax, 0) as p_support_material_no_tax,
    coalesce(c.f_support_material_no_tax, 0) as f_support_material_no_tax,
    coalesce(c.d_support_material_no_tax, 0) as d_support_material_no_tax,
    coalesce(c.p_work_fee, 0) as p_work_fee,
    coalesce(c.f_work_fee, 0) as f_work_fee,
    coalesce(c.d_work_fee, 0) as d_work_fee,
    coalesce(c.p_machine_fee, 0) as p_machine_fee,
    coalesce(c.f_machine_fee, 0) as f_machine_fee,
    coalesce(c.d_machine_fee, 0) as d_machine_fee
  from 
  (
    select
      adjustment_no,
      item_credential_no,
      location_code,
      product_code,
      sum(adjustment_amt_no_tax) as adjustment_amt_no_tax
    from csx_ods.source_sync_r_d_data_relation_cas_sale_adjustment
    where sdt = '19990101' and posting_time >= '2020-08-01 00:00:00' and posting_time < '2020-09-01 00:00:00'
      and ( adjustment_reason in('fac_remark_sale','fac_remark') 
			and item_wms_biz_type in('18','19','20','21','22','23','24','25','26','27','55','58','68','72','73',
									'A18','A20','A21','A22','A23','A24','A25','A55') )
    group by adjustment_no, item_credential_no, location_code, product_code
  ) a left join
  (
    select
      adjustment_no,
      source_credential_no,
      product_code,
      sum(p_main_material_no_tax) as p_main_material_no_tax,
      sum(f_main_material_no_tax) as f_main_material_no_tax,
      sum(d_main_material_no_tax) as d_main_material_no_tax,
      sum(p_package_material_no_tax) as p_package_material_no_tax,
      sum(f_package_material_no_tax) as f_package_material_no_tax,
      sum(d_package_material_no_tax) as d_package_material_no_tax,
      sum(p_support_material_no_tax) as p_support_material_no_tax,
      sum(f_support_material_no_tax) as f_support_material_no_tax,
      sum(d_support_material_no_tax) as d_support_material_no_tax,
      sum(p_work_fee) as p_work_fee,
      sum(f_work_fee) as f_work_fee,
      sum(d_work_fee) as d_work_fee,
      sum(p_machine_fee) as p_machine_fee,
      sum(f_machine_fee) as f_machine_fee,
      sum(d_machine_fee) as d_machine_fee
    from csx_ods.source_sync_r_d_data_sync_fac_adjustment_item
    where sdt = '19990101'
    group by adjustment_no, source_credential_no, product_code
  ) c on a.adjustment_no = c.adjustment_no and a.item_credential_no = c.source_credential_no and a.product_code = c.product_code
  left join 
  (
    select shop_id, province_name
    from csx_dw.dws_basic_w_a_csx_shop_m where sdt = 'current'
  ) b on a.location_code = b.shop_id -- if(a.location_code like 'E%', concat('9', substr(a.location_code, 2, 3)), a.location_code)
  left join
  (
    select goods_id, division_code, division_name, department_id, department_name
    from csx_dw.dws_basic_w_a_csx_product_m where sdt = 'current'
  ) d on a.product_code = d.goods_id
)a group by province_name, division_code, division_name, department_id, department_name;

-- 工厂月末分摊-调整销售订单-明细
select 
  b.province_name,
  c.posting_time,
  c.source_wms_order_no,
  c.source_wms_biz_type,
  c.source_wms_biz_type_name,
  c.source_credential_no,
  c.source_order_no,
  c.adjustment_no,
  c.adjustment_reason,
  c.credential_no,
  c.credential_source_order_no,
  c.company_code,
  c.company_name,
  c.location_code,
  c.location_name,
  c.fac_location_code,
  c.fac_location_name,
  c.reservoir_area_code,
  c.reservoir_area_name,
  c.adjustment_qty,
  c.adjustment_amt_no_tax,
  c.adjustment_amt,
  c.tax_rate,
  c.batch_no,
  c.fac_cost_center_code,
  c.fac_cost_center_name,
  c.fac_line_code,
  c.fac_order_no,
  d.division_code, 
  d.division_name, 
  d.department_id, 
  d.department_name,
  c.product_code,
  c.product_name,
  c.p_main_material_no_tax,
  c.p_package_material_no_tax,
  c.p_support_material_no_tax,
  c.p_work_fee,
  c.p_machine_fee,
  c.p_cost_subtotal,
  c.d_main_material_no_tax,
  c.d_package_material_no_tax,
  c.d_support_material_no_tax,
  c.d_work_fee,
  c.d_machine_fee,
  c.d_cost_subtotal,
  c.f_main_material_no_tax,
  c.f_package_material_no_tax,
  c.f_support_material_no_tax,
  c.f_work_fee,
  c.f_machine_fee,
  c.f_cost_subtotal
from 
(
  select distinct
    adjustment_no,
    item_credential_no,
    location_code
  from csx_ods.source_sync_r_d_data_relation_cas_sale_adjustment
  where sdt = '19990101' and posting_time >= '2020-08-01 00:00:00' and  posting_time < '2020-09-01 00:00:00'
    and ( adjustment_reason in('fac_remark_sale','fac_remark') 
			and item_wms_biz_type in('18','19','20','21','22','23','24','25','26','27','55','58','68','72','73',
									'A18','A20','A21','A22','A23','A24','A25','A55') )
) a left join
(
  select *
  from csx_ods.source_sync_r_d_data_sync_fac_adjustment_item
  where sdt = '19990101'
) c on a.adjustment_no = c.adjustment_no and a.item_credential_no = c.source_credential_no
left join 
(
  select shop_id, province_name
  from csx_dw.dws_basic_w_a_csx_shop_m where sdt = 'current'
) b on a.location_code = b.shop_id
left join
(
  select goods_id, division_code, division_name, department_id, department_name
  from csx_dw.dws_basic_w_a_csx_product_m where sdt = 'current'
) d on c.product_code = d.goods_id;

-- 工厂月末分摊-调整跨公司调拨订单
select 
  province_name,
  division_code, 
  division_name, 
  department_id, 
  department_name,
  sum(adjustment_amt_no_tax) as adjustment_amt_no_tax,
  sum(p_main_material_no_tax) as p_main_material_no_tax,
  sum(p_package_material_no_tax) as p_package_material_no_tax,
  sum(p_support_material_no_tax) as p_support_material_no_tax,
  sum(p_work_fee) as p_work_fee,
  sum(p_machine_fee) as p_machine_fee,
  sum(d_main_material_no_tax) as d_main_material_no_tax,
  sum(d_package_material_no_tax) as d_package_material_no_tax,
  sum(d_support_material_no_tax) as d_support_material_no_tax,
  sum(d_work_fee) as d_work_fee,
  sum(d_machine_fee) as d_machine_fee,
  sum(f_main_material_no_tax) as f_main_material_no_tax,
  sum(f_package_material_no_tax) as f_package_material_no_tax,
  sum(f_support_material_no_tax) as f_support_material_no_tax,
  sum(f_work_fee) as f_work_fee,
  sum(f_machine_fee) as f_machine_fee
from(
  select 
    b.province_name,
    a.adjustment_amt_no_tax, -- 工厂月末分摊-调整跨公司调拨订单
    d.division_code, 
    d.division_name, 
    d.department_id, 
    d.department_name,
    coalesce(c.p_main_material_no_tax, 0) as p_main_material_no_tax,
    coalesce(c.f_main_material_no_tax, 0) as f_main_material_no_tax,
    coalesce(c.d_main_material_no_tax, 0) as d_main_material_no_tax,
    coalesce(c.p_package_material_no_tax, 0) as p_package_material_no_tax,
    coalesce(c.f_package_material_no_tax, 0) as f_package_material_no_tax,
    coalesce(c.d_package_material_no_tax, 0) as d_package_material_no_tax,
    coalesce(c.p_support_material_no_tax, 0) as p_support_material_no_tax,
    coalesce(c.f_support_material_no_tax, 0) as f_support_material_no_tax,
    coalesce(c.d_support_material_no_tax, 0) as d_support_material_no_tax,
    coalesce(c.p_work_fee, 0) as p_work_fee,
    coalesce(c.f_work_fee, 0) as f_work_fee,
    coalesce(c.d_work_fee, 0) as d_work_fee,
    coalesce(c.p_machine_fee, 0) as p_machine_fee,
    coalesce(c.f_machine_fee, 0) as f_machine_fee,
    coalesce(c.d_machine_fee, 0) as d_machine_fee
  from 
  (
    select
      adjustment_no,
      item_credential_no,
      location_code,
	  product_code,
      sum(adjustment_amt_no_tax) as adjustment_amt_no_tax
    from csx_ods.source_sync_r_d_data_relation_cas_sale_adjustment
    where sdt = '19990101' and posting_time >= '2020-08-01 00:00:00' and  posting_time < '2020-09-01 00:00:00'
      and ( (adjustment_reason in('fac_remark_sale','fac_remark') and item_wms_biz_type in('06','07','08','09','15','17','A06','A07','A08','A09','A15') )
				or (adjustment_reason = 'fac_remark_sale' AND item_wms_biz_type='12') )
    group by adjustment_no, item_credential_no, location_code, product_code
  ) a left join
  (
    select
      adjustment_no,
      source_credential_no,
      product_code,
      sum(p_main_material_no_tax) as p_main_material_no_tax,
      sum(f_main_material_no_tax) as f_main_material_no_tax,
      sum(d_main_material_no_tax) as d_main_material_no_tax,
      sum(p_package_material_no_tax) as p_package_material_no_tax,
      sum(f_package_material_no_tax) as f_package_material_no_tax,
      sum(d_package_material_no_tax) as d_package_material_no_tax,
      sum(p_support_material_no_tax) as p_support_material_no_tax,
      sum(f_support_material_no_tax) as f_support_material_no_tax,
      sum(d_support_material_no_tax) as d_support_material_no_tax,
      sum(p_work_fee) as p_work_fee,
      sum(f_work_fee) as f_work_fee,
      sum(d_work_fee) as d_work_fee,
      sum(p_machine_fee) as p_machine_fee,
      sum(f_machine_fee) as f_machine_fee,
      sum(d_machine_fee) as d_machine_fee
    from csx_ods.source_sync_r_d_data_sync_fac_adjustment_item
    where sdt = '19990101'
    group by adjustment_no, source_credential_no, product_code
  ) c on a.adjustment_no = c.adjustment_no and a.item_credential_no = c.source_credential_no and a.product_code = c.product_code
  left join 
  (
    select shop_id, province_name
    from csx_dw.dws_basic_w_a_csx_shop_m where sdt = 'current'
  ) b on a.location_code = b.shop_id
  left join
  (
    select goods_id, division_code, division_name, department_id, department_name
    from csx_dw.dws_basic_w_a_csx_product_m where sdt = 'current'
  ) d on a.product_code = d.goods_id
)a group by province_name, division_code, division_name, department_id, department_name;

-- 工厂月末分摊-调整跨公司调拨订单-明细
select 
  b.province_name,
  c.posting_time,
  c.source_wms_order_no,
  c.source_wms_biz_type,
  c.source_wms_biz_type_name,
  c.source_credential_no,
  c.source_order_no,
  c.adjustment_no,
  c.adjustment_reason,
  c.credential_no,
  c.credential_source_order_no,
  c.company_code,
  c.company_name,
  c.location_code,
  c.location_name,
  c.fac_location_code,
  c.fac_location_name,
  c.reservoir_area_code,
  c.reservoir_area_name,
  c.adjustment_qty,
  c.adjustment_amt_no_tax,
  c.adjustment_amt,
  c.tax_rate,
  c.batch_no,
  c.fac_cost_center_code,
  c.fac_cost_center_name,
  c.fac_line_code,
  c.fac_order_no,
  d.division_code, 
  d.division_name, 
  d.department_id, 
  d.department_name,
  c.product_code,
  c.product_name,
  c.p_main_material_no_tax,
  c.p_package_material_no_tax,
  c.p_support_material_no_tax,
  c.p_work_fee,
  c.p_machine_fee,
  c.p_cost_subtotal,
  c.d_main_material_no_tax,
  c.d_package_material_no_tax,
  c.d_support_material_no_tax,
  c.d_work_fee,
  c.d_machine_fee,
  c.d_cost_subtotal,
  c.f_main_material_no_tax,
  c.f_package_material_no_tax,
  c.f_support_material_no_tax,
  c.f_work_fee,
  c.f_machine_fee,
  c.f_cost_subtotal
from 
(
  select
    adjustment_no,
    item_credential_no,
    location_code,
    sum(adjustment_amt_no_tax) as adjustment_amt_no_tax
  from csx_ods.source_sync_r_d_data_relation_cas_sale_adjustment
  where sdt = '19990101' and posting_time >= '2020-08-01 00:00:00' and  posting_time < '2020-09-01 00:00:00'
    and ( (adjustment_reason in('fac_remark_sale','fac_remark') and item_wms_biz_type in('06','07','08','09','15','17','A06','A07','A08','A09','A15') )
				or (adjustment_reason = 'fac_remark_sale' AND item_wms_biz_type='12') )
  group by adjustment_no, item_credential_no, location_code
) a left join
(
  select *
  from csx_ods.source_sync_r_d_data_sync_fac_adjustment_item
  where sdt = '19990101'
) c on a.adjustment_no = c.adjustment_no and a.item_credential_no = c.source_credential_no
left join 
(
  select shop_id, province_name
  from csx_dw.dws_basic_w_a_csx_shop_m where sdt = 'current'
) b on a.location_code = b.shop_id
left join
(
  select goods_id, division_code, division_name, department_id, department_name
  from csx_dw.dws_basic_w_a_csx_product_m where sdt = 'current'
) d on c.product_code = d.goods_id;


-- 工厂月末分摊-调整其他
select 
  province_name,
  division_code, 
  division_name, 
  department_id, 
  department_name,
  sum(adjustment_amt_no_tax) as adjustment_amt_no_tax,
  sum(p_main_material_no_tax) as p_main_material_no_tax,
  sum(p_package_material_no_tax) as p_package_material_no_tax,
  sum(p_support_material_no_tax) as p_support_material_no_tax,
  sum(p_work_fee) as p_work_fee,
  sum(p_machine_fee) as p_machine_fee,
  sum(d_main_material_no_tax) as d_main_material_no_tax,
  sum(d_package_material_no_tax) as d_package_material_no_tax,
  sum(d_support_material_no_tax) as d_support_material_no_tax,
  sum(d_work_fee) as d_work_fee,
  sum(d_machine_fee) as d_machine_fee,
  sum(f_main_material_no_tax) as f_main_material_no_tax,
  sum(f_package_material_no_tax) as f_package_material_no_tax,
  sum(f_support_material_no_tax) as f_support_material_no_tax,
  sum(f_work_fee) as f_work_fee,
  sum(f_machine_fee) as f_machine_fee
from(
  select 
    b.province_name,
	d.division_code, 
	d.division_name, 
	d.department_id, 
	d.department_name,
    a.adjustment_amt_no_tax, -- 工厂月末分摊-调整跨公司调拨订单
    coalesce(c.p_main_material_no_tax, 0) as p_main_material_no_tax,
    coalesce(c.f_main_material_no_tax, 0) as f_main_material_no_tax,
    coalesce(c.d_main_material_no_tax, 0) as d_main_material_no_tax,
    coalesce(c.p_package_material_no_tax, 0) as p_package_material_no_tax,
    coalesce(c.f_package_material_no_tax, 0) as f_package_material_no_tax,
    coalesce(c.d_package_material_no_tax, 0) as d_package_material_no_tax,
    coalesce(c.p_support_material_no_tax, 0) as p_support_material_no_tax,
    coalesce(c.f_support_material_no_tax, 0) as f_support_material_no_tax,
    coalesce(c.d_support_material_no_tax, 0) as d_support_material_no_tax,
    coalesce(c.p_work_fee, 0) as p_work_fee,
    coalesce(c.f_work_fee, 0) as f_work_fee,
    coalesce(c.d_work_fee, 0) as d_work_fee,
    coalesce(c.p_machine_fee, 0) as p_machine_fee,
    coalesce(c.f_machine_fee, 0) as f_machine_fee,
    coalesce(c.d_machine_fee, 0) as d_machine_fee
  from 
  (
    select
      adjustment_no,
      item_credential_no,
      location_code,
	  product_code,
      sum(adjustment_amt_no_tax) as adjustment_amt_no_tax
    from csx_ods.source_sync_r_d_data_relation_cas_sale_adjustment
    where sdt = '19990101' and posting_time >= '2020-08-01 00:00:00' and  posting_time < '2020-09-01 00:00:00'
      and ( adjustment_reason='fac_remark_sale' 
			and item_wms_biz_type not in('18','19','20','21','22','23','24','25','26','27','55','58','68','72','73','A18','A20','A21','A22','A23','A24','A25','A55',
										'06','07','08','09','15','17','A06','A07','A08','A09','A15','12') )
    group by adjustment_no, item_credential_no, location_code, product_code
  ) a left join
  (
    select
      adjustment_no,
      source_credential_no,
	  product_code,
      sum(p_main_material_no_tax) as p_main_material_no_tax,
      sum(f_main_material_no_tax) as f_main_material_no_tax,
      sum(d_main_material_no_tax) as d_main_material_no_tax,
      sum(p_package_material_no_tax) as p_package_material_no_tax,
      sum(f_package_material_no_tax) as f_package_material_no_tax,
      sum(d_package_material_no_tax) as d_package_material_no_tax,
      sum(p_support_material_no_tax) as p_support_material_no_tax,
      sum(f_support_material_no_tax) as f_support_material_no_tax,
      sum(d_support_material_no_tax) as d_support_material_no_tax,
      sum(p_work_fee) as p_work_fee,
      sum(f_work_fee) as f_work_fee,
      sum(d_work_fee) as d_work_fee,
      sum(p_machine_fee) as p_machine_fee,
      sum(f_machine_fee) as f_machine_fee,
      sum(d_machine_fee) as d_machine_fee
    from csx_ods.source_sync_r_d_data_sync_fac_adjustment_item
    where sdt = '19990101'
    group by adjustment_no, source_credential_no, product_code
  ) c on a.adjustment_no = c.adjustment_no and a.item_credential_no = c.source_credential_no and a.product_code = c.product_code
  left join 
  (
    select shop_id, province_name
    from csx_dw.dws_basic_w_a_csx_shop_m where sdt = 'current'
  ) b on a.location_code = b.shop_id
  left join
  (
    select goods_id, division_code, division_name, department_id, department_name
    from csx_dw.dws_basic_w_a_csx_product_m where sdt = 'current'
  ) d on a.product_code = d.goods_id
)a 
group by province_name, division_code, division_name, department_id, department_name;

-- 工厂月末分摊-调整其他-明细
select 
  b.province_name,
  c.posting_time,
  c.source_wms_order_no,
  c.source_wms_biz_type,
  c.source_wms_biz_type_name,
  c.source_credential_no,
  c.source_order_no,
  c.adjustment_no,
  c.adjustment_reason,
  c.credential_no,
  c.credential_source_order_no,
  c.company_code,
  c.company_name,
  c.location_code,
  c.location_name,
  c.fac_location_code,
  c.fac_location_name,
  c.reservoir_area_code,
  c.reservoir_area_name,
  c.adjustment_qty,
  c.adjustment_amt_no_tax,
  c.adjustment_amt,
  c.tax_rate,
  c.batch_no,
  c.fac_cost_center_code,
  c.fac_cost_center_name,
  c.fac_line_code,
  c.fac_order_no,
  d.division_code, 
  d.division_name, 
  d.department_id, 
  d.department_name,
  c.product_code,
  c.product_name,
  c.p_main_material_no_tax,
  c.p_package_material_no_tax,
  c.p_support_material_no_tax,
  c.p_work_fee,
  c.p_machine_fee,
  c.p_cost_subtotal,
  c.d_main_material_no_tax,
  c.d_package_material_no_tax,
  c.d_support_material_no_tax,
  c.d_work_fee,
  c.d_machine_fee,
  c.d_cost_subtotal,
  c.f_main_material_no_tax,
  c.f_package_material_no_tax,
  c.f_support_material_no_tax,
  c.f_work_fee,
  c.f_machine_fee,
  c.f_cost_subtotal
from 
(
  select distinct
    adjustment_no,
    item_credential_no,
    location_code
  from csx_ods.source_sync_r_d_data_relation_cas_sale_adjustment
    where sdt = '19990101' and posting_time >= '2020-08-01 00:00:00' and  posting_time < '2020-09-01 00:00:00'
      and ( adjustment_reason='fac_remark_sale' 
			and item_wms_biz_type not in('18','19','20','21','22','23','24','25','26','27','55','58','68','72','73','A18','A20','A21','A22','A23','A24','A25','A55',
										'06','07','08','09','15','17','A06','A07','A08','A09','A15','12') )
) a left join
(
  select *
  from csx_ods.source_sync_r_d_data_sync_fac_adjustment_item
  where sdt = '19990101'
) c on a.adjustment_no = c.adjustment_no and a.item_credential_no = c.source_credential_no
left join 
(
  select shop_id, province_name
  from csx_dw.dws_basic_w_a_csx_shop_m where sdt = 'current'
) b on a.location_code = b.shop_id
left join
(
  select goods_id, division_code, division_name, department_id, department_name
  from csx_dw.dws_basic_w_a_csx_product_m where sdt = 'current'
) d on c.product_code = d.goods_id;
