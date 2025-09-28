

username="all_select"
password="I&^lshoejfj02934"
columns='id,header_id,item_no,credential_no,move_type,direction,receive_type,bar_code,product_code,product_name,purchase_group_code,purchase_group_name,unit,gift_flag,associate,inner_order_no,reckoning_diff_qty,receiving_diff_qty,on_way_qty,qty,profit_center,cost_center_code,price,location_code,real_location_code,other_side_location_code,supplier_code,supplier_type,shipper_code,shipper_name,currency,customer_code,category_code,valuation_category_code,workshop_code,workshop_name,tax_code,tax_rate,avg_price_flag,amt,settle_amt,settle_price,protocol_amt,protocol_amt_no_tax,protocol_price,protocol_price_no_tax,amt_no_tax,tax_amt,adjust_amt,next_adjust_amt,accounting_group,asset_no,batch_no,reservoir_area_code,reservoir_area_name,create_by,update_by,update_time,create_time,link_wms_order_no,wms_order_no,frozen,company_code,company_name,location_name,real_location_name,other_side_location_name,supplier_name,other_area_code,other_area_name,wms_batch_no,move_name,source_order_no,plus_price,link_wms_batch_no,cost_center_name,used_by_department_code,used_by_department_name,settle_company_code,purchase_org_name,purchase_org_code,wms_order_time,source_order_type,in_out_type,customer_name,category_name,small_category_code,small_category_name,is_same_company,wms_order_type,valuation_category_name,settle_location_code,settle_location_name,settle_company_name,plus_price_proportion,price_type,is_direct_accounting,source_order_type_code,other_side_company_code,other_side_company_name,posting_time,settle_area_code,settle_area_name,is_write_off,write_off_flag,is_finance_split,price_no_tax'
sqoop import \
 --connect jdbc:mysql://10.0.74.154:3306/csx_b2b_accounting?tinyInt1isBit=false \
 --username "$username" \
 --password "$password" \
 --table accounting_credential_item \
 --fields-terminated-by '\001' \
 --columns "${columns}" \
 --hive-drop-import-delims \
 --null-string '\\N'  \
 --null-non-string '\\N' \
 --hive-overwrite \
 --hive-import \
 --hive-database csx_ods \
 --hive-table source_cas_r_d_accounting_credential_item \
 --hive-partition-key sdt \
 --hive-partition-value "19990101"





username="all_select"
password="I&^lshoejfj02934"
columns='id,header_id,credential_no,credential_type,reassessment,wms_biz_type,posting_time,input_time,input_user,settlement_status,ref_no,ref_type,source_order_no,source_order_type,is_direct_accounting,is_remedy,repay_flag,is_same_company,is_company_purchase,credential_date,ref_biz_type,ref_biz_no,transfer_no,make_finance,sap_order_status,sap_order_no,sap_order_time,orig_credential_no,out_order_line_no,serial_no,diff_record,balance_down,loss_reason_code,purchase_org_code,frozen,update_time,update_by,create_by,create_time,wms_order_no,wms_batch_no,is_financial_vouchers,is_write_off,shipper_name,shipper_code,company_code,company_name,purchase_org_name,wms_order_type,wms_order_type_code,source_order_type_code,wms_biz_type_name,write_off_flag,is_finance_split'
sqoop import \
 --connect jdbc:mysql://10.0.74.154:3306/csx_b2b_accounting?tinyInt1isBit=false \
 --username "$username" \
 --password "$password" \
 --table  accounting_credential_header \
 --fields-terminated-by '\001' \
 --columns "${columns}" \
 --hive-drop-import-delims \
 --null-string '\\N'  \
 --null-non-string '\\N' \
 --hive-overwrite \
 --hive-import \
 --hive-database csx_ods \
 --hive-table source_cas_r_d_accounting_credential_header \
 --hive-partition-key sdt \
 --hive-partition-value "19990101"


/*业务盘点*/
--盘点过账
SELECT
	create_time as '创建时间', 
	posting_time as '记账日期',
	company_code as '公司代码',
	company_name as '公司名称',
	location_code as 'DC编码',
	location_name as 'DC名称',
	reservoir_area_code as '库区编码',
	reservoir_area_name as '库区名称',
	purchase_group_code as '采购组编码',
	purchase_group_name as '采购组名称',
	product_code as '商品编码',
	product_name as '商品名称',
	move_type as '移动类型编码',
	move_name as '移动类型名称',
	(case when direction='-' then -1*qty else qty end) as '盘点数量',
	(case when direction='-' then -1*amt_no_tax else amt_no_tax end) as '盘点不含税金额',
	(case when direction='-' then -1*amt else amt end) as '盘点含税金额'
FROM
	(select * from csx_ods.source_cas_r_d_accounting_credential_item where sdt='19990101') item 
WHERE
	to_date(item.posting_time) >= '2020-03-01' 
	AND to_date(item.posting_time) < '2020-04-01'
	AND substr(move_type,1,3) in ('110','111') 
	AND substr(reservoir_area_code,1,2) <>'PD';
	

--盘点未过账
SELECT
	create_time as '创建时间', 
	posting_time as '记账日期',
	company_code as '公司代码',
	company_name as '公司名称',
	location_code as 'DC编码',
	location_name as 'DC名称',
	reservoir_area_code as '库区编码',
	reservoir_area_name as '库区名称',
	purchase_group_code as '采购组编码',
	purchase_group_name as '采购组名称',
	product_code as '商品编码',
	product_name as '商品名称',
	move_type as '移动类型编码',
	move_name as '移动类型名称',
	(case when direction='-' then -1*qty else qty end) as '盘点数量',
	(case when direction='-' then -1*amt_no_tax else amt_no_tax end) as '盘点不含税金额',
	(case when direction='-' then -1*amt else amt end) as '盘点含税金额'
FROM
	(select * from csx_ods.source_cas_r_d_accounting_credential_item where sdt='19990101') item  
WHERE
	posting_time >= '2020-03-01 00:00:00' 
	AND posting_time < '2020-04-01 00:00:00'
	AND substr(move_type,1,3) in ('115','116') 
	AND amt_no_tax<>'0';




--报损
create temporary table b2b_tmp.yw_bs_01
as
 SELECT
	item.create_time, 
	item.posting_time,
	item.company_code,
	item.company_name,
	item.location_code,
	item.location_name,
	item.reservoir_area_code,
	item.reservoir_area_name,
	item.purchase_group_code,
	item.purchase_group_name,
	item.product_code ,
	item.product_name ,
	item.move_type,
	item.move_name,
	header.wms_biz_type,
	header.wms_biz_type_name,
	(case when item.direction='-' then -1*item.qty else item.qty end) as bs_qty,
	(case when item.direction='-' then -1*item.amt_no_tax else item.amt_no_tax end) as bs_amt_no_tax,
	(case when item.direction='-' then -1*item.amt else item.amt end) as bs_amt
FROM
	(select * from csx_ods.source_cas_r_d_accounting_credential_item where sdt='19990101') item
	LEFT JOIN (select * from csx_ods.source_cas_r_d_accounting_credential_header where sdt='19990101')header on item.credential_no=header.credential_no
WHERE
	to_date(item.posting_time) >= '2020-03-01' 
	AND to_date(item.posting_time) < '2020-04-01'
	AND substr(item.move_type,1,3) in ('117') 



--SELECT
--	sum((case when item.direction='-' then -1*item.qty else item.qty end) )as bs_qty,
--	sum((case when item.direction='-' then -1*item.amt_no_tax else item.amt_no_tax end)) as bs_amt_no_tax,
--	sum((case when item.direction='-' then -1*item.amt else item.amt end) )as bs_amt
--FROM
--	(select * from csx_ods.source_cas_r_d_accounting_credential_item where sdt='19990101') item
--	LEFT JOIN (select * from csx_ods.source_cas_r_d_accounting_credential_header where sdt='19990101')header on item.credential_no=header.credential_no
--WHERE
--	to_date(item.posting_time)>= '2020-03-01' 
--	AND to_date(item.posting_time)< '2020-04-01' 
--	AND substr(item.move_type,1,3) in ('117') 