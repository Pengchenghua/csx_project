/*业务盘点*/
--盘点未过账
insert overwrite directory '/tmp/raoyanhua/linshi01' row format delimited fields terminated by '\t'
SELECT
	create_time as `创建时间`, 
	posting_time as `记账日期`,
	company_code as `公司代码`,
	company_name as `公司名称`,
	location_code as `DC编码`,
	location_name as `DC名称`,
	reservoir_area_code as `库区编码`,
	reservoir_area_name as `库区名称`,
	purchase_group_code as `采购组编码`,
	purchase_group_name as `采购组名称`,
	product_code as `商品编码`,
	product_name as `商品名称`,
	move_type as `移动类型编码`,
	move_name as `移动类型名称`,
	--(case when direction='-' then -1*qty else qty end) as `盘点数量`,
	--(case when direction='-' then -1*amt_no_tax else amt_no_tax end) as `盘点不含税金额`,
	--(case when direction='-' then -1*amt else amt end) as `盘点含税金额'
	(case when direction='+' then -1*qty else qty end) as `盘点数量`,
	(case when direction='+' then -1*amt_no_tax else amt_no_tax end) as `盘点不含税金额`,
	(case when direction='+' then -1*amt else amt end) as `盘点含税金额`

FROM
	(select * from csx_ods.source_cas_r_d_accounting_credential_item where sdt='19990101') item 
WHERE
	to_date(item.posting_time) >= '2020-04-01' 
	AND to_date(item.posting_time) < '2020-05-01'
	AND substr(move_type,1,3) in ('110','111') 
  --AND substr(reservoir_area_code,1,2) <>'PD';
	AND substr(reservoir_area_code,1,2)='PD';

/*财务盘点*/	
--盘点过账	
insert overwrite directory '/tmp/raoyanhua/linshi02' row format delimited fields terminated by '\t'
SELECT
	create_time as `创建时间`, 
	posting_time as `记账日期`,
	company_code as `公司代码`,
	company_name as `公司名称`,
	location_code as `DC编码`,
	location_name as `DC名称`,
	reservoir_area_code as `库区编码`,
	reservoir_area_name as `库区名称`,
	purchase_group_code as `采购组编码`,
	purchase_group_name as `采购组名称`,
	product_code as `商品编码`,
	product_name as `商品名称`,
	move_type as `移动类型编码`,
	move_name as `移动类型名称`,
	(case when direction='-' then -1*qty else qty end) as `盘点数量`,
	(case when direction='-' then -1*amt_no_tax else amt_no_tax end) as `盘点不含税金额`,
	(case when direction='-' then -1*amt else amt end) as `盘点含税金额`
FROM
	(select * from csx_ods.source_cas_r_d_accounting_credential_item where sdt='19990101') item  
WHERE
	posting_time >= '2020-04-01 00:00:00' 
	AND posting_time < '2020-05-01 00:00:00'
	AND substr(move_type,1,3) in ('115','116') 
	AND amt_no_tax<>'0';


--报损
insert overwrite directory '/tmp/raoyanhua/linshi03' row format delimited fields terminated by '\t'
 SELECT
	item.credential_no as `凭证号`,
	item.create_time as `创建时间`, 
	item.posting_time as `记账日期`,
	item.company_code as `公司代码`,
	item.company_name as `公司名称`,
	item.location_code as `DC编码`,
	item.location_name as `DC名称`,
	item.reservoir_area_code as `库区编码`,
	item.reservoir_area_name as `库区名称`,
	item.purchase_group_code as `采购组编码`,
	item.purchase_group_name as `采购组名称`,
	item.product_code as `商品编码`,
	item.product_name as `商品名称`,
	item.move_type as `移动类型编码`,
	item.move_name as `移动类型名称`,
	header.wms_biz_type as `报损类型`,
	header.wms_biz_type_name as `报损名称`,
	(case when item.direction='-' then -1*item.qty else item.qty end) as bs_qty,
	(case when item.direction='-' then -1*item.amt_no_tax else item.amt_no_tax end) as bs_amt_no_tax,
	(case when item.direction='-' then -1*item.amt else item.amt end) as bs_amt
FROM
	(select * from csx_ods.source_cas_r_d_accounting_credential_item where sdt='19990101') item
	LEFT JOIN (select * from csx_ods.source_cas_r_d_accounting_credential_header where sdt='19990101')header on item.credential_no=header.credential_no
WHERE
	to_date(item.posting_time) >= '2020-04-01' 
	AND to_date(item.posting_time) < '2020-05-01'
	AND substr(item.move_type,1,3) in ('117');
	
	
----------------------------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------------	
--晓敏表里：不含税 -1060993.2 含税 -936917.48 ，  现在跑数：不含税 -1060993.2 含税 -936917.48
/*业务盘点*/
--盘点未过账
SELECT
	sum((case when direction='+' then -1*qty else qty end)) as `盘点数量`,
	sum((case when direction='+' then -1*amt_no_tax else amt_no_tax end)) as `盘点不含税金额`,
	sum((case when direction='+' then -1*amt else amt end)) as `盘点含税金额`

FROM
	(select * from csx_ods.source_cas_r_d_accounting_credential_item where sdt='19990101') item 
WHERE
	to_date(item.posting_time) >= '2020-03-01' 
	AND to_date(item.posting_time) < '2020-04-01'
	AND substr(move_type,1,3) in ('110','111') 
  --AND substr(reservoir_area_code,1,2) <>'PD';
	AND substr(reservoir_area_code,1,2)='PD';
	

--晓敏表里：不含税 -3016028.11 含税 -3102764.77，  现在跑数：不含税 -3014138.49 含税 -3100653.25
/*财务盘点*/	
--盘点过账	
SELECT
	sum((case when direction='-' then -1*qty else qty end)) as `盘点数量`,
	sum((case when direction='-' then -1*amt_no_tax else amt_no_tax end)) as `盘点不含税金额`,
	sum((case when direction='-' then -1*amt else amt end)) as `盘点含税金额`
FROM
	(select * from csx_ods.source_cas_r_d_accounting_credential_item where sdt='19990101') item  
WHERE
	posting_time >= '2020-03-01 00:00:00' 
	AND posting_time < '2020-04-01 00:00:00'
	AND substr(move_type,1,3) in ('115','116') 
	AND amt_no_tax<>'0';


--
--报损
insert overwrite directory '/tmp/raoyanhua/linshi03' row format delimited fields terminated by '\t'
 SELECT
	sum((case when item.direction='-' then -1*item.qty else item.qty end)) as bs_qty,
	sum((case when item.direction='-' then -1*item.amt_no_tax else item.amt_no_tax end)) as bs_amt_no_tax,
	sum((case when item.direction='-' then -1*item.amt else item.amt end)) as bs_amt
FROM
	(select * from csx_ods.source_cas_r_d_accounting_credential_item where sdt='19990101') item
	LEFT JOIN (select * from csx_ods.source_cas_r_d_accounting_credential_header where sdt='19990101')header on item.credential_no=header.credential_no
WHERE
	to_date(item.posting_time) >= '2020-03-01' 
	AND to_date(item.posting_time) < '2020-04-01'
	AND substr(item.move_type,1,3) in ('117');