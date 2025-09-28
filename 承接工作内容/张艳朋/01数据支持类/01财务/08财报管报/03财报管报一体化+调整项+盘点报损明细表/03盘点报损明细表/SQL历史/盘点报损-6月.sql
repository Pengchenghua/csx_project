--无需等关账，月初同步两张表csx_ods.source_cas_r_d_accounting_credential_header，csx_ods.source_cas_r_d_accounting_credential_item


-- 本月第一天，上月第一天，上上月第一天
set i_sdate_11 =trunc(date_sub(current_date,0),'MM');
set i_sdate_12 =add_months(trunc(date_sub(current_date,0),'MM'),-1);
set i_sdate_13 =add_months(trunc(date_sub(current_date,0),'MM'),-2);

-- 本月第一天，上月第一天，上上月第一天
set i_sdate_21 =regexp_replace(trunc(date_sub(current_date,1),'MM'),'-','');
set i_sdate_22 =regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-1),'-','');
set i_sdate_23 =regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-2),'-','');

--select ${hiveconf:i_sdate_11},${hiveconf:i_sdate_12},${hiveconf:i_sdate_13},${hiveconf:i_sdate_21},${hiveconf:i_sdate_22},${hiveconf:i_sdate_23};


/*业务盘点*/
--盘点未过账
insert overwrite directory '/tmp/raoyanhua/pandian01' row format delimited fields terminated by '\t'
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
	to_date(item.posting_time) >= ${hiveconf:i_sdate_12} 
	AND to_date(item.posting_time) < ${hiveconf:i_sdate_11}
	AND substr(move_type,1,3) in ('110','111') 
  --AND substr(reservoir_area_code,1,2) <>'PD';
	AND substr(reservoir_area_code,1,2)='PD';

/*财务盘点*/	
--盘点过账	
insert overwrite directory '/tmp/raoyanhua/pandian02' row format delimited fields terminated by '\t'
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
	--posting_time >= '2020-05-01 00:00:00' 
	--AND posting_time < '2020-06-01 00:00:00'
	posting_time >= ${hiveconf:i_sdate_12} 
	AND posting_time < ${hiveconf:i_sdate_11}	
	AND substr(move_type,1,3) in ('115','116') 
	AND amt_no_tax<>'0';

/*
insert overwrite directory '/tmp/raoyanhua/pandian02' row format delimited fields terminated by '\t'
SELECT
	b.province_name,
	a.create_time as `创建时间`, 
	a.posting_time as `记账日期`,
	a.company_code as `公司代码`,
	a.company_name as `公司名称`,
	a.location_code as `DC编码`,
	a.location_name as `DC名称`,
	a.reservoir_area_code as `库区编码`,
	a.reservoir_area_name as `库区名称`,
	a.purchase_group_code as `采购组编码`,
	a.purchase_group_name as `采购组名称`,
	a.product_code as `商品编码`,
	a.product_name as `商品名称`,
	a.move_type as `移动类型编码`,
	a.move_name as `移动类型名称`,
	(case when a.direction='-' then -1*a.qty else a.qty end) as `盘点数量`,
	(case when a.direction='-' then -1*a.amt_no_tax else a.amt_no_tax end) as `盘点不含税金额`,
	(case when a.direction='-' then -1*a.amt else a.amt end) as `盘点含税金额`
FROM
	(select * from csx_ods.source_cas_r_d_accounting_credential_item where sdt='19990101'  
	and posting_time >= ${hiveconf:i_sdate_12} 
	AND posting_time < ${hiveconf:i_sdate_11}	
	AND substr(move_type,1,3) in ('115','116') 
	AND amt_no_tax<>'0'	) a
left join 
(select shop_id,shop_name,province_name
from csx_dw.shop_m where sdt = 'current') b on b.shop_id=if(a.location_code like '9%',concat('E',substr(a.location_code,2,3)),a.location_code);
*/

--报损
insert overwrite directory '/tmp/raoyanhua/pandian03' row format delimited fields terminated by '\t'
 SELECT
	--item.wms_order_no,
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
	d.cost_center_code,	--成本中心编码
	d.cost_center_name,	--成本中心名称	
	--c.cost_center_code,		--成本中心编码
	--c.cost_center_name,		--成本中心名称
	c.reservoir_area_prop,		--库区属性	
	(case when item.direction='-' then -1*item.qty else item.qty end) as bs_qty,
	(case when item.direction='-' then -1*item.amt_no_tax else item.amt_no_tax end) as bs_amt_no_tax,
	(case when item.direction='-' then -1*item.amt else item.amt end) as bs_amt
from
	(select * from csx_ods.source_cas_r_d_accounting_credential_item 
	where sdt='19990101'
	and to_date(posting_time) >= ${hiveconf:i_sdate_12}
	and to_date(posting_time) < ${hiveconf:i_sdate_11}
	and substr(move_type,1,3) in ('117')
	) item
left join (select * from csx_ods.source_cas_r_d_accounting_credential_header where sdt='19990101')header on item.credential_no=header.credential_no
left join (select * from csx_ods.source_sync_r_d_data_sync_broken_item 
			where sdt = '19990101'
			--and (( wms_biz_type <>'64' and reservoir_area_prop = 'C' and ( purchase_group_code like 'H%' or purchase_group_code like 'U%' ) ) 
			--	or wms_biz_type = '64' )
			and posting_time >= ${hiveconf:i_sdate_12} 
			and posting_time < ${hiveconf:i_sdate_11}
			) c on c.credential_no=item.credential_no and c.product_code=item.product_code
left join 
(select t.*,t1.cost_center_name
from
(select order_code,frmloss_type_code,frmloss_type_name,cost_center_code from csx_ods.source_wms_r_d_frmloss_order_header_v1 where sdt='19990101')t
left join
(select cost_center_code,cost_center_name
  from
  (select cost_center_code,cost_center_name,row_number() over(partition by cost_center_code order by update_time desc) as rank 
    from csx_ods.source_basic_w_a_md_cost_center where sdt=regexp_replace(date_sub(current_date,1),'-','')
  )a
  where rank=1
)t1
on t.cost_center_code=t1.cost_center_code
) d on d.order_code=item.wms_order_no			
;

--select * 
--from csx_ods.source_cas_r_d_accounting_credential_item 
--	where sdt='19990101'
--	and to_date(posting_time) >= ${hiveconf:i_sdate_12}
--	and to_date(posting_time) < ${hiveconf:i_sdate_11}
--	and substr(move_type,1,3) in ('117')
--	and credential_no='PZ20200615001449'
--	and wms_order_no='BS200615000010'
--	and product_code='1163327';




----------------------------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------------	
加两个字段库区属性和成本中心