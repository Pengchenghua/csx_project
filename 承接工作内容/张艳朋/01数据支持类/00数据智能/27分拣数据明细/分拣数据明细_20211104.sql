SELECT
	replace(a.plan_date, '-', '') as sdt,
	a.send_location_code,
	a.order_code,
	a.origin_order_code,
	a.shipped_type,
	b.product_code,
	b.product_name,
	b.plan_qty,
	b.shipped_qty,
	b.unit,
	b.sale_unit,
	b.split_group_code,
	b.split_group_name,
	b.price
FROM csx_b2b_wms.wms_shipped_order_header a
LEFT JOIN csx_b2b_wms.wms_shipped_order_item b ON a.order_code = b.order_code
WHERE a.plan_date = '${sdate}'
and a.shipped_type in ('S01','S18')
${if(len(dc)==0,"","AND a.send_location_code in( '"+dc+"') ")}
${if(len(split_group)==0,"","AND b.split_group_name in( '"+split_group+"') ")}

-- and (b.split_group_code<>'' and b.split_group_code is not null)
-- and b.split_group_code IN ('OG204090002')
-- AND a.send_location_code = 'W0A7'
ORDER BY a.send_location_code,a.plan_date,a.order_code DESC;