-- 采购周期报价取数
SELECT
	product_code,
	product_name,
	supplier_code,
	supplier_name,
	location_code,
	location_name,
	purchase_group_code,
	purchase_group_name,
	purchase_org_code,
	purchase_org_name,
	unit,
	spec,
	purchase_price,
	cycle_start_time,
	DATE_FORMAT(cycle_start_time, '%Y-%m-%d')cycle_start_time,
	DATE_FORMAT(cycle_end_time, '%Y-%m-%d')cycle_end_time,
	cycle_price_status,
	create_time,
	create_by,
	update_time,
	update_by,
	create_by_id,
	update_by_id,
	cycle_price_source,
	inquiry_code,
	winning_bid_num
FROM
	csx_b2b_scm.scm_product_purchase_cycle_price x
where
	location_code = 'W0BK'
	-- and create_time >='2024-04-29 23:04:12.0'
    and cycle_price_status=0
	and cycle_start_time >=20240501