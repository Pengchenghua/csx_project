-- 客户需求商品-黄钊
select * from b2b_mall_prod.product_rpc_search_record where create_time>='2023-06-01 00:00:00'


select
	a.sap_sub_cus_code,
	a.search_key,
	a.product_code,
	b.product_name ,
	a.create_by,
	a.create_time,
	a.update_by,
	a.update_time,
	a.sap_cus_code,
	a.unit,
	a.remarks,
	a.inventory_dc_code
from
	b2b_mall_prod.product_rpc_search_record a
left join 
b2b_mall_prod.yszx_base_product b on
	a.product_code = b.product_code
where
	a.create_time >= '2023-06-01 00:00:00'
