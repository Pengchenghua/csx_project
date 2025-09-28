drop table if exists csx_analyse_tmp.csx_analyse_tmp_beijing_purchase_order;
create table csx_analyse_tmp.csx_analyse_tmp_beijing_purchase_order
as
select 
	purchase_order_code,super_class_name,source_type_name,business_type_name,goods_code,goods_name,unit_name,
	case when order_goods_status='1' then '已创建'
		when order_goods_status='2' then '已发货'
		when order_goods_status='3' then '入库中'
		when order_goods_status='4' then '已完成'
		when order_goods_status='5' then '已取消'
	end as order_goods_status_name,
	order_qty,if(order_price2 !=0,order_price2,order_price1) as order_price,order_qty*if(order_price2 !=0,order_price2,order_price1) as order_price_total,
	shipped_qty,receive_qty,shipped_amt,receive_amt,purchase_group_code,purchase_group_name,category_middle_code,category_middle_name,
	supplier_code,supplier_name,send_dc_code,send_dc_name,receive_dc_code,receive_dc_name,order_create_date,sdt
from 
	csx_analyse.csx_analyse_scm_purchase_order_flow_di
where 
	sdt>='20220101' and sdt<='20221231'
	and dc_code in('W0A3','WB04','W048')
	and order_goods_status='4'
;
select * from csx_analyse_tmp.csx_analyse_tmp_beijing_purchase_order

	