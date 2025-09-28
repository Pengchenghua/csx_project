
--select * from csx_dws.csx_dws_scm_order_detail_di where sdt between '20220821' and '20230821'
	
drop table csx_analyse_tmp.csx_analyse_tmp_purchase_order_shidiao_00;
create table csx_analyse_tmp.csx_analyse_tmp_purchase_order_shidiao_00
as	
select
	performance_province_name,performance_city_name,receive_dc_code,receive_dc_name,goods_code,goods_name,unit_name,bar_code,
	sum(receive_qty) as receive_qty,sum(receive_amt) as receive_amt,sum(receive_amt)/sum(receive_qty) as avg_amt
from	
	csx_analyse.csx_analyse_scm_purchase_order_flow_di
where
	sdt between '20220821' and '20230821'
	and order_goods_status='4' --订单商品状态 状态(1-已创建,2-已发货,3-入库中,4-已完成,5-已取消)
	and source_type_name in ('采购导入','智能补货','日采补货','手工创建')
	and receive_amt>0.2
	and receive_dc_code in('W0R9','W0A5','W0N0','W0W7','W0X6','W0T1','W0N1','W0AS','W0A8','W0F4','W0L3','WB56','W0AH','W0G9','WA96','WB67','W0K6','W0BK','W0A7','W0X2','W0Z9','W0A6','W0Q2','W0A3','W0Q9','W0P8','W0A2','W0L4','W0BR','W0BH')
group by 
	performance_province_name,performance_city_name,receive_dc_code,receive_dc_name,goods_code,goods_name,unit_name,bar_code
;
select * from csx_analyse_tmp.csx_analyse_tmp_purchase_order_shidiao_00
	
	

