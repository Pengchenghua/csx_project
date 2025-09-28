-- 临时表
drop table csx_analyse_tmp.csx_analyse_tmp_yszx_order_buyer_remarks;
create table csx_analyse_tmp.csx_analyse_tmp_yszx_order_buyer_remarks
as
select
	a.customer_code,
	--a.customer_name,
	regexp_replace(a.customer_name,'\n|\t|\r|\,|\"|\\\\n','') as customer_name, 
	a.sub_customer_code,
	--a.sub_customer_name,
	regexp_replace(a.sub_customer_name,'\n|\t|\r|\,|\"|\\\\n','') as sub_customer_name, 
	a.goods_code,
	--a.goods_name,
	regexp_replace(a.goods_name,'\n|\t|\r|\,|\"|\\\\n','') as goods_name, 
	--a.buyer_remarks,
	regexp_replace(a.buyer_remarks,'\n|\t|\r|\,|\"|\\\\n','') as buyer_remarks, 
	row_number()over() as rn
from
	(
	select 
		customer_code,customer_name,sub_customer_code,sub_customer_name,goods_code,goods_name,buyer_remarks,inventory_dc_code
	from 
		csx_dwd.csx_dwd_csms_yszx_order_detail_di 
	where 
		sdt between '20230701' and '20230816' 
		and delivery_type_code in(0,1)
		and buyer_remarks !='' 
		and order_status not in ('CREATED', 'PAID', 'CONFIRMED') 
		and item_status <> 0
		and order_business_type in ('NORMAL','WELFARE')
	) a 
	join -- 过滤外部城市服务商订单
		(
		select
			shop_code
		from 
			csx_dim.csx_dim_shop
		where 
			sdt = 'current' 
			and purpose != '09'
		) e on e.shop_code=a.inventory_dc_code
;
select * from csx_analyse_tmp.csx_analyse_tmp_yszx_order_buyer_remarks where rn <=400000;
select * from csx_analyse_tmp.csx_analyse_tmp_yszx_order_buyer_remarks where rn >400000;