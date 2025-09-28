select
	a.customer_code,a.order_code,a.goods_code,a.sale_amt,a.profit,b.pricing_strategy,
	case when b.pricing_strategy=1 then '客户报价'
		when b.pricing_strategy=2 then '采购价上浮'
		when b.pricing_strategy=3 then '指定价格'
	end as pricing_strategy_name
from
	(
	select
		customer_code,order_code,goods_code,sale_amt,profit
	from
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt>='20230901' and sdt<='20230905'
		and channel_code in('1','7','9')
	) a 
	left join
		(
		select
			customer_code,order_code,goods_code,pricing_strategy -- 定价策略:1-客户报价，2-采购价上浮，3-指定价格
		from
			csx_dwd.csx_dwd_csms_yszx_additional_order_detail_df
		where
			order_code!=''
			and goods_code!=''
			and additional_order_status !=0 -- 加单状态:1-采购待处理、2-待录单、3-已录单、0-已取消
		group by 
			customer_code,order_code,goods_code,pricing_strategy
		) b on b.order_code=a.order_code and b.goods_code=a.goods_code and b.customer_code=a.customer_code