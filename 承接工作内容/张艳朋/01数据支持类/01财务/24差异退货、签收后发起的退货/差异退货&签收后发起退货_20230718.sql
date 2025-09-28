-- 签收后发起的退货 和 差异退货 到省区 拆到月份 按单量统计占比 金额 


-- 签收差异管理 销售退货管理
select
	b.performance_province_name,a.smonth,a.refund_order_type_name,count(distinct a.refund_code) as refund_code_cnt,sum(refund_scale_total_amt) as refund_scale_total_amt
from
	(
	select 
		substr(sdt,1,6) as smonth,refund_code,sale_order_code,customer_code,
		case when refund_order_type_code=0 then '签收差异管理' when refund_order_type_code=1 then '销售退货管理' end as refund_order_type_name,
		sum(cast(case when refund_scale_total_amt > 0 then refund_scale_total_amt
					when scale_status = 1 then round(real_return_qty*sale_price, 1)
					else round(real_return_qty*sale_price, 2)
					end AS decimal(20,6))) as refund_scale_total_amt
	from 
		csx_dwd.csx_dwd_oms_sale_refund_order_detail_di
	where
		sdt between '20221001' and '20230630'
		and order_status_code=30 -- 退货单状态: 10-差异待审(预留)  20-处理中  30-处理完成  -1-差异拒绝
		and order_business_type_code=1 -- 订单业务类型: 1-日配 2-福利 3-大宗贸易 4-内购
		and child_return_type_code=0 -- 子退货单类型 ：0-父退货单 1-子退货单逆向 2-子退货单正向
	group by 
		substr(sdt,1,6),refund_code,sale_order_code,customer_code,
		case when refund_order_type_code=0 then '签收差异管理' when refund_order_type_code=1 then '销售退货管理' end	
	) a 
	join
		(
		select 
			customer_code,customer_name,sales_user_number,sales_user_name,first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name,
			performance_province_name,performance_city_name
		from 
			csx_dim.csx_dim_crm_customer_info
		where 
			sdt = 'current'
			and channel_code in('1','7','9')
		) b on b.customer_code=a.customer_code
group by 
	b.performance_province_name,a.smonth,a.refund_order_type_name
;

-- 单据统计
		select 
			performance_province_name,substr(sdt,1,6) smonth,
			count(distinct order_code) as order_code_cnt,
			sum(sale_amt) as sale_amt
		from 
			csx_dws.csx_dws_sale_detail_di
		where 
			sdt between '20221001' and '20230630'
			and channel_code in('1','7','9')
			and business_type_code in (1) 
			and refund_order_flag=0
			and order_channel_code not in (4,6) -- 调价返利
		group by 
			performance_province_name,substr(sdt,1,6)
			
		