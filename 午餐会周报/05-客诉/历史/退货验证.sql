
select a.*,h.source_type,h.first_level_reason_code,h.goods_code
from 
	( 
	select
		sdt,customer_code,order_code,original_order_code,goods_code,sale_amt,profit,performance_region_name,performance_province_name,performance_city_name,refund_order_flag,
		if((order_channel_code in ('4','5','6') or refund_order_flag=1),original_order_code,order_code) as order_code_new
	from
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt>='${sdt_3m}' and sdt<='${sdt_tdm}' -- 最近三个月
		and channel_code in('1','7','9')
		and business_type_code not in(4,6) -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.bbc 7.大宗一部 8.大宗二部 9.商超)
		and order_channel_code =1 -- 1-b端 2-m端 3-bbc 4-客户返利管理 5-价格补救 6-客户调价管理 -1-sap
		-- and refund_order_flag= 0  剔除退货，后改不剔
		and performance_province_name != '平台-B'
		
	) a 
left join 
    (select
    *
    from csx_dwd.csx_dwd_oms_sale_refund_order_detail_di
    where sdt>='20240101'
    and child_return_type_code in(1,2)
    and parent_refund_code<>''
    )h on a.original_order_code=h.sale_order_code and a.goods_code=h.goods_code
    left join 
    (
    	select  
    		bloc_code,     --  集团编码
    		bloc_name,     --  集团名称
    		parent_id,customer_id,
    		customer_code,
    		customer_name,     --  客户名称
    		first_category_name,     --  一级客户分类名称
    		second_category_name,     --  二级客户分类名称
    		performance_region_name,     --  销售大区名称(业绩划分)
    		performance_province_name,     --  销售归属省区名称
    		performance_city_name     --  城市组名称(业绩划分)
    	from csx_dim.csx_dim_crm_customer_info
    	where sdt='current'
    	and customer_type_code=4
    )b on a.customer_code=b.customer_code
    where b.performance_province_name ='陕西省' ;
	
	
	
	
	
select a.*,b.performance_province_name
from 
(select
*
from csx_dwd.csx_dwd_oms_sale_refund_order_detail_di
where sdt>='20240101'
and child_return_type_code in(1,2)
and parent_refund_code<>''
)a
left join 
(
	select  
		bloc_code,     --  集团编码
		bloc_name,     --  集团名称
		parent_id,customer_id,
		customer_code,
		customer_name,     --  客户名称
		first_category_name,     --  一级客户分类名称
		second_category_name,     --  二级客户分类名称
		performance_region_name,     --  销售大区名称(业绩划分)
		performance_province_name,     --  销售归属省区名称
		performance_city_name     --  城市组名称(业绩划分)
	from csx_dim.csx_dim_crm_customer_info
	where sdt='current'
	and customer_type_code=4
)b on a.customer_code=b.customer_code
where b.performance_province_name ='陕西省'	