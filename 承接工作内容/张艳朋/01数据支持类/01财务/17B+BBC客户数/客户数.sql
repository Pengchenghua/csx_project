	

	select
		substr(sdt,1,4) as syear,
		count(distinct customer_code) as customer_cnt,
		count(distinct case when business_type_code=1 then customer_code else null end) as rp_customer_cnt
	from
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt>='20200101' and sdt<='20221231'
		and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
		-- and business_type_code !=4 -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.bbc 7.大宗一部 8.大宗二部 9.商超)
		-- and order_channel_code !=4 -- 订单来源渠道: 1-b端 2-m端 3-bbc 4-调价返利 -1-sap
	group by 
		substr(sdt,1,4)
	order by 
		1
;

	select
		substr(sdt,1,4) as syear,
		count(distinct customer_code) as customer_cnt,
		count(distinct case when business_type_code=1 then customer_code else null end) as rp_customer_cnt
	from
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt>='20200101' and sdt<='20221231'
		and substr(sdt,5,4) between '1001' and '1231'
		and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
		-- and business_type_code !=4 -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.bbc 7.大宗一部 8.大宗二部 9.商超)
		-- and order_channel_code !=4 -- 订单来源渠道: 1-b端 2-m端 3-bbc 4-调价返利 -1-sap
	group by 
		substr(sdt,1,4)
	order by 
		1
;

	select
		substr(sdt,1,4) as syear,
		count(distinct customer_code) as customer_cnt,
		count(distinct case when business_type_code=1 then customer_code else null end) as rp_customer_cnt
	from
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt>='20200101' and sdt<='20221231'
		and substr(sdt,5,4) between '0701' and '0930'
		and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
		-- and business_type_code !=4 -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.bbc 7.大宗一部 8.大宗二部 9.商超)
		-- and order_channel_code !=4 -- 订单来源渠道: 1-b端 2-m端 3-bbc 4-调价返利 -1-sap
	group by 
		substr(sdt,1,4)
	order by 
		1

