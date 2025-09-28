-- ==================================================================================================================================================================================
-- 集体奖项_福利&BBC超额奖

	
	select 
		concat('20230501-','20230630') as sdt_s,
		performance_region_name,
		if(performance_province_name in ('上海松江','上海宝山'),'上海市',performance_province_name) as performance_province_name,
		-- performance_city_name,
		sum(sale_amt)as sale_amt,
		sum(profit_no_tax) as profit_no_tax, -- 不含税
		sum(if(business_type_code=2,sale_amt,0)) as fl_sale_amt,
		sum(if(business_type_code=6,sale_amt,0)) as bbc_sale_amt,
		sum(if(business_type_code=2,profit_no_tax,0)) as fl_profit_no_tax,
		sum(if(business_type_code=6,profit_no_tax,0)) as bbc_profit_no_tax
	from 
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt between '20230501' and '20230630' -- regexp_replace(to_date(date_sub(current_date(),1)),'-','')
		and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)	
		and business_type_code in (2,6) -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
		and goods_code not in ('8718','8708','8649')
		and performance_province_name !='平台-B'
	group by 
		performance_region_name,if(performance_province_name in ('上海松江','上海宝山'),'上海市',performance_province_name) -- ,performance_city_name

;


	select 
		substr(sdt,1,6) as `年月`,
		performance_region_name as `大区`,
		performance_province_name as `省区`,
		sum(sale_amt) as `销售额-含税`,
		sum(profit_no_tax) as `毛利额-不含税`, -- 不含税
		sum(if(business_type_code=2,sale_amt,0)) as `福利销售额-含税`,
		sum(if(business_type_code=6,sale_amt,0)) as `BBC销售额-含税`,
		sum(if(business_type_code=2,profit_no_tax,0)) as `福利毛利额-不含税`,
		sum(if(business_type_code=6,profit_no_tax,0)) as `BBC毛利额-不含税`
	from 
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt between '20220801' and '20221031' -- regexp_replace(to_date(date_sub(current_date(),1)),'-','')
		and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)	
		and business_type_code in (2,6) -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
		and goods_code not in ('8718','8708','8649')
		and performance_province_name !='平台-B'
	group by 
		substr(sdt,1,6),performance_region_name,performance_province_name

;

