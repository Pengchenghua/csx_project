-- ====================================================================================================================================================
-- 基础数据

--SET hive.execution.engine=spark;

insert overwrite directory '/tmp/zhangyanpeng/20211022_01' row format delimited fields terminated by '\t' 

select	
	c.region_name,
	c.province_name,
	a.business_type_name,
	a.customer_no,
	b.customer_name,
	b.second_category_name,
	a.s_sdt,
	substr(e.first_order_date,1,6) as first_order_date,
	substr(d.min_sdt,1,6) as min_sdt,
	b.cooperation_mode_name,
	a.excluding_tax_sales,
	a.excluding_tax_profit,
	a.profit_rate
from
	(
	select
		substr(sdt,1,6) as s_sdt,region_code,province_code,customer_no,business_type_name,
		sum(excluding_tax_sales) as excluding_tax_sales,
		sum(excluding_tax_profit) as excluding_tax_profit,
		sum(excluding_tax_profit)/abs(sum(excluding_tax_sales)) as profit_rate
	from 
		csx_dw.dws_sale_r_d_detail
	where 
		sdt >= '20190101' 
		and sdt <= '20220331'
		and channel_code in ('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
		and business_type_code not in ('7','8','9') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
	group by 
		substr(sdt,1,6),region_code,province_code,customer_no,business_type_name
	) as a
	left join 
		(
		select 
			customer_no,customer_name,work_no,sales_name,first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name,
			cooperation_mode_name
		from 
			csx_dw.dws_crm_w_a_customer
		where 
			sdt='current'
		group by 
			customer_no,customer_name,work_no,sales_name,first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name,
			cooperation_mode_name
		) b on a.customer_no=b.customer_no
	left join 
		(
		select 
			province_code,province_name,region_code,region_name
		from 
			csx_dw.dws_sale_w_a_area_belong
		group by 
			province_code,province_name,region_code,region_name
		) c on c.province_code=a.province_code	
	left join 
		(
		select 
			customer_no,
			business_type_name,
			min(sdt) as min_sdt
		from 
			csx_dw.dws_sale_r_d_detail
		where 
			sdt>='20190101'
			and sdt<='20220331'
			and channel_code in ('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
			and business_type_code not in ('7','8','9') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
		group by 
			customer_no,business_type_name
		) d on d.customer_no=a.customer_no and d.business_type_name=a.business_type_name
	left join 
		(
		select 
			customer_no,customer_name,first_order_date
		from 
			csx_dw.dws_crm_w_a_customer_active
		where 
			sdt='current'
		group by 
			customer_no,customer_name,first_order_date
		) e on e.customer_no=a.customer_no	
		


	