-- ====================================================================================================================================================
-- 4月日配业绩

select
	a.region_code,
	c.region_name,
	a.province_code,
	c.province_name,
	a.city_group_code,
	c.city_group_name,
	a.profit_rate_type,
	a.customer_cnt,
	a.sales_value,
	a.profit
from
	(
	select
		region_code,province_code,city_group_code,
		case when profit_rate<0 then '负毛利'
			when profit_rate>=0 and profit_rate<=0.05 then '0%-5%'
			when profit_rate>0.05 and profit_rate<=0.1 then '5%-10%'
			when profit_rate>0.1 and profit_rate<=0.2 then '10%-20%'
			when profit_rate>0.2 then '20%以上'
			else '其他'
		end as profit_rate_type,
		count(distinct customer_no) as customer_cnt,
		sum(sales_value) as sales_value,
		sum(profit) as profit
	from
		(
		select
			region_code,province_code,city_group_code,customer_no,
			coalesce(sum(profit)/abs(sum(sales_value)),0) as profit_rate,
			sum(sales_value) as sales_value,
			sum(profit) as profit
		from 
			csx_dw.dws_sale_r_d_detail
		where 
			sdt >= '20210401' 
			and sdt <= '20210430'
			-- and channel_code in ('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
			and business_type_code in ('1') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
		group by 
			region_code,province_code,city_group_code,customer_no
		) as tmp
	group by 
		region_code,province_code,city_group_code,
		case when profit_rate<0 then '负毛利'
			when profit_rate>=0 and profit_rate<=0.05 then '0%-5%'
			when profit_rate>0.05 and profit_rate<=0.1 then '5%-10%'
			when profit_rate>0.1 and profit_rate<=0.2 then '10%-20%'
			when profit_rate>0.2 then '20%以上'
			else '其他'
		end
	) as a
	left join 
		(
		select 
			province_code,province_name,region_code,region_name,city_group_code,city_group_name
		from 
			csx_dw.dws_sale_w_a_area_belong
		group by 
			province_code,province_name,region_code,region_name,city_group_code,city_group_name
		) c on c.city_group_code=a.city_group_code	
		
		

	