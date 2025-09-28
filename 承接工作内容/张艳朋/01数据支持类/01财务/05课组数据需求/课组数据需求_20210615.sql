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
		
		
		
-- ====================================================================================================================================================
		
insert overwrite directory '/tmp/zhangyanpeng/20210615_01' row format delimited fields terminated by '\t'

select 
	c.department_id,produce_month,province_name,a.order_code,a.goods_code,
	worker_cost, -- 人工
	machine_cost, -- 机时
	support_material_cost -- 辅材
from 
	(
	select 
		produce_month,province_name,order_code,goods_code
	from 
		(
		select 
			produce_month,province_name,order_code,goods_code,
			row_number() over(partition by goods_code,produce_month,province_name order by order_code asc) as ranks
		from 
			csx_dw.dws_mms_r_a_factory_order
		where 
			sdt >= '20210301' and sdt <= '20210531'
		) a 
	where 
		ranks = 1
	) a 
	left join 
		(
		select 
			order_code,product_code,
			worker_cost/reckon_factor  as worker_cost,
			machine_cost/reckon_factor as machine_cost,
			support_material_cost/reckon_factor as support_material_cost
		from 
			csx_dw.dws_mms_w_a_setting_order_craft   --工厂工单工艺路线
		where 
			sdt = 'current'
		) b on a.order_code = b.order_code and a.goods_code = b.product_code
	left join
		(
		select
			goods_id,goods_name,department_id,department_name
		from
			csx_dw.dws_basic_w_a_csx_product_m
		where
			sdt='current'
		group by 
			goods_id,goods_name,department_id,department_name
		) c on c.goods_id=a.goods_code
		
		

	