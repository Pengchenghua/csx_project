--============================================================================================================================================================
-- 上周有日配业务的客户在本周下单情况

insert overwrite directory '/tmp/zhangyanpeng/20210421_linshi_1' row format delimited fields terminated by '\t' 

select	
	c.region_name as `大区`,
	c.province_name as `省份`,
	c.city_group_name as `城市组`,
	a.customer_no as `客户编码`,
	b.customer_name as `客户名称`,
	b.work_no as `销售员工号`,
	b.sales_name as `销售员名称`,
	b.first_category_name as `一级分类名称`,
	b.second_category_name as `二级分类名称`,
	b.third_category_name as `三级分类名称`,
	b.sign_date as `签约日期`,
	f.max_sdt as `最后一次下日配单日期`,
	a.s_sdt as `3.1至4.30日配业务下单次数`,
	a.sales_value as `3.1至4.30日配业务销售额`,
	a.profit as `3.1至4.30日配业务定价毛利额`,
	a.profit_rate as `3.1至4.30日配业务定价毛利率`,
	case when d.customer_no is not null then '是' else '否' end as `5.1至5.7是否下日配业务`,
	coalesce(d.sales_value,0) as `5.1至5.7日配业务销售额`,
	coalesce(d.profit,0) as `5.1至5.7日配业务定价毛利额`,
	coalesce(d.profit_rate,0) as `5.1至5.7日配业务定价毛利率`,
	case when e.customer_no is not null then '是' else '否' end as `5.1至5.7是否下其他业务`,
	coalesce(e.sales_value,0) as `5.1至5.7其他业务销售额`,
	coalesce(e.profit,0) as `5.1至5.7其他业务定价毛利额`,
	coalesce(e.profit_rate,0) as `5.1至5.7其他业务定价毛利率`
from
	(
	select
		region_code,province_code,city_group_code,customer_no,
		sum(sales_value) as sales_value,
		sum(profit) as profit,
		sum(profit)/abs(sum(sales_value)) as profit_rate,
		count(distinct sdt) as s_sdt
	from 
		csx_dw.dws_sale_r_d_detail
	where 
		sdt >= '20210301' 
		and sdt <= '20210430'
		and channel_code in ('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
		and business_type_code in ('1') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
		and sales_type !='fanli'
	group by 
		region_code,province_code,city_group_code,customer_no
	) as a
	left join 
		(
		select 
			customer_no,customer_name,work_no,sales_name,first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name,
			regexp_replace(split_part(sign_time,' ',1),'-','') as sign_date
		from 
			csx_dw.dws_crm_w_a_customer
		where 
			sdt='current'
		group by 
			customer_no,customer_name,work_no,sales_name,first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name,
			regexp_replace(split_part(sign_time,' ',1),'-','')
		) b on a.customer_no=b.customer_no
	left join 
		(
		select 
			city_group_code,city_group_name,province_code,province_name,region_code,region_name
		from 
			csx_dw.dws_sale_w_a_area_belong
		group by 
			city_group_code,city_group_name,province_code,province_name,region_code,region_name
		) c on c.city_group_code=a.city_group_code	
	left join -- 本周日配业务客户及销售额
		(
		select 
			customer_no,
			sum(sales_value) as sales_value,
			sum(profit) as profit,
			sum(profit)/abs(sum(sales_value)) as profit_rate
		from 
			csx_dw.dws_sale_r_d_detail
		where 
			sdt>='20210501'
			and sdt<='20210507'
			and channel_code in ('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
			and business_type_code in ('1') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
			and sales_type !='fanli'
		group by 
			customer_no
		) d on d.customer_no=a.customer_no
	left join -- 本周非日配业务客户及销售额
		(
		select 
			customer_no,
			sum(sales_value) as sales_value,
			sum(profit) as profit,
			sum(profit)/abs(sum(sales_value)) as profit_rate
		from 
			csx_dw.dws_sale_r_d_detail
		where 
			sdt>='20210501'
			and sdt<='20210507'
			and channel_code in ('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
			and business_type_code not in ('1') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
			and sales_type !='fanli'
		group by 
			customer_no
		) e on e.customer_no=a.customer_no
	left join 
		(
		select 
			customer_no,
			max(sdt) as max_sdt
		from 
			csx_dw.dws_sale_r_d_detail
		where 
			sdt>='20210301'
			and sdt<='20210507'
			and channel_code in ('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
			and business_type_code in ('1') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
			and sales_type !='fanli'
		group by 
			customer_no
		) f on f.customer_no=a.customer_no
where
	c.region_name='华西大区'
	