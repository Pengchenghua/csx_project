--====================================================================================================================================
--断约
insert overwrite directory '/tmp/zhangyanpeng/20210121_break_contract_2' row format delimited fields terminated by '\t'
select
	a.province_name,
	a.customer_no,
	c.customer_name,
	c.first_supervisor_work_no,
	c.first_supervisor_name,
	c.work_no,
	c.sales_name,
	a.sales_value,
	a.profit,
	case when b.customer_no is null then '断约' else '未断约' end as is_break_appointment
from 
	(
	select 
		province_name,customer_no,
		sum(sales_value)as sales_value,
		sum(profit)as profit
	from 
		csx_dw.dws_sale_r_d_detail 
	where 
		sdt between '20200701' and '20200930'
		and business_type_code in ('1') -- 业务类型编码(1.日配单 2.福利单 3.批发内购 4.城市服务商 5.省区大宗 6.BBC)
		and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
		and province_name='福建省'
	group by 
		province_name,customer_no
	)a 
	left join   
		(
		select
			a.province_name,
			a.customer_no,
			c.customer_name,
			a.sales_value,
			a.profit
		from 
			(
			select 
				province_name,customer_no,
				sum(sales_value)as sales_value,
				sum(profit)as profit
			from 
				csx_dw.dws_sale_r_d_detail 
			where 
				sdt between '20201001' and '20201231'
				and business_type_code in ('1') -- 业务类型编码(1.日配单 2.福利单 3.批发内购 4.城市服务商 5.省区大宗 6.BBC)
				and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
				and province_name='福建省'
			group by 
				province_name,customer_no
			)a 
			left join
				(
				select 
					customer_no,customer_name,attribute
				from 
					csx_dw.dws_crm_w_a_customer_m_v1
				where 
					sdt = 'current'
					and customer_no<>''	
				group by
					customer_no,customer_name,attribute
				) c on c.customer_no = a.customer_no
			left join
				(
				select 
					t1.province_name,t1.customer_no,'未履约且仅有退单' as remark
				from
					(
					select 
						province_name,customer_no
					from 
						csx_dw.dws_sale_r_d_detail 
					where 
						sdt between '20201001' and '20201231'
						and business_type_code in ('1') -- 业务类型编码(1.日配单 2.福利单 3.批发内购 4.城市服务商 5.省区大宗 6.BBC)
						and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
						and province_name='福建省'
						and return_flag='X'
					group by 
						province_name,customer_no
					) as t1
					left join
						(
						select 
							province_name,customer_no
						from 
							csx_dw.dws_sale_r_d_detail 
						where 
							sdt between '20201001' and '20201231'
							and business_type_code in ('1') -- 业务类型编码(1.日配单 2.福利单 3.批发内购 4.城市服务商 5.省区大宗 6.BBC)
							and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
							and province_name='福建省'
							and return_flag != 'X'
						group by 
							province_name,customer_no
					) as t2 on t2.province_name=t1.province_name and t2.customer_no=t1.customer_no
				where 
					t2.customer_no is null
				) d on d.province_name=a.province_name and d.customer_no=a.customer_no
		where
			a.sales_value !=0
			and d.customer_no is null
		) b on b.province_name=a.province_name and b.customer_no=a.customer_no
	left join
		(
		select 
			customer_no,customer_name,attribute,first_supervisor_work_no,first_supervisor_name,work_no,sales_name
		from 
			csx_dw.dws_crm_w_a_customer
		where 
			sdt = 'current'
			and customer_no<>''	
		group by
			customer_no,customer_name,attribute,first_supervisor_work_no,first_supervisor_name,work_no,sales_name
		) c on c.customer_no = a.customer_no
	left join
		(
		select 
			t1.province_name,t1.customer_no,'未履约且仅有退单' as remark
		from
			(
			select 
				province_name,customer_no
			from 
				csx_dw.dws_sale_r_d_detail 
			where 
				sdt between '20200701' and '20200930'
				and business_type_code in ('1') -- 业务类型编码(1.日配单 2.福利单 3.批发内购 4.城市服务商 5.省区大宗 6.BBC)
				and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
				and province_name='福建省'
				and return_flag='X'
			group by 
				province_name,customer_no
			) as t1
			left join
				(
				select 
					province_name,customer_no
				from 
					csx_dw.dws_sale_r_d_detail 
				where 
					sdt between '20200701' and '20200930'
					and business_type_code in ('1') -- 业务类型编码(1.日配单 2.福利单 3.批发内购 4.城市服务商 5.省区大宗 6.BBC)
					and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
					and province_name='福建省'
					and return_flag != 'X'
				group by 
					province_name,customer_no
			) as t2 on t2.province_name=t1.province_name and t2.customer_no=t1.customer_no
		where 
			t2.customer_no is null
		) d on d.province_name=a.province_name and d.customer_no=a.customer_no
where
	a.sales_value !=0
	and d.customer_no is null
		
		
--====================================================================================================================================
--履约
insert overwrite directory '/tmp/zhangyanpeng/20210121_break_contract_3' row format delimited fields terminated by '\t'
select
	a.province_name,
	a.customer_no,
	c.customer_name,
	c.first_supervisor_work_no,
	c.first_supervisor_name,
	c.work_no,
	c.sales_name,
	a.sales_value,
	a.profit
from 
	(
	select 
		province_name,customer_no,
		sum(sales_value)as sales_value,
		sum(profit)as profit
	from 
		csx_dw.dws_sale_r_d_detail 
	where 
		sdt between '20201001' and '20201231'
		and business_type_code in ('1') -- 业务类型编码(1.日配单 2.福利单 3.批发内购 4.城市服务商 5.省区大宗 6.BBC)
		and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
		and province_name='福建省'
	group by 
		province_name,customer_no
	)a 
	left join
		(
		select 
			customer_no,customer_name,attribute,first_supervisor_work_no,first_supervisor_name,work_no,sales_name
		from 
			csx_dw.dws_crm_w_a_customer
		where 
			sdt = 'current'
			and customer_no<>''	
		group by
			customer_no,customer_name,attribute,first_supervisor_work_no,first_supervisor_name,work_no,sales_name
		) c on c.customer_no = a.customer_no
	left join
		(
		select 
			t1.province_name,t1.customer_no,'未履约且仅有退单' as remark
		from
			(
			select 
				province_name,customer_no
			from 
				csx_dw.dws_sale_r_d_detail 
			where 
				sdt between '20201001' and '20201231'
				and business_type_code in ('1') -- 业务类型编码(1.日配单 2.福利单 3.批发内购 4.城市服务商 5.省区大宗 6.BBC)
				and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
				and province_name='福建省'
				and return_flag='X'
			group by 
				province_name,customer_no
			) as t1
			left join
				(
				select 
					province_name,customer_no
				from 
					csx_dw.dws_sale_r_d_detail 
				where 
					sdt between '20201001' and '20201231'
					and business_type_code in ('1') -- 业务类型编码(1.日配单 2.福利单 3.批发内购 4.城市服务商 5.省区大宗 6.BBC)
					and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
					and province_name='福建省'
					and return_flag != 'X'
				group by 
					province_name,customer_no
			) as t2 on t2.province_name=t1.province_name and t2.customer_no=t1.customer_no
		where 
			t2.customer_no is null
		) d on d.province_name=a.province_name and d.customer_no=a.customer_no
where
	a.sales_value !=0
	and d.customer_no is null

