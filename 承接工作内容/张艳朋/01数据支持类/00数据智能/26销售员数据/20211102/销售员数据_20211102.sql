insert overwrite directory '/tmp/zhangyanpeng/20211018_01' row format delimited fields terminated by '\t'
		
		select
			b.sales_region_name,
			b.province_name,
			b.city_group_name,
			--c.sales_supervisor_work_no,
			--c.sales_supervisor_name,
			a.work_no,
			a.sales_name,
			a.smonth,
			sum(a.sales_value),
			count(distinct case when a.sales_value>=0 then a.customer_no else null end) as cnt
		from
			(
			select 
				customer_no,work_no,sales_name,substr(sdt,1,6) smonth,
				sum(sales_value) sales_value
			from 
				csx_dw.dws_sale_r_d_detail
			where 
				sdt>='20210101' and sdt<='20211101'
				and channel_code in('1','7','9')
				and business_type_code in ('1','2','3','5','6') 
			group by 
				customer_no,work_no,sales_name,substr(sdt,1,6)
			) a 
			left join 
				(
				select 
					customer_no,customer_name,work_no,sales_name,sales_region_name,province_name,city_group_name
				from 
					csx_dw.dws_crm_w_a_customer
				where 
					sdt = '20211101'
					and customer_no<>''	
				) b on b.customer_no=a.customer_no	
			--left join
			--	(
			--	select
			--		sales_id,sales_name,work_no,position,sales_supervisor_id,sales_supervisor_work_no,sales_supervisor_name
			--	from
			--		(
			--		select
			--			id as sales_id,
			--			name as sales_name,
			--			user_number as work_no,
			--			user_position as position,
			--			-- 主管
			--			first_value(case when leader_user_position = 'SALES_MANAGER' then leader_id end, true) over(partition by id order by distance) as sales_supervisor_id,
			--			first_value(case when leader_user_position = 'SALES_MANAGER' then leader_name end, true) over(partition by id order by distance) as sales_supervisor_name,
			--			first_value(case when leader_user_position = 'SALES_MANAGER' then leader_user_number end, true) over(partition by id order by distance) as sales_supervisor_work_no,
			--			row_number() over(partition by id order by distance desc) as rank
			--		from 
			--			csx_dw.dwd_uc_w_a_user_adjust
			--		where 
			--			sdt = 'current'
			--		) tmp 
			--	where 
			--		tmp.rank = 1
			--		and sales_supervisor_work_no is not null
			--	group by 
			--		sales_id,sales_name,work_no,position,sales_supervisor_id,sales_supervisor_work_no,sales_supervisor_name
			--	) c on c.work_no=a.work_no	
		group by 
			b.sales_region_name,
			b.province_name,
			b.city_group_name,
			--c.sales_supervisor_work_no,
			--c.sales_supervisor_name,
			a.work_no,
			a.sales_name,
			a.smonth;
