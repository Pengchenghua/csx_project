--客服经理绩效数据
-- -- -- -- -- -- -  服务管家-- -客服经理-- -- 高级客服经理
drop table csx_analyse_tmp.customer_sale_service_manager_leader01;
create  table csx_analyse_tmp.customer_sale_service_manager_leader01
as 
select
	sdt,customer_no,customer_name,region_name,province_name,city_group_name,
	rp_service_user_id_new,
	rp_service_user_work_no_new,
	rp_service_user_name_new,
	rp_leader_id,
	rp_leader_user_number,
	rp_leader_name,
	rp_city_leader_id,
	b.user_number as rp_leader_city_user_number,
	b.name as rp_leader_city_name,
	fl_service_user_id_new,
	fl_service_user_work_no_new,
	fl_service_user_name_new,
	fl_leader_id,
	fl_leader_user_number,
	fl_leader_name,
	fl_city_leader_id,
	c.user_number as fl_leader_city_user_number,
	c.name as fl_leader_city_name,
	bbc_service_user_id_new,
	bbc_service_user_work_no_new,
	bbc_service_user_name_new,
	bbc_leader_id,
	bbc_leader_user_number,
	bbc_leader_name,
	bbc_city_leader_id,
	d.user_number as bbc_leader_city_user_number,
	d.name as bbc_leader_city_name
from 
	(
      select
		sdt,customer_no,customer_name,region_name,province_name,city_group_name,
		rp_service_user_id_new,
		rp_service_user_work_no_new,
		rp_service_user_name_new,
		rp_leader_id,
		b.user_number as rp_leader_user_number,
		b.name as rp_leader_name,
		b.leader_id as rp_city_leader_id,      			
		fl_service_user_id_new,
		fl_service_user_work_no_new,
		fl_service_user_name_new,
		fl_leader_id,
		c.user_number as fl_leader_user_number,
		c.name as fl_leader_name,
		c.leader_id as fl_city_leader_id,      			
		bbc_service_user_id_new,
		bbc_service_user_work_no_new,
		bbc_service_user_name_new,
		bbc_leader_id,
		d.user_number as bbc_leader_user_number,
		d.name as bbc_leader_name,
		d.leader_id as bbc_city_leader_id    
      from 
		(
		select
      		sdt,
			customer_no,
			customer_name,
			region_name,
			province_name,
			city_group_name,
      		rp_service_user_id_new,
      		rp_service_user_work_no_new,
      		rp_service_user_name_new,
      		b.leader_id as rp_leader_id,
      		fl_service_user_id_new,
      		fl_service_user_work_no_new,
      		fl_service_user_name_new,
      		c.leader_id as fl_leader_id,
      		bbc_service_user_id_new,
      		bbc_service_user_work_no_new,
      		bbc_service_user_name_new,
      		d.leader_id as bbc_leader_id
		from 
			(select distinct 
      			sdt,customer_no,
				customer_name,
				region_name,
				province_name,
				city_group_name,
				sales_name,         -- 系统维护
				user_position,
				sales_name_new,
				user_position_new,
      			rp_service_user_id_new,
      			rp_service_user_work_no_new,
      			rp_service_user_name_new,
      			fl_service_user_id_new,
      			fl_service_user_work_no_new,
      			fl_service_user_name_new,
      			bbc_service_user_id_new,
      			bbc_service_user_work_no_new,
      			bbc_service_user_name_new
      		from
      		  	csx_analyse.csx_analyse_report_crm_customer_sale_service_manager_info_df
      		where
      			sdt in ('20250709')
			)a
			left join
			(
			select id,leader_id
			from csx_ods.csx_ods_csx_b2b_ucenter_user_df
			where sdt=regexp_replace(date_sub(current_date(),1),'-','')
			) b on split(a.rp_service_user_id_new,'、')[0]=b.id    
			left join 
			(
			select id,leader_id
			from csx_ods.csx_ods_csx_b2b_ucenter_user_df
			where sdt=regexp_replace(date_sub(current_date(),1),'-','')
			) c on split(a.fl_service_user_id_new,'、')[0]=c.id
      
			left join 
			(
			select id,leader_id
			from csx_ods.csx_ods_csx_b2b_ucenter_user_df
			where sdt=regexp_replace(date_sub(current_date(),1),'-','')
			) d on split(a.bbc_service_user_id_new,'、')[0]=d.id
		)a  
		left join 
		(
		select id,leader_id,user_number,name
		from csx_ods.csx_ods_csx_b2b_ucenter_user_df
		where sdt=regexp_replace(date_sub(current_date(),1),'-','')
		and user_position in ('POSITION-26586','POSITION-26629') --  POSITION-26586	客服经理 POSITION-26629	高级客服经理
		) b on a.rp_leader_id=b.id    
		left join 
		(
		select id,leader_id,user_number,name
		from csx_ods.csx_ods_csx_b2b_ucenter_user_df
		where sdt=regexp_replace(date_sub(current_date(),1),'-','')
		and user_position in ('POSITION-26586','POSITION-26629') --  POSITION-26586	客服经理 POSITION-26629	高级客服经理
		) c on a.fl_leader_id=c.id 
		left join 
		(
		select id,leader_id,user_number,name
		from csx_ods.csx_ods_csx_b2b_ucenter_user_df
		where sdt=regexp_replace(date_sub(current_date(),1),'-','')
		and user_position in ('POSITION-26586','POSITION-26629') --  POSITION-26586	客服经理 POSITION-26629	高级客服经理
		) d on a.bbc_leader_id=d.id
	)a 
left join 
(
select id,user_number,name
from csx_ods.csx_ods_csx_b2b_ucenter_user_df
where sdt=regexp_replace(date_sub(current_date(),1),'-','')
and user_position in ('POSITION-26586','POSITION-26629') --  POSITION-26586	客服经理 POSITION-26629	高级客服经理
) b on a.rp_city_leader_id=b.id

left join 
(
select id,user_number,name
from csx_ods.csx_ods_csx_b2b_ucenter_user_df
where sdt=regexp_replace(date_sub(current_date(),1),'-','')
and user_position in ('POSITION-26586','POSITION-26629') --  POSITION-26586	客服经理 POSITION-26629	高级客服经理
) c on a.fl_city_leader_id=c.id

left join 
(
select id,user_number,name
from csx_ods.csx_ods_csx_b2b_ucenter_user_df
where sdt=regexp_replace(date_sub(current_date(),1),'-','')
and user_position in ('POSITION-26586','POSITION-26629') --  POSITION-26586	客服经理 POSITION-26629	高级客服经理
) d on a.bbc_city_leader_id=d.id
;
   
