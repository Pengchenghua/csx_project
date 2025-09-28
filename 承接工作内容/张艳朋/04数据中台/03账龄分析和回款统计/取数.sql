select
	distinct
	sales_user_name,
	case when sales_user_name = supervisor_user_name then '' else supervisor_user_name end as supervisor_user_name
from
	(
	select
		user_name as sales_user_name,
		leader_user_name as supervisor_user_name,
		row_number() over(partition by user_id order by distance desc) as ranks
	from 
		csx_dim.csx_dim_uc_user_extend
	where 
		sdt = 'current' and status = 0 and delete_flag = '0' and user_position = 'SALES' and distance in (0,1)
	) a 
where ranks = 1

;
select
	distinct
	case when sales_name = leader_user_name then '' else leader_user_name end as sales_supervisor_name,
	sales_name
from
	(
	select
		user_id as sales_id,
		user_name as sales_name,
		leader_user_name,
		row_number() over(partition by user_id order by distance desc) as ranks,
		case when province_id in ('35','36') then '35' else province_id end as province_id
	from 
		-- csx_dw.dwd_uc_w_a_user_adjust
		csx_dim.csx_dim_uc_user_extend
	where 
		sdt = 'current' and status = 0 and delete_flag = '0' and user_position = 'SALES' and distance in (0,1)
	) a 
where 
	ranks = 1
	${if(len(prov)== 0, "", " and province_id in ('"+prov+ "'))")} 

select
	distinct
	case when sales_name = leader_user_name then '' else leader_user_name end as sales_supervisor_name,
	sales_name
from
	(
	select
		user_id as sales_id,
		user_name as sales_name,
		leader_user_name,
		row_number() over(partition by user_id order by distance desc) as ranks,
		case when province_id in ('35','36') then '35' else province_id end as province_id
	from 
		-- csx_dw.dwd_uc_w_a_user_adjust
		csx_dim.csx_dim_uc_user_extend
	where 
		sdt = 'current' and status = 0 and delete_flag = '0' and user_position = 'SALES' and distance in (0,1)
	) a 
where 
	ranks = 1
	${if(len(prov)== 0, "", " and province_id in ('"+prov+ "')")} 
	
select
	distinct
	case when sales_name = leader_user_name then '' else leader_user_name end as sales_supervisor_name,
	sales_name
from
	(
	select
		user_id as sales_id,
		user_name as sales_name,
		leader_user_name,
		row_number() over(partition by user_id order by distance desc) as ranks,
		case when province_id in ('35','36') then '35' else province_id end as province_id
	from 
		-- csx_dw.dwd_uc_w_a_user_adjust
		csx_dim.csx_dim_uc_user_extend
	where 
		sdt = 'current' and status = 0 and delete_flag = '0' and user_position = 'SALES' and distance in (0,1)
	) a 
where 
	ranks = 1
	and province_id in ('15')