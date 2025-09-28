select
	a.*,b.user_number
from
	(
	select 
		*,
		--sdt as create_date,
		concat(substr(create_time,1,4),'年',substr(create_time,6,2),'月',substr(create_time,9,2),'日') as complaint_date,
		substr(create_time,12,8) as complaint_time,
		--complaint_deal_time as deal_time,
		regexp_replace(substr(complaint_deal_time,1,10),'-','') as deal_date,
		--round((coalesce(unix_timestamp(complaint_deal_time),0)-unix_timestamp(create_time))/3600,2) as processing_time,
		round((unix_timestamp(complaint_deal_time)-unix_timestamp(create_time))/3600,2) as processing_time,
		case when complaint_status=10 then '待判责'
			when complaint_status=20 then '处理中'
			when complaint_status=21 then '待审核'
			when complaint_status=30 then '已完成'
			when complaint_status=-1 then '已取消'
		end as complaint_status_name,
		case when complaint_deal_status=10 then '待处理'
			when complaint_deal_status=20 then '待修改'
			when complaint_deal_status=30 then '已处理待审'
			when complaint_deal_status=31 then '已驳回待审核'
			when complaint_deal_status=40 then '已完成'
			when complaint_deal_status=-1 then '已取消'
		end as complaint_deal_status_name	
		
	from 
		csx_dw.dws_oms_r_d_complaint_detail
	--where
	--	complaint_status=30
	) a 
	left join(select id,user_number,name from csx_dw.dws_basic_w_a_user where sdt='current' and del_flag = '0') b on b.id=a.create_by_id
where
	1=1
	${if(len(SDATE)==0,"","AND sdt>='"+SDATE+"'")}
	${if(len(EDATE)==0,"","AND sdt<='"+EDATE+"'")}
	${if(len(SBJ)==0,"","AND deal_date>='"+SBJ+"'")}
	${if(len(EBJ)==0,"","AND deal_date<='"+EBJ+"'")}
	${if(len(sq)==0,"","AND province_name in( '"+sq+"') ")}
	${if(len(complaint_status_name)==0,"","AND complaint_status_name in( '"+complaint_status_name+"') ")}
	${if(len(complaint_deal_status_name)==0,"","AND complaint_deal_status_name in( '"+complaint_deal_status_name+"') ")}
	${if(len(main_category_code)==0,"","AND main_category_code in( '"+main_category_code+"') ")}
;
select 
	province_name
from 
	csx_dw.dws_oms_r_d_complaint_detail
where
	1=1
group by 
	province_name
;

	select 
		main_category_code,main_category_name
	from 
		csx_dw.dws_oms_r_d_complaint_detail
	where
		1=1
		--sdt=regexp_replace(substr(cast(date_sub(now(),1) as string),1,10),'-','')
	group by 
		main_category_code,main_category_name
	order by 
		1
;
select
	complaint_status,
		case when complaint_status=10 then '待判责'
			when complaint_status=20 then '处理中'
			when complaint_status=21 then '待审核'
			when complaint_status=30 then '已完成'
			when complaint_status=-1 then '已取消'
		end as complaint_status_name
from
	(
	select 
		distinct complaint_status
	from 
		csx_dw.dws_oms_r_d_complaint_detail
	where
		1=1
		--sdt=regexp_replace(substr(cast(date_sub(now(),1) as string),1,10),'-','')
	) a 
order by 1 desc
;
select
	complaint_deal_status,
	case when complaint_deal_status=10 then '待处理'
		when complaint_deal_status=20 then '待修改'
		when complaint_deal_status=30 then '已处理待审'
		when complaint_deal_status=31 then '已驳回待审核'
		when complaint_deal_status=40 then '已完成'
		when complaint_deal_status=-1 then '已取消'
	end as complaint_deal_status_name
from
	(
	select 
		complaint_deal_status
	from 
		csx_dw.dws_oms_r_d_complaint_detail
	where
		1=1
		--sdt=regexp_replace(substr(cast(date_sub(now(),1) as string),1,10),'-','')
		and complaint_deal_status is not null
	group by 
		complaint_deal_status
	) a 
order by 1 desc
;
-- ====================================================================================================================================================================================
select
	a.*,b.user_number
from
	(
	select 
		*,
		--sdt as create_date,
		concat(substr(cast(create_time as string),1,4),'年',substr(cast(create_time as string),6,2),'月',substr(cast(create_time as string),9,2),'日') as complaint_date,
		substr(cast(create_time as string),12,8) as complaint_time,
		regexp_replace(to_date(complaint_deal_time),'-','') as deal_date,
		round((unix_timestamp(complaint_deal_time)-unix_timestamp(create_time))/3600,2) as processing_time,
		case when complaint_status_code=10 then '待判责'
			when complaint_status_code=20 then '处理中'
			when complaint_status_code=21 then '待审核'
			when complaint_status_code=30 then '已完成'
			when complaint_status_code=-1 then '已取消'
		end as complaint_status_name,
		case when complaint_deal_status=10 then '待处理'
			when complaint_deal_status=20 then '待修改'
			when complaint_deal_status=30 then '已处理待审'
			when complaint_deal_status=31 then '已驳回待审核'
			when complaint_deal_status=40 then '已完成'
			when complaint_deal_status=-1 then '已取消'
		end as complaint_deal_status_name	
	from 
		-- csx_dw.dws_oms_r_d_complaint_detail
		csx_dws.csx_dws_oms_complaint_detail_di
	) a 
	left join
		(
		select 
			user_id,user_number,user_name 
		from 
			-- csx_dw.dws_basic_w_a_user 
			csx_dim.csx_dim_uc_user
		where 
			sdt='current' 
			and del_flag = '0'
		) b on b.user_id=a.create_by_id
where
	1=1
	${if(len(SDATE)==0,"","AND sdt>='"+SDATE+"'")}
	${if(len(EDATE)==0,"","AND sdt<='"+EDATE+"'")}
	${if(len(SBJ)==0,"","AND deal_date>='"+SBJ+"'")}
	${if(len(EBJ)==0,"","AND deal_date<='"+EBJ+"'")}
	${if(len(sq)==0,"","AND province_name in( '"+sq+"') ")}
	${if(len(complaint_status_name)==0,"","AND complaint_status_name in( '"+complaint_status_name+"') ")}
	${if(len(complaint_deal_status_name)==0,"","AND complaint_deal_status_name in( '"+complaint_deal_status_name+"') ")}
	${if(len(main_category_code)==0,"","AND main_category_code in( '"+main_category_code+"') ")}
;
select 
	province_name
from 
	csx_dw.dws_oms_r_d_complaint_detail
where
	1=1
group by 
	province_name
;

	select 
		main_category_code,main_category_name
	from 
		csx_dw.dws_oms_r_d_complaint_detail
	where
		1=1
		--sdt=regexp_replace(substr(cast(date_sub(now(),1) as string),1,10),'-','')
	group by 
		main_category_code,main_category_name
	order by 
		1
;
select
	complaint_status,
		case when complaint_status=10 then '待判责'
			when complaint_status=20 then '处理中'
			when complaint_status=21 then '待审核'
			when complaint_status=30 then '已完成'
			when complaint_status=-1 then '已取消'
		end as complaint_status_name
from
	(
	select 
		distinct complaint_status
	from 
		csx_dw.dws_oms_r_d_complaint_detail
	where
		1=1
		--sdt=regexp_replace(substr(cast(date_sub(now(),1) as string),1,10),'-','')
	) a 
order by 1 desc
;
select
	complaint_deal_status,
	case when complaint_deal_status=10 then '待处理'
		when complaint_deal_status=20 then '待修改'
		when complaint_deal_status=30 then '已处理待审'
		when complaint_deal_status=31 then '已驳回待审核'
		when complaint_deal_status=40 then '已完成'
		when complaint_deal_status=-1 then '已取消'
	end as complaint_deal_status_name
from
	(
	select 
		complaint_deal_status
	from 
		csx_dw.dws_oms_r_d_complaint_detail
	where
		1=1
		--sdt=regexp_replace(substr(cast(date_sub(now(),1) as string),1,10),'-','')
		and complaint_deal_status is not null
	group by 
		complaint_deal_status
	) a 
order by 1 desc
;
-- ========================================================================================================================================================================================
select
	*
from
	csx_analyse.csx_analyse_fr_oms_complaint_detail_di
where
	1=1
	${if(len(SDATE)==0,"","AND sdt>='"+SDATE+"'")}
	${if(len(EDATE)==0,"","AND sdt<='"+EDATE+"'")}
	${if(len(SBJ)==0,"","AND deal_date>='"+SBJ+"'")}
	${if(len(EBJ)==0,"","AND deal_date<='"+EBJ+"'")}
	${if(len(sq)==0,"","AND performance_province_name in( '"+sq+"') ")}
	${if(len(complaint_status_code)==0,"","AND complaint_status_code in("+complaint_status_code+") ")}
	${if(len(complaint_deal_status)==0,"","AND complaint_deal_status in("+complaint_deal_status+") ")}
	${if(len(main_category_code)==0,"","AND main_category_code in( '"+main_category_code+"') ")}
	
	
