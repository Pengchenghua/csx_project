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
	${if(len(complaint_status_name)==0,"","AND complaint_status_name in( '"+complaint_status_name+"') ")}
;

-- 旧
select
	*
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
		case when complaint_status=10 then '待处理'
			when complaint_status=20 then '已处理待确认'
			when complaint_status=21 then '驳回待确认'
			when complaint_status=30 then '已处理'
			when complaint_status=-1 then '已取消'
		end as complaint_status_name
	from 
		csx_dw.dws_oms_r_d_complaint_detail
	--where
	--	complaint_status=30
	) a 
where
	1=1
	${if(len(SDATE)==0,"","AND sdt>='"+SDATE+"'")}
	${if(len(EDATE)==0,"","AND sdt<='"+EDATE+"'")}
	${if(len(SBJ)==0,"","AND deal_date>='"+SBJ+"'")}
	${if(len(EBJ)==0,"","AND deal_date<='"+EBJ+"'")}
	${if(len(sq)==0,"","AND province_name in( '"+sq+"') ")}
	${if(len(complaint_status_name)==0,"","AND complaint_status_name in( '"+complaint_status_name+"') ")}
;



select
	complaint_status,
	case when complaint_status=10 then '待处理'
		when complaint_status=20 then '已处理待确认'
		when complaint_status=21 then '驳回待确认'
		when complaint_status=30 then '已处理'
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
	distinct 
	complaint_status_code,
	complaint_status_name
from
	csx_analyse.csx_analyse_fr_oms_complaint_detail_di
order by 1 desc
	
-- 新	
select 
	distinct
	performance_province_name
from 
	csx_analyse.csx_analyse_fr_oms_complaint_detail_di
where
	1=1
	
		
select 
	distinct province_name
from 
	csx_dw.dws_oms_r_d_complaint_detail
where
	1=1
;



select
	*
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
		case when complaint_status=10 then '待处理'
			when complaint_status=20 then '已处理待确认'
			when complaint_status=21 then '驳回待确认'
			when complaint_status=30 then '已处理'
			when complaint_status=-1 then '已取消'
		end as complaint_status_name
	from 
		csx_dw.dws_oms_r_d_complaint_detail
	--where
	--	complaint_status=30
	) a 
where
	1=1
	${if(len(SDATE)==0,"","AND sdt>='"+SDATE+"'")}
	${if(len(EDATE)==0,"","AND sdt<='"+EDATE+"'")}
	${if(len(SBJ)==0,"","AND deal_date>='"+SBJ+"'")}
	${if(len(EBJ)==0,"","AND deal_date<='"+EBJ+"'")}
	${if(len(sq)==0,"","AND province_name in( '"+sq+"') ")}
	${if(len(complaint_status_name)==0,"","AND complaint_status_name in( '"+complaint_status_name+"') ")}
;



select
	*
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
		case when complaint_status=10 then '待处理'
			when complaint_status=20 then '已处理待确认'
			when complaint_status=21 then '驳回待确认'
			when complaint_status=30 then '已处理'
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
where
	1=1
	${if(len(SDATE)==0,"","AND sdt>='"+SDATE+"'")}
	${if(len(EDATE)==0,"","AND sdt<='"+EDATE+"'")}
	${if(len(SBJ)==0,"","AND deal_date>='"+SBJ+"'")}
	${if(len(EBJ)==0,"","AND deal_date<='"+EBJ+"'")}
	${if(len(sq)==0,"","AND province_name in( '"+sq+"') ")}
	${if(len(complaint_status_name)==0,"","AND complaint_status_name in( '"+complaint_status_name+"') ")}
	${if(len(complaint_deal_status_name)==0,"","AND complaint_deal_status_name in( '"+complaint_deal_status_name+"') ")}
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
	