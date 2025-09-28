select
	*
from
	csx_analyse.csx_analyse_fr_sell_exceptions_monitor_m_df
where
	1=1
	${if(len(SDATE)==0,"","AND sdt='"+SDATE+"'")}
	${if(len(shop_code)==0,"","AND shop_code='"+shop_code+"'")}
	${if(len(ex_small_class_union)==0,"","AND ex_small_class_union in('"+ex_small_class_union+"')")}
	${if(len(sq)==0,"","AND performance_province_name in( '"+sq+"') ")}
	${if(len(status_name)==0,"","AND status_name in( '"+status_name+"') ")}
	${if(len(days)==0,"","AND residence_time_day>="+days+"")}	
;

-- 门店编码
select
	distinct 
	shop_code
from
	csx_analyse.csx_analyse_fr_sell_exceptions_monitor_m_df
where
	1=1
	${if(len(SDATE)==0,"","AND sdt='"+SDATE+"'")}
order by 1 desc
;

-- 异常小类
select 
	distinct
	ex_small_class_union
from 
	csx_analyse.csx_analyse_fr_sell_exceptions_monitor_m_df
where
	1=1
	${if(len(SDATE)==0,"","AND sdt='"+SDATE+"'")}
order by 1
;

-- 状态
select 
	distinct
	status,
	status_name
from 
	csx_analyse.csx_analyse_fr_sell_exceptions_monitor_m_df
where
	1=1
	${if(len(SDATE)==0,"","AND sdt='"+SDATE+"'")}
order by 1
;

select
	performance_province_code,
	performance_province_name,
	performance_city_code,
	performance_city_name,
	sdt,
	shop_code,
	shop_name,
	status_name,
	ex_big_class_name,
	ex_small_class_name,
	count(biz_id) as exceptions_total
from
	csx_analyse.csx_analyse_fr_sell_exceptions_monitor_m_df
where
	1=1
	${if(len(SDATE)==0,"","AND sdt='"+SDATE+"'")}
	${if(len(shop_code)==0,"","AND shop_code='"+shop_code+"'")}
	-- ${if(len(ex_small_class_union)==0,"","AND ex_small_class_union in('"+ex_small_class_union+"')")}
	${if(len(sq)==0,"","AND performance_province_name in( '"+sq+"') ")}
	-- ${if(len(status_name)==0,"","AND status_name in( '"+status_name+"') ")}
	-- ${if(len(days)==0,"","AND residence_time_day>="+days+"")}	
group by 
	performance_province_code,
	performance_province_name,
	performance_city_code,
	performance_city_name,
	sdt,
	shop_code,
	shop_name,
	status_name,
	ex_big_class_name,
	ex_small_class_name	
;
select * from 
	(
	select
		performance_province_code,performance_province_name,performance_city_code,performance_city_name,sdt,
		shop_code,shop_name,status_name,ex_big_class_name,ex_small_class_name,
		count(biz_id) as exceptions_total
	from
		csx_analyse.csx_analyse_fr_sell_exceptions_monitor_m_df
	where
		1=1
		${if(len(SDATE)==0,"","AND sdt='"+SDATE+"'")}
		${if(len(shop_code)==0,"","AND shop_code='"+shop_code+"'")}
		-- ${if(len(ex_small_class_union)==0,"","AND ex_small_class_union in('"+ex_small_class_union+"')")}
		${if(len(sq)==0,"","AND performance_province_name in( '"+sq+"') ")}
		-- ${if(len(status_name)==0,"","AND status_name in( '"+status_name+"') ")}
		-- ${if(len(days)==0,"","AND residence_time_day>="+days+"")}	
	group by 
		performance_province_code,performance_province_name,performance_city_code,performance_city_name,sdt,
		shop_code,shop_name,status_name,ex_big_class_name,ex_small_class_name
	) a 
	left join
		(
		select
			shop_code,max(residence_time_hour) as max_residence_time_hour,
			sum(residence_time_hour)/count(biz_id) as avg_residence_time_hour
		from
			csx_analyse.csx_analyse_fr_sell_exceptions_monitor_m_df
		where
			1=1
			${if(len(SDATE)==0,"","AND sdt='"+SDATE+"'")}
			${if(len(shop_code)==0,"","AND shop_code='"+shop_code+"'")}
		group by
			shop_code
		) b on b.shop_code=a.shop_code;
		
		
--===========================================================================================================================================================================		
select
	*
from
	data_analysis_prd.report_fr_sell_exceptions_monitor_m_df
where
	1=1
	${if(len(SDATE)==0,"","AND sdt='"+SDATE+"'")}
	${if(len(shop_code)==0,"","AND shop_code='"+shop_code+"'")}
	${if(len(ex_small_class_union)==0,"","AND ex_small_class_union in('"+ex_small_class_union+"')")}
	${if(len(sq)==0,"","AND performance_province_name in( '"+sq+"') ")}
	${if(len(status_name)==0,"","AND status_name in( '"+status_name+"') ")}
	${if(len(days)==0,"","AND residence_time_day>="+days+"")}	
;

select * from 
	(
	select
		performance_province_code,performance_province_name,performance_city_code,performance_city_name,sdt,
		shop_code,shop_name,status_name,ex_big_class_name,ex_small_class_name,
		count(biz_id) as exceptions_total
	from
		data_analysis_prd.report_fr_sell_exceptions_monitor_m_df
	where
		1=1
		${if(len(SDATE)==0,"","AND sdt='"+SDATE+"'")}
		${if(len(shop_code)==0,"","AND shop_code='"+shop_code+"'")}
		${if(len(sq)==0,"","AND performance_province_name in( '"+sq+"') ")}
	group by 
		performance_province_code,performance_province_name,performance_city_code,performance_city_name,sdt,
		shop_code,shop_name,status_name,ex_big_class_name,ex_small_class_name
	) a 
	left join
		(
		select
			shop_code,max(residence_time_hour) as max_residence_time_hour,
			sum(residence_time_hour)/count(biz_id) as avg_residence_time_hour
		from
			data_analysis_prd.report_fr_sell_exceptions_monitor_m_df
		where
			1=1
			${if(len(SDATE)==0,"","AND sdt='"+SDATE+"'")}
			${if(len(shop_code)==0,"","AND shop_code='"+shop_code+"'")}
		group by
			shop_code
		) b on b.shop_code=a.shop_code
		
select
	distinct 
	shop_code
from
	data_analysis_prd.report_fr_sell_exceptions_monitor_m_df
where
	1=1
	${if(len(SDATE)==0,"","AND sdt='"+SDATE+"'")}
order by 1 desc


select 
	distinct
	ex_small_class_union
from 
	data_analysis_prd.report_fr_sell_exceptions_monitor_m_df
where
	1=1
	${if(len(SDATE)==0,"","AND sdt='"+SDATE+"'")}
order by 1
			
			
select 
	distinct
	status,
	status_name
from 
	data_analysis_prd.report_fr_sell_exceptions_monitor_m_df
where
	1=1
	${if(len(SDATE)==0,"","AND sdt='"+SDATE+"'")}
order by 1

		


	