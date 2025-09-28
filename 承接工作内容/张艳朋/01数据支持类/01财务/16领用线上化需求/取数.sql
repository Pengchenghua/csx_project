select 
	* 
from 
	csx_analyse.csx_analyse_wms_requisition_order_detail_di
where  
	sdt >= '${SDATE}' and sdt<='${EDATE}'	
	${if(len(s_excute_date)==0,"","AND excute_date>='"+s_excute_date+"'")}
	${if(len(e_excute_date)==0,"","AND excute_date<='"+e_excute_date+"'")}
	${if(len(stu)==0,"","and status in ("+stu+") ")}
	${if(len(sp_stu)==0,"","and approval_status in ("+sp_stu+") ")}
	${if(len(sq)==0,"","and province_name in ('"+sq+"') ")}
	${if(len(typ)==0,"","and requisition_type_name in ('"+typ+"') ")}	
	${if(len(dc)==0,"","and dc_code in ('"+dc+"') ")}
	${if(len(cbzx)==0,"","and cost_center_name in ('"+cbzx+"') ")}
;

select 
	* 
from 
	data_analysis_prd.report_csx_analyse_wms_requisition_order_detail_di
where  
	sdt >= '${SDATE}' and sdt<='${EDATE}'	
	${if(len(s_excute_date)==0,"","AND excute_date>='"+s_excute_date+"'")}
	${if(len(e_excute_date)==0,"","AND excute_date<='"+e_excute_date+"'")}
	${if(len(stu)==0,"","and status in ("+stu+") ")}
	${if(len(sp_stu)==0,"","and approval_status in ("+sp_stu+") ")}
	${if(len(sq)==0,"","and province_name in ('"+sq+"') ")}
	${if(len(typ)==0,"","and requisition_type_name in ('"+typ+"') ")}	
	${if(len(dc)==0,"","and dc_code in ('"+dc+"') ")}
	${if(len(cbzx)==0,"","and cost_center_name in ('"+cbzx+"') ")}
;
SELECT  
	dc_code 
from 
	data_analysis_prd.report_csx_analyse_wms_requisition_order_detail_di
group by 
	dc_code
order by 
;
SELECT  
	cost_center_name
from 
	data_analysis_prd.report_csx_analyse_wms_requisition_order_detail_di
where
	cost_center_name !=''
group by 
	cost_center_name
order by 
	1
;
SELECT  
	requisition_type_name
from 
	data_analysis_prd.report_csx_analyse_wms_requisition_order_detail_di
where
	requisition_type_name !=''
group by 
	requisition_type_name
order by 
	1
;
select 
	first_name,second_name,requisition_type_name,sum(requisition_amt_no_tax)/10000 as requisition_amt_no_tax
from 
	data_analysis_prd.report_csx_analyse_wms_requisition_order_detail_di
where
	-- approval_status=1 
	status=1	
	and excute_date>= '${s_excute_date}'
	and excute_date<= '${e_excute_date}'
	${if(len(SDATE)==0,"","AND sdt>='"+SDATE+"'")}
	${if(len(EDATE)==0,"","AND sdt<='"+EDATE+"'")}
	${if(len(sq)==0,"","and province_name in ('"+sq+"') ")}
	${if(len(dc)==0,"","and dc_code in ('"+dc+"') ")}
group by 
	first_name,second_name,requisition_type_name
;

select 
	area_name,province_name,city_name,requisition_type_name,sum(requisition_amt_no_tax)/10000 as requisition_amt_no_tax
from 
	data_analysis_prd.report_csx_analyse_wms_requisition_order_detail_di
where
	-- approval_status=1 
	status=1	
	and excute_date>= '${s_excute_date}'
	and excute_date<= '${e_excute_date}'
	${if(len(SDATE)==0,"","AND sdt>='"+SDATE+"'")}
	${if(len(EDATE)==0,"","AND sdt<='"+EDATE+"'")}
	${if(len(sq)==0,"","and province_name in ('"+sq+"') ")}
	${if(len(dc)==0,"","and dc_code in ('"+dc+"') ")}
group by 
	area_name,province_name,city_name,requisition_type_name	
;

select
	a.requisition_type_name,
	a.requisition_amt_no_tax/b.requisition_amt_no_tax as requisition_amt_no_tax_rate
from
	(
	select 
		'总计' as flag,requisition_type_name,sum(requisition_amt_no_tax) as requisition_amt_no_tax
	from 
		data_analysis_prd.report_csx_analyse_wms_requisition_order_detail_di
	where
		-- approval_status=1 
		status=1	
		and excute_date>= '${s_excute_date}'
		and excute_date<= '${e_excute_date}'
		${if(len(SDATE)==0,"","AND sdt>='"+SDATE+"'")}
		${if(len(EDATE)==0,"","AND sdt<='"+EDATE+"'")}
		${if(len(sq)==0,"","and province_name in ('"+sq+"') ")}
		${if(len(dc)==0,"","and dc_code in ('"+dc+"') ")}
	group by 
		requisition_type_name
	) a 
	left join
		(	
		select 
			'总计' as flag,sum(requisition_amt_no_tax) as requisition_amt_no_tax
		from 
			data_analysis_prd.report_csx_analyse_wms_requisition_order_detail_di
		where
			-- approval_status=1 
			status=1 
			and excute_date>= '${s_excute_date}'
			and excute_date<= '${e_excute_date}'
			${if(len(SDATE)==0,"","AND sdt>='"+SDATE+"'")}
			${if(len(EDATE)==0,"","AND sdt<='"+EDATE+"'")}
			${if(len(sq)==0,"","and province_name in ('"+sq+"') ")}
			${if(len(dc)==0,"","and dc_code in ('"+dc+"') ")}
		) b on b.flag=a.flag
order by 
	2 desc	


;

select 
	substr(excute_date,1,6) as smonth,sum(requisition_amt_no_tax)/10000 as requisition_amt_no_tax
from 
	data_analysis_prd.report_csx_analyse_wms_requisition_order_detail_di
where
	approval_status=1  
	${if(len(sq)==0,"","and province_name in ('"+sq+"') ")}
	${if(len(dc)==0,"","and dc_code in ('"+dc+"') ")}
	and excute_date>=date_format(date_sub(curdate()-day(curdate())+1,interval 13 month),'%Y%m%d')
group by 
	substr(excute_date,1,6)
order by 
	1
;

select 
	substr(excute_date,1,6) as smonth,sum(requisition_amt_no_tax)/10000 as requisition_amt_no_tax
from 
	data_analysis_prd.report_csx_analyse_wms_requisition_order_detail_di
where
	approval_status=1
	and excute_date>= '${s_excute_date}'
	and excute_date<= '${e_excute_date}'	
	${if(len(sq)==0,"","and province_name in ('"+sq+"') ")}
	${if(len(dc)==0,"","and dc_code in ('"+dc+"') ")}
	-- and excute_date>=date_format(date_sub(curdate()-day(curdate())+1,interval 13 month),'%Y%m%d')
group by 
	substr(excute_date,1,6)
order by 
	1
