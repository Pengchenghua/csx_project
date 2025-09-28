--================================================================================================
	
insert overwrite directory '/tmp/zhangyanpeng/20210907_linshi_1' row format delimited fields terminated by '\t' 
	
select 
	*
from 
	csx_dw.dws_basic_w_a_employee_org_m
where 
	sdt = '20210906'
	--and emp_status='on'
;
	
--================================================================================================

select 
	work_no,sales_name,
	count(case when regexp_replace(substr(sign_time,1,10),'-','') between '20210301' and '20210831' then customer_no else null end)cnt1,
	count(case when regexp_replace(substr(sign_time,1,10),'-','') between '20210601' and '20210831' then customer_no else null end)cnt2,
	count(case when regexp_replace(substr(sign_time,1,10),'-','') between '20210801' and '20210831' then customer_no else null end)cnt3
from 
	csx_dw.dws_crm_w_a_customer
where 
	sdt='20210906'
	and work_no in ('80952743','80007454','80768089','80895348','80929704','80937132','81016757','81026931','80917566','80936091','80691224','80895351','81089088','81101470','80816155','80764642','80973546','80980614','80952742','81056954','81080592','81082956','81095855','80001032','80012225','80960666','80969699','80972915','81094022','80895350','80912701','80924363','80927331','80929710','80939525','81081095','81101897')
group by 
	work_no,sales_name
;
	
