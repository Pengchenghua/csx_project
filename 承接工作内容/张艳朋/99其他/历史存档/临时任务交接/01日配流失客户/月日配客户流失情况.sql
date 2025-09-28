--XX月日配客户流失情况， 历史有日配近90天无日配、历史有日配近90天无任何业务

--select ${hiveconf:last_month_lastday},${hiveconf:last_month};

set last_month_lastday =regexp_replace(add_months(last_day(date_sub(current_date,1)),-1),'-','');
set last_month =substr(${hiveconf:last_month_lastday},1,6);


select ${hiveconf:last_month} smonth,c.sales_province_name,c.city_group_name,c.customer_no,c.customer_name,
'是' liushi_ripei,
if(substr(regexp_replace(date_add(from_unixtime(unix_timestamp(a.last_order_date,'yyyymmdd'),'yyyy-mm-dd'), 90), '-', ''),1,6)
   >${hiveconf:last_month},'否','是') liushi_all
--d.company_code,e.company_name,d.sign_company_code,f.company_name
from 
(
select * from csx_dw.dws_crm_w_a_customer_active where sdt='current'
)a 
left join 
(
select * 
from csx_dw.dws_crm_w_a_customer 
where sdt=${hiveconf:last_month_lastday}
and channel_code in('1','7','9')
)c on c.customer_no=a.customer_no
where substr(regexp_replace(date_add(from_unixtime(unix_timestamp(a.normal_last_order_date,'yyyymmdd'),'yyyy-mm-dd'), 90), '-', ''),1,6)=${hiveconf:last_month}
;


