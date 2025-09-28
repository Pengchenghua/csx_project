--SAP应收、逾期：省区城市汇总
select a.smonth,
  coalesce(c.sales_region_name,b.sales_region_name,d.sales_region_name) sales_region_name,
  if(b.province_name is null and d.sales_province_name is null and a.province_name is null,
  	case when a.customer_no in ('G7150')  then '平台-食百采购'
  	     when substr(f.company_name, 1, 2) in('上海', '北京', '重庆') then concat(substr(f.company_name, 1, 2), '市')
  		 when substr(f.company_name, 1, 2)= '永辉' then '福建省'
  		 else concat(substr(f.company_name, 1, 2), '省') end,
  	coalesce(b.province_name,d.sales_province_name,a.province_name,'其他')) as province_name,
  coalesce(b.city_group_name,d.city_group_name,a.sales_city,'其他') as city_group_name,
  sum(if(a.ac_all>=0,a.ac_all,0)) as ac_all,
  sum(if(a.ac_all>=0 and a.ac_all-a.ac_wdq>=0,a.ac_all-a.ac_wdq,0)) as ac_yq
from
(
  select substr(sdt,1,6) smonth,*
  from csx_tmp.ads_fr_account_receivables
  where sdt in('20210531','20210630','20210731','20210831')
)a 
left join
(
  select * 
  from csx_dw.dws_crm_w_a_customer where sdt='current'
)b on a.customer_no=b.customer_no
left join
(
  select distinct sales_region_name,province_name
  from csx_dw.dws_crm_w_a_customer where sdt='current'
)c on c.province_name=b.province_name
left join
 (select * 
from csx_dw.dws_basic_w_a_csx_shop_m where sdt='current'
)d on a.customer_no=concat('S', d.shop_id)
left join (select distinct code as company_code,name as company_name from csx_dw.dws_basic_w_a_company_code where sdt='current')f on a.comp_code = f.company_code
group by a.smonth,
  coalesce(c.sales_region_name,b.sales_region_name,d.sales_region_name),
  if(b.province_name is null and d.sales_province_name is null and a.province_name is null,
  	case when a.customer_no in ('G7150')  then '平台-食百采购'
  	     when substr(f.company_name, 1, 2) in('上海', '北京', '重庆') then concat(substr(f.company_name, 1, 2), '市')
  		 when substr(f.company_name, 1, 2)= '永辉' then '福建省'
  		 else concat(substr(f.company_name, 1, 2), '省') end,
  	coalesce(b.province_name,d.sales_province_name,a.province_name,'其他')),
  coalesce(b.city_group_name,d.city_group_name,a.sales_city,'其他');

--SAP应收、逾期：客户明细
insert overwrite directory '/tmp/raoyanhua/linshi01' row format delimited fields terminated by '\t'
select a.smonth,
  coalesce(c.sales_region_name,b.sales_region_name,d.sales_region_name) sales_region_name,
  if(b.province_name is null and d.sales_province_name is null and a.province_name is null,
  	case when a.customer_no in ('G7150')  then '平台-食百采购'
  	     when substr(f.company_name, 1, 2) in('上海', '北京', '重庆') then concat(substr(f.company_name, 1, 2), '市')
  		 when substr(f.company_name, 1, 2)= '永辉' then '福建省'
  		 else concat(substr(f.company_name, 1, 2), '省') end,
  	coalesce(b.province_name,d.sales_province_name,a.province_name,'其他')) as province_name,
  coalesce(b.city_group_name,d.city_group_name,a.sales_city,'其他') as city_group_name,
  a.channel_name,a.hkont,a.account_name,a.comp_code,a.comp_name,a.province_name,
  a.sales_city,a.first_supervisor_name,a.sales_name,a.customer_no,a.customer_name,a.credit_limit,a.temp_credit_limit,
  a.first_category,a.second_category,a.third_category,
  a.payment_terms, a.payment_name,  --a.payment_days,a.zterm,a.diff,
  if(a.ac_all>=0,a.ac_all,0) as ac_all,
  if(a.ac_all>=0 and a.ac_all-a.ac_wdq>=0,a.ac_all-a.ac_wdq,0) as ac_yq
from
(
  select substr(sdt,1,6) smonth,*
  from csx_tmp.ads_fr_account_receivables
  where sdt in('20210531','20210630','20210731','20210831')
)a 
left join
(
  select * 
  from csx_dw.dws_crm_w_a_customer where sdt='current'
)b on a.customer_no=b.customer_no
left join
(
  select distinct sales_region_name,province_name
  from csx_dw.dws_crm_w_a_customer where sdt='current'
)c on c.province_name=b.province_name
left join
 (select * 
from csx_dw.dws_basic_w_a_csx_shop_m where sdt='current'
)d on a.customer_no=concat('S', d.shop_id)
left join (select distinct code as company_code,name as company_name from csx_dw.dws_basic_w_a_company_code where sdt='current')f on a.comp_code = f.company_code;


				 
