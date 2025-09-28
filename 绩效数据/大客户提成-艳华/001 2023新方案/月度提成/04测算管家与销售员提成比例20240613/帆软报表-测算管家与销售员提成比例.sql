-- 测算原则认为共享占比XX，销售+管家占比=1-XX
-- 如 有管家：管家0.15、销售0.6；无管家：销售0.6+0.15=0.75

-- drop table csx_analyse_tmp.csx_analyse_fr_tc_customer_bill_month_dff_rate_detail_chai;
create  table csx_analyse_tmp.csx_analyse_fr_tc_customer_bill_month_dff_rate_detail_chai
as
select
*
,if(service_falg<>'',tc_sales,
	if(rp_service_user_id<>'' or region_name='华南大区',tc_sales_rp,tc_sales_rp/rp_sales_fp_rate *0.75)+
	if(bbc_service_user_id<>'' or region_name='华南大区',tc_sales_bbc,tc_sales_bbc/bbc_sales_sale_fp_rate *0.75)+
	if(fl_service_user_id<>'' or region_name='华南大区',tc_sales_fl,tc_sales_fl/fl_sales_sale_fp_rate *0.75)) as tc_sales_15
,if(service_falg<>'',tc_rp_service,tc_rp_service/rp_service_user_fp_rate *0.15) as tc_rp_service_15
,if(service_falg<>'',tc_fl_service,tc_fl_service/fl_service_user_fp_rate *0.15) as tc_fl_service_15
,if(service_falg<>'',tc_bbc_service,tc_bbc_service/bbc_service_user_fp_rate *0.15) as tc_bbc_service_15

,if(service_falg<>'',tc_sales,
	if(rp_service_user_id<>'' or region_name='华南大区',tc_sales_rp,tc_sales_rp/rp_sales_fp_rate *0.80)+
	if(bbc_service_user_id<>'' or region_name='华南大区',tc_sales_bbc,tc_sales_bbc/bbc_sales_sale_fp_rate *0.80)+
	if(fl_service_user_id<>'' or region_name='华南大区',tc_sales_fl,tc_sales_fl/fl_sales_sale_fp_rate *0.80)) as tc_sales_20
,if(service_falg<>'',tc_rp_service,tc_rp_service/rp_service_user_fp_rate *0.20) as tc_rp_service_20
,if(service_falg<>'',tc_fl_service,tc_fl_service/fl_service_user_fp_rate *0.20) as tc_fl_service_20
,if(service_falg<>'',tc_bbc_service,tc_bbc_service/bbc_service_user_fp_rate *0.20) as tc_bbc_service_20

,if(service_falg<>'',tc_sales,
	if(rp_service_user_id<>'' or region_name='华南大区',tc_sales_rp,tc_sales_rp/rp_sales_fp_rate *0.85)+
	if(bbc_service_user_id<>'' or region_name='华南大区',tc_sales_bbc,tc_sales_bbc/bbc_sales_sale_fp_rate *0.85)+
	if(fl_service_user_id<>'' or region_name='华南大区',tc_sales_fl,tc_sales_fl/fl_sales_sale_fp_rate *0.85)) as tc_sales_25
,if(service_falg<>'',tc_rp_service,tc_rp_service/rp_service_user_fp_rate *0.25) as tc_rp_service_25
,if(service_falg<>'',tc_fl_service,tc_fl_service/fl_service_user_fp_rate *0.25) as tc_fl_service_25
,if(service_falg<>'',tc_bbc_service,tc_bbc_service/bbc_service_user_fp_rate *0.25) as tc_bbc_service_25
from csx_analyse.csx_analyse_fr_tc_customer_bill_month_dff_rate_detail_chai
where smt in('202404','202405')




-- 帆软结果表
-- 表1、客户汇总表
select 
smt,
(case when province_name='河南省' then '华北大区' 
      when province_name in ('安徽省','湖北省') then '华东大区' 
else region_name end) as region_name,
case when province_name in('上海松江','上海宝山','江苏苏州') then '上海'
	when province_name in('广东省') then '广东深圳'
	else province_name end as province_name,	
(case when province_name in ('上海松江') then '上海松江' 
      when province_name in ('江苏苏州') then '江苏苏州'  
	  when province_name in ('上海宝山') then '上海宝山'
      when city_group_name in ('南京市') then '南京主城'       
else city_group_name end) as city_group_name
,customer_code
,customer_name
,work_no
,sales_name
,rp_service_user_work_no
,rp_service_user_name
,fl_service_user_work_no
,fl_service_user_name
,bbc_service_user_work_no
,bbc_service_user_name
-- ,bill_month
,happen_month -- 销售月
,bill_date  -- 结算日期
,paid_date  -- 核销日期（打款日期）	
,dff_rate
,pay_amt
,rp_pay_amt
,bbc_pay_amt
,bbc_ly_pay_amt
,bbc_zy_pay_amt
,fl_pay_amt
-- ,sale_amt
-- ,rp_sale_amt
-- ,bbc_sale_amt
-- ,bbc_ly_sale_amt
-- ,bbc_zy_sale_amt
-- ,fl_sale_amt
-- ,profit
-- ,rp_profit
-- ,bbc_profit
-- ,bbc_ly_profit
-- ,bbc_zy_profit
-- ,fl_profit
,profit_rate
,rp_profit_rate
,bbc_profit_rate
,bbc_ly_profit_rate
,bbc_zy_profit_rate
,fl_profit_rate
,cust_rp_profit_rate_tc
,cust_bbc_ly_profit_rate_tc
,cust_bbc_zy_profit_rate_tc
,cust_fl_profit_rate_tc
,rp_sales_fp_rate
,fl_sales_sale_fp_rate
,bbc_sales_sale_fp_rate
,rp_service_user_fp_rate
,fl_service_user_fp_rate
,bbc_service_user_fp_rate
,new_cust_rate
,sales_profit_basic
,sales_profit_finish
,sales_target_rate
,sales_target_rate_tc
,rp_service_profit_basic
,rp_service_profit_finish
,rp_service_target_rate
,rp_service_target_rate_tc
,fl_service_profit_basic
,fl_service_profit_finish
,fl_service_target_rate
,fl_service_target_rate_tc
,bbc_service_profit_basic
,bbc_service_profit_finish
,bbc_service_target_rate
,bbc_service_target_rate_tc
,tc_sales
,tc_rp_service
,tc_fl_service
,tc_bbc_service
-- 本月销售额毛利额
,by_sale_amt
,by_rp_sale_amt
,by_bbc_sale_amt_zy
,by_bbc_sale_amt_ly
,by_fl_sale_amt
,by_profit
,by_rp_profit
,by_bbc_profit_zy
,by_bbc_profit_ly
,by_fl_profit
-- 服务费
,service_falg
,service_fee
,original_tc_sales
,original_tc_rp_service
,original_tc_fl_service
,original_tc_bbc_service

-- 调整管家比例后
,tc_sales_rp
,tc_sales_bbc
,tc_sales_fl

,tc_sales_15
,tc_rp_service_15
,tc_fl_service_15
,tc_bbc_service_15

,tc_sales_20
,tc_rp_service_20
,tc_fl_service_20
,tc_bbc_service_20

,tc_sales_25
,tc_rp_service_25
,tc_fl_service_25
,tc_bbc_service_25
from csx_analyse_tmp.csx_analyse_fr_tc_customer_bill_month_dff_rate_detail_chai
where 
	1=1 
	${if(len(EDATE)==0,"","AND smt in( '"+EDATE+"')")}
	${if(len(sq)==0,"","AND (case when province_name in('上海松江','上海宝山','江苏苏州') then '上海'
	when province_name in('广东省') then '广东深圳'
	else province_name end) in( '"+sq+"') ")}
	${if(len(city)==0,"","AND (case when province_name in ('上海松江') then '上海松江' 
      when province_name in ('江苏苏州') then '江苏苏州' 
	  when province_name in ('上海宝山') then '上海宝山'
      when city_group_name in ('南京市') then '南京主城'        
else city_group_name end) in( '"+city+"') ")}
	${if(len(customer_no)==0,"","and customer_code in ('"+replace(customer_no,",",
	"','")+"') ")}
	${if(len(sales_name)==0,"","and sales_name in ('"+replace(sales_name,",","','")+"') ")}
	${if(len(rp_service_user_name)==0,"","and rp_service_user_name in ('"+replace(rp_service_user_name,",","','")+"') ")}
	${if(len(fl_service_user_name)==0,"","and fl_service_user_name in ('"+replace(fl_service_user_name,",","','")+"') ")}
	${if(len(bbc_service_user_name)==0,"","and bbc_service_user_name in ('"+replace(bbc_service_user_name,",","','")+"') ")}
	
	
	
	
-- 表2 销售员汇总表
select 
smt,
(case when province_name='河南省' then '华北大区' 
      when province_name in ('安徽省','湖北省') then '华东大区' 
else region_name end) as region_name,
case when province_name in('上海松江','上海宝山','江苏苏州') then '上海'
	when province_name in('广东省') then '广东深圳'
	else province_name end as province_name,	
(case when province_name in ('上海松江') then '上海松江' 
      when province_name in ('江苏苏州') then '江苏苏州' 
	  when province_name in ('上海宝山') then '上海宝山'
      when city_group_name in ('南京市') then '南京主城'        
else city_group_name end) as city_group_name,
work_no,
sales_name,
sales_profit_basic,
sales_profit_finish,
sales_target_rate,
sales_target_rate_tc,
sum(pay_amt) pay_amt,
sum(rp_pay_amt) rp_pay_amt,
sum(bbc_pay_amt) bbc_pay_amt,
sum(bbc_ly_pay_amt) bbc_ly_pay_amt,
sum(bbc_zy_pay_amt) bbc_zy_pay_amt,
sum(fl_pay_amt) fl_pay_amt,
-- sum(sale_amt) sale_amt,
-- sum(rp_sale_amt) rp_sale_amt,
-- sum(bbc_sale_amt) bbc_sale_amt,
-- sum(bbc_ly_sale_amt) bbc_ly_sale_amt,
-- sum(bbc_zy_sale_amt) bbc_zy_sale_amt,
-- sum(fl_sale_amt) fl_sale_amt,
sum(tc_sales) tc_sales,
sum(original_tc_sales) original_tc_sales,
sum(tc_sales_rp) as tc_sales_rp,
sum(tc_sales_bbc) as tc_sales_bbc,
sum(tc_sales_fl) as tc_sales_fl,
sum(tc_sales_15) as tc_sales_15,
sum(tc_sales_20) as tc_sales_20,	
sum(tc_sales_25) as tc_sales_25	
from csx_analyse_tmp.csx_analyse_fr_tc_customer_bill_month_dff_rate_detail_chai
where 
	1=1 
	${if(len(EDATE)==0,"","AND smt in( '"+EDATE+"')")}
	${if(len(sq)==0,"","AND (case when province_name in('上海松江','上海宝山','江苏苏州') then '上海'
		when province_name in('广东省') then '广东深圳'
	else province_name end) in( '"+sq+"') ")}
	${if(len(city)==0,"","AND (case when province_name in ('上海松江') then '上海松江' 
      when province_name in ('江苏苏州') then '江苏苏州' 
	  when province_name in ('上海宝山') then '上海宝山'
      when city_group_name in ('南京市') then '南京主城'        
else city_group_name end) in( '"+city+"') ")}
	${if(len(sales_name)==0,"","and sales_name in ('"+replace(sales_name,",","','")+"') ")}	
group by 
smt,
(case when province_name='河南省' then '华北大区' 
      when province_name in ('安徽省','湖北省') then '华东大区' 
else region_name end),
case when province_name in('上海松江','上海宝山','江苏苏州') then '上海'
	when province_name in('广东省') then '广东深圳'
	else province_name end,	
(case when province_name in ('上海松江') then '上海松江' 
      when province_name in ('江苏苏州') then '江苏苏州' 
	  when province_name in ('上海宝山') then '上海宝山'
      when city_group_name in ('南京市') then '南京主城'        
else city_group_name end),
work_no,
sales_name,
sales_profit_basic,
sales_profit_finish,
sales_target_rate,
sales_target_rate_tc;







-- 表3 管家汇总表	
select 
smt,
(case when province_name='河南省' then '华北大区' 
      when province_name in ('安徽省','湖北省') then '华东大区' 
else region_name end) as region_name,
case when province_name in('上海松江','上海宝山','江苏苏州') then '上海'
	when province_name in('广东省') then '广东深圳'
	else province_name end as province_name,	
(case when province_name in ('上海松江') then '上海松江' 
      when province_name in ('江苏苏州') then '江苏苏州' 
	  when province_name in ('上海宝山') then '上海宝山'
      when city_group_name in ('南京市') then '南京主城'        
else city_group_name end) as city_group_name,
service_user_work_no,
service_user_name,
service_profit_basic,
service_profit_finish,
service_target_rate,
service_target_rate_tc,
sum(pay_amt) pay_amt,
-- sum(sale_amt) sale_amt,
sum(tc_service) tc_service,
sum(original_tc_service) original_tc_service,
sum(tc_service_15) tc_service_15,
sum(tc_service_20) tc_service_20,
sum(tc_service_25) tc_service_25
from 
(
select 
smt,
region_name,
province_name,
city_group_name,
rp_service_user_work_no as service_user_work_no,
rp_service_user_name as service_user_name,
rp_service_profit_basic as service_profit_basic,
rp_service_profit_finish as service_profit_finish,
rp_service_target_rate as service_target_rate,
rp_service_target_rate_tc as service_target_rate_tc,
rp_pay_amt as pay_amt,
rp_sale_amt as sale_amt,
tc_rp_service as tc_service,
original_tc_rp_service as original_tc_service
-- 调整管家比例后
,tc_rp_service_15 as tc_service_15
,tc_rp_service_20 as tc_service_20
,tc_rp_service_25 as tc_service_25
from csx_analyse_tmp.csx_analyse_fr_tc_customer_bill_month_dff_rate_detail_chai
where 
	rp_service_user_work_no<>''
	${if(len(EDATE)==0,"","AND smt in( '"+EDATE+"')")}
	${if(len(sq)==0,"","AND (case when province_name in('上海松江','上海宝山','江苏苏州') then '上海'
		when province_name in('广东省') then '广东深圳'
	else province_name end) in( '"+sq+"') ")}
	${if(len(city)==0,"","AND (case when province_name in ('上海松江') then '上海松江' 
      when province_name in ('江苏苏州') then '江苏苏州' 
	  when province_name in ('上海宝山') then '上海宝山'
      when city_group_name in ('南京市') then '南京主城'        
else city_group_name end) in( '"+city+"') ")}
	${if(len(customer_no)==0,"","and customer_code in ('"+replace(customer_no,",",
	"','")+"') ")}
	${if(len(rp_service_user_name)==0,"","and rp_service_user_name in ('"+replace(rp_service_user_name,",","','")+"') ")}

union all
select 
smt,
region_name,
province_name,
city_group_name,
fl_service_user_work_no as service_user_work_no,
fl_service_user_name as service_user_name,
fl_service_profit_basic as service_profit_basic,
fl_service_profit_finish as service_profit_finish,
fl_service_target_rate as service_target_rate,
fl_service_target_rate_tc as service_target_rate_tc,
fl_pay_amt as pay_amt,
fl_sale_amt as sale_amt,
tc_fl_service as tc_service,
original_tc_fl_service as original_tc_service
-- 调整管家比例后
,tc_fl_service_15 as tc_service_15
,tc_fl_service_20 as tc_service_20
,tc_fl_service_25 as tc_service_25
from csx_analyse_tmp.csx_analyse_fr_tc_customer_bill_month_dff_rate_detail_chai
where 
	fl_service_user_work_no<>''
	${if(len(EDATE)==0,"","AND smt in( '"+EDATE+"')")}
	${if(len(sq)==0,"","AND (case when province_name in('上海松江','上海宝山','江苏苏州') then '上海'
		when province_name in('广东省') then '广东深圳'
	else province_name end) in( '"+sq+"') ")}
	${if(len(city)==0,"","AND (case when province_name in ('上海松江') then '上海松江' 
      when province_name in ('江苏苏州') then '江苏苏州' 
	  when province_name in ('上海宝山') then '上海宝山'
      when city_group_name in ('南京市') then '南京主城'        
else city_group_name end) in( '"+city+"') ")}
	${if(len(customer_no)==0,"","and customer_code in ('"+replace(customer_no,",",
	"','")+"') ")}
	${if(len(fl_service_user_name)==0,"","and fl_service_user_name in ('"+replace(fl_service_user_name,",","','")+"') ")}

union all
select 
smt,
region_name,
province_name,
city_group_name,
bbc_service_user_work_no as service_user_work_no,
bbc_service_user_name as service_user_name,
bbc_service_profit_basic as service_profit_basic,
bbc_service_profit_finish as service_profit_finish,
bbc_service_target_rate as service_target_rate,
bbc_service_target_rate_tc as service_target_rate_tc,
bbc_pay_amt as pay_amt,
bbc_sale_amt as sale_amt,
tc_bbc_service as tc_service,
original_tc_bbc_service as original_tc_service
-- 调整管家比例后
,tc_bbc_service_15 as tc_service_15
,tc_bbc_service_20 as tc_service_20
,tc_bbc_service_25 as tc_service_25
from csx_analyse_tmp.csx_analyse_fr_tc_customer_bill_month_dff_rate_detail_chai
where 
	bbc_service_user_work_no<>''
	${if(len(EDATE)==0,"","AND smt in( '"+EDATE+"')")}
	${if(len(sq)==0,"","AND (case when province_name in('上海松江','上海宝山','江苏苏州') then '上海'
		when province_name in('广东省') then '广东深圳'
	else province_name end) in( '"+sq+"') ")}
	${if(len(city)==0,"","AND (case when province_name in ('上海松江') then '上海松江' 
      when province_name in ('江苏苏州') then '江苏苏州' 
	  when province_name in ('上海宝山') then '上海宝山'
      when city_group_name in ('南京市') then '南京主城'        
else city_group_name end) in( '"+city+"') ")}
	${if(len(customer_no)==0,"","and customer_code in ('"+replace(customer_no,",",
	"','")+"') ")}
	${if(len(bbc_service_user_name)==0,"","and bbc_service_user_name in ('"+replace(bbc_service_user_name,",","','")+"') ")}
)a
group by 
smt,
(case when province_name='河南省' then '华北大区' 
      when province_name in ('安徽省','湖北省') then '华东大区' 
else region_name end),
case when province_name in('上海松江','上海宝山','江苏苏州') then '上海'
	when province_name in('广东省') then '广东深圳'
	else province_name end,	
(case when province_name in ('上海松江') then '上海松江' 
      when province_name in ('江苏苏州') then '江苏苏州' 
	  when province_name in ('上海宝山') then '上海宝山'
      when city_group_name in ('南京市') then '南京主城'        
else city_group_name end),
service_user_work_no,
service_user_name,
service_profit_basic,
service_profit_finish,
service_target_rate,
service_target_rate_tc;


-- 数据验证
select '旧表' as aa,smt,
sum(tc_sales) tc_sales,
0 as tc_sales_fen
from csx_analyse.csx_analyse_fr_tc_customer_bill_month_dff_rate_detail
where smt in('202404','202405')
group by smt

union all
select '新表' as aa,smt,
sum(tc_sales) tc_sales,
sum(tc_sales_rp+tc_sales_bbc+tc_sales_fl) as tc_sales_fen
from csx_analyse.csx_analyse_fr_tc_customer_bill_month_dff_rate_detail_chai
where smt in('202404','202405')
group by smt

union all
select '新表2' as aa,smt,
sum(tc_sales) tc_sales,
sum(tc_sales_rp+tc_sales_bbc+tc_sales_fl) as tc_sales_fen
from csx_analyse_tmp.csx_analyse_fr_tc_customer_bill_month_dff_rate_detail_chai
where smt in('202404','202405')
group by smt
	
	
	