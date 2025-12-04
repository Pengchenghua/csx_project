-- 标讯导入相关表
-- 删除月份数据
delete from data_analysis_prd.bid_info where bid_date>='2025-09-01'
;

select id,bid_name,bid_amount,win_bid_date ,win_bid_amount,supply_deadline,bid_date,bid_status     from data_analysis_prd.bid_info where   bid_date>='2025-08-01'
and bid_name like '2028年度“百城e采”商城供应商征集'
;

select count(*) from data_analysis_prd.bid_info where bid_date>='2025-08-01';


-- 查询
  select *
from data_analysis_prd.report_csx_analyse_tmp_report_bid_info a
where sdt = DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 1 DAY), '%Y%m%d')
and bid_no='BXFJ20250729000002'



;

select * from csx_analyse_tmp_ppt_bid_info ;





SELECT
  performance_region_name,
  performance_province_name,
  sum(case when win_bid_date !='' then 1 else 0 end ) as win_bid_date_cn,
  count(bid_customer_name) as total_cnt,
  SUM(CASE WHEN bid_status_name = '流标（投标后）' THEN 1 ELSE 0 END) AS loss_bid_cust,
  SUM(CASE WHEN bid_status_name = '中标' THEN 1 ELSE 0 END) AS win_bid_cust,
  SUM(CASE WHEN bid_status_name = '中标' THEN COALESCE(CAST(win_bid_amount AS DECIMAL(26,6)), 0) ELSE 0 END) AS win_bid_amount,
  sum(CASE WHEN bid_status_name = '中标' THEN COALESCE(CAST(annual_amount AS DECIMAL(26,6)), 0) ELSE 0 END) annual_amount,
  -- 新客
  SUM(CASE WHEN bid_status_name = '中标' and business_attribute_name='日配' and new_cust_flag=1 THEN 1 ELSE 0 END) AS rp_win_bid_cust,
  SUM(CASE WHEN bid_status_name = '中标' and business_attribute_name='日配' and new_cust_flag=1 THEN COALESCE(CAST(win_bid_amount AS DECIMAL(26,6)), 0) ELSE 0 END) AS rp_win_bid_amount,
  SUM(CASE WHEN bid_status_name = '中标' and business_attribute_name !='日配' and new_cust_flag=1 THEN 1 ELSE 0 END) AS qt_win_bid_cust,
  SUM(CASE WHEN bid_status_name = '中标' and business_attribute_name !='日配' and new_cust_flag=1 THEN COALESCE(CAST(win_bid_amount AS DECIMAL(26,6)), 0) ELSE 0 END) AS qt_win_bid_amount, 
   -- 老客
  SUM(CASE WHEN bid_status_name = '中标' and business_attribute_name='日配' and new_cust_flag !=1 THEN 1 ELSE 0 END) AS rp_old_win_bid_cust,
  SUM(CASE WHEN bid_status_name = '中标' and business_attribute_name='日配' and new_cust_flag !=1 THEN COALESCE(CAST(win_bid_amount AS DECIMAL(26,6)), 0) ELSE 0 END) AS rp_ole_win_bid_amount,
  SUM(CASE WHEN bid_status_name = '中标' and business_attribute_name !='日配' and new_cust_flag !=1 THEN 1 ELSE 0 END) AS qt_old_win_bid_cust,
  SUM(CASE WHEN bid_status_name = '中标' and business_attribute_name !='日配' and new_cust_flag !=1 THEN COALESCE(CAST(win_bid_amount AS DECIMAL(26,6)), 0) ELSE 0 END) AS qt_ole_win_bid_amount
FROM
  csx_analyse_tmp_ppt_bid_info a 
WHERE
   sdt=DATE_FORMAT(DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY), '%Y%m%d')	
  ${if(len(province)=0,"","and performance_province_name in ('"+province+"')")}
  and bid_date >= '${sdt}'  and bid_date<='${edt}'
GROUP BY
  performance_region_name,
  performance_province_name 
;
 
  