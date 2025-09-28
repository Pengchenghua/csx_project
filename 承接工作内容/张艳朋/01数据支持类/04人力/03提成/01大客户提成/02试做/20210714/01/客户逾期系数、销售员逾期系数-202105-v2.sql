
-- 昨日、昨日、昨日月1日
--select ${hiveconf:i_sdate_11},${hiveconf:i_sdate_12},${hiveconf:i_sdate_12},${hiveconf:i_sdate_11};
--set i_sdate_1 =date_sub(current_date,1);
--set i_sdate_11 =regexp_replace(date_sub(current_date,1),'-','');
--set i_sdate_12 =regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),0),'-','');

set i_sdate_1 =last_day(add_months(date_sub(current_date,1),-1));
set i_sdate_11 =regexp_replace(last_day(add_months(date_sub(current_date,1),-1)),'-','');	
set i_sdate_12 =regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-1),'-','');					

	
set i_sdate_1 ='2021-05-31';
set i_sdate_11 ='20210531';
set i_sdate_12 ='20210501';


--5月签呈，签呈处理销售员服务管家关系，当月
drop table csx_tmp.tmp_customer_service_manager_info;
create table csx_tmp.tmp_customer_service_manager_info
as  
   select distinct customer_no,service_user_work_no,service_user_name,
    work_no,sales_name,is_part_time_service_manager,
    sales_sale_rate,  --销售员_销售额提成比例
	sales_front_profit_rate,  --销售员_前端毛利提成比例
    service_user_sale_rate,  --服务管家_销售额提成比例
	service_user_front_profit_rate	 --服务管家_前端毛利提成比例
  from csx_dw.report_crm_w_a_customer_service_manager_info 
  --where sdt=${hiveconf:i_sdate_11}
  where sdt='20210617' 
  and customer_no not in(
  '117326','107901','116861','104036','116650','113940','108201','108817','109406','115909','110575','108105','110696',
  '117244','118042','113935','115656','113666','113873','116169','111999','113569','114054','115602','110898','117015',
  '112016','106921','113877','115205','105915','115881','112302','118379','111943','112747','118546','116858','118367',
  '108283','115753','116821','110896','115051','116857','115936','114083','114099','107398','118461','114085','111204',
  '109722','104281','116785','117155','117983','118117','109017','107852','104034','113918','115287','103775','115826',
  '109544','118474','117680','115308','118072','100326','104086','104217','112072','114486','PF1265','104151','104358',
  '103372','105182','106423','106721','112054','102565','102790','103784','103855','105150','105181','111844','112976',
  '112980','117554','102734','107404','102784','111400','102901','112288','112923','113467','103199','104469','104501',
  '112038','115982','115987','116282','PF0365','102202','105355','105886','108067','114275','102647','112071','115236',
  '115253','115471','115490','115565','115919','116014','116019','116311','117544','117911','118670','118717',
  '101916','104549','104612','105446','105882','107946','108739','109377','112871','117324','PF0094','118689'
  )  
union all   select '117326' as customer_no,'' as service_user_work_no,'' as service_user_name,'81054396' as work_no,'黄文君' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '107901' as customer_no,'' as service_user_work_no,'' as service_user_name,'81079217' as work_no,'黄伟宏' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '116861' as customer_no,'XM000001' as service_user_work_no,'彭东京' as service_user_name,'XM000001' as work_no,'彭东京' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '104036' as customer_no,'' as service_user_work_no,'' as service_user_name,'80895349' as work_no,'陈扬平' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '116650' as customer_no,'' as service_user_work_no,'' as service_user_name,'81004682' as work_no,'王能海' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '113940' as customer_no,'81041061' as service_user_work_no,'郭志江' as service_user_name,'签呈未知' as work_no,'虚拟AA' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '108201' as customer_no,'' as service_user_work_no,'' as service_user_name,'81004682' as work_no,'王能海' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '108817' as customer_no,'81027767' as service_user_work_no,'刘昊' as service_user_name,'81027767' as work_no,'刘昊' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '109406' as customer_no,'' as service_user_work_no,'' as service_user_name,'81102813' as work_no,'卢慧' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '115909' as customer_no,'XM000001' as service_user_work_no,'彭东京' as service_user_name,'XM000001' as work_no,'彭东京' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '110575' as customer_no,'' as service_user_work_no,'' as service_user_name,'80939618' as work_no,'陈逸青' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '108105' as customer_no,'' as service_user_work_no,'' as service_user_name,'81010913' as work_no,'刘华伟' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '110696' as customer_no,'' as service_user_work_no,'' as service_user_name,'80961539' as work_no,'张兴烂' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '117244' as customer_no,'80974184' as service_user_work_no,'郭荔丽' as service_user_name,'签呈未知' as work_no,'虚拟AA' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '118042' as customer_no,'' as service_user_work_no,'' as service_user_name,'81054396' as work_no,'黄文君' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '113935' as customer_no,'81041061' as service_user_work_no,'郭志江' as service_user_name,'签呈未知' as work_no,'虚拟AA' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '115656' as customer_no,'80974184' as service_user_work_no,'郭荔丽' as service_user_name,'80974184' as work_no,'郭荔丽' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '113666' as customer_no,'81088296' as service_user_work_no,'陈惠燕' as service_user_name,'81088296' as work_no,'陈惠燕' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '113873' as customer_no,'81041061' as service_user_work_no,'郭志江' as service_user_name,'签呈未知' as work_no,'虚拟AA' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '116169' as customer_no,'' as service_user_work_no,'' as service_user_name,'81004682' as work_no,'王能海' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '111999' as customer_no,'81088296' as service_user_work_no,'陈惠燕' as service_user_name,'81088296' as work_no,'陈惠燕' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '113569' as customer_no,'' as service_user_work_no,'' as service_user_name,'80939618' as work_no,'陈逸青' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '114054' as customer_no,'81088296' as service_user_work_no,'陈慧燕' as service_user_name,'签呈未知' as work_no,'虚拟AA' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '115602' as customer_no,'80974184' as service_user_work_no,'郭荔丽' as service_user_name,'签呈未知' as work_no,'虚拟AA' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '110898' as customer_no,'' as service_user_work_no,'' as service_user_name,'81004682' as work_no,'王能海' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '117015' as customer_no,'' as service_user_work_no,'' as service_user_name,'80939618' as work_no,'陈逸青' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '112016' as customer_no,'81088296' as service_user_work_no,'陈惠燕' as service_user_name,'81088296' as work_no,'陈惠燕' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '106921' as customer_no,'' as service_user_work_no,'' as service_user_name,'81018341' as work_no,'刘继锋' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '113877' as customer_no,'81027767' as service_user_work_no,'刘昊' as service_user_name,'81027767' as work_no,'刘昊' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '115205' as customer_no,'81088296' as service_user_work_no,'陈惠燕' as service_user_name,'81023105' as work_no,'林长茂' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '105915' as customer_no,'' as service_user_work_no,'' as service_user_name,'81004682' as work_no,'王能海' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '115881' as customer_no,'' as service_user_work_no,'' as service_user_name,'81094607' as work_no,'林志高' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '112302' as customer_no,'' as service_user_work_no,'' as service_user_name,'80969261' as work_no,'黄诗偶' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '118379' as customer_no,'' as service_user_work_no,'' as service_user_name,'81094607' as work_no,'林志高' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '111943' as customer_no,'' as service_user_work_no,'' as service_user_name,'81004682' as work_no,'王能海' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '112747' as customer_no,'81088296' as service_user_work_no,'陈惠燕' as service_user_name,'80989132' as work_no,'姚市敏' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '118546' as customer_no,'' as service_user_work_no,'' as service_user_name,'81054396' as work_no,'黄文君' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '116858' as customer_no,'XM000001' as service_user_work_no,'彭东京' as service_user_name,'XM000001' as work_no,'彭东京' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '118367' as customer_no,'81027767' as service_user_work_no,'刘昊' as service_user_name,'81027767' as work_no,'刘昊' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '108283' as customer_no,'' as service_user_work_no,'' as service_user_name,'81094607' as work_no,'林志高' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '115753' as customer_no,'' as service_user_work_no,'' as service_user_name,'81102813' as work_no,'卢慧' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '116821' as customer_no,'' as service_user_work_no,'' as service_user_name,'81079217' as work_no,'黄伟宏' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '110896' as customer_no,'81027767' as service_user_work_no,'刘昊' as service_user_name,'81027767' as work_no,'刘昊' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '115051' as customer_no,'' as service_user_work_no,'' as service_user_name,'81079217' as work_no,'黄伟宏' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '116857' as customer_no,'XM000001' as service_user_work_no,'彭东京' as service_user_name,'XM000001' as work_no,'彭东京' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '115936' as customer_no,'' as service_user_work_no,'' as service_user_name,'80939618' as work_no,'陈逸青' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '114083' as customer_no,'81088296' as service_user_work_no,'陈慧燕' as service_user_name,'签呈未知' as work_no,'虚拟AA' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '114099' as customer_no,'' as service_user_work_no,'' as service_user_name,'81079217' as work_no,'黄伟宏' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '107398' as customer_no,'80974184' as service_user_work_no,'郭荔丽' as service_user_name,'签呈未知' as work_no,'虚拟AA' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '118461' as customer_no,'' as service_user_work_no,'' as service_user_name,'81079217' as work_no,'黄伟宏' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '114085' as customer_no,'81088296' as service_user_work_no,'陈慧燕' as service_user_name,'签呈未知' as work_no,'虚拟AA' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '111204' as customer_no,'' as service_user_work_no,'' as service_user_name,'80939618' as work_no,'陈逸青' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '109722' as customer_no,'' as service_user_work_no,'' as service_user_name,'81079217' as work_no,'黄伟宏' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '104281' as customer_no,'80974184' as service_user_work_no,'郭荔丽' as service_user_name,'签呈未知' as work_no,'虚拟AA' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '116785' as customer_no,'' as service_user_work_no,'' as service_user_name,'80895349' as work_no,'陈扬平' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '117155' as customer_no,'' as service_user_work_no,'' as service_user_name,'签呈未知' as work_no,'虚拟AA' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '117983' as customer_no,'81027767' as service_user_work_no,'刘昊' as service_user_name,'81027767' as work_no,'刘昊' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '118117' as customer_no,'' as service_user_work_no,'' as service_user_name,'80895349' as work_no,'陈扬平' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '109017' as customer_no,'81027767' as service_user_work_no,'刘昊' as service_user_name,'81027767' as work_no,'刘昊' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '107852' as customer_no,'' as service_user_work_no,'' as service_user_name,'81004682' as work_no,'王能海' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '104034' as customer_no,'' as service_user_work_no,'' as service_user_name,'80969261' as work_no,'黄诗偶' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '113918' as customer_no,'81041061' as service_user_work_no,'郭志江' as service_user_name,'签呈未知' as work_no,'虚拟AA' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '115287' as customer_no,'' as service_user_work_no,'' as service_user_name,'80895349' as work_no,'陈扬平' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '103775' as customer_no,'' as service_user_work_no,'' as service_user_name,'81010913' as work_no,'刘华伟' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '115826' as customer_no,'80974184' as service_user_work_no,'郭荔丽' as service_user_name,'签呈未知' as work_no,'虚拟AA' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '109544' as customer_no,'' as service_user_work_no,'' as service_user_name,'81010913' as work_no,'刘华伟' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '118474' as customer_no,'81027767' as service_user_work_no,'刘昊' as service_user_name,'81027767' as work_no,'刘昊' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '117680' as customer_no,'' as service_user_work_no,'' as service_user_name,'80961539' as work_no,'张兴烂' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '115308' as customer_no,'' as service_user_work_no,'' as service_user_name,'80971545' as work_no,'郑毓麟' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '118072' as customer_no,'' as service_user_work_no,'' as service_user_name,'81062154' as work_no,'钟子洋' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '100326' as customer_no,'' as service_user_work_no,'' as service_user_name,'80012225' as work_no,'林挺波' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '104086' as customer_no,'81020517' as service_user_work_no,'刘晓晴' as service_user_name,'80012225' as work_no,'林挺波' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '104217' as customer_no,'' as service_user_work_no,'' as service_user_name,'80012225' as work_no,'林挺波' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '112072' as customer_no,'81020517' as service_user_work_no,'刘晓晴' as service_user_name,'80012225' as work_no,'林挺波' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '114486' as customer_no,'' as service_user_work_no,'' as service_user_name,'80012225' as work_no,'林挺波' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select 'PF1265' as customer_no,'81020517' as service_user_work_no,'刘晓晴' as service_user_name,'80012225' as work_no,'林挺波' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '104151' as customer_no,'' as service_user_work_no,'' as service_user_name,'80895351' as work_no,'李航' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '104358' as customer_no,'' as service_user_work_no,'' as service_user_name,'80895351' as work_no,'李航' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '103372' as customer_no,'81080592' as service_user_work_no,'叶铭菲' as service_user_name,'81080592' as work_no,'叶铭菲' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '105182' as customer_no,'' as service_user_work_no,'' as service_user_name,'80895351' as work_no,'李航' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '106423' as customer_no,'' as service_user_work_no,'' as service_user_name,'81080592' as work_no,'叶铭菲' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '106721' as customer_no,'' as service_user_work_no,'' as service_user_name,'80895351' as work_no,'李航' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '112054' as customer_no,'' as service_user_work_no,'' as service_user_name,'80895351' as work_no,'李航' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '102565' as customer_no,'' as service_user_work_no,'' as service_user_name,'80960666' as work_no,'冯桂华' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '102790' as customer_no,'' as service_user_work_no,'' as service_user_name,'80960666' as work_no,'冯桂华' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '103784' as customer_no,'' as service_user_work_no,'' as service_user_name,'80960666' as work_no,'冯桂华' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '103855' as customer_no,'' as service_user_work_no,'' as service_user_name,'80960666' as work_no,'冯桂华' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '105150' as customer_no,'' as service_user_work_no,'' as service_user_name,'80973546' as work_no,'王博' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '105181' as customer_no,'' as service_user_work_no,'' as service_user_name,'81080592' as work_no,'叶铭菲' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '111844' as customer_no,'' as service_user_work_no,'' as service_user_name,'80960666' as work_no,'冯桂华' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '112976' as customer_no,'' as service_user_work_no,'' as service_user_name,'80960666' as work_no,'冯桂华' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '112980' as customer_no,'' as service_user_work_no,'' as service_user_name,'80960666' as work_no,'冯桂华' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '117554' as customer_no,'' as service_user_work_no,'' as service_user_name,'80960666' as work_no,'冯桂华' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '102734' as customer_no,'' as service_user_work_no,'' as service_user_name,'80980614' as work_no,'池春梅' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '107404' as customer_no,'81080592' as service_user_work_no,'叶铭菲' as service_user_name,'80973546' as work_no,'王博' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '102784' as customer_no,'' as service_user_work_no,'' as service_user_name,'80980614' as work_no,'池春梅' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '111400' as customer_no,'' as service_user_work_no,'' as service_user_name,'80973546' as work_no,'王博' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '102901' as customer_no,'' as service_user_work_no,'' as service_user_name,'80980614' as work_no,'池春梅' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '112288' as customer_no,'' as service_user_work_no,'' as service_user_name,'80980614' as work_no,'池春梅' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '112923' as customer_no,'' as service_user_work_no,'' as service_user_name,'80980614' as work_no,'池春梅' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '113467' as customer_no,'' as service_user_work_no,'' as service_user_name,'80980614' as work_no,'池春梅' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '103199' as customer_no,'' as service_user_work_no,'' as service_user_name,'80980614' as work_no,'池春梅' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '104469' as customer_no,'' as service_user_work_no,'' as service_user_name,'80980614' as work_no,'池春梅' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '104501' as customer_no,'' as service_user_work_no,'' as service_user_name,'80980614' as work_no,'池春梅' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '112038' as customer_no,'' as service_user_work_no,'' as service_user_name,'80980614' as work_no,'池春梅' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '115982' as customer_no,'' as service_user_work_no,'' as service_user_name,'80980614' as work_no,'池春梅' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '115987' as customer_no,'' as service_user_work_no,'' as service_user_name,'80980614' as work_no,'池春梅' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '116282' as customer_no,'' as service_user_work_no,'' as service_user_name,'80980614' as work_no,'池春梅' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select 'PF0365' as customer_no,'' as service_user_work_no,'' as service_user_name,'80980614' as work_no,'池春梅' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '102202' as customer_no,'' as service_user_work_no,'' as service_user_name,'80972915' as work_no,'陈伟豪' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '105355' as customer_no,'' as service_user_work_no,'' as service_user_name,'80972915' as work_no,'陈伟豪' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '105886' as customer_no,'' as service_user_work_no,'' as service_user_name,'80972915' as work_no,'陈伟豪' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '108067' as customer_no,'' as service_user_work_no,'' as service_user_name,'80972915' as work_no,'陈伟豪' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '114275' as customer_no,'' as service_user_work_no,'' as service_user_name,'80972915' as work_no,'陈伟豪' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '102647' as customer_no,'' as service_user_work_no,'' as service_user_name,'80972915' as work_no,'陈伟豪' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '112071' as customer_no,'' as service_user_work_no,'' as service_user_name,'80972915' as work_no,'陈伟豪' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '115236' as customer_no,'' as service_user_work_no,'' as service_user_name,'81049168' as work_no,'莫昌川' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '115253' as customer_no,'' as service_user_work_no,'' as service_user_name,'81049168' as work_no,'莫昌川' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '115471' as customer_no,'' as service_user_work_no,'' as service_user_name,'81049168' as work_no,'莫昌川' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '115490' as customer_no,'' as service_user_work_no,'' as service_user_name,'81049168' as work_no,'莫昌川' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '115565' as customer_no,'' as service_user_work_no,'' as service_user_name,'81049168' as work_no,'莫昌川' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '115919' as customer_no,'' as service_user_work_no,'' as service_user_name,'81049168' as work_no,'莫昌川' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '116014' as customer_no,'' as service_user_work_no,'' as service_user_name,'81049168' as work_no,'莫昌川' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '116019' as customer_no,'' as service_user_work_no,'' as service_user_name,'81049168' as work_no,'莫昌川' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '116311' as customer_no,'' as service_user_work_no,'' as service_user_name,'81049168' as work_no,'莫昌川' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '117544' as customer_no,'' as service_user_work_no,'' as service_user_name,'81049168' as work_no,'莫昌川' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '117911' as customer_no,'' as service_user_work_no,'' as service_user_name,'81049168' as work_no,'莫昌川' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '118670' as customer_no,'' as service_user_work_no,'' as service_user_name,'81049168' as work_no,'莫昌川' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '118717' as customer_no,'' as service_user_work_no,'' as service_user_name,'81049168' as work_no,'莫昌川' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '101916' as customer_no,'签呈未知' as service_user_work_no,'张欢欢' as service_user_name,'80924363' as work_no,'胡康灿' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '104549' as customer_no,'' as service_user_work_no,'' as service_user_name,'80929710' as work_no,'王坚' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '104612' as customer_no,'81095855' as service_user_work_no,'陈琳' as service_user_name,'81095855' as work_no,'陈琳' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '105446' as customer_no,'' as service_user_work_no,'' as service_user_name,'80924363' as work_no,'胡康灿' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '105882' as customer_no,'81095855' as service_user_work_no,'陈琳' as service_user_name,'81095855' as work_no,'陈琳' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '107946' as customer_no,'81095855' as service_user_work_no,'陈琳' as service_user_name,'81095855' as work_no,'陈琳' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '108739' as customer_no,'81095855' as service_user_work_no,'陈琳' as service_user_name,'81095855' as work_no,'陈琳' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '109377' as customer_no,'81095855' as service_user_work_no,'陈琳' as service_user_name,'81095855' as work_no,'陈琳' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '112871' as customer_no,'' as service_user_work_no,'' as service_user_name,'80929710' as work_no,'王坚' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '117324' as customer_no,'' as service_user_work_no,'' as service_user_name,'80912701' as work_no,'蓝梦玲' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select 'PF0094' as customer_no,'81095855' as service_user_work_no,'陈琳' as service_user_name,'81095855' as work_no,'陈琳' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '118689' as customer_no,'81079631' as service_user_work_no,'李燕玲' as service_user_name,'' as work_no,'' as sales_name,'否' as is_part_time_service_manager,0 as sales_sale_rate,0 as sales_front_profit_rate,0.1 as service_user_sale_rate,0 as service_user_front_profit_rate
;
--set i_sdate                = '2020-11-30';
--set i_date                 =date_add(${hiveconf:i_sdate},1);

--订单应收金额、逾期日期、逾期天数
drop table csx_tmp.tmp_cust_order_overdue_dtl_0;
create table csx_tmp.tmp_cust_order_overdue_dtl_0
as
select
	c.channel_name,
	c.channel_code,	
	a.order_no,	-- 来源单号
	a.customer_no,	-- 客户编码
	c.customer_name,	-- 客户名称
	a.appoint_place_code,  --履约地点编码
	a.company_code,	-- 签约公司编码
	b.company_name,	-- 签约公司名称
	regexp_replace(substr(a.happen_date,1,10),'-','') happen_date,	-- 发生时间		
	regexp_replace(substr(a.overdue_date,1,10),'-','') overdue_date,	-- 逾期时间	
	a.source_statement_amount,	-- 源单据对账金额
	a.money_back_status,	-- 回款状态
	a.unpaid_amount receivable_amount,	-- 应收金额
	a.account_period_code,	--账期编码 
	a.account_period_name,	--账期名称 
	a.account_period_val,	--账期值
	a.beginning_mark,	--是否期初
	a.bad_debt_amount,	
	a.over_days,	-- 逾期天数
	if(a.account_period_code like 'Y%', if(a.account_period_val = 31, 45, a.account_period_val + 15), a.account_period_val) as acc_val_calculation_factor,	-- 标准账期
	${hiveconf:i_sdate_11} sdt
from
	(
	select 
		source_bill_no as order_no,	-- 来源单号
		customer_code as customer_no,	-- 客户编码
		--customer_name,	-- 客户名称
		appoint_place_code,  --履约地点编码
		company_code,	-- 签约公司编码
		--company_name,	-- 签约公司名称
		happen_date,	-- 发生时间		
		overdue_date,	-- 逾期时间	
		source_statement_amount,	-- 源单据对账金额
		money_back_status,	-- 回款状态
		unpaid_amount,	-- 未回款金额
		account_period_code,	--账期编码 
		account_period_name,	--账期名称 
		account_period_val,	--账期值
		'否' as beginning_mark,	--是否期初
		bad_debt_amount,
		if((money_back_status<>'ALL' or (datediff(${hiveconf:i_sdate_1}, overdue_date)+1)>=1),datediff(${hiveconf:i_sdate_1}, overdue_date)+1,0) as over_days	-- 逾期天数
	--from csx_ods.source_sss_r_d_source_bill
	from csx_dw.dwd_sss_r_d_sale_order_statement_detail_20201116  --销售单对账
	where sdt=${hiveconf:i_sdate_11}
	and date(happen_date)<=${hiveconf:i_sdate_1}
	--and beginning_mark='1'  	-- 期初标识 0-是 1-否
	--and money_back_status<>'ALL'
	union all
	select 
		id as order_no,	-- 来源单号
		customer_code as customer_no,	-- 客户编码
		--customer_name,	-- 客户名称
		'' appoint_place_code,  --履约地点编码
		company_code,	-- 签约公司编码
		--company_name,	-- 签约公司名称		
		date_sub(from_unixtime(unix_timestamp(overdue_date,'yyyy-MM-dd hh:mm:ss')),coalesce(account_period_val,0)) as happen_date,	-- 发生时间		
		overdue_date,	-- 逾期时间	
		beginning_amount source_statement_amount,	-- 源单据对账金额
		money_back_status,	-- 回款状态
		unpaid_amount,	-- 未回款金额
		account_period_code,	--账期编码 
		account_period_name,	--账期名称 
		account_period_val,	--账期值
		'是' as beginning_mark,	--是否期初	
		bad_debt_amount,
		if((money_back_status<>'ALL' or (datediff(${hiveconf:i_sdate_1}, overdue_date)+1)>=1),datediff(${hiveconf:i_sdate_1}, overdue_date)+1,0) as over_days	-- 逾期天数
	--from csx_ods.source_sss_r_a_beginning_receivable
	from csx_dw.dwd_sss_r_a_beginning_receivable_20201116 
	where sdt=${hiveconf:i_sdate_11}
	--and money_back_status<>'ALL'
	)a
left join 
	(
	select 
		code as company_code,
		name as company_name 
	from csx_dw.dws_basic_w_a_company_code 
	where sdt = 'current'
	)b on a.company_code = b.company_code
left join
	(
	select 
		customer_no,
		customer_name,
		channel_name,
		channel_code 
	from csx_dw.dws_crm_w_a_customer 
	--where sdt=${hiveconf:i_sdate_11} 
	where sdt='20210617'
	)c on a.customer_no=c.customer_no;


--应收金额、逾期日期、逾期天数
--签呈，部分客户历史订单逾期剔除
drop table csx_tmp.tmp_cust_order_overdue_dtl_1;
create table csx_tmp.tmp_cust_order_overdue_dtl_1
as
select *
from csx_tmp.tmp_cust_order_overdue_dtl_0
where customer_no not in('105235','105557')
and customer_no not in('104901','115852','105381')
and customer_no not in('104666','107577')
and customer_no not in('111506','115156')
--4月签呈，20210331前发生的历史订单逾期剔除，4-8月处理；
union all
select *
from csx_tmp.tmp_cust_order_overdue_dtl_0
where customer_no in('105235','105557')
and happen_date>'20210331'
--4月签呈，20210430前发生的历史订单逾期剔除，4-8月处理；其中'115156','105381'4-6月处理，'115520','115458'本月处理
union all
select *
from csx_tmp.tmp_cust_order_overdue_dtl_0
where customer_no in('104901','115852','105381')
and happen_date>'20210430'
--5月签呈，逾期日期20210630前的历史订单逾期剔除，5-7月处理；
union all
select *
from csx_tmp.tmp_cust_order_overdue_dtl_0
where customer_no in('104666','107577')
and happen_date>'20210630'
--5月签呈，逾期日期20210731前的历史订单逾期剔除，5-7月处理；
union all
select *
from csx_tmp.tmp_cust_order_overdue_dtl_0
where customer_no in('111506','115156')
and happen_date>'20210731';




-- 查询结果集
--计算逾期系数
insert overwrite directory '/tmp/raoyanhua/yuqi_dakehu' row format delimited fields terminated by '\t'
select 
	a.channel_name,	-- 渠道
	b.sales_province_name,	-- 省区
	a.customer_no,	-- 客户编码
	a.customer_name,	-- 客户名称
	b.work_no,	-- 销售员工号
	b.sales_name,	-- 销售员
	d.service_user_work_no,d.service_user_name,d.is_part_time_service_manager,
	c.account_period_code,	-- 账期编码
	if(c.account_period_code like 'Y%',if(c.account_period_val=31,45,c.account_period_val+15),c.account_period_val) account_period_val,	-- 帐期天数
	c.account_period_name,	-- 账期名称
	a.company_code,	-- 公司代码
	a.company_name,	-- 公司名称
	case when a.receivable_amount>=0 then a.receivable_amount else 0 end receivable_amount,	-- 应收金额
	case when a.over_amt>=0 and a.receivable_amount>0 then a.over_amt else 0 end over_amt,	-- 逾期金额
	case when a.over_amt_s>=0 and a.receivable_amount>0 then a.over_amt_s else 0 end over_amt_s,	-- 逾期金额*逾期天数
	case when a.receivable_amount_s>=0 and a.receivable_amount>0 then a.receivable_amount_s else 0 end receivable_amount_s,	-- 应收金额*帐期天数	
    coalesce(round(case when coalesce(case when a.receivable_amount>=0 then a.receivable_amount else 0 end, 0) <= 1 then 0  
						else (coalesce(case when a.over_amt_s>=0 and a.receivable_amount>0 then a.over_amt_s else 0 end,0)
						/(case when a.receivable_amount_s>=0 and a.receivable_amount>0 then a.receivable_amount_s else 0 end)) end
		  , 6),0) over_rate 	-- 逾期系数
		    
from
	(select
		channel_name,
		customer_no,
		customer_name,
		--account_period_code,
		--COALESCE(account_period_val,0) account_period_val,
		--account_period_name,
		company_code,
		company_name,
		sum(receivable_amount) as receivable_amount,
		sum(case when over_days>=1 then receivable_amount else 0 end ) as over_amt,
		sum(case when over_days>=1 then receivable_amount*over_days else 0 end) as over_amt_s,
		sum(case when receivable_amount>=0 then receivable_amount else 0 end* if(COALESCE(account_period_val,0)=0,1,acc_val_calculation_factor)) as receivable_amount_s
	from csx_tmp.tmp_cust_order_overdue_dtl  
	where channel_name = '大客户' and sdt = ${hiveconf:i_sdate_11} 
	--签呈客户不考核，不算提成,因此不算逾期 2021年3月签呈取消剔除103717
	and customer_no not in('111118','102755','104023','105673','104402')
	and customer_no not in('107338','104123','102629','104526','106375','106380','106335','107268','104296','108391','108390','108072','108503')
	--签呈客户仅4月不考核，不算提成，4-6月不算逾期
	--and customer_no not in('PF0320','105177')
	--5月签呈客户5月剔除逾期，前者剔除逾期，后者仅5月剔除逾期
	and customer_no not in('103883','103167','105673','104352','104662','104514','104746','104172')
	--and customer_no not in('104055','106463')
	--6月签呈客户仅6月剔除逾期，其中 106463 从8月开始不剔除，106765 从12月开始不剔除，105240一直剔除
	--and customer_no not in('105157','107570','106905','104484','109382','106463','106765')
	--and customer_no not in('106463','106765')
	and customer_no not in('106765')
	and customer_no not in('105240')
	--678共3个月因财务对账不考核逾期
	--and customer_no not in('105527', '104445', '102202','100563','PF1206','102751','104775','103309','104116','PF0424','103183','102890',
	--						'102890','PF0320','105618','103320','104725','PF0094','103374','103772','PF1205','103094','104430','104519',
	--						'104478','103876','103782','104335','102790')
	--7月签呈，7、8、9共3个月不算逾期
	--and customer_no not in('106626','106997','111383','105169','105672','106652','105543','110679','105254','108773')
	--7月签呈，第1行仅7月剔除，第2行8月看情况，第3行历史问题已断约不考核逾期，第4行仅7月剔除逾期且不算提成
	----and customer_no not in('105493','105758','105832','105994','107015','111417')
							--'104677','111865','105528','105381','104867','107986','105717',
							--'107065','108096','108452','107851','106811','105572',
							--'105493','105758','105832','105994','107015','111417',
							--'113108','113067','110656','111837','111296','105202')
	--7月坏账签呈中，105601客户8月逾期剔除
	----and customer_no not in('105601')
	--8月签呈，其中107181客户8-9月剔除逾期，其他仅剔除8月
	--and customer_no not in('107181','105669','111905','110661','110677','110682','107459')
	----and customer_no not in('107181')
	--9月签呈 四川 算到业务代理人，每月剔除逾期和销售
	and customer_no not in('104179','112092')
	--9月签呈 安徽 已断约每月剔除? '106997'、'105169'9月有销售
	and customer_no not in('104352','105493','105758','105832','105994','107015','106626','106997','111383','105169','106652','105254','108773')
	--9月签呈 重庆 合伙人客户，9月剔除逾期和销售
	--and customer_no not in('114265','114248','114401','111933','113080','113392')
	--9月签呈 重庆 剔除9月逾期，其中'109484'剔除9月的逾期和销售
	--and customer_no not in('109484','107790','110664')	
	--9月签呈 江苏、贵州、四川 剔除9月逾期，其中'104268'10月也剔除
	--and customer_no not in('104268')
	--and customer_no not in('107621','109342','109403','109671','113154','113486','113762','114145','114482','111553','111559','111589',
	--						'111617','111618','111621','111622','111630','111632','111641','111643','113934','108797','104268','103997')
	--9月签呈 福建 SAP有逾期，签呈无逾期，反馈说省区财务核对没问题，需进一步确认 9月先剔除逾期
	--and customer_no not in('102890','102890','106526','106587','106697','107305','108333','109401','110670','PF0320')	
	--10月签呈 当月剔除逾期系数
	--and customer_no not in('105090','111935','110664','109293','111534','111810','112199','112201','112361','112874')
	--11月签呈 当月剔除逾期系数,其中 山西省 109461 只9-10月算到业务代理人，每月剔除逾期和销售
	--and customer_no not in('109461','112437','112176','104268')
	--and customer_no not in('109322','114045','112635','113643','107980')
	--12月同时有城市服务商和其他业务类型业绩客户，剔除当月逾期系数
	--and customer_no not in('102894','103175','104192','106214','106298','106299','106380','107268','109509','110248',
	--		'110518','110930','111427','111500','111853','113281','113936','113992','114265','114997')
	--12月签呈 当月剔除逾期系数,其中'113744','113824','113826','113831'剔除提成
	--and customer_no not in('107882','106469','108800','112180','111333','113744','113824','113826','113831')
	--1月签呈 当月剔除逾期系数
    --and customer_no not in('111333','114510')
	and customer_no not in('111333')
	--2月签呈 当月剔除逾期系数，当月剔除、每月剔除、不算提成
	--and customer_no not in('116529','111506','111623','112326','109484','105302','115206')
	and customer_no not in('104532')
	--and customer_no not in('116015','115721','115721','116877','116883','116015','116556','116826','103253','103284','103296',
    --                       '103297','103304','103306','103311','104818','104828','104829','104835','105113','106283','106284',
    --                       '106298','106299','106301','106306','106307','106308','106309','106320','106321','106325','106326',
    --                       '106330','102844','114054','109000','114083','114085','115909','115971','116215',
	--					   '111100','116058','116188','105601')
    --3月签呈 当月剔除逾期系数;3-4月剔除逾期系数;每月剔除逾期系数;剔除逾期系数不算提成(其中'PF0065','112574','106782'3-5月不发提成);剔除逾期系数.不算提成.每月*2
	--and customer_no not in('111506','108800','112180')
	and customer_no not in('112129')
	and customer_no not in('114904','115313','115314','115325','115326','115391')
	and customer_no not in('PF0065','112574','106782')  --'116957','116629','116215'仅3月
	and customer_no not in('115721','116877','116883','116015','116556','116826')
	and customer_no not in('103253','103284','103296','103297','103304','103306','103311','104818','104828','104829','104835',
                           '105113','106283','106284','106298','106299','106301','106306','106307','106308','106309','106320',
                           '106321','106325','106326','106330','104609')
	--4月签呈 当月剔除逾期系数;	剔除逾期系数，不算提成
    --and customer_no not in('111506','105685','113744','116085','103369')
    --and customer_no not in('114265','117412','116957')
	--4月签呈 每月处理：剔除逾期系数，每月剔除;剔除逾期系数，不算提成，每月处理
    and customer_no not in('112045','115393','112248','104817','105601','104381','105304','105714','116099','104445','108127')
    and customer_no not in('102844','117940')	
	--5月签呈 当月剔除逾期系数，不算提成
	and customer_no not in('116957','106805','106228')
	--5月签呈 当月剔除逾期系数，不算提成，每月
	and customer_no not in('105280','106287','106427','110930','111100','112675','115202','115631')	
    --5月签呈 当月剔除逾期系数 '106052'5月系统错误
	and customer_no not in('107661','108088','113816','PF0424','118274','114391','108824','103369','114718','114785','115620','117305','115017','113609','106052')
	group by channel_name,customer_no,customer_name,company_code,company_name

	--4月签呈 '118689'系统中为业务代理人，但需要人为计算销售员大客户提成,每月处理
	union all
	select
		channel_name,
		customer_no,
		customer_name,
		--account_period_code,
		--COALESCE(account_period_val,0) account_period_val,
		--account_period_name,
		company_code,
		company_name,
		sum(receivable_amount) as receivable_amount,
		sum(case when over_days>=1 then receivable_amount else 0 end ) as over_amt,
		sum(case when over_days>=1 then receivable_amount*over_days else 0 end) as over_amt_s,
		sum(case when receivable_amount>=0 then receivable_amount else 0 end* if(COALESCE(account_period_val,0)=0,1,acc_val_calculation_factor)) as receivable_amount_s
	from csx_tmp.tmp_cust_order_overdue_dtl  
	where customer_no = '118689' and sdt = ${hiveconf:i_sdate_11} 
	group by channel_name,customer_no,customer_name,company_code,company_name
	)a
left join
	(select
		customer_no,
		company_code,
		payment_terms account_period_code,
		case when payment_terms like 'Y%' then concat('月结',COALESCE(cast(payment_days as int),0)) 
			 else concat('票到',COALESCE(cast(payment_days as int),0)) end account_period_name,
		COALESCE(cast(payment_days as int),0) account_period_val
	from csx_dw.dws_crm_w_a_customer_company a
	where sdt='current'
	and customer_no<>''
	)c on (a.customer_no=c.customer_no and a.company_code=c.company_code)
--剔除业务代理与内购客户
join		 ----渠道编号-1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 7.企业购 8.其他 9.业务代理
  (
    select * from csx_dw.dws_crm_w_a_customer 
	--4月签呈 '118689'系统中为业务代理人，但需要人为计算销售员大客户提成,每月处理
    --where sdt=${hiveconf:i_sdate_11} 
	where sdt='20210617'
	and (channel_code in('1','7','8') or customer_no='118689') and (customer_name not like '%内%购%' and customer_name not like '%临保%')	
  )b on b.customer_no=a.customer_no  
--join (select * from csx_dw.dws_crm_w_a_customer where sdt=${hiveconf:i_sdate_11} and dev_source_code not in('2','4')) b on b.customer_no=a.customer_no  --剔除业务代理与内购客户
--剔除当月有城市服务商与批发内购业绩的客户逾期系数
left join 		--业务类型编码(1.日配单 2.福利单 3.批发内购 4.城市服务商 5.省区大宗 6.BBC)  --剔除内购客户、城市服务商
  (
	select distinct customer_no 
	from csx_dw.dws_sale_r_d_detail 
	where sdt>=${hiveconf:i_sdate_12} 
	and sdt<=${hiveconf:i_sdate_11} 
	and business_type_code in('3','4')
	--5月签呈，不剔除城市服务商2.0，按大客户提成方案计算
	and customer_no not in(
        '115023','112906','112857','116024','113211','112875','111930','114859','117558','119047','118262',
		'114853','118731','116061','118259','112410','116432','118759','116056','113829','116032','115681',
		'107890','116185','115589','118219','115450','118264','118509','117817','111964','118472','118870',
		'111473','113634','115904','118221','117989','108713','115335','116038','114841','117889','119034',
		'117393','118574','115860','114485','117496','114075','109363','116055','113735','117961','111734',
		'116027','116959','117548','117458','112207','109357','114940','108726','109460','117610')
  )e on e.customer_no=a.customer_no
----关联服务管家
--left join		
--  (  
--      select customer_no,
--	 concat_ws(',', collect_list(service_user_work_no)) as service_user_work_no,
--	 concat_ws(',', collect_list(service_user_name)) as service_user_name
--	from 
--	  (select distinct customer_no,service_user_work_no,service_user_name
--	  from csx_dw.dws_crm_w_a_customer_sales_link 
--      where sdt=${hiveconf:i_sdate_11} 
--	  )a
--	group by customer_no
--  )d on d.customer_no=a.customer_no	
--关联服务管家 5月计算用，客户对应销售员与服务管家
left join		
  (  
  select distinct customer_no,
    service_user_work_no,service_user_name,	  
    work_no,sales_name,is_part_time_service_manager
  from csx_tmp.tmp_customer_service_manager_info	
  --from csx_dw.report_crm_w_a_customer_service_manager_info 
  --where sdt=${hiveconf:i_sdate_11}
  --where sdt='20210617' 
  )d on d.customer_no=a.customer_no	  
where e.customer_no is null
;
	
	

--客户逾期系数
drop table csx_tmp.temp_cust_over_rate;
create table csx_tmp.temp_cust_over_rate
as
select 
	channel_name,	-- 渠道
	customer_no,	-- 客户编码
	customer_name,	-- 客户名称
	sum(case when receivable_amount>=0 then receivable_amount else 0 end) receivable_amount,	-- 应收金额
	sum(case when over_amt>=0 and receivable_amount>0 then over_amt else 0 end) over_amt,	-- 逾期金额
	--sum(case when over_amt>=0 then over_amt_s else 0 end) over_amt_s,	-- 逾期金额*逾期天数
	--sum(case when receivable_amount>=0 then receivable_amount_s else 0 end) receivable_amount_s,	-- 应收金额*帐期天数
	sum(case when over_amt_s>=0 and receivable_amount>0 then over_amt_s else 0 end) over_amt_s,	-- 逾期金额*逾期天数
	sum(case when receivable_amount_s>=0 and receivable_amount>0 then receivable_amount_s else 0 end) receivable_amount_s,	-- 应收金额*帐期天数	
    coalesce(round(case  when coalesce(SUM(case when receivable_amount>=0 then receivable_amount else 0 end), 0) <= 1 then 0  
				else coalesce(sum(case when over_amt_s>=0 and receivable_amount>0 then over_amt_s else 0 end), 0)
				/sum(case when receivable_amount_s>=0 and receivable_amount>0 then receivable_amount_s else 0 end) end
		  , 6),0) over_rate 	-- 逾期系数
from
	(select
		channel_name,
		customer_no,
		customer_name,
		--account_period_code,
		--COALESCE(account_period_val,0) account_period_val,
		--account_period_name,
		company_code,
		company_name ,
		sum(receivable_amount) as receivable_amount,
		sum(case when over_days>=1 then receivable_amount else 0 end ) as over_amt,
		sum(case when over_days>=1 then receivable_amount*over_days else 0 end) as over_amt_s,
		sum(case when receivable_amount>=0 then receivable_amount else 0 end* if(COALESCE(account_period_val,0)=0,1,acc_val_calculation_factor)) as receivable_amount_s				
	from csx_tmp.tmp_cust_order_overdue_dtl a 
	where channel_name = '大客户' and sdt = ${hiveconf:i_sdate_11}
	--签呈客户不考核，不算提成,因此不算逾期 2021年3月签呈取消剔除103717
	and customer_no not in('111118','102755','104023','105673','104402')
	and customer_no not in('107338','104123','102629','104526','106375','106380','106335','107268','104296','108391','108390','108072','108503')
	--签呈客户仅4月不考核，不算提成，4-6月不算逾期
	--and customer_no not in('PF0320','105177')
	--5月签呈客户5月剔除逾期，前者剔除逾期，后者仅5月剔除逾期
	and customer_no not in('103883','103167','105673','104352','104662','104514','104746','104172')
	--and customer_no not in('104055','106463')
	--6月签呈客户仅6月剔除逾期，其中 106463 从8月开始不剔除，106765 从12月开始不剔除，105240一直剔除
	--and customer_no not in('105157','107570','106905','104484','109382','106463','106765')
	--and customer_no not in('106463','106765')
	and customer_no not in('106765')
	and customer_no not in('105240')
	--678共3个月因财务对账不考核逾期
	--and customer_no not in('105527', '104445', '102202','100563','PF1206','102751','104775','103309','104116','PF0424','103183','102890',
	--						'102890','PF0320','105618','103320','104725','PF0094','103374','103772','PF1205','103094','104430','104519',
	--						'104478','103876','103782','104335','102790')
	--7月签呈，7、8、9共3个月不算逾期
	--and customer_no not in('106626','106997','111383','105169','105672','106652','105543','110679','105254','108773')
	--7月签呈，第1行仅7月剔除，第2行8月看情况，第3行历史问题已断约不考核逾期，第4行仅7月剔除逾期且不算提成
	----and customer_no not in('105493','105758','105832','105994','107015','111417')
							--'104677','111865','105528','105381','104867','107986','105717',
							--'107065','108096','108452','107851','106811','105572',
							--'105493','105758','105832','105994','107015','111417',
							--'113108','113067','110656','111837','111296','105202')
	--7月坏账签呈中，105601客户8月逾期剔除
	----and customer_no not in('105601')
	--8月签呈，其中107181客户8-9月剔除逾期，其他仅剔除8月
	--and customer_no not in('107181','105669','111905','110661','110677','110682','107459')
	----and customer_no not in('107181')
	--9月签呈 四川 算到业务代理人，每月剔除逾期和销售
	and customer_no not in('104179','112092')
	--9月签呈 安徽 已断约每月剔除? '106997'、'105169'9月有销售
	and customer_no not in('104352','105493','105758','105832','105994','107015','106626','106997','111383','105169','106652','105254','108773')
	--9月签呈 重庆 合伙人客户，9月剔除逾期和销售
	--and customer_no not in('114265','114248','114401','111933','113080','113392')
	--9月签呈 重庆 剔除9月逾期，其中'109484'剔除9月的逾期和销售
	--and customer_no not in('109484','107790','110664')	
	--9月签呈 江苏、贵州、四川 剔除9月逾期，其中'104268'10月也剔除
	--and customer_no not in('104268')
	--and customer_no not in('107621','109342','109403','109671','113154','113486','113762','114145','114482','111553','111559','111589',
	--						'111617','111618','111621','111622','111630','111632','111641','111643','113934','108797','104268','103997')
	--9月签呈 福建 SAP有逾期，签呈无逾期，反馈说省区财务核对没问题，需进一步确认 9月先剔除逾期
	--and customer_no not in('102890','102890','106526','106587','106697','107305','108333','109401','110670','PF0320')	
	--10月签呈 当月剔除逾期系数
	--and customer_no not in('105090','111935','110664','109293','111534','111810','112199','112201','112361','112874')
	--11月签呈 当月剔除逾期系数,其中 山西省 109461 只9-10月算到业务代理人，每月剔除逾期和销售
	--and customer_no not in('109461','112437','112176','104268')
	--and customer_no not in('109322','114045','112635','113643','107980')
	--12月同时有城市服务商和其他业务类型业绩客户，剔除当月逾期系数
	--and customer_no not in('102894','103175','104192','106214','106298','106299','106380','107268','109509','110248',
	--		'110518','110930','111427','111500','111853','113281','113936','113992','114265','114997')
	--12月签呈 当月剔除逾期系数,其中'113744','113824','113826','113831'剔除提成
	--and customer_no not in('107882','106469','108800','112180','111333','113744','113824','113826','113831')
	--1月签呈 当月剔除逾期系数
    --and customer_no not in('111333','114510')
	and customer_no not in('111333')
	--2月签呈 当月剔除逾期系数，当月剔除、每月剔除、不算提成
	--and customer_no not in('116529','111506','111623','112326','109484','105302','115206')
	and customer_no not in('104532')
	--and customer_no not in('116015','115721','115721','116877','116883','116015','116556','116826','103253','103284','103296',
    --                       '103297','103304','103306','103311','104818','104828','104829','104835','105113','106283','106284',
    --                       '106298','106299','106301','106306','106307','106308','106309','106320','106321','106325','106326',
    --                       '106330','102844','114054','109000','114083','114085','115909','115971','116215',
	--					   '111100','116058','116188','105601')
    --3月签呈 当月剔除逾期系数;3-4月剔除逾期系数;每月剔除逾期系数;剔除逾期系数不算提成(其中'PF0065','112574','106782'3-5月不发提成);剔除逾期系数.不算提成.每月*2
	--and customer_no not in('111506','108800','112180')
	--and customer_no not in('112129')
	and customer_no not in('114904','115313','115314','115325','115326','115391')
	and customer_no not in('PF0065','112574','106782')  --'116957','116629','116215'仅3月
	and customer_no not in('115721','116877','116883','116015','116556','116826')
	and customer_no not in('103253','103284','103296','103297','103304','103306','103311','104818','104828','104829','104835',
                           '105113','106283','106284','106298','106299','106301','106306','106307','106308','106309','106320',
                           '106321','106325','106326','106330','104609')
	--4月签呈 当月剔除逾期系数;	剔除逾期系数，不算提成
    --and customer_no not in('111506','105685','113744','116085','103369')
    --and customer_no not in('114265','117412','116957')
	--4月签呈 每月处理：剔除逾期系数，每月剔除;剔除逾期系数，不算提成，每月处理
    and customer_no not in('112045','115393','112248','104817','105601','104381','105304','105714','116099','104445','108127')
    and customer_no not in('102844','117940')	
	--5月签呈 当月剔除逾期系数，不算提成
	and customer_no not in('116957','106805','106228')	
	--5月签呈 当月剔除逾期系数，不算提成，每月
	and customer_no not in('105280','106287','106427','110930','111100','112675','115202','115631')		
    --5月签呈 当月剔除逾期系数 '106052'5月系统错误
	and customer_no not in('107661','108088','113816','PF0424','118274','114391','108824','103369','114718','114785','115620','117305','115017','113609','106052')	
	group by channel_name,customer_no,customer_name,company_code,company_name)a	
group by channel_name,customer_no,customer_name;



--销售员逾期系数
drop table csx_tmp.temp_salesname_over_rate_0;
create table csx_tmp.temp_salesname_over_rate_0
as
select 
	a.channel_name,	-- 渠道
	b.work_no,	-- 销售员工号
	b.sales_name,	-- 销售员
	sum(case when receivable_amount>=0 then receivable_amount else 0 end) receivable_amount,	-- 应收金额
	sum(case when over_amt>=0 and receivable_amount>0 then over_amt else 0 end) over_amt,	-- 逾期金额
	sum(case when over_amt_s>=0 and receivable_amount>0 then over_amt_s else 0 end) over_amt_s,	-- 逾期金额*逾期天数
	sum(case when receivable_amount_s>=0 and receivable_amount>0 then receivable_amount_s else 0 end) receivable_amount_s,	-- 应收金额*帐期天数	
	coalesce(round(case  when coalesce(sum(case when receivable_amount>=0 then receivable_amount else 0 end), 0) <= 1 then 0  
				else coalesce(sum(case when over_amt_s>=0 and receivable_amount>0 then over_amt_s else 0 end), 0)
				/(sum(case when receivable_amount_s>=0 and receivable_amount>0 then receivable_amount_s else 0 end)) end
		  , 6),0) over_rate 	-- 逾期系数		  
from
	(select
		channel_name,
		customer_no,
		customer_name,
		company_code,
		company_name ,
		sum(receivable_amount) as receivable_amount,
		sum(case when over_days>=1 then receivable_amount else 0 end ) as over_amt,
		sum(case when over_days>=1 then receivable_amount*over_days else 0 end) as over_amt_s,
		sum(case when receivable_amount>=0 then receivable_amount else 0 end* if(COALESCE(account_period_val,0)=0,1,acc_val_calculation_factor)) as receivable_amount_s
	from csx_tmp.tmp_cust_order_overdue_dtl a 
	where channel_name = '大客户' and sdt = ${hiveconf:i_sdate_11}
	--签呈客户不考核，不算提成,因此不算逾期  2021年3月签呈取消剔除103717
	and customer_no not in('111118','102755','104023','105673','104402')
	and customer_no not in('107338','104123','102629','104526','106375','106380','106335','107268','104296','108391','108390','108072','108503')
	--签呈客户仅4月不考核，不算提成，4-6月不算逾期
	--and customer_no not in('PF0320','105177')
	--5月签呈客户5月剔除逾期，前者剔除逾期，后者仅5月剔除逾期
	and customer_no not in('103883','103167','105673','104352','104662','104514','104746','104172')
	--and customer_no not in('104055','106463')
	--6月签呈客户仅6月剔除逾期，其中 106463 从8月开始不剔除，106765 从12月开始不剔除，105240一直剔除
	--and customer_no not in('105157','107570','106905','104484','109382','106463','106765')
	--and customer_no not in('106463','106765')
	and customer_no not in('106765')
	and customer_no not in('105240')
	--678共3个月因财务对账不考核逾期
	--and customer_no not in('105527', '104445', '102202','100563','PF1206','102751','104775','103309','104116','PF0424','103183','102890',
	--						'102890','PF0320','105618','103320','104725','PF0094','103374','103772','PF1205','103094','104430','104519',
	--						'104478','103876','103782','104335','102790')
	--7月签呈，7、8、9共3个月不算逾期
	--and customer_no not in('106626','106997','111383','105169','105672','106652','105543','110679','105254','108773')
	--7月签呈，第1行仅7月剔除，第2行8月看情况，第3行历史问题已断约不考核逾期，第4行仅7月剔除逾期且不算提成
	----and customer_no not in('105493','105758','105832','105994','107015','111417')
							--'104677','111865','105528','105381','104867','107986','105717',
							--'107065','108096','108452','107851','106811','105572',
							--'105493','105758','105832','105994','107015','111417',
							--'113108','113067','110656','111837','111296','105202')
	--7月坏账签呈中，105601客户8月逾期剔除
	----and customer_no not in('105601')
	--8月签呈，其中107181客户8-9月剔除逾期，其他仅剔除8月
	--and customer_no not in('107181','105669','111905','110661','110677','110682','107459')
	----and customer_no not in('107181')
	--9月签呈 四川 算到业务代理人，每月剔除逾期和销售
	and customer_no not in('104179','112092')
	--9月签呈 安徽 已断约每月剔除? '106997'、'105169'9月有销售
	and customer_no not in('104352','105493','105758','105832','105994','107015','106626','106997','111383','105169','106652','105254','108773')
	--9月签呈 重庆 合伙人客户，9月剔除逾期和销售
	--and customer_no not in('114265','114248','114401','111933','113080','113392')
	--9月签呈 重庆 剔除9月逾期，其中'109484'剔除9月的逾期和销售
	--and customer_no not in('109484','107790','110664')	
	--9月签呈 江苏、贵州、四川 剔除9月逾期，其中'104268'10月也剔除
	--and customer_no not in('104268')
	--and customer_no not in('107621','109342','109403','109671','113154','113486','113762','114145','114482','111553','111559','111589',
	--						'111617','111618','111621','111622','111630','111632','111641','111643','113934','108797','104268','103997')
	--9月签呈 福建 SAP有逾期，签呈无逾期，反馈说省区财务核对没问题，需进一步确认 9月先剔除逾期
	--and customer_no not in('102890','102890','106526','106587','106697','107305','108333','109401','110670','PF0320')	
	--10月签呈 当月剔除逾期系数
	--and customer_no not in('105090','111935','110664','109293','111534','111810','112199','112201','112361','112874')
	--11月签呈 当月剔除逾期系数,其中 山西省 109461 只9-10月算到业务代理人，每月剔除逾期和销售
	--and customer_no not in('109461','112437','112176','104268')
	--and customer_no not in('109322','114045','112635','113643','107980')
	--12月同时有城市服务商和其他业务类型业绩客户，剔除当月逾期系数
	--and customer_no not in('102894','103175','104192','106214','106298','106299','106380','107268','109509','110248',
	--		'110518','110930','111427','111500','111853','113281','113936','113992','114265','114997')
	--12月签呈 当月剔除逾期系数,其中'113744','113824','113826','113831'剔除提成
	--and customer_no not in('107882','106469','108800','112180','111333','113744','113824','113826','113831')	
	--1月签呈 当月剔除逾期系数
    --and customer_no not in('111333','114510')
	and customer_no not in('111333')
	--2月签呈 当月剔除逾期系数，当月剔除、每月剔除、不算提成
	--and customer_no not in('116529','111506','111623','112326','109484','105302','115206')
	and customer_no not in('104532')
	--and customer_no not in('116015','115721','115721','116877','116883','116015','116556','116826','103253','103284','103296',
    --                       '103297','103304','103306','103311','104818','104828','104829','104835','105113','106283','106284',
    --                       '106298','106299','106301','106306','106307','106308','106309','106320','106321','106325','106326',
    --                       '106330','102844','114054','109000','114083','114085','115909','115971','116215',
	--					   '111100','116058','116188','105601')	
    --3月签呈 当月剔除逾期系数;3-4月剔除逾期系数;每月剔除逾期系数;剔除逾期系数不算提成(其中'PF0065','112574','106782'3-5月不发提成);剔除逾期系数.不算提成.每月*2
	--and customer_no not in('111506','108800','112180')
	--and customer_no not in('112129')
	and customer_no not in('114904','115313','115314','115325','115326','115391')
	and customer_no not in('PF0065','112574','106782')  --'116957','116629','116215'仅3月
	and customer_no not in('115721','116877','116883','116015','116556','116826')
	and customer_no not in('103253','103284','103296','103297','103304','103306','103311','104818','104828','104829','104835',
                           '105113','106283','106284','106298','106299','106301','106306','106307','106308','106309','106320',
                           '106321','106325','106326','106330','104609')
	--4月签呈 当月剔除逾期系数;	剔除逾期系数，不算提成
    --and customer_no not in('111506','105685','113744','116085','103369')
    --and customer_no not in('114265','117412','116957')
	--4月签呈 每月处理：剔除逾期系数，每月剔除;剔除逾期系数，不算提成，每月处理
    and customer_no not in('112045','115393','112248','104817','105601','104381','105304','105714','116099','104445','108127')
    and customer_no not in('102844','117940')	
	--5月签呈 当月剔除逾期系数，不算提成
	and customer_no not in('116957','106805','106228')
	--5月签呈 当月剔除逾期系数，不算提成，每月
	and customer_no not in('105280','106287','106427','110930','111100','112675','115202','115631')		
    --5月签呈 当月剔除逾期系数 '106052'5月系统错误
	and customer_no not in('107661','108088','113816','PF0424','118274','114391','108824','103369','114718','114785','115620','117305','115017','113609','106052')
	group by channel_name,customer_no,customer_name,company_code,company_name)a	
----渠道编号-1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 7.企业购 8.其他 9.业务代理
--剔除业务代理与内购客户
--4月签呈，将以下客户的销售员调整为xx 每月处理
left join
  (
    select customer_no,
	--  case when customer_no in('113873','113918','113935','113940','115656','117244','115826','115602','104281','107398') then '签呈,未知'
	--       when customer_no in('114054','109000','114083','114085','115909','115971') then '签呈,未知'
	--	   else work_no end as work_no,
	--  case when customer_no in('113873','113918','113935','113940','115656','117244','115826','115602','104281','107398') then '虚拟AA'
	--       when customer_no in('114054','109000','114083','114085','115909','115971') then '虚拟AA'
	--	   else sales_name end as sales_name
	work_no,sales_name
	from csx_dw.dws_crm_w_a_customer 
	--where sdt=${hiveconf:i_sdate_11} 
	where sdt='20210617'	
	--4月签呈 '118689'系统中为业务代理人，但需要人为计算销售员大客户提成,每月处理
	and (channel_code in('1','7','8') or customer_no='118689') and (customer_name not like '%内%购%' and customer_name not like '%临保%')
  )b on b.customer_no=a.customer_no  
--left join (select * from csx_dw.dws_crm_w_a_customer where sdt=${hiveconf:i_sdate_11} and dev_source_code not in('2','4')) b on b.customer_no=a.customer_no  --剔除业务代理与内购客户
--剔除当月有城市服务商与批发内购业绩的客户逾期系数
left join 		--业务类型编码(1.日配单 2.福利单 3.批发内购 4.城市服务商 5.省区大宗 6.BBC)  --剔除内购客户、城市服务商
  (
	select distinct customer_no 
	from csx_dw.dws_sale_r_d_detail 
	where sdt>=${hiveconf:i_sdate_12} 
	and sdt<=${hiveconf:i_sdate_11} 
	and business_type_code in('3','4')
	--5月签呈，不剔除城市服务商2.0，按大客户提成方案计算
	and customer_no not in(
        '115023','112906','112857','116024','113211','112875','111930','114859','117558','119047','118262',
		'114853','118731','116061','118259','112410','116432','118759','116056','113829','116032','115681',
		'107890','116185','115589','118219','115450','118264','118509','117817','111964','118472','118870',
		'111473','113634','115904','118221','117989','108713','115335','116038','114841','117889','119034',
		'117393','118574','115860','114485','117496','114075','109363','116055','113735','117961','111734',
		'116027','116959','117548','117458','112207','109357','114940','108726','109460','117610')
  )c on c.customer_no=a.customer_no
where c.customer_no is null	
group by a.channel_name,b.work_no,b.sales_name;


--签呈处理：销售员逾期系数  
--108818 80945044 剔除逾期系数，只剔除逾期金额保留应收
--112635 80941188 逾期金额减去211000，在结果处理
--105696 80949208 逾期金额减去530188.59，在结果处理

drop table csx_tmp.temp_salesname_over_rate;
create table csx_tmp.temp_salesname_over_rate
as
select *
from csx_tmp.temp_salesname_over_rate_0
where (work_no not in('80949208','80941188','80945044') or work_no is null)
union all  select '大客户' as channel_name,'80949208' as work_no,'王永涛' as sales_name,3719418.22 as receivable_amount,742951.58 as over_amt,7419860.22 as over_amt_s,187055518.5 as receivable_amount_s,0.03966662 as over_rate
union all  select '大客户' as channel_name,'80941188' as work_no,'陈志源' as sales_name,1406084.58 as receivable_amount,13825.9 as over_amt,954409.6262 as over_amt_s,60236961 as receivable_amount_s,0.015844253 as over_rate
union all  select '大客户' as channel_name,'80945044' as work_no,'田飞' as sales_name,1416345.81 as receivable_amount,274225.73 as over_amt,27796598.5 as over_amt_s,76523795.7 as receivable_amount_s,0.363241241 as over_rate
;
				

--服务管家逾期率
drop table csx_tmp.temp_service_user_over_rate;
create table csx_tmp.temp_service_user_over_rate
as
select 
	a.channel_name,	-- 渠道
	d.service_user_work_no,
	d.service_user_name,
	sum(case when receivable_amount>=0 then receivable_amount else 0 end) receivable_amount,	-- 应收金额
	sum(case when over_amt>=0 and receivable_amount>0 then over_amt else 0 end) over_amt,	-- 逾期金额
	sum(case when over_amt_s>=0 and receivable_amount>0 then over_amt_s else 0 end) over_amt_s,	-- 逾期金额*逾期天数
	sum(case when receivable_amount_s>=0 and receivable_amount>0 then receivable_amount_s else 0 end) receivable_amount_s,	-- 应收金额*帐期天数	
    coalesce(round(case  when coalesce(sum(case when receivable_amount>=0 then receivable_amount else 0 end), 0) <= 1 then 0  
				else coalesce(sum(case when over_amt_s>=0 and receivable_amount>0 then over_amt_s else 0 end), 0)
				/(sum(case when receivable_amount_s>=0 and receivable_amount>0 then receivable_amount_s else 0 end)) end
		  , 6),0) over_rate 	-- 逾期系数
from
	(select
		channel_name,
		customer_no,
		customer_name,
		company_code,
		company_name ,
		sum(receivable_amount) as receivable_amount,
		sum(case when over_days>=1 then receivable_amount else 0 end ) as over_amt,
		sum(case when over_days>=1 then receivable_amount*over_days else 0 end) as over_amt_s,
		sum(case when receivable_amount>=0 then receivable_amount else 0 end* if(COALESCE(account_period_val,0)=0,1,acc_val_calculation_factor)) as receivable_amount_s
	from csx_tmp.tmp_cust_order_overdue_dtl a 
	where channel_name = '大客户' and sdt = ${hiveconf:i_sdate_11}
	--签呈客户不考核，不算提成,因此不算逾期  2021年3月签呈取消剔除103717
	and customer_no not in('111118','102755','104023','105673','104402')
	and customer_no not in('107338','104123','102629','104526','106375','106380','106335','107268','104296','108391','108390','108072','108503')
	--签呈客户仅4月不考核，不算提成，4-6月不算逾期
	--and customer_no not in('PF0320','105177')
	--5月签呈客户5月剔除逾期，前者剔除逾期，后者仅5月剔除逾期
	and customer_no not in('103883','103167','105673','104352','104662','104514','104746','104172')
	--and customer_no not in('104055','106463')
	--6月签呈客户仅6月剔除逾期，其中 106463 从8月开始不剔除，106765 从12月开始不剔除，105240一直剔除
	--and customer_no not in('105157','107570','106905','104484','109382','106463','106765')
	--and customer_no not in('106463','106765')
	and customer_no not in('106765')
	and customer_no not in('105240')
	--678共3个月因财务对账不考核逾期
	--and customer_no not in('105527', '104445', '102202','100563','PF1206','102751','104775','103309','104116','PF0424','103183','102890',
	--						'102890','PF0320','105618','103320','104725','PF0094','103374','103772','PF1205','103094','104430','104519',
	--						'104478','103876','103782','104335','102790')
	--7月签呈，7、8、9共3个月不算逾期
	--and customer_no not in('106626','106997','111383','105169','105672','106652','105543','110679','105254','108773')
	--7月签呈，第1行仅7月剔除，第2行8月看情况，第3行历史问题已断约不考核逾期，第4行仅7月剔除逾期且不算提成
	----and customer_no not in('105493','105758','105832','105994','107015','111417')
							--'104677','111865','105528','105381','104867','107986','105717',
							--'107065','108096','108452','107851','106811','105572',
							--'105493','105758','105832','105994','107015','111417',
							--'113108','113067','110656','111837','111296','105202')
	--7月坏账签呈中，105601客户8月逾期剔除
	----and customer_no not in('105601')
	--8月签呈，其中107181客户8-9月剔除逾期，其他仅剔除8月
	--and customer_no not in('107181','105669','111905','110661','110677','110682','107459')
	----and customer_no not in('107181')
	--9月签呈 四川 算到业务代理人，每月剔除逾期和销售
	and customer_no not in('104179','112092')
	--9月签呈 安徽 已断约每月剔除? '106997'、'105169'9月有销售
	and customer_no not in('104352','105493','105758','105832','105994','107015','106626','106997','111383','105169','106652','105254','108773')
	--9月签呈 重庆 合伙人客户，9月剔除逾期和销售
	--and customer_no not in('114265','114248','114401','111933','113080','113392')
	--9月签呈 重庆 剔除9月逾期，其中'109484'剔除9月的逾期和销售
	--and customer_no not in('109484','107790','110664')	
	--9月签呈 江苏、贵州、四川 剔除9月逾期，其中'104268'10月也剔除
	--and customer_no not in('104268')
	--and customer_no not in('107621','109342','109403','109671','113154','113486','113762','114145','114482','111553','111559','111589',
	--						'111617','111618','111621','111622','111630','111632','111641','111643','113934','108797','104268','103997')
	--9月签呈 福建 SAP有逾期，签呈无逾期，反馈说省区财务核对没问题，需进一步确认 9月先剔除逾期
	--and customer_no not in('102890','102890','106526','106587','106697','107305','108333','109401','110670','PF0320')	
	--10月签呈 当月剔除逾期系数
	--and customer_no not in('105090','111935','110664','109293','111534','111810','112199','112201','112361','112874')
	--11月签呈 当月剔除逾期系数,其中 山西省 109461 只9-10月算到业务代理人，每月剔除逾期和销售
	--and customer_no not in('109461','112437','112176','104268')
	--and customer_no not in('109322','114045','112635','113643','107980')
	--12月同时有城市服务商和其他业务类型业绩客户，剔除当月逾期系数
	--and customer_no not in('102894','103175','104192','106214','106298','106299','106380','107268','109509','110248',
	--		'110518','110930','111427','111500','111853','113281','113936','113992','114265','114997')
	--12月签呈 当月剔除逾期系数,其中'113744','113824','113826','113831'剔除提成
	--and customer_no not in('107882','106469','108800','112180','111333','113744','113824','113826','113831')	
	--1月签呈 当月剔除逾期系数
    --and customer_no not in('111333','114510')
	and customer_no not in('111333')
	--2月签呈 当月剔除逾期系数，当月剔除、每月剔除、不算提成
	--and customer_no not in('116529','111506','111623','112326','109484','105302','115206')
	and customer_no not in('104532')
	--and customer_no not in('116015','115721','115721','116877','116883','116015','116556','116826','103253','103284','103296',
    --                       '103297','103304','103306','103311','104818','104828','104829','104835','105113','106283','106284',
    --                       '106298','106299','106301','106306','106307','106308','106309','106320','106321','106325','106326',
    --                       '106330','102844','114054','109000','114083','114085','115909','115971','116215',
	--					   '111100','116058','116188','105601')	
    --3月签呈 当月剔除逾期系数;3-4月剔除逾期系数;每月剔除逾期系数;剔除逾期系数不算提成(其中'PF0065','112574','106782'3-5月不发提成);剔除逾期系数.不算提成.每月*2
	--and customer_no not in('111506','108800','112180')
	--and customer_no not in('112129')
	and customer_no not in('114904','115313','115314','115325','115326','115391')
	and customer_no not in('PF0065','112574','106782')  --'116957','116629','116215'仅3月
	and customer_no not in('115721','116877','116883','116015','116556','116826')
	and customer_no not in('103253','103284','103296','103297','103304','103306','103311','104818','104828','104829','104835',
                           '105113','106283','106284','106298','106299','106301','106306','106307','106308','106309','106320',
                           '106321','106325','106326','106330','104609')
	--4月签呈 当月剔除逾期系数;	剔除逾期系数，不算提成
    --and customer_no not in('111506','105685','113744','116085','103369')
    --and customer_no not in('114265','117412','116957')
	--4月签呈 每月处理：剔除逾期系数，每月剔除;剔除逾期系数，不算提成，每月处理
	and customer_no not in('112045','115393','112248','104817','105601','104381','105304','105714','116099','104445','108127')
    and customer_no not in('102844','117940')	
	--5月签呈 当月剔除逾期系数，不算提成
	and customer_no not in('116957','106805','106228')
	--5月签呈 当月剔除逾期系数，不算提成，每月
	and customer_no not in('105280','106287','106427','110930','111100','112675','115202','115631')		
    --5月签呈 当月剔除逾期系数 '106052'5月系统错误
	and customer_no not in('107661','108088','113816','PF0424','118274','114391','108824','103369','114718','114785','115620','117305','115017','113609','106052')
	group by channel_name,customer_no,customer_name,company_code,company_name)a	
----渠道编号-1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 7.企业购 8.其他 9.业务代理
--剔除业务代理与内购客户
--4月签呈，将以下客户的销售员调整为xx 每月处理
left join
  (
    select customer_no,
	--  case when customer_no in('113873','113918','113935','113940','115656','117244','115826','115602','104281','107398') then '签呈,未知'
	--       when customer_no in('114054','109000','114083','114085','115909','115971') then '签呈,未知'
	--	   else work_no end as work_no,
	--  case when customer_no in('113873','113918','113935','113940','115656','117244','115826','115602','104281','107398') then '虚拟AA'
	--       when customer_no in('114054','109000','114083','114085','115909','115971') then '虚拟AA'
	--	   else sales_name end as sales_name		 
	work_no,sales_name
	from csx_dw.dws_crm_w_a_customer 
	--where sdt=${hiveconf:i_sdate_11} 
	where sdt='20210617' 
	--4月签呈 '118689'系统中为业务代理人，但需要人为计算销售员大客户提成,每月处理
	and (channel_code in('1','7','8') or customer_no='118689') and (customer_name not like '%内%购%' and customer_name not like '%临保%')
  )b on b.customer_no=a.customer_no  
--left join (select * from csx_dw.dws_crm_w_a_customer where sdt=${hiveconf:i_sdate_11} and dev_source_code not in('2','4')) b on b.customer_no=a.customer_no  --剔除业务代理与内购客户
--剔除当月有城市服务商与批发内购业绩的客户逾期系数
left join 		--业务类型编码(1.日配单 2.福利单 3.批发内购 4.城市服务商 5.省区大宗 6.BBC)  --剔除内购客户、城市服务商
  (
	select distinct customer_no 
	from csx_dw.dws_sale_r_d_detail 
	where sdt>=${hiveconf:i_sdate_12} 
	and sdt<=${hiveconf:i_sdate_11} 
	and business_type_code in('3','4')
	--5月签呈，不剔除城市服务商2.0，按大客户提成方案计算
	and customer_no not in(
        '115023','112906','112857','116024','113211','112875','111930','114859','117558','119047','118262',
		'114853','118731','116061','118259','112410','116432','118759','116056','113829','116032','115681',
		'107890','116185','115589','118219','115450','118264','118509','117817','111964','118472','118870',
		'111473','113634','115904','118221','117989','108713','115335','116038','114841','117889','119034',
		'117393','118574','115860','114485','117496','114075','109363','116055','113735','117961','111734',
		'116027','116959','117548','117458','112207','109357','114940','108726','109460','117610')	
  )c on c.customer_no=a.customer_no
    
----关联服务管家
--join		
--  (  
--      select customer_no,
--	 concat_ws(',', collect_list(service_user_work_no)) as service_user_work_no,
--	 concat_ws(',', collect_list(service_user_name)) as service_user_name
--	from 
--	  (select distinct customer_no,service_user_work_no,service_user_name
--	  from csx_dw.dws_crm_w_a_customer_sales_link 
--      where sdt=${hiveconf:i_sdate_11} 
--	  and is_additional_info = 1 and service_user_id <> 0
--      )a
--	group by customer_no
--  )d on d.customer_no=a.customer_no
--关联服务管家 5月计算用，客户对应销售员与服务管家
left join		
  (  
  select distinct customer_no,service_user_work_no,service_user_name,
    work_no,sales_name,is_part_time_service_manager
  from csx_tmp.tmp_customer_service_manager_info	
  --from csx_dw.report_crm_w_a_customer_service_manager_info 
  --where sdt=${hiveconf:i_sdate_11}
  --where sdt='20210617' 
  )d on d.customer_no=a.customer_no	  
where c.customer_no is null	
group by a.channel_name,d.service_user_work_no,d.service_user_name;



--大宗供应链的逾期系数
insert overwrite directory '/tmp/raoyanhua/yuqi_dazong' row format delimited fields terminated by '\t'
select 
	a.channel_name,	-- 渠道
	b.sales_province_name,	-- 省区
	a.customer_no,	-- 客户编码
	a.customer_name,	-- 客户名称
	b.work_no,	-- 销售员工号
	b.sales_name,	-- 销售员
	c.account_period_code,	-- 账期编码
	if(c.account_period_code like 'Y%',if(c.account_period_val=31,45,c.account_period_val+15),c.account_period_val) account_period_val,	-- 帐期天数
	c.account_period_name,	-- 账期名称
	a.company_code,	-- 公司代码
	a.company_name,	-- 公司名称
	case when a.receivable_amount>=0 then a.receivable_amount else 0 end receivable_amount,	-- 应收金额
	case when a.over_amt>=0 and a.receivable_amount>0 then a.over_amt else 0 end over_amt,	-- 逾期金额
	case when a.over_amt_s>=0 and a.receivable_amount>0 then a.over_amt_s else 0 end over_amt_s,	-- 逾期金额*逾期天数
	case when a.receivable_amount_s>=0 and a.receivable_amount>0 then a.receivable_amount_s else 0 end receivable_amount_s,	-- 应收金额*帐期天数	
    coalesce(round(case when coalesce(case when a.receivable_amount>=0 then a.receivable_amount else 0 end, 0) <= 1 then 0  
						else (coalesce(case when a.over_amt_s>=0 and a.receivable_amount>0 then a.over_amt_s else 0 end,0)
						/(case when a.receivable_amount_s>=0 and a.receivable_amount>0 then a.receivable_amount_s else 0 end)) end
		  , 6),0) over_rate 	-- 逾期系数
		    
from
	(select
		channel_name,
		customer_no,
		customer_name,
		--account_period_code,
		--COALESCE(account_period_val,0) account_period_val,
		--account_period_name,
		company_code,
		company_name,
		sum(receivable_amount) as receivable_amount,
		sum(case when over_days>=1 then receivable_amount else 0 end ) as over_amt,
		sum(case when over_days>=1 then receivable_amount*over_days else 0 end) as over_amt_s,
		sum(case when receivable_amount>=0 then receivable_amount else 0 end* if(COALESCE(account_period_val,0)=0,1,acc_val_calculation_factor)) as receivable_amount_s
	from csx_tmp.tmp_cust_order_overdue_dtl  
	where (channel_name like '大宗%' or channel_name like '%供应链%')
	and sdt = ${hiveconf:i_sdate_11} 
	group by channel_name,customer_no,customer_name,company_code,company_name
	)a
join		 ----渠道编号-1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 7.企业购 8.其他 9.业务代理
  (
    select * from csx_dw.dws_crm_w_a_customer 
	--where sdt=${hiveconf:i_sdate_11} 
	where sdt='20210617'
	and channel_code in('4','5','6') 
  )b on b.customer_no=a.customer_no  
left join
	(select
		customer_no,
		company_code,
		payment_terms account_period_code,
		case when payment_terms like 'Y%' then concat('月结',COALESCE(cast(payment_days as int),0)) 
			 else concat('票到',COALESCE(cast(payment_days as int),0)) end account_period_name,
		COALESCE(cast(payment_days as int),0) account_period_val
	from csx_dw.dws_crm_w_a_customer_company a
	where sdt='current'
	and customer_no<>''
	)c on (a.customer_no=c.customer_no and a.company_code=c.company_code);


--5月查询结果是perform_dc_code in('W0L4','W0K7','W0AW')三个城市服务商2.0仓过机的客户都只在这些仓有过机，以后最好再看下
--城市服务商2.0的逾期系数
insert overwrite directory '/tmp/raoyanhua/yuqi_csfws2' row format delimited fields terminated by '\t'
select 
	a.channel_name,	-- 渠道
	b.sales_province_name,	-- 省区
	a.customer_no,	-- 客户编码
	a.customer_name,	-- 客户名称
	b.work_no,	-- 销售员工号
	b.sales_name,	-- 销售员
	c.account_period_code,	-- 账期编码
	if(c.account_period_code like 'Y%',if(c.account_period_val=31,45,c.account_period_val+15),c.account_period_val) account_period_val,	-- 帐期天数
	c.account_period_name,	-- 账期名称
	a.company_code,	-- 公司代码
	a.company_name,	-- 公司名称
	case when a.receivable_amount>=0 then a.receivable_amount else 0 end receivable_amount,	-- 应收金额
	case when a.over_amt>=0 and a.receivable_amount>0 then a.over_amt else 0 end over_amt,	-- 逾期金额
	case when a.over_amt_s>=0 and a.receivable_amount>0 then a.over_amt_s else 0 end over_amt_s,	-- 逾期金额*逾期天数
	case when a.receivable_amount_s>=0 and a.receivable_amount>0 then a.receivable_amount_s else 0 end receivable_amount_s,	-- 应收金额*帐期天数	
    coalesce(round(case when coalesce(case when a.receivable_amount>=0 then a.receivable_amount else 0 end, 0) <= 1 then 0  
						else (coalesce(case when a.over_amt_s>=0 and a.receivable_amount>0 then a.over_amt_s else 0 end,0)
						/(case when a.receivable_amount_s>=0 and a.receivable_amount>0 then a.receivable_amount_s else 0 end)) end
		  , 6),0) over_rate 	-- 逾期系数
		    
from
	(select
		channel_name,
		customer_no,
		customer_name,
		--appoint_place_code,  --履约地点编码
		--account_period_code,
		--COALESCE(account_period_val,0) account_period_val,
		--account_period_name,
		company_code,
		company_name,
		sum(receivable_amount) as receivable_amount,
		sum(case when over_days>=1 then receivable_amount else 0 end ) as over_amt,
		sum(case when over_days>=1 then receivable_amount*over_days else 0 end) as over_amt_s,
		sum(case when receivable_amount>=0 then receivable_amount else 0 end* if(COALESCE(account_period_val,0)=0,1,acc_val_calculation_factor)) as receivable_amount_s
	from
	(
	select * from csx_tmp.tmp_cust_order_overdue_dtl  
	where channel_name = '大客户'
	and sdt = ${hiveconf:i_sdate_11} 
	)a 
    --城市服务商2.0 按履约DC
    join
	(select distinct inventory_dc_code
	from csx_ods.source_csms_w_a_yszx_town_service_provider_config
	)d on a.appoint_place_code=d.inventory_dc_code	
	group by channel_name,customer_no,customer_name,company_code,company_name
	)a
join		 ----渠道编号-1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 7.企业购 8.其他 9.业务代理
  (
    select * from csx_dw.dws_crm_w_a_customer 
	--where sdt=${hiveconf:i_sdate_11} 
	where sdt='20210617'
	and channel_code in('1','7') 
  )b on b.customer_no=a.customer_no  
left join
	(select
		customer_no,
		company_code,
		payment_terms account_period_code,
		case when payment_terms like 'Y%' then concat('月结',COALESCE(cast(payment_days as int),0)) 
			 else concat('票到',COALESCE(cast(payment_days as int),0)) end account_period_name,
		COALESCE(cast(payment_days as int),0) account_period_val
	from csx_dw.dws_crm_w_a_customer_company a
	where sdt='current'
	and customer_no<>''
	)c on (a.customer_no=c.customer_no and a.company_code=c.company_code);


--查询城市服务商2.0客户,按库存DC
--select distinct inventory_dc_code from csx_ods.source_csms_w_a_yszx_town_service_provider_config;
select a.*,c.work_no,c.sales_name
from 
(
  select province_name,customer_no,customer_name,business_type_name,dc_code,
    sum(sales_value)sales_value
  from csx_dw.dws_sale_r_d_detail
  where sdt>='20210501'
  and sdt<'20210601'
  and channel_code in('1','7','9')
  --and business_type_code not in('3','4')
  group by province_name,customer_no,customer_name,business_type_name,dc_code
)a 
join 
(
  select distinct customer_no
  from csx_dw.dws_sale_r_d_detail
  where sdt>='20210501'
  and sdt<'20210601'
  and channel_code in('1','7','9')
  and dc_code in('W0AW','W0K7','W0L4')
) b on b.customer_no=a.customer_no
left join 
	(select distinct customer_no,customer_name,work_no,sales_name,sales_province_name
	from csx_dw.dws_crm_w_a_customer 
	--where sdt=${hiveconf:i_sdate_11} 
	where sdt='20210617'
	)c on c.customer_no=a.customer_no;






/*
--截至某天的订单应收明细
insert overwrite directory '/tmp/raoyanhua/ysmx' row format delimited fields terminated by '\t'
select 
	b.sales_province,	-- 省区
	b.work_no,	-- 销售员工号
	b.sales_name,	-- 销售员
	c.account_period_code,	-- 最新账期编码
	if(c.account_period_code like 'Y%',if(c.account_period_val=31,45,c.account_period_val+15),c.account_period_val) account_period_val,	-- 最新帐期天数
	a.*,
	if(a.over_days>0,'逾期','未逾期') is_overdue	
from
	(select *
	from csx_tmp.tmp_cust_order_overdue_dtl  
	where channel_name = '大客户' and sdt = ${hiveconf:i_sdate_11} 
	--签呈客户不考核，不算提成 2021年3月签呈取消剔除 103717
	and customer_no not in('111118','102755','104023','105673','104402')
	and customer_no not in('107338','104123','102629','104526','106375','106380','106335','107268','104296','108391','108390','108072','108503')
	)a 
join (select * from csx_dw.dws_crm_w_a_customer_m_v1 where sdt=${hiveconf:i_sdate_11} and attribute_code <> 5) b on b.customer_no=a.customer_no
left join
	(select
		customer_no,
		company_code,
		payment_terms account_period_code,
		case when payment_terms like 'Y%' then concat('月结',COALESCE(cast(payment_days as int),0)) 
			 else concat('票到',COALESCE(cast(payment_days as int),0)) end account_period_name,
		COALESCE(cast(payment_days as int),0) account_period_val
	from csx_dw.dws_crm_w_a_customer_company a
	where sdt='current'
	and customer_no<>''
	)c on (a.customer_no=c.customer_no and a.company_code=c.company_code)
;


