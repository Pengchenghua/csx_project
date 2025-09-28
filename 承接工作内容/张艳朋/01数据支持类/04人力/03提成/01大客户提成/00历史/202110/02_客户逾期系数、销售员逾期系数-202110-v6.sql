
-- 昨日、昨日、昨日月1日
--select ${hiveconf:i_sdate_11},${hiveconf:i_sdate_12},${hiveconf:i_sdate_12},${hiveconf:i_sdate_11};
--set i_sdate_1 =date_sub(current_date,1);
--set i_sdate_11 =regexp_replace(date_sub(current_date,1),'-','');
--set i_sdate_12 =regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),0),'-','');

--set i_sdate_1 =last_day(add_months(date_sub(current_date,1),-1));
--set i_sdate_11 =regexp_replace(last_day(add_months(date_sub(current_date,1),-1)),'-','');	
--set i_sdate_12 =regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-1),'-','');					

	
set i_sdate_1 ='2021-10-31';
set i_sdate_11 ='20211031';
set i_sdate_12 ='20211001';


--6月签呈，签呈处理销售员服务管家关系，每月
drop table csx_tmp.tmp_tc_customer_service_manager_info;
create table csx_tmp.tmp_tc_customer_service_manager_info
as  
select 
	distinct customer_no,service_user_work_no,service_user_name,
    work_no,sales_name,is_part_time_service_manager,
    sales_sale_rate,  --销售员_销售额提成比例
	sales_front_profit_rate,  --销售员_前端毛利提成比例
    service_user_sale_rate,  --服务管家_销售额提成比例
	service_user_front_profit_rate	 --服务管家_前端毛利提成比例
from 
	csx_dw.report_crm_w_a_customer_service_manager_info 
where 
	sdt=${hiveconf:i_sdate_11}
	--where sdt='20210630' 
	and customer_no not in (
	'118072','100326','104086','112072','PF1265','104358','105182','106721','112054','102565','102790','103784','103855','105150','111844','102734','107404','102784','102901',
	'112288','113467','103199','104469','104501','112038','115982','115987','PF0365','105355','105886','114275','102647','100563','101482','102229','102508','102633','102686',
	'103062','103141','103372','103714','103964','104007','104054','104165','104281','104612','104954','104970','105156','105177','105181','105225','105441','105721','105806',
	'105838','105882','106423','106481','106881','107371','107398','108127','108739','109377','111241','112044','112052','112053','112058','112062','112067','112327','113101',
	'113635','113646','114344','115324','115595','115602','115646','115656','115679','115826','115857','116123','116211','117244','118687','PF0094','PF1205','104666','104883',
	'105381','105717','105821','106124','106314','106354','106530','106989','107346','107363','107514','107577','107956','107986','111137','111506','111566','112865','113184',
	'114872','114876','115156','115458','115520','115781','115833','116052','116467','117007','117755','117820','118187','118920','103995','107683','111122','117057','117095',
	'118540','120044','116957','116629','114524','115195','120024','120317','121235','121441','122261','113352','115535','118032','119218','111647','113151','113672','113783',
	'114652','114799','115102','116071','117317','117702','117811','118174','118595','118815','118868','119872','119918','120021','121457','121781','113926','114964','116170',
	'118816','119825','115300','118780','115042','117249','111135','112808','113784','119231','122219','102534','102798','102806','105186','105480','105540','106469','106524',
	'106538','107438','111892','112210','115915','117022','103945','104222','104229','104241','104251','104255','104414','104762','104872','104965','105005','105024','105247',
	'105483','105518','105521','105569','105639','105715','105756','105768','105790','105802','106095','106300','106317','106434','106521','106572','106602','106693','106737',
	'106898','106925','106958','106987','107000','107050','107073','107255','107276','107453','107461','107500','107593','107655','107674','107838','108021','108176','108749',
	'108795','108806','108837','108853','108960','109291','109349','109381','109786','109977','110664','111074','111219','111349','111556','111771','112754','112813','112911',
	'113105','113152','113274','113463','113564','113571','113590','113643','113645','114127','114287','114354','114437','114505','114704','114784','114800','114821','114921',
	'115082','115178','115259','115284','115294','115407','115527','115710','115742','115807','116141','116485','116539','116665','116670','116762','116847','116928','116943',
	'116962','116988','116998','117016','117020','117058','117067','117068','117116','117120','117348','117454','117533','117543','117727','117728','117729','117748','117749',
	'117753','117761','117766','117773','117776','117777','117781','117782','117783','117784','117785','117786','117790','117791','117795','117796','117800','117805','117860',
	'117918','118026','118239','118345','118470','118475','118768','118933','118943','118973','119076','119252','119513','119543','119659','119756','120031','120092','120105',
	'120244','120272','120359','120476','120486','120497','120541','120581','120640','120645','120811','120860','120958','120983','121026','121045','121074','121081','121340',
	'121415','121610','121767','122052','122087','122532','122592','122670','122730','112803','115254','115471','117911','118670','120982','121870','110017','113263','114812',
	'115366','115883','106516','106945','108293','109014','111009','113085','113400','115402','115508','118238','119976','121338','121606'
	)
union all   select '118072' as customer_no,'' as service_user_work_no,'' as service_user_name,'81084686' as work_no,'何丽姿' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '100326' as customer_no,'80009493' as service_user_work_no,'郑银燕' as service_user_name,'80007454' as work_no,'李翔' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '104086' as customer_no,'81098902' as service_user_work_no,'刘雪雪' as service_user_name,'81026931' as work_no,'林圳' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '112072' as customer_no,'81098902' as service_user_work_no,'刘雪雪' as service_user_name,'81026931' as work_no,'林圳' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select 'PF1265' as customer_no,'81105401' as service_user_work_no,'卢烊' as service_user_name,'80012225' as work_no,'林挺波' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '104358' as customer_no,'81129243' as service_user_work_no,'汪敏禄' as service_user_name,'81129243' as work_no,'汪敏禄' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '105182' as customer_no,'81133021' as service_user_work_no,'黄升' as service_user_name,'81133021' as work_no,'黄升' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '106721' as customer_no,'81133021' as service_user_work_no,'黄升' as service_user_name,'81133021' as work_no,'黄升' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '112054' as customer_no,'' as service_user_work_no,'' as service_user_name,'80764642' as work_no,'张小玲' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '102565' as customer_no,'81105401' as service_user_work_no,'卢烊' as service_user_name,'80960666' as work_no,'冯桂华' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '102790' as customer_no,'81105401' as service_user_work_no,'卢烊' as service_user_name,'80960666' as work_no,'冯桂华' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '103784' as customer_no,'' as service_user_work_no,'' as service_user_name,'80960666' as work_no,'冯桂华' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '103855' as customer_no,'81105401' as service_user_work_no,'卢烊' as service_user_name,'80960666' as work_no,'冯桂华' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '105150' as customer_no,'81133021' as service_user_work_no,'黄升' as service_user_name,'81133021' as work_no,'黄升' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '111844' as customer_no,'81105401' as service_user_work_no,'卢烊' as service_user_name,'80960666' as work_no,'冯桂华' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '102734' as customer_no,'81131450' as service_user_work_no,'张珠妹' as service_user_name,'80980614' as work_no,'池春梅' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '107404' as customer_no,'80952742' as service_user_work_no,'王秀云' as service_user_name,'80952742' as work_no,'王秀云' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '102784' as customer_no,'81099343' as service_user_work_no,'林瑾鑫' as service_user_name,'80980614' as work_no,'池春梅' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '102901' as customer_no,'81131450' as service_user_work_no,'张珠妹' as service_user_name,'80980614' as work_no,'池春梅' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '112288' as customer_no,'81133021' as service_user_work_no,'黄升' as service_user_name,'80980614' as work_no,'池春梅' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '113467' as customer_no,'81131450' as service_user_work_no,'张珠妹' as service_user_name,'80980614' as work_no,'池春梅' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '103199' as customer_no,'81099343' as service_user_work_no,'林瑾鑫' as service_user_name,'80980614' as work_no,'池春梅' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '104469' as customer_no,'81099343' as service_user_work_no,'林瑾鑫' as service_user_name,'80980614' as work_no,'池春梅' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '104501' as customer_no,'' as service_user_work_no,'' as service_user_name,'80980614' as work_no,'池春梅' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '112038' as customer_no,'81099343' as service_user_work_no,'林瑾鑫' as service_user_name,'80980614' as work_no,'池春梅' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '115982' as customer_no,'81099343' as service_user_work_no,'林瑾鑫' as service_user_name,'80980614' as work_no,'池春梅' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '115987' as customer_no,'81099343' as service_user_work_no,'林瑾鑫' as service_user_name,'80980614' as work_no,'池春梅' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select 'PF0365' as customer_no,'81099343' as service_user_work_no,'林瑾鑫' as service_user_name,'80980614' as work_no,'池春梅' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '105355' as customer_no,'81105401' as service_user_work_no,'卢烊' as service_user_name,'80972915' as work_no,'陈伟豪' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '105886' as customer_no,'80691224' as service_user_work_no,'王少端' as service_user_name,'80691224' as work_no,'王少端' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '114275' as customer_no,'' as service_user_work_no,'' as service_user_name,'80972915' as work_no,'陈伟豪' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '102647' as customer_no,'' as service_user_work_no,'' as service_user_name,'80972915' as work_no,'陈伟豪' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '100563' as customer_no,'81099343' as service_user_work_no,'林瑾鑫' as service_user_name,'81099343' as work_no,'林瑾鑫' as sales_name,'是' as is_part_time_service_manager,0 as sales_sale_rate,0 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '101482' as customer_no,'80952742' as service_user_work_no,'王秀云' as service_user_name,'80952742' as work_no,'王秀云' as sales_name,'是' as is_part_time_service_manager,0 as sales_sale_rate,0 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '102229' as customer_no,'80691224' as service_user_work_no,'王少端' as service_user_name,'80691224' as work_no,'王少端' as sales_name,'是' as is_part_time_service_manager,0 as sales_sale_rate,0 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '102508' as customer_no,'80691224' as service_user_work_no,'王少端' as service_user_name,'80691224' as work_no,'王少端' as sales_name,'是' as is_part_time_service_manager,0 as sales_sale_rate,0 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '102633' as customer_no,'80691224' as service_user_work_no,'王少端' as service_user_name,'80691224' as work_no,'王少端' as sales_name,'是' as is_part_time_service_manager,0 as sales_sale_rate,0 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '102686' as customer_no,'81131450' as service_user_work_no,'张珠妹' as service_user_name,'81131450' as work_no,'张珠妹' as sales_name,'是' as is_part_time_service_manager,0 as sales_sale_rate,0 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '103062' as customer_no,'80952742' as service_user_work_no,'王秀云' as service_user_name,'80952742' as work_no,'王秀云' as sales_name,'是' as is_part_time_service_manager,0 as sales_sale_rate,0 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '103141' as customer_no,'80816155' as service_user_work_no,'张磊磊' as service_user_name,'80816155' as work_no,'张磊磊' as sales_name,'是' as is_part_time_service_manager,0 as sales_sale_rate,0 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '103372' as customer_no,'81129243' as service_user_work_no,'汪敏禄' as service_user_name,'81089088' as work_no,'陈先贵' as sales_name,'是' as is_part_time_service_manager,0 as sales_sale_rate,0 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '103714' as customer_no,'80816155' as service_user_work_no,'张磊磊' as service_user_name,'80816155' as work_no,'张磊磊' as sales_name,'是' as is_part_time_service_manager,0 as sales_sale_rate,0 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '103964' as customer_no,'80816155' as service_user_work_no,'张磊磊' as service_user_name,'80816155' as work_no,'张磊磊' as sales_name,'是' as is_part_time_service_manager,0 as sales_sale_rate,0 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '104007' as customer_no,'80691224' as service_user_work_no,'王少端' as service_user_name,'80691224' as work_no,'王少端' as sales_name,'是' as is_part_time_service_manager,0 as sales_sale_rate,0 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '104054' as customer_no,'81129243' as service_user_work_no,'汪敏禄' as service_user_name,'81129243' as work_no,'汪敏禄' as sales_name,'是' as is_part_time_service_manager,0 as sales_sale_rate,0 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '104165' as customer_no,'80691224' as service_user_work_no,'王少端' as service_user_name,'80691224' as work_no,'王少端' as sales_name,'是' as is_part_time_service_manager,0 as sales_sale_rate,0 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '104281' as customer_no,'80974184;81107303' as service_user_work_no,'郭荔丽;姚细美' as service_user_name,'80974184' as work_no,'郭荔丽' as sales_name,'是' as is_part_time_service_manager,0 as sales_sale_rate,0 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '104612' as customer_no,'81099343' as service_user_work_no,'林瑾鑫' as service_user_name,'81099343' as work_no,'林瑾鑫' as sales_name,'是' as is_part_time_service_manager,0 as sales_sale_rate,0 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '104954' as customer_no,'80691224' as service_user_work_no,'王少端' as service_user_name,'80691224' as work_no,'王少端' as sales_name,'是' as is_part_time_service_manager,0 as sales_sale_rate,0 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '104970' as customer_no,'80691224' as service_user_work_no,'王少端' as service_user_name,'80691224' as work_no,'王少端' as sales_name,'是' as is_part_time_service_manager,0 as sales_sale_rate,0 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '105156' as customer_no,'80952742' as service_user_work_no,'王秀云' as service_user_name,'80952742' as work_no,'王秀云' as sales_name,'是' as is_part_time_service_manager,0 as sales_sale_rate,0 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '105177' as customer_no,'80952742' as service_user_work_no,'王秀云' as service_user_name,'80952742' as work_no,'王秀云' as sales_name,'是' as is_part_time_service_manager,0 as sales_sale_rate,0 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '105181' as customer_no,'81131450' as service_user_work_no,'张珠妹' as service_user_name,'81131450' as work_no,'张珠妹' as sales_name,'是' as is_part_time_service_manager,0 as sales_sale_rate,0 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '105225' as customer_no,'80691224' as service_user_work_no,'王少端' as service_user_name,'80691224' as work_no,'王少端' as sales_name,'是' as is_part_time_service_manager,0 as sales_sale_rate,0 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '105441' as customer_no,'80691224' as service_user_work_no,'王少端' as service_user_name,'80691224' as work_no,'王少端' as sales_name,'是' as is_part_time_service_manager,0 as sales_sale_rate,0 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '105721' as customer_no,'80952742' as service_user_work_no,'王秀云' as service_user_name,'80952742' as work_no,'王秀云' as sales_name,'是' as is_part_time_service_manager,0 as sales_sale_rate,0 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '105806' as customer_no,'80816155' as service_user_work_no,'张磊磊' as service_user_name,'80816155' as work_no,'张磊磊' as sales_name,'是' as is_part_time_service_manager,0 as sales_sale_rate,0 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '105838' as customer_no,'80816155' as service_user_work_no,'张磊磊' as service_user_name,'80816155' as work_no,'张磊磊' as sales_name,'是' as is_part_time_service_manager,0 as sales_sale_rate,0 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '105882' as customer_no,'81099343' as service_user_work_no,'林瑾鑫' as service_user_name,'81099343' as work_no,'林瑾鑫' as sales_name,'是' as is_part_time_service_manager,0 as sales_sale_rate,0 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '106423' as customer_no,'81131450' as service_user_work_no,'张珠妹' as service_user_name,'81131450' as work_no,'张珠妹' as sales_name,'是' as is_part_time_service_manager,0 as sales_sale_rate,0 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '106481' as customer_no,'81131450' as service_user_work_no,'张珠妹' as service_user_name,'81131450' as work_no,'张珠妹' as sales_name,'是' as is_part_time_service_manager,0 as sales_sale_rate,0 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '106881' as customer_no,'81131450' as service_user_work_no,'张珠妹' as service_user_name,'81131450' as work_no,'张珠妹' as sales_name,'是' as is_part_time_service_manager,0 as sales_sale_rate,0 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '107371' as customer_no,'81129243' as service_user_work_no,'汪敏禄' as service_user_name,'81129243' as work_no,'汪敏禄' as sales_name,'是' as is_part_time_service_manager,0 as sales_sale_rate,0 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '107398' as customer_no,'80974184;81107303' as service_user_work_no,'郭荔丽;姚细美' as service_user_name,'80974184' as work_no,'郭荔丽' as sales_name,'是' as is_part_time_service_manager,0 as sales_sale_rate,0 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '108127' as customer_no,'81133021' as service_user_work_no,'黄升' as service_user_name,'81133021' as work_no,'黄升' as sales_name,'是' as is_part_time_service_manager,0 as sales_sale_rate,0 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '108739' as customer_no,'81099343' as service_user_work_no,'林瑾鑫' as service_user_name,'81099343' as work_no,'林瑾鑫' as sales_name,'是' as is_part_time_service_manager,0 as sales_sale_rate,0 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '109377' as customer_no,'81099343' as service_user_work_no,'林瑾鑫' as service_user_name,'81099343' as work_no,'林瑾鑫' as sales_name,'是' as is_part_time_service_manager,0 as sales_sale_rate,0 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '111241' as customer_no,'80952742' as service_user_work_no,'王秀云' as service_user_name,'80952742' as work_no,'王秀云' as sales_name,'是' as is_part_time_service_manager,0 as sales_sale_rate,0 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '112044' as customer_no,'81082956' as service_user_work_no,'郑妍' as service_user_name,'81082956' as work_no,'郑妍' as sales_name,'是' as is_part_time_service_manager,0 as sales_sale_rate,0 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '112052' as customer_no,'81099343' as service_user_work_no,'林瑾鑫' as service_user_name,'81099343' as work_no,'林瑾鑫' as sales_name,'是' as is_part_time_service_manager,0 as sales_sale_rate,0 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '112053' as customer_no,'80816155' as service_user_work_no,'张磊磊' as service_user_name,'80816155' as work_no,'张磊磊' as sales_name,'是' as is_part_time_service_manager,0 as sales_sale_rate,0 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '112058' as customer_no,'81082956' as service_user_work_no,'郑妍' as service_user_name,'81082956' as work_no,'郑妍' as sales_name,'是' as is_part_time_service_manager,0 as sales_sale_rate,0 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '112062' as customer_no,'80816155' as service_user_work_no,'张磊磊' as service_user_name,'80816155' as work_no,'张磊磊' as sales_name,'是' as is_part_time_service_manager,0 as sales_sale_rate,0 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '112067' as customer_no,'81133021' as service_user_work_no,'黄升' as service_user_name,'81133021' as work_no,'黄升' as sales_name,'是' as is_part_time_service_manager,0 as sales_sale_rate,0 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '112327' as customer_no,'80691224' as service_user_work_no,'王少端' as service_user_name,'80691224' as work_no,'王少端' as sales_name,'是' as is_part_time_service_manager,0 as sales_sale_rate,0 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '113101' as customer_no,'80691224' as service_user_work_no,'王少端' as service_user_name,'80691224' as work_no,'王少端' as sales_name,'是' as is_part_time_service_manager,0 as sales_sale_rate,0 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '113635' as customer_no,'80691224' as service_user_work_no,'王少端' as service_user_name,'80691224' as work_no,'王少端' as sales_name,'是' as is_part_time_service_manager,0 as sales_sale_rate,0 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '113646' as customer_no,'80691224' as service_user_work_no,'王少端' as service_user_name,'80691224' as work_no,'王少端' as sales_name,'是' as is_part_time_service_manager,0 as sales_sale_rate,0 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '114344' as customer_no,'' as service_user_work_no,'' as service_user_name,'80005782' as work_no,'杨海燕' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '115324' as customer_no,'80816155' as service_user_work_no,'张磊磊' as service_user_name,'80816155' as work_no,'张磊磊' as sales_name,'是' as is_part_time_service_manager,0 as sales_sale_rate,0 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '115595' as customer_no,'80816155' as service_user_work_no,'张磊磊' as service_user_name,'80816155' as work_no,'张磊磊' as sales_name,'是' as is_part_time_service_manager,0 as sales_sale_rate,0 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '115602' as customer_no,'80974184;81107303' as service_user_work_no,'郭荔丽;姚细美' as service_user_name,'80974184' as work_no,'郭荔丽' as sales_name,'是' as is_part_time_service_manager,0 as sales_sale_rate,0 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '115646' as customer_no,'80816155' as service_user_work_no,'张磊磊' as service_user_name,'80816155' as work_no,'张磊磊' as sales_name,'是' as is_part_time_service_manager,0 as sales_sale_rate,0 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '115656' as customer_no,'80974184;81107303' as service_user_work_no,'郭荔丽;姚细美' as service_user_name,'80974184' as work_no,'郭荔丽' as sales_name,'是' as is_part_time_service_manager,0 as sales_sale_rate,0 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '115679' as customer_no,'80816155' as service_user_work_no,'张磊磊' as service_user_name,'80816155' as work_no,'张磊磊' as sales_name,'是' as is_part_time_service_manager,0 as sales_sale_rate,0 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '115826' as customer_no,'80974184;81107303' as service_user_work_no,'郭荔丽;姚细美' as service_user_name,'80974184' as work_no,'郭荔丽' as sales_name,'是' as is_part_time_service_manager,0 as sales_sale_rate,0 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '115857' as customer_no,'80816155' as service_user_work_no,'张磊磊' as service_user_name,'80816155' as work_no,'张磊磊' as sales_name,'是' as is_part_time_service_manager,0 as sales_sale_rate,0 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '116123' as customer_no,'81129243' as service_user_work_no,'汪敏禄' as service_user_name,'80917566' as work_no,'叶伟杰' as sales_name,'是' as is_part_time_service_manager,0 as sales_sale_rate,0 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '116211' as customer_no,'80816155' as service_user_work_no,'张磊磊' as service_user_name,'80816155' as work_no,'张磊磊' as sales_name,'是' as is_part_time_service_manager,0 as sales_sale_rate,0 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '117244' as customer_no,'80974184;81107303' as service_user_work_no,'郭荔丽;姚细美' as service_user_name,'80974184' as work_no,'郭荔丽' as sales_name,'是' as is_part_time_service_manager,0 as sales_sale_rate,0 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '118687' as customer_no,'81099343' as service_user_work_no,'林瑾鑫' as service_user_name,'81099343' as work_no,'林瑾鑫' as sales_name,'是' as is_part_time_service_manager,0 as sales_sale_rate,0 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select 'PF0094' as customer_no,'81099343' as service_user_work_no,'林瑾鑫' as service_user_name,'81099343' as work_no,'林瑾鑫' as sales_name,'是' as is_part_time_service_manager,0 as sales_sale_rate,0 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select 'PF1205' as customer_no,'80816155' as service_user_work_no,'张磊磊' as service_user_name,'80816155' as work_no,'张磊磊' as sales_name,'是' as is_part_time_service_manager,0 as sales_sale_rate,0 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '104666' as customer_no,'81101454' as service_user_work_no,'刘宇' as service_user_name,'80941561' as work_no,'杨艳' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '104883' as customer_no,'' as service_user_work_no,'' as service_user_name,'80887786' as work_no,'陈维强' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '105381' as customer_no,'81059406' as service_user_work_no,'郑婷婷' as service_user_name,'80875723' as work_no,'韩瑞荣' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '105717' as customer_no,'81075199' as service_user_work_no,'万荣' as service_user_name,'80875723' as work_no,'韩瑞荣' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '105821' as customer_no,'81075198' as service_user_work_no,'胡会丹' as service_user_name,'80887789' as work_no,'陈龙' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '106124' as customer_no,'81075199' as service_user_work_no,'万荣' as service_user_name,'80887786' as work_no,'陈维强' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '106314' as customer_no,'81075198' as service_user_work_no,'胡会丹' as service_user_name,'81084224' as work_no,'戚修圣' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '106354' as customer_no,'81059406' as service_user_work_no,'郑婷婷' as service_user_name,'80980883' as work_no,'罗春兰' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '106530' as customer_no,'81059406' as service_user_work_no,'郑婷婷' as service_user_name,'80875723' as work_no,'韩瑞荣' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '106989' as customer_no,'81075199' as service_user_work_no,'万荣' as service_user_name,'80950647' as work_no,'张伟炜' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '107346' as customer_no,'81075198' as service_user_work_no,'胡会丹' as service_user_name,'80928418' as work_no,'宋伟静' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '107363' as customer_no,'81075198' as service_user_work_no,'胡会丹' as service_user_name,'80980883' as work_no,'罗春兰' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '107514' as customer_no,'81075199' as service_user_work_no,'万荣' as service_user_name,'80887786' as work_no,'陈维强' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '107577' as customer_no,'81101454' as service_user_work_no,'刘宇' as service_user_name,'80941561' as work_no,'杨艳' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '107956' as customer_no,'81046822' as service_user_work_no,'王士玲' as service_user_name,'80959250' as work_no,'管琪' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '107986' as customer_no,'81046822' as service_user_work_no,'王士玲' as service_user_name,'81074192' as work_no,'乔雪' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '111137' as customer_no,'81075198' as service_user_work_no,'胡会丹' as service_user_name,'81074192' as work_no,'乔雪' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '111506' as customer_no,'81059406' as service_user_work_no,'郑婷婷' as service_user_name,'80980883' as work_no,'罗春兰' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '111566' as customer_no,'81075199' as service_user_work_no,'万荣' as service_user_name,'80948458' as work_no,'杨旻杰' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '112865' as customer_no,'81075198' as service_user_work_no,'胡会丹' as service_user_name,'80928418' as work_no,'宋伟静' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '113184' as customer_no,'81101454' as service_user_work_no,'刘宇' as service_user_name,'80941561' as work_no,'杨艳' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '114872' as customer_no,'81075198' as service_user_work_no,'胡会丹' as service_user_name,'80928418' as work_no,'宋伟静' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '114876' as customer_no,'81075199' as service_user_work_no,'万荣' as service_user_name,'80887786' as work_no,'陈维强' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '115156' as customer_no,'81059406' as service_user_work_no,'郑婷婷' as service_user_name,'80980883' as work_no,'罗春兰' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '115458' as customer_no,'81101454' as service_user_work_no,'刘宇' as service_user_name,'80980883' as work_no,'罗春兰' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '115520' as customer_no,'81101454' as service_user_work_no,'刘宇' as service_user_name,'80875723' as work_no,'韩瑞荣' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '115781' as customer_no,'81075198' as service_user_work_no,'胡会丹' as service_user_name,'80959250' as work_no,'管琪' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '115833' as customer_no,'81046822' as service_user_work_no,'王士玲' as service_user_name,'81074192' as work_no,'乔雪' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '116052' as customer_no,'81059406' as service_user_work_no,'郑婷婷' as service_user_name,'80980883' as work_no,'罗春兰' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '116467' as customer_no,'81101454' as service_user_work_no,'刘宇' as service_user_name,'80875723' as work_no,'韩瑞荣' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '117007' as customer_no,'81046822' as service_user_work_no,'王士玲' as service_user_name,'81074192' as work_no,'乔雪' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '117755' as customer_no,'81075199' as service_user_work_no,'万荣' as service_user_name,'80950647' as work_no,'张伟炜' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '117820' as customer_no,'81059406' as service_user_work_no,'郑婷婷' as service_user_name,'80980883' as work_no,'罗春兰' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '118187' as customer_no,'81046822' as service_user_work_no,'王士玲' as service_user_name,'80941561' as work_no,'杨艳' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '118920' as customer_no,'81101454' as service_user_work_no,'刘宇' as service_user_name,'80941561' as work_no,'杨艳' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '103995' as customer_no,'81102505' as service_user_work_no,'杜多' as service_user_name,'80907460' as work_no,'薛小伟' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '107683' as customer_no,'81102505' as service_user_work_no,'杜多' as service_user_name,'80907460' as work_no,'薛小伟' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '111122' as customer_no,'81111582' as service_user_work_no,'蔡汶艳' as service_user_name,'80958965' as work_no,'李斌' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '117057' as customer_no,'81097979' as service_user_work_no,'顾健' as service_user_name,'80853037' as work_no,'李晓燕' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '117095' as customer_no,'81111582' as service_user_work_no,'蔡汶艳' as service_user_name,'80958965' as work_no,'李斌' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '118540' as customer_no,'81111582' as service_user_work_no,'蔡汶艳' as service_user_name,'81016134' as work_no,'钱梓强' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '120044' as customer_no,'81111582' as service_user_work_no,'蔡汶艳' as service_user_name,'80953780' as work_no,'许佳惠' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '116957' as customer_no,'' as service_user_work_no,'' as service_user_name,'81055718' as work_no,'吴庆平' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '116629' as customer_no,'' as service_user_work_no,'' as service_user_name,'81055717' as work_no,'魏隆强' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '114524' as customer_no,'' as service_user_work_no,'' as service_user_name,'81086756' as work_no,'姚艳婷' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '115195' as customer_no,'' as service_user_work_no,'' as service_user_name,'81086756' as work_no,'姚艳婷' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '120024' as customer_no,'' as service_user_work_no,'' as service_user_name,'80952743' as work_no,'兰华明' as sales_name,'否' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '120317' as customer_no,'' as service_user_work_no,'' as service_user_name,'81084686' as work_no,'何丽姿' as sales_name,'否' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '121235' as customer_no,'80698149' as service_user_work_no,'魏丹' as service_user_name,'81113771' as work_no,'孟子寒' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '121441' as customer_no,'80698149' as service_user_work_no,'魏丹' as service_user_name,'81116795' as work_no,'程科研' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '122261' as customer_no,'80698149' as service_user_work_no,'魏丹' as service_user_name,'81113856' as work_no,'景昊宇' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '113352' as customer_no,'80870416' as service_user_work_no,'陈雯' as service_user_name,'81014258' as work_no,'朱龙华' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '115535' as customer_no,'80870416' as service_user_work_no,'陈雯' as service_user_name,'81014258' as work_no,'朱龙华' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '118032' as customer_no,'80870416' as service_user_work_no,'陈雯' as service_user_name,'81014258' as work_no,'朱龙华' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '119218' as customer_no,'80870416' as service_user_work_no,'陈雯' as service_user_name,'81112990' as work_no,'成恒博' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '111647' as customer_no,'81055334' as service_user_work_no,'王媛' as service_user_name,'81041503' as work_no,'蔡依贝' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '113151' as customer_no,'81055334' as service_user_work_no,'王媛' as service_user_name,'81014892' as work_no,'张峰源' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '113672' as customer_no,'81055334' as service_user_work_no,'王媛' as service_user_name,'81025317' as work_no,'池冬妮' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '113783' as customer_no,'81055334' as service_user_work_no,'王媛' as service_user_name,'81054983' as work_no,'杜治廷' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '114652' as customer_no,'81055334' as service_user_work_no,'王媛' as service_user_name,'81041503' as work_no,'蔡依贝' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '114799' as customer_no,'81055334' as service_user_work_no,'王媛' as service_user_name,'81014892' as work_no,'张峰源' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '115102' as customer_no,'81055334' as service_user_work_no,'王媛' as service_user_name,'81041503' as work_no,'蔡依贝' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '116071' as customer_no,'81055334' as service_user_work_no,'王媛' as service_user_name,'81041503' as work_no,'蔡依贝' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '117317' as customer_no,'81055334' as service_user_work_no,'王媛' as service_user_name,'81014892' as work_no,'张峰源' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '117702' as customer_no,'81055334' as service_user_work_no,'王媛' as service_user_name,'81041503' as work_no,'蔡依贝' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '117811' as customer_no,'81055334' as service_user_work_no,'王媛' as service_user_name,'81054983' as work_no,'杜治廷' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '118174' as customer_no,'81055334' as service_user_work_no,'王媛' as service_user_name,'81054983' as work_no,'杜治廷' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '118595' as customer_no,'81055334' as service_user_work_no,'王媛' as service_user_name,'81041503' as work_no,'蔡依贝' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '118815' as customer_no,'81055334' as service_user_work_no,'王媛' as service_user_name,'80978886' as work_no,'曹杰' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '118868' as customer_no,'81055334' as service_user_work_no,'王媛' as service_user_name,'81041503' as work_no,'蔡依贝' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '119872' as customer_no,'81055334' as service_user_work_no,'王媛' as service_user_name,'81041503' as work_no,'蔡依贝' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '119918' as customer_no,'81055334' as service_user_work_no,'王媛' as service_user_name,'81014892' as work_no,'张峰源' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '120021' as customer_no,'81055334' as service_user_work_no,'王媛' as service_user_name,'81014892' as work_no,'张峰源' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '121457' as customer_no,'81055334' as service_user_work_no,'王媛' as service_user_name,'81014892' as work_no,'张峰源' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '121781' as customer_no,'81055334' as service_user_work_no,'王媛' as service_user_name,'81014892' as work_no,'张峰源' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '113926' as customer_no,'81063577' as service_user_work_no,'唐楠' as service_user_name,'81001235' as work_no,'田志伟' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '114964' as customer_no,'81063577' as service_user_work_no,'唐楠' as service_user_name,'81001235' as work_no,'田志伟' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '116170' as customer_no,'81063577' as service_user_work_no,'唐楠' as service_user_name,'81001235' as work_no,'田志伟' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '118816' as customer_no,'81063577' as service_user_work_no,'唐楠' as service_user_name,'11000022' as work_no,'陕西B' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '119825' as customer_no,'81063577' as service_user_work_no,'唐楠' as service_user_name,'81001235' as work_no,'田志伟' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '115300' as customer_no,'81104239' as service_user_work_no,'张超峰' as service_user_name,'11000022' as work_no,'陕西B' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '118780' as customer_no,'81104239' as service_user_work_no,'张超峰' as service_user_name,'11000022' as work_no,'陕西B' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '115042' as customer_no,'81113197' as service_user_work_no,'席静' as service_user_name,'81001235' as work_no,'田志伟' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '117249' as customer_no,'81119373' as service_user_work_no,'赵俊俊' as service_user_name,'81001235' as work_no,'田志伟' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '111135' as customer_no,'81123797' as service_user_work_no,'宋唐珂' as service_user_name,'81001235' as work_no,'田志伟' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '112808' as customer_no,'81123797' as service_user_work_no,'宋唐珂' as service_user_name,'81001235' as work_no,'田志伟' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '113784' as customer_no,'81123797' as service_user_work_no,'宋唐珂' as service_user_name,'81001235' as work_no,'田志伟' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '119231' as customer_no,'81123797' as service_user_work_no,'宋唐珂' as service_user_name,'81001235' as work_no,'田志伟' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '122219' as customer_no,'81123797' as service_user_work_no,'宋唐珂' as service_user_name,'81113771' as work_no,'孟子寒' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '102534' as customer_no,'80980977' as service_user_work_no,'黄小红' as service_user_name,'80980977' as work_no,'黄小红' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '102798' as customer_no,'80980977' as service_user_work_no,'黄小红' as service_user_name,'80980977' as work_no,'黄小红' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '102806' as customer_no,'80980977' as service_user_work_no,'黄小红' as service_user_name,'80980977' as work_no,'黄小红' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '105186' as customer_no,'80980977' as service_user_work_no,'黄小红' as service_user_name,'80980977' as work_no,'黄小红' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '105480' as customer_no,'80909562' as service_user_work_no,'陈乾' as service_user_name,'80909562' as work_no,'陈乾' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '105540' as customer_no,'80909562' as service_user_work_no,'陈乾' as service_user_name,'80909562' as work_no,'陈乾' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '106469' as customer_no,'80909562' as service_user_work_no,'陈乾' as service_user_name,'80909562' as work_no,'陈乾' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '106524' as customer_no,'80909562' as service_user_work_no,'陈乾' as service_user_name,'80909562' as work_no,'陈乾' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '106538' as customer_no,'80909562' as service_user_work_no,'陈乾' as service_user_name,'80909562' as work_no,'陈乾' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '107438' as customer_no,'80909562' as service_user_work_no,'陈乾' as service_user_name,'80909562' as work_no,'陈乾' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '111892' as customer_no,'80909562' as service_user_work_no,'陈乾' as service_user_name,'80909562' as work_no,'陈乾' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '112210' as customer_no,'80909562' as service_user_work_no,'陈乾' as service_user_name,'80909562' as work_no,'陈乾' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '115915' as customer_no,'80980977' as service_user_work_no,'黄小红' as service_user_name,'80980977' as work_no,'黄小红' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '117022' as customer_no,'80909562' as service_user_work_no,'陈乾' as service_user_name,'80909562' as work_no,'陈乾' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '103945' as customer_no,'80898492' as service_user_work_no,'李玉琴' as service_user_name,'80083850' as work_no,'张辉' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '104222' as customer_no,'80898492' as service_user_work_no,'李玉琴' as service_user_name,'80083850' as work_no,'张辉' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '104229' as customer_no,'80898492' as service_user_work_no,'李玉琴' as service_user_name,'80083850' as work_no,'张辉' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '104241' as customer_no,'80898492' as service_user_work_no,'李玉琴' as service_user_name,'80083850' as work_no,'张辉' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '104251' as customer_no,'80898492' as service_user_work_no,'李玉琴' as service_user_name,'80083850' as work_no,'张辉' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '104255' as customer_no,'80898492' as service_user_work_no,'李玉琴' as service_user_name,'80083850' as work_no,'张辉' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '104414' as customer_no,'80898492' as service_user_work_no,'李玉琴' as service_user_name,'80083850' as work_no,'张辉' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '104762' as customer_no,'81030501' as service_user_work_no,'熊龙钦' as service_user_name,'80121234' as work_no,'虚拟业务-刘' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '104872' as customer_no,'81062857' as service_user_work_no,'蒋冬玲' as service_user_name,'83402973' as work_no,'虚拟业务-赵' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '104965' as customer_no,'80898492' as service_user_work_no,'李玉琴' as service_user_name,'80083850' as work_no,'张辉' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '105005' as customer_no,'80898492' as service_user_work_no,'李玉琴' as service_user_name,'80083850' as work_no,'张辉' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '105024' as customer_no,'80898492' as service_user_work_no,'李玉琴' as service_user_name,'80083850' as work_no,'张辉' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '105247' as customer_no,'81030501' as service_user_work_no,'熊龙钦' as service_user_name,'80902387' as work_no,'张秋菊' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '105483' as customer_no,'80909562' as service_user_work_no,'陈乾' as service_user_name,'80909562' as work_no,'陈乾' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '105518' as customer_no,'81051770' as service_user_work_no,'尹晓余' as service_user_name,'80913082' as work_no,'袁明华' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '105521' as customer_no,'81030501' as service_user_work_no,'熊龙钦' as service_user_name,'80902387' as work_no,'张秋菊' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '105569' as customer_no,'81030501' as service_user_work_no,'熊龙钦' as service_user_name,'80902387' as work_no,'张秋菊' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '105639' as customer_no,'81095973' as service_user_work_no,'郭冠男' as service_user_name,'80912842' as work_no,'刘悦' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '105715' as customer_no,'81051770' as service_user_work_no,'尹晓余' as service_user_name,'80913082' as work_no,'袁明华' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '105756' as customer_no,'80920125' as service_user_work_no,'熊朱丽' as service_user_name,'80083850' as work_no,'张辉' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '105768' as customer_no,'81126405' as service_user_work_no,'冯饴' as service_user_name,'80927654' as work_no,'向亚聪' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '105790' as customer_no,'80085036' as service_user_work_no,'李丹' as service_user_name,'80895095' as work_no,'罗泽剑' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '105802' as customer_no,'80085036' as service_user_work_no,'李丹' as service_user_name,'80895095' as work_no,'罗泽剑' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '106095' as customer_no,'80619984' as service_user_work_no,'翟敏' as service_user_name,'80927653' as work_no,'陈炳' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '106300' as customer_no,'80909562' as service_user_work_no,'陈乾' as service_user_name,'80909562' as work_no,'陈乾' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '106317' as customer_no,'81021726' as service_user_work_no,'钟丹' as service_user_name,'80933752' as work_no,'闫帅' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '106434' as customer_no,'80085036' as service_user_work_no,'李丹' as service_user_name,'80895095' as work_no,'罗泽剑' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '106521' as customer_no,'80936439' as service_user_work_no,'曾江鹏' as service_user_name,'80878413' as work_no,'欧启' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '106572' as customer_no,'81051770' as service_user_work_no,'尹晓余' as service_user_name,'80913082' as work_no,'袁明华' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '106602' as customer_no,'81095973' as service_user_work_no,'郭冠男' as service_user_name,'80912842' as work_no,'刘悦' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '106693' as customer_no,'80619984' as service_user_work_no,'翟敏' as service_user_name,'80927653' as work_no,'陈炳' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '106737' as customer_no,'81126405' as service_user_work_no,'冯饴' as service_user_name,'80927654' as work_no,'向亚聪' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '106898' as customer_no,'81092475' as service_user_work_no,'冉启薇' as service_user_name,'80901735' as work_no,'刘川龙' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '106925' as customer_no,'81126405' as service_user_work_no,'冯饴' as service_user_name,'80927654' as work_no,'向亚聪' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '106958' as customer_no,'80619984' as service_user_work_no,'翟敏' as service_user_name,'80927653' as work_no,'陈炳' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '106987' as customer_no,'81095973' as service_user_work_no,'郭冠男' as service_user_name,'80912842' as work_no,'刘悦' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '107000' as customer_no,'80085036' as service_user_work_no,'李丹' as service_user_name,'80895095' as work_no,'罗泽剑' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '107050' as customer_no,'81030501' as service_user_work_no,'熊龙钦' as service_user_name,'80121234' as work_no,'虚拟业务-刘' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '107073' as customer_no,'81126405' as service_user_work_no,'冯饴' as service_user_name,'80927654' as work_no,'向亚聪' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '107255' as customer_no,'81095973' as service_user_work_no,'郭冠男' as service_user_name,'80912842' as work_no,'刘悦' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '107276' as customer_no,'80619984' as service_user_work_no,'翟敏' as service_user_name,'80927653' as work_no,'陈炳' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '107453' as customer_no,'81091240' as service_user_work_no,'汤莎' as service_user_name,'80960714' as work_no,'李远军' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '107461' as customer_no,'81091240' as service_user_work_no,'汤莎' as service_user_name,'80960714' as work_no,'李远军' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '107500' as customer_no,'81091240' as service_user_work_no,'汤莎' as service_user_name,'80960714' as work_no,'李远军' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '107593' as customer_no,'81092475' as service_user_work_no,'冉启薇' as service_user_name,'80901735' as work_no,'刘川龙' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '107655' as customer_no,'81051770' as service_user_work_no,'尹晓余' as service_user_name,'80913082' as work_no,'袁明华' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '107674' as customer_no,'81095973' as service_user_work_no,'郭冠男' as service_user_name,'80912842' as work_no,'刘悦' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '107838' as customer_no,'80619984' as service_user_work_no,'翟敏' as service_user_name,'80927653' as work_no,'陈炳' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '108021' as customer_no,'81030501' as service_user_work_no,'熊龙钦' as service_user_name,'80121234' as work_no,'虚拟业务-刘' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '108176' as customer_no,'81091240' as service_user_work_no,'汤莎' as service_user_name,'80960714' as work_no,'李远军' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '108749' as customer_no,'81051770' as service_user_work_no,'尹晓余' as service_user_name,'80913082' as work_no,'袁明华' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '108795' as customer_no,'81091240' as service_user_work_no,'汤莎' as service_user_name,'80960714' as work_no,'李远军' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '108806' as customer_no,'81095973' as service_user_work_no,'郭冠男' as service_user_name,'80912842' as work_no,'刘悦' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '108837' as customer_no,'81051770' as service_user_work_no,'尹晓余' as service_user_name,'80913082' as work_no,'袁明华' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '108853' as customer_no,'81051770' as service_user_work_no,'尹晓余' as service_user_name,'80913082' as work_no,'袁明华' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '108960' as customer_no,'81092475' as service_user_work_no,'冉启薇' as service_user_name,'80901735' as work_no,'刘川龙' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '109291' as customer_no,'81030501' as service_user_work_no,'熊龙钦' as service_user_name,'80121234' as work_no,'虚拟业务-刘' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '109349' as customer_no,'81126405' as service_user_work_no,'冯饴' as service_user_name,'80927654' as work_no,'向亚聪' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '109381' as customer_no,'81021726' as service_user_work_no,'钟丹' as service_user_name,'80933752' as work_no,'闫帅' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '109786' as customer_no,'81126405' as service_user_work_no,'冯饴' as service_user_name,'80927654' as work_no,'向亚聪' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '109977' as customer_no,'81062857' as service_user_work_no,'蒋冬玲' as service_user_name,'83402973' as work_no,'虚拟业务-赵' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '110664' as customer_no,'81051770' as service_user_work_no,'尹晓余' as service_user_name,'80913082' as work_no,'袁明华' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '111074' as customer_no,'81051770' as service_user_work_no,'尹晓余' as service_user_name,'80913082' as work_no,'袁明华' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '111219' as customer_no,'80085036' as service_user_work_no,'李丹' as service_user_name,'80895095' as work_no,'罗泽剑' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '111349' as customer_no,'81095973' as service_user_work_no,'郭冠男' as service_user_name,'80912842' as work_no,'刘悦' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '111556' as customer_no,'81021726' as service_user_work_no,'钟丹' as service_user_name,'80933752' as work_no,'闫帅' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '111771' as customer_no,'81126405' as service_user_work_no,'冯饴' as service_user_name,'80927654' as work_no,'向亚聪' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '112754' as customer_no,'81021726' as service_user_work_no,'钟丹' as service_user_name,'80933752' as work_no,'闫帅' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '112813' as customer_no,'80898492' as service_user_work_no,'李玉琴' as service_user_name,'80083850' as work_no,'张辉' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '112911' as customer_no,'81126405' as service_user_work_no,'冯饴' as service_user_name,'80927654' as work_no,'向亚聪' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '113105' as customer_no,'81062857' as service_user_work_no,'蒋冬玲' as service_user_name,'83402973' as work_no,'虚拟业务-赵' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '113152' as customer_no,'81021726' as service_user_work_no,'钟丹' as service_user_name,'80933752' as work_no,'闫帅' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '113274' as customer_no,'81126405' as service_user_work_no,'冯饴' as service_user_name,'80927654' as work_no,'向亚聪' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '113463' as customer_no,'81030501' as service_user_work_no,'熊龙钦' as service_user_name,'80121234' as work_no,'虚拟业务-刘' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '113564' as customer_no,'81051770' as service_user_work_no,'尹晓余' as service_user_name,'80913082' as work_no,'袁明华' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '113571' as customer_no,'81051770' as service_user_work_no,'尹晓余' as service_user_name,'80913082' as work_no,'袁明华' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '113590' as customer_no,'81051770' as service_user_work_no,'尹晓余' as service_user_name,'80913082' as work_no,'袁明华' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '113643' as customer_no,'81126405' as service_user_work_no,'冯饴' as service_user_name,'80927654' as work_no,'向亚聪' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '113645' as customer_no,'81051770' as service_user_work_no,'尹晓余' as service_user_name,'80913082' as work_no,'袁明华' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '114127' as customer_no,'81030501' as service_user_work_no,'熊龙钦' as service_user_name,'80902387' as work_no,'张秋菊' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '114287' as customer_no,'81030501' as service_user_work_no,'熊龙钦' as service_user_name,'80902387' as work_no,'张秋菊' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '114354' as customer_no,'81051770' as service_user_work_no,'尹晓余' as service_user_name,'80913082' as work_no,'袁明华' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '114437' as customer_no,'80619984' as service_user_work_no,'翟敏' as service_user_name,'80927653' as work_no,'陈炳' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '114505' as customer_no,'80619984' as service_user_work_no,'翟敏' as service_user_name,'80927653' as work_no,'陈炳' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '114704' as customer_no,'81030501' as service_user_work_no,'熊龙钦' as service_user_name,'80902387' as work_no,'张秋菊' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '114784' as customer_no,'80619984' as service_user_work_no,'翟敏' as service_user_name,'80927653' as work_no,'陈炳' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '114800' as customer_no,'80085036' as service_user_work_no,'李丹' as service_user_name,'80895095' as work_no,'罗泽剑' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '114821' as customer_no,'80898492' as service_user_work_no,'李玉琴' as service_user_name,'80083850' as work_no,'张辉' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '114921' as customer_no,'81095973' as service_user_work_no,'郭冠男' as service_user_name,'80912842' as work_no,'刘悦' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '115082' as customer_no,'80085036' as service_user_work_no,'李丹' as service_user_name,'80895095' as work_no,'罗泽剑' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '115178' as customer_no,'81091240' as service_user_work_no,'汤莎' as service_user_name,'80960714' as work_no,'李远军' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '115259' as customer_no,'81089258' as service_user_work_no,'邓新渝' as service_user_name,'81039868' as work_no,'刘昱廷' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '115284' as customer_no,'81095973' as service_user_work_no,'郭冠男' as service_user_name,'80912842' as work_no,'刘悦' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '115294' as customer_no,'81095973' as service_user_work_no,'郭冠男' as service_user_name,'80912842' as work_no,'刘悦' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '115407' as customer_no,'80619984' as service_user_work_no,'翟敏' as service_user_name,'80927653' as work_no,'陈炳' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '115527' as customer_no,'81051770' as service_user_work_no,'尹晓余' as service_user_name,'80913082' as work_no,'袁明华' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '115710' as customer_no,'81091240' as service_user_work_no,'汤莎' as service_user_name,'80960714' as work_no,'李远军' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '115742' as customer_no,'80936439' as service_user_work_no,'曾江鹏' as service_user_name,'80878413' as work_no,'欧启' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '115807' as customer_no,'81091240' as service_user_work_no,'汤莎' as service_user_name,'80960714' as work_no,'李远军' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '116141' as customer_no,'81095973' as service_user_work_no,'郭冠男' as service_user_name,'80912842' as work_no,'刘悦' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '116485' as customer_no,'80085036' as service_user_work_no,'李丹' as service_user_name,'80895095' as work_no,'罗泽剑' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '116539' as customer_no,'81126405' as service_user_work_no,'冯饴' as service_user_name,'80927654' as work_no,'向亚聪' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '116665' as customer_no,'81126405' as service_user_work_no,'冯饴' as service_user_name,'80927654' as work_no,'向亚聪' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '116670' as customer_no,'81089258' as service_user_work_no,'邓新渝' as service_user_name,'81039868' as work_no,'刘昱廷' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '116762' as customer_no,'80619984' as service_user_work_no,'翟敏' as service_user_name,'80927653' as work_no,'陈炳' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '116847' as customer_no,'80619984' as service_user_work_no,'翟敏' as service_user_name,'80927653' as work_no,'陈炳' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '116928' as customer_no,'81091240' as service_user_work_no,'汤莎' as service_user_name,'80960714' as work_no,'李远军' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '116943' as customer_no,'80085036' as service_user_work_no,'李丹' as service_user_name,'80895095' as work_no,'罗泽剑' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '116962' as customer_no,'81091240' as service_user_work_no,'汤莎' as service_user_name,'80960714' as work_no,'李远军' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '116988' as customer_no,'80085036' as service_user_work_no,'李丹' as service_user_name,'80895095' as work_no,'罗泽剑' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '116998' as customer_no,'80085036' as service_user_work_no,'李丹' as service_user_name,'80895095' as work_no,'罗泽剑' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '117016' as customer_no,'80085036' as service_user_work_no,'李丹' as service_user_name,'80895095' as work_no,'罗泽剑' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '117020' as customer_no,'80085036' as service_user_work_no,'李丹' as service_user_name,'80895095' as work_no,'罗泽剑' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '117058' as customer_no,'81089258' as service_user_work_no,'邓新渝' as service_user_name,'81039868' as work_no,'刘昱廷' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '117067' as customer_no,'80909562' as service_user_work_no,'陈乾' as service_user_name,'80909562' as work_no,'陈乾' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '117068' as customer_no,'80936439' as service_user_work_no,'曾江鹏' as service_user_name,'80878413' as work_no,'欧启' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '117116' as customer_no,'80619984' as service_user_work_no,'翟敏' as service_user_name,'80927653' as work_no,'陈炳' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '117120' as customer_no,'80619984' as service_user_work_no,'翟敏' as service_user_name,'80927653' as work_no,'陈炳' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '117348' as customer_no,'81051770' as service_user_work_no,'尹晓余' as service_user_name,'80913082' as work_no,'袁明华' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '117454' as customer_no,'80085036' as service_user_work_no,'李丹' as service_user_name,'80895095' as work_no,'罗泽剑' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '117533' as customer_no,'80619984' as service_user_work_no,'翟敏' as service_user_name,'80927653' as work_no,'陈炳' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '117543' as customer_no,'81126405' as service_user_work_no,'冯饴' as service_user_name,'80927654' as work_no,'向亚聪' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '117727' as customer_no,'80980977' as service_user_work_no,'黄小红' as service_user_name,'80980977' as work_no,'黄小红' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '117728' as customer_no,'80619984' as service_user_work_no,'翟敏' as service_user_name,'80619984' as work_no,'翟敏' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '117729' as customer_no,'80619984' as service_user_work_no,'翟敏' as service_user_name,'80927653' as work_no,'陈炳' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '117748' as customer_no,'80898492' as service_user_work_no,'李玉琴' as service_user_name,'80927653' as work_no,'陈炳' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '117749' as customer_no,'80619984' as service_user_work_no,'翟敏' as service_user_name,'81027274' as work_no,'李亚秋' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '117753' as customer_no,'80898492' as service_user_work_no,'李玉琴' as service_user_name,'80083850' as work_no,'张辉' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '117761' as customer_no,'80619984' as service_user_work_no,'翟敏' as service_user_name,'80619984' as work_no,'翟敏' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '117766' as customer_no,'80619984' as service_user_work_no,'翟敏' as service_user_name,'80927653' as work_no,'陈炳' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '117773' as customer_no,'80619984' as service_user_work_no,'翟敏' as service_user_name,'80619984' as work_no,'翟敏' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '117776' as customer_no,'80898492' as service_user_work_no,'李玉琴' as service_user_name,'80927653' as work_no,'陈炳' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '117777' as customer_no,'80619984' as service_user_work_no,'翟敏' as service_user_name,'80927653' as work_no,'陈炳' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '117781' as customer_no,'80619984' as service_user_work_no,'翟敏' as service_user_name,'81027274' as work_no,'李亚秋' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '117782' as customer_no,'80619984' as service_user_work_no,'翟敏' as service_user_name,'80927653' as work_no,'陈炳' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '117783' as customer_no,'80619984' as service_user_work_no,'翟敏' as service_user_name,'81027274' as work_no,'李亚秋' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '117784' as customer_no,'80619984' as service_user_work_no,'翟敏' as service_user_name,'81027274' as work_no,'李亚秋' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '117785' as customer_no,'80980977' as service_user_work_no,'黄小红' as service_user_name,'80980977' as work_no,'黄小红' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '117786' as customer_no,'80619984' as service_user_work_no,'翟敏' as service_user_name,'81027274' as work_no,'李亚秋' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '117790' as customer_no,'80980977' as service_user_work_no,'黄小红' as service_user_name,'80980977' as work_no,'黄小红' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '117791' as customer_no,'80898492' as service_user_work_no,'李玉琴' as service_user_name,'80927653' as work_no,'陈炳' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '117795' as customer_no,'80980977' as service_user_work_no,'黄小红' as service_user_name,'80980977' as work_no,'黄小红' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '117796' as customer_no,'80619984' as service_user_work_no,'翟敏' as service_user_name,'80927653' as work_no,'陈炳' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '117800' as customer_no,'80619984' as service_user_work_no,'翟敏' as service_user_name,'80619984' as work_no,'翟敏' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '117805' as customer_no,'80619984' as service_user_work_no,'翟敏' as service_user_name,'80619984' as work_no,'翟敏' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '117860' as customer_no,'81030501' as service_user_work_no,'熊龙钦' as service_user_name,'80902387' as work_no,'张秋菊' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '117918' as customer_no,'80619984' as service_user_work_no,'翟敏' as service_user_name,'80927653' as work_no,'陈炳' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '118026' as customer_no,'80909562' as service_user_work_no,'陈乾' as service_user_name,'80909562' as work_no,'陈乾' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '118239' as customer_no,'81126405' as service_user_work_no,'冯饴' as service_user_name,'80927654' as work_no,'向亚聪' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '118345' as customer_no,'81051770' as service_user_work_no,'尹晓余' as service_user_name,'80913082' as work_no,'袁明华' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '118470' as customer_no,'81089258' as service_user_work_no,'邓新渝' as service_user_name,'81039868' as work_no,'刘昱廷' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '118475' as customer_no,'81051770' as service_user_work_no,'尹晓余' as service_user_name,'80913082' as work_no,'袁明华' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '118768' as customer_no,'81021726' as service_user_work_no,'钟丹' as service_user_name,'80933752' as work_no,'闫帅' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '118933' as customer_no,'81091240' as service_user_work_no,'汤莎' as service_user_name,'80960714' as work_no,'李远军' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '118943' as customer_no,'80085036' as service_user_work_no,'李丹' as service_user_name,'80895095' as work_no,'罗泽剑' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '118973' as customer_no,'81089258' as service_user_work_no,'邓新渝' as service_user_name,'81039868' as work_no,'刘昱廷' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '119076' as customer_no,'80619984' as service_user_work_no,'翟敏' as service_user_name,'80927653' as work_no,'陈炳' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '119252' as customer_no,'81030501' as service_user_work_no,'熊龙钦' as service_user_name,'80902387' as work_no,'张秋菊' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '119513' as customer_no,'81126405' as service_user_work_no,'冯饴' as service_user_name,'80927654' as work_no,'向亚聪' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '119543' as customer_no,'81051770' as service_user_work_no,'尹晓余' as service_user_name,'80913082' as work_no,'袁明华' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '119659' as customer_no,'81089258' as service_user_work_no,'邓新渝' as service_user_name,'81039868' as work_no,'刘昱廷' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '119756' as customer_no,'81089258' as service_user_work_no,'邓新渝' as service_user_name,'81039868' as work_no,'刘昱廷' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '120031' as customer_no,'80909562' as service_user_work_no,'陈乾' as service_user_name,'80909562' as work_no,'陈乾' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '120092' as customer_no,'81089258' as service_user_work_no,'邓新渝' as service_user_name,'81039868' as work_no,'刘昱廷' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '120105' as customer_no,'81089258' as service_user_work_no,'邓新渝' as service_user_name,'81039868' as work_no,'刘昱廷' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '120244' as customer_no,'80619984' as service_user_work_no,'翟敏' as service_user_name,'80927653' as work_no,'陈炳' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '120272' as customer_no,'80619984' as service_user_work_no,'翟敏' as service_user_name,'80927653' as work_no,'陈炳' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '120359' as customer_no,'81030501' as service_user_work_no,'熊龙钦' as service_user_name,'80902387' as work_no,'张秋菊' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '120476' as customer_no,'81051770' as service_user_work_no,'尹晓余' as service_user_name,'80913082' as work_no,'袁明华' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '120486' as customer_no,'80619984' as service_user_work_no,'翟敏' as service_user_name,'80927653' as work_no,'陈炳' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '120497' as customer_no,'81089258' as service_user_work_no,'邓新渝' as service_user_name,'81039868' as work_no,'刘昱廷' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '120541' as customer_no,'81095973' as service_user_work_no,'郭冠男' as service_user_name,'80912842' as work_no,'刘悦' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '120581' as customer_no,'81126405' as service_user_work_no,'冯饴' as service_user_name,'80927654' as work_no,'向亚聪' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '120640' as customer_no,'81126405' as service_user_work_no,'冯饴' as service_user_name,'80927654' as work_no,'向亚聪' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '120645' as customer_no,'81051770' as service_user_work_no,'尹晓余' as service_user_name,'80913082' as work_no,'袁明华' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '120811' as customer_no,'81089258' as service_user_work_no,'邓新渝' as service_user_name,'81039868' as work_no,'刘昱廷' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '120860' as customer_no,'80085036' as service_user_work_no,'李丹' as service_user_name,'80895095' as work_no,'罗泽剑' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '120958' as customer_no,'81126405' as service_user_work_no,'冯饴' as service_user_name,'80927654' as work_no,'向亚聪' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '120983' as customer_no,'81030501' as service_user_work_no,'熊龙钦' as service_user_name,'80902387' as work_no,'张秋菊' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '121026' as customer_no,'81095973' as service_user_work_no,'郭冠男' as service_user_name,'80912842' as work_no,'刘悦' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '121045' as customer_no,'80085036' as service_user_work_no,'李丹' as service_user_name,'80895095' as work_no,'罗泽剑' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '121074' as customer_no,'81095973' as service_user_work_no,'郭冠男' as service_user_name,'80912842' as work_no,'刘悦' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '121081' as customer_no,'81095973' as service_user_work_no,'郭冠男' as service_user_name,'80912842' as work_no,'刘悦' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '121340' as customer_no,'81091240' as service_user_work_no,'汤莎' as service_user_name,'80960714' as work_no,'李远军' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '121415' as customer_no,'81089258' as service_user_work_no,'邓新渝' as service_user_name,'81039868' as work_no,'刘昱廷' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '121610' as customer_no,'81089258' as service_user_work_no,'邓新渝' as service_user_name,'81039868' as work_no,'刘昱廷' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '121767' as customer_no,'81126405' as service_user_work_no,'冯饴' as service_user_name,'80927654' as work_no,'向亚聪' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '122052' as customer_no,'81089258' as service_user_work_no,'邓新渝' as service_user_name,'81039868' as work_no,'刘昱廷' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '122087' as customer_no,'81021726' as service_user_work_no,'钟丹' as service_user_name,'80933752' as work_no,'闫帅' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '122532' as customer_no,'80936439' as service_user_work_no,'曾江鹏' as service_user_name,'80878413' as work_no,'欧启' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '122592' as customer_no,'81126405' as service_user_work_no,'冯饴' as service_user_name,'80927654' as work_no,'向亚聪' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '122670' as customer_no,'81051770' as service_user_work_no,'尹晓余' as service_user_name,'80913082' as work_no,'袁明华' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '122730' as customer_no,'81089258' as service_user_work_no,'邓新渝' as service_user_name,'81039868' as work_no,'刘昱廷' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '112803' as customer_no,'81133412' as service_user_work_no,'侯芳' as service_user_name,'81125246' as work_no,'邓金龙' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '115254' as customer_no,'81133412' as service_user_work_no,'侯芳' as service_user_name,'81125246' as work_no,'邓金龙' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '115471' as customer_no,'81133412' as service_user_work_no,'侯芳' as service_user_name,'81049168' as work_no,'莫昌川' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '117911' as customer_no,'81133412' as service_user_work_no,'侯芳' as service_user_name,'81049168' as work_no,'莫昌川' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '118670' as customer_no,'81133412' as service_user_work_no,'侯芳' as service_user_name,'81049168' as work_no,'莫昌川' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '120982' as customer_no,'81133412' as service_user_work_no,'侯芳' as service_user_name,'81125246' as work_no,'邓金龙' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '121870' as customer_no,'81133412' as service_user_work_no,'侯芳' as service_user_name,'81049168' as work_no,'莫昌川' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '110017' as customer_no,'81133021' as service_user_work_no,'黄升' as service_user_name,'80980614' as work_no,'池春梅' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '113263' as customer_no,'81133021' as service_user_work_no,'黄升' as service_user_name,'80980614' as work_no,'池春梅' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '114812' as customer_no,'81131450' as service_user_work_no,'张珠妹' as service_user_name,'80980614' as work_no,'池春梅' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '115366' as customer_no,'81131450' as service_user_work_no,'张珠妹' as service_user_name,'80980614' as work_no,'池春梅' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '115883' as customer_no,'81053313' as service_user_work_no,'段冬冬' as service_user_name,'80890405' as work_no,'瞿林峰' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '106516' as customer_no,'' as service_user_work_no,'' as service_user_name,'80404654' as work_no,'聂淑梅' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '106945' as customer_no,'' as service_user_work_no,'' as service_user_name,'80404654' as work_no,'聂淑梅' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '108293' as customer_no,'' as service_user_work_no,'' as service_user_name,'80404654' as work_no,'聂淑梅' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '109014' as customer_no,'' as service_user_work_no,'' as service_user_name,'80404654' as work_no,'聂淑梅' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '111009' as customer_no,'' as service_user_work_no,'' as service_user_name,'80404654' as work_no,'聂淑梅' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '113085' as customer_no,'' as service_user_work_no,'' as service_user_name,'80404654' as work_no,'聂淑梅' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '113400' as customer_no,'' as service_user_work_no,'' as service_user_name,'80404654' as work_no,'聂淑梅' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '115402' as customer_no,'' as service_user_work_no,'' as service_user_name,'80404654' as work_no,'聂淑梅' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '115508' as customer_no,'' as service_user_work_no,'' as service_user_name,'80404654' as work_no,'聂淑梅' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '118238' as customer_no,'' as service_user_work_no,'' as service_user_name,'80404654' as work_no,'聂淑梅' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '119976' as customer_no,'' as service_user_work_no,'' as service_user_name,'80404654' as work_no,'聂淑梅' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '121338' as customer_no,'' as service_user_work_no,'' as service_user_name,'80404654' as work_no,'聂淑梅' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '121606' as customer_no,'' as service_user_work_no,'' as service_user_name,'80404654' as work_no,'聂淑梅' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate

	
; --5


--订单应收金额、逾期日期、逾期天数
drop table csx_tmp.tmp_tc_cust_order_overdue_dtl_0; --6
create table csx_tmp.tmp_tc_cust_order_overdue_dtl_0
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
	--if(a.account_period_code like 'Y%', if(a.account_period_val = 31, 45, a.account_period_val + 15), a.account_period_val) as acc_val_calculation_factor,	-- 标准账期
	max(if(COALESCE(account_period_val,0)=0,1,if(a.account_period_code like 'Y%', if(a.account_period_val = 31, 45, a.account_period_val + 15), a.account_period_val))) over(partition by a.customer_no) as acc_val_calculation_factor,
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
	where 
		sdt=${hiveconf:i_sdate_11}
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
	where 
		sdt=${hiveconf:i_sdate_11}
		--and money_back_status<>'ALL'
		
	-- 202106月签呈 加上认领金额 当月处理
	--union all
	--select
	--	'' as order_no,	-- 来源单号
	--	'105042' as customer_no,	-- 客户编码
	--	--customer_name,	-- 客户名称
	--	'' appoint_place_code,  --履约地点编码
	--	'' as company_code,	-- 签约公司编码
	--	--company_name,	-- 签约公司名称		
	--	'' as happen_date,	-- 发生时间		
	--	'' as overdue_date,	-- 逾期时间	
	--	0 as source_statement_amount,	-- 源单据对账金额
	--	'' as money_back_status,	-- 回款状态
	--	20528.2 as unpaid_amount,	-- 未回款金额
	--	'Y003' as account_period_code,	--账期编码 
	--	'月结31天' as account_period_name,	--账期名称 
	--	31 as account_period_val,	--账期值
	--	'否' as beginning_mark,	--是否期初	
	--	0 as bad_debt_amount,
	--	0 as over_days	-- 逾期天数
	
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
	where sdt=${hiveconf:i_sdate_11} 
	)c on a.customer_no=c.customer_no
; --7


--应收金额、逾期日期、逾期天数
--签呈，部分客户历史订单逾期剔除
drop table csx_tmp.tmp_tc_cust_order_overdue_dtl; --8
create table csx_tmp.tmp_tc_cust_order_overdue_dtl
as
select *
from 
	csx_tmp.tmp_tc_cust_order_overdue_dtl_0
where 
	customer_no not in('105235','105557')
	and customer_no not in('104901','115852') -- '105381'
	--and customer_no not in('104666','107577')
	--and customer_no not in('111506') -- '115156'
	--4月签呈，20210331前发生的历史订单逾期剔除，4-8月处理；
union all
select *
from 
	csx_tmp.tmp_tc_cust_order_overdue_dtl_0
where 
	customer_no in('105235','105557')
	and happen_date>'20210331'
--4月签呈，20210430前发生的历史订单逾期剔除，4-8月处理；其中'115156','105381'4-6月处理，'115520','115458'本月处理
union all
select *
from 
	csx_tmp.tmp_tc_cust_order_overdue_dtl_0
where 
	customer_no in('104901','115852') -- '105381'
	and happen_date>'20210430'
--5月签呈，逾期日期20210630前的历史订单逾期剔除，5-7月处理；
--union all
--select *
--from 
--	csx_tmp.tmp_tc_cust_order_overdue_dtl_0
--where 
--	customer_no in('104666','107577')
--	and happen_date>'20210630'
--5月签呈，逾期日期20210731前的历史订单逾期剔除，5-7月处理；
--union all
--select *
--from 
--	csx_tmp.tmp_tc_cust_order_overdue_dtl_0
--where 
--	customer_no in('111506') -- '115156'
--	and happen_date>'20210731'
; --9




-- 查询结果集
--计算逾期系数

insert overwrite directory '/tmp/zhangyanpeng/yuqi_dakehu' row format delimited fields terminated by '\t'
select 
	a.channel_name,	-- 渠道
	b.sales_province_name,	-- 省区
	a.customer_no,	-- 客户编码
	a.customer_name,	-- 客户名称
	d.work_no,	-- 销售员工号
	d.sales_name,	-- 销售员
	d.service_user_work_no,d.service_user_name,d.is_part_time_service_manager,
	c.account_period_code,	-- 账期编码
	if(c.account_period_code like 'Y%',if(c.account_period_val=31,45,c.account_period_val+15),c.account_period_val) account_period_val,	-- 帐期天数
	c.account_period_name,	-- 账期名称
	a.company_code,	-- 公司代码
	a.company_name,	-- 公司名称,
	case when a.receivable_amount>=0 then a.receivable_amount else 0 end receivable_amount,	-- 应收金额
	case when a.over_amt>=0 and a.receivable_amount>0 then a.over_amt else 0 end over_amt,	-- 逾期金额
	case when a.over_amt_s>=0 and a.receivable_amount>0 then a.over_amt_s else 0 end over_amt_s,	-- 逾期金额*逾期天数
	case when a.receivable_amount_s>=0 and a.receivable_amount>0 then a.receivable_amount_s else 0 end receivable_amount_s,	-- 应收金额*帐期天数	
	coalesce(round(case when coalesce(case when a.receivable_amount>=0 then a.receivable_amount else 0 end, 0) <= 1 then 0  
						else (coalesce(case when a.over_amt_s>=0 and a.receivable_amount>0 then a.over_amt_s else 0 end,0)
						/(case when a.receivable_amount_s>=0 and a.receivable_amount>0 then a.receivable_amount_s else 0 end)) end
		  , 6),0) over_rate 	-- 逾期系数
			
from
	(
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
		sum(case when receivable_amount>=0 then receivable_amount else 0 end* acc_val_calculation_factor) as receivable_amount_s
	from csx_tmp.tmp_tc_cust_order_overdue_dtl  
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
		--and customer_no not in('PF0065','112574','106782')  --'116957','116629','116215'仅3月
		and customer_no not in('115721','116877','116883','116015','116556','116826')
		and customer_no not in('103253','103284','103296','103297','103304','103306','103311','104818','104828','104829','104835',
							'105113','104609')
		--4月签呈 当月剔除逾期系数;	剔除逾期系数，不算提成
		--and customer_no not in('111506','105685','113744','116085','103369')
		--and customer_no not in('114265','117412','116957')
		--4月签呈 每月处理：剔除逾期系数，每月剔除;剔除逾期系数，不算提成，每月处理
		and customer_no not in('112045','115393','112248','104817','105601','104381','105304','105714','116099','104445','108127')
		and customer_no not in('102844','117940')	
		--5月签呈 当月剔除逾期系数，不算提成
		--and customer_no not in('116957','106805','106228')
		--5月签呈 当月剔除逾期系数，不算提成，每月
		and customer_no not in('105280','106287','106427','110930','111100','112675','115202','115631')	
		--5月签呈 当月剔除逾期系数 '106052'5月系统错误
		--and customer_no not in('107661','108088','113816','PF0424','118274','114391','108824','103369','114718','114785','115620','117305','115017','113609','106052')
		--202106月签呈 当月剔除逾期系数
		--and customer_no not in ('114488','111296','107867','116022','114842','108818','108956','103369')
		--202106月签呈，不算提成 每月处理
		and customer_no not in('119861','105525')
		--202106月签呈，剔除逾期 当月处理
		--and customer_no not in('112635','106469','105696')
		--202106月签呈，每月处理
		and customer_no not in ('105696')
		--202106月系统底层数据错误 剔除逾期 当月处理
		--and customer_no not in ('104217')
		--202107月签呈，当月处理
		--and customer_no not in ('103369','120466','104086','107867','106469','108956','104842','110696','111120','106349','105381','105717')
		--202107月签呈，7-8月处理
		and customer_no not in ('112410','114853','117406','116315','114595','114466','111365','108589','108444','108393','105627','104102','103120','103038')
		--202107月签呈，不算提成，每月处理
		and customer_no not in('115971')
		--202108月签呈，剔除逾期，每月处理
		and customer_no not in ('106349')
		--202108月签呈，剔除逾期，当月处理
		--and customer_no not in ('107305','118794','107867','107050','106469','108956','113913','118738','120318','110696','111120','103369','119480','107761','118128','110926',
		--'119619','119561','119019','119018','119017','119012','119011','119004','118996','118993','118992','118894','118366','104086')
		--202108月签呈，剔除逾期，8-9月处理，其中，'116736','118277' 8-10月处理
		and customer_no not in ('105947','105975','106000','119198','118901','113583','113576','113569','113443','111204','110575','116736','118277')
		--202109月签呈，剔除逾期，每月处理
		and customer_no not in ('105081','106876','105249','112732','PF0345','107809','108373','106166','108146','108269','104400','105499','113001','106265','116914','113521',
		'109206','116313','105081','118667','107691','113555','103034','113929','107134','115204','102754','104086')
		--202109月签呈，剔除逾期，每月处理
		and customer_no not in ('115204','102754','119247','119250','119255','119224','119246','119209','119022','119214','119253','119257','119262','119227','119254','119242',
		'114075')
		--202109月签呈，剔除逾期，当月处理
		--and customer_no not in ('115955','111433','114979','117216','104666','107577','113184','107428','113609','116383','116980','119921','122045','119888','120120','120354',
		--'120385','119619','119561','118996','118992','118993','119017','119012','119011','118894','119018','119004','119019','118366','119422','103369','104966','107867',
		--'107050','106469','108818','109381','112754','111805','104547','119045','117342','119977','117974','119062','119116','118103','120639','120396','116661')
		--202109月签呈，剔除逾期，9-2月处理，其中'101653','102633','104358','105886' 9-10月处理
		and customer_no not in ('PF0065','101653','102633','104358','105886')
		--202110月签呈，剔除逾期，每月处理
		and customer_no not in ('110696','111120','106349','103369','118342')
		--202110月签呈，剔除逾期，当月处理
		and customer_no not in ('122230','120318','116505','116522','107761','119480','107867','118215','104666','107577','113184','120120','113609','119888','122045','121707')
		--202110月签呈，战略客户，不考核提成，每月处理
		and customer_no not in('119925','122495')
		--202110月签呈，剔除逾期，当月处理
		and customer_no not in('106526','112477')		
	group by 
		channel_name,customer_no,customer_name,company_code,company_name

	--4月签呈 '118689'系统中为业务代理人，但需要人为计算销售员大客户提成,每月处理
	--202108月签呈，'116957','116629'系统中为业务代理人，但需要人为计算销售员大客户提成，每月处理
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
		sum(case when receivable_amount>=0 then receivable_amount else 0 end* acc_val_calculation_factor) as receivable_amount_s
	from 
		csx_tmp.tmp_tc_cust_order_overdue_dtl  
	where 
		customer_no in ('118689','116957','116629') and sdt = ${hiveconf:i_sdate_11} 
	group by 
		channel_name,customer_no,customer_name,company_code,company_name
	)a
left join
	(
	select
		customer_no,
		company_code,
		payment_terms account_period_code,
		case when payment_terms like 'Y%' then concat('月结',COALESCE(cast(payment_days as int),0)) 
			 else concat('票到',COALESCE(cast(payment_days as int),0)) end account_period_name,
		COALESCE(cast(payment_days as int),0) account_period_val
	from 
		csx_dw.dws_crm_w_a_customer_company a
	where 
		sdt='current'
		and customer_no<>''
	) c on (a.customer_no=c.customer_no and a.company_code=c.company_code)
--剔除业务代理与内购客户
join		 ----渠道编号-1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 7.企业购 8.其他 9.业务代理
	(
	select 
		* 
	from 
		csx_dw.dws_crm_w_a_customer 
		--4月签呈 '118689'系统中为业务代理人，但需要人为计算销售员大客户提成,每月处理
		--202108月签呈，'116957','116629'系统中为业务代理人，但需要人为计算销售员大客户提成，每月处理
	where 
		sdt=${hiveconf:i_sdate_11} 
		--where sdt='20210617'
		and (channel_code in('1','7','8') or customer_no in ('118689','116957','116629')) and (customer_name not like '%内%购%' and customer_name not like '%临保%')	
	)b on b.customer_no=a.customer_no  
--join (select * from csx_dw.dws_crm_w_a_customer where sdt=${hiveconf:i_sdate_11} and dev_source_code not in('2','4')) b on b.customer_no=a.customer_no  --剔除业务代理与内购客户
--剔除当月有城市服务商与批发内购业绩的客户逾期系数
left join 		--业务类型编码(1.日配单 2.福利单 3.批发内购 4.城市服务商 5.省区大宗 6.BBC)  --剔除内购客户、城市服务商
	(
	select 
		distinct customer_no 
	from 
		csx_dw.dws_sale_r_d_detail 
	where 
		sdt>=${hiveconf:i_sdate_12} 
		and sdt<=${hiveconf:i_sdate_11} 
		and business_type_code in('3','4')
		--5月签呈，不剔除城市服务商2.0，按大客户提成方案计算
		and customer_no not in(
		'107890','109363','111364','112410','113735','114859','115023','115738','115904','115941','118264','119022','119242','119247','119257','120376','120846','120879',
		'120999','121287','121337','122394','108713','109460','111734','112906','113829','115681','116056','116061','118259','118262','119227','119246','119250','119253',
		'119262','120147','120768','121384','121398','121467','121994','122406','122497','112207','113617','113634','114485','114853','114940','115151','115392','116027',
		'116032','116038','116959','117496','117548','118078','118221','119254','120404','120939','121483','121495','121855','122603','122623','108726','109357','114246',
		'116055','117558','117817','117889','118219','118509','119209','119214','119224','119255','119397','119892','120294','120826','121020','121032','121039','121276',
		'121298','122534','122559','122567','122577','122988')
	)e on e.customer_no=a.customer_no
--关联服务管家 202106月计算用，客户对应销售员与服务管家
left join		
	(  
	select 
		distinct customer_no,service_user_work_no,service_user_name,	  
		work_no,sales_name,is_part_time_service_manager
	from 
		csx_tmp.tmp_tc_customer_service_manager_info	
		--from csx_dw.report_crm_w_a_customer_service_manager_info 
		--where sdt=${hiveconf:i_sdate_11}
	)d on d.customer_no=a.customer_no	  
where 
	e.customer_no is null
; --11
	
	

--客户逾期系数
drop table csx_tmp.temp_tc_cust_over_rate; --13
create table csx_tmp.temp_tc_cust_over_rate
as 
select 
	channel_name,	-- 渠道
	customer_no,	-- 客户编码
	customer_name,	-- 客户名称,
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
	(
	select
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
		sum(case when receivable_amount>=0 then receivable_amount else 0 end* acc_val_calculation_factor) as receivable_amount_s				
	from csx_tmp.tmp_tc_cust_order_overdue_dtl a 
	where 
		channel_name = '大客户' and sdt = ${hiveconf:i_sdate_11}
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
		--and customer_no not in('PF0065','112574','106782')  --'116957','116629','116215'仅3月
		and customer_no not in('115721','116877','116883','116015','116556','116826')
		and customer_no not in('103253','103284','103296','103297','103304','103306','103311','104818','104828','104829','104835',
							'105113','104609')
		--4月签呈 当月剔除逾期系数;	剔除逾期系数，不算提成
		--and customer_no not in('111506','105685','113744','116085','103369')
		--and customer_no not in('114265','117412','116957')
		--4月签呈 每月处理：剔除逾期系数，每月剔除;剔除逾期系数，不算提成，每月处理
		and customer_no not in('112045','115393','112248','104817','105601','104381','105304','105714','116099','104445','108127')
		and customer_no not in('102844','117940')	
		--5月签呈 当月剔除逾期系数，不算提成
		--and customer_no not in('116957','106805','106228')	
		--5月签呈 当月剔除逾期系数，不算提成，每月
		and customer_no not in('105280','106287','106427','110930','111100','112675','115202','115631')		
		--5月签呈 当月剔除逾期系数 '106052'5月系统错误
		--and customer_no not in('107661','108088','113816','PF0424','118274','114391','108824','103369','114718','114785','115620','117305','115017','113609','106052')
		--202106月签呈 当月剔除逾期系数
		--and customer_no not in ('114488','111296','107867','116022','114842','108818','108956','103369')
		--202106月签呈，不算提成 每月处理
		and customer_no not in('119861','105525')
		--202106月签呈，剔除逾期 当月处理
		--and customer_no not in('112635','106469','105696')
		--202106月签呈，每月处理
		and customer_no not in ('105696')
		--202106月系统底层数据错误 剔除逾期 当月处理
		--and customer_no not in ('104217')
		--202107月签呈，当月处理
		--and customer_no not in ('103369','120466','104086','107867','106469','108956','104842','110696','111120','106349','105381','105717')
		--202107月签呈，7-8月处理
		and customer_no not in ('112410','114853','117406','116315','114595','114466','111365','108589','108444','108393','105627','104102','103120','103038')
		--202107月签呈，不算提成，每月处理
		and customer_no not in('115971')
		--202108月签呈，剔除逾期，每月处理
		and customer_no not in ('106349')
		--202108月签呈，剔除逾期，当月处理
		--and customer_no not in ('107305','118794','107867','107050','106469','108956','113913','118738','120318','110696','111120','103369','119480','107761','118128','110926',
		--'119619','119561','119019','119018','119017','119012','119011','119004','118996','118993','118992','118894','118366','104086')
		--202108月签呈，剔除逾期，8-9月处理，其中，'116736','118277' 8-10月处理
		and customer_no not in ('105947','105975','106000','119198','118901','113583','113576','113569','113443','111204','110575','116736','118277')
		--202109月签呈，剔除逾期，每月处理
		and customer_no not in ('105081','106876','105249','112732','PF0345','107809','108373','106166','108146','108269','104400','105499','113001','106265','116914','113521',
		'109206','116313','105081','118667','107691','113555','103034','113929','107134','115204','102754','104086')
		--202109月签呈，剔除逾期，每月处理
		and customer_no not in ('115204','102754','119247','119250','119255','119224','119246','119209','119022','119214','119253','119257','119262','119227','119254','119242',
		'114075')
		--202109月签呈，剔除逾期，当月处理
		--and customer_no not in ('115955','111433','114979','117216','104666','107577','113184','107428','113609','116383','116980','119921','122045','119888','120120','120354',
		--'120385','119619','119561','118996','118992','118993','119017','119012','119011','118894','119018','119004','119019','118366','119422','103369','104966','107867',
		--'107050','106469','108818','109381','112754','111805','104547','119045','117342','119977','117974','119062','119116','118103','120639','120396','116661')
		--202109月签呈，剔除逾期，9-2月处理，其中'101653','102633','104358','105886' 9-10月处理
		and customer_no not in ('PF0065','101653','102633','104358','105886')
		--202110月签呈，剔除逾期，每月处理
		and customer_no not in ('110696','111120','106349','103369','118342')
		--202110月签呈，剔除逾期，当月处理
		and customer_no not in ('122230','120318','116505','116522','107761','119480','107867','118215','104666','107577','113184','120120','113609','119888','122045','121707')
		--202110月签呈，战略客户，不考核提成，每月处理
		and customer_no not in('119925','122495')
		--202110月签呈，剔除逾期，当月处理
		and customer_no not in('106526','112477')	
	group by 
		channel_name,customer_no,customer_name,company_code,company_name
	)a	
group by 
	channel_name,customer_no,customer_name
; --14



--销售员逾期系数
drop table csx_tmp.temp_tc_salesname_over_rate_0;
create table csx_tmp.temp_tc_salesname_over_rate_0
as
select 
	a.channel_name,	-- 渠道
	b.work_no,	-- 销售员工号
	b.sales_name,	-- 销售员
	sum(case when receivable_amount>=0 then receivable_amount else 0 end) receivable_amount,	-- 应收金额
	sum(case when over_amt>=0 and receivable_amount>0 then over_amt else 0 end) over_amt,	-- 逾期金额
	sum(case when over_amt_s>=0 and receivable_amount>0 then over_amt_s else 0 end) over_amt_s,	-- 逾期金额*逾期天数
	sum(case when receivable_amount_s>=0 and receivable_amount>0 then receivable_amount_s else 0 end) receivable_amount_s,	-- 应收金额*帐期天数	
	--coalesce(round(case  when coalesce(sum(case when receivable_amount>=0 then receivable_amount else 0 end), 0) <= 1 then 0  
	--			else coalesce(sum(case when over_amt_s>=0 and receivable_amount>0 then over_amt_s else 0 end), 0)
	--			/(sum(case when receivable_amount_s>=0 and receivable_amount>0 then receivable_amount_s else 0 end)) end
	--	  , 6),0) over_rate 	-- 逾期系数
	--202110月签呈，销售员整体按0.5计算，当月处理
	if(b.work_no='80945044',0.5,coalesce(round(case  when coalesce(sum(case when receivable_amount>=0 then receivable_amount else 0 end), 0) <= 1 then 0  
				else coalesce(sum(case when over_amt_s>=0 and receivable_amount>0 then over_amt_s else 0 end), 0)
				/(sum(case when receivable_amount_s>=0 and receivable_amount>0 then receivable_amount_s else 0 end)) end
		  , 6),0)) over_rate 	-- 逾期系数			  
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
	from 
		csx_tmp.tmp_tc_cust_order_overdue_dtl a 
	where 
		channel_name = '大客户' and sdt = ${hiveconf:i_sdate_11}
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
		--and customer_no not in('PF0065','112574','106782')  --'116957','116629','116215'仅3月
		and customer_no not in('115721','116877','116883','116015','116556','116826')
		and customer_no not in('103253','103284','103296','103297','103304','103306','103311','104818','104828','104829','104835',
							   '105113','104609')
		--4月签呈 当月剔除逾期系数;	剔除逾期系数，不算提成
		--and customer_no not in('111506','105685','113744','116085','103369')
		--and customer_no not in('114265','117412','116957')
		--4月签呈 每月处理：剔除逾期系数，每月剔除;剔除逾期系数，不算提成，每月处理
		and customer_no not in('112045','115393','112248','104817','105601','104381','105304','105714','116099','104445','108127')
		and customer_no not in('102844','117940')	
		--5月签呈 当月剔除逾期系数，不算提成
		--and customer_no not in('116957','106805','106228')
		--5月签呈 当月剔除逾期系数，不算提成，每月
		and customer_no not in('105280','106287','106427','110930','111100','112675','115202','115631')		
		--5月签呈 当月剔除逾期系数 '106052'5月系统错误
		--and customer_no not in('107661','108088','113816','PF0424','118274','114391','108824','103369','114718','114785','115620','117305','115017','113609','106052')
		--202106月签呈 当月剔除逾期系数
		--and customer_no not in ('114488','111296','107867','116022','114842','108818','108956','103369')
		--202106月签呈，不算提成 每月处理
		and customer_no not in('119861','105525')
		--202106月签呈，剔除逾期 当月处理
		--and customer_no not in('112635','106469','105696')
		--202106月签呈，每月处理
		and customer_no not in ('105696')
		--202106月系统底层数据错误 剔除逾期 当月处理
		--and customer_no not in ('104217')
		--202107月签呈，当月处理
		--and customer_no not in ('103369','120466','104086','107867','106469','108956','104842','110696','111120','106349','105381','105717')
		--202107月签呈，7-8月处理
		and customer_no not in ('112410','114853','117406','116315','114595','114466','111365','108589','108444','108393','105627','104102','103120','103038')
		--202107月签呈，不算提成，每月处理
		and customer_no not in('115971')
		--202108月签呈，剔除逾期，每月处理
		and customer_no not in ('106349')
		--202108月签呈，剔除逾期，当月处理
		--and customer_no not in ('107305','118794','107867','107050','106469','108956','113913','118738','120318','110696','111120','103369','119480','107761','118128','110926',
		--'119619','119561','119019','119018','119017','119012','119011','119004','118996','118993','118992','118894','118366','104086')
		--202108月签呈，剔除逾期，8-9月处理，其中，'116736','118277' 8-10月处理
		and customer_no not in ('105947','105975','106000','119198','118901','113583','113576','113569','113443','111204','110575','116736','118277')
		--202109月签呈，剔除逾期，每月处理
		and customer_no not in ('105081','106876','105249','112732','PF0345','107809','108373','106166','108146','108269','104400','105499','113001','106265','116914','113521',
		'109206','116313','105081','118667','107691','113555','103034','113929','107134','115204','102754','104086')
		--202109月签呈，剔除逾期，每月处理
		and customer_no not in ('115204','102754','119247','119250','119255','119224','119246','119209','119022','119214','119253','119257','119262','119227','119254','119242',
		'114075')
		--202109月签呈，剔除逾期，当月处理
		--and customer_no not in ('115955','111433','114979','117216','104666','107577','113184','107428','113609','116383','116980','119921','122045','119888','120120','120354',
		--'120385','119619','119561','118996','118992','118993','119017','119012','119011','118894','119018','119004','119019','118366','119422','103369','104966','107867',
		--'107050','106469','108818','109381','112754','111805','104547','119045','117342','119977','117974','119062','119116','118103','120639','120396','116661')
		--202109月签呈，剔除逾期，9-2月处理，其中'101653','102633','104358','105886' 9-10月处理
		and customer_no not in ('PF0065','101653','102633','104358','105886')
		--202110月签呈，剔除逾期，每月处理
		and customer_no not in ('110696','111120','106349','103369','118342')
		--202110月签呈，剔除逾期，当月处理
		and customer_no not in ('122230','120318','116505','116522','107761','119480','107867','118215','104666','107577','113184','120120','113609','119888','122045','121707')
		--202110月签呈，战略客户，不考核提成，每月处理
		and customer_no not in('119925','122495')
		--202110月签呈，剔除逾期，当月处理
		and customer_no not in('106526','112477')	
	group by 
		channel_name,customer_no,customer_name,company_code,company_name
	)a	
	----渠道编号-1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 7.企业购 8.其他 9.业务代理
	--剔除业务代理与内购客户
	--202107月签呈，将以下客户的销售员调整为张辉 每月处理
	--202108月签呈，'116957','116629'系统中为业务代理人，但需要人为计算销售员大客户提成，每月处理
	--202108月签呈，更改销售员，每月处理 '114524','115195'
	left join
		(
		select 
			customer_no,
			case when customer_no in ('114524','115195') then '81086756'
				else work_no end as work_no,
			case when customer_no in ('114524','115195') then '姚艳婷'
				else sales_name end as sales_name
			--work_no,
			--sales_name
		from 
			csx_dw.dws_crm_w_a_customer 
		where 
			sdt=${hiveconf:i_sdate_11} 
			--where sdt='20210617'	
			--4月签呈 '118689'系统中为业务代理人，但需要人为计算销售员大客户提成,每月处理
			--202108月签呈，'116957','116629'系统中为业务代理人，但需要人为计算销售员大客户提成，每月处理
			and (channel_code in('1','7','8') or customer_no in ('118689','116957','116629')) and (customer_name not like '%内%购%' and customer_name not like '%临保%')
		)b on b.customer_no=a.customer_no  
	--left join (select * from csx_dw.dws_crm_w_a_customer where sdt=${hiveconf:i_sdate_11} and dev_source_code not in('2','4')) b on b.customer_no=a.customer_no  --剔除业务代理与内购客户
	--剔除当月有城市服务商与批发内购业绩的客户逾期系数
	left join 		--业务类型编码(1.日配单 2.福利单 3.批发内购 4.城市服务商 5.省区大宗 6.BBC)  --剔除内购客户、城市服务商
		(
		select 
			distinct customer_no 
		from 
			csx_dw.dws_sale_r_d_detail 
		where 
			sdt>=${hiveconf:i_sdate_12} 
			and sdt<=${hiveconf:i_sdate_11} 
			and business_type_code in('3','4')
			--5月签呈，不剔除城市服务商2.0，按大客户提成方案计算
		and customer_no not in(
		'107890','109363','111364','112410','113735','114859','115023','115738','115904','115941','118264','119022','119242','119247','119257','120376','120846','120879',
		'120999','121287','121337','122394','108713','109460','111734','112906','113829','115681','116056','116061','118259','118262','119227','119246','119250','119253',
		'119262','120147','120768','121384','121398','121467','121994','122406','122497','112207','113617','113634','114485','114853','114940','115151','115392','116027',
		'116032','116038','116959','117496','117548','118078','118221','119254','120404','120939','121483','121495','121855','122603','122623','108726','109357','114246',
		'116055','117558','117817','117889','118219','118509','119209','119214','119224','119255','119397','119892','120294','120826','121020','121032','121039','121276',
		'121298','122534','122559','122567','122577','122988')
		)c on c.customer_no=a.customer_no
where 
	c.customer_no is null	
group by 
	a.channel_name,b.work_no,b.sales_name;


--签呈处理：销售员逾期系数  
--108818 80945044 剔除逾期系数，只剔除逾期金额保留应收
--112635 80941188 逾期金额减去211000，在结果处理
--105696 80949208 逾期金额减去530188.59，在结果处理

--202106月签呈
--112635 80941188 逾期金额减去承兑汇票的21.1万
--106469 80909562 逾期金额减去承兑汇票的10万
--105696 80949208 逾期金额减去530188.59

drop table csx_tmp.temp_tc_salesname_over_rate;
create table csx_tmp.temp_tc_salesname_over_rate
as
select *
from csx_tmp.temp_tc_salesname_over_rate_0
;
				

--服务管家逾期率
drop table csx_tmp.temp_tc_service_user_over_rate;
create table csx_tmp.temp_tc_service_user_over_rate
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
	from 
		csx_tmp.tmp_tc_cust_order_overdue_dtl a 
	where 
		channel_name = '大客户' and sdt = ${hiveconf:i_sdate_11}
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
		--and customer_no not in('PF0065','112574','106782')  --'116957','116629','116215'仅3月
		and customer_no not in('115721','116877','116883','116015','116556','116826')
		and customer_no not in('103253','103284','103296','103297','103304','103306','103311','104818','104828','104829','104835',
							   '105113','104609')
		--4月签呈 当月剔除逾期系数;	剔除逾期系数，不算提成
		--and customer_no not in('111506','105685','113744','116085','103369')
		--and customer_no not in('114265','117412','116957')
		--4月签呈 每月处理：剔除逾期系数，每月剔除;剔除逾期系数，不算提成，每月处理
		and customer_no not in('112045','115393','112248','104817','105601','104381','105304','105714','116099','104445','108127')
		and customer_no not in('102844','117940')	
		--5月签呈 当月剔除逾期系数，不算提成
		--and customer_no not in('116957','106805','106228')
		--5月签呈 当月剔除逾期系数，不算提成，每月
		and customer_no not in('105280','106287','106427','110930','111100','112675','115202','115631')		
		--5月签呈 当月剔除逾期系数 '106052'5月系统错误
		--and customer_no not in('107661','108088','113816','PF0424','118274','114391','108824','103369','114718','114785','115620','117305','115017','113609','106052')
		--202106月签呈 当月剔除逾期系数
		--and customer_no not in ('114488','111296','107867','116022','114842','108818','108956','103369')
		--202106月签呈，不算提成 每月处理
		and customer_no not in('119861','105525')
		--202106月签呈，剔除逾期 当月处理
		--and customer_no not in('112635','106469','105696')
		--202106月签呈，每月处理
		and customer_no not in ('105696')
		--202106月系统底层数据错误 剔除逾期 当月处理
		--and customer_no not in ('104217')
		--202107月签呈，当月处理
		--and customer_no not in ('103369','120466','104086','107867','106469','108956','104842','110696','111120','106349','105381','105717')
		--202107月签呈，7-8月处理
		and customer_no not in ('112410','114853','117406','116315','114595','114466','111365','108589','108444','108393','105627','104102','103120','103038')
		--202107月签呈，不算提成，每月处理
		and customer_no not in('115971')
		--202108月签呈，剔除逾期，每月处理
		and customer_no not in ('106349')
		--202108月签呈，剔除逾期，当月处理
		--and customer_no not in ('107305','118794','107867','107050','106469','108956','113913','118738','120318','110696','111120','103369','119480','107761','118128','110926',
		--'119619','119561','119019','119018','119017','119012','119011','119004','118996','118993','118992','118894','118366','104086')
		--202108月签呈，剔除逾期，8-9月处理，其中，'116736','118277' 8-10月处理
		and customer_no not in ('105947','105975','106000','119198','118901','113583','113576','113569','113443','111204','110575','116736','118277')
		--202109月签呈，剔除逾期，每月处理
		and customer_no not in ('105081','106876','105249','112732','PF0345','107809','108373','106166','108146','108269','104400','105499','113001','106265','116914','113521',
		'109206','116313','105081','118667','107691','113555','103034','113929','107134','115204','102754','104086')
		--202109月签呈，剔除逾期，每月处理
		and customer_no not in ('115204','102754','119247','119250','119255','119224','119246','119209','119022','119214','119253','119257','119262','119227','119254','119242',
		'114075')
		--202109月签呈，剔除逾期，当月处理
		--and customer_no not in ('115955','111433','114979','117216','104666','107577','113184','107428','113609','116383','116980','119921','122045','119888','120120','120354',
		--'120385','119619','119561','118996','118992','118993','119017','119012','119011','118894','119018','119004','119019','118366','119422','103369','104966','107867',
		--'107050','106469','108818','109381','112754','111805','104547','119045','117342','119977','117974','119062','119116','118103','120639','120396','116661')
		--202109月签呈，剔除逾期，9-2月处理，其中'101653','102633','104358','105886' 9-10月处理
		and customer_no not in ('PF0065','101653','102633','104358','105886')
		--202110月签呈，剔除逾期，每月处理
		and customer_no not in ('110696','111120','106349','103369','118342')
		--202110月签呈，剔除逾期，当月处理
		and customer_no not in ('122230','120318','116505','116522','107761','119480','107867','118215','104666','107577','113184','120120','113609','119888','122045','121707')
		--202110月签呈，战略客户，不考核提成，每月处理
		and customer_no not in('119925','122495')
		--202110月签呈，剔除逾期，当月处理
		and customer_no not in('106526','112477')	
	group by 
		channel_name,customer_no,customer_name,company_code,company_name
	)a	
	----渠道编号-1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 7.企业购 8.其他 9.业务代理
	--剔除业务代理与内购客户
	--4月签呈，将以下客户的销售员调整为xx 每月处理
	left join
		(
		select 
			customer_no,
	--  case when customer_no in('113873','113918','113935','113940','115656','117244','115826','115602','104281','107398') then '签呈,未知'
	--       when customer_no in('114054','109000','114083','114085','115909','115971') then '签呈,未知'
	--	   else work_no end as work_no,
	--  case when customer_no in('113873','113918','113935','113940','115656','117244','115826','115602','104281','107398') then '虚拟AA'
	--       when customer_no in('114054','109000','114083','114085','115909','115971') then '虚拟AA'
	--	   else sales_name end as sales_name		 
			work_no,sales_name
		from 
			csx_dw.dws_crm_w_a_customer 
		where 
			sdt=${hiveconf:i_sdate_11} 
			--where sdt='20210617' 
			--4月签呈 '118689'系统中为业务代理人，但需要人为计算销售员大客户提成,每月处理
			--202108月签呈，'116957','116629'系统中为业务代理人，但需要人为计算销售员大客户提成，每月处理
			and (channel_code in('1','7','8') or customer_no in ('118689','116957','116629')) and (customer_name not like '%内%购%' and customer_name not like '%临保%')
		)b on b.customer_no=a.customer_no  
		--left join (select * from csx_dw.dws_crm_w_a_customer where sdt=${hiveconf:i_sdate_11} and dev_source_code not in('2','4')) b on b.customer_no=a.customer_no  --剔除业务代理与内购客户
		--剔除当月有城市服务商与批发内购业绩的客户逾期系数
	left join 		--业务类型编码(1.日配单 2.福利单 3.批发内购 4.城市服务商 5.省区大宗 6.BBC)  --剔除内购客户、城市服务商
		(
		select 
			distinct customer_no 
		from 
			csx_dw.dws_sale_r_d_detail 
		where 
			sdt>=${hiveconf:i_sdate_12} 
			and sdt<=${hiveconf:i_sdate_11} 
			and business_type_code in('3','4')
			--5月签呈，不剔除城市服务商2.0，按大客户提成方案计算
		and customer_no not in(
		'107890','109363','111364','112410','113735','114859','115023','115738','115904','115941','118264','119022','119242','119247','119257','120376','120846','120879',
		'120999','121287','121337','122394','108713','109460','111734','112906','113829','115681','116056','116061','118259','118262','119227','119246','119250','119253',
		'119262','120147','120768','121384','121398','121467','121994','122406','122497','112207','113617','113634','114485','114853','114940','115151','115392','116027',
		'116032','116038','116959','117496','117548','118078','118221','119254','120404','120939','121483','121495','121855','122603','122623','108726','109357','114246',
		'116055','117558','117817','117889','118219','118509','119209','119214','119224','119255','119397','119892','120294','120826','121020','121032','121039','121276',
		'121298','122534','122559','122567','122577','122988')
		)c on c.customer_no=a.customer_no
    
	--关联服务管家 5月计算用，客户对应销售员与服务管家
	left join		
		(  
		select 
			distinct customer_no,service_user_work_no,service_user_name,
			work_no,sales_name,is_part_time_service_manager
		from 
			csx_tmp.tmp_tc_customer_service_manager_info	
		)d on d.customer_no=a.customer_no	  
where 
	c.customer_no is null	
group by 
	a.channel_name,d.service_user_work_no,d.service_user_name;



--大宗供应链的逾期系数
insert overwrite directory '/tmp/zhangyanpeng/yuqi_dazong' row format delimited fields terminated by '\t'
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
	(
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
	from 
		csx_tmp.tmp_tc_cust_order_overdue_dtl  
	where 
		(channel_name like '大宗%' or channel_name like '%供应链%')
		and sdt = ${hiveconf:i_sdate_11} 
	group by 
		channel_name,customer_no,customer_name,company_code,company_name
	)a
	join		 ----渠道编号-1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 7.企业购 8.其他 9.业务代理
		(
		select 
			* 
		from 
			csx_dw.dws_crm_w_a_customer 
		where 
			sdt=${hiveconf:i_sdate_11} 
			and channel_code in('4','5','6') 
		)b on b.customer_no=a.customer_no  
	left join
		(
		select
			customer_no,
			company_code,
			payment_terms account_period_code,
			case when payment_terms like 'Y%' then concat('月结',COALESCE(cast(payment_days as int),0)) 
				else concat('票到',COALESCE(cast(payment_days as int),0)) end account_period_name,
			COALESCE(cast(payment_days as int),0) account_period_val
		from 
			csx_dw.dws_crm_w_a_customer_company a
		where 
			sdt='current'
			and customer_no<>''
		)c on (a.customer_no=c.customer_no and a.company_code=c.company_code);


--5月查询结果是perform_dc_code in('W0L4','W0K7','W0AW','W0BY')三个城市服务商2.0仓过机的客户都只在这些仓有过机，以后最好再看下
--城市服务商2.0的逾期系数
insert overwrite directory '/tmp/zhangyanpeng/yuqi_csfws2' row format delimited fields terminated by '\t'
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
	(
	select
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
		select 
			* 
		from 
			csx_tmp.tmp_tc_cust_order_overdue_dtl  
		where 
			channel_name = '大客户'
			and sdt = ${hiveconf:i_sdate_11} 
		)a 
		--城市服务商2.0 按履约DC
		join
			(
			select 
				distinct inventory_dc_code
			from 
				csx_ods.source_csms_w_a_yszx_town_service_provider_config
			)d on a.appoint_place_code=d.inventory_dc_code	
	group by 
		channel_name,customer_no,customer_name,company_code,company_name
	)a
	join		 ----渠道编号-1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 7.企业购 8.其他 9.业务代理
		(
		select 
			* 
		from 
			csx_dw.dws_crm_w_a_customer 
		where 
			sdt=${hiveconf:i_sdate_11} 
			--where sdt='20210617'
			and channel_code in('1','7') 
		)b on b.customer_no=a.customer_no  
	left join
		(
		select
			customer_no,
			company_code,
			payment_terms account_period_code,
			case when payment_terms like 'Y%' then concat('月结',COALESCE(cast(payment_days as int),0)) 
				else concat('票到',COALESCE(cast(payment_days as int),0)) end account_period_name,
			COALESCE(cast(payment_days as int),0) account_period_val
		from 
			csx_dw.dws_crm_w_a_customer_company a
		where 
			sdt='current'
			and customer_no<>''
		)c on (a.customer_no=c.customer_no and a.company_code=c.company_code)
;


--=============================================================================================================================================================================
--查询城市服务商2.0客户,按库存DC

--select distinct inventory_dc_code from csx_ods.source_csms_w_a_yszx_town_service_provider_config; -- W0AW、W0BY、W0K7、W0L4
/*
select 
	a.*,c.work_no,c.sales_name
from 
	(
	select 
		province_name,customer_no,customer_name,business_type_name,dc_code,
		sum(sales_value)sales_value
	from 
		csx_dw.dws_sale_r_d_detail
	where 
		sdt>='20211001'
		and sdt<='20211031'
		and channel_code in('1','7','9')
		--and business_type_code not in('3','4')
	group by 
		province_name,customer_no,customer_name,business_type_name,dc_code
	)a 
	join 
		(
		select 
			distinct customer_no
		from 
			csx_dw.dws_sale_r_d_detail
		where 
			sdt>='20211001'
			and sdt<='20211031'
			and channel_code in('1','7','9')
			and dc_code in('W0AW','W0K7','W0L4','W0BY')
		) b on b.customer_no=a.customer_no
	left join 
		(
		select 
			distinct customer_no,customer_name,work_no,sales_name,sales_province_name
		from 
			csx_dw.dws_crm_w_a_customer 
			--where sdt=${hiveconf:i_sdate_11} 
		where 
			sdt='20211031'
		)c on c.customer_no=a.customer_no;

*/




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
*/


