
-- 昨日、昨日、昨日月1日
--select ${hiveconf:i_sdate_11},${hiveconf:i_sdate_12},${hiveconf:i_sdate_12},${hiveconf:i_sdate_11};
--set i_sdate_1 =date_sub(current_date,1);
--set i_sdate_11 =regexp_replace(date_sub(current_date,1),'-','');
--set i_sdate_12 =regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),0),'-','');

--set i_sdate_1 =last_day(add_months(date_sub(current_date,1),-1));
--set i_sdate_11 =regexp_replace(last_day(add_months(date_sub(current_date,1),-1)),'-','');	
--set i_sdate_12 =regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-1),'-','');					

	
set i_sdate_1 ='2022-01-31';
set i_sdate_11 ='20220131';
set i_sdate_12 ='20220101';


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
	'100326','PF1265','105182','106721','102565','103855','105150','111844','102734','107404','102784','102901','112288','113467','103199','104469','104501','115982','115987',
	'PF0365','105355','105886','100563','101482','102229','102508','102633','102686','103062','103141','103372','103714','104007','104054','104165','104612','104954','104970',
	'105156','105177','105181','105225','105441','105721','105806','105838','105882','106423','106481','106881','107371','108127','108739','109377','111241','112062','112327',
	'113101','113635','113646','114344','115324','115646','115679','115857','116211','118687','PF0094','PF1205','103995','107683','111122','117095','118540','116957','114524',
	'115195','120024','120317','107901','115753','118569','119688','117145','119021','119454','121444','121229','121443','123035','119925','122495','105502','105761','106346',
	'107093','107408','109435','113439','114269','114576','114682','114945','115883','116237','116527','116721','116727','116915','118458','118582','119441','119965','119992',
	'120410','120618','121128','121288','121300','121703','122152','123240','123279','123365','124046','125290','117755','124484','105896','122091','122171','105887','108637',
	'115311','120847','121480','121488','122543','123342','123896','104623','105748','105862','105891','109369','111842','113695','116566','117017','117090','121055','121856',
	'121861','121866','121871','123225','123901','124668','105552','105839','106709','108461','111560','112915','114173','114968','116925','117332','118449','120333','121233',
	'124546','122089','122148','122164','122190','122758','122780','124533','107897','109456','117862','119760','120456','121100','121193','120595','105774','107411','107530',
	'108282','113487','113550','113731','113857','122147','122159','122167','122185','122186','122188','111764','112285','112681','113015','113394','114650','114804','116987',
	'117311','118762','120495','121890','123341','125370','104281','115656','105593','107398','109447','111207','115537','115602','115826','117244','123065','125534','116015',
	'115721','119703','118887','116556','116733','116702','116923','116863','116883','116877','116826','123534','116967','117026','117025','117009','117045','117040','117035',
	'116944','117030','117027','116903','116994','117019','116993','116989','123716','120100','119806','119803','119815','120463','122125','122106','121763','122375','122286',
	'122371','123060','123383','123644','123813','124179','125244','125508','103183','104086','104397','100984','105499','114843','108589','102580','102890','PF0099','116099',
	'106301','106306','120246','120689','117047','118836','115899','116398','122143','117108','123827','117121','117222','123442','120735','118183','125137','123923','120836',
	'123034','123859','123650','124555','124641','104318','104085','120781','118744','106433','115051','108283','119897','109722','114516','120365','119210','119168','119519',
	'122555','121507','122269','123222','123262','123257','123242','123247','123253','124602','115205','110807','115252','115643','117929','120459','121206','124098','102755',
	'103175','103868','103874','103887','103898','103908','103926','103927','104460','109000','105673','106563','114522','115431','118504','120554','113281','115935','118072',
	'118602','118748','119990','125677','110872','118825','105164','105165','119757','111628','111612','114289','117396','108162','115829','108180','108152','116445','120458',
	'123032','124481','125355','111331','110863','114667','108267','111298','115476','118914','111318','111336','112629','116242','116639','117304','117605','112181','113641',
	'117119','118824','118848','119196','120098','120130','120985','121368','123623','103096','120708','120757','120803','120828','120861','120881','120882','120883','120888',
	'120902','120904','120906','120918','120920','120923','120932','120933','120934','120949','120954','120955','120956','120970','120984','120986','120987','120990','120995',
	'121022','121056','121176','124524','124033','118816','119825','118815','117249','118126','113926','111000','119535','107514','107956','108618','108910','109092','110026',
	'113477','115458','115833','115889','117843','118020','118553','121389','123917'
	)
union all   select '100326' as customer_no,'80009493' as service_user_work_no,'郑银燕' as service_user_name,'80007454' as work_no,'李翔' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select 'PF1265' as customer_no,'80972915' as service_user_work_no,'陈伟豪' as service_user_name,'80012225' as work_no,'林挺波' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '105182' as customer_no,'81133021' as service_user_work_no,'黄升' as service_user_name,'81133021' as work_no,'黄升' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '106721' as customer_no,'81133021' as service_user_work_no,'黄升' as service_user_name,'81133021' as work_no,'黄升' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '102565' as customer_no,'81129243' as service_user_work_no,'汪敏禄' as service_user_name,'80960666' as work_no,'冯桂华' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '103855' as customer_no,'81099343' as service_user_work_no,'林瑾鑫' as service_user_name,'81099343' as work_no,'林瑾鑫' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '105150' as customer_no,'81133021' as service_user_work_no,'黄升' as service_user_name,'81133021' as work_no,'黄升' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '111844' as customer_no,'81129243' as service_user_work_no,'汪敏禄' as service_user_name,'80960666' as work_no,'冯桂华' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '102734' as customer_no,'81099343' as service_user_work_no,'林瑾鑫' as service_user_name,'80980614' as work_no,'池春梅' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '107404' as customer_no,'80952742' as service_user_work_no,'王秀云' as service_user_name,'80952742' as work_no,'王秀云' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '102784' as customer_no,'81099343' as service_user_work_no,'林瑾鑫' as service_user_name,'80980614' as work_no,'池春梅' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '102901' as customer_no,'81131450' as service_user_work_no,'张珠妹' as service_user_name,'80980614' as work_no,'池春梅' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '112288' as customer_no,'81105401' as service_user_work_no,'卢烊' as service_user_name,'80980614' as work_no,'池春梅' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '113467' as customer_no,'81105401' as service_user_work_no,'卢烊' as service_user_name,'80980614' as work_no,'池春梅' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '103199' as customer_no,'81105401' as service_user_work_no,'卢烊' as service_user_name,'80980614' as work_no,'池春梅' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '104469' as customer_no,'81105401' as service_user_work_no,'卢烊' as service_user_name,'80980614' as work_no,'池春梅' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '104501' as customer_no,'' as service_user_work_no,'' as service_user_name,'80980614' as work_no,'池春梅' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '115982' as customer_no,'81105401' as service_user_work_no,'卢烊' as service_user_name,'80980614' as work_no,'池春梅' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '115987' as customer_no,'81105401' as service_user_work_no,'卢烊' as service_user_name,'80980614' as work_no,'池春梅' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select 'PF0365' as customer_no,'81105401' as service_user_work_no,'卢烊' as service_user_name,'80980614' as work_no,'池春梅' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '105355' as customer_no,'80972915' as service_user_work_no,'陈伟豪' as service_user_name,'80972915' as work_no,'陈伟豪' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '105886' as customer_no,'80691224' as service_user_work_no,'王少端' as service_user_name,'80691224' as work_no,'王少端' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
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
union all   select '104007' as customer_no,'80691224' as service_user_work_no,'王少端' as service_user_name,'80691224' as work_no,'王少端' as sales_name,'是' as is_part_time_service_manager,0 as sales_sale_rate,0 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '104054' as customer_no,'81129243' as service_user_work_no,'汪敏禄' as service_user_name,'81129243' as work_no,'汪敏禄' as sales_name,'是' as is_part_time_service_manager,0 as sales_sale_rate,0 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '104165' as customer_no,'80691224' as service_user_work_no,'王少端' as service_user_name,'80691224' as work_no,'王少端' as sales_name,'是' as is_part_time_service_manager,0 as sales_sale_rate,0 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
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
union all   select '108127' as customer_no,'81133021' as service_user_work_no,'黄升' as service_user_name,'81133021' as work_no,'黄升' as sales_name,'是' as is_part_time_service_manager,0 as sales_sale_rate,0 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '108739' as customer_no,'81099343' as service_user_work_no,'林瑾鑫' as service_user_name,'81099343' as work_no,'林瑾鑫' as sales_name,'是' as is_part_time_service_manager,0 as sales_sale_rate,0 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '109377' as customer_no,'81099343' as service_user_work_no,'林瑾鑫' as service_user_name,'81099343' as work_no,'林瑾鑫' as sales_name,'是' as is_part_time_service_manager,0 as sales_sale_rate,0 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '111241' as customer_no,'80952742' as service_user_work_no,'王秀云' as service_user_name,'80952742' as work_no,'王秀云' as sales_name,'是' as is_part_time_service_manager,0 as sales_sale_rate,0 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '112062' as customer_no,'80816155' as service_user_work_no,'张磊磊' as service_user_name,'80816155' as work_no,'张磊磊' as sales_name,'是' as is_part_time_service_manager,0 as sales_sale_rate,0 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '112327' as customer_no,'80691224' as service_user_work_no,'王少端' as service_user_name,'80691224' as work_no,'王少端' as sales_name,'是' as is_part_time_service_manager,0 as sales_sale_rate,0 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '113101' as customer_no,'80691224' as service_user_work_no,'王少端' as service_user_name,'80691224' as work_no,'王少端' as sales_name,'是' as is_part_time_service_manager,0 as sales_sale_rate,0 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '113635' as customer_no,'80691224' as service_user_work_no,'王少端' as service_user_name,'80691224' as work_no,'王少端' as sales_name,'是' as is_part_time_service_manager,0 as sales_sale_rate,0 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '113646' as customer_no,'80691224' as service_user_work_no,'王少端' as service_user_name,'80691224' as work_no,'王少端' as sales_name,'是' as is_part_time_service_manager,0 as sales_sale_rate,0 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '114344' as customer_no,'' as service_user_work_no,'' as service_user_name,'80005782' as work_no,'杨海燕' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '115324' as customer_no,'80816155' as service_user_work_no,'张磊磊' as service_user_name,'80816155' as work_no,'张磊磊' as sales_name,'是' as is_part_time_service_manager,0 as sales_sale_rate,0 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '115646' as customer_no,'80816155' as service_user_work_no,'张磊磊' as service_user_name,'80816155' as work_no,'张磊磊' as sales_name,'是' as is_part_time_service_manager,0 as sales_sale_rate,0 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '115679' as customer_no,'80816155' as service_user_work_no,'张磊磊' as service_user_name,'80816155' as work_no,'张磊磊' as sales_name,'是' as is_part_time_service_manager,0 as sales_sale_rate,0 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '115857' as customer_no,'80816155' as service_user_work_no,'张磊磊' as service_user_name,'80816155' as work_no,'张磊磊' as sales_name,'是' as is_part_time_service_manager,0 as sales_sale_rate,0 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '116211' as customer_no,'80816155' as service_user_work_no,'张磊磊' as service_user_name,'80816155' as work_no,'张磊磊' as sales_name,'是' as is_part_time_service_manager,0 as sales_sale_rate,0 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '118687' as customer_no,'81099343' as service_user_work_no,'林瑾鑫' as service_user_name,'81099343' as work_no,'林瑾鑫' as sales_name,'是' as is_part_time_service_manager,0 as sales_sale_rate,0 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select 'PF0094' as customer_no,'81099343' as service_user_work_no,'林瑾鑫' as service_user_name,'81099343' as work_no,'林瑾鑫' as sales_name,'是' as is_part_time_service_manager,0 as sales_sale_rate,0 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select 'PF1205' as customer_no,'80816155' as service_user_work_no,'张磊磊' as service_user_name,'80816155' as work_no,'张磊磊' as sales_name,'是' as is_part_time_service_manager,0 as sales_sale_rate,0 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '103995' as customer_no,'' as service_user_work_no,'' as service_user_name,'80907460' as work_no,'薛小伟' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '107683' as customer_no,'' as service_user_work_no,'' as service_user_name,'80907460' as work_no,'薛小伟' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '111122' as customer_no,'81111582' as service_user_work_no,'蔡汶艳' as service_user_name,'80958965' as work_no,'李斌' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '117095' as customer_no,'81111582' as service_user_work_no,'蔡汶艳' as service_user_name,'80958965' as work_no,'李斌' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '118540' as customer_no,'81111582' as service_user_work_no,'蔡汶艳' as service_user_name,'81016134' as work_no,'钱梓强' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '116957' as customer_no,'' as service_user_work_no,'' as service_user_name,'81055718' as work_no,'吴庆平' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '114524' as customer_no,'' as service_user_work_no,'' as service_user_name,'80083849' as work_no,'黄丽' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '115195' as customer_no,'' as service_user_work_no,'' as service_user_name,'80083849' as work_no,'黄丽' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '120024' as customer_no,'' as service_user_work_no,'' as service_user_name,'80952743' as work_no,'兰华明' as sales_name,'否' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '120317' as customer_no,'' as service_user_work_no,'' as service_user_name,'81084686' as work_no,'何丽姿' as sales_name,'否' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '107901' as customer_no,'' as service_user_work_no,'' as service_user_name,'80958648' as work_no,'王萃芸' as sales_name,'否' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '115753' as customer_no,'' as service_user_work_no,'' as service_user_name,'80958648' as work_no,'王萃芸' as sales_name,'否' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '118569' as customer_no,'' as service_user_work_no,'' as service_user_name,'80958648' as work_no,'王萃芸' as sales_name,'否' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '119688' as customer_no,'80912927' as service_user_work_no,'刘秋霞' as service_user_name,'81129344' as work_no,'洪少灵' as sales_name,'否' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '117145' as customer_no,'' as service_user_work_no,'' as service_user_name,'81129344' as work_no,'洪少灵' as sales_name,'否' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '119021' as customer_no,'' as service_user_work_no,'' as service_user_name,'81129344' as work_no,'洪少灵' as sales_name,'否' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '119454' as customer_no,'' as service_user_work_no,'' as service_user_name,'81129006' as work_no,'林君' as sales_name,'否' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '121444' as customer_no,'' as service_user_work_no,'' as service_user_name,'81079631' as work_no,'李燕玲' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '121229' as customer_no,'' as service_user_work_no,'' as service_user_name,'81079631' as work_no,'李燕玲' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '121443' as customer_no,'' as service_user_work_no,'' as service_user_name,'81079631' as work_no,'李燕玲' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '123035' as customer_no,'' as service_user_work_no,'' as service_user_name,'81099363' as work_no,'南召雪' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '119925' as customer_no,'' as service_user_work_no,'' as service_user_name,'81107987' as work_no,'赵杰' as sales_name,'否' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '122495' as customer_no,'' as service_user_work_no,'' as service_user_name,'81107987' as work_no,'赵杰' as sales_name,'否' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '105502' as customer_no,'' as service_user_work_no,'' as service_user_name,'80890405' as work_no,'瞿林峰' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '105761' as customer_no,'' as service_user_work_no,'' as service_user_name,'81084752' as work_no,'张鹏' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '106346' as customer_no,'' as service_user_work_no,'' as service_user_name,'81053313' as work_no,'段冬冬' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '107093' as customer_no,'' as service_user_work_no,'' as service_user_name,'80160212' as work_no,'徐芸' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '107408' as customer_no,'' as service_user_work_no,'' as service_user_name,'80890405' as work_no,'瞿林峰' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '109435' as customer_no,'' as service_user_work_no,'' as service_user_name,'80160212' as work_no,'徐芸' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '113439' as customer_no,'' as service_user_work_no,'' as service_user_name,'80890405' as work_no,'瞿林峰' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '114269' as customer_no,'' as service_user_work_no,'' as service_user_name,'80890405' as work_no,'瞿林峰' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '114576' as customer_no,'' as service_user_work_no,'' as service_user_name,'80890405' as work_no,'瞿林峰' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '114682' as customer_no,'81053313' as service_user_work_no,'段冬冬' as service_user_name,'80890405' as work_no,'瞿林峰' as sales_name,'是' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '114945' as customer_no,'' as service_user_work_no,'' as service_user_name,'81053313' as work_no,'段冬冬' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '115883' as customer_no,'' as service_user_work_no,'' as service_user_name,'80890405' as work_no,'瞿林峰' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '116237' as customer_no,'' as service_user_work_no,'' as service_user_name,'80160212' as work_no,'徐芸' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '116527' as customer_no,'' as service_user_work_no,'' as service_user_name,'80160212' as work_no,'徐芸' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '116721' as customer_no,'81053313' as service_user_work_no,'段冬冬' as service_user_name,'80890405' as work_no,'瞿林峰' as sales_name,'是' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '116727' as customer_no,'' as service_user_work_no,'' as service_user_name,'80890405' as work_no,'瞿林峰' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '116915' as customer_no,'' as service_user_work_no,'' as service_user_name,'80890405' as work_no,'瞿林峰' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '118458' as customer_no,'' as service_user_work_no,'' as service_user_name,'81089337' as work_no,'葛香俊' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '118582' as customer_no,'' as service_user_work_no,'' as service_user_name,'81084752' as work_no,'张鹏' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '119441' as customer_no,'' as service_user_work_no,'' as service_user_name,'81084752' as work_no,'张鹏' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '119965' as customer_no,'' as service_user_work_no,'' as service_user_name,'81053313' as work_no,'段冬冬' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '119992' as customer_no,'' as service_user_work_no,'' as service_user_name,'81089337' as work_no,'葛香俊' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '120410' as customer_no,'' as service_user_work_no,'' as service_user_name,'81084752' as work_no,'张鹏' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '120618' as customer_no,'' as service_user_work_no,'' as service_user_name,'81053313' as work_no,'段冬冬' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '121128' as customer_no,'' as service_user_work_no,'' as service_user_name,'81084752' as work_no,'张鹏' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '121288' as customer_no,'' as service_user_work_no,'' as service_user_name,'81053313' as work_no,'段冬冬' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '121300' as customer_no,'' as service_user_work_no,'' as service_user_name,'81089337' as work_no,'葛香俊' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '121703' as customer_no,'' as service_user_work_no,'' as service_user_name,'81089337' as work_no,'葛香俊' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '122152' as customer_no,'' as service_user_work_no,'' as service_user_name,'81089337' as work_no,'葛香俊' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '123240' as customer_no,'' as service_user_work_no,'' as service_user_name,'81053313' as work_no,'段冬冬' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '123279' as customer_no,'' as service_user_work_no,'' as service_user_name,'80160212' as work_no,'徐芸' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '123365' as customer_no,'' as service_user_work_no,'' as service_user_name,'81084752' as work_no,'张鹏' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '124046' as customer_no,'' as service_user_work_no,'' as service_user_name,'80160212' as work_no,'徐芸' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '125290' as customer_no,'' as service_user_work_no,'' as service_user_name,'81089337' as work_no,'葛香俊' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '117755' as customer_no,'81075199' as service_user_work_no,'万荣' as service_user_name,'80948458' as work_no,'杨旻杰' as sales_name,'是' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '124484' as customer_no,'81075199' as service_user_work_no,'万荣' as service_user_name,'80948458' as work_no,'杨旻杰' as sales_name,'是' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '105896' as customer_no,'81089337' as service_user_work_no,'葛香俊' as service_user_name,'80887605' as work_no,'朱祥如' as sales_name,'是' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '122091' as customer_no,'81089337' as service_user_work_no,'葛香俊' as service_user_name,'80887605' as work_no,'朱祥如' as sales_name,'是' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '122171' as customer_no,'81089337' as service_user_work_no,'葛香俊' as service_user_name,'80887605' as work_no,'朱祥如' as sales_name,'是' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '105887' as customer_no,'' as service_user_work_no,'' as service_user_name,'81080105' as work_no,'张元浩' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '108637' as customer_no,'' as service_user_work_no,'' as service_user_name,'81080105' as work_no,'张元浩' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '115311' as customer_no,'' as service_user_work_no,'' as service_user_name,'81080105' as work_no,'张元浩' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '120847' as customer_no,'' as service_user_work_no,'' as service_user_name,'81080105' as work_no,'张元浩' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '121480' as customer_no,'' as service_user_work_no,'' as service_user_name,'81080105' as work_no,'张元浩' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '121488' as customer_no,'' as service_user_work_no,'' as service_user_name,'81080105' as work_no,'张元浩' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '122543' as customer_no,'' as service_user_work_no,'' as service_user_name,'81080105' as work_no,'张元浩' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '123342' as customer_no,'' as service_user_work_no,'' as service_user_name,'81080105' as work_no,'张元浩' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '123896' as customer_no,'81052035' as service_user_work_no,'袁成臣' as service_user_name,'80913079' as work_no,'张宇' as sales_name,'是' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '104623' as customer_no,'81084752' as service_user_work_no,'张鹏' as service_user_name,'80913408' as work_no,'杨丹丹' as sales_name,'是' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '105748' as customer_no,'81084752' as service_user_work_no,'张鹏' as service_user_name,'80913408' as work_no,'杨丹丹' as sales_name,'是' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '105862' as customer_no,'81084752' as service_user_work_no,'张鹏' as service_user_name,'80913408' as work_no,'杨丹丹' as sales_name,'是' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '105891' as customer_no,'81084752' as service_user_work_no,'张鹏' as service_user_name,'80913408' as work_no,'杨丹丹' as sales_name,'是' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '109369' as customer_no,'81084752' as service_user_work_no,'张鹏' as service_user_name,'80913408' as work_no,'杨丹丹' as sales_name,'是' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '111842' as customer_no,'81084752' as service_user_work_no,'张鹏' as service_user_name,'80913408' as work_no,'杨丹丹' as sales_name,'是' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '113695' as customer_no,'81084752' as service_user_work_no,'张鹏' as service_user_name,'80913408' as work_no,'杨丹丹' as sales_name,'是' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '116566' as customer_no,'81084752' as service_user_work_no,'张鹏' as service_user_name,'80913408' as work_no,'杨丹丹' as sales_name,'是' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '117017' as customer_no,'81084752' as service_user_work_no,'张鹏' as service_user_name,'80913408' as work_no,'杨丹丹' as sales_name,'是' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '117090' as customer_no,'81084752' as service_user_work_no,'张鹏' as service_user_name,'80913408' as work_no,'杨丹丹' as sales_name,'是' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '121055' as customer_no,'81084752' as service_user_work_no,'张鹏' as service_user_name,'80913408' as work_no,'杨丹丹' as sales_name,'是' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '121856' as customer_no,'81084752' as service_user_work_no,'张鹏' as service_user_name,'80913408' as work_no,'杨丹丹' as sales_name,'是' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '121861' as customer_no,'81084752' as service_user_work_no,'张鹏' as service_user_name,'80913408' as work_no,'杨丹丹' as sales_name,'是' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '121866' as customer_no,'81084752' as service_user_work_no,'张鹏' as service_user_name,'80913408' as work_no,'杨丹丹' as sales_name,'是' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '121871' as customer_no,'81084752' as service_user_work_no,'张鹏' as service_user_name,'80913408' as work_no,'杨丹丹' as sales_name,'是' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '123225' as customer_no,'81084752' as service_user_work_no,'张鹏' as service_user_name,'80913408' as work_no,'杨丹丹' as sales_name,'是' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '123901' as customer_no,'81084752' as service_user_work_no,'张鹏' as service_user_name,'80913408' as work_no,'杨丹丹' as sales_name,'是' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '124668' as customer_no,'81084752' as service_user_work_no,'张鹏' as service_user_name,'80913408' as work_no,'杨丹丹' as sales_name,'是' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '105552' as customer_no,'' as service_user_work_no,'' as service_user_name,'80980678' as work_no,'谢建军' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '105839' as customer_no,'' as service_user_work_no,'' as service_user_name,'80980678' as work_no,'谢建军' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '106709' as customer_no,'' as service_user_work_no,'' as service_user_name,'80980678' as work_no,'谢建军' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '108461' as customer_no,'' as service_user_work_no,'' as service_user_name,'80980678' as work_no,'谢建军' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '111560' as customer_no,'' as service_user_work_no,'' as service_user_name,'80980678' as work_no,'谢建军' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '112915' as customer_no,'' as service_user_work_no,'' as service_user_name,'80980678' as work_no,'谢建军' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '114173' as customer_no,'' as service_user_work_no,'' as service_user_name,'80980678' as work_no,'谢建军' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '114968' as customer_no,'' as service_user_work_no,'' as service_user_name,'80980678' as work_no,'谢建军' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '116925' as customer_no,'' as service_user_work_no,'' as service_user_name,'80980678' as work_no,'谢建军' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '117332' as customer_no,'' as service_user_work_no,'' as service_user_name,'80980678' as work_no,'谢建军' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '118449' as customer_no,'' as service_user_work_no,'' as service_user_name,'80980678' as work_no,'谢建军' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '120333' as customer_no,'' as service_user_work_no,'' as service_user_name,'80980678' as work_no,'谢建军' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '121233' as customer_no,'' as service_user_work_no,'' as service_user_name,'80980678' as work_no,'谢建军' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '124546' as customer_no,'' as service_user_work_no,'' as service_user_name,'80980678' as work_no,'谢建军' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '122089' as customer_no,'' as service_user_work_no,'' as service_user_name,'81145749' as work_no,'刘一鸣' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '122148' as customer_no,'' as service_user_work_no,'' as service_user_name,'81145749' as work_no,'刘一鸣' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '122164' as customer_no,'' as service_user_work_no,'' as service_user_name,'81145749' as work_no,'刘一鸣' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '122190' as customer_no,'' as service_user_work_no,'' as service_user_name,'81145749' as work_no,'刘一鸣' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '122758' as customer_no,'' as service_user_work_no,'' as service_user_name,'81145749' as work_no,'刘一鸣' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '122780' as customer_no,'' as service_user_work_no,'' as service_user_name,'81145749' as work_no,'刘一鸣' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '124533' as customer_no,'' as service_user_work_no,'' as service_user_name,'81145749' as work_no,'刘一鸣' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '107897' as customer_no,'' as service_user_work_no,'' as service_user_name,'80970937' as work_no,'高京庭' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '109456' as customer_no,'' as service_user_work_no,'' as service_user_name,'80970937' as work_no,'高京庭' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '117862' as customer_no,'' as service_user_work_no,'' as service_user_name,'80970937' as work_no,'高京庭' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '119760' as customer_no,'' as service_user_work_no,'' as service_user_name,'80970937' as work_no,'高京庭' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '120456' as customer_no,'' as service_user_work_no,'' as service_user_name,'80970937' as work_no,'高京庭' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '121100' as customer_no,'' as service_user_work_no,'' as service_user_name,'80970937' as work_no,'高京庭' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '121193' as customer_no,'' as service_user_work_no,'' as service_user_name,'80970937' as work_no,'高京庭' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '120595' as customer_no,'' as service_user_work_no,'' as service_user_name,'81089336' as work_no,'陈静' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '105774' as customer_no,'' as service_user_work_no,'' as service_user_name,'80935770' as work_no,'陈海洋' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '107411' as customer_no,'' as service_user_work_no,'' as service_user_name,'80935770' as work_no,'陈海洋' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '107530' as customer_no,'' as service_user_work_no,'' as service_user_name,'80935770' as work_no,'陈海洋' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '108282' as customer_no,'' as service_user_work_no,'' as service_user_name,'80935770' as work_no,'陈海洋' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '113487' as customer_no,'' as service_user_work_no,'' as service_user_name,'80935770' as work_no,'陈海洋' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '113550' as customer_no,'' as service_user_work_no,'' as service_user_name,'80935770' as work_no,'陈海洋' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '113731' as customer_no,'' as service_user_work_no,'' as service_user_name,'80935770' as work_no,'陈海洋' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '113857' as customer_no,'' as service_user_work_no,'' as service_user_name,'80935770' as work_no,'陈海洋' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '122147' as customer_no,'' as service_user_work_no,'' as service_user_name,'80935770' as work_no,'陈海洋' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '122159' as customer_no,'' as service_user_work_no,'' as service_user_name,'80935770' as work_no,'陈海洋' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '122167' as customer_no,'' as service_user_work_no,'' as service_user_name,'80935770' as work_no,'陈海洋' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '122185' as customer_no,'' as service_user_work_no,'' as service_user_name,'80935770' as work_no,'陈海洋' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '122186' as customer_no,'' as service_user_work_no,'' as service_user_name,'80935770' as work_no,'陈海洋' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '122188' as customer_no,'' as service_user_work_no,'' as service_user_name,'80935770' as work_no,'陈海洋' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '111764' as customer_no,'' as service_user_work_no,'' as service_user_name,'81014929' as work_no,'张路平' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '112285' as customer_no,'' as service_user_work_no,'' as service_user_name,'81014929' as work_no,'张路平' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '112681' as customer_no,'' as service_user_work_no,'' as service_user_name,'81014929' as work_no,'张路平' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '113015' as customer_no,'' as service_user_work_no,'' as service_user_name,'81014929' as work_no,'张路平' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '113394' as customer_no,'' as service_user_work_no,'' as service_user_name,'81014929' as work_no,'张路平' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '114650' as customer_no,'' as service_user_work_no,'' as service_user_name,'81014929' as work_no,'张路平' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '114804' as customer_no,'' as service_user_work_no,'' as service_user_name,'81014929' as work_no,'张路平' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '116987' as customer_no,'' as service_user_work_no,'' as service_user_name,'81014929' as work_no,'张路平' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '117311' as customer_no,'' as service_user_work_no,'' as service_user_name,'81014929' as work_no,'张路平' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '118762' as customer_no,'' as service_user_work_no,'' as service_user_name,'81014929' as work_no,'张路平' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '120495' as customer_no,'' as service_user_work_no,'' as service_user_name,'81014929' as work_no,'张路平' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '121890' as customer_no,'' as service_user_work_no,'' as service_user_name,'81014929' as work_no,'张路平' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '123341' as customer_no,'' as service_user_work_no,'' as service_user_name,'81014929' as work_no,'张路平' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '125370' as customer_no,'' as service_user_work_no,'' as service_user_name,'81014929' as work_no,'张路平' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '104281' as customer_no,'' as service_user_work_no,'' as service_user_name,'80974184' as work_no,'郭荔丽' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '115656' as customer_no,'' as service_user_work_no,'' as service_user_name,'80974184' as work_no,'郭荔丽' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '105593' as customer_no,'' as service_user_work_no,'' as service_user_name,'1000002' as work_no,'莆田B' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '107398' as customer_no,'' as service_user_work_no,'' as service_user_name,'80974184' as work_no,'郭荔丽' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '109447' as customer_no,'' as service_user_work_no,'' as service_user_name,'1000002' as work_no,'莆田B' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '111207' as customer_no,'' as service_user_work_no,'' as service_user_name,'80974184' as work_no,'郭荔丽' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '115537' as customer_no,'' as service_user_work_no,'' as service_user_name,'80974184' as work_no,'郭荔丽' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '115602' as customer_no,'' as service_user_work_no,'' as service_user_name,'80974184' as work_no,'郭荔丽' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '115826' as customer_no,'' as service_user_work_no,'' as service_user_name,'80974184' as work_no,'郭荔丽' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '117244' as customer_no,'' as service_user_work_no,'' as service_user_name,'80974184' as work_no,'郭荔丽' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '123065' as customer_no,'' as service_user_work_no,'' as service_user_name,'1000002' as work_no,'莆田B' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '125534' as customer_no,'' as service_user_work_no,'' as service_user_name,'80974184' as work_no,'郭荔丽' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '116015' as customer_no,'' as service_user_work_no,'' as service_user_name,'81139788' as work_no,'邓肯' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '115721' as customer_no,'' as service_user_work_no,'' as service_user_name,'81122750' as work_no,'吴煌锦' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '119703' as customer_no,'' as service_user_work_no,'' as service_user_name,'81122750' as work_no,'吴煌锦' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '118887' as customer_no,'' as service_user_work_no,'' as service_user_name,'81122750' as work_no,'吴煌锦' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '116556' as customer_no,'' as service_user_work_no,'' as service_user_name,'81139788' as work_no,'邓肯' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '116733' as customer_no,'' as service_user_work_no,'' as service_user_name,'81055717' as work_no,'魏隆强' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '116702' as customer_no,'' as service_user_work_no,'' as service_user_name,'81122750' as work_no,'吴煌锦' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '116923' as customer_no,'' as service_user_work_no,'' as service_user_name,'81122750' as work_no,'吴煌锦' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '116863' as customer_no,'' as service_user_work_no,'' as service_user_name,'81122750' as work_no,'吴煌锦' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '116883' as customer_no,'' as service_user_work_no,'' as service_user_name,'81122750' as work_no,'吴煌锦' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '116877' as customer_no,'' as service_user_work_no,'' as service_user_name,'81122750' as work_no,'吴煌锦' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '116826' as customer_no,'' as service_user_work_no,'' as service_user_name,'81139788' as work_no,'邓肯' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '123534' as customer_no,'' as service_user_work_no,'' as service_user_name,'81055717' as work_no,'魏隆强' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '116967' as customer_no,'' as service_user_work_no,'' as service_user_name,'81122750' as work_no,'吴煌锦' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '117026' as customer_no,'' as service_user_work_no,'' as service_user_name,'81122750' as work_no,'吴煌锦' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '117025' as customer_no,'' as service_user_work_no,'' as service_user_name,'81122750' as work_no,'吴煌锦' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '117009' as customer_no,'' as service_user_work_no,'' as service_user_name,'81122750' as work_no,'吴煌锦' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '117045' as customer_no,'' as service_user_work_no,'' as service_user_name,'81122750' as work_no,'吴煌锦' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '117040' as customer_no,'' as service_user_work_no,'' as service_user_name,'81122750' as work_no,'吴煌锦' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '117035' as customer_no,'' as service_user_work_no,'' as service_user_name,'81122750' as work_no,'吴煌锦' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '116944' as customer_no,'' as service_user_work_no,'' as service_user_name,'81122750' as work_no,'吴煌锦' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '117030' as customer_no,'' as service_user_work_no,'' as service_user_name,'81122750' as work_no,'吴煌锦' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '117027' as customer_no,'' as service_user_work_no,'' as service_user_name,'81122750' as work_no,'吴煌锦' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '116903' as customer_no,'' as service_user_work_no,'' as service_user_name,'81122750' as work_no,'吴煌锦' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '116994' as customer_no,'' as service_user_work_no,'' as service_user_name,'81122750' as work_no,'吴煌锦' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '117019' as customer_no,'' as service_user_work_no,'' as service_user_name,'81122750' as work_no,'吴煌锦' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '116993' as customer_no,'' as service_user_work_no,'' as service_user_name,'81122750' as work_no,'吴煌锦' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '116989' as customer_no,'' as service_user_work_no,'' as service_user_name,'81122750' as work_no,'吴煌锦' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '123716' as customer_no,'' as service_user_work_no,'' as service_user_name,'81122750' as work_no,'吴煌锦' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '120100' as customer_no,'' as service_user_work_no,'' as service_user_name,'81122750' as work_no,'吴煌锦' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '119806' as customer_no,'' as service_user_work_no,'' as service_user_name,'81122750' as work_no,'吴煌锦' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '119803' as customer_no,'' as service_user_work_no,'' as service_user_name,'81122750' as work_no,'吴煌锦' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '119815' as customer_no,'' as service_user_work_no,'' as service_user_name,'81122750' as work_no,'吴煌锦' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '120463' as customer_no,'' as service_user_work_no,'' as service_user_name,'90000002' as work_no,'三明B' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '122125' as customer_no,'' as service_user_work_no,'' as service_user_name,'81122750' as work_no,'吴煌锦' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '122106' as customer_no,'' as service_user_work_no,'' as service_user_name,'81122750' as work_no,'吴煌锦' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '121763' as customer_no,'' as service_user_work_no,'' as service_user_name,'81122750' as work_no,'吴煌锦' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '122375' as customer_no,'' as service_user_work_no,'' as service_user_name,'81122750' as work_no,'吴煌锦' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '122286' as customer_no,'' as service_user_work_no,'' as service_user_name,'81122750' as work_no,'吴煌锦' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '122371' as customer_no,'' as service_user_work_no,'' as service_user_name,'81122750' as work_no,'吴煌锦' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '123060' as customer_no,'' as service_user_work_no,'' as service_user_name,'90000002' as work_no,'三明B' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '123383' as customer_no,'' as service_user_work_no,'' as service_user_name,'LL000001' as work_no,'彭先檩（三明）' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '123644' as customer_no,'' as service_user_work_no,'' as service_user_name,'81122750' as work_no,'吴煌锦' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '123813' as customer_no,'' as service_user_work_no,'' as service_user_name,'81122750' as work_no,'吴煌锦' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '124179' as customer_no,'' as service_user_work_no,'' as service_user_name,'81102377' as work_no,'刘寒漪' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '125244' as customer_no,'' as service_user_work_no,'' as service_user_name,'81139788' as work_no,'邓肯' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '125508' as customer_no,'' as service_user_work_no,'' as service_user_name,'81122750' as work_no,'吴煌锦' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '103183' as customer_no,'' as service_user_work_no,'' as service_user_name,'80007454' as work_no,'李翔' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '104086' as customer_no,'' as service_user_work_no,'' as service_user_name,'81026931' as work_no,'林圳' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '104397' as customer_no,'' as service_user_work_no,'' as service_user_name,'80007454' as work_no,'李翔' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '100984' as customer_no,'' as service_user_work_no,'' as service_user_name,'80007454' as work_no,'李翔' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '105499' as customer_no,'' as service_user_work_no,'' as service_user_name,'81026931' as work_no,'林圳' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '114843' as customer_no,'' as service_user_work_no,'' as service_user_name,'81016757' as work_no,'吴周机' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '108589' as customer_no,'' as service_user_work_no,'' as service_user_name,'80936091' as work_no,'刘鹏' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '102580' as customer_no,'' as service_user_work_no,'' as service_user_name,'80929710' as work_no,'王坚' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '102890' as customer_no,'' as service_user_work_no,'' as service_user_name,'80895350' as work_no,'陈聪' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select 'PF0099' as customer_no,'' as service_user_work_no,'' as service_user_name,'80924363' as work_no,'胡康灿' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '116099' as customer_no,'' as service_user_work_no,'' as service_user_name,'80912701' as work_no,'蓝梦玲' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '106301' as customer_no,'' as service_user_work_no,'' as service_user_name,'81042140' as work_no,'林志雄' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '106306' as customer_no,'' as service_user_work_no,'' as service_user_name,'81042140' as work_no,'林志雄' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '120246' as customer_no,'' as service_user_work_no,'' as service_user_name,'81062977' as work_no,'邱维海' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '120689' as customer_no,'' as service_user_work_no,'' as service_user_name,'81118962' as work_no,'陈威仁' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '117047' as customer_no,'' as service_user_work_no,'' as service_user_name,'81043405' as work_no,'魏桓' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '118836' as customer_no,'' as service_user_work_no,'' as service_user_name,'81042140' as work_no,'林志雄' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '115899' as customer_no,'' as service_user_work_no,'' as service_user_name,'81118962' as work_no,'陈威仁' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '116398' as customer_no,'' as service_user_work_no,'' as service_user_name,'81043405' as work_no,'魏桓' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '122143' as customer_no,'' as service_user_work_no,'' as service_user_name,'81097555' as work_no,'沈文乾' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '117108' as customer_no,'' as service_user_work_no,'' as service_user_name,'81042140' as work_no,'林志雄' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '123827' as customer_no,'' as service_user_work_no,'' as service_user_name,'81042140' as work_no,'林志雄' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '117121' as customer_no,'' as service_user_work_no,'' as service_user_name,'81062977' as work_no,'邱维海' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '117222' as customer_no,'' as service_user_work_no,'' as service_user_name,'81097555' as work_no,'沈文乾' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '123442' as customer_no,'' as service_user_work_no,'' as service_user_name,'81062977' as work_no,'邱维海' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '120735' as customer_no,'' as service_user_work_no,'' as service_user_name,'81062977' as work_no,'邱维海' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '118183' as customer_no,'' as service_user_work_no,'' as service_user_name,'81118962' as work_no,'陈威仁' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '125137' as customer_no,'' as service_user_work_no,'' as service_user_name,'81043405' as work_no,'魏桓' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '123923' as customer_no,'' as service_user_work_no,'' as service_user_name,'LL000003' as work_no,'彭先檩（龙岩）' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '120836' as customer_no,'' as service_user_work_no,'' as service_user_name,'81042140' as work_no,'林志雄' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '123034' as customer_no,'' as service_user_work_no,'' as service_user_name,'90000001' as work_no,'龙岩B' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '123859' as customer_no,'' as service_user_work_no,'' as service_user_name,'81043405' as work_no,'魏桓' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '123650' as customer_no,'' as service_user_work_no,'' as service_user_name,'81062977' as work_no,'邱维海' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '124555' as customer_no,'' as service_user_work_no,'' as service_user_name,'90000001' as work_no,'龙岩B' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '124641' as customer_no,'' as service_user_work_no,'' as service_user_name,'81062977' as work_no,'邱维海' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '104318' as customer_no,'' as service_user_work_no,'' as service_user_name,'44555151' as work_no,'泉州B' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '104085' as customer_no,'' as service_user_work_no,'' as service_user_name,'44555151' as work_no,'泉州B' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '120781' as customer_no,'' as service_user_work_no,'' as service_user_name,'10000001' as work_no,'彭先檩（泉州）' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '118744' as customer_no,'' as service_user_work_no,'' as service_user_name,'81094607' as work_no,'林志高' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '106433' as customer_no,'' as service_user_work_no,'' as service_user_name,'44555151' as work_no,'泉州B' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '115051' as customer_no,'' as service_user_work_no,'' as service_user_name,'81129344' as work_no,'洪少灵' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '108283' as customer_no,'' as service_user_work_no,'' as service_user_name,'81094607' as work_no,'林志高' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '119897' as customer_no,'' as service_user_work_no,'' as service_user_name,'10000001' as work_no,'彭先檩（泉州）' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '109722' as customer_no,'' as service_user_work_no,'' as service_user_name,'81094607' as work_no,'林志高' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '114516' as customer_no,'' as service_user_work_no,'' as service_user_name,'80969261' as work_no,'黄诗偶' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '120365' as customer_no,'' as service_user_work_no,'' as service_user_name,'81094607' as work_no,'林志高' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '119210' as customer_no,'' as service_user_work_no,'' as service_user_name,'81094607' as work_no,'林志高' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '119168' as customer_no,'' as service_user_work_no,'' as service_user_name,'81094607' as work_no,'林志高' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '119519' as customer_no,'' as service_user_work_no,'' as service_user_name,'81094607' as work_no,'林志高' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '122555' as customer_no,'' as service_user_work_no,'' as service_user_name,'81094607' as work_no,'林志高' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '121507' as customer_no,'' as service_user_work_no,'' as service_user_name,'81094607' as work_no,'林志高' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '122269' as customer_no,'' as service_user_work_no,'' as service_user_name,'10000001' as work_no,'彭先檩（泉州）' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '123222' as customer_no,'' as service_user_work_no,'' as service_user_name,'10000001' as work_no,'彭先檩（泉州）' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '123262' as customer_no,'' as service_user_work_no,'' as service_user_name,'10000001' as work_no,'彭先檩（泉州）' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '123257' as customer_no,'' as service_user_work_no,'' as service_user_name,'10000001' as work_no,'彭先檩（泉州）' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '123242' as customer_no,'' as service_user_work_no,'' as service_user_name,'10000001' as work_no,'彭先檩（泉州）' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '123247' as customer_no,'' as service_user_work_no,'' as service_user_name,'10000001' as work_no,'彭先檩（泉州）' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '123253' as customer_no,'' as service_user_work_no,'' as service_user_name,'10000001' as work_no,'彭先檩（泉州）' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '124602' as customer_no,'' as service_user_work_no,'' as service_user_name,'10000001' as work_no,'彭先檩（泉州）' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '115205' as customer_no,'' as service_user_work_no,'' as service_user_name,'81088296' as work_no,'陈惠燕' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '110807' as customer_no,'' as service_user_work_no,'' as service_user_name,'81034648' as work_no,'李紫珊' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '115252' as customer_no,'' as service_user_work_no,'' as service_user_name,'81034648' as work_no,'李紫珊' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '115643' as customer_no,'' as service_user_work_no,'' as service_user_name,'81093307' as work_no,'黄少伟' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '117929' as customer_no,'' as service_user_work_no,'' as service_user_name,'81034648' as work_no,'李紫珊' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '120459' as customer_no,'' as service_user_work_no,'' as service_user_name,'XM000001' as work_no,'彭东京' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '121206' as customer_no,'' as service_user_work_no,'' as service_user_name,'XM000001' as work_no,'彭东京' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '124098' as customer_no,'' as service_user_work_no,'' as service_user_name,'81093307' as work_no,'黄少伟' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '102755' as customer_no,'' as service_user_work_no,'' as service_user_name,'10000002' as work_no,'彭先檩（厦门）' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '103175' as customer_no,'' as service_user_work_no,'' as service_user_name,'10000002' as work_no,'彭先檩（厦门）' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '103868' as customer_no,'' as service_user_work_no,'' as service_user_name,'10000002' as work_no,'彭先檩（厦门）' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '103874' as customer_no,'' as service_user_work_no,'' as service_user_name,'10000002' as work_no,'彭先檩（厦门）' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '103887' as customer_no,'' as service_user_work_no,'' as service_user_name,'10000002' as work_no,'彭先檩（厦门）' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '103898' as customer_no,'' as service_user_work_no,'' as service_user_name,'10000002' as work_no,'彭先檩（厦门）' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '103908' as customer_no,'' as service_user_work_no,'' as service_user_name,'10000002' as work_no,'彭先檩（厦门）' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '103926' as customer_no,'' as service_user_work_no,'' as service_user_name,'10000002' as work_no,'彭先檩（厦门）' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '103927' as customer_no,'' as service_user_work_no,'' as service_user_name,'10000002' as work_no,'彭先檩（厦门）' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '104460' as customer_no,'' as service_user_work_no,'' as service_user_name,'10000002' as work_no,'彭先檩（厦门）' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '109000' as customer_no,'' as service_user_work_no,'' as service_user_name,'80989132' as work_no,'姚市敏' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '105673' as customer_no,'' as service_user_work_no,'' as service_user_name,'81034648' as work_no,'李紫珊' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '106563' as customer_no,'' as service_user_work_no,'' as service_user_name,'10010007' as work_no,'厦门B' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '114522' as customer_no,'' as service_user_work_no,'' as service_user_name,'10000002' as work_no,'彭先檩（厦门）' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '115431' as customer_no,'' as service_user_work_no,'' as service_user_name,'10000002' as work_no,'彭先檩（厦门）' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '118504' as customer_no,'' as service_user_work_no,'' as service_user_name,'10000002' as work_no,'彭先檩（厦门）' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '120554' as customer_no,'' as service_user_work_no,'' as service_user_name,'XN000001' as work_no,'彭先檩（漳州）' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '113281' as customer_no,'' as service_user_work_no,'' as service_user_name,'LL000002' as work_no,'彭先檩（宁德）' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '115935' as customer_no,'' as service_user_work_no,'' as service_user_name,'81041732' as work_no,'符逢芬' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '118072' as customer_no,'' as service_user_work_no,'' as service_user_name,'81084686' as work_no,'何丽姿' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '118602' as customer_no,'' as service_user_work_no,'' as service_user_name,'80595641' as work_no,'王权威' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '118748' as customer_no,'' as service_user_work_no,'' as service_user_name,'80595641' as work_no,'王权威' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '119990' as customer_no,'' as service_user_work_no,'' as service_user_name,'81055537' as work_no,'林锋' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '125677' as customer_no,'' as service_user_work_no,'' as service_user_name,'LL000002' as work_no,'彭先檩（宁德）' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '110872' as customer_no,'' as service_user_work_no,'' as service_user_name,'80938757' as work_no,'周丽' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '118825' as customer_no,'' as service_user_work_no,'' as service_user_name,'80938757' as work_no,'周丽' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '105164' as customer_no,'' as service_user_work_no,'' as service_user_name,'80991769' as work_no,'郑宇祥' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '105165' as customer_no,'' as service_user_work_no,'' as service_user_name,'80991769' as work_no,'郑宇祥' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '119757' as customer_no,'' as service_user_work_no,'' as service_user_name,'80991769' as work_no,'郑宇祥' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '111628' as customer_no,'' as service_user_work_no,'' as service_user_name,'80991769' as work_no,'郑宇祥' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '111612' as customer_no,'' as service_user_work_no,'' as service_user_name,'80991769' as work_no,'郑宇祥' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '114289' as customer_no,'' as service_user_work_no,'' as service_user_name,'80991769' as work_no,'郑宇祥' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '117396' as customer_no,'' as service_user_work_no,'' as service_user_name,'80991769' as work_no,'郑宇祥' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '108162' as customer_no,'' as service_user_work_no,'' as service_user_name,'81043023' as work_no,'邓清兵' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '115829' as customer_no,'' as service_user_work_no,'' as service_user_name,'80751663' as work_no,'肖秀华' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '108180' as customer_no,'' as service_user_work_no,'' as service_user_name,'80751663' as work_no,'肖秀华' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '108152' as customer_no,'' as service_user_work_no,'' as service_user_name,'80751663' as work_no,'肖秀华' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '116445' as customer_no,'' as service_user_work_no,'' as service_user_name,'80751663' as work_no,'肖秀华' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '120458' as customer_no,'' as service_user_work_no,'' as service_user_name,'80751663' as work_no,'肖秀华' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '123032' as customer_no,'' as service_user_work_no,'' as service_user_name,'80751663' as work_no,'肖秀华' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '124481' as customer_no,'' as service_user_work_no,'' as service_user_name,'80751663' as work_no,'肖秀华' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '125355' as customer_no,'' as service_user_work_no,'' as service_user_name,'80751663' as work_no,'肖秀华' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '111331' as customer_no,'' as service_user_work_no,'' as service_user_name,'81051600' as work_no,'吴玉萍' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '110863' as customer_no,'' as service_user_work_no,'' as service_user_name,'81051600' as work_no,'吴玉萍' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '114667' as customer_no,'' as service_user_work_no,'' as service_user_name,'81051600' as work_no,'吴玉萍' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '108267' as customer_no,'' as service_user_work_no,'' as service_user_name,'81051600' as work_no,'吴玉萍' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '111298' as customer_no,'' as service_user_work_no,'' as service_user_name,'81051600' as work_no,'吴玉萍' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '115476' as customer_no,'' as service_user_work_no,'' as service_user_name,'81051600' as work_no,'吴玉萍' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '118914' as customer_no,'' as service_user_work_no,'' as service_user_name,'81006145' as work_no,'池万春' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '111318' as customer_no,'' as service_user_work_no,'' as service_user_name,'81006145' as work_no,'池万春' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '111336' as customer_no,'' as service_user_work_no,'' as service_user_name,'81006145' as work_no,'池万春' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '112629' as customer_no,'' as service_user_work_no,'' as service_user_name,'81006145' as work_no,'池万春' as sales_name,'是' as is_part_time_service_manager,0.1 as sales_sale_rate,0.2 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '116242' as customer_no,'' as service_user_work_no,'' as service_user_name,'81052035' as work_no,'袁成臣' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '116639' as customer_no,'' as service_user_work_no,'' as service_user_name,'81052035' as work_no,'袁成臣' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '117304' as customer_no,'' as service_user_work_no,'' as service_user_name,'81052035' as work_no,'袁成臣' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '117605' as customer_no,'' as service_user_work_no,'' as service_user_name,'81052035' as work_no,'袁成臣' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '112181' as customer_no,'' as service_user_work_no,'' as service_user_name,'80965415' as work_no,'王旭' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '113641' as customer_no,'' as service_user_work_no,'' as service_user_name,'80965415' as work_no,'王旭' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '117119' as customer_no,'' as service_user_work_no,'' as service_user_name,'80965415' as work_no,'王旭' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '118824' as customer_no,'' as service_user_work_no,'' as service_user_name,'81089336' as work_no,'陈静' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '118848' as customer_no,'' as service_user_work_no,'' as service_user_name,'81089336' as work_no,'陈静' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '119196' as customer_no,'' as service_user_work_no,'' as service_user_name,'81089336' as work_no,'陈静' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '120098' as customer_no,'' as service_user_work_no,'' as service_user_name,'81089336' as work_no,'陈静' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '120130' as customer_no,'' as service_user_work_no,'' as service_user_name,'81089336' as work_no,'陈静' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '120985' as customer_no,'' as service_user_work_no,'' as service_user_name,'81089336' as work_no,'陈静' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '121368' as customer_no,'' as service_user_work_no,'' as service_user_name,'81089336' as work_no,'陈静' as sales_name,'是' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '123623' as customer_no,'' as service_user_work_no,'' as service_user_name,'81122116' as work_no,'陈滨滨' as sales_name,'是' as is_part_time_service_manager,0 as sales_sale_rate,0 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '103096' as customer_no,'' as service_user_work_no,'' as service_user_name,'80927693' as work_no,'王玉花' as sales_name,'是' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '120708' as customer_no,'81144439' as service_user_work_no,'李超' as service_user_name,'80768307' as work_no,'肖洪峰' as sales_name,'是' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '120757' as customer_no,'81130569' as service_user_work_no,'李锦程' as service_user_name,'81129879' as work_no,'刘伟' as sales_name,'是' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '120803' as customer_no,'81130569' as service_user_work_no,'李锦程' as service_user_name,'81129879' as work_no,'刘伟' as sales_name,'是' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '120828' as customer_no,'81130569' as service_user_work_no,'李锦程' as service_user_name,'81129879' as work_no,'刘伟' as sales_name,'是' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '120861' as customer_no,'81130569' as service_user_work_no,'李锦程' as service_user_name,'81129879' as work_no,'刘伟' as sales_name,'是' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '120881' as customer_no,'81130569' as service_user_work_no,'李锦程' as service_user_name,'81129879' as work_no,'刘伟' as sales_name,'是' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '120882' as customer_no,'81130569' as service_user_work_no,'李锦程' as service_user_name,'81129879' as work_no,'刘伟' as sales_name,'是' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '120883' as customer_no,'81130569' as service_user_work_no,'李锦程' as service_user_name,'81129879' as work_no,'刘伟' as sales_name,'是' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '120888' as customer_no,'81130569' as service_user_work_no,'李锦程' as service_user_name,'81129879' as work_no,'刘伟' as sales_name,'是' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '120902' as customer_no,'81130569' as service_user_work_no,'李锦程' as service_user_name,'81129879' as work_no,'刘伟' as sales_name,'是' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '120904' as customer_no,'81144439' as service_user_work_no,'李超' as service_user_name,'80768307' as work_no,'肖洪峰' as sales_name,'是' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '120906' as customer_no,'81130569' as service_user_work_no,'李锦程' as service_user_name,'81129879' as work_no,'刘伟' as sales_name,'是' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '120918' as customer_no,'81130569' as service_user_work_no,'李锦程' as service_user_name,'81129879' as work_no,'刘伟' as sales_name,'是' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '120920' as customer_no,'81144439' as service_user_work_no,'李超' as service_user_name,'80768307' as work_no,'肖洪峰' as sales_name,'是' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '120923' as customer_no,'81144439' as service_user_work_no,'李超' as service_user_name,'80768307' as work_no,'肖洪峰' as sales_name,'是' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '120932' as customer_no,'81144439' as service_user_work_no,'李超' as service_user_name,'80768307' as work_no,'肖洪峰' as sales_name,'是' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '120933' as customer_no,'81144439' as service_user_work_no,'李超' as service_user_name,'80768307' as work_no,'肖洪峰' as sales_name,'是' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '120934' as customer_no,'81130569' as service_user_work_no,'李锦程' as service_user_name,'81129879' as work_no,'刘伟' as sales_name,'是' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '120949' as customer_no,'81144439' as service_user_work_no,'李超' as service_user_name,'80768307' as work_no,'肖洪峰' as sales_name,'是' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '120954' as customer_no,'81130569' as service_user_work_no,'李锦程' as service_user_name,'81129879' as work_no,'刘伟' as sales_name,'是' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '120955' as customer_no,'81130569' as service_user_work_no,'李锦程' as service_user_name,'81129879' as work_no,'刘伟' as sales_name,'是' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '120956' as customer_no,'81144439' as service_user_work_no,'李超' as service_user_name,'80768307' as work_no,'肖洪峰' as sales_name,'是' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '120970' as customer_no,'81130569' as service_user_work_no,'李锦程' as service_user_name,'81129879' as work_no,'刘伟' as sales_name,'是' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '120984' as customer_no,'81144439' as service_user_work_no,'李超' as service_user_name,'80768307' as work_no,'肖洪峰' as sales_name,'是' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '120986' as customer_no,'81144439' as service_user_work_no,'李超' as service_user_name,'80768307' as work_no,'肖洪峰' as sales_name,'是' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '120987' as customer_no,'81144439' as service_user_work_no,'李超' as service_user_name,'80768307' as work_no,'肖洪峰' as sales_name,'是' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '120990' as customer_no,'81144439' as service_user_work_no,'李超' as service_user_name,'80768307' as work_no,'肖洪峰' as sales_name,'是' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '120995' as customer_no,'81130569' as service_user_work_no,'李锦程' as service_user_name,'81129879' as work_no,'刘伟' as sales_name,'是' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '121022' as customer_no,'81144439' as service_user_work_no,'李超' as service_user_name,'80768307' as work_no,'肖洪峰' as sales_name,'是' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '121056' as customer_no,'81144439' as service_user_work_no,'李超' as service_user_name,'80768307' as work_no,'肖洪峰' as sales_name,'是' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '121176' as customer_no,'81130569' as service_user_work_no,'李锦程' as service_user_name,'81129879' as work_no,'刘伟' as sales_name,'是' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '124524' as customer_no,'81103326;81138104' as service_user_work_no,'彭倩倩;郭凯利' as service_user_name,'81103065' as work_no,'王世超' as sales_name,'否' as is_part_time_service_manager,0.45 as sales_sale_rate,0.4 as sales_front_profit_rate,0.05 as service_user_sale_rate,0.1 as service_user_front_profit_rate
union all   select '124033' as customer_no,'81020509' as service_user_work_no,'陈素彩' as service_user_name,'81103065' as work_no,'王世超' as sales_name,'否' as is_part_time_service_manager,0.9 as sales_sale_rate,0.8 as sales_front_profit_rate,0.1 as service_user_sale_rate,0.2 as service_user_front_profit_rate
union all   select '118816' as customer_no,'' as service_user_work_no,'' as service_user_name,'81001235' as work_no,'田志伟' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '119825' as customer_no,'' as service_user_work_no,'' as service_user_name,'81001235' as work_no,'田志伟' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '118815' as customer_no,'' as service_user_work_no,'' as service_user_name,'81112985' as work_no,'魏桃' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '117249' as customer_no,'' as service_user_work_no,'' as service_user_name,'81001235' as work_no,'田志伟' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '118126' as customer_no,'' as service_user_work_no,'' as service_user_name,'81001235' as work_no,'田志伟' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '113926' as customer_no,'' as service_user_work_no,'' as service_user_name,'81001235' as work_no,'田志伟' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '111000' as customer_no,'' as service_user_work_no,'' as service_user_name,'81014892' as work_no,'张峰源' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '119535' as customer_no,'' as service_user_work_no,'' as service_user_name,'81014892' as work_no,'张峰源' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '107514' as customer_no,'' as service_user_work_no,'' as service_user_name,'80887786' as work_no,'陈维强' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '107956' as customer_no,'' as service_user_work_no,'' as service_user_name,'81120197' as work_no,'蒋春' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '108618' as customer_no,'' as service_user_work_no,'' as service_user_name,'80928418' as work_no,'宋伟静' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '108910' as customer_no,'' as service_user_work_no,'' as service_user_name,'80928418' as work_no,'宋伟静' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '109092' as customer_no,'' as service_user_work_no,'' as service_user_name,'80928418' as work_no,'宋伟静' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '110026' as customer_no,'' as service_user_work_no,'' as service_user_name,'80955105' as work_no,'徐兵' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '113477' as customer_no,'' as service_user_work_no,'' as service_user_name,'80887786' as work_no,'陈维强' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '115458' as customer_no,'' as service_user_work_no,'' as service_user_name,'80980883' as work_no,'罗春兰' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '115833' as customer_no,'' as service_user_work_no,'' as service_user_name,'81074192' as work_no,'乔雪' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '115889' as customer_no,'' as service_user_work_no,'' as service_user_name,'81024326' as work_no,'江天' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '117843' as customer_no,'' as service_user_work_no,'' as service_user_name,'81024326' as work_no,'江天' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '118020' as customer_no,'' as service_user_work_no,'' as service_user_name,'80928418' as work_no,'宋伟静' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '118553' as customer_no,'' as service_user_work_no,'' as service_user_name,'81027770' as work_no,'计蓉霞' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '121389' as customer_no,'' as service_user_work_no,'' as service_user_name,'80948458' as work_no,'杨旻杰' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate
union all   select '123917' as customer_no,'' as service_user_work_no,'' as service_user_name,'81024326' as work_no,'江天' as sales_name,'否' as is_part_time_service_manager,1 as sales_sale_rate,1 as sales_front_profit_rate,0 as service_user_sale_rate,0 as service_user_front_profit_rate


	
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
		--and customer_no not in('111118','102755','104023','105673','104402')
		--and customer_no not in('107338','104123','102629','104526','106375','106380','106335','107268','104296','108391','108390','108072','108503')
		--签呈客户仅4月不考核，不算提成，4-6月不算逾期
		--and customer_no not in('PF0320','105177')
		--5月签呈客户5月剔除逾期，前者剔除逾期，后者仅5月剔除逾期
		--and customer_no not in('103883','103167','105673','104352','104662','104514','104746','104172')
		--and customer_no not in('104055','106463')
		--6月签呈客户仅6月剔除逾期，其中 106463 从8月开始不剔除，106765 从12月开始不剔除，105240一直剔除
		--and customer_no not in('105157','107570','106905','104484','109382','106463','106765')
		--and customer_no not in('106463','106765')
		--and customer_no not in('106765')
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
		--and customer_no not in('111333')
		--2月签呈 当月剔除逾期系数，当月剔除、每月剔除、不算提成
		--and customer_no not in('116529','111506','111623','112326','109484','105302','115206')
		--and customer_no not in('104532')
		--and customer_no not in('116015','115721','115721','116877','116883','116015','116556','116826','103253','103284','103296',
		--                       '103297','103304','103306','103311','104818','104828','104829','104835','105113','106283','106284',
		--                       '106298','106299','106301','106306','106307','106308','106309','106320','106321','106325','106326',
		--                       '106330','102844','114054','109000','114083','114085','115909','115971','116215',
		--					   '111100','116058','116188','105601')
		--3月签呈 当月剔除逾期系数;3-4月剔除逾期系数;每月剔除逾期系数;剔除逾期系数不算提成(其中'PF0065','112574','106782'3-5月不发提成);剔除逾期系数.不算提成.每月*2
		--and customer_no not in('111506','108800','112180')
		--and customer_no not in('112129')
		--and customer_no not in('114904','115313','115314','115325','115326','115391')
		--and customer_no not in('PF0065','112574','106782')  --'116957','116629','116215'仅3月
		--and customer_no not in('115721','116877','116883','116015','116556','116826')
		--and customer_no not in('103253','103284','103296','103297','103304','103306','103311','104818','104828','104829','104835',
		--					'105113','104609')
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
		--and customer_no not in ('112410','114853','117406','116315','114595','114466','111365','108589','108444','108393','105627','104102','103120','103038')
		--202107月签呈，不算提成，每月处理
		and customer_no not in('115971')
		--202108月签呈，剔除逾期，每月处理
		and customer_no not in ('106349')
		--202108月签呈，剔除逾期，当月处理
		--and customer_no not in ('107305','118794','107867','107050','106469','108956','113913','118738','120318','110696','111120','103369','119480','107761','118128','110926',
		--'119619','119561','119019','119018','119017','119012','119011','119004','118996','118993','118992','118894','118366','104086')
		--202108月签呈，剔除逾期，8-9月处理，其中，'116736','118277' 8-10月处理
		--and customer_no not in ('105947','105975','106000','119198','118901','113583','113576','113569','113443','111204','110575','116736','118277')
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
		and customer_no not in ('PF0065')
		--202110月签呈，剔除逾期，每月处理
		and customer_no not in ('110696','111120','106349','103369','118342')
		--202110月签呈，剔除逾期，当月处理
		--and customer_no not in ('122230','120318','116505','116522','107761','119480','107867','118215','104666','107577','113184','120120','113609','119888','122045','121707')
		--202110月签呈，战略客户，不考核提成，每月处理
		--202111月签呈，该战略客户恢复提成，每月处理
		--and customer_no not in('119925','122495')
		--202111月签呈，不算提成，不计逾期，每月处理
		and customer_no not in ('101653','119977')
		--202111月签呈，不算提成，当月处理
		--and customer_no not in ('123104','123127','123128','123131','123135','123136')	
		--202111月签呈，剔除逾期，当月处理
		--and customer_no not in ('106526','106526','116148','119964','120239','120715','116922','110926','122192','116522','119320','122281','112586','119534','107576',
		--'112434','120770','108853','115206','113425','115110','118996','118992','118993','119017','119012','119011','118894','119018','119004','119019','120900')
		--202111月签呈，剔除逾期，每月处理
		and customer_no not in ('120317')
		--202112月签呈，不算提成，当月处理
		--and customer_no not in ('124079')
		--202112月签呈，剔除逾期，当月处理
		--and customer_no not in ('122631','107099','111327','115231','122234','121190','112016','113666','117927','104192')
		--202201月签呈，不算提成，每月
		and customer_no not in ('104192','123395','117927','115393','103369','104817','105280','105242','105242','105696','106585','112586','109788','112129','116522','116505',
		'123730','118243','104086','116313','119977','102754','106721','105182','105181','107404','118103','108127','115204','119131','106526','114794','122517','115643',
		'115252','117929','117884','119100','120121')			
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
		'120939','124473','121298','125284','124601','124498','122567','123244','121625','117817')
		--202111月签呈，由于没有仓储配送，客户从城市服务商仓库过机，正常计算提成，每月处理
		and customer_no not in('121444','121229','121443')
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
		--and customer_no not in('111118','102755','104023','105673','104402')
		--and customer_no not in('107338','104123','102629','104526','106375','106380','106335','107268','104296','108391','108390','108072','108503')
		--签呈客户仅4月不考核，不算提成，4-6月不算逾期
		--and customer_no not in('PF0320','105177')
		--5月签呈客户5月剔除逾期，前者剔除逾期，后者仅5月剔除逾期
		--and customer_no not in('103883','103167','105673','104352','104662','104514','104746','104172')
		--and customer_no not in('104055','106463')
		--6月签呈客户仅6月剔除逾期，其中 106463 从8月开始不剔除，106765 从12月开始不剔除，105240一直剔除
		--and customer_no not in('105157','107570','106905','104484','109382','106463','106765')
		--and customer_no not in('106463','106765')
		--and customer_no not in('106765')
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
		--and customer_no not in('111333')
		--2月签呈 当月剔除逾期系数，当月剔除、每月剔除、不算提成
		--and customer_no not in('116529','111506','111623','112326','109484','105302','115206')
		--and customer_no not in('104532')
		--and customer_no not in('116015','115721','115721','116877','116883','116015','116556','116826','103253','103284','103296',
		--                       '103297','103304','103306','103311','104818','104828','104829','104835','105113','106283','106284',
		--                       '106298','106299','106301','106306','106307','106308','106309','106320','106321','106325','106326',
		--                       '106330','102844','114054','109000','114083','114085','115909','115971','116215',
		--					   '111100','116058','116188','105601')
		--3月签呈 当月剔除逾期系数;3-4月剔除逾期系数;每月剔除逾期系数;剔除逾期系数不算提成(其中'PF0065','112574','106782'3-5月不发提成);剔除逾期系数.不算提成.每月*2
		--and customer_no not in('111506','108800','112180')
		--and customer_no not in('112129')
		--and customer_no not in('114904','115313','115314','115325','115326','115391')
		--and customer_no not in('PF0065','112574','106782')  --'116957','116629','116215'仅3月
		--and customer_no not in('115721','116877','116883','116015','116556','116826')
		--and customer_no not in('103253','103284','103296','103297','103304','103306','103311','104818','104828','104829','104835',
		--					'105113','104609')
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
		--and customer_no not in ('112410','114853','117406','116315','114595','114466','111365','108589','108444','108393','105627','104102','103120','103038')
		--202107月签呈，不算提成，每月处理
		and customer_no not in('115971')
		--202108月签呈，剔除逾期，每月处理
		and customer_no not in ('106349')
		--202108月签呈，剔除逾期，当月处理
		--and customer_no not in ('107305','118794','107867','107050','106469','108956','113913','118738','120318','110696','111120','103369','119480','107761','118128','110926',
		--'119619','119561','119019','119018','119017','119012','119011','119004','118996','118993','118992','118894','118366','104086')
		--202108月签呈，剔除逾期，8-9月处理，其中，'116736','118277' 8-10月处理
		--and customer_no not in ('105947','105975','106000','119198','118901','113583','113576','113569','113443','111204','110575','116736','118277')
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
		and customer_no not in ('PF0065')
		--202110月签呈，剔除逾期，每月处理
		and customer_no not in ('110696','111120','106349','103369','118342')
		--202110月签呈，剔除逾期，当月处理
		--and customer_no not in ('122230','120318','116505','116522','107761','119480','107867','118215','104666','107577','113184','120120','113609','119888','122045','121707')
		--202110月签呈，战略客户，不考核提成，每月处理
		--202111月签呈，该战略客户恢复提成，每月处理
		--and customer_no not in('119925','122495')
		--202111月签呈，不算提成，不计逾期，每月处理
		and customer_no not in ('101653','119977')
		--202111月签呈，不算提成，当月处理
		--and customer_no not in ('123104','123127','123128','123131','123135','123136')	
		--202111月签呈，剔除逾期，当月处理
		--and customer_no not in ('106526','106526','116148','119964','120239','120715','116922','110926','122192','116522','119320','122281','112586','119534','107576',
		--'112434','120770','108853','115206','113425','115110','118996','118992','118993','119017','119012','119011','118894','119018','119004','119019','120900')	
		--202111月签呈，剔除逾期，每月处理
		and customer_no not in ('120317')
		--202112月签呈，不算提成，当月处理
		--and customer_no not in ('124079')	
		--202112月签呈，剔除逾期，当月处理
		--and customer_no not in ('122631','107099','111327','115231','122234','121190','112016','113666','117927','104192')
		--202201月签呈，不算提成，每月
		and customer_no not in ('104192','123395','117927','115393','103369','104817','105280','105242','105242','105696','106585','112586','109788','112129','116522','116505',
		'123730','118243','104086','116313','119977','102754','106721','105182','105181','107404','118103','108127','115204','119131','106526','114794','122517','115643',
		'115252','117929','117884','119100','120121')	
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
		--and customer_no not in('111118','102755','104023','105673','104402')
		--and customer_no not in('107338','104123','102629','104526','106375','106380','106335','107268','104296','108391','108390','108072','108503')
		--签呈客户仅4月不考核，不算提成，4-6月不算逾期
		--and customer_no not in('PF0320','105177')
		--5月签呈客户5月剔除逾期，前者剔除逾期，后者仅5月剔除逾期
		--and customer_no not in('103883','103167','105673','104352','104662','104514','104746','104172')
		--and customer_no not in('104055','106463')
		--6月签呈客户仅6月剔除逾期，其中 106463 从8月开始不剔除，106765 从12月开始不剔除，105240一直剔除
		--and customer_no not in('105157','107570','106905','104484','109382','106463','106765')
		--and customer_no not in('106463','106765')
		--and customer_no not in('106765')
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
		--and customer_no not in('111333')
		--2月签呈 当月剔除逾期系数，当月剔除、每月剔除、不算提成
		--and customer_no not in('116529','111506','111623','112326','109484','105302','115206')
		--and customer_no not in('104532')
		--and customer_no not in('116015','115721','115721','116877','116883','116015','116556','116826','103253','103284','103296',
		--                       '103297','103304','103306','103311','104818','104828','104829','104835','105113','106283','106284',
		--                       '106298','106299','106301','106306','106307','106308','106309','106320','106321','106325','106326',
		--                       '106330','102844','114054','109000','114083','114085','115909','115971','116215',
		--					   '111100','116058','116188','105601')	
		--3月签呈 当月剔除逾期系数;3-4月剔除逾期系数;每月剔除逾期系数;剔除逾期系数不算提成(其中'PF0065','112574','106782'3-5月不发提成);剔除逾期系数.不算提成.每月*2
		--and customer_no not in('111506','108800','112180')
		--and customer_no not in('112129')
		--and customer_no not in('114904','115313','115314','115325','115326','115391')
		--and customer_no not in('PF0065','112574','106782')  --'116957','116629','116215'仅3月
		--and customer_no not in('115721','116877','116883','116015','116556','116826')
		--and customer_no not in('103253','103284','103296','103297','103304','103306','103311','104818','104828','104829','104835',
		--					   '105113','104609')
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
		--and customer_no not in ('112410','114853','117406','116315','114595','114466','111365','108589','108444','108393','105627','104102','103120','103038')
		--202107月签呈，不算提成，每月处理
		and customer_no not in('115971')
		--202108月签呈，剔除逾期，每月处理
		and customer_no not in ('106349')
		--202108月签呈，剔除逾期，当月处理
		--and customer_no not in ('107305','118794','107867','107050','106469','108956','113913','118738','120318','110696','111120','103369','119480','107761','118128','110926',
		--'119619','119561','119019','119018','119017','119012','119011','119004','118996','118993','118992','118894','118366','104086')
		--202108月签呈，剔除逾期，8-9月处理，其中，'116736','118277' 8-10月处理
		--and customer_no not in ('105947','105975','106000','119198','118901','113583','113576','113569','113443','111204','110575','116736','118277')
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
		and customer_no not in ('PF0065')
		--202110月签呈，剔除逾期，每月处理
		and customer_no not in ('110696','111120','106349','103369','118342')
		--202110月签呈，剔除逾期，当月处理
		--and customer_no not in ('122230','120318','116505','116522','107761','119480','107867','118215','104666','107577','113184','120120','113609','119888','122045','121707')
		--202110月签呈，战略客户，不考核提成，每月处理
		--202111月签呈，该战略客户恢复提成，每月处理
		--and customer_no not in('119925','122495')
		--202111月签呈，不算提成，不计逾期，每月处理
		and customer_no not in ('101653','119977')
		--202111月签呈，不算提成，当月处理
		--and customer_no not in ('123104','123127','123128','123131','123135','123136')	
		--202111月签呈，剔除逾期，当月处理
		--and customer_no not in ('106526','106526','116148','119964','120239','120715','116922','110926','122192','116522','119320','122281','112586','119534','107576',
		--'112434','120770','108853','115206','113425','115110','118996','118992','118993','119017','119012','119011','118894','119018','119004','119019','120900')
		--202111月签呈，剔除逾期，每月处理
		and customer_no not in ('120317')
		--202112月签呈，不算提成，当月处理
		--and customer_no not in ('124079')	
		--202112月签呈，剔除逾期，当月处理
		--and customer_no not in ('122631','107099','111327','115231','122234','121190','112016','113666','117927','104192')
		--202201月签呈，不算提成，每月
		and customer_no not in ('104192','123395','117927','115393','103369','104817','105280','105242','105242','105696','106585','112586','109788','112129','116522','116505',
		'123730','118243','104086','116313','119977','102754','106721','105182','105181','107404','118103','108127','115204','119131','106526','114794','122517','115643',
		'115252','117929','117884','119100','120121')	
	group by 
		channel_name,customer_no,customer_name,company_code,company_name
	)a	
	----渠道编号-1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 7.企业购 8.其他 9.业务代理
	--剔除业务代理与内购客户
	--202107月签呈，将以下客户的销售员调整为张辉 每月处理
	--202108月签呈，'116957','116629'系统中为业务代理人，但需要人为计算销售员大客户提成，每月处理
	--202108月签呈，更改销售员，每月处理 '114524','115195'
	--left join
	--	(
	--	select 
	--		customer_no,
	--		case when customer_no in ('114524','115195') then '81086756'
	--			else work_no end as work_no,
	--		case when customer_no in ('114524','115195') then '姚艳婷'
	--			else sales_name end as sales_name
	--		--work_no,
	--		--sales_name
	--	from 
	--		csx_dw.dws_crm_w_a_customer 
	--	where 
	--		sdt=${hiveconf:i_sdate_11} 
	--		--where sdt='20210617'	
	--		--4月签呈 '118689'系统中为业务代理人，但需要人为计算销售员大客户提成,每月处理
	--		--202108月签呈，'116957','116629'系统中为业务代理人，但需要人为计算销售员大客户提成，每月处理
	--		and (channel_code in('1','7','8') or customer_no in ('118689','116957','116629')) and (customer_name not like '%内%购%' and customer_name not like '%临保%')
	--	)b on b.customer_no=a.customer_no 
	left join		
		(  
		select 
			distinct customer_no,service_user_work_no,service_user_name,	  
			work_no,sales_name,is_part_time_service_manager
		from 
			csx_tmp.tmp_tc_customer_service_manager_info	
			--from csx_dw.report_crm_w_a_customer_service_manager_info 
			--where sdt=${hiveconf:i_sdate_11}
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
		'120939','124473','121298','125284','124601','124498','122567','123244','121625','117817')
		--202111月签呈，由于没有仓储配送，客户从城市服务商仓库过机，正常计算提成，每月处理
		and customer_no not in('121444','121229','121443')
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
		--and customer_no not in('111118','102755','104023','105673','104402')
		--and customer_no not in('107338','104123','102629','104526','106375','106380','106335','107268','104296','108391','108390','108072','108503')
		--签呈客户仅4月不考核，不算提成，4-6月不算逾期
		--and customer_no not in('PF0320','105177')
		--5月签呈客户5月剔除逾期，前者剔除逾期，后者仅5月剔除逾期
		--and customer_no not in('103883','103167','105673','104352','104662','104514','104746','104172')
		--and customer_no not in('104055','106463')
		--6月签呈客户仅6月剔除逾期，其中 106463 从8月开始不剔除，106765 从12月开始不剔除，105240一直剔除
		--and customer_no not in('105157','107570','106905','104484','109382','106463','106765')
		--and customer_no not in('106463','106765')
		--and customer_no not in('106765')
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
		--and customer_no not in('111333')
		--2月签呈 当月剔除逾期系数，当月剔除、每月剔除、不算提成
		--and customer_no not in('116529','111506','111623','112326','109484','105302','115206')
		--and customer_no not in('104532')
		--and customer_no not in('116015','115721','115721','116877','116883','116015','116556','116826','103253','103284','103296',
		--                       '103297','103304','103306','103311','104818','104828','104829','104835','105113','106283','106284',
		--                       '106298','106299','106301','106306','106307','106308','106309','106320','106321','106325','106326',
		--                       '106330','102844','114054','109000','114083','114085','115909','115971','116215',
		--					   '111100','116058','116188','105601')	
		--3月签呈 当月剔除逾期系数;3-4月剔除逾期系数;每月剔除逾期系数;剔除逾期系数不算提成(其中'PF0065','112574','106782'3-5月不发提成);剔除逾期系数.不算提成.每月*2
		--and customer_no not in('111506','108800','112180')
		--and customer_no not in('112129')
		--and customer_no not in('114904','115313','115314','115325','115326','115391')
		--and customer_no not in('PF0065','112574','106782')  --'116957','116629','116215'仅3月
		--and customer_no not in('115721','116877','116883','116015','116556','116826')
		--and customer_no not in('103253','103284','103296','103297','103304','103306','103311','104818','104828','104829','104835',
		--					   '105113','104609')
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
		--and customer_no not in ('112410','114853','117406','116315','114595','114466','111365','108589','108444','108393','105627','104102','103120','103038')
		--202107月签呈，不算提成，每月处理
		and customer_no not in('115971')
		--202108月签呈，剔除逾期，每月处理
		and customer_no not in ('106349')
		--202108月签呈，剔除逾期，当月处理
		--and customer_no not in ('107305','118794','107867','107050','106469','108956','113913','118738','120318','110696','111120','103369','119480','107761','118128','110926',
		--'119619','119561','119019','119018','119017','119012','119011','119004','118996','118993','118992','118894','118366','104086')
		--202108月签呈，剔除逾期，8-9月处理，其中，'116736','118277' 8-10月处理
		--and customer_no not in ('105947','105975','106000','119198','118901','113583','113576','113569','113443','111204','110575','116736','118277')
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
		and customer_no not in ('PF0065')
		--202110月签呈，剔除逾期，每月处理
		and customer_no not in ('110696','111120','106349','103369','118342')
		--202110月签呈，剔除逾期，当月处理
		--and customer_no not in ('122230','120318','116505','116522','107761','119480','107867','118215','104666','107577','113184','120120','113609','119888','122045','121707')
		--202110月签呈，战略客户，不考核提成，每月处理
		--202111月签呈，该战略客户恢复提成，每月处理
		--and customer_no not in('119925','122495')
		--202111月签呈，不算提成，不计逾期，每月处理
		and customer_no not in ('101653','119977')
		--202111月签呈，不算提成，当月处理
		--and customer_no not in ('123104','123127','123128','123131','123135','123136')	
		--202111月签呈，剔除逾期，当月处理
		--and customer_no not in ('106526','106526','116148','119964','120239','120715','116922','110926','122192','116522','119320','122281','112586','119534','107576',
		--'112434','120770','108853','115206','113425','115110','118996','118992','118993','119017','119012','119011','118894','119018','119004','119019','120900')	
		--202111月签呈，剔除逾期，每月处理
		and customer_no not in ('120317')
		--202112月签呈，不算提成，当月处理
		--and customer_no not in ('124079')	
		--202112月签呈，剔除逾期，当月处理
		--and customer_no not in ('122631','107099','111327','115231','122234','121190','112016','113666','117927','104192')
		--202201月签呈，不算提成，每月
		and customer_no not in ('104192','123395','117927','115393','103369','104817','105280','105242','105242','105696','106585','112586','109788','112129','116522','116505',
		'123730','118243','104086','116313','119977','102754','106721','105182','105181','107404','118103','108127','115204','119131','106526','114794','122517','115643',
		'115252','117929','117884','119100','120121')	
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
		'120939','124473','121298','125284','124601','124498','122567','123244','121625','117817')
		--202111月签呈，由于没有仓储配送，客户从城市服务商仓库过机，正常计算提成，每月处理
		and customer_no not in('121444','121229','121443')
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
		sdt>='20211101'
		and sdt<='20211130'
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
			sdt>='20211101'
			and sdt<='20211130'
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
			sdt='20211130'
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


--安徽省按照大客户计算的客户

/*
select 
	a.customer_no
from 
	(
	select 
		province_name,customer_no,customer_name,business_type_name,
		sum(sales_value)sales_value
	from 
		csx_dw.dws_sale_r_d_detail
	where 
		sdt>='20220101'
		and sdt<='20220131'
		and channel_code in('1','7','9')
		and business_type_code in ('4')
	group by 
		province_name,customer_no,customer_name,business_type_name
	)a 
	join 
		(
		select 
			customer_no,customer_name,work_no,sales_name,sales_province_name
		from 
			csx_dw.dws_crm_w_a_customer 
		where 
			sdt='20220131'
			and sales_province_name='安徽省'
			and work_no not in ('81138989','81138992','81123285','81119588','81086805','81054801','80972242','80884343','81133185','81107924','81087574','80897767',
			'81034712','80886641')
		)c on c.customer_no=a.customer_no
group by 
	a.customer_no
;
*/

