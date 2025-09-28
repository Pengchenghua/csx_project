-- 月初 月末 年初
set month_start_day ='20220101';	
set month_end_day ='20220131';	
set year_start_day ='20220101';		


-- 签呈处理销售员服务管家关系
drop table csx_tmp.tc_customer_service_manager_info_new;
create table csx_tmp.tc_customer_service_manager_info_new
as  
select 
	distinct customer_no,service_user_work_no,service_user_name,work_no,sales_name,is_part_time_service_manager,
    sales_sale_rate as salesperson_sales_value_fp_rate, --销售员_销售额_分配比例
	sales_profit_rate as salesperson_profit_fp_rate,  --销售员_定价毛利额分配比例
    service_user_sale_rate as service_user_sales_value_fp_rate,  --服务管家_销售额_分配比例
	service_user_profit_rate as service_user_profit_fp_rate --服务管家_定价毛利额_分配比例
from 
	csx_dw.report_crm_w_a_customer_service_manager_info_new
where 
	sdt=${hiveconf:month_end_day}
	and customer_no not in ('X000000','105502','105761','100326','PF1265','105182','106721','102565','103855','105150','111844','102734','107404','102784','102901','112288',
	'113467','103199','104469','104501','115982','115987','PF0365','105355','105886','100563','101482','102229','102508','102633','102686','103062','103141','103372','103714',
	'104007','104054','104165','104612','104954','104970','105156','105177','105181','105225','105441','105721','105806','105838','105882','106423','106481','106881','107371',
	'108127','108739','109377','111241','112062','112327','113101','113635','113646','114344','115324','115646','115679','115857','116211','118687','PF0094','PF1205','116957',
	'120024','120317','107901','115753','118569','119688','117145','119021','119454','121444','121229','121443','104281','115656','105593','107398','109447','111207','115537',
	'115602','115826','117244','123065','125534','116015','115721','119703','118887','116556','116733','116702','116923','116863','116883','116877','116826','123534','116967',
	'117026','117025','117009','117045','117040','117035','116944','117030','117027','116903','116994','117019','116993','116989','123716','120100','119806','119803','119815',
	'120463','122125','122106','121763','122375','122286','122371','123060','123383','123644','123813','124179','125244','125508','103183','104086','104397','100984','105499',
	'114843','108589','102580','102890','PF0099','116099','106301','106306','120246','120689','117047','118836','115899','116398','122143','117108','123827','117121','117222',
	'123442','120735','118183','125137','123923','120836','123034','123859','123650','124555','124641','104318','104085','120781','118744','106433','115051','108283','119897',
	'109722','114516','120365','119210','119168','119519','122555','121507','122269','123222','123262','123257','123242','123247','123253','124602','115205','110807','115252',
	'115643','117929','120459','121206','124098','102755','103175','103868','103874','103887','103898','103908','103926','103927','104460','109000','105673','106563','114522',
	'115431','118504','120554','113281','115935','118072','118602','118748','119990','125677','110872','118825','105164','105165','119757','111628','111612','114289','117396',
	'108162','115829','108180','108152','116445','120458','123032','124481','125355','111331','110863','114667','108267','111298','115476','118914','111318','111336','112629',
	'123623','103096'
	)
union all   select '105502' as customer_no,'' as service_user_work_no,'' as service_user_name,'80890405' as work_no,'瞿林峰' as sales_name,'是' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '105761' as customer_no,'' as service_user_work_no,'' as service_user_name,'81084752' as work_no,'张鹏' as sales_name,'是' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '100326' as customer_no,'80009493' as service_user_work_no,'郑银燕' as service_user_name,'80007454' as work_no,'李翔' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select 'PF1265' as customer_no,'80972915' as service_user_work_no,'陈伟豪' as service_user_name,'80012225' as work_no,'林挺波' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '105182' as customer_no,'81133021' as service_user_work_no,'黄升' as service_user_name,'81133021' as work_no,'黄升' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '106721' as customer_no,'81133021' as service_user_work_no,'黄升' as service_user_name,'81133021' as work_no,'黄升' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '102565' as customer_no,'81129243' as service_user_work_no,'汪敏禄' as service_user_name,'80960666' as work_no,'冯桂华' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '103855' as customer_no,'81099343' as service_user_work_no,'林瑾鑫' as service_user_name,'81099343' as work_no,'林瑾鑫' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '105150' as customer_no,'81133021' as service_user_work_no,'黄升' as service_user_name,'81133021' as work_no,'黄升' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '111844' as customer_no,'81129243' as service_user_work_no,'汪敏禄' as service_user_name,'80960666' as work_no,'冯桂华' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '102734' as customer_no,'81099343' as service_user_work_no,'林瑾鑫' as service_user_name,'80980614' as work_no,'池春梅' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '107404' as customer_no,'80952742' as service_user_work_no,'王秀云' as service_user_name,'80952742' as work_no,'王秀云' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '102784' as customer_no,'81099343' as service_user_work_no,'林瑾鑫' as service_user_name,'80980614' as work_no,'池春梅' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '102901' as customer_no,'81131450' as service_user_work_no,'张珠妹' as service_user_name,'80980614' as work_no,'池春梅' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '112288' as customer_no,'81105401' as service_user_work_no,'卢烊' as service_user_name,'80980614' as work_no,'池春梅' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '113467' as customer_no,'81105401' as service_user_work_no,'卢烊' as service_user_name,'80980614' as work_no,'池春梅' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '103199' as customer_no,'81105401' as service_user_work_no,'卢烊' as service_user_name,'80980614' as work_no,'池春梅' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '104469' as customer_no,'81105401' as service_user_work_no,'卢烊' as service_user_name,'80980614' as work_no,'池春梅' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '104501' as customer_no,'' as service_user_work_no,'' as service_user_name,'80980614' as work_no,'池春梅' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '115982' as customer_no,'81105401' as service_user_work_no,'卢烊' as service_user_name,'80980614' as work_no,'池春梅' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '115987' as customer_no,'81105401' as service_user_work_no,'卢烊' as service_user_name,'80980614' as work_no,'池春梅' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select 'PF0365' as customer_no,'81105401' as service_user_work_no,'卢烊' as service_user_name,'80980614' as work_no,'池春梅' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '105355' as customer_no,'80972915' as service_user_work_no,'陈伟豪' as service_user_name,'80972915' as work_no,'陈伟豪' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '105886' as customer_no,'80691224' as service_user_work_no,'王少端' as service_user_name,'80691224' as work_no,'王少端' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '100563' as customer_no,'81099343' as service_user_work_no,'林瑾鑫' as service_user_name,'81099343' as work_no,'林瑾鑫' as sales_name,'是' as is_part_time_service_manager,0 as salesperson_sales_value_fp_rate,0 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '101482' as customer_no,'80952742' as service_user_work_no,'王秀云' as service_user_name,'80952742' as work_no,'王秀云' as sales_name,'是' as is_part_time_service_manager,0 as salesperson_sales_value_fp_rate,0 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '102229' as customer_no,'80691224' as service_user_work_no,'王少端' as service_user_name,'80691224' as work_no,'王少端' as sales_name,'是' as is_part_time_service_manager,0 as salesperson_sales_value_fp_rate,0 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '102508' as customer_no,'80691224' as service_user_work_no,'王少端' as service_user_name,'80691224' as work_no,'王少端' as sales_name,'是' as is_part_time_service_manager,0 as salesperson_sales_value_fp_rate,0 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '102633' as customer_no,'80691224' as service_user_work_no,'王少端' as service_user_name,'80691224' as work_no,'王少端' as sales_name,'是' as is_part_time_service_manager,0 as salesperson_sales_value_fp_rate,0 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '102686' as customer_no,'81131450' as service_user_work_no,'张珠妹' as service_user_name,'81131450' as work_no,'张珠妹' as sales_name,'是' as is_part_time_service_manager,0 as salesperson_sales_value_fp_rate,0 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '103062' as customer_no,'80952742' as service_user_work_no,'王秀云' as service_user_name,'80952742' as work_no,'王秀云' as sales_name,'是' as is_part_time_service_manager,0 as salesperson_sales_value_fp_rate,0 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '103141' as customer_no,'80816155' as service_user_work_no,'张磊磊' as service_user_name,'80816155' as work_no,'张磊磊' as sales_name,'是' as is_part_time_service_manager,0 as salesperson_sales_value_fp_rate,0 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '103372' as customer_no,'81129243' as service_user_work_no,'汪敏禄' as service_user_name,'81089088' as work_no,'陈先贵' as sales_name,'是' as is_part_time_service_manager,0 as salesperson_sales_value_fp_rate,0 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '103714' as customer_no,'80816155' as service_user_work_no,'张磊磊' as service_user_name,'80816155' as work_no,'张磊磊' as sales_name,'是' as is_part_time_service_manager,0 as salesperson_sales_value_fp_rate,0 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '104007' as customer_no,'80691224' as service_user_work_no,'王少端' as service_user_name,'80691224' as work_no,'王少端' as sales_name,'是' as is_part_time_service_manager,0 as salesperson_sales_value_fp_rate,0 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '104054' as customer_no,'81129243' as service_user_work_no,'汪敏禄' as service_user_name,'81129243' as work_no,'汪敏禄' as sales_name,'是' as is_part_time_service_manager,0 as salesperson_sales_value_fp_rate,0 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '104165' as customer_no,'80691224' as service_user_work_no,'王少端' as service_user_name,'80691224' as work_no,'王少端' as sales_name,'是' as is_part_time_service_manager,0 as salesperson_sales_value_fp_rate,0 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '104612' as customer_no,'81099343' as service_user_work_no,'林瑾鑫' as service_user_name,'81099343' as work_no,'林瑾鑫' as sales_name,'是' as is_part_time_service_manager,0 as salesperson_sales_value_fp_rate,0 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '104954' as customer_no,'80691224' as service_user_work_no,'王少端' as service_user_name,'80691224' as work_no,'王少端' as sales_name,'是' as is_part_time_service_manager,0 as salesperson_sales_value_fp_rate,0 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '104970' as customer_no,'80691224' as service_user_work_no,'王少端' as service_user_name,'80691224' as work_no,'王少端' as sales_name,'是' as is_part_time_service_manager,0 as salesperson_sales_value_fp_rate,0 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '105156' as customer_no,'80952742' as service_user_work_no,'王秀云' as service_user_name,'80952742' as work_no,'王秀云' as sales_name,'是' as is_part_time_service_manager,0 as salesperson_sales_value_fp_rate,0 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '105177' as customer_no,'80952742' as service_user_work_no,'王秀云' as service_user_name,'80952742' as work_no,'王秀云' as sales_name,'是' as is_part_time_service_manager,0 as salesperson_sales_value_fp_rate,0 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '105181' as customer_no,'81131450' as service_user_work_no,'张珠妹' as service_user_name,'81131450' as work_no,'张珠妹' as sales_name,'是' as is_part_time_service_manager,0 as salesperson_sales_value_fp_rate,0 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '105225' as customer_no,'80691224' as service_user_work_no,'王少端' as service_user_name,'80691224' as work_no,'王少端' as sales_name,'是' as is_part_time_service_manager,0 as salesperson_sales_value_fp_rate,0 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '105441' as customer_no,'80691224' as service_user_work_no,'王少端' as service_user_name,'80691224' as work_no,'王少端' as sales_name,'是' as is_part_time_service_manager,0 as salesperson_sales_value_fp_rate,0 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '105721' as customer_no,'80952742' as service_user_work_no,'王秀云' as service_user_name,'80952742' as work_no,'王秀云' as sales_name,'是' as is_part_time_service_manager,0 as salesperson_sales_value_fp_rate,0 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '105806' as customer_no,'80816155' as service_user_work_no,'张磊磊' as service_user_name,'80816155' as work_no,'张磊磊' as sales_name,'是' as is_part_time_service_manager,0 as salesperson_sales_value_fp_rate,0 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '105838' as customer_no,'80816155' as service_user_work_no,'张磊磊' as service_user_name,'80816155' as work_no,'张磊磊' as sales_name,'是' as is_part_time_service_manager,0 as salesperson_sales_value_fp_rate,0 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '105882' as customer_no,'81099343' as service_user_work_no,'林瑾鑫' as service_user_name,'81099343' as work_no,'林瑾鑫' as sales_name,'是' as is_part_time_service_manager,0 as salesperson_sales_value_fp_rate,0 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '106423' as customer_no,'81131450' as service_user_work_no,'张珠妹' as service_user_name,'81131450' as work_no,'张珠妹' as sales_name,'是' as is_part_time_service_manager,0 as salesperson_sales_value_fp_rate,0 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '106481' as customer_no,'81131450' as service_user_work_no,'张珠妹' as service_user_name,'81131450' as work_no,'张珠妹' as sales_name,'是' as is_part_time_service_manager,0 as salesperson_sales_value_fp_rate,0 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '106881' as customer_no,'81131450' as service_user_work_no,'张珠妹' as service_user_name,'81131450' as work_no,'张珠妹' as sales_name,'是' as is_part_time_service_manager,0 as salesperson_sales_value_fp_rate,0 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '107371' as customer_no,'81129243' as service_user_work_no,'汪敏禄' as service_user_name,'81129243' as work_no,'汪敏禄' as sales_name,'是' as is_part_time_service_manager,0 as salesperson_sales_value_fp_rate,0 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '108127' as customer_no,'81133021' as service_user_work_no,'黄升' as service_user_name,'81133021' as work_no,'黄升' as sales_name,'是' as is_part_time_service_manager,0 as salesperson_sales_value_fp_rate,0 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '108739' as customer_no,'81099343' as service_user_work_no,'林瑾鑫' as service_user_name,'81099343' as work_no,'林瑾鑫' as sales_name,'是' as is_part_time_service_manager,0 as salesperson_sales_value_fp_rate,0 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '109377' as customer_no,'81099343' as service_user_work_no,'林瑾鑫' as service_user_name,'81099343' as work_no,'林瑾鑫' as sales_name,'是' as is_part_time_service_manager,0 as salesperson_sales_value_fp_rate,0 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '111241' as customer_no,'80952742' as service_user_work_no,'王秀云' as service_user_name,'80952742' as work_no,'王秀云' as sales_name,'是' as is_part_time_service_manager,0 as salesperson_sales_value_fp_rate,0 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '112062' as customer_no,'80816155' as service_user_work_no,'张磊磊' as service_user_name,'80816155' as work_no,'张磊磊' as sales_name,'是' as is_part_time_service_manager,0 as salesperson_sales_value_fp_rate,0 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '112327' as customer_no,'80691224' as service_user_work_no,'王少端' as service_user_name,'80691224' as work_no,'王少端' as sales_name,'是' as is_part_time_service_manager,0 as salesperson_sales_value_fp_rate,0 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '113101' as customer_no,'80691224' as service_user_work_no,'王少端' as service_user_name,'80691224' as work_no,'王少端' as sales_name,'是' as is_part_time_service_manager,0 as salesperson_sales_value_fp_rate,0 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '113635' as customer_no,'80691224' as service_user_work_no,'王少端' as service_user_name,'80691224' as work_no,'王少端' as sales_name,'是' as is_part_time_service_manager,0 as salesperson_sales_value_fp_rate,0 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '113646' as customer_no,'80691224' as service_user_work_no,'王少端' as service_user_name,'80691224' as work_no,'王少端' as sales_name,'是' as is_part_time_service_manager,0 as salesperson_sales_value_fp_rate,0 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '114344' as customer_no,'' as service_user_work_no,'' as service_user_name,'80005782' as work_no,'杨海燕' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '115324' as customer_no,'80816155' as service_user_work_no,'张磊磊' as service_user_name,'80816155' as work_no,'张磊磊' as sales_name,'是' as is_part_time_service_manager,0 as salesperson_sales_value_fp_rate,0 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '115646' as customer_no,'80816155' as service_user_work_no,'张磊磊' as service_user_name,'80816155' as work_no,'张磊磊' as sales_name,'是' as is_part_time_service_manager,0 as salesperson_sales_value_fp_rate,0 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '115679' as customer_no,'80816155' as service_user_work_no,'张磊磊' as service_user_name,'80816155' as work_no,'张磊磊' as sales_name,'是' as is_part_time_service_manager,0 as salesperson_sales_value_fp_rate,0 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '115857' as customer_no,'80816155' as service_user_work_no,'张磊磊' as service_user_name,'80816155' as work_no,'张磊磊' as sales_name,'是' as is_part_time_service_manager,0 as salesperson_sales_value_fp_rate,0 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '116211' as customer_no,'80816155' as service_user_work_no,'张磊磊' as service_user_name,'80816155' as work_no,'张磊磊' as sales_name,'是' as is_part_time_service_manager,0 as salesperson_sales_value_fp_rate,0 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '118687' as customer_no,'81099343' as service_user_work_no,'林瑾鑫' as service_user_name,'81099343' as work_no,'林瑾鑫' as sales_name,'是' as is_part_time_service_manager,0 as salesperson_sales_value_fp_rate,0 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select 'PF0094' as customer_no,'81099343' as service_user_work_no,'林瑾鑫' as service_user_name,'81099343' as work_no,'林瑾鑫' as sales_name,'是' as is_part_time_service_manager,0 as salesperson_sales_value_fp_rate,0 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select 'PF1205' as customer_no,'80816155' as service_user_work_no,'张磊磊' as service_user_name,'80816155' as work_no,'张磊磊' as sales_name,'是' as is_part_time_service_manager,0 as salesperson_sales_value_fp_rate,0 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '116957' as customer_no,'' as service_user_work_no,'' as service_user_name,'81055718' as work_no,'吴庆平' as sales_name,'否' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '120024' as customer_no,'' as service_user_work_no,'' as service_user_name,'80952743' as work_no,'兰华明' as sales_name,'否' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '120317' as customer_no,'' as service_user_work_no,'' as service_user_name,'81084686' as work_no,'何丽姿' as sales_name,'否' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '107901' as customer_no,'' as service_user_work_no,'' as service_user_name,'80958648' as work_no,'王萃芸' as sales_name,'否' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '115753' as customer_no,'' as service_user_work_no,'' as service_user_name,'80958648' as work_no,'王萃芸' as sales_name,'否' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '118569' as customer_no,'' as service_user_work_no,'' as service_user_name,'80958648' as work_no,'王萃芸' as sales_name,'否' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '119688' as customer_no,'80912927' as service_user_work_no,'刘秋霞' as service_user_name,'81129344' as work_no,'洪少灵' as sales_name,'否' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '117145' as customer_no,'' as service_user_work_no,'' as service_user_name,'81129344' as work_no,'洪少灵' as sales_name,'否' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '119021' as customer_no,'' as service_user_work_no,'' as service_user_name,'81129344' as work_no,'洪少灵' as sales_name,'否' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '119454' as customer_no,'' as service_user_work_no,'' as service_user_name,'81129006' as work_no,'林君' as sales_name,'否' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '121444' as customer_no,'' as service_user_work_no,'' as service_user_name,'81079631' as work_no,'李燕玲' as sales_name,'否' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '121229' as customer_no,'' as service_user_work_no,'' as service_user_name,'81079631' as work_no,'李燕玲' as sales_name,'否' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '121443' as customer_no,'' as service_user_work_no,'' as service_user_name,'81079631' as work_no,'李燕玲' as sales_name,'否' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '104281' as customer_no,'' as service_user_work_no,'' as service_user_name,'80974184' as work_no,'郭荔丽' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '115656' as customer_no,'' as service_user_work_no,'' as service_user_name,'80974184' as work_no,'郭荔丽' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '105593' as customer_no,'' as service_user_work_no,'' as service_user_name,'1000002' as work_no,'莆田B' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '107398' as customer_no,'' as service_user_work_no,'' as service_user_name,'80974184' as work_no,'郭荔丽' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '109447' as customer_no,'' as service_user_work_no,'' as service_user_name,'1000002' as work_no,'莆田B' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '111207' as customer_no,'' as service_user_work_no,'' as service_user_name,'80974184' as work_no,'郭荔丽' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '115537' as customer_no,'' as service_user_work_no,'' as service_user_name,'80974184' as work_no,'郭荔丽' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '115602' as customer_no,'' as service_user_work_no,'' as service_user_name,'80974184' as work_no,'郭荔丽' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '115826' as customer_no,'' as service_user_work_no,'' as service_user_name,'80974184' as work_no,'郭荔丽' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '117244' as customer_no,'' as service_user_work_no,'' as service_user_name,'80974184' as work_no,'郭荔丽' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '123065' as customer_no,'' as service_user_work_no,'' as service_user_name,'1000002' as work_no,'莆田B' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '125534' as customer_no,'' as service_user_work_no,'' as service_user_name,'80974184' as work_no,'郭荔丽' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '116015' as customer_no,'' as service_user_work_no,'' as service_user_name,'81139788' as work_no,'邓肯' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '115721' as customer_no,'' as service_user_work_no,'' as service_user_name,'81122750' as work_no,'吴煌锦' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '119703' as customer_no,'' as service_user_work_no,'' as service_user_name,'81122750' as work_no,'吴煌锦' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '118887' as customer_no,'' as service_user_work_no,'' as service_user_name,'81122750' as work_no,'吴煌锦' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '116556' as customer_no,'' as service_user_work_no,'' as service_user_name,'81139788' as work_no,'邓肯' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '116733' as customer_no,'' as service_user_work_no,'' as service_user_name,'81055717' as work_no,'魏隆强' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '116702' as customer_no,'' as service_user_work_no,'' as service_user_name,'81122750' as work_no,'吴煌锦' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '116923' as customer_no,'' as service_user_work_no,'' as service_user_name,'81122750' as work_no,'吴煌锦' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '116863' as customer_no,'' as service_user_work_no,'' as service_user_name,'81122750' as work_no,'吴煌锦' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '116883' as customer_no,'' as service_user_work_no,'' as service_user_name,'81122750' as work_no,'吴煌锦' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '116877' as customer_no,'' as service_user_work_no,'' as service_user_name,'81122750' as work_no,'吴煌锦' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '116826' as customer_no,'' as service_user_work_no,'' as service_user_name,'81139788' as work_no,'邓肯' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '123534' as customer_no,'' as service_user_work_no,'' as service_user_name,'81055717' as work_no,'魏隆强' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '116967' as customer_no,'' as service_user_work_no,'' as service_user_name,'81122750' as work_no,'吴煌锦' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '117026' as customer_no,'' as service_user_work_no,'' as service_user_name,'81122750' as work_no,'吴煌锦' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '117025' as customer_no,'' as service_user_work_no,'' as service_user_name,'81122750' as work_no,'吴煌锦' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '117009' as customer_no,'' as service_user_work_no,'' as service_user_name,'81122750' as work_no,'吴煌锦' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '117045' as customer_no,'' as service_user_work_no,'' as service_user_name,'81122750' as work_no,'吴煌锦' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '117040' as customer_no,'' as service_user_work_no,'' as service_user_name,'81122750' as work_no,'吴煌锦' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '117035' as customer_no,'' as service_user_work_no,'' as service_user_name,'81122750' as work_no,'吴煌锦' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '116944' as customer_no,'' as service_user_work_no,'' as service_user_name,'81122750' as work_no,'吴煌锦' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '117030' as customer_no,'' as service_user_work_no,'' as service_user_name,'81122750' as work_no,'吴煌锦' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '117027' as customer_no,'' as service_user_work_no,'' as service_user_name,'81122750' as work_no,'吴煌锦' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '116903' as customer_no,'' as service_user_work_no,'' as service_user_name,'81122750' as work_no,'吴煌锦' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '116994' as customer_no,'' as service_user_work_no,'' as service_user_name,'81122750' as work_no,'吴煌锦' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '117019' as customer_no,'' as service_user_work_no,'' as service_user_name,'81122750' as work_no,'吴煌锦' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '116993' as customer_no,'' as service_user_work_no,'' as service_user_name,'81122750' as work_no,'吴煌锦' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '116989' as customer_no,'' as service_user_work_no,'' as service_user_name,'81122750' as work_no,'吴煌锦' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '123716' as customer_no,'' as service_user_work_no,'' as service_user_name,'81122750' as work_no,'吴煌锦' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '120100' as customer_no,'' as service_user_work_no,'' as service_user_name,'81122750' as work_no,'吴煌锦' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '119806' as customer_no,'' as service_user_work_no,'' as service_user_name,'81122750' as work_no,'吴煌锦' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '119803' as customer_no,'' as service_user_work_no,'' as service_user_name,'81122750' as work_no,'吴煌锦' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '119815' as customer_no,'' as service_user_work_no,'' as service_user_name,'81122750' as work_no,'吴煌锦' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '120463' as customer_no,'' as service_user_work_no,'' as service_user_name,'90000002' as work_no,'三明B' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '122125' as customer_no,'' as service_user_work_no,'' as service_user_name,'81122750' as work_no,'吴煌锦' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '122106' as customer_no,'' as service_user_work_no,'' as service_user_name,'81122750' as work_no,'吴煌锦' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '121763' as customer_no,'' as service_user_work_no,'' as service_user_name,'81122750' as work_no,'吴煌锦' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '122375' as customer_no,'' as service_user_work_no,'' as service_user_name,'81122750' as work_no,'吴煌锦' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '122286' as customer_no,'' as service_user_work_no,'' as service_user_name,'81122750' as work_no,'吴煌锦' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '122371' as customer_no,'' as service_user_work_no,'' as service_user_name,'81122750' as work_no,'吴煌锦' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '123060' as customer_no,'' as service_user_work_no,'' as service_user_name,'90000002' as work_no,'三明B' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '123383' as customer_no,'' as service_user_work_no,'' as service_user_name,'LL000001' as work_no,'彭先檩（三明）' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '123644' as customer_no,'' as service_user_work_no,'' as service_user_name,'81122750' as work_no,'吴煌锦' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '123813' as customer_no,'' as service_user_work_no,'' as service_user_name,'81122750' as work_no,'吴煌锦' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '124179' as customer_no,'' as service_user_work_no,'' as service_user_name,'81102377' as work_no,'刘寒漪' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '125244' as customer_no,'' as service_user_work_no,'' as service_user_name,'81139788' as work_no,'邓肯' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '125508' as customer_no,'' as service_user_work_no,'' as service_user_name,'81122750' as work_no,'吴煌锦' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '103183' as customer_no,'' as service_user_work_no,'' as service_user_name,'80007454' as work_no,'李翔' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '104086' as customer_no,'' as service_user_work_no,'' as service_user_name,'81026931' as work_no,'林圳' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '104397' as customer_no,'' as service_user_work_no,'' as service_user_name,'80007454' as work_no,'李翔' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '100984' as customer_no,'' as service_user_work_no,'' as service_user_name,'80007454' as work_no,'李翔' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '105499' as customer_no,'' as service_user_work_no,'' as service_user_name,'81026931' as work_no,'林圳' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '114843' as customer_no,'' as service_user_work_no,'' as service_user_name,'81016757' as work_no,'吴周机' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '108589' as customer_no,'' as service_user_work_no,'' as service_user_name,'80936091' as work_no,'刘鹏' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '102580' as customer_no,'' as service_user_work_no,'' as service_user_name,'80929710' as work_no,'王坚' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '102890' as customer_no,'' as service_user_work_no,'' as service_user_name,'80895350' as work_no,'陈聪' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select 'PF0099' as customer_no,'' as service_user_work_no,'' as service_user_name,'80924363' as work_no,'胡康灿' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '116099' as customer_no,'' as service_user_work_no,'' as service_user_name,'80912701' as work_no,'蓝梦玲' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '106301' as customer_no,'' as service_user_work_no,'' as service_user_name,'81042140' as work_no,'林志雄' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '106306' as customer_no,'' as service_user_work_no,'' as service_user_name,'81042140' as work_no,'林志雄' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '120246' as customer_no,'' as service_user_work_no,'' as service_user_name,'81062977' as work_no,'邱维海' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '120689' as customer_no,'' as service_user_work_no,'' as service_user_name,'81118962' as work_no,'陈威仁' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '117047' as customer_no,'' as service_user_work_no,'' as service_user_name,'81043405' as work_no,'魏桓' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '118836' as customer_no,'' as service_user_work_no,'' as service_user_name,'81042140' as work_no,'林志雄' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '115899' as customer_no,'' as service_user_work_no,'' as service_user_name,'81118962' as work_no,'陈威仁' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '116398' as customer_no,'' as service_user_work_no,'' as service_user_name,'81043405' as work_no,'魏桓' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '122143' as customer_no,'' as service_user_work_no,'' as service_user_name,'81097555' as work_no,'沈文乾' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '117108' as customer_no,'' as service_user_work_no,'' as service_user_name,'81042140' as work_no,'林志雄' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '123827' as customer_no,'' as service_user_work_no,'' as service_user_name,'81042140' as work_no,'林志雄' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '117121' as customer_no,'' as service_user_work_no,'' as service_user_name,'81062977' as work_no,'邱维海' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '117222' as customer_no,'' as service_user_work_no,'' as service_user_name,'81097555' as work_no,'沈文乾' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '123442' as customer_no,'' as service_user_work_no,'' as service_user_name,'81062977' as work_no,'邱维海' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '120735' as customer_no,'' as service_user_work_no,'' as service_user_name,'81062977' as work_no,'邱维海' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '118183' as customer_no,'' as service_user_work_no,'' as service_user_name,'81118962' as work_no,'陈威仁' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '125137' as customer_no,'' as service_user_work_no,'' as service_user_name,'81043405' as work_no,'魏桓' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '123923' as customer_no,'' as service_user_work_no,'' as service_user_name,'LL000003' as work_no,'彭先檩（龙岩）' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '120836' as customer_no,'' as service_user_work_no,'' as service_user_name,'81042140' as work_no,'林志雄' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '123034' as customer_no,'' as service_user_work_no,'' as service_user_name,'90000001' as work_no,'龙岩B' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '123859' as customer_no,'' as service_user_work_no,'' as service_user_name,'81043405' as work_no,'魏桓' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '123650' as customer_no,'' as service_user_work_no,'' as service_user_name,'81062977' as work_no,'邱维海' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '124555' as customer_no,'' as service_user_work_no,'' as service_user_name,'90000001' as work_no,'龙岩B' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '124641' as customer_no,'' as service_user_work_no,'' as service_user_name,'81062977' as work_no,'邱维海' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '104318' as customer_no,'' as service_user_work_no,'' as service_user_name,'44555151' as work_no,'泉州B' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '104085' as customer_no,'' as service_user_work_no,'' as service_user_name,'44555151' as work_no,'泉州B' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '120781' as customer_no,'' as service_user_work_no,'' as service_user_name,'10000001' as work_no,'彭先檩（泉州）' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '118744' as customer_no,'' as service_user_work_no,'' as service_user_name,'81094607' as work_no,'林志高' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '106433' as customer_no,'' as service_user_work_no,'' as service_user_name,'44555151' as work_no,'泉州B' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '115051' as customer_no,'' as service_user_work_no,'' as service_user_name,'81129344' as work_no,'洪少灵' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '108283' as customer_no,'' as service_user_work_no,'' as service_user_name,'81094607' as work_no,'林志高' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '119897' as customer_no,'' as service_user_work_no,'' as service_user_name,'10000001' as work_no,'彭先檩（泉州）' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '109722' as customer_no,'' as service_user_work_no,'' as service_user_name,'81094607' as work_no,'林志高' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '114516' as customer_no,'' as service_user_work_no,'' as service_user_name,'80969261' as work_no,'黄诗偶' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '120365' as customer_no,'' as service_user_work_no,'' as service_user_name,'81094607' as work_no,'林志高' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '119210' as customer_no,'' as service_user_work_no,'' as service_user_name,'81094607' as work_no,'林志高' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '119168' as customer_no,'' as service_user_work_no,'' as service_user_name,'81094607' as work_no,'林志高' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '119519' as customer_no,'' as service_user_work_no,'' as service_user_name,'81094607' as work_no,'林志高' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '122555' as customer_no,'' as service_user_work_no,'' as service_user_name,'81094607' as work_no,'林志高' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '121507' as customer_no,'' as service_user_work_no,'' as service_user_name,'81094607' as work_no,'林志高' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '122269' as customer_no,'' as service_user_work_no,'' as service_user_name,'10000001' as work_no,'彭先檩（泉州）' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '123222' as customer_no,'' as service_user_work_no,'' as service_user_name,'10000001' as work_no,'彭先檩（泉州）' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '123262' as customer_no,'' as service_user_work_no,'' as service_user_name,'10000001' as work_no,'彭先檩（泉州）' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '123257' as customer_no,'' as service_user_work_no,'' as service_user_name,'10000001' as work_no,'彭先檩（泉州）' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '123242' as customer_no,'' as service_user_work_no,'' as service_user_name,'10000001' as work_no,'彭先檩（泉州）' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '123247' as customer_no,'' as service_user_work_no,'' as service_user_name,'10000001' as work_no,'彭先檩（泉州）' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '123253' as customer_no,'' as service_user_work_no,'' as service_user_name,'10000001' as work_no,'彭先檩（泉州）' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '124602' as customer_no,'' as service_user_work_no,'' as service_user_name,'10000001' as work_no,'彭先檩（泉州）' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '115205' as customer_no,'' as service_user_work_no,'' as service_user_name,'81088296' as work_no,'陈惠燕' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '110807' as customer_no,'' as service_user_work_no,'' as service_user_name,'81034648' as work_no,'李紫珊' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '115252' as customer_no,'' as service_user_work_no,'' as service_user_name,'81034648' as work_no,'李紫珊' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '115643' as customer_no,'' as service_user_work_no,'' as service_user_name,'81093307' as work_no,'黄少伟' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '117929' as customer_no,'' as service_user_work_no,'' as service_user_name,'81034648' as work_no,'李紫珊' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '120459' as customer_no,'' as service_user_work_no,'' as service_user_name,'XM000001' as work_no,'彭东京' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '121206' as customer_no,'' as service_user_work_no,'' as service_user_name,'XM000001' as work_no,'彭东京' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '124098' as customer_no,'' as service_user_work_no,'' as service_user_name,'81093307' as work_no,'黄少伟' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '102755' as customer_no,'' as service_user_work_no,'' as service_user_name,'10000002' as work_no,'彭先檩（厦门）' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '103175' as customer_no,'' as service_user_work_no,'' as service_user_name,'10000002' as work_no,'彭先檩（厦门）' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '103868' as customer_no,'' as service_user_work_no,'' as service_user_name,'10000002' as work_no,'彭先檩（厦门）' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '103874' as customer_no,'' as service_user_work_no,'' as service_user_name,'10000002' as work_no,'彭先檩（厦门）' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '103887' as customer_no,'' as service_user_work_no,'' as service_user_name,'10000002' as work_no,'彭先檩（厦门）' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '103898' as customer_no,'' as service_user_work_no,'' as service_user_name,'10000002' as work_no,'彭先檩（厦门）' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '103908' as customer_no,'' as service_user_work_no,'' as service_user_name,'10000002' as work_no,'彭先檩（厦门）' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '103926' as customer_no,'' as service_user_work_no,'' as service_user_name,'10000002' as work_no,'彭先檩（厦门）' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '103927' as customer_no,'' as service_user_work_no,'' as service_user_name,'10000002' as work_no,'彭先檩（厦门）' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '104460' as customer_no,'' as service_user_work_no,'' as service_user_name,'10000002' as work_no,'彭先檩（厦门）' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '109000' as customer_no,'' as service_user_work_no,'' as service_user_name,'80989132' as work_no,'姚市敏' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '105673' as customer_no,'' as service_user_work_no,'' as service_user_name,'81034648' as work_no,'李紫珊' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '106563' as customer_no,'' as service_user_work_no,'' as service_user_name,'10010007' as work_no,'厦门B' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '114522' as customer_no,'' as service_user_work_no,'' as service_user_name,'10000002' as work_no,'彭先檩（厦门）' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '115431' as customer_no,'' as service_user_work_no,'' as service_user_name,'10000002' as work_no,'彭先檩（厦门）' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '118504' as customer_no,'' as service_user_work_no,'' as service_user_name,'10000002' as work_no,'彭先檩（厦门）' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '120554' as customer_no,'' as service_user_work_no,'' as service_user_name,'XN000001' as work_no,'彭先檩（漳州）' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '113281' as customer_no,'' as service_user_work_no,'' as service_user_name,'LL000002' as work_no,'彭先檩（宁德）' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '115935' as customer_no,'' as service_user_work_no,'' as service_user_name,'81041732' as work_no,'符逢芬' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '118072' as customer_no,'' as service_user_work_no,'' as service_user_name,'81084686' as work_no,'何丽姿' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '118602' as customer_no,'' as service_user_work_no,'' as service_user_name,'80595641' as work_no,'王权威' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '118748' as customer_no,'' as service_user_work_no,'' as service_user_name,'80595641' as work_no,'王权威' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '119990' as customer_no,'' as service_user_work_no,'' as service_user_name,'81055537' as work_no,'林锋' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '125677' as customer_no,'' as service_user_work_no,'' as service_user_name,'LL000002' as work_no,'彭先檩（宁德）' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '110872' as customer_no,'' as service_user_work_no,'' as service_user_name,'80938757' as work_no,'周丽' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '118825' as customer_no,'' as service_user_work_no,'' as service_user_name,'80938757' as work_no,'周丽' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '105164' as customer_no,'' as service_user_work_no,'' as service_user_name,'80991769' as work_no,'郑宇祥' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '105165' as customer_no,'' as service_user_work_no,'' as service_user_name,'80991769' as work_no,'郑宇祥' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '119757' as customer_no,'' as service_user_work_no,'' as service_user_name,'80991769' as work_no,'郑宇祥' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '111628' as customer_no,'' as service_user_work_no,'' as service_user_name,'80991769' as work_no,'郑宇祥' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '111612' as customer_no,'' as service_user_work_no,'' as service_user_name,'80991769' as work_no,'郑宇祥' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '114289' as customer_no,'' as service_user_work_no,'' as service_user_name,'80991769' as work_no,'郑宇祥' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '117396' as customer_no,'' as service_user_work_no,'' as service_user_name,'80991769' as work_no,'郑宇祥' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '108162' as customer_no,'' as service_user_work_no,'' as service_user_name,'81043023' as work_no,'邓清兵' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '115829' as customer_no,'' as service_user_work_no,'' as service_user_name,'80751663' as work_no,'肖秀华' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '108180' as customer_no,'' as service_user_work_no,'' as service_user_name,'80751663' as work_no,'肖秀华' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '108152' as customer_no,'' as service_user_work_no,'' as service_user_name,'80751663' as work_no,'肖秀华' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '116445' as customer_no,'' as service_user_work_no,'' as service_user_name,'80751663' as work_no,'肖秀华' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '120458' as customer_no,'' as service_user_work_no,'' as service_user_name,'80751663' as work_no,'肖秀华' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '123032' as customer_no,'' as service_user_work_no,'' as service_user_name,'80751663' as work_no,'肖秀华' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '124481' as customer_no,'' as service_user_work_no,'' as service_user_name,'80751663' as work_no,'肖秀华' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '125355' as customer_no,'' as service_user_work_no,'' as service_user_name,'80751663' as work_no,'肖秀华' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '111331' as customer_no,'' as service_user_work_no,'' as service_user_name,'81051600' as work_no,'吴玉萍' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '110863' as customer_no,'' as service_user_work_no,'' as service_user_name,'81051600' as work_no,'吴玉萍' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '114667' as customer_no,'' as service_user_work_no,'' as service_user_name,'81051600' as work_no,'吴玉萍' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '108267' as customer_no,'' as service_user_work_no,'' as service_user_name,'81051600' as work_no,'吴玉萍' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '111298' as customer_no,'' as service_user_work_no,'' as service_user_name,'81051600' as work_no,'吴玉萍' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '115476' as customer_no,'' as service_user_work_no,'' as service_user_name,'81051600' as work_no,'吴玉萍' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '118914' as customer_no,'' as service_user_work_no,'' as service_user_name,'81006145' as work_no,'池万春' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '111318' as customer_no,'' as service_user_work_no,'' as service_user_name,'81006145' as work_no,'池万春' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '111336' as customer_no,'' as service_user_work_no,'' as service_user_name,'81006145' as work_no,'池万春' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '112629' as customer_no,'' as service_user_work_no,'' as service_user_name,'81006145' as work_no,'池万春' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '123623' as customer_no,'' as service_user_work_no,'' as service_user_name,'81122116' as work_no,'陈滨滨' as sales_name,'是' as is_part_time_service_manager,0 as salesperson_sales_value_fp_rate,0 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '103096' as customer_no,'' as service_user_work_no,'' as service_user_name,'80927693' as work_no,'王玉花' as sales_name,'是' as is_part_time_service_manager,0.7 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate


; --5


--客户应收金额、逾期金额
drop table csx_tmp.tc_cust_overdue_0;
create table csx_tmp.tc_cust_overdue_0
as
select
	sdt,
	customer_code as customer_no,
	customer_name,company_code,company_name,channel_code,channel_name,payment_terms,payment_days,payment_name,receivable_amount,overdue_amount,max_overdue_day,
	overdue_coefficient_numerator, -- 逾期金额*逾期天数 计算因子，用于计算逾期系数分子
	overdue_coefficient_denominator -- 应收金额*账期天数 计算因子，用于计算逾期系数分母
from
	csx_dw.dws_sss_r_d_customer_settle_detail
where
	sdt=${hiveconf:month_end_day}
	and province_name in ('重庆市','福建省')
; 

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
	a.payment_terms,	-- 账期编码
	a.payment_days,	-- 帐期天数
	a.payment_name,	-- 账期名称
	a.company_code,	-- 公司代码
	a.company_name,	-- 公司名称,
	a.receivable_amount,	-- 应收金额
	a.overdue_amount,	-- 逾期金额
	overdue_coefficient_numerator, -- 逾期金额*逾期天数 计算因子，用于计算逾期系数分子
	overdue_coefficient_denominator, -- 应收金额*账期天数 计算因子，用于计算逾期系数分母
	overdue_coefficient_numerator/overdue_coefficient_denominator as over_rate -- 逾期系数			
from
	(
	select
		customer_no,
		customer_name,company_code,company_name,channel_code,channel_name,payment_terms,payment_days,payment_name,receivable_amount,overdue_amount,max_overdue_day,
		overdue_coefficient_numerator, -- 逾期金额*逾期天数 计算因子，用于计算逾期系数分子
		overdue_coefficient_denominator -- 应收金额*账期天数 计算因子，用于计算逾期系数分母
	from 
		csx_tmp.tc_cust_overdue_0  
	where 
		channel_name = '大客户' 
		and sdt = ${hiveconf:month_end_day} 	
	)a
	--剔除业务代理与内购客户
	join		
		(
		select 
			* 
		from 
			csx_dw.dws_crm_w_a_customer 
		where 
			sdt=${hiveconf:month_end_day} 
			and (channel_code in('1','7','8'))  ----渠道编号-1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 7.企业购 8.其他 9.业务代理
			and (customer_name not like '%内%购%' and customer_name not like '%临保%')	
		)b on b.customer_no=a.customer_no  
	--剔除当月有城市服务商与批发内购业绩的客户逾期系数
	left join 
		(
		select 
			distinct customer_no 
		from 
			csx_dw.dws_sale_r_d_detail 
		where 
			sdt>=${hiveconf:month_start_day} 
			and sdt<=${hiveconf:month_end_day} 
			and business_type_code in('3','4')
			-- 不剔除城市服务商2.0，按大客户提成方案计算
			and customer_no not in('117817','120939','121298','121625','122567','123244','124473','124498','124601','125284')
		)e on e.customer_no=a.customer_no
	--关联客户对应销售员与服务管家
	left join		
		(  
		select 
			distinct customer_no,service_user_work_no,service_user_name,	  
			work_no,sales_name,is_part_time_service_manager
		from 
			csx_tmp.tc_customer_service_manager_info_new
		)d on d.customer_no=a.customer_no	  
where 
	e.customer_no is null
	and (a.receivable_amount>0 or a.receivable_amount is null)
; 
	

--客户逾期系数
drop table csx_tmp.tc_cust_over_rate; --13
create table csx_tmp.tc_cust_over_rate
as 
select 
	channel_name,	-- 渠道
	customer_no,	-- 客户编码
	customer_name,	-- 客户名称,
	sum(case when receivable_amount>=0 then receivable_amount else 0 end) receivable_amount,	-- 应收金额
	sum(case when overdue_amount>=0 and receivable_amount>0 then overdue_amount else 0 end) overdue_amount,	-- 逾期金额
	sum(case when overdue_coefficient_numerator>=0 and receivable_amount>0 
		then overdue_coefficient_numerator else 0 end) as overdue_coefficient_numerator,	-- 逾期金额*逾期天数
	sum(case when overdue_coefficient_denominator>=0 and receivable_amount>0 
		then overdue_coefficient_denominator else 0 end) overdue_coefficient_denominator,	-- 应收金额*帐期天数	
	coalesce(round(
		case when coalesce(sum(case when receivable_amount>=0 then receivable_amount else 0 end), 0) <= 1 then 0  
		else coalesce(sum(case when overdue_coefficient_numerator>=0 and receivable_amount>0 then overdue_coefficient_numerator else 0 end), 0)
		/(sum(case when overdue_coefficient_denominator>=0  and receivable_amount>0 then overdue_coefficient_denominator else 0 end)) end, 6),0) as over_rate -- 逾期系数 
from 
	csx_tmp.tc_cust_overdue_0 a 
where 
	channel_name = '大客户' 
	and sdt = ${hiveconf:month_end_day} 
group by 
	channel_name,customer_no,customer_name
--having
--	sum(receivable_amount)>0 or sum(receivable_amount) is null

;

--销售员逾期系数
drop table csx_tmp.tc_salesname_over_rate;
create table csx_tmp.tc_salesname_over_rate
as
select 
	a.channel_name,	-- 渠道
	d.work_no,	-- 销售员工号
	d.sales_name,	-- 销售员
	sum(case when receivable_amount>=0 then receivable_amount else 0 end) receivable_amount,	-- 应收金额
	sum(case when overdue_amount>=0 and receivable_amount>0 then overdue_amount else 0 end) overdue_amount,	-- 逾期金额
	sum(case when overdue_coefficient_numerator>=0 and receivable_amount>0 
		then overdue_coefficient_numerator else 0 end) as overdue_coefficient_numerator,	-- 逾期金额*逾期天数
	sum(case when overdue_coefficient_denominator>=0 and receivable_amount>0 
		then overdue_coefficient_denominator else 0 end) overdue_coefficient_denominator,	-- 应收金额*帐期天数	
	coalesce(round(
		case when coalesce(sum(case when receivable_amount>=0 then receivable_amount else 0 end), 0) <= 1 then 0  
		else coalesce(sum(case when overdue_coefficient_numerator>=0 and receivable_amount>0 then overdue_coefficient_numerator else 0 end), 0)
		/(sum(case when overdue_coefficient_denominator>=0  and receivable_amount>0 then overdue_coefficient_denominator else 0 end)) end, 6),0) as over_rate -- 逾期系数 		
from
	(
	select
		customer_no,
		customer_name,company_code,company_name,channel_code,channel_name,payment_terms,payment_days,payment_name,receivable_amount,overdue_amount,max_overdue_day,
		overdue_coefficient_numerator, -- 逾期金额*逾期天数 计算因子，用于计算逾期系数分子
		overdue_coefficient_denominator -- 应收金额*账期天数 计算因子，用于计算逾期系数分母
	from 
		csx_tmp.tc_cust_overdue_0  
	where 
		channel_name = '大客户' 
		and sdt = ${hiveconf:month_end_day} 	
	)a
	--剔除业务代理与内购客户
	join		
		(
		select 
			* 
		from 
			csx_dw.dws_crm_w_a_customer 
		where 
			sdt=${hiveconf:month_end_day} 
			and (channel_code in('1','7','8'))  ----渠道编号-1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 7.企业购 8.其他 9.业务代理
			and (customer_name not like '%内%购%' and customer_name not like '%临保%')	
		)b on b.customer_no=a.customer_no  
	--剔除当月有城市服务商与批发内购业绩的客户逾期系数
	left join 
		(
		select 
			distinct customer_no 
		from 
			csx_dw.dws_sale_r_d_detail 
		where 
			sdt>=${hiveconf:month_start_day} 
			and sdt<=${hiveconf:month_end_day} 
			and business_type_code in('3','4')
			-- 不剔除城市服务商2.0，按大客户提成方案计算
			and customer_no not in('117817','120939','121298','121625','122567','123244','124473','124498','124601','125284')
		)e on e.customer_no=a.customer_no
	--关联客户对应销售员与服务管家
	left join		
		(  
		select 
			distinct customer_no,service_user_work_no,service_user_name,	  
			work_no,sales_name,is_part_time_service_manager
		from 
			csx_tmp.tc_customer_service_manager_info_new
		)d on d.customer_no=a.customer_no	  
where 
	e.customer_no is null
group by 
	a.channel_name,	-- 渠道
	d.work_no,	-- 销售员工号
	d.sales_name	-- 销售员
;
				

--服务管家逾期率
drop table csx_tmp.tc_service_user_over_rate;
create table csx_tmp.tc_service_user_over_rate
as
select 
	a.channel_name,	-- 渠道
	d.service_user_work_no,
	d.service_user_name,
	sum(case when receivable_amount>=0 then receivable_amount else 0 end) receivable_amount,	-- 应收金额
	sum(case when overdue_amount>=0 and receivable_amount>0 then overdue_amount else 0 end) overdue_amount,	-- 逾期金额
	sum(case when overdue_coefficient_numerator>=0 and receivable_amount>0 
		then overdue_coefficient_numerator else 0 end) as overdue_coefficient_numerator,	-- 逾期金额*逾期天数
	sum(case when overdue_coefficient_denominator>=0 and receivable_amount>0 
		then overdue_coefficient_denominator else 0 end) overdue_coefficient_denominator,	-- 应收金额*帐期天数	
	coalesce(round(
		case when coalesce(sum(case when receivable_amount>=0 then receivable_amount else 0 end), 0) <= 1 then 0  
		else coalesce(sum(case when overdue_coefficient_numerator>=0 and receivable_amount>0 then overdue_coefficient_numerator else 0 end), 0)
		/(sum(case when overdue_coefficient_denominator>=0  and receivable_amount>0 then overdue_coefficient_denominator else 0 end)) end, 6),0) as over_rate -- 逾期系数 		
from
	(
	select
		customer_no,
		customer_name,company_code,company_name,channel_code,channel_name,payment_terms,payment_days,payment_name,receivable_amount,overdue_amount,max_overdue_day,
		overdue_coefficient_numerator, -- 逾期金额*逾期天数 计算因子，用于计算逾期系数分子
		overdue_coefficient_denominator -- 应收金额*账期天数 计算因子，用于计算逾期系数分母
	from 
		csx_tmp.tc_cust_overdue_0  
	where 
		channel_name = '大客户' 
		and sdt = ${hiveconf:month_end_day} 	
	)a
	--剔除业务代理与内购客户
	join		
		(
		select 
			* 
		from 
			csx_dw.dws_crm_w_a_customer 
		where 
			sdt=${hiveconf:month_end_day} 
			and (channel_code in('1','7','8'))  ----渠道编号-1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 7.企业购 8.其他 9.业务代理
			and (customer_name not like '%内%购%' and customer_name not like '%临保%')	
		)b on b.customer_no=a.customer_no  
	--剔除当月有城市服务商与批发内购业绩的客户逾期系数
	left join 
		(
		select 
			distinct customer_no 
		from 
			csx_dw.dws_sale_r_d_detail 
		where 
			sdt>=${hiveconf:month_start_day} 
			and sdt<=${hiveconf:month_end_day} 
			and business_type_code in('3','4')
			-- 不剔除城市服务商2.0，按大客户提成方案计算
			and customer_no not in('117817','120939','121298','121625','122567','123244','124473','124498','124601','125284')
		)e on e.customer_no=a.customer_no
	--关联客户对应销售员与服务管家
	left join		
		(  
		select 
			distinct customer_no,service_user_work_no,service_user_name,	  
			work_no,sales_name,is_part_time_service_manager
		from 
			csx_tmp.tc_customer_service_manager_info_new
		)d on d.customer_no=a.customer_no	  
where 
	e.customer_no is null
group by 
	a.channel_name,	-- 渠道
	d.service_user_work_no,
	d.service_user_name
;



--大宗供应链的逾期系数
insert overwrite directory '/tmp/zhangyanpeng/yuqi_dazong' row format delimited fields terminated by '\t'
select 
	a.channel_name,	-- 渠道
	b.sales_province_name,	-- 省区
	a.customer_no,	-- 客户编码
	a.customer_name,	-- 客户名称
	b.work_no,	-- 销售员工号
	b.sales_name,	-- 销售员
	a.payment_terms,	-- 账期编码
	a.payment_days,	-- 帐期天数
	a.payment_name,	-- 账期名称
	a.company_code,	-- 公司代码
	a.company_name,	-- 公司名称
	a.receivable_amount,	-- 应收金额
	a.overdue_amount,	-- 逾期金额
	overdue_coefficient_numerator, -- 逾期金额*逾期天数 计算因子，用于计算逾期系数分子
	overdue_coefficient_denominator, -- 应收金额*账期天数 计算因子，用于计算逾期系数分母
	if(overdue_coefficient_numerator/overdue_coefficient_denominator<0,0,overdue_coefficient_numerator/overdue_coefficient_denominator) as over_rate -- 逾期系数			    
from
	(
	select
		customer_no,
		customer_name,company_code,company_name,channel_code,channel_name,payment_terms,payment_days,payment_name,receivable_amount,overdue_amount,max_overdue_day,
		overdue_coefficient_numerator, -- 逾期金额*逾期天数 计算因子，用于计算逾期系数分子
		overdue_coefficient_denominator -- 应收金额*账期天数 计算因子，用于计算逾期系数分母		
	from 
		csx_tmp.tc_cust_overdue_0  
	where 
		(channel_name like '大宗%' or channel_name like '%供应链%')
		and sdt =${hiveconf:month_end_day} 
	)a
	join		 
		(
		select 
			* 
		from 
			csx_dw.dws_crm_w_a_customer 
		where 
			sdt=${hiveconf:month_end_day} 
			and channel_code in('4','5','6') ----渠道编号-1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 7.企业购 8.其他 9.业务代理
		)b on b.customer_no=a.customer_no 
where
	(a.receivable_amount>0 or a.receivable_amount is null)
;

--=============================================================================================================================================================================
--5月查询结果是perform_dc_code in('W0L4','W0K7','W0AW','W0BY')三个城市服务商2.0仓过机的客户都只在这些仓有过机，以后最好再看下
--城市服务商2.0的逾期系数
--insert overwrite directory '/tmp/zhangyanpeng/yuqi_csfws2' row format delimited fields terminated by '\t'
--select 
--	a.channel_name,	-- 渠道
--	b.sales_province_name,	-- 省区
--	a.customer_no,	-- 客户编码
--	a.customer_name,	-- 客户名称
--	b.work_no,	-- 销售员工号
--	b.sales_name,	-- 销售员
--	c.account_period_code,	-- 账期编码
--	if(c.account_period_code like 'Y%',if(c.account_period_val=31,45,c.account_period_val+15),c.account_period_val) account_period_val,	-- 帐期天数
--	c.account_period_name,	-- 账期名称
--	a.company_code,	-- 公司代码
--	a.company_name,	-- 公司名称
--	case when a.receivable_amount>=0 then a.receivable_amount else 0 end receivable_amount,	-- 应收金额
--	case when a.over_amt>=0 and a.receivable_amount>0 then a.over_amt else 0 end over_amt,	-- 逾期金额
--	case when a.over_amt_s>=0 and a.receivable_amount>0 then a.over_amt_s else 0 end over_amt_s,	-- 逾期金额*逾期天数
--	case when a.receivable_amount_s>=0 and a.receivable_amount>0 then a.receivable_amount_s else 0 end receivable_amount_s,	-- 应收金额*帐期天数	
--    coalesce(round(case when coalesce(case when a.receivable_amount>=0 then a.receivable_amount else 0 end, 0) <= 1 then 0  
--						else (coalesce(case when a.over_amt_s>=0 and a.receivable_amount>0 then a.over_amt_s else 0 end,0)
--						/(case when a.receivable_amount_s>=0 and a.receivable_amount>0 then a.receivable_amount_s else 0 end)) end
--		  , 6),0) over_rate 	-- 逾期系数
--		    
--from
--	(
--	select
--		channel_name,
--		customer_no,
--		customer_name,
--		--appoint_place_code,  --履约地点编码
--		--account_period_code,
--		--COALESCE(account_period_val,0) account_period_val,
--		--account_period_name,
--		company_code,
--		company_name,
--		sum(receivable_amount) as receivable_amount,
--		sum(case when over_days>=1 then receivable_amount else 0 end ) as over_amt,
--		sum(case when over_days>=1 then receivable_amount*over_days else 0 end) as over_amt_s,
--		sum(case when receivable_amount>=0 then receivable_amount else 0 end* if(COALESCE(account_period_val,0)=0,1,acc_val_calculation_factor)) as receivable_amount_s
--	from
--		(
--		select 
--			* 
--		from 
--			csx_tmp.tmp_tc_cust_order_overdue_dtl  
--		where 
--			channel_name = '大客户'
--			and sdt = ${hiveconf:i_sdate_11} 
--		)a 
--		--城市服务商2.0 按履约DC
--		join
--			(
--			select 
--				distinct inventory_dc_code
--			from 
--				csx_ods.source_csms_w_a_yszx_town_service_provider_config
--			)d on a.appoint_place_code=d.inventory_dc_code	
--	group by 
--		channel_name,customer_no,customer_name,company_code,company_name
--	)a
--	join		 ----渠道编号-1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 7.企业购 8.其他 9.业务代理
--		(
--		select 
--			* 
--		from 
--			csx_dw.dws_crm_w_a_customer 
--		where 
--			sdt=${hiveconf:i_sdate_11} 
--			--where sdt='20210617'
--			and channel_code in('1','7') 
--		)b on b.customer_no=a.customer_no  
--	left join
--		(
--		select
--			customer_no,
--			company_code,
--			payment_terms account_period_code,
--			case when payment_terms like 'Y%' then concat('月结',COALESCE(cast(payment_days as int),0)) 
--				else concat('票到',COALESCE(cast(payment_days as int),0)) end account_period_name,
--			COALESCE(cast(payment_days as int),0) account_period_val
--		from 
--			csx_dw.dws_crm_w_a_customer_company a
--		where 
--			sdt='current'
--			and customer_no<>''
--		)c on (a.customer_no=c.customer_no and a.company_code=c.company_code)
--;



--查询城市服务商2.0客户,按库存DC

--select distinct inventory_dc_code from csx_ods.source_csms_w_a_yszx_town_service_provider_config; -- W0AW、W0BY、W0K7、W0L4

--select 
--	a.*,c.work_no,c.sales_name
--from 
--	(
--	select 
--		province_name,customer_no,customer_name,business_type_name,dc_code,
--		sum(sales_value)sales_value
--	from 
--		csx_dw.dws_sale_r_d_detail
--	where 
--		sdt>='20211101'
--		and sdt<='20211130'
--		and channel_code in('1','7','9')
--		--and business_type_code not in('3','4')
--	group by 
--		province_name,customer_no,customer_name,business_type_name,dc_code
--	)a 
--	join 
--		(
--		select 
--			distinct customer_no
--		from 
--			csx_dw.dws_sale_r_d_detail
--		where 
--			sdt>='20211101'
--			and sdt<='20211130'
--			and channel_code in('1','7','9')
--			and dc_code in('W0AW','W0K7','W0L4','W0BY')
--		) b on b.customer_no=a.customer_no
--	left join 
--		(
--		select 
--			distinct customer_no,customer_name,work_no,sales_name,sales_province_name
--		from 
--			csx_dw.dws_crm_w_a_customer 
--			--where sdt=${hiveconf:i_sdate_11} 
--		where 
--			sdt='20211130'
--		)c on c.customer_no=a.customer_no;


--安徽省按照大客户计算的客户

--select 
--	a.customer_no
--from 
--	(
--	select 
--		province_name,customer_no,customer_name,business_type_name,
--		sum(sales_value)sales_value
--	from 
--		csx_dw.dws_sale_r_d_detail
--	where 
--		sdt>='20220101'
--		and sdt<='20220131'
--		and channel_code in('1','7','9')
--		and business_type_code in ('4')
--	group by 
--		province_name,customer_no,customer_name,business_type_name
--	)a 
--	join 
--		(
--		select 
--			customer_no,customer_name,work_no,sales_name,sales_province_name
--		from 
--			csx_dw.dws_crm_w_a_customer 
--		where 
--			sdt='20220131'
--			and sales_province_name='安徽省'
--			and work_no not in ('81138989','81138992','81123285','81119588','81086805','81054801','80972242','80884343','81133185','81107924','81087574','80897767',
--							'81034712','80886641')
--		)c on c.customer_no=a.customer_no
--group by 
--	a.customer_no
--;


