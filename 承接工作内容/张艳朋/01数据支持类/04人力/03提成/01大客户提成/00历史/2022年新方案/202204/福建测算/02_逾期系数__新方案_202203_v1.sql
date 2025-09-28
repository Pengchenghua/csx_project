-- 创建人员信息表，获取销售员和服务管家的城市，因为存在一个业务员名下客户跨城市的情况
drop table csx_tmp.tc_person_info; --5
create table csx_tmp.tc_person_info
as
select 
	distinct a.id,a.user_number,a.name,b.city_group_code,b.city_group_name,b.province_code,b.province_name,b.region_code,b.region_name
from
	(
	select
		id,user_number,name,user_position,city_name,prov_name
	from
		csx_dw.dws_basic_w_a_user
	where
		sdt=regexp_replace(date_sub(current_date,1),'-','')
		and del_flag = '0'
	) a
	left join -- 区域表
		( 
		select distinct
			city_code,city_name,area_province_code,area_province_name,city_group_code,city_group_name,province_code,province_name,region_code,region_name
		from
			csx_dw.dws_sale_w_a_area_belong
		) b on b.city_name=a.city_name and b.area_province_name=a.prov_name
;

-- 月初 月末 年初
set month_start_day ='20220301';	
set month_end_day ='20220331';	
set year_start_day ='20220101';		


-- 签呈处理销售员服务管家关系
drop table csx_tmp.tc_customer_service_manager_info_new;
create table csx_tmp.tc_customer_service_manager_info_new
as  
select 
	distinct customer_no,service_user_work_no,service_user_name,work_no,sales_name,is_part_time_service_manager,
    sales_sale_rate as salesperson_sales_value_fp_rate, --销售员_销售额_分配比例
	sales_profit_rate as salesperson_profit_fp_rate,  --销售员_定价毛利额分配比例
    if(work_no=service_user_work_no and is_part_time_service_manager='是',0,service_user_sale_rate) as service_user_sales_value_fp_rate,  --服务管家_销售额_分配比例
	if(work_no=service_user_work_no and is_part_time_service_manager='是',0,service_user_profit_rate) as service_user_profit_fp_rate --服务管家_定价毛利额_分配比例
from 
	csx_dw.report_crm_w_a_customer_service_manager_info_new
where 
	sdt=${hiveconf:month_end_day}
	and customer_no not in ('X000000','105502','100326','PF1265','102734','102784','102901','112288','113467','103199','104469','104501','115982','115987','PF0365','102633',
	'103141','104054','104954','105225','105721','105806','105838','107371','112062','113635','113646','114344','115679','118687','105593','107398','109447','123065','116015',
	'115721','119703','118887','116556','116733','116702','116923','116863','116883','116877','116826','123534','116967','117026','117025','117009','117045','117040','117035',
	'116944','117030','117027','116903','116994','117019','116993','116989','123716','120100','119806','119803','119815','120463','122125','122106','121763','122375','122286',
	'122371','123060','123383','123644','123813','124179','125244','125508','103183','104086','104397','100984','105499','114843','108589','102580','102890','PF0099','116099',
	'106301','106306','120246','120689','117047','118836','115899','116398','122143','117108','123827','117121','117222','123442','120735','118183','125137','123923','120836',
	'123034','123859','123650','124555','124641','104318','104085','120781','118744','106433','115051','108283','119897','109722','114516','120365','119210','119168','119519',
	'122555','121507','122269','123222','123262','123257','123242','123247','123253','124602','115205','115643','120459','121206','102755','103175','103868','103874','103887',
	'103898','103908','103926','103927','104460','109000','106563','114522','115431','118504','120554','113281','115935','118602','118748','119990','125677','110872','118825',
	'108162','115829','108180','108152','116445','120458','123032','124481','125355','111331','110863','114667','108267','111298','115476','111318','112629','123623','103096',
	'116639','117304','117605','112181','113641','117119','118824','118848','119196','120098','120130','120985','121368','116242','123311','124033','118376','124524','121038',
	'121068','121231','121353','121466','121820','124531','124846','125462','125587','125702','PF0129','103358','103831','102130','105164','105165','108190','119757','111628',
	'111612','113377','114289','115130','115438','123486','118189','117396','118655','118840','119731','121584','123280','123294','108810','111336','111867','115659','115901',
	'116646','118914','119428','121675','123184','124161','125040','104281','115656','111207','115537','115602','115826','125534','117244','102686','105181','106423','106481',
	'106881','103714','PF1205','115646','115324','115857','116211','115204','119524','118341','118340','120245','101482','103062','105177','105156','111241','107404','102229',
	'102508','104007','104165','104970','105441','112327','113101','105886','125064','103372','101653','102565','104547','120994','117006','122681','113840','106183','117012',
	'118608','111860','110667','111844','118752','113548','113615','117299','117676','121357','121388','106265','113001','116914','117736','100563','PF0094','104612','105882',
	'108739','109377','103855','108127','105150','105182','106721','105355'
	)
union all   select '105502' as customer_no,'' as service_user_work_no,'' as service_user_name,'80970937' as work_no,'高京庭' as sales_name,'否' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '100326' as customer_no,'80009493' as service_user_work_no,'郑银燕' as service_user_name,'80007454' as work_no,'李翔' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select 'PF1265' as customer_no,'80972915' as service_user_work_no,'陈伟豪' as service_user_name,'80012225' as work_no,'林挺波' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '102734' as customer_no,'81099343' as service_user_work_no,'林瑾鑫' as service_user_name,'80980614' as work_no,'池春梅' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
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
union all   select '102633' as customer_no,'80691224' as service_user_work_no,'王少端' as service_user_name,'80691224' as work_no,'王少端' as sales_name,'是' as is_part_time_service_manager,0 as salesperson_sales_value_fp_rate,0 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '103141' as customer_no,'80816155' as service_user_work_no,'张磊磊' as service_user_name,'80816155' as work_no,'张磊磊' as sales_name,'是' as is_part_time_service_manager,0 as salesperson_sales_value_fp_rate,0 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '104054' as customer_no,'81129243' as service_user_work_no,'汪敏禄' as service_user_name,'81129243' as work_no,'汪敏禄' as sales_name,'是' as is_part_time_service_manager,0 as salesperson_sales_value_fp_rate,0 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '104954' as customer_no,'80691224' as service_user_work_no,'王少端' as service_user_name,'80691224' as work_no,'王少端' as sales_name,'是' as is_part_time_service_manager,0 as salesperson_sales_value_fp_rate,0 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '105225' as customer_no,'80691224' as service_user_work_no,'王少端' as service_user_name,'80691224' as work_no,'王少端' as sales_name,'是' as is_part_time_service_manager,0 as salesperson_sales_value_fp_rate,0 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '105721' as customer_no,'80952742' as service_user_work_no,'王秀云' as service_user_name,'80952742' as work_no,'王秀云' as sales_name,'是' as is_part_time_service_manager,0 as salesperson_sales_value_fp_rate,0 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '105806' as customer_no,'80816155' as service_user_work_no,'张磊磊' as service_user_name,'80816155' as work_no,'张磊磊' as sales_name,'是' as is_part_time_service_manager,0 as salesperson_sales_value_fp_rate,0 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '105838' as customer_no,'80816155' as service_user_work_no,'张磊磊' as service_user_name,'80816155' as work_no,'张磊磊' as sales_name,'是' as is_part_time_service_manager,0 as salesperson_sales_value_fp_rate,0 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '107371' as customer_no,'81129243' as service_user_work_no,'汪敏禄' as service_user_name,'81129243' as work_no,'汪敏禄' as sales_name,'是' as is_part_time_service_manager,0 as salesperson_sales_value_fp_rate,0 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '112062' as customer_no,'80816155' as service_user_work_no,'张磊磊' as service_user_name,'80816155' as work_no,'张磊磊' as sales_name,'是' as is_part_time_service_manager,0 as salesperson_sales_value_fp_rate,0 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '113635' as customer_no,'80691224' as service_user_work_no,'王少端' as service_user_name,'80691224' as work_no,'王少端' as sales_name,'是' as is_part_time_service_manager,0 as salesperson_sales_value_fp_rate,0 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '113646' as customer_no,'80691224' as service_user_work_no,'王少端' as service_user_name,'80691224' as work_no,'王少端' as sales_name,'是' as is_part_time_service_manager,0 as salesperson_sales_value_fp_rate,0 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '114344' as customer_no,'' as service_user_work_no,'' as service_user_name,'80005782' as work_no,'杨海燕' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '115679' as customer_no,'80816155' as service_user_work_no,'张磊磊' as service_user_name,'80816155' as work_no,'张磊磊' as sales_name,'是' as is_part_time_service_manager,0 as salesperson_sales_value_fp_rate,0 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '118687' as customer_no,'81099343' as service_user_work_no,'林瑾鑫' as service_user_name,'81099343' as work_no,'林瑾鑫' as sales_name,'是' as is_part_time_service_manager,0 as salesperson_sales_value_fp_rate,0 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '105593' as customer_no,'' as service_user_work_no,'' as service_user_name,'1000002' as work_no,'莆田B' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '107398' as customer_no,'' as service_user_work_no,'' as service_user_name,'80974184' as work_no,'郭荔丽' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '109447' as customer_no,'' as service_user_work_no,'' as service_user_name,'1000002' as work_no,'莆田B' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '123065' as customer_no,'' as service_user_work_no,'' as service_user_name,'1000002' as work_no,'莆田B' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
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
union all   select '115643' as customer_no,'' as service_user_work_no,'' as service_user_name,'81093307' as work_no,'黄少伟' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '120459' as customer_no,'' as service_user_work_no,'' as service_user_name,'XM000001' as work_no,'彭东京' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '121206' as customer_no,'' as service_user_work_no,'' as service_user_name,'XM000001' as work_no,'彭东京' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
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
union all   select '106563' as customer_no,'' as service_user_work_no,'' as service_user_name,'10010007' as work_no,'厦门B' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '114522' as customer_no,'' as service_user_work_no,'' as service_user_name,'10000002' as work_no,'彭先檩（厦门）' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '115431' as customer_no,'' as service_user_work_no,'' as service_user_name,'10000002' as work_no,'彭先檩（厦门）' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '118504' as customer_no,'' as service_user_work_no,'' as service_user_name,'10000002' as work_no,'彭先檩（厦门）' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '120554' as customer_no,'' as service_user_work_no,'' as service_user_name,'XN000001' as work_no,'彭先檩（漳州）' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '113281' as customer_no,'' as service_user_work_no,'' as service_user_name,'LL000002' as work_no,'彭先檩（宁德）' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '115935' as customer_no,'' as service_user_work_no,'' as service_user_name,'81041732' as work_no,'符逢芬' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '118602' as customer_no,'' as service_user_work_no,'' as service_user_name,'80595641' as work_no,'王权威' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '118748' as customer_no,'' as service_user_work_no,'' as service_user_name,'80595641' as work_no,'王权威' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '119990' as customer_no,'' as service_user_work_no,'' as service_user_name,'81055537' as work_no,'林锋' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '125677' as customer_no,'' as service_user_work_no,'' as service_user_name,'LL000002' as work_no,'彭先檩（宁德）' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '110872' as customer_no,'' as service_user_work_no,'' as service_user_name,'80938757' as work_no,'周丽' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '118825' as customer_no,'' as service_user_work_no,'' as service_user_name,'80938757' as work_no,'周丽' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
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
union all   select '111318' as customer_no,'' as service_user_work_no,'' as service_user_name,'81006145' as work_no,'池万春' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '112629' as customer_no,'' as service_user_work_no,'' as service_user_name,'81006145' as work_no,'池万春' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '123623' as customer_no,'' as service_user_work_no,'' as service_user_name,'81122116' as work_no,'陈滨滨' as sales_name,'是' as is_part_time_service_manager,0 as salesperson_sales_value_fp_rate,0 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '103096' as customer_no,'' as service_user_work_no,'' as service_user_name,'80927693' as work_no,'王玉花' as sales_name,'是' as is_part_time_service_manager,0.7 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '116639' as customer_no,'' as service_user_work_no,'' as service_user_name,'81052035' as work_no,'袁成臣' as sales_name,'是' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '117304' as customer_no,'' as service_user_work_no,'' as service_user_name,'81052035' as work_no,'袁成臣' as sales_name,'是' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '117605' as customer_no,'' as service_user_work_no,'' as service_user_name,'81052035' as work_no,'袁成臣' as sales_name,'是' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '112181' as customer_no,'' as service_user_work_no,'' as service_user_name,'80965415' as work_no,'王旭' as sales_name,'是' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '113641' as customer_no,'' as service_user_work_no,'' as service_user_name,'80965415' as work_no,'王旭' as sales_name,'是' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '117119' as customer_no,'' as service_user_work_no,'' as service_user_name,'80965415' as work_no,'王旭' as sales_name,'是' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '118824' as customer_no,'' as service_user_work_no,'' as service_user_name,'81089336' as work_no,'陈静' as sales_name,'是' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '118848' as customer_no,'' as service_user_work_no,'' as service_user_name,'81089336' as work_no,'陈静' as sales_name,'是' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '119196' as customer_no,'' as service_user_work_no,'' as service_user_name,'81089336' as work_no,'陈静' as sales_name,'是' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '120098' as customer_no,'' as service_user_work_no,'' as service_user_name,'81089336' as work_no,'陈静' as sales_name,'是' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '120130' as customer_no,'' as service_user_work_no,'' as service_user_name,'81089336' as work_no,'陈静' as sales_name,'是' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '120985' as customer_no,'' as service_user_work_no,'' as service_user_name,'81089336' as work_no,'陈静' as sales_name,'是' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '121368' as customer_no,'' as service_user_work_no,'' as service_user_name,'81089336' as work_no,'陈静' as sales_name,'是' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '116242' as customer_no,'' as service_user_work_no,'' as service_user_name,'81052035' as work_no,'袁成臣' as sales_name,'是' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '123311' as customer_no,'81103328' as service_user_work_no,'武艳' as service_user_name,'81103065' as work_no,'王世超' as sales_name,'否' as is_part_time_service_manager,0.9 as salesperson_sales_value_fp_rate,0.8 as salesperson_profit_fp_rate,0.1 as service_user_sales_value_fp_rate,0.2 as service_user_profit_fp_rate
union all   select '124033' as customer_no,'81020509' as service_user_work_no,'陈素彩' as service_user_name,'81103065' as work_no,'王世超' as sales_name,'否' as is_part_time_service_manager,0.9 as salesperson_sales_value_fp_rate,0.8 as salesperson_profit_fp_rate,0.1 as service_user_sales_value_fp_rate,0.2 as service_user_profit_fp_rate
union all   select '118376' as customer_no,'81103328' as service_user_work_no,'武艳' as service_user_name,'81103065' as work_no,'王世超' as sales_name,'否' as is_part_time_service_manager,0.9 as salesperson_sales_value_fp_rate,0.8 as salesperson_profit_fp_rate,0.1 as service_user_sales_value_fp_rate,0.2 as service_user_profit_fp_rate
union all   select '124524' as customer_no,'81103328;81138104' as service_user_work_no,'武艳;郭凯利' as service_user_name,'81103065' as work_no,'王世超' as sales_name,'否' as is_part_time_service_manager,0.9 as salesperson_sales_value_fp_rate,0.8 as salesperson_profit_fp_rate,0.1 as service_user_sales_value_fp_rate,0.2 as service_user_profit_fp_rate
union all   select '121038' as customer_no,'' as service_user_work_no,'' as service_user_name,'81095268' as work_no,'黄益聪' as sales_name,'否' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '121068' as customer_no,'' as service_user_work_no,'' as service_user_name,'81095268' as work_no,'黄益聪' as sales_name,'否' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '121231' as customer_no,'' as service_user_work_no,'' as service_user_name,'81095268' as work_no,'黄益聪' as sales_name,'否' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '121353' as customer_no,'' as service_user_work_no,'' as service_user_name,'81095268' as work_no,'黄益聪' as sales_name,'否' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '121466' as customer_no,'' as service_user_work_no,'' as service_user_name,'81095268' as work_no,'黄益聪' as sales_name,'否' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '121820' as customer_no,'' as service_user_work_no,'' as service_user_name,'81095268' as work_no,'黄益聪' as sales_name,'否' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '124531' as customer_no,'' as service_user_work_no,'' as service_user_name,'81095268' as work_no,'黄益聪' as sales_name,'否' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '124846' as customer_no,'' as service_user_work_no,'' as service_user_name,'81095268' as work_no,'黄益聪' as sales_name,'否' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '125462' as customer_no,'' as service_user_work_no,'' as service_user_name,'81095268' as work_no,'黄益聪' as sales_name,'否' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '125587' as customer_no,'' as service_user_work_no,'' as service_user_name,'81095268' as work_no,'黄益聪' as sales_name,'否' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '125702' as customer_no,'' as service_user_work_no,'' as service_user_name,'81095268' as work_no,'黄益聪' as sales_name,'否' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select 'PF0129' as customer_no,'' as service_user_work_no,'' as service_user_name,'81095268' as work_no,'黄益聪' as sales_name,'否' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '103358' as customer_no,'' as service_user_work_no,'' as service_user_name,'80991769' as work_no,'郑宇祥' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '103831' as customer_no,'' as service_user_work_no,'' as service_user_name,'80991769' as work_no,'郑宇祥' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '102130' as customer_no,'' as service_user_work_no,'' as service_user_name,'80991769' as work_no,'郑宇祥' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '105164' as customer_no,'' as service_user_work_no,'' as service_user_name,'80991769' as work_no,'郑宇祥' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '105165' as customer_no,'' as service_user_work_no,'' as service_user_name,'80991769' as work_no,'郑宇祥' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '108190' as customer_no,'' as service_user_work_no,'' as service_user_name,'80991769' as work_no,'郑宇祥' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '119757' as customer_no,'' as service_user_work_no,'' as service_user_name,'80991769' as work_no,'郑宇祥' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '111628' as customer_no,'' as service_user_work_no,'' as service_user_name,'80991769' as work_no,'郑宇祥' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '111612' as customer_no,'' as service_user_work_no,'' as service_user_name,'80991769' as work_no,'郑宇祥' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '113377' as customer_no,'' as service_user_work_no,'' as service_user_name,'80991769' as work_no,'郑宇祥' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '114289' as customer_no,'' as service_user_work_no,'' as service_user_name,'80991769' as work_no,'郑宇祥' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '115130' as customer_no,'' as service_user_work_no,'' as service_user_name,'80991769' as work_no,'郑宇祥' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '115438' as customer_no,'' as service_user_work_no,'' as service_user_name,'80991769' as work_no,'郑宇祥' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '123486' as customer_no,'' as service_user_work_no,'' as service_user_name,'80991769' as work_no,'郑宇祥' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '118189' as customer_no,'' as service_user_work_no,'' as service_user_name,'80991769' as work_no,'郑宇祥' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '117396' as customer_no,'' as service_user_work_no,'' as service_user_name,'80991769' as work_no,'郑宇祥' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '118655' as customer_no,'' as service_user_work_no,'' as service_user_name,'80991769' as work_no,'郑宇祥' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '118840' as customer_no,'' as service_user_work_no,'' as service_user_name,'80991769' as work_no,'郑宇祥' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '119731' as customer_no,'' as service_user_work_no,'' as service_user_name,'80991769' as work_no,'郑宇祥' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '121584' as customer_no,'' as service_user_work_no,'' as service_user_name,'80991769' as work_no,'郑宇祥' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '123280' as customer_no,'' as service_user_work_no,'' as service_user_name,'80991769' as work_no,'郑宇祥' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '123294' as customer_no,'' as service_user_work_no,'' as service_user_name,'81043023' as work_no,'邓清兵' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '108810' as customer_no,'81051600' as service_user_work_no,'吴玉萍' as service_user_name,'81006145' as work_no,'池万春' as sales_name,'否' as is_part_time_service_manager,0.7 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '111336' as customer_no,'81051600' as service_user_work_no,'吴玉萍' as service_user_name,'81006145' as work_no,'池万春' as sales_name,'否' as is_part_time_service_manager,0.7 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '111867' as customer_no,'81051600' as service_user_work_no,'吴玉萍' as service_user_name,'81006145' as work_no,'池万春' as sales_name,'否' as is_part_time_service_manager,0.7 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '115659' as customer_no,'81051600' as service_user_work_no,'吴玉萍' as service_user_name,'81006145' as work_no,'池万春' as sales_name,'否' as is_part_time_service_manager,0.7 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '115901' as customer_no,'81051600' as service_user_work_no,'吴玉萍' as service_user_name,'81006145' as work_no,'池万春' as sales_name,'否' as is_part_time_service_manager,0.7 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '116646' as customer_no,'81051600' as service_user_work_no,'吴玉萍' as service_user_name,'81006145' as work_no,'池万春' as sales_name,'否' as is_part_time_service_manager,0.7 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '118914' as customer_no,'81051600' as service_user_work_no,'吴玉萍' as service_user_name,'81006145' as work_no,'池万春' as sales_name,'否' as is_part_time_service_manager,0.7 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '119428' as customer_no,'81051600' as service_user_work_no,'吴玉萍' as service_user_name,'81006145' as work_no,'池万春' as sales_name,'否' as is_part_time_service_manager,0.7 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '121675' as customer_no,'81051600' as service_user_work_no,'吴玉萍' as service_user_name,'81006145' as work_no,'池万春' as sales_name,'否' as is_part_time_service_manager,0.7 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '123184' as customer_no,'81051600' as service_user_work_no,'吴玉萍' as service_user_name,'81006145' as work_no,'池万春' as sales_name,'否' as is_part_time_service_manager,0.7 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '124161' as customer_no,'81051600' as service_user_work_no,'吴玉萍' as service_user_name,'81006145' as work_no,'池万春' as sales_name,'否' as is_part_time_service_manager,0.7 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '125040' as customer_no,'81051600' as service_user_work_no,'吴玉萍' as service_user_name,'81006145' as work_no,'池万春' as sales_name,'否' as is_part_time_service_manager,0.7 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '104281' as customer_no,'80974184;81107303' as service_user_work_no,'郭荔丽;姚细美' as service_user_name,'XN00000001' as work_no,'虚拟B' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '115656' as customer_no,'80974184;81107303' as service_user_work_no,'郭荔丽;姚细美' as service_user_name,'XN00000001' as work_no,'虚拟B' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '111207' as customer_no,'80974184;81107303' as service_user_work_no,'郭荔丽;姚细美' as service_user_name,'XN00000001' as work_no,'虚拟B' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '115537' as customer_no,'80974184;81107303' as service_user_work_no,'郭荔丽;姚细美' as service_user_name,'XN00000001' as work_no,'虚拟B' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '115602' as customer_no,'80974184;81107303' as service_user_work_no,'郭荔丽;姚细美' as service_user_name,'XN00000001' as work_no,'虚拟B' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '115826' as customer_no,'80974184;81107303' as service_user_work_no,'郭荔丽;姚细美' as service_user_name,'XN00000001' as work_no,'虚拟B' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '125534' as customer_no,'80974184;81107303' as service_user_work_no,'郭荔丽;姚细美' as service_user_name,'XN00000001' as work_no,'虚拟B' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '117244' as customer_no,'80974184;81107303' as service_user_work_no,'郭荔丽;姚细美' as service_user_name,'XN00000001' as work_no,'虚拟B' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '102686' as customer_no,'' as service_user_work_no,'' as service_user_name,'81131450' as work_no,'张珠妹' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '105181' as customer_no,'' as service_user_work_no,'' as service_user_name,'81131450' as work_no,'张珠妹' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '106423' as customer_no,'' as service_user_work_no,'' as service_user_name,'81131450' as work_no,'张珠妹' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '106481' as customer_no,'' as service_user_work_no,'' as service_user_name,'81131450' as work_no,'张珠妹' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '106881' as customer_no,'' as service_user_work_no,'' as service_user_name,'81131450' as work_no,'张珠妹' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '103714' as customer_no,'' as service_user_work_no,'' as service_user_name,'80816155' as work_no,'张磊磊' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select 'PF1205' as customer_no,'' as service_user_work_no,'' as service_user_name,'80816155' as work_no,'张磊磊' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '115646' as customer_no,'' as service_user_work_no,'' as service_user_name,'80816155' as work_no,'张磊磊' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '115324' as customer_no,'' as service_user_work_no,'' as service_user_name,'80816155' as work_no,'张磊磊' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '115857' as customer_no,'' as service_user_work_no,'' as service_user_name,'80816155' as work_no,'张磊磊' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '116211' as customer_no,'' as service_user_work_no,'' as service_user_name,'80816155' as work_no,'张磊磊' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '115204' as customer_no,'' as service_user_work_no,'' as service_user_name,'80895350' as work_no,'陈聪' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '119524' as customer_no,'' as service_user_work_no,'' as service_user_name,'80895350' as work_no,'陈聪' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '118341' as customer_no,'' as service_user_work_no,'' as service_user_name,'80895350' as work_no,'陈聪' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '118340' as customer_no,'' as service_user_work_no,'' as service_user_name,'80895350' as work_no,'陈聪' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '120245' as customer_no,'' as service_user_work_no,'' as service_user_name,'80895350' as work_no,'陈聪' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '101482' as customer_no,'' as service_user_work_no,'' as service_user_name,'80952742' as work_no,'王秀云' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '103062' as customer_no,'' as service_user_work_no,'' as service_user_name,'80952742' as work_no,'王秀云' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '105177' as customer_no,'' as service_user_work_no,'' as service_user_name,'80952742' as work_no,'王秀云' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '105156' as customer_no,'' as service_user_work_no,'' as service_user_name,'80952742' as work_no,'王秀云' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '111241' as customer_no,'' as service_user_work_no,'' as service_user_name,'80952742' as work_no,'王秀云' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '107404' as customer_no,'' as service_user_work_no,'' as service_user_name,'80952742' as work_no,'王秀云' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '102229' as customer_no,'' as service_user_work_no,'' as service_user_name,'80691224' as work_no,'王少端' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '102508' as customer_no,'' as service_user_work_no,'' as service_user_name,'80691224' as work_no,'王少端' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '104007' as customer_no,'' as service_user_work_no,'' as service_user_name,'80691224' as work_no,'王少端' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '104165' as customer_no,'' as service_user_work_no,'' as service_user_name,'80691224' as work_no,'王少端' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '104970' as customer_no,'' as service_user_work_no,'' as service_user_name,'80691224' as work_no,'王少端' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '105441' as customer_no,'' as service_user_work_no,'' as service_user_name,'80691224' as work_no,'王少端' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '112327' as customer_no,'' as service_user_work_no,'' as service_user_name,'80691224' as work_no,'王少端' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '113101' as customer_no,'' as service_user_work_no,'' as service_user_name,'80691224' as work_no,'王少端' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '105886' as customer_no,'' as service_user_work_no,'' as service_user_name,'80691224' as work_no,'王少端' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '125064' as customer_no,'80691224' as service_user_work_no,'王少端' as service_user_name,'80960666' as work_no,'冯桂华' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '103372' as customer_no,'81129243' as service_user_work_no,'汪敏禄' as service_user_name,'81089088' as work_no,'陈先贵' as sales_name,'否' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '101653' as customer_no,'81129243' as service_user_work_no,'汪敏禄' as service_user_name,'81089088' as work_no,'陈先贵' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '102565' as customer_no,'81129243' as service_user_work_no,'汪敏禄' as service_user_name,'80960666' as work_no,'冯桂华' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '104547' as customer_no,'81129243' as service_user_work_no,'汪敏禄' as service_user_name,'81089088' as work_no,'陈先贵' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '120994' as customer_no,'81129243' as service_user_work_no,'汪敏禄' as service_user_name,'81089088' as work_no,'陈先贵' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '117006' as customer_no,'81129243' as service_user_work_no,'汪敏禄' as service_user_name,'81089088' as work_no,'陈先贵' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '122681' as customer_no,'81129243' as service_user_work_no,'汪敏禄' as service_user_name,'81089088' as work_no,'陈先贵' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '113840' as customer_no,'81129243' as service_user_work_no,'汪敏禄' as service_user_name,'81089088' as work_no,'陈先贵' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '106183' as customer_no,'81129243' as service_user_work_no,'汪敏禄' as service_user_name,'81089088' as work_no,'陈先贵' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '117012' as customer_no,'81129243' as service_user_work_no,'汪敏禄' as service_user_name,'81089088' as work_no,'陈先贵' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '118608' as customer_no,'81129243' as service_user_work_no,'汪敏禄' as service_user_name,'81089088' as work_no,'陈先贵' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '111860' as customer_no,'81129243' as service_user_work_no,'汪敏禄' as service_user_name,'80960666' as work_no,'冯桂华' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '110667' as customer_no,'81129243' as service_user_work_no,'汪敏禄' as service_user_name,'81089088' as work_no,'陈先贵' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '111844' as customer_no,'81129243' as service_user_work_no,'汪敏禄' as service_user_name,'80960666' as work_no,'冯桂华' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '118752' as customer_no,'81129243' as service_user_work_no,'汪敏禄' as service_user_name,'80960666' as work_no,'冯桂华' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '113548' as customer_no,'81129243' as service_user_work_no,'汪敏禄' as service_user_name,'81089088' as work_no,'陈先贵' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '113615' as customer_no,'81129243' as service_user_work_no,'汪敏禄' as service_user_name,'80960666' as work_no,'冯桂华' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '117299' as customer_no,'81129243' as service_user_work_no,'汪敏禄' as service_user_name,'81089088' as work_no,'陈先贵' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '117676' as customer_no,'81129243' as service_user_work_no,'汪敏禄' as service_user_name,'80960666' as work_no,'冯桂华' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '121357' as customer_no,'81129243' as service_user_work_no,'汪敏禄' as service_user_name,'81089088' as work_no,'陈先贵' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '121388' as customer_no,'81129243' as service_user_work_no,'汪敏禄' as service_user_name,'81089088' as work_no,'陈先贵' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '106265' as customer_no,'81098902' as service_user_work_no,'刘雪雪' as service_user_name,'81026931' as work_no,'林圳' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '113001' as customer_no,'81098902' as service_user_work_no,'刘雪雪' as service_user_name,'81026931' as work_no,'林圳' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '116914' as customer_no,'81098902' as service_user_work_no,'刘雪雪' as service_user_name,'81026931' as work_no,'林圳' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '117736' as customer_no,'81098902' as service_user_work_no,'刘雪雪' as service_user_name,'81026931' as work_no,'林圳' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '100563' as customer_no,'' as service_user_work_no,'' as service_user_name,'81099343' as work_no,'林瑾鑫' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select 'PF0094' as customer_no,'' as service_user_work_no,'' as service_user_name,'81099343' as work_no,'林瑾鑫' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '104612' as customer_no,'' as service_user_work_no,'' as service_user_name,'81099343' as work_no,'林瑾鑫' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '105882' as customer_no,'' as service_user_work_no,'' as service_user_name,'81099343' as work_no,'林瑾鑫' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '108739' as customer_no,'' as service_user_work_no,'' as service_user_name,'81099343' as work_no,'林瑾鑫' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '109377' as customer_no,'' as service_user_work_no,'' as service_user_name,'81099343' as work_no,'林瑾鑫' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '103855' as customer_no,'' as service_user_work_no,'' as service_user_name,'81099343' as work_no,'林瑾鑫' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '108127' as customer_no,'' as service_user_work_no,'' as service_user_name,'81133021' as work_no,'黄升' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '105150' as customer_no,'' as service_user_work_no,'' as service_user_name,'81133021' as work_no,'黄升' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '105182' as customer_no,'' as service_user_work_no,'' as service_user_name,'81133021' as work_no,'黄升' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '106721' as customer_no,'' as service_user_work_no,'' as service_user_name,'81133021' as work_no,'黄升' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '105355' as customer_no,'' as service_user_work_no,'' as service_user_name,'80972915' as work_no,'陈伟豪' as sales_name,'是' as is_part_time_service_manager,0.3 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate

; --5


--客户应收金额、逾期金额
drop table csx_tmp.tc_cust_overdue_0;
create table csx_tmp.tc_cust_overdue_0
as
select
	sdt,
	--customer_code as customer_no,
	customer_no,
	customer_name,company_code,company_name,channel_code,channel_name,payment_terms,payment_days,payment_name,receivable_amount,overdue_amount,max_overdue_day,
	overdue_coefficient_numerator, -- 逾期金额*逾期天数 计算因子，用于计算逾期系数分子
	overdue_coefficient_denominator -- 应收金额*账期天数 计算因子，用于计算逾期系数分母
from
	--csx_dw.dws_sss_r_d_customer_settle_detail
	csx_dw.dws_sss_r_a_customer_company_accounts
where
	sdt=${hiveconf:month_end_day}
	--202201月签呈，剔除逾期，2022年1-4月
	and customer_no not in ('119131','107404')
	--202202月签呈，剔除逾期，每月
	and customer_no not in ('104192','123395','117927','119925')
	--202202月签呈，客户已转代理人，剔除逾期，每月
	and customer_no not in ('122221','123086')
	--202202月签呈，剔除逾期，每月
	and customer_no not in ('116661','103369','104817','105601','104381','105304','105714','118794','120595','120830','122837','116932','119209','119022','119214','119257',
	'113425','112129')
	--202202月签呈，剔除逾期，当月
	--and customer_no not in ('116727','113439','105502','114576','114269','116721','119965','114945','120618','125613','124379','125247','125256','124025','124667','124621',
	--'124370','125469','123599','124782')
	--202202月签呈，公司BBC客户，不算提成，每月
	and customer_no not in ('123623')
	--202202月签呈，客户地采产品较多，不算提成，当月
	--and customer_no not in ('102866')
	--202202月签呈，每月
	and customer_no not in ('106626','112779','113122','124976','119977','102754')
	--202202月签呈，剔除逾期，每月
	and customer_no not in ('106526','120317','119906','108127','105838','105806')
	--202202月签呈，2022年1-6月
	and customer_no not in ('104086','116313','105249','106721','115204')
	--202202月签呈，2022年1-12月
	and customer_no not in ('106721','105182','105181','118103')
; 

--逾期客户信息
drop table csx_tmp.tc_cust_overdue_info;
create table csx_tmp.tc_cust_overdue_info
as
select
	a.channel_code,
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
	case when a.receivable_amount>=0 then a.receivable_amount else 0 end as receivable_amount,	-- 应收金额
	case when a.overdue_amount>=0 and a.receivable_amount>0 then a.overdue_amount else 0 end as overdue_amount,	-- 逾期金额
	case when overdue_coefficient_numerator>=0 and receivable_amount>0 
		then overdue_coefficient_numerator else 0 end as overdue_coefficient_numerator, -- 逾期金额*逾期天数 计算因子，用于计算逾期系数分子
	case when overdue_coefficient_denominator>=0 and receivable_amount>0 
		then overdue_coefficient_denominator else 0 end as overdue_coefficient_denominator, -- 应收金额*账期天数 计算因子，用于计算逾期系数分母
	coalesce(round(case when coalesce(case when a.receivable_amount>=0 then a.receivable_amount else 0 end, 0) <= 1 then 0  
		else coalesce(case when overdue_coefficient_numerator>=0 and a.receivable_amount>0 then overdue_coefficient_numerator else 0 end, 0)
		/(case when overdue_coefficient_denominator>=0 and a.receivable_amount>0 then overdue_coefficient_denominator else 0 end) end, 6),0) as over_rate -- 逾期系数		
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
			and customer_no not in('104698','112875','120939','121039','121625','124473','124498','125161','125898'
				)
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

-- 查询结果集
--计算逾期系数
insert overwrite directory '/tmp/zhangyanpeng/yuqi_dakehu' row format delimited fields terminated by '\t'	
select * from csx_tmp.tc_cust_overdue_info
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
			and customer_no not in('104698','112875','120939','121039','121625','124473','124498','125161','125898'
			)
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
			and customer_no not in('104698','112875','120939','121039','121625','124473','124498','125161','125898'
			)
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

--查询城市服务商2.0客户,按库存DC

--select distinct inventory_dc_code from csx_ods.source_csms_w_a_yszx_town_service_provider_config; -- W0AW、W0BY、W0K7、W0L4；W0AW、W0K7

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
--		sdt>='20220201'
--		and sdt<='20220228'
--		and channel_code in('1','7','9')
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
--			sdt>='20220201'
--			and sdt<='20220228'
--			and channel_code in('1','7','9')
--			and dc_code in('W0AW','W0K7')
--		) b on b.customer_no=a.customer_no
--	left join 
--		(
--		select 
--			distinct customer_no,customer_name,work_no,sales_name,sales_province_name
--		from 
--			csx_dw.dws_crm_w_a_customer 
--		where 
--			sdt='20220228'
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
--		sdt>='20220301'
--		and sdt<='20220331'
--		and channel_code in('1','7','9')
--		and business_type_code in ('4')
--	group by 
--		province_name,customer_no,customer_name,business_type_name
--	)a 
--	--淮南名单：81107924 陈治强  81034712 董冬燕，除了淮南都按大客户
--	join 
--		(
--		select 
--			customer_no,customer_name,work_no,sales_name,sales_province_name
--		from 
--			csx_dw.dws_crm_w_a_customer 
--		where 
--			sdt='20220331'
--			and sales_province_name='安徽省'
--			and work_no not in ('81107924','81034712')
--		)c on c.customer_no=a.customer_no
--group by 
--	a.customer_no
--;


