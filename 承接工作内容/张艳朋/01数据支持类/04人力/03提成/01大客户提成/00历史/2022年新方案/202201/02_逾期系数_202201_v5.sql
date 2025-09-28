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
	and customer_no not in ('105502','105761','106346','107093','107408','109435','113439','114269','114576','114682','114945','115883','116237','116527','116721','116727',
	'116915','118458','118582','119441','119965','119992','120410','120618','121128','121288','121300','121703','122152','123240','123279','123365','124046','125290','105896',
	'122091','122171','105887','108637','115311','120847','121480','121488','122543','123342','123896','104623','105748','105862','105891','109369','111842','113695','116566',
	'117017','117090','121055','121856','121861','121866','121871','123225','123901','124668','105552','105839','106709','108461','111560','112915','114173','114968','116925',
	'117332','118449','120333','121233','124546','122089','122148','122164','122190','122758','122780','124533','107897','109456','117862','119760','120456','121100','121193',
	'120595','105774','107411','107530','108282','113487','113550','113731','113857','122147','122159','122167','122185','122186','122188'
	)
union all   select '105502' as customer_no,'' as service_user_work_no,'' as service_user_name,'80890405' as work_no,'瞿林峰' as sales_name,'是' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '105761' as customer_no,'' as service_user_work_no,'' as service_user_name,'81084752' as work_no,'张鹏' as sales_name,'是' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '106346' as customer_no,'' as service_user_work_no,'' as service_user_name,'81053313' as work_no,'段冬冬' as sales_name,'是' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '107093' as customer_no,'' as service_user_work_no,'' as service_user_name,'80160212' as work_no,'徐芸' as sales_name,'是' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '107408' as customer_no,'' as service_user_work_no,'' as service_user_name,'80890405' as work_no,'瞿林峰' as sales_name,'是' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '109435' as customer_no,'' as service_user_work_no,'' as service_user_name,'80160212' as work_no,'徐芸' as sales_name,'是' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '113439' as customer_no,'' as service_user_work_no,'' as service_user_name,'80890405' as work_no,'瞿林峰' as sales_name,'是' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '114269' as customer_no,'' as service_user_work_no,'' as service_user_name,'80890405' as work_no,'瞿林峰' as sales_name,'是' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '114576' as customer_no,'' as service_user_work_no,'' as service_user_name,'80890405' as work_no,'瞿林峰' as sales_name,'是' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '114682' as customer_no,'' as service_user_work_no,'' as service_user_name,'80890405' as work_no,'瞿林峰' as sales_name,'是' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '114945' as customer_no,'' as service_user_work_no,'' as service_user_name,'81053313' as work_no,'段冬冬' as sales_name,'是' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '115883' as customer_no,'' as service_user_work_no,'' as service_user_name,'80890405' as work_no,'瞿林峰' as sales_name,'是' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '116237' as customer_no,'' as service_user_work_no,'' as service_user_name,'80160212' as work_no,'徐芸' as sales_name,'是' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '116527' as customer_no,'' as service_user_work_no,'' as service_user_name,'80160212' as work_no,'徐芸' as sales_name,'是' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '116721' as customer_no,'' as service_user_work_no,'' as service_user_name,'80890405' as work_no,'瞿林峰' as sales_name,'是' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '116727' as customer_no,'' as service_user_work_no,'' as service_user_name,'80890405' as work_no,'瞿林峰' as sales_name,'是' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '116915' as customer_no,'' as service_user_work_no,'' as service_user_name,'80890405' as work_no,'瞿林峰' as sales_name,'是' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '118458' as customer_no,'' as service_user_work_no,'' as service_user_name,'81089337' as work_no,'葛香俊' as sales_name,'是' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '118582' as customer_no,'' as service_user_work_no,'' as service_user_name,'81084752' as work_no,'张鹏' as sales_name,'是' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '119441' as customer_no,'' as service_user_work_no,'' as service_user_name,'81084752' as work_no,'张鹏' as sales_name,'是' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '119965' as customer_no,'' as service_user_work_no,'' as service_user_name,'81053313' as work_no,'段冬冬' as sales_name,'是' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '119992' as customer_no,'' as service_user_work_no,'' as service_user_name,'81089337' as work_no,'葛香俊' as sales_name,'是' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '120410' as customer_no,'' as service_user_work_no,'' as service_user_name,'81084752' as work_no,'张鹏' as sales_name,'是' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '120618' as customer_no,'' as service_user_work_no,'' as service_user_name,'81053313' as work_no,'段冬冬' as sales_name,'是' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '121128' as customer_no,'' as service_user_work_no,'' as service_user_name,'81084752' as work_no,'张鹏' as sales_name,'是' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '121288' as customer_no,'' as service_user_work_no,'' as service_user_name,'81053313' as work_no,'段冬冬' as sales_name,'是' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '121300' as customer_no,'' as service_user_work_no,'' as service_user_name,'81089337' as work_no,'葛香俊' as sales_name,'是' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '121703' as customer_no,'' as service_user_work_no,'' as service_user_name,'81089337' as work_no,'葛香俊' as sales_name,'是' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '122152' as customer_no,'' as service_user_work_no,'' as service_user_name,'81089337' as work_no,'葛香俊' as sales_name,'是' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '123240' as customer_no,'' as service_user_work_no,'' as service_user_name,'81053313' as work_no,'段冬冬' as sales_name,'是' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '123279' as customer_no,'' as service_user_work_no,'' as service_user_name,'80160212' as work_no,'徐芸' as sales_name,'是' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '123365' as customer_no,'' as service_user_work_no,'' as service_user_name,'81084752' as work_no,'张鹏' as sales_name,'是' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '124046' as customer_no,'' as service_user_work_no,'' as service_user_name,'80160212' as work_no,'徐芸' as sales_name,'是' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '125290' as customer_no,'' as service_user_work_no,'' as service_user_name,'81089337' as work_no,'葛香俊' as sales_name,'是' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '105896' as customer_no,'81089337' as service_user_work_no,'葛香俊' as service_user_name,'80887605' as work_no,'朱祥如' as sales_name,'否' as is_part_time_service_manager,0.7 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '122091' as customer_no,'81089337' as service_user_work_no,'葛香俊' as service_user_name,'80887605' as work_no,'朱祥如' as sales_name,'否' as is_part_time_service_manager,0.7 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '122171' as customer_no,'81089337' as service_user_work_no,'葛香俊' as service_user_name,'80887605' as work_no,'朱祥如' as sales_name,'否' as is_part_time_service_manager,0.7 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '105887' as customer_no,'' as service_user_work_no,'' as service_user_name,'81080105' as work_no,'张元浩' as sales_name,'否' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '108637' as customer_no,'' as service_user_work_no,'' as service_user_name,'81080105' as work_no,'张元浩' as sales_name,'否' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '115311' as customer_no,'' as service_user_work_no,'' as service_user_name,'81080105' as work_no,'张元浩' as sales_name,'否' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '120847' as customer_no,'' as service_user_work_no,'' as service_user_name,'81080105' as work_no,'张元浩' as sales_name,'否' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '121480' as customer_no,'' as service_user_work_no,'' as service_user_name,'81080105' as work_no,'张元浩' as sales_name,'否' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '121488' as customer_no,'' as service_user_work_no,'' as service_user_name,'81080105' as work_no,'张元浩' as sales_name,'否' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '122543' as customer_no,'' as service_user_work_no,'' as service_user_name,'81080105' as work_no,'张元浩' as sales_name,'否' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '123342' as customer_no,'' as service_user_work_no,'' as service_user_name,'81080105' as work_no,'张元浩' as sales_name,'否' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '123896' as customer_no,'81052035' as service_user_work_no,'袁成臣' as service_user_name,'80913079' as work_no,'张宇' as sales_name,'否' as is_part_time_service_manager,0.7 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '104623' as customer_no,'81084752' as service_user_work_no,'张鹏' as service_user_name,'80913408' as work_no,'杨丹丹' as sales_name,'否' as is_part_time_service_manager,0.7 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '105748' as customer_no,'81084752' as service_user_work_no,'张鹏' as service_user_name,'80913408' as work_no,'杨丹丹' as sales_name,'否' as is_part_time_service_manager,0.7 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '105862' as customer_no,'81084752' as service_user_work_no,'张鹏' as service_user_name,'80913408' as work_no,'杨丹丹' as sales_name,'否' as is_part_time_service_manager,0.7 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '105891' as customer_no,'81084752' as service_user_work_no,'张鹏' as service_user_name,'80913408' as work_no,'杨丹丹' as sales_name,'否' as is_part_time_service_manager,0.7 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '109369' as customer_no,'81084752' as service_user_work_no,'张鹏' as service_user_name,'80913408' as work_no,'杨丹丹' as sales_name,'否' as is_part_time_service_manager,0.7 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '111842' as customer_no,'81084752' as service_user_work_no,'张鹏' as service_user_name,'80913408' as work_no,'杨丹丹' as sales_name,'否' as is_part_time_service_manager,0.7 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '113695' as customer_no,'81084752' as service_user_work_no,'张鹏' as service_user_name,'80913408' as work_no,'杨丹丹' as sales_name,'否' as is_part_time_service_manager,0.7 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '116566' as customer_no,'81084752' as service_user_work_no,'张鹏' as service_user_name,'80913408' as work_no,'杨丹丹' as sales_name,'否' as is_part_time_service_manager,0.7 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '117017' as customer_no,'81084752' as service_user_work_no,'张鹏' as service_user_name,'80913408' as work_no,'杨丹丹' as sales_name,'否' as is_part_time_service_manager,0.7 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '117090' as customer_no,'81084752' as service_user_work_no,'张鹏' as service_user_name,'80913408' as work_no,'杨丹丹' as sales_name,'否' as is_part_time_service_manager,0.7 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '121055' as customer_no,'81084752' as service_user_work_no,'张鹏' as service_user_name,'80913408' as work_no,'杨丹丹' as sales_name,'否' as is_part_time_service_manager,0.7 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '121856' as customer_no,'81084752' as service_user_work_no,'张鹏' as service_user_name,'80913408' as work_no,'杨丹丹' as sales_name,'否' as is_part_time_service_manager,0.7 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '121861' as customer_no,'81084752' as service_user_work_no,'张鹏' as service_user_name,'80913408' as work_no,'杨丹丹' as sales_name,'否' as is_part_time_service_manager,0.7 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '121866' as customer_no,'81084752' as service_user_work_no,'张鹏' as service_user_name,'80913408' as work_no,'杨丹丹' as sales_name,'否' as is_part_time_service_manager,0.7 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '121871' as customer_no,'81084752' as service_user_work_no,'张鹏' as service_user_name,'80913408' as work_no,'杨丹丹' as sales_name,'否' as is_part_time_service_manager,0.7 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '123225' as customer_no,'81084752' as service_user_work_no,'张鹏' as service_user_name,'80913408' as work_no,'杨丹丹' as sales_name,'否' as is_part_time_service_manager,0.7 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '123901' as customer_no,'81084752' as service_user_work_no,'张鹏' as service_user_name,'80913408' as work_no,'杨丹丹' as sales_name,'否' as is_part_time_service_manager,0.7 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '124668' as customer_no,'81084752' as service_user_work_no,'张鹏' as service_user_name,'80913408' as work_no,'杨丹丹' as sales_name,'否' as is_part_time_service_manager,0.7 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '105552' as customer_no,'' as service_user_work_no,'' as service_user_name,'80980678' as work_no,'谢建军' as sales_name,'否' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '105839' as customer_no,'' as service_user_work_no,'' as service_user_name,'80980678' as work_no,'谢建军' as sales_name,'否' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '106709' as customer_no,'' as service_user_work_no,'' as service_user_name,'80980678' as work_no,'谢建军' as sales_name,'否' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '108461' as customer_no,'' as service_user_work_no,'' as service_user_name,'80980678' as work_no,'谢建军' as sales_name,'否' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '111560' as customer_no,'' as service_user_work_no,'' as service_user_name,'80980678' as work_no,'谢建军' as sales_name,'否' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '112915' as customer_no,'' as service_user_work_no,'' as service_user_name,'80980678' as work_no,'谢建军' as sales_name,'否' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '114173' as customer_no,'' as service_user_work_no,'' as service_user_name,'80980678' as work_no,'谢建军' as sales_name,'否' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '114968' as customer_no,'' as service_user_work_no,'' as service_user_name,'80980678' as work_no,'谢建军' as sales_name,'否' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '116925' as customer_no,'' as service_user_work_no,'' as service_user_name,'80980678' as work_no,'谢建军' as sales_name,'否' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '117332' as customer_no,'' as service_user_work_no,'' as service_user_name,'80980678' as work_no,'谢建军' as sales_name,'否' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '118449' as customer_no,'' as service_user_work_no,'' as service_user_name,'80980678' as work_no,'谢建军' as sales_name,'否' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '120333' as customer_no,'' as service_user_work_no,'' as service_user_name,'80980678' as work_no,'谢建军' as sales_name,'否' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '121233' as customer_no,'' as service_user_work_no,'' as service_user_name,'80980678' as work_no,'谢建军' as sales_name,'否' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '124546' as customer_no,'' as service_user_work_no,'' as service_user_name,'80980678' as work_no,'谢建军' as sales_name,'否' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '122089' as customer_no,'' as service_user_work_no,'' as service_user_name,'81145749' as work_no,'刘一鸣' as sales_name,'否' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '122148' as customer_no,'' as service_user_work_no,'' as service_user_name,'81145749' as work_no,'刘一鸣' as sales_name,'否' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '122164' as customer_no,'' as service_user_work_no,'' as service_user_name,'81145749' as work_no,'刘一鸣' as sales_name,'否' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '122190' as customer_no,'' as service_user_work_no,'' as service_user_name,'81145749' as work_no,'刘一鸣' as sales_name,'否' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '122758' as customer_no,'' as service_user_work_no,'' as service_user_name,'81145749' as work_no,'刘一鸣' as sales_name,'否' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '122780' as customer_no,'' as service_user_work_no,'' as service_user_name,'81145749' as work_no,'刘一鸣' as sales_name,'否' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '124533' as customer_no,'' as service_user_work_no,'' as service_user_name,'81145749' as work_no,'刘一鸣' as sales_name,'否' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '107897' as customer_no,'' as service_user_work_no,'' as service_user_name,'80970937' as work_no,'高京庭' as sales_name,'否' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '109456' as customer_no,'' as service_user_work_no,'' as service_user_name,'80970937' as work_no,'高京庭' as sales_name,'否' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '117862' as customer_no,'' as service_user_work_no,'' as service_user_name,'80970937' as work_no,'高京庭' as sales_name,'否' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '119760' as customer_no,'' as service_user_work_no,'' as service_user_name,'80970937' as work_no,'高京庭' as sales_name,'否' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '120456' as customer_no,'' as service_user_work_no,'' as service_user_name,'80970937' as work_no,'高京庭' as sales_name,'否' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '121100' as customer_no,'' as service_user_work_no,'' as service_user_name,'80970937' as work_no,'高京庭' as sales_name,'否' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '121193' as customer_no,'' as service_user_work_no,'' as service_user_name,'80970937' as work_no,'高京庭' as sales_name,'否' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '120595' as customer_no,'81089336' as service_user_work_no,'陈静' as service_user_name,'81089336' as work_no,'陈静' as sales_name,'是' as is_part_time_service_manager,0.7 as salesperson_sales_value_fp_rate,0.5 as salesperson_profit_fp_rate,0.3 as service_user_sales_value_fp_rate,0.5 as service_user_profit_fp_rate
union all   select '105774' as customer_no,'' as service_user_work_no,'' as service_user_name,'80935770' as work_no,'陈海洋' as sales_name,'否' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '107411' as customer_no,'' as service_user_work_no,'' as service_user_name,'80935770' as work_no,'陈海洋' as sales_name,'否' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '107530' as customer_no,'' as service_user_work_no,'' as service_user_name,'80935770' as work_no,'陈海洋' as sales_name,'否' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '108282' as customer_no,'' as service_user_work_no,'' as service_user_name,'80935770' as work_no,'陈海洋' as sales_name,'否' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '113487' as customer_no,'' as service_user_work_no,'' as service_user_name,'80935770' as work_no,'陈海洋' as sales_name,'否' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '113550' as customer_no,'' as service_user_work_no,'' as service_user_name,'80935770' as work_no,'陈海洋' as sales_name,'否' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '113731' as customer_no,'' as service_user_work_no,'' as service_user_name,'80935770' as work_no,'陈海洋' as sales_name,'否' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '113857' as customer_no,'' as service_user_work_no,'' as service_user_name,'80935770' as work_no,'陈海洋' as sales_name,'否' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '122147' as customer_no,'' as service_user_work_no,'' as service_user_name,'80935770' as work_no,'陈海洋' as sales_name,'否' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '122159' as customer_no,'' as service_user_work_no,'' as service_user_name,'80935770' as work_no,'陈海洋' as sales_name,'否' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '122167' as customer_no,'' as service_user_work_no,'' as service_user_name,'80935770' as work_no,'陈海洋' as sales_name,'否' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '122185' as customer_no,'' as service_user_work_no,'' as service_user_name,'80935770' as work_no,'陈海洋' as sales_name,'否' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '122186' as customer_no,'' as service_user_work_no,'' as service_user_name,'80935770' as work_no,'陈海洋' as sales_name,'否' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate
union all   select '122188' as customer_no,'' as service_user_work_no,'' as service_user_name,'80935770' as work_no,'陈海洋' as sales_name,'否' as is_part_time_service_manager,1 as salesperson_sales_value_fp_rate,1 as salesperson_profit_fp_rate,0 as service_user_sales_value_fp_rate,0 as service_user_profit_fp_rate



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


