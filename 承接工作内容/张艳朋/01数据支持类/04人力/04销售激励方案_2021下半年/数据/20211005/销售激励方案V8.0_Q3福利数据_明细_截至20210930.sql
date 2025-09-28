-- 由于标识错误 需剔除部分客户：'118849','118365','114646','118215','120645','115237','113387'
-- 销售主管激励案_Q3福利激励案_Q3福利毛利额 定价毛利额改为前端毛利额 20210906 
-- 贺仕文由销售员更改为主管

--==================================================================================================================================================================================
set current_start_day ='20210901';

set current_end_day ='20210930';

set last_month_start_day ='20210801';

set last_month_end_day ='20210831';


--===================================================================================================================================================================
-- 销售员明细

insert overwrite directory '/tmp/zhangyanpeng/20210929_13' row format delimited fields terminated by '\t'


	select
		substr(sdt,1,6) as smonth,province_name,customer_no,customer_name,work_no,sales_name,order_no,channel_name,
		supervisor_work_no,supervisor_name,
		business_type_name,
		sum(sales_value) as sales_value, --含税销售额
		sum(profit) as profit, --含税毛利额
		sum(front_profit) as front_profit, --前端含税毛利
		sum(profit)/abs(sum(sales_value)) as profit_rate,
		sum(front_profit)/abs(sum(sales_value)) as front_profit_rate
	from 
		csx_dw.dws_sale_r_d_detail
	where 
		sdt between '20210701' and ${hiveconf:current_end_day}
		and channel_code in ('1','7','9') 
		and (business_type_code in ('1','2','6') or (business_type_code in ('4') and supervisor_work_no in ('80886641','80972242')))
		--and supervisor_work_no !=''
		and customer_no not in ('118849','118365','114646','118215','120645','115237','113387') --剔除部分客户
		-- 20210918 剔除部分客户
		and customer_no not in ('117529','118693','118892','118897','118907','118911','118912','118913','118915','118918','119032','120123','121015','120704','119925','110931',
		'112735','118136','116205','117015','115936','109544','112088','108105','108201','110898','106306','106320','106330','106298','106325','117108','106321','106326','106283',
		'106299','106309','120024','119990','106805','118072') -- 20210918 剔除部分客户
		-- 20210923 剔除部分客户
		and customer_no not in ('121112','121109','120666','120360','110807','110807','110807','110807','110807','115971','120360','117255','117814','110807','115643')
		and work_no in ('80939468','81018608','81048704','80929704','80890405','80894243','80948458','81123145','81018639','81099211','80980614','80948175','80980678','80887605',
		'81090020','81027765','80917566','80948172','80902387','80943162','80941145','80935770','80924363','81001273','80941188','80965415','80992518','81021464','80937132','80007454',
		'80963376','80913408','80939468','81018608','81048704','81123145','81122839','81119082','80937797','81001920')
	group by
		substr(sdt,1,6) ,province_name,customer_no,customer_name,work_no,sales_name,order_no,channel_name,
		supervisor_work_no,supervisor_name,
		business_type_name
;
		
--===================================================================================================================================================================
-- 销售主管明细

insert overwrite directory '/tmp/zhangyanpeng/20210929_14' row format delimited fields terminated by '\t'


	select
		substr(sdt,1,6) as smonth,province_name,customer_no,customer_name,order_no,channel_name,
		supervisor_work_no,supervisor_name,
		business_type_name,
		sum(sales_value) as sales_value, --含税销售额
		sum(profit) as profit, --含税毛利额
		sum(front_profit) as front_profit, --前端含税毛利
		sum(profit)/abs(sum(sales_value)) as profit_rate,
		sum(front_profit)/abs(sum(sales_value)) as front_profit_rate
	from 
		csx_dw.dws_sale_r_d_detail
	where 
		sdt between '20210701' and ${hiveconf:current_end_day}
		and channel_code in ('1','7','9') 
		and (business_type_code in ('1','2','6') or (business_type_code in ('4') and supervisor_work_no in ('80886641','80972242')))
		--and supervisor_work_no !=''
		and customer_no not in ('118849','118365','114646','118215','120645','115237','113387') --剔除部分客户
		-- 20210918 剔除部分客户
		and customer_no not in ('117529','118693','118892','118897','118907','118911','118912','118913','118915','118918','119032','120123','121015','120704','119925','110931',
		'112735','118136','116205','117015','115936','109544','112088','108105','108201','110898','106306','106320','106330','106298','106325','117108','106321','106326','106283',
		'106299','106309','120024','119990','106805','118072') -- 20210918 剔除部分客户
		-- 20210923 剔除部分客户
		and customer_no not in ('121112','121109','120666','120360','110807','110807','110807','110807','110807','115971','120360','117255','117814','110807','115643')
		and supervisor_work_no in ('80946212','80816799','80768089','80946212','80768089','80764642')
	group by
		substr(sdt,1,6) ,province_name,customer_no,customer_name,order_no,channel_name,
		supervisor_work_no,supervisor_name,
		business_type_name
;


