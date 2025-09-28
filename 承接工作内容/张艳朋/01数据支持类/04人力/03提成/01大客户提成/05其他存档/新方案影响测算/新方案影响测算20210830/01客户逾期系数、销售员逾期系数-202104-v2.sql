
-- 昨日、昨日、昨日月1日
--select ${hiveconf:i_sdate_11},${hiveconf:i_sdate_12},${hiveconf:i_sdate_12},${hiveconf:i_sdate_11};
--set i_sdate_1 =date_sub(current_date,1);
--set i_sdate_11 =regexp_replace(date_sub(current_date,1),'-','');
--set i_sdate_12 =regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),0),'-','');

set i_sdate_1 =last_day(add_months(date_sub(current_date,1),-1));
set i_sdate_11 =regexp_replace(last_day(add_months(date_sub(current_date,1),-1)),'-','');	
set i_sdate_12 =regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-1),'-','');					

	
set i_sdate_1 ='2021-04-30';
set i_sdate_11 ='20210430';
set i_sdate_12 ='20210401';


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
	where sdt=${hiveconf:i_sdate_11} 
	)c on a.customer_no=c.customer_no;


--应收金额、逾期日期、逾期天数
--签呈，部分客户历史订单逾期剔除
drop table csx_tmp.tmp_cust_order_overdue_dtl;
create table csx_tmp.tmp_cust_order_overdue_dtl
as
select *
from csx_tmp.tmp_cust_order_overdue_dtl_0
where customer_no not in('105235','105557')
and customer_no not in('104901','115852','115156','105381','115520','115458')
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
where customer_no in('104901','115852','115156','105381','115520','115458')
and happen_date>'20210430';




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
	d.service_user_work_no,d.service_user_name,
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
    and customer_no not in('111506','105685','113744','116085','103369')
    and customer_no not in('114265','117412','116957')
	--4月签呈 每月处理：剔除逾期系数，每月剔除;剔除逾期系数，不算提成，每月处理
    and customer_no not in('112045','115393','112248','104817','105601','104381','105304','105714','116099','104445','108127')
    and customer_no not in('102844','117940')	
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
		customer_number,
		company_code,
		payment_terms account_period_code,
		case when payment_terms like 'Y%' then concat('月结',COALESCE(cast(payment_days as int),0)) 
			 else concat('票到',COALESCE(cast(payment_days as int),0)) end account_period_name,
		COALESCE(cast(payment_days as int),0) account_period_val
	from csx_dw.dws_crm_r_a_customer_account_day a
	where sdt='current'
	and customer_number<>''
	)c on (a.customer_no=c.customer_number and a.company_code=c.company_code)
--剔除业务代理与内购客户
join		 ----渠道编号-1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 7.企业购 8.其他 9.业务代理
  (
    select * from csx_dw.dws_crm_w_a_customer 
	--4月签呈 '118689'系统中为业务代理人，但需要人为计算销售员大客户提成,每月处理
    where sdt=${hiveconf:i_sdate_11} and (channel_code in('1','7','8') or customer_no='118689') and (customer_name not like '%内%购%' and customer_name not like '%临保%')	
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
  )e on e.customer_no=a.customer_no
--关联服务管家
left join		
  (  
      select customer_no,
	 concat_ws(',', collect_list(service_user_work_no)) as service_user_work_no,
	 concat_ws(',', collect_list(service_user_name)) as service_user_name
	from 
	  (select distinct customer_no,service_user_work_no,service_user_name
	  from csx_dw.dws_crm_w_a_customer_sales_link 
      where sdt=${hiveconf:i_sdate_11} 
	  and customer_no not in('113672','118249','118439','113783','115575','117811','118174','114948','115715','118126','114154',
          '111265','112670','112808','113784','115535','115706','116170','118032','111000','117945','115102',
          '117249','115042','114799','114295','111647','114652','116071','116683','118595','118815','113544',
          '111135','118802','113151','117317','109461','113873','113918','113935','113940','111999','112016',
          '113666','112747','114054','114083','114085','115205','115909','116857','116858','116861','115656',
          '117244','115826','115602','104281','107398','103830','104035','104036','105638','105947','105975',
          '106000','106875','113443','113576','113785','113979','115287','116785','118117','107877','110575',
          '111038','111204','111952','113450','113455','113569','113588','115244','115657','115936','117015',
          '115906','109401','110696','113536','113583','117680','104034','111195','112302','113652','114516',
          '118102','103775','108105','108425','109544','112088','113659','115831','117225','117516','106921',
          '112663','114830','115215','105915','107852','108201','110898','111943','113249','113860','116169',
          '116650','118498','115308','117145','108283','109722','113082','114680','115881','107901','114099',
          '115051','116821','118461','109406','115047','115753','118379')
	  ----4月签呈，将以下客户的服务管家调整为xx
union all	  
select '113672' customer_no,'签呈,未知' service_user_work_no,'潘阳' service_user_name
union all
select '118249' customer_no,'签呈,未知' service_user_work_no,'潘阳' service_user_name
union all
select '118439' customer_no,'签呈,未知' service_user_work_no,'潘阳' service_user_name
union all
select '113783' customer_no,'签呈,未知' service_user_work_no,'潘阳' service_user_name
union all
select '115575' customer_no,'签呈,未知' service_user_work_no,'潘阳' service_user_name
union all
select '117811' customer_no,'签呈,未知' service_user_work_no,'潘阳' service_user_name
union all
select '118174' customer_no,'签呈,未知' service_user_work_no,'潘阳' service_user_name
union all
select '114948' customer_no,'签呈,未知' service_user_work_no,'潘阳' service_user_name
union all
select '115715' customer_no,'签呈,未知' service_user_work_no,'潘阳' service_user_name
union all
select '118126' customer_no,'签呈,未知' service_user_work_no,'潘阳' service_user_name
union all
select '114154' customer_no,'签呈,未知' service_user_work_no,'潘阳' service_user_name
union all
select '111265' customer_no,'签呈,未知' service_user_work_no,'潘阳' service_user_name
union all
select '112670' customer_no,'签呈,未知' service_user_work_no,'潘阳' service_user_name
union all
select '112808' customer_no,'签呈,未知' service_user_work_no,'潘阳' service_user_name
union all
select '113784' customer_no,'签呈,未知' service_user_work_no,'潘阳' service_user_name
union all
select '115535' customer_no,'签呈,未知' service_user_work_no,'潘阳' service_user_name
union all
select '115706' customer_no,'签呈,未知' service_user_work_no,'潘阳' service_user_name
union all
select '116170' customer_no,'签呈,未知' service_user_work_no,'潘阳' service_user_name
union all
select '118032' customer_no,'签呈,未知' service_user_work_no,'潘阳' service_user_name
union all
select '111000' customer_no,'签呈,未知' service_user_work_no,'潘阳' service_user_name
union all
select '117945' customer_no,'签呈,未知' service_user_work_no,'潘阳' service_user_name
union all
select '115102' customer_no,'签呈,未知' service_user_work_no,'唐楠' service_user_name
union all
select '117249' customer_no,'签呈,未知' service_user_work_no,'唐楠' service_user_name
union all
select '115042' customer_no,'签呈,未知' service_user_work_no,'唐楠' service_user_name
union all
select '114799' customer_no,'签呈,未知' service_user_work_no,'唐楠' service_user_name
union all
select '114295' customer_no,'签呈,未知' service_user_work_no,'王媛' service_user_name
union all
select '111647' customer_no,'签呈,未知' service_user_work_no,'王媛' service_user_name
union all
select '114652' customer_no,'签呈,未知' service_user_work_no,'王媛' service_user_name
union all
select '116071' customer_no,'签呈,未知' service_user_work_no,'王媛' service_user_name
union all
select '116683' customer_no,'签呈,未知' service_user_work_no,'王媛' service_user_name
union all
select '118595' customer_no,'签呈,未知' service_user_work_no,'王媛' service_user_name
union all
select '118815' customer_no,'签呈,未知' service_user_work_no,'王媛' service_user_name
union all
select '113544' customer_no,'签呈,未知' service_user_work_no,'王媛' service_user_name
union all
select '111135' customer_no,'签呈,未知' service_user_work_no,'王媛' service_user_name
union all
select '118802' customer_no,'签呈,未知' service_user_work_no,'王媛' service_user_name
union all
select '113151' customer_no,'签呈,未知' service_user_work_no,'王媛' service_user_name
union all
select '117317' customer_no,'签呈,未知' service_user_work_no,'王媛' service_user_name
union all
select '109461' customer_no,'签呈,未知' service_user_work_no,'王媛，唐楠' service_user_name
union all
select '113873' customer_no,'签呈,未知' service_user_work_no,'郭志江' service_user_name
union all
select '113918' customer_no,'签呈,未知' service_user_work_no,'郭志江' service_user_name
union all
select '113935' customer_no,'签呈,未知' service_user_work_no,'郭志江' service_user_name
union all
select '113940' customer_no,'签呈,未知' service_user_work_no,'郭志江' service_user_name
union all
select '111999' customer_no,'81088296' service_user_work_no,'陈慧燕' service_user_name
union all
select '112016' customer_no,'81088296' service_user_work_no,'陈慧燕' service_user_name
union all
select '113666' customer_no,'81088296' service_user_work_no,'陈慧燕' service_user_name
union all
select '112747' customer_no,'81088296' service_user_work_no,'陈慧燕' service_user_name
union all
select '114054' customer_no,'81088296' service_user_work_no,'陈慧燕' service_user_name
union all
select '114083' customer_no,'81088296' service_user_work_no,'陈慧燕' service_user_name
union all
select '114085' customer_no,'81088296' service_user_work_no,'陈慧燕' service_user_name
union all
select '115205' customer_no,'81088296' service_user_work_no,'陈慧燕' service_user_name
union all
select '115909' customer_no,'XM000001' service_user_work_no,'彭东京' service_user_name
union all
select '116857' customer_no,'XM000001' service_user_work_no,'彭东京' service_user_name
union all
select '116858' customer_no,'XM000001' service_user_work_no,'彭东京' service_user_name
union all
select '116861' customer_no,'XM000001' service_user_work_no,'彭东京' service_user_name
union all
select '115656' customer_no,'80974184' service_user_work_no,'郭荔丽' service_user_name
union all
select '117244' customer_no,'80974184' service_user_work_no,'郭荔丽' service_user_name
union all
select '115826' customer_no,'80974184' service_user_work_no,'郭荔丽' service_user_name
union all
select '115602' customer_no,'80974184' service_user_work_no,'郭荔丽' service_user_name
union all
select '104281' customer_no,'80974184' service_user_work_no,'郭荔丽' service_user_name
union all
select '107398' customer_no,'80974184' service_user_work_no,'郭荔丽' service_user_name
union all
select '103830' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '104035' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '104036' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '105638' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '105947' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '105975' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '106000' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '106875' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '113443' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '113576' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '113785' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '113979' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '115287' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '116785' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '118117' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '107877' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '110575' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '111038' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '111204' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '111952' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '113450' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '113455' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '113569' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '113588' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '115244' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '115657' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '115936' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '117015' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '115906' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '109401' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '110696' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '113536' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '113583' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '117680' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '104034' customer_no,'81011095' service_user_work_no,'周巧丽' service_user_name
union all
select '111195' customer_no,'81011095' service_user_work_no,'周巧丽' service_user_name
union all
select '112302' customer_no,'81011095' service_user_work_no,'周巧丽' service_user_name
union all
select '113652' customer_no,'81011095' service_user_work_no,'周巧丽' service_user_name
union all
select '114516' customer_no,'81011095' service_user_work_no,'周巧丽' service_user_name
union all
select '118102' customer_no,'81011095' service_user_work_no,'周巧丽' service_user_name
union all
select '103775' customer_no,'81011095' service_user_work_no,'周巧丽' service_user_name
union all
select '108105' customer_no,'81011095' service_user_work_no,'周巧丽' service_user_name
union all
select '108425' customer_no,'81011095' service_user_work_no,'周巧丽' service_user_name
union all
select '109544' customer_no,'81011095' service_user_work_no,'周巧丽' service_user_name
union all
select '112088' customer_no,'81011095' service_user_work_no,'周巧丽' service_user_name
union all
select '113659' customer_no,'81011095' service_user_work_no,'周巧丽' service_user_name
union all
select '115831' customer_no,'81011095' service_user_work_no,'周巧丽' service_user_name
union all
select '117225' customer_no,'81011095' service_user_work_no,'周巧丽' service_user_name
union all
select '117516' customer_no,'81011095' service_user_work_no,'周巧丽' service_user_name
union all
select '106921' customer_no,'81011095' service_user_work_no,'周巧丽' service_user_name
union all
select '112663' customer_no,'81011095' service_user_work_no,'周巧丽' service_user_name
union all
select '114830' customer_no,'81011095' service_user_work_no,'周巧丽' service_user_name
union all
select '115215' customer_no,'81011095' service_user_work_no,'周巧丽' service_user_name
union all
select '105915' customer_no,'81011095' service_user_work_no,'周巧丽' service_user_name
union all
select '107852' customer_no,'81011095' service_user_work_no,'周巧丽' service_user_name
union all
select '108201' customer_no,'81011095' service_user_work_no,'周巧丽' service_user_name
union all
select '110898' customer_no,'81011095' service_user_work_no,'周巧丽' service_user_name
union all
select '111943' customer_no,'81011095' service_user_work_no,'周巧丽' service_user_name
union all
select '113249' customer_no,'81011095' service_user_work_no,'周巧丽' service_user_name
union all
select '113860' customer_no,'81011095' service_user_work_no,'周巧丽' service_user_name
union all
select '116169' customer_no,'81011095' service_user_work_no,'周巧丽' service_user_name
union all
select '116650' customer_no,'81011095' service_user_work_no,'周巧丽' service_user_name
union all
select '118498' customer_no,'81011095' service_user_work_no,'周巧丽' service_user_name
union all
select '115308' customer_no,'81011095' service_user_work_no,'周巧丽' service_user_name
union all
select '117145' customer_no,'81011095' service_user_work_no,'周巧丽' service_user_name
union all
select '108283' customer_no,'80832481' service_user_work_no,'廖锶尔' service_user_name
union all
select '109722' customer_no,'80832481' service_user_work_no,'廖锶尔' service_user_name
union all
select '113082' customer_no,'80832481' service_user_work_no,'廖锶尔' service_user_name
union all
select '114680' customer_no,'80832481' service_user_work_no,'廖锶尔' service_user_name
union all
select '115881' customer_no,'80832481' service_user_work_no,'廖锶尔' service_user_name
union all
select '107901' customer_no,'80832481' service_user_work_no,'廖锶尔' service_user_name
union all
select '114099' customer_no,'80832481' service_user_work_no,'廖锶尔' service_user_name
union all
select '115051' customer_no,'80832481' service_user_work_no,'廖锶尔' service_user_name
union all
select '116821' customer_no,'80832481' service_user_work_no,'廖锶尔' service_user_name
union all
select '118461' customer_no,'80832481' service_user_work_no,'廖锶尔' service_user_name
union all
select '109406' customer_no,'80832481' service_user_work_no,'廖锶尔' service_user_name
union all
select '115047' customer_no,'80832481' service_user_work_no,'廖锶尔' service_user_name
union all
select '115753' customer_no,'80832481' service_user_work_no,'廖锶尔' service_user_name
union all
select '118379' customer_no,'80832481' service_user_work_no,'廖锶尔' service_user_name  
	  )a
	group by customer_no
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
	and customer_no not in('112129')
	and customer_no not in('114904','115313','115314','115325','115326','115391')
	and customer_no not in('PF0065','112574','106782')  --'116957','116629','116215'仅3月
	and customer_no not in('115721','116877','116883','116015','116556','116826')
	and customer_no not in('103253','103284','103296','103297','103304','103306','103311','104818','104828','104829','104835',
                           '105113','106283','106284','106298','106299','106301','106306','106307','106308','106309','106320',
                           '106321','106325','106326','106330','104609')
	--4月签呈 当月剔除逾期系数;	剔除逾期系数，不算提成
    and customer_no not in('111506','105685','113744','116085','103369')
    and customer_no not in('114265','117412','116957')
	--4月签呈 每月处理：剔除逾期系数，每月剔除;剔除逾期系数，不算提成，每月处理
    and customer_no not in('112045','115393','112248','104817','105601','104381','105304','105714','116099','104445','108127')
    and customer_no not in('102844','117940')							   
	group by channel_name,customer_no,customer_name,company_code,company_name)a	
group by channel_name,customer_no,customer_name;



--销售员逾期系数
drop table csx_tmp.temp_salesname_over_rate;
create table csx_tmp.temp_salesname_over_rate
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
	and customer_no not in('112129')
	and customer_no not in('114904','115313','115314','115325','115326','115391')
	and customer_no not in('PF0065','112574','106782')  --'116957','116629','116215'仅3月
	and customer_no not in('115721','116877','116883','116015','116556','116826')
	and customer_no not in('103253','103284','103296','103297','103304','103306','103311','104818','104828','104829','104835',
                           '105113','106283','106284','106298','106299','106301','106306','106307','106308','106309','106320',
                           '106321','106325','106326','106330','104609')
	--4月签呈 当月剔除逾期系数;	剔除逾期系数，不算提成
    and customer_no not in('111506','105685','113744','116085','103369')
    and customer_no not in('114265','117412','116957')
	--4月签呈 每月处理：剔除逾期系数，每月剔除;剔除逾期系数，不算提成，每月处理
    and customer_no not in('112045','115393','112248','104817','105601','104381','105304','105714','116099','104445','108127')
    and customer_no not in('102844','117940')							   
	group by channel_name,customer_no,customer_name,company_code,company_name)a	
----渠道编号-1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 7.企业购 8.其他 9.业务代理
--剔除业务代理与内购客户
--4月签呈，将以下客户的销售员调整为xx 每月处理
left join
  (
    select customer_no,
	  case when customer_no in('113873','113918','113935','113940','115656','117244','115826','115602','104281','107398') then '签呈,未知'
	       when customer_no in('114054','109000','114083','114085','115909','115971') then '签呈,未知'
		   else work_no end as work_no,
	  case when customer_no in('113873','113918','113935','113940','115656','117244','115826','115602','104281','107398') then '虚拟AA'
	       when customer_no in('114054','109000','114083','114085','115909','115971') then '虚拟AA'
		   else sales_name end as sales_name
	--work_no,sales_name
	from csx_dw.dws_crm_w_a_customer 
    where sdt=${hiveconf:i_sdate_11} 
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
  )c on c.customer_no=a.customer_no
where c.customer_no is null	
group by a.channel_name,b.work_no,b.sales_name;


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
	and customer_no not in('112129')
	and customer_no not in('114904','115313','115314','115325','115326','115391')
	and customer_no not in('PF0065','112574','106782')  --'116957','116629','116215'仅3月
	and customer_no not in('115721','116877','116883','116015','116556','116826')
	and customer_no not in('103253','103284','103296','103297','103304','103306','103311','104818','104828','104829','104835',
                           '105113','106283','106284','106298','106299','106301','106306','106307','106308','106309','106320',
                           '106321','106325','106326','106330','104609')
	--4月签呈 当月剔除逾期系数;	剔除逾期系数，不算提成
    and customer_no not in('111506','105685','113744','116085','103369')
    and customer_no not in('114265','117412','116957')
	--4月签呈 每月处理：剔除逾期系数，每月剔除;剔除逾期系数，不算提成，每月处理
	and customer_no not in('112045','115393','112248','104817','105601','104381','105304','105714','116099','104445','108127')
    and customer_no not in('102844','117940')							   
	group by channel_name,customer_no,customer_name,company_code,company_name)a	
----渠道编号-1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 7.企业购 8.其他 9.业务代理
--剔除业务代理与内购客户
--4月签呈，将以下客户的销售员调整为xx 每月处理
left join
  (
    select customer_no,
	  case when customer_no in('113873','113918','113935','113940','115656','117244','115826','115602','104281','107398') then '签呈,未知'
	       when customer_no in('114054','109000','114083','114085','115909','115971') then '签呈,未知'
		   else work_no end as work_no,
	  case when customer_no in('113873','113918','113935','113940','115656','117244','115826','115602','104281','107398') then '虚拟AA'
	       when customer_no in('114054','109000','114083','114085','115909','115971') then '虚拟AA'
		   else sales_name end as sales_name		 
	--work_no,sales_name
	from csx_dw.dws_crm_w_a_customer 
    where sdt=${hiveconf:i_sdate_11} 
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
  )c on c.customer_no=a.customer_no
--关联服务管家
join		
  (  
      select customer_no,
	 concat_ws(',', collect_list(service_user_work_no)) as service_user_work_no,
	 concat_ws(',', collect_list(service_user_name)) as service_user_name
	from 
	  (select distinct customer_no,service_user_work_no,service_user_name
	  from csx_dw.dws_crm_w_a_customer_sales_link 
      where sdt=${hiveconf:i_sdate_11} 
	  and is_additional_info = 1 and service_user_id <> 0
	  and customer_no not in('113672','118249','118439','113783','115575','117811','118174','114948','115715','118126','114154',
          '111265','112670','112808','113784','115535','115706','116170','118032','111000','117945','115102',
          '117249','115042','114799','114295','111647','114652','116071','116683','118595','118815','113544',
          '111135','118802','113151','117317','109461','113873','113918','113935','113940','111999','112016',
          '113666','112747','114054','114083','114085','115205','115909','116857','116858','116861','115656',
          '117244','115826','115602','104281','107398','103830','104035','104036','105638','105947','105975',
          '106000','106875','113443','113576','113785','113979','115287','116785','118117','107877','110575',
          '111038','111204','111952','113450','113455','113569','113588','115244','115657','115936','117015',
          '115906','109401','110696','113536','113583','117680','104034','111195','112302','113652','114516',
          '118102','103775','108105','108425','109544','112088','113659','115831','117225','117516','106921',
          '112663','114830','115215','105915','107852','108201','110898','111943','113249','113860','116169',
          '116650','118498','115308','117145','108283','109722','113082','114680','115881','107901','114099',
          '115051','116821','118461','109406','115047','115753','118379')
	  ----4月签呈，将以下客户的服务管家调整为xx
union all	  
select '113672' customer_no,'签呈,未知' service_user_work_no,'潘阳' service_user_name
union all
select '118249' customer_no,'签呈,未知' service_user_work_no,'潘阳' service_user_name
union all
select '118439' customer_no,'签呈,未知' service_user_work_no,'潘阳' service_user_name
union all
select '113783' customer_no,'签呈,未知' service_user_work_no,'潘阳' service_user_name
union all
select '115575' customer_no,'签呈,未知' service_user_work_no,'潘阳' service_user_name
union all
select '117811' customer_no,'签呈,未知' service_user_work_no,'潘阳' service_user_name
union all
select '118174' customer_no,'签呈,未知' service_user_work_no,'潘阳' service_user_name
union all
select '114948' customer_no,'签呈,未知' service_user_work_no,'潘阳' service_user_name
union all
select '115715' customer_no,'签呈,未知' service_user_work_no,'潘阳' service_user_name
union all
select '118126' customer_no,'签呈,未知' service_user_work_no,'潘阳' service_user_name
union all
select '114154' customer_no,'签呈,未知' service_user_work_no,'潘阳' service_user_name
union all
select '111265' customer_no,'签呈,未知' service_user_work_no,'潘阳' service_user_name
union all
select '112670' customer_no,'签呈,未知' service_user_work_no,'潘阳' service_user_name
union all
select '112808' customer_no,'签呈,未知' service_user_work_no,'潘阳' service_user_name
union all
select '113784' customer_no,'签呈,未知' service_user_work_no,'潘阳' service_user_name
union all
select '115535' customer_no,'签呈,未知' service_user_work_no,'潘阳' service_user_name
union all
select '115706' customer_no,'签呈,未知' service_user_work_no,'潘阳' service_user_name
union all
select '116170' customer_no,'签呈,未知' service_user_work_no,'潘阳' service_user_name
union all
select '118032' customer_no,'签呈,未知' service_user_work_no,'潘阳' service_user_name
union all
select '111000' customer_no,'签呈,未知' service_user_work_no,'潘阳' service_user_name
union all
select '117945' customer_no,'签呈,未知' service_user_work_no,'潘阳' service_user_name
union all
select '115102' customer_no,'签呈,未知' service_user_work_no,'唐楠' service_user_name
union all
select '117249' customer_no,'签呈,未知' service_user_work_no,'唐楠' service_user_name
union all
select '115042' customer_no,'签呈,未知' service_user_work_no,'唐楠' service_user_name
union all
select '114799' customer_no,'签呈,未知' service_user_work_no,'唐楠' service_user_name
union all
select '114295' customer_no,'签呈,未知' service_user_work_no,'王媛' service_user_name
union all
select '111647' customer_no,'签呈,未知' service_user_work_no,'王媛' service_user_name
union all
select '114652' customer_no,'签呈,未知' service_user_work_no,'王媛' service_user_name
union all
select '116071' customer_no,'签呈,未知' service_user_work_no,'王媛' service_user_name
union all
select '116683' customer_no,'签呈,未知' service_user_work_no,'王媛' service_user_name
union all
select '118595' customer_no,'签呈,未知' service_user_work_no,'王媛' service_user_name
union all
select '118815' customer_no,'签呈,未知' service_user_work_no,'王媛' service_user_name
union all
select '113544' customer_no,'签呈,未知' service_user_work_no,'王媛' service_user_name
union all
select '111135' customer_no,'签呈,未知' service_user_work_no,'王媛' service_user_name
union all
select '118802' customer_no,'签呈,未知' service_user_work_no,'王媛' service_user_name
union all
select '113151' customer_no,'签呈,未知' service_user_work_no,'王媛' service_user_name
union all
select '117317' customer_no,'签呈,未知' service_user_work_no,'王媛' service_user_name
union all
select '109461' customer_no,'签呈,未知' service_user_work_no,'王媛，唐楠' service_user_name
union all
select '113873' customer_no,'签呈,未知' service_user_work_no,'郭志江' service_user_name
union all
select '113918' customer_no,'签呈,未知' service_user_work_no,'郭志江' service_user_name
union all
select '113935' customer_no,'签呈,未知' service_user_work_no,'郭志江' service_user_name
union all
select '113940' customer_no,'签呈,未知' service_user_work_no,'郭志江' service_user_name
union all
select '111999' customer_no,'81088296' service_user_work_no,'陈慧燕' service_user_name
union all
select '112016' customer_no,'81088296' service_user_work_no,'陈慧燕' service_user_name
union all
select '113666' customer_no,'81088296' service_user_work_no,'陈慧燕' service_user_name
union all
select '112747' customer_no,'81088296' service_user_work_no,'陈慧燕' service_user_name
union all
select '114054' customer_no,'81088296' service_user_work_no,'陈慧燕' service_user_name
union all
select '114083' customer_no,'81088296' service_user_work_no,'陈慧燕' service_user_name
union all
select '114085' customer_no,'81088296' service_user_work_no,'陈慧燕' service_user_name
union all
select '115205' customer_no,'81088296' service_user_work_no,'陈慧燕' service_user_name
union all
select '115909' customer_no,'XM000001' service_user_work_no,'彭东京' service_user_name
union all
select '116857' customer_no,'XM000001' service_user_work_no,'彭东京' service_user_name
union all
select '116858' customer_no,'XM000001' service_user_work_no,'彭东京' service_user_name
union all
select '116861' customer_no,'XM000001' service_user_work_no,'彭东京' service_user_name
union all
select '115656' customer_no,'80974184' service_user_work_no,'郭荔丽' service_user_name
union all
select '117244' customer_no,'80974184' service_user_work_no,'郭荔丽' service_user_name
union all
select '115826' customer_no,'80974184' service_user_work_no,'郭荔丽' service_user_name
union all
select '115602' customer_no,'80974184' service_user_work_no,'郭荔丽' service_user_name
union all
select '104281' customer_no,'80974184' service_user_work_no,'郭荔丽' service_user_name
union all
select '107398' customer_no,'80974184' service_user_work_no,'郭荔丽' service_user_name
union all
select '103830' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '104035' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '104036' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '105638' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '105947' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '105975' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '106000' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '106875' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '113443' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '113576' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '113785' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '113979' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '115287' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '116785' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '118117' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '107877' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '110575' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '111038' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '111204' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '111952' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '113450' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '113455' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '113569' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '113588' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '115244' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '115657' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '115936' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '117015' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '115906' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '109401' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '110696' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '113536' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '113583' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '117680' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '104034' customer_no,'81011095' service_user_work_no,'周巧丽' service_user_name
union all
select '111195' customer_no,'81011095' service_user_work_no,'周巧丽' service_user_name
union all
select '112302' customer_no,'81011095' service_user_work_no,'周巧丽' service_user_name
union all
select '113652' customer_no,'81011095' service_user_work_no,'周巧丽' service_user_name
union all
select '114516' customer_no,'81011095' service_user_work_no,'周巧丽' service_user_name
union all
select '118102' customer_no,'81011095' service_user_work_no,'周巧丽' service_user_name
union all
select '103775' customer_no,'81011095' service_user_work_no,'周巧丽' service_user_name
union all
select '108105' customer_no,'81011095' service_user_work_no,'周巧丽' service_user_name
union all
select '108425' customer_no,'81011095' service_user_work_no,'周巧丽' service_user_name
union all
select '109544' customer_no,'81011095' service_user_work_no,'周巧丽' service_user_name
union all
select '112088' customer_no,'81011095' service_user_work_no,'周巧丽' service_user_name
union all
select '113659' customer_no,'81011095' service_user_work_no,'周巧丽' service_user_name
union all
select '115831' customer_no,'81011095' service_user_work_no,'周巧丽' service_user_name
union all
select '117225' customer_no,'81011095' service_user_work_no,'周巧丽' service_user_name
union all
select '117516' customer_no,'81011095' service_user_work_no,'周巧丽' service_user_name
union all
select '106921' customer_no,'81011095' service_user_work_no,'周巧丽' service_user_name
union all
select '112663' customer_no,'81011095' service_user_work_no,'周巧丽' service_user_name
union all
select '114830' customer_no,'81011095' service_user_work_no,'周巧丽' service_user_name
union all
select '115215' customer_no,'81011095' service_user_work_no,'周巧丽' service_user_name
union all
select '105915' customer_no,'81011095' service_user_work_no,'周巧丽' service_user_name
union all
select '107852' customer_no,'81011095' service_user_work_no,'周巧丽' service_user_name
union all
select '108201' customer_no,'81011095' service_user_work_no,'周巧丽' service_user_name
union all
select '110898' customer_no,'81011095' service_user_work_no,'周巧丽' service_user_name
union all
select '111943' customer_no,'81011095' service_user_work_no,'周巧丽' service_user_name
union all
select '113249' customer_no,'81011095' service_user_work_no,'周巧丽' service_user_name
union all
select '113860' customer_no,'81011095' service_user_work_no,'周巧丽' service_user_name
union all
select '116169' customer_no,'81011095' service_user_work_no,'周巧丽' service_user_name
union all
select '116650' customer_no,'81011095' service_user_work_no,'周巧丽' service_user_name
union all
select '118498' customer_no,'81011095' service_user_work_no,'周巧丽' service_user_name
union all
select '115308' customer_no,'81011095' service_user_work_no,'周巧丽' service_user_name
union all
select '117145' customer_no,'81011095' service_user_work_no,'周巧丽' service_user_name
union all
select '108283' customer_no,'80832481' service_user_work_no,'廖锶尔' service_user_name
union all
select '109722' customer_no,'80832481' service_user_work_no,'廖锶尔' service_user_name
union all
select '113082' customer_no,'80832481' service_user_work_no,'廖锶尔' service_user_name
union all
select '114680' customer_no,'80832481' service_user_work_no,'廖锶尔' service_user_name
union all
select '115881' customer_no,'80832481' service_user_work_no,'廖锶尔' service_user_name
union all
select '107901' customer_no,'80832481' service_user_work_no,'廖锶尔' service_user_name
union all
select '114099' customer_no,'80832481' service_user_work_no,'廖锶尔' service_user_name
union all
select '115051' customer_no,'80832481' service_user_work_no,'廖锶尔' service_user_name
union all
select '116821' customer_no,'80832481' service_user_work_no,'廖锶尔' service_user_name
union all
select '118461' customer_no,'80832481' service_user_work_no,'廖锶尔' service_user_name
union all
select '109406' customer_no,'80832481' service_user_work_no,'廖锶尔' service_user_name
union all
select '115047' customer_no,'80832481' service_user_work_no,'廖锶尔' service_user_name
union all
select '115753' customer_no,'80832481' service_user_work_no,'廖锶尔' service_user_name
union all
select '118379' customer_no,'80832481' service_user_work_no,'廖锶尔' service_user_name	  
	  )a
	group by customer_no
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
    where sdt=${hiveconf:i_sdate_11} and channel_code in('4','5','6') 
  )b on b.customer_no=a.customer_no  
left join
	(select
		customer_number,
		company_code,
		payment_terms account_period_code,
		case when payment_terms like 'Y%' then concat('月结',COALESCE(cast(payment_days as int),0)) 
			 else concat('票到',COALESCE(cast(payment_days as int),0)) end account_period_name,
		COALESCE(cast(payment_days as int),0) account_period_val
	from csx_dw.dws_crm_r_a_customer_account_day a
	where sdt='current'
	and customer_number<>''
	)c on (a.customer_no=c.customer_number and a.company_code=c.company_code)  
;


















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
		customer_number,
		company_code,
		payment_terms account_period_code,
		case when payment_terms like 'Y%' then concat('月结',COALESCE(cast(payment_days as int),0)) 
			 else concat('票到',COALESCE(cast(payment_days as int),0)) end account_period_name,
		COALESCE(cast(payment_days as int),0) account_period_val
	from csx_dw.dws_crm_r_a_customer_account_day a
	where sdt='current'
	and customer_number<>''
	)c on (a.customer_no=c.customer_number and a.company_code=c.company_code)
;


