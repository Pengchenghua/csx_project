
-- 月初 月末 年初
set month_start_day ='20220901';	
set month_end_day ='20220930';
set last_month_end_day='20220831';
set year_start_day ='20220101';		


--客户应收金额、逾期金额
drop table if exists csx_tmp.tc_cust_overdue_0;
create table csx_tmp.tc_cust_overdue_0
as
select
	a.sdt,
	--a.customer_code as customer_no,
	a.customer_no,
	a.customer_name,a.company_code,a.company_name,a.channel_code,a.channel_name,a.payment_terms,a.payment_days,a.payment_name,a.receivable_amount,a.overdue_amount,a.max_overdue_day,
	a.overdue_coefficient_numerator, -- 逾期金额*逾期天数 计算因子，用于计算逾期系数分子
	a.overdue_coefficient_denominator -- 应收金额*账期天数 计算因子，用于计算逾期系数分母
from	
	(
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
		--and customer_no not in ('119131','107404')
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
		--and customer_no not in ('104086','116313','105249','106721','115204')
		--202202月签呈，2022年1-12月
		and customer_no not in ('106721','105182','105181','118103')
		--202203月签 剔除逾期 每月
		and customer_no not in ('120314','120426','111120','105235','105557')
		--202203月签呈，剔除逾期 每月
		and customer_no not in ('120314','120426','104192','123395','117927','115393','119925','122495','115393','105696')
		--202203月签呈，剔除逾期 当月
		--and customer_no not in ('125837','123009','123162','123776','124250','124486','120417','125004','124667','124370','123599','125469','124379')
		--202203月签呈 不算提成和逾期 每月 
		and customer_no not in ('104192','123395','117927','126154')
		--202203月签呈 不算提成和逾期 当月 
		--and customer_no not in ('123859')
		--202203月签呈 剔除逾期 3-5月
		--and customer_no not in ('123096','123100','123068','123106','122987','108744')
		--202203月签呈 安徽省 剔除逾期 当月
		--and customer_no not in ('125686','125029','125028','124584','116727','105502','114576','114269','119965','120618')
		--202204月签呈 剔除逾期 当月
		--and customer_no not in ('121625','123244','124473','124217','125686','125029','124584','124784','125017','125028','108102','124068','120376')
		--202204月签呈 剔除逾期 每月
		and customer_no not in ('104596','121244','121248','121259','121274','121286','121305')
		--202204月签呈 剔除逾期 每月
		and customer_no not in ('112718','124486','120417')	
		--202204月签呈 不算提成和逾期 每月 
		and customer_no not in ('120459','121206','102524','111204','109377')	
		--202204月签呈 不算提成和逾期 当月 
		--and customer_no not in ('119729','124658')
		--202204月签呈 剔除逾期 期间 202204-202206
		--and customer_no not in ('125064')	
		--202205月签呈 剔除逾期 每月
		and customer_no not in ('105265','124811','123244','124473','124217','120102','122968')	
		--202205月签呈 剔除逾期 当月
		--and customer_no not in ('121625','125686','125029','124584','124784','125017','125028','108102','124068','127008','104901','119443','113477','119297','106124','118326',
		--'112954','106456','109474','126275','120295','114872','116052','126031','109092','105656')	
		--202205月签呈 剔除逾期 期间 202205-202207
		--and customer_no not in ('122985')	
		--202205月签呈 剔除逾期 当月
		--and customer_no not in ('103772')	
		--202205月签呈 剔除逾期 期间 202205-202206
		--and customer_no not in ('106721','105181','107404','122599','106330','106298','106309','106308','106306','106320','122155','120836','117108','106299','106321','106326',
		--'106307','106325','106283','124041','106284','106301','116668','116718','118836','119442','122901','123827','115803')
		--202205月签呈 剔除逾期 期间 202205-202301
		and customer_no not in ('102754')	
		--202205月签呈 剔除逾期 当月
		--and customer_no not in ('116099','107852','122703')	
		--202205月签呈 剔除逾期 当月
		--and customer_no not in ('108599','113980','109788','106585','105242','123599','125469','114494','123516')	
		--202206月签呈 剔除逾期 每月
		and customer_no not in ('106765','111696','115330','117376','125433','123811')	
		--202206月签呈 剔除逾期 当月
		--and customer_no not in ('116714')
		--202206月签呈 剔除逾期 期间 202206-202208
		and customer_no not in ('125064')	
		--202206月签呈 剔除逾期 期间 202206-202302
		and customer_no not in ('115204')
		--202206月签呈 剔除逾期 当月
		--and customer_no not in ('116955','106765','111696','115330','117376','125433','122874','121602','123811','123428','123543','125922','124085')	
		--202207月签呈 不算提成且剔除逾期 202207-202208 
		and customer_no not in ('116902','117705','122054','125137')
		--202207月签呈 剔除逾期 当月
		--and customer_no not in ('125686','119175','125992','105523','106765','124270','126266','107956','121015','121468','123158','114567','122322','124085','104086','118117','126280','114799','121457','125223','125654','121781','PF0500')		
	) a 
	--剔除业务代理与内购客户
	--join		
	--	(
	--	select 
	--		* 
	--	from 
	--		csx_dw.dws_crm_w_a_customer 
	--	where 
	--		sdt=${hiveconf:month_end_day} 
	--		and (channel_code in('1','7','8'))  ----渠道编号-1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 7.企业购 8.其他 9.业务代理
	--		and (customer_name not like '%内%购%' and customer_name not like '%临保%')	
	--	)b on b.customer_no=a.customer_no  
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
			--'127923' 城市服务商业务按大客户算 每月
			--福建省'126690' 城市服务商业务销售额*0.2% 不计算毛利提成 每月
			and customer_no not in('120939','125719','124193','113829','124351','121337'
				)
		)e on e.customer_no=a.customer_no
where
	e.customer_no is null
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

