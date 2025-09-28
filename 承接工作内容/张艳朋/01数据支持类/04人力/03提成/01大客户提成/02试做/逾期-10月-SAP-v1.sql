--20200518将利润中心调整凭证号剔除
--20200525增加更新需剔除的利润中心调整凭证号
--20200529月结类客户账期+15天

--20200714 处理6月坏账签呈 平账 csx_dw.ads_fis_r_a_customer_bad_debt
----20200911 处理7-8月坏账签呈 平账 csx_tmp.salary_customer_bad_debt
CREATE TABLE `csx_tmp.salary_customer_bad_debt`
	(	
		province_name string comment '省区',
		`comp_code` string      comment '公司编码' ,
		`customer_no` string comment '客户编号',
		`customer_name` string comment '客户名称',		
		`budat` string        comment '过机日期' ,
		`dmbtr` decimal(26,2) comment '坏账金额' 
	)comment '客户坏账金额_平账'
	partitioned by (sdt string comment '分区日期')
row format delimited fields terminated by ','
stored as textfile;

--load data inpath '/tmp/raoyanhua/customer_bad_debt_7.csv' overwrite into table csx_tmp.salary_customer_bad_debt partition (sdt='20200731');
--select * from csx_tmp.salary_customer_bad_debt where sdt='20200731';

--load data inpath '/tmp/raoyanhua/customer_bad_debt_8.csv' overwrite into table csx_tmp.salary_customer_bad_debt partition (sdt='20200831');
--select * from csx_tmp.salary_customer_bad_debt where sdt='20200831';


--drop table csx_tmp.ads_fis_r_a_customer_days_overdue_dtl;
CREATE TABLE `csx_tmp.ads_fis_r_a_customer_days_overdue_dtl`
	(	
		`channel` string      comment '渠道编码' ,
		`channel_code` string comment '渠道名称',
		`subject_code` string        comment '科目代码' ,
		`subject_name` string comment '科目名称' ,
		`comp_code` string    comment '公司代码' ,
		`comp_name` string    comment '公司名称' ,
		`shop_id` string      comment '门店编码,暂时为空' ,
		`shop_name` string    comment '门店名称，暂时为空' ,
		`customer_no` string  comment '客户编码' ,
		`customer_name` string comment '客户名称' ,
		`zterm` string        comment '帐期类型' ,
		`payment_terms` string comment '帐期付款条件',		
		`payment_days` int    comment '帐期天数',
		`sdate` string        comment '凭证日期',
		`edate` string        comment '帐期结束日期',
		`over_days` int       comment '逾期天数，负数未逾期，正数逾期' ,
		`ac_all` decimal(26,6) comment '金额，负数为回款金额，正数应收金额' ,
		`over_rate` decimal(26,6) comment '逾期率' ,		
		write_time timestamp comment '插入时间'
	)comment '客户应收逾期明细'
	partitioned by (sdt string comment '分区日期，计算逾期天数')
	STORED AS parquet;	
	
	

	
-- 昨日、昨日月1日，上月1日，上月最后一日
--select ${hiveconf:i_sdate_11},${hiveconf:i_sdate_12},${hiveconf:i_sdate_22},${hiveconf:i_sdate_23};

set i_sdate_11 =regexp_replace(date_sub(current_date,1),'-','');
set i_sdate_12 =regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),0),'-','');
	
set i_sdate_22 =regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-1),'-','');					
set i_sdate_23 =regexp_replace(last_day(add_months(date_sub(current_date,1),-1)),'-','');	

set i_sdate_23 ='20201031';

--set mapreduce.job.queuename=caishixian;
--set hive.execution.engine=mr; 
set i_sdate                = '2020-10-31';
set i_date                 =date_add(${hiveconf:i_sdate},1);
drop table if exists csx_tmp.csx_hepecc_bsid
;

CREATE temporary  table if NOT EXISTS csx_tmp.csx_hepecc_bsid as
select
	a.hkont          ,
	a.bukrs comp_code,
	case
		when length(a.kunnr)<3
			then a.lifnr
			else a.kunnr
	end kunnr   ,
	a.budat     ,
	'A'as prctr    ,
--	shop_name,
	a.dmbtr     ,
	case
		when kunnr in ('V7126',
					   'V7127',
					   'V7128',
					   'V7129',
					   'V7130',
					   'V7131',
					   'V7132',
					   'V7000')
			then 'Y004'
		--补充	
		when lpad(a.kunnr,10,'0')='0000104402' and a.bukrs='2211' then 'Y004'
		when lpad(a.kunnr,10,'0')='0000105047' and a.bukrs='2300' then 'Z002'
		when lpad(a.kunnr,10,'0')='0000105131' and a.bukrs='2300' then 'Z002'
		when lpad(a.kunnr,10,'0')='0000105280' and a.bukrs='2300' then 'Z002'
		when lpad(a.kunnr,10,'0')='0000910001' and a.bukrs='2207' then 'Z002'
		when lpad(a.kunnr,10,'0')='0000910001' and a.bukrs='2211' then 'Z002'
		when lpad(a.kunnr,10,'0')='0000910001' and a.bukrs='2408' then 'Z002'		
		when lpad(a.kunnr,10,'0')='0000105953' and a.bukrs='2300' then 'Y003'
		when lpad(a.kunnr,10,'0')='0000106187' and a.bukrs='2300' then 'Y003'
		when lpad(a.kunnr,10,'0')='0000106782' and a.bukrs='2121' then 'Y003'
			else c.zterm
	end zterm,
	case
		when kunnr in ('V7126',
					   'V7127',
					   'V7128',
					   'V7129',
					   'V7130',
					   'V7131',
					   'V7132',
					   'V7000')
			then 45
		--补充
		when lpad(a.kunnr,10,'0')='0000104402' and a.bukrs='2211' then 45
		when lpad(a.kunnr,10,'0')='0000105047' and a.bukrs='2300' then 7
		when lpad(a.kunnr,10,'0')='0000105131' and a.bukrs='2300' then 7
		when lpad(a.kunnr,10,'0')='0000105280' and a.bukrs='2300' then 7
		when lpad(a.kunnr,10,'0')='0000910001' and a.bukrs='2207' then 7
		when lpad(a.kunnr,10,'0')='0000910001' and a.bukrs='2211' then 7
		when lpad(a.kunnr,10,'0')='0000910001' and a.bukrs='2408' then 7		
		when lpad(a.kunnr,10,'0')='0000105953' and a.bukrs='2300' then 31
		when lpad(a.kunnr,10,'0')='0000106187' and a.bukrs='2300' then 31
		when lpad(a.kunnr,10,'0')='0000106782' and a.bukrs='2121' then 31
		--when c.zterm='0' COALESCE(c.diff,0)
		--when c.zterm='Z007' then COALESCE(c.diff,0)
		--when c.zterm='Z001' then COALESCE(c.diff,0)
			else c.diff
	end diff ,
	concat(substr(a.budat,1,4),'-',substr(a.budat,5,2),'-',substr(a.budat,7,2)) sdate,
	case
		when kunnr in ('V7126',
					   'V7127',
					   'V7128',
					   'V7129',
					   'V7130',
					   'V7131',
					   'V7132',
					   'V7000')
			then date_add(last_day(concat(substr(a.budat,1,4),'-',substr(a.budat,5,2),'-',substr(a.budat,7,2))),45)
		when c.zterm like 'Y%'
			then date_add(last_day(concat(substr(a.budat,1,4),'-',substr(a.budat,5,2),'-',substr(a.budat,7,2))),COALESCE(c.diff,0))
			else date_add(concat(substr(a.budat,1,4),'-',substr(a.budat,5,2),'-',substr(a.budat,7,2)),COALESCE(c.diff,0))
	end edate
from
	(
		select *
		from
			ods_ecc.ecc_ytbcustomer
		where
			--sdt      =regexp_replace(${hiveconf:i_date},'-','')
			--and budat<regexp_replace(${hiveconf:i_date},'-','')
			sdt='20201105'and  budat<'20201101'
			and mandt='800'
			and
			(
				substr(hkont,1,3)<>'139'
				or
				(
					substr(hkont,1,3)='139'
					and budat       >='20190201'
				)
			)
			and 
			-- 剔除利润调整凭证 科目+年度+凭证号+公司代码
			concat_ws('-',hkont ,gjahr,belnr,bukrs) not in (	
			'1122010000-2020-0090526358-1933',
			'1122010000-2020-0090526357-1933',
			'1122010000-2020-0090446438-1933',
			'1122010000-2020-0090446437-1933',
			'1122010000-2020-0090446436-1933',
			'1122010000-2020-0101042210-2200',
			'1122010000-2020-0100794408-2121',
			'1122010000-2020-0100794407-2121',
			'1122010000-2020-0100698829-2121',
			'1122010000-2020-0100698828-2121',
			'1122010000-2020-0100698815-2121',
			'1122010000-2020-0100698814-2121',
			'1122010000-2020-0100698811-2121',
			'1122010000-2020-0100698810-2121',
			'1122010000-2020-0100698807-2121',
			'1122010000-2020-0100698806-2121',
			'1122010000-2020-0100599788-2202',
			'1122010000-2020-0100387789-2400',
			'1122010000-2020-0100384016-2300',
			'1122010000-2020-0100343582-2403',
			'1122010000-2020-0100343559-2403',
			'1122010000-2020-0100343558-2403',
			'1122010000-2020-0100339686-2402',
			'1122010000-2020-0100245041-2303',
			'1122010000-2020-0100154283-2700',
			'1122010000-2020-0100066952-2105',
			'1122010000-2020-0100004543-2800',
		 '1122010000-2020-0100183238-2700',
		 '1122010000-2020-0100404461-2402',
		 '1122010000-2020-0100467273-2400',
		 '1122010000-2020-0100468834-2300',
		 '1122010000-2020-0100755372-2202',
		 '1122010000-2020-0100873656-2121',
		 '1122010000-2020-0101263298-2200',
		 '1122010000-2020-0090572072-1933')
	)a
	left join
		(
			select
				customer_number,
				company_code,
				payment_terms zterm,
				cast(payment_days as int) diff
			from
				csx_dw.dws_crm_r_a_customer_account_day a
			where sdt=${hiveconf:i_sdate_23}
			and customer_number<>''
		)c
		on (lpad(a.kunnr,10,'0')=lpad(c.customer_number,10,'0') and a.bukrs=c.company_code)
--补充坏账信息-导入数据平账处理			
--union all	
--select '1122010000' hkont,
--	a.comp_code,
--	case when a.customer_no in('PF0319','PF0462') then a.customer_no else lpad(a.customer_no,10,'0')end kunnr,
--	a.budat,
--	'A'as prctr,
--	a.dmbtr,
--	c.zterm,
--	c.diff,
--	concat(substr(a.budat,1,4),'-',substr(a.budat,5,2),'-',substr(a.budat,7,2)) sdate,
--	case when c.zterm like 'Y%'
--			then date_add(last_day(concat(substr(a.budat,1,4),'-',substr(a.budat,5,2),'-',substr(a.budat,7,2))),COALESCE(c.diff,0))
--			else date_add(concat(substr(a.budat,1,4),'-',substr(a.budat,5,2),'-',substr(a.budat,7,2)),COALESCE(c.diff,0))
--	end edate
--from
--	(select *
--	from csx_tmp.salary_customer_bad_debt
--	where sdt='20200831'
--	)a
--	left join
--		(select
--			customer_number,
--			company_code,
--			payment_terms zterm,
--			cast(payment_days as int) diff
--		from csx_dw.dws_crm_r_a_customer_account_day a
--		where sdt=${hiveconf:i_sdate_23} and customer_number<>''
--		)c on(lpad(a.customer_no,10,'0')=lpad(c.customer_number,10,'0') and a.comp_code=c.company_code)						
;		



drop table csx_tmp.temp_account_out;

CREATE temporary table csx_tmp.temp_account_out as
select
	a.*,
	row_number() OVER(PARTITION BY hkont,comp_code,kunnr,prctr ORDER BY
					  budat asc)rno,
	sum(amount)over(PARTITION BY hkont,comp_code,kunnr,prctr order by
					budat asc ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING ) sum_sq,
	sum(amount)over(PARTITION BY hkont,comp_code,kunnr,prctr order by
					budat asc)sum_bq
from
	(
		select
			comp_code              ,
			kunnr                  ,
			hkont                  ,
			cast(budat as int)budat,
			prctr                  ,
		--	shop_name              ,
			sdate                  ,
			edate                  ,
			zterm                  ,
			diff                   ,
			sum(dmbtr)amount
		from
			csx_tmp.csx_hepecc_bsid a
		where
			dmbtr>=0
		group by
			comp_code,
			kunnr    ,
			hkont    ,
			budat    ,
			prctr    ,
		--	shop_name,
			sdate    ,
			edate    ,
			zterm    ,
			diff
	)
	a
;

drop table csx_tmp.temp_account_in
;

CREATE temporary table csx_tmp.temp_account_in as
select
	hkont    ,
	comp_code,
	kunnr    ,
	prctr    ,
	sum(dmbtr)amount
from
	csx_tmp.csx_hepecc_bsid a
where
	dmbtr<0
group by
	hkont    ,
	comp_code,
	kunnr    ,
	prctr
;

--已收账款不足应收账款
drop table csx_tmp.temp_account_left
;

CREATE temporary table csx_tmp.temp_account_left as
select
	a.comp_code,
	a.prctr    ,
--	a.shop_name,
	a.kunnr    ,
	a.hkont    ,
	a.budat    ,
	a.sdate    ,
	a.edate    ,
	zterm      ,
	diff       ,
	case
		when coalesce(a.sum_sq,0)+b.amount<0
			then a.sum_bq        +b.amount
			else a.amount
	end amount ,
	a.rno      ,
	a.sum_bq+b.amount amount_left
from
	csx_tmp.temp_account_out a
	join
		csx_tmp.temp_account_in b
		on
			(
				a.hkont        =b.hkont
				and a.comp_code=b.comp_code
				and a.kunnr    =b.kunnr
				and a.prctr    =b.prctr
			)
where
	a.sum_bq+b.amount>=0
--已收账款超过应收账款
union all
select
	a.comp_code              ,
	a.prctr                  ,
--	a.shop_name              ,
	a.kunnr                  ,
	a.hkont                  ,
	a.budat                  ,
	a.sdate                  ,
	a.edate                  ,
	zterm                    ,
	diff                     ,
	a.sum_bq+b.amount amount ,
	a.rno                    ,
	a.sum_bq+b.amount amount_left
from
	csx_tmp.temp_account_out a
	join
		(
			select
				hkont    ,
				comp_code,
				kunnr    ,
				prctr    ,
				max(rno)rno_max
			from
				csx_tmp.temp_account_out
			group by
				hkont    ,
				comp_code,
				kunnr    ,
				prctr
		)
		c
		on
			(
				a.hkont        =c.hkont
				and a.comp_code=c.comp_code
				and a.kunnr    =c.kunnr
				and a.rno      =c.rno_max
				and a.prctr    =c.prctr
			)
	join
		csx_tmp.temp_account_in b
		on
			(
				a.hkont        =b.hkont
				and a.comp_code=b.comp_code
				and a.kunnr    =b.kunnr
				and a.prctr    =b.prctr
			)
where
	a.sum_bq+b.amount<0
--只有应收没有收款
union all
select
	a.comp_code,
	a.prctr    ,
--	a.shop_name,
	a.kunnr    ,
	a.hkont    ,
	a.budat    ,
	a.sdate    ,
	a.edate    ,
	zterm      ,
	diff       ,
	a.amount   ,
	a.rno      ,
	a.sum_bq amount_left
from
	csx_tmp.temp_account_out a
	left join
		csx_tmp.temp_account_in b
		on
			(
				a.hkont        =b.hkont
				and a.comp_code=b.comp_code
				and a.kunnr    =b.kunnr
				and a.prctr    =b.prctr
			)
where
	b.amount is null
union all
--只有预付没有收款
select
	a.comp_code     ,
	a.prctr         ,
--	a.shop_name     ,
	a.kunnr         ,
	a.hkont         ,
	a.budat         ,
	a.sdate         ,
	a.edate         ,
	zterm           ,
	diff            ,
	a.amount amount ,
	null     rno    ,
	a.amount amount_left
from
	(
		select
			comp_code              ,
			kunnr                  ,
			hkont                  ,
			cast(budat as int)budat,
			prctr                  ,
		--	shop_name              ,
			sdate                  ,
			edate                  ,
			zterm                  ,
			diff                   ,
			sum(dmbtr)amount
		from
			csx_tmp.csx_hepecc_bsid a
		where
			dmbtr<0
		group by
			comp_code,
			kunnr    ,
			hkont    ,
			budat    ,
			prctr    ,
		--	shop_name,
			sdate    ,
			edate    ,
			zterm    ,
			diff
	)
	a
	left join
		(
			select
				hkont    ,
				comp_code,
				kunnr    ,
				prctr    ,
				sum(amount)amount
			from
				csx_tmp.temp_account_out
			group by
				hkont    ,
				comp_code,
				kunnr    ,
				prctr
		)
		c
		on
			(
				a.hkont        =c.hkont
				and a.comp_code=c.comp_code
				and a.kunnr    =c.kunnr
				and a.prctr    =c.prctr
			)
where
	c.amount is null
;


set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table csx_tmp.ads_fis_r_a_customer_days_overdue_dtl partition(sdt)
select
c.channel,c.channel_code         ,
	a.hkont     as subject_code    ,
	d.account_name  as subject_name,
	a.comp_code     ,
	b.comp_name     ,
	regexp_replace(a.prctr,'(^0*)','')  as shop_id       ,
	'' as shop_name     ,
	regexp_replace(a.kunnr ,'(^0*)','') customer_no        ,
	customer_name ,
	zterm           ,
	case
		when zterm like 'Y%'
			then concat('月结',diff)
			else concat('票到',diff)
	end payment_terms,
	diff as payment_days,	
	a.sdate,
	 a.edate,
	datediff(${hiveconf:i_sdate}, a.edate)as  over_days,
	sum	(amount)ac_all,
	'' over_rate,
	from_utc_timestamp(current_timestamp(),'GMT') write_time,
	regexp_replace(${hiveconf:i_sdate},'-','') sdt
from
	csx_tmp.temp_account_left a
join (select code as comp_code, name as comp_name from csx_dw.dws_basic_w_a_company_code where sdt = 'current')b on a.comp_code = b.comp_code
			left join
		(select customer_no,customer_name,channel,channel_code from csx_dw.dws_crm_w_a_customer_m_v1 where sdt=regexp_replace(${hiveconf:i_sdate},'-','') ) c
		on
			lpad(a.kunnr,10,'0')=lpad(c.customer_no,10,'0')
	left join
		csx_dw.sap_account_type d
		on
			a.hkont=d.accunt_code
	left join 
	(select shop_name,location_code from csx_dw.csx_shop where sdt='current')e on regexp_replace(a.prctr,'(^0*)','') = e.location_code
		--	where a.kunnr='0000107469'
group by
	c.channel,c.channel_code     ,
	a.hkont       ,
	d.account_name,
	a.comp_code   ,
	b.comp_name   ,
	a.prctr       ,
--	e.shop_name   ,
	regexp_replace(a.kunnr ,'(^0*)','')      ,
	c.customer_name   ,
	zterm         ,
	diff,
	a.sdate,
	case
		when zterm like 'Y%'
			then concat('月结',diff)
			else concat('票到',diff)
	end,
	datediff(${hiveconf:i_sdate}, a.edate),	${hiveconf:i_sdate},
	 a.edate;



-- 查询结果集
--计算逾期率
insert overwrite directory '/tmp/raoyanhua/yuqi01' row format delimited fields terminated by '\t'
select 
	b.sales_province,a.channel,b.work_no,b.sales_name,a.customer_no,a.customer_name,a.zterm,
	if(a.zterm like 'Y%',if(a.payment_days=31,45,a.payment_days+15),a.payment_days) payment_days,a.payment_terms,a.comp_code,a.comp_name,
	a.ac_all,a.over_amt,a.over_amt_1,a.diff_ac_all,a.over_rate --,b.attribute
from
	(select
		channel,
		customer_no,
		customer_name,
		zterm,
		COALESCE(payment_days,0) payment_days,
		payment_terms,
		comp_code,
		comp_name ,
		--sum(case when over_days>=0 then ac_all else 0 end) as ac_all,
		sum(ac_all) as ac_all,
		sum(case when over_days>=1 then ac_all else 0 end ) as over_amt,
		SUM(case when over_days>=1 then ac_all*over_days else 0 end) as over_amt_1,
		--sum(case when over_days>=0 then ac_all else 0 end)* if(COALESCE(payment_days,0)=0,1,if(zterm like 'Y%',if(payment_days=31,45,payment_days+15),payment_days)) as diff_ac_all,
		sum(case when ac_all>=0 then ac_all else 0 end)* if(COALESCE(payment_days,0)=0,1,if(zterm like 'Y%',if(payment_days=31,45,payment_days+15),payment_days)) as diff_ac_all,
		coalesce(round(case  when coalesce(SUM(case when ac_all>=0 then ac_all else 0 end), 0) <= 1 then 0  
					else coalesce(SUM(case when over_days>=1 then ac_all*over_days else 0 end), 0)
						/(sum(case when ac_all>=0 then ac_all else 0 end)* if(COALESCE(payment_days,0)=0,1,if(zterm like 'Y%',if(payment_days=31,45,payment_days+15),payment_days))) end
			  , 6),0) over_rate
	  
	from csx_tmp.ads_fis_r_a_customer_days_overdue_dtl a 
	where channel like '大客户' and sdt = ${hiveconf:i_sdate_23} 
	and subject_code='1122010000'
	--签呈客户不考核，不算提成,因此不算逾期
	and customer_no not in('111118','103717','102755','104023','105673','104402')
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
	and customer_no not in('104268')
	--and customer_no not in('107621','109342','109403','109671','113154','113486','113762','114145','114482','111553','111559','111589',
	--						'111617','111618','111621','111622','111630','111632','111641','111643','113934','108797','104268','103997')
	--9月签呈 福建 SAP有逾期，签呈无逾期，反馈说省区财务核对没问题，需进一步确认 9月先剔除逾期
	--and customer_no not in('102890','102890','106526','106587','106697','107305','108333','109401','110670','PF0320')	
	--10月签呈 当月剔除逾期系数
	and customer_no not in('105090','111935','110664','109293','111534','111810','112199','112201','112361','112874')
	group by channel, customer_no, customer_name, zterm, payment_days, payment_terms, comp_code, comp_name )a
join (select * from csx_dw.dws_crm_w_a_customer_m_v1 where sdt=${hiveconf:i_sdate_23} and attribute_code<>'5') b on b.customer_no=a.customer_no;













-- 客户逾期明细
insert overwrite directory '/tmp/raoyanhua/yuqi02' row format delimited fields terminated by '\t'
select b.sales_province,b.work_no,b.sales_name,a.* 
from 
(select * from csx_tmp.ads_fis_r_a_customer_days_overdue_dtl  where channel='大客户' and sdt=${hiveconf:i_sdate_23} and subject_code='1122010000')a
left join (select * from csx_dw.dws_crm_w_a_customer_m_v1 where sdt=${hiveconf:i_sdate_23} and attribute_code<>'5') b on b.customer_no=a.customer_no
;




--客户逾期率
drop table csx_tmp.temp_cust_over_rate;
create table csx_tmp.temp_cust_over_rate
as
select 
	channel,
	customer_no,
	customer_name,
	sum(case when ac_all>=0 then ac_all else 0 end) ac_all,
	sum(case when over_amt>=0 then over_amt else 0 end) over_amt,
	sum(case when over_amt>=0 then over_amt_1 else 0 end) over_amt_1,
	sum(case when ac_all>=0 then diff_ac_all else 0 end) diff_ac_all,
    coalesce(round(case  when coalesce(SUM(case when ac_all>=0 then ac_all else 0 end), 0) <= 1 then 0  
				else coalesce(SUM(case when over_amt>=0 then over_amt_1 else 0 end), 0)/(sum(case when ac_all>=0 then diff_ac_all else 0 end)) end
		  , 6),0) over_rate
from
	(select
		channel,
		customer_no,
		customer_name,
		zterm,
		COALESCE(payment_days,0) payment_days,
		payment_terms,
		comp_code,
		comp_name ,
		--sum(case when over_days>=0 then ac_all else 0 end) as ac_all,
		sum(ac_all) as ac_all,
		sum(case when over_days>=1 then ac_all else 0 end ) as over_amt,
		SUM(case when over_days>=1 then ac_all*over_days else 0 end) as over_amt_1,
		sum(ac_all)* if(COALESCE(payment_days,0)=0,1,if(zterm like 'Y%',if(payment_days=31,45,payment_days+15),payment_days)) as diff_ac_all
	from csx_tmp.ads_fis_r_a_customer_days_overdue_dtl a 
	where channel = '大客户' and sdt = ${hiveconf:i_sdate_23} 
	and subject_code='1122010000'
	--签呈客户不考核，不算提成,因此不算逾期
	and customer_no not in('111118','103717','102755','104023','105673','104402')
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
	and customer_no not in('104268')
	--and customer_no not in('107621','109342','109403','109671','113154','113486','113762','114145','114482','111553','111559','111589',
	--						'111617','111618','111621','111622','111630','111632','111641','111643','113934','108797','104268','103997')
	--9月签呈 福建 SAP有逾期，签呈无逾期，反馈说省区财务核对没问题，需进一步确认 9月先剔除逾期
	--and customer_no not in('102890','102890','106526','106587','106697','107305','108333','109401','110670','PF0320')
	--10月签呈 当月剔除逾期系数
	and customer_no not in('105090','111935','110664','109293','111534','111810','112199','112201','112361','112874')	
	group by channel,customer_no,customer_name,zterm,payment_days,payment_terms,comp_code,comp_name)a
group by channel,customer_no,customer_name;



--销售员逾期率
drop table csx_tmp.temp_salesname_over_rate;
create table csx_tmp.temp_salesname_over_rate
as
select 
	a.channel,
	b.work_no,
	b.sales_name,
	sum(case when ac_all>=0 then ac_all else 0 end) ac_all,
	sum(case when over_amt>=0 then over_amt else 0 end) over_amt,
	sum(case when over_amt>=0 then over_amt_1 else 0 end) over_amt_1,
	sum(case when ac_all>=0 then diff_ac_all else 0 end) diff_ac_all,
    coalesce(round(case  when coalesce(SUM(case when ac_all>=0 then ac_all else 0 end), 0) <= 1 then 0  
				else coalesce(SUM(case when over_amt>=0 then over_amt_1 else 0 end), 0)/(sum(case when ac_all>=0 then diff_ac_all else 0 end)) end
		  , 6),0) over_rate
from
	(select
		channel,
		customer_no,
		customer_name,
		zterm,
		COALESCE(payment_days,0) payment_days,
		payment_terms,
		comp_code,
		comp_name ,
		--sum(case when over_days>=0 then ac_all else 0 end) as ac_all,
		sum(ac_all) as ac_all,
		sum(case when over_days>=1 then ac_all else 0 end ) as over_amt,
		SUM(case when over_days>=1 then ac_all*over_days else 0 end) as over_amt_1,
		sum(ac_all)* if(COALESCE(payment_days,0)=0,1,if(zterm like 'Y%',if(payment_days=31,45,payment_days+15),payment_days)) as diff_ac_all
	from csx_tmp.ads_fis_r_a_customer_days_overdue_dtl a 
	where channel = '大客户' and sdt = ${hiveconf:i_sdate_23} 
	and subject_code='1122010000'
	--签呈客户不考核，不算提成,因此不算逾期
	and customer_no not in('111118','103717','102755','104023','105673','104402')
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
	and customer_no not in('104268')
	--and customer_no not in('107621','109342','109403','109671','113154','113486','113762','114145','114482','111553','111559','111589',
	--						'111617','111618','111621','111622','111630','111632','111641','111643','113934','108797','104268','103997')
	--9月签呈 福建 SAP有逾期，签呈无逾期，反馈说省区财务核对没问题，需进一步确认 9月先剔除逾期
	--and customer_no not in('102890','102890','106526','106587','106697','107305','108333','109401','110670','PF0320')	
	--10月签呈 当月剔除逾期系数
	and customer_no not in('105090','111935','110664','109293','111534','111810','112199','112201','112361','112874')	
	group by channel,customer_no,customer_name,zterm,payment_days,payment_terms,comp_code,comp_name)a
left join (select * from csx_dw.dws_crm_w_a_customer_m_v1 where sdt=${hiveconf:i_sdate_23} and attribute_code<>'5') b on b.customer_no=a.customer_no
group by a.channel,b.work_no,b.sales_name;


