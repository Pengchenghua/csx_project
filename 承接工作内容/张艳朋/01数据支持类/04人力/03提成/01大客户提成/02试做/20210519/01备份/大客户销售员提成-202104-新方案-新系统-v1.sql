
-- 新建表 销售提成_销售员收入组
--drop table csx_tmp.sales_income_info;
create table if not exists `csx_tmp.sales_income_info` (
  `cust_type` STRING comment '销售员类别',
  `sales_name` STRING comment '业务员名称',
  `work_no` STRING comment '业务员工号',
  `income_type` STRING comment '业务员收入组类'
) comment '销售提成_销售员收入组'
partitioned by (sdt string comment '日期分区')
row format delimited fields terminated by ','
stored as textfile;

--load data inpath '/tmp/raoyanhua/sales_income_info_04.csv' overwrite into table csx_tmp.sales_income_info partition (sdt='20210430');
--select * from csx_tmp.sales_income_info where sdt='20210430';




--有销售的销售员名单及收入组
select b.work_no,b.sales_name,c.income_type,
	sum(sales_value) sales_value,
	sum(profit) profit,
	sum(front_profit) front_profit
from
(
select province_code,province_name,customer_no,substr(sdt,1,6) smonth,
	sum(sales_value) sales_value,
	sum(profit) profit,
	sum(front_profit) front_profit
--from csx_dw.dws_sale_r_d_detail
from csx_dw.dws_sale_r_d_detail
where sdt>='20210401'
and sdt<'20210501'
and channel_code in('1','7')
group by province_code,province_name,customer_no,substr(sdt,1,6)
)a	
left join (select * from csx_dw.dws_crm_w_a_customer where sdt='20210430') b on b.customer_no=a.customer_no
left join (select distinct work_no,income_type from csx_tmp.sales_income_info where sdt='20210430') c on c.work_no=b.work_no
where c.income_type is null
and b.sales_name not like '%B%' 
and b.sales_name not like '%C%'
group by b.work_no,b.sales_name,c.income_type;

--★★★★★★★★~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~★★★★★★★★
--★★★★★★★★首先确认需对哪些销售员补充收入组★★★★★★★★
--★★★★★★★★~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~★★★★★★★★


-- 昨日、昨日月1日，上月1日，上月最后一日
--select ${hiveconf:i_sdate_11},${hiveconf:i_sdate_12},${hiveconf:i_sdate_12},${hiveconf:i_sdate_11};

--set i_sdate_11 =regexp_replace(date_sub(current_date,1),'-','');
--set i_sdate_12 =regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),0),'-','');

set i_sdate_11 =regexp_replace(last_day(add_months(date_sub(current_date,1),-1)),'-','');	
set i_sdate_12 =regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-1),'-','');					

set i_sdate_11 ='20210430';	
set i_sdate_12 ='20210401';				



---每日销售员提成系数（销额提成比例、前端毛利提成比例）
drop table csx_tmp.tmp_salesname_rate_ytd;
create table csx_tmp.tmp_salesname_rate_ytd
as
select sdt,work_no,sales_name,income_type,ytd,
case when ((ytd<=10000000 and income_type in('Q1','Q2','Q3','Q4','Q5')) 
			or (ytd>10000000 and ytd<=20000000 and income_type in('Q2','Q3','Q4','Q5'))
			or (ytd>20000000 and ytd<=30000000 and income_type in('Q3','Q4','Q5'))
			or (ytd>30000000 and ytd<=40000000 and income_type in('Q4','Q5'))
			or (ytd>40000000 and ytd<=50000000 and income_type in('Q5'))) then 0.002
	 when ((ytd>10000000 and ytd<=20000000 and income_type in('Q1'))
			or (ytd>20000000 and ytd<=30000000 and income_type in('Q2'))
			or (ytd>30000000 and ytd<=40000000 and income_type in('Q3'))
			or (ytd>40000000 and ytd<=50000000 and income_type in('Q4'))
			or (ytd>50000000 and income_type in('Q5'))) then 0.0025
	 when ((ytd>20000000 and ytd<=30000000 and income_type in('Q1'))
			or (ytd>30000000 and ytd<=40000000 and income_type in('Q2'))
			or (ytd>40000000 and ytd<=50000000 and income_type in('Q3'))
			or (ytd>50000000 and income_type in('Q3','Q4'))) then 0.003
	 when ((ytd>30000000 and ytd<=40000000 and income_type in('Q1'))
			or (ytd>40000000 and income_type in('Q2'))) then 0.0035
	 when (ytd>40000000 and income_type in('Q1')) then 0.004			
else 0.002 end sale_rate,

case when ((ytd<=10000000 and income_type in('Q1','Q2','Q3','Q4','Q5')) 
			or (ytd>10000000 and ytd<=20000000 and income_type in('Q2','Q3','Q4','Q5'))
			or (ytd>20000000 and ytd<=30000000 and income_type in('Q3','Q4','Q5'))
			or (ytd>30000000 and ytd<=40000000 and income_type in('Q4','Q5'))
			or (ytd>40000000 and ytd<=50000000 and income_type in('Q5'))) then 0.1
	 when ((ytd>10000000 and ytd<=20000000 and income_type in('Q1'))
			or (ytd>20000000 and ytd<=30000000 and income_type in('Q2'))
			or (ytd>30000000 and ytd<=40000000 and income_type in('Q3'))
			or (ytd>40000000 and ytd<=50000000 and income_type in('Q4'))
			or (ytd>50000000 and income_type in('Q5'))) then 0.125
	 when ((ytd>20000000 and ytd<=30000000 and income_type in('Q1'))
			or (ytd>30000000 and ytd<=40000000 and income_type in('Q2'))
			or (ytd>40000000 and ytd<=50000000 and income_type in('Q3'))
			or (ytd>50000000 and income_type in('Q3','Q4'))) then 0.15
	 when ((ytd>30000000 and ytd<=40000000 and income_type in('Q1'))
			or (ytd>40000000 and income_type in('Q2'))) then 0.175
	 when (ytd>40000000 and income_type in('Q1')) then 0.2			
else 0.1 end profit_rate
from 
	(select a.sdt,b.work_no,b.sales_name,coalesce(c.income_type,'Q1')income_type,
	sum(a.sales_value)over(PARTITION BY b.work_no,b.sales_name,substr(a.sdt,1,4) order by a.sdt ROWS BETWEEN UNBOUNDED PRECEDING AND 0 PRECEDING )ytd
	from 
		(select sdt,customer_no,substr(sdt,1,6) smonth,
				if(substr(sdt,1,6)=substr(regexp_replace(date_sub(current_date,1),'-',''),1,6),
					regexp_replace(date_sub(current_date,1),'-',''),
					regexp_replace(last_day(to_date(from_unixtime(unix_timestamp(sdt,'yyyyMMdd')))),'-','')
					) as sdt_last,  --sdt所在月最后1日，当月为昨日
		sum(sales_value) sales_value
		from csx_dw.dws_sale_r_d_detail
		where sdt>='20210101' and sdt<=${hiveconf:i_sdate_11} --昨日月1日
		and channel_code in('1','7','9')
		and business_type_code not in('3','4')
		--福建泉州签呈，订单12月销售530181.06元，1月全部退货，不算提成
		and (order_no not in ('OM20122800005550','RH21011900000203') or order_no is null)		
		and (order_no not in ('OC200529000043','OC200529000044','OC200529000045','OC200529000046',
					'OC20111000000021','OC20111000000022','OC20111000000023','OC20111000000024','OC20111000000025') or order_no is null)
		--签呈客户不考核，不算提成 2021年3月签呈取消剔除103717
		and customer_no not in('111118','102755','104023','105673','104402')
		and customer_no not in('107338','104123','102629','104526','106375','106380','106335','107268','104296','108391','108390','108072','108503')
		--签呈客户仅4月不考核，不算提成
		--and customer_no not in('PF0320','105177')
		--9月签呈 四川 算到业务代理人，每月剔除逾期和销售
		--and customer_no not in('104179','112092')
		--9月签呈 重庆 合伙人客户，9月剔除逾期和销售
		--and customer_no not in('114265','114248','114401','111933','113080','113392')
		--9月签呈 重庆 剔除9月逾期，其中'109484'剔除9月的逾期和销售
		--and customer_no not in('109484')
		--12月签呈 当月剔除逾期系数,其中'113744','113824','113826','113831'剔除提成
		--and customer_no not in('113744','113824','113826','113831')
		--2月签呈客户仅2月不考核，不算提成
	    --and customer_no not in('116015','115721','115721','116877','116883','116015','116556','116826','103253','103284','103296',
        --                       '103297','103304','103306','103311','104818','104828','104829','104835','105113','106283','106284',
        --                       '106298','106299','106301','106306','106307','106308','106309','106320','106321','106325','106326',
        --                       '106330','102844','114054','109000','114083','114085','115909','115971','116215',
	    --					   '111100','116058','116188','105601')			
        --3月签呈 剔除逾期系数不算提成(其中'PF0065','112574','106782'3-5月不发提成);剔除逾期系数.不算提成.每月*2
	    and customer_no not in('PF0065','112574','106782')  --'116957','116629','116215'仅3月
	    and customer_no not in('115721','116877','116883','116015','116556','116826','114054','109000','114083','114085','115909','115971')
	    and customer_no not in('103253','103284','103296','103297','103304','103306','103311','104818','104828','104829','104835',
                               '105113','106283','106284','106298','106299','106301','106306','106307','106308','106309','106320',
                               '106321','106325','106326','106330','104609')		
		group by sdt,customer_no,substr(sdt,1,6),
				if(substr(sdt,1,6)=substr(regexp_replace(date_sub(current_date,1),'-',''),1,6),
					regexp_replace(date_sub(current_date,1),'-',''),
					regexp_replace(last_day(to_date(from_unixtime(unix_timestamp(sdt,'yyyyMMdd')))),'-','')
					)	
		)a 
	left join   --CRM客户信息取每月最后一天
		(select * ,
			case when channel_code='9' then '业务代理' end as ywdl_cust,
			case when (customer_name like '%内%购%' or customer_name like '%临保%') then '内购' end as ng_cust
			--case when dev_source_code='2' then '业务代理' end as ywdl_cust,
			--case when dev_source_code='4' then '内购' end as ng_cust		
		from csx_dw.dws_crm_w_a_customer 
		--where sdt in('20200131','20200229','20200331','20200430','20200531','20200630','20200731','20200831','20200930','20201031','20201130','20201231')
		where sdt>=regexp_replace(trunc(date_sub(current_date,1),'YY'),'-','')  --昨日所在年第1天
		and sdt=if(substr(sdt,1,6)=substr(regexp_replace(date_sub(current_date,1),'-',''),1,6),
					regexp_replace(date_sub(current_date,1),'-',''),
					regexp_replace(last_day(to_date(from_unixtime(unix_timestamp(sdt,'yyyyMMdd')))),'-','')
					)  --sdt为每月最后一天
		)b on b.customer_no=a.customer_no and b.sdt=a.sdt_last 	
	left join (select distinct work_no,income_type from csx_tmp.sales_income_info where sdt=${hiveconf:i_sdate_11}) c on c.work_no=b.work_no   --上月最后1日
	--left join   --CRM客户开发来源为业务代理--剔除
	--		(
	--		select distinct customer_no,substr(sdt,1,6) smonth 
	--		from csx_dw.dws_crm_w_a_customer 
	--		where sdt=${hiveconf:i_sdate_11}    --上月最后1日 
	--		and dev_source_code='2'
	--		)d on d.customer_no=a.customer_no
	where b.ywdl_cust is null
	and b.ng_cust is null
	)a;


--01、客户本月每天-销售员销额、最终前端毛利统计
drop table csx_tmp.temp_new_cust_00;
create table csx_tmp.temp_new_cust_00
as
select 
b.sales_province_name dist,a.customer_no cust_id,b.customer_name cust_name,b.work_no,b.sales_name,d.service_user_work_no,d.service_user_name,a.smonth,
coalesce(c.sale_rate,0.002) sale_rate,coalesce(c.profit_rate,0.1) profit_rate,
sum(sales_value)sales_value,
sum(profit) profit,sum(profit)/sum(sales_value) prorate,
sum(front_profit) front_profit,sum(front_profit)/sum(sales_value) fnl_prorate,
round(sum(a.sales_value)*coalesce(c.sale_rate,0.002)+if(sum(a.front_profit)<0,0,coalesce(sum(a.front_profit),0)*coalesce(c.profit_rate,0.1)),2) salary
from 
  (
  select sdt,substr(sdt,1,6) smonth,province_name,customer_no,
    sum(sales_value)sales_value,
    sum(profit) profit,sum(profit)/abs(sum(sales_value)) prorate,
    sum(front_profit) as front_profit,
    sum(front_profit)/abs(sum(sales_value)) as fnl_prorate
  from csx_dw.dws_sale_r_d_detail
  where sdt>=${hiveconf:i_sdate_12} and sdt<=${hiveconf:i_sdate_11} --昨日月1日
  and channel_code in('1','7','9')
  and business_type_code not in('3','4')
  --福建泉州签呈，订单12月销售530181.06元，1月全部退货，不算提成
  --and (order_no not in ('OM20122800005550','RH21011900000203') or order_no is null)  
  --and (order_no not in ('OC200529000043','OC200529000044','OC200529000045','OC200529000046',
  --					'OC20111000000021','OC20111000000022','OC20111000000023','OC20111000000024','OC20111000000025') or order_no is null)
  --签呈客户不考核，不算提成 2021年3月签呈取消剔除 103717
  and customer_no not in('111118','102755','104023','105673','104402')
  and customer_no not in('107338','104123','102629','104526','106375','106380','106335','107268','104296','108391','108390','108072','108503')
  --签呈客户仅4月不考核，不算提成
  --and customer_no not in('PF0320','105177')
  --签呈客户仅7月不考核，不算提成
  --and customer_no not in('113108','113067','110656','111837','111296','105202')
  --9月签呈 四川 算到业务代理人，每月剔除逾期和销售
  and customer_no not in('104179','112092')
  --9月签呈 重庆 合伙人客户，9月剔除逾期和销售
  --and customer_no not in('114265','114248','114401','111933','113080','113392')
  --9月签呈 重庆 剔除9月逾期，其中'109484'剔除9月的逾期和销售
  --and customer_no not in('109484')
  --12月签呈 当月剔除逾期系数,其中'113744','113824','113826','113831'剔除提成
  --and customer_no not in('113744','113824','113826','113831') 
  --2月签呈客户仅2月不考核，不算提成
  --and customer_no not in('116015','115721','115721','116877','116883','116015','116556','116826','103253','103284','103296',
  --                          '103297','103304','103306','103311','104818','104828','104829','104835','105113','106283','106284',
  --                          '106298','106299','106301','106306','106307','106308','106309','106320','106321','106325','106326',
  --                          '106330','102844','114054','109000','114083','114085','115909','115971','116215',
  --					   '111100','116058','116188','105601')	 
  --3月签呈 剔除逾期系数不算提成(其中'PF0065','112574','106782'3-5月不发提成);剔除逾期系数.不算提成.每月*2
  and customer_no not in('PF0065','112574','106782')  --'116957','116629','116215'仅3月
  and customer_no not in('115721','116877','116883','116015','116556','116826','114054','109000','114083','114085','115909','115971')
  and customer_no not in('103253','103284','103296','103297','103304','103306','103311','104818','104828','104829','104835',
                         '105113','106283','106284','106298','106299','106301','106306','106307','106308','106309','106320',
                         '106321','106325','106326','106330','104609')  
  group by sdt,substr(sdt,1,6),province_name,customer_no


--★★★扣减前端毛利 3月签呈

--★★★合并返利签呈处理		
 	 
	 
--★★★合并返利新增  		
  )a
left join 
	(select distinct customer_no,customer_name,work_no,sales_name,sales_province_name,
		case when channel_code='9' then '业务代理' end as ywdl_cust,
		case when (customer_name like '%内%购%' or customer_name like '%临保%') then '内购' end as ng_cust
		--case when dev_source_code='2' then '业务代理' end as ywdl_cust,
		--case when dev_source_code='4' then '内购' end as ng_cust
	from csx_dw.dws_crm_w_a_customer where sdt=${hiveconf:i_sdate_11}   --上月最后1日
	)b on b.customer_no=a.customer_no
left join 
	(select  work_no,sales_name,sdt,max(sale_rate) sale_rate,max(profit_rate) profit_rate
	from csx_tmp.tmp_salesname_rate_ytd where sdt>=${hiveconf:i_sdate_12} and sdt<=${hiveconf:i_sdate_11}  --上月1日，昨日月1日
	group by work_no,sales_name,sdt
	)c on c.work_no=b.work_no and c.sales_name=b.sales_name and c.sdt=a.sdt
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
	  and is_additional_info = 1 and service_user_id <> 0
	  ----3月签呈，将以下客户的服务管家调整为郭志江
	  --union all
      --select '113873' customer_no,'签呈，未知' service_user_work_no,'郭志江' service_user_name
	  --union all
      --select '113918' customer_no,'签呈，未知' service_user_work_no,'郭志江' service_user_name
	  --union all
      --select '113935' customer_no,'签呈，未知' service_user_work_no,'郭志江' service_user_name
	  --union all
      --select '113936' customer_no,'签呈，未知' service_user_work_no,'郭志江' service_user_name
	  --union all
      --select '113940' customer_no,'签呈，未知' service_user_work_no,'郭志江' service_user_name	  
	  )a
	group by customer_no
  )d on d.customer_no=a.customer_no		
where b.ywdl_cust is null
and b.ng_cust is null
group by b.sales_province_name,a.customer_no,b.customer_name,b.work_no,b.sales_name,d.service_user_work_no,d.service_user_name,c.sale_rate,c.profit_rate,a.smonth;



--大客户前端毛利扣点后结果
drop table csx_tmp.temp_new_cust_01;
create table csx_tmp.temp_new_cust_01
as
select 
a.dist,a.cust_id,a.cust_name,a.work_no,a.sales_name,a.service_user_work_no,a.service_user_name,a.smonth,
sum(a.sales_value) sales_value,
sum(a.profit) profit,
sum(a.profit)/abs(sum(a.sales_value)) prorate,
sum(a.front_profit)-sum(a.sales_value*coalesce(z.rate,0)) fnl_profit,
(sum(a.front_profit)-sum(a.sales_value*coalesce(z.rate,0)))/abs(sum(a.sales_value)) fnl_prorate,
--round(sum(a.sales_value*coalesce(a.sale_rate,0.002))+
--	  if((sum(a.front_profit)-sum(a.sales_value*coalesce(z.rate,0)))<=0,0,sum(coalesce(a.front_profit-a.sales_value*coalesce(z.rate,0),0)*coalesce(a.profit_rate,0.1))),2) salary
--销售额奖金包、前端毛利奖金包，未乘分配比例
round(sum(a.sales_value*coalesce(a.sale_rate,0.002)),2) salary_sales_value,
round(if((sum(a.front_profit)-sum(a.sales_value*coalesce(z.rate,0)))<=0,0,sum(coalesce(a.front_profit-a.sales_value*coalesce(z.rate,0),0)*coalesce(a.profit_rate,0.1))),2) salary_fnl_profit
from csx_tmp.temp_new_cust_00 a
left join
(  --福建区域大客户扣点 20200115
select '104824'cust_id, 0.02 rate
union all
select '104847'cust_id, 0.02 rate
union all
select '104854'cust_id, 0.02 rate
union all
select '104859'cust_id, 0.02 rate
union all
select '104870'cust_id, 0.02 rate
union all
select 'PF0649'cust_id, 0.09 rate
union all
select '102784'cust_id, 0.01 rate
union all
select '102901'cust_id, 0.01 rate
union all
select '102734'cust_id, 0.01 rate
union all
select '103372'cust_id, 0.03 rate
union all
select '103048'cust_id, 0.02 rate
union all
select '105249'cust_id, 0.02 rate
union all
select '106369'cust_id, 0.01 rate
union all
select '105150'cust_id, 0.1 rate
union all
select '105177'cust_id, 0.1 rate
union all
select '105182'cust_id, 0.1 rate
union all
select '105164'cust_id, 0.1 rate
union all
select '105181'cust_id, 0.1 rate
union all
select '105156'cust_id, 0.1 rate
union all
select '105165'cust_id, 0.1 rate
union all
select '106423'cust_id, 0.1 rate
union all
select '106721'cust_id, 0.1 rate
union all
select '106805'cust_id, 0.1 rate
union all
select '107404'cust_id, 0.1 rate
union all
select '105567'cust_id, 0.06 rate
union all
select '105399'cust_id, 0.01 rate
)z on z.cust_id=a.cust_id
where a.cust_id not in('115935')
--and a.cust_id not in('105220','105539','106239','106637','106713','106900','106910','107022','107100','107242','107298','107361','107532',
--'108236','110242','110660','110866','112460','115274','116461','117704','102534','102798','102806','103808','104741',
--'105186','114724','115915','115920','104758','105956','105965','106288','106559','106878','107104','107910','112492',
--'112633','113423','117479','105480','105483','106300','106469','106524','106538','107438','111892','112210','117067',
--'103945','103954','104222','104229','104241','104251','104255','104379','104414','104538','104965','105005','105024',
--'105756','112813')
--and a.cust_id not in('116603','117002','115935')
group by a.dist,a.cust_id,a.cust_name,a.work_no,a.sales_name,a.service_user_work_no,a.service_user_name,a.smonth

---- 签呈提成方式前端毛利*6%
--union all
--select 
--a.dist,a.cust_id,a.cust_name,a.work_no,a.sales_name,a.service_user_work_no,a.service_user_name,a.smonth,
--sum(a.sales_value) sales_value,
--sum(a.profit) profit,
--sum(a.profit)/abs(sum(a.sales_value)) prorate,
--sum(a.front_profit) fnl_profit,
--sum(a.front_profit)/abs(sum(a.sales_value)) fnl_prorate,
----round(if(sum(a.front_profit)<=0,0,sum(coalesce(a.front_profit,0)*0.06)),2) salary
--0 salary_sales_value,
--round(if(sum(a.front_profit)<=0,0,sum(coalesce(a.front_profit,0)*0.06)),2) salary_fnl_profit
--from csx_tmp.temp_new_cust_00 a
--where a.cust_id in(
--'105220','105539','106239','106637','106713','106900','106910','107022','107100','107242','107298','107361','107532',
--'108236','110242','110660','110866','112460','115274','116461','117704','102534','102798','102806','103808','104741',
--'105186','114724','115915','115920','104758','105956','105965','106288','106559','106878','107104','107910','112492',
--'112633','113423','117479','105480','105483','106300','106469','106524','106538','107438','111892','112210','117067',
--'103945','103954','104222','104229','104241','104251','104255','104379','104414','104538','104965','105005','105024',
--'105756','112813'
--)
--group by a.dist,a.cust_id,a.cust_name,a.work_no,a.sales_name,a.service_user_work_no,a.service_user_name,a.smonth

---- 签呈只算销售提成，不算前端毛利提成
-- 2月签呈 115935 客户每月提成方式前端毛利*0%
--union all
--select 
--a.dist,a.cust_id,a.cust_name,a.work_no,a.sales_name,a.service_user_work_no,a.service_user_name,a.smonth,
--sum(a.sales_value) sales_value,
--sum(a.profit) profit,
--sum(a.profit)/abs(sum(a.sales_value)) prorate,
--sum(a.front_profit) fnl_profit,
--sum(a.front_profit)/abs(sum(a.sales_value)) fnl_prorate,
----round(sum(a.sales_value*coalesce(a.sale_rate,0.002)),2) salary
--round(sum(a.sales_value*coalesce(a.sale_rate,0.002)),2) salary_sales_value,
--0 salary_fnl_profit
--from csx_tmp.temp_new_cust_00 a
--where a.cust_id in('115935')
--group by a.dist,a.cust_id,a.cust_name,a.work_no,a.sales_name,a.service_user_work_no,a.service_user_name,a.smonth
-- 3月签呈 115935 每月处理，提成方式销售额*0.2%*0.1，前端毛利*0%
union all
select 
a.dist,a.cust_id,a.cust_name,a.work_no,a.sales_name,a.service_user_work_no,a.service_user_name,a.smonth,
sum(a.sales_value) sales_value,
sum(a.profit) profit,
sum(a.profit)/abs(sum(a.sales_value)) prorate,
sum(a.front_profit) fnl_profit,
sum(a.front_profit)/abs(sum(a.sales_value)) fnl_prorate,
--round(sum(a.sales_value*coalesce(a.sale_rate,0.002)),2) salary
round(sum(a.sales_value*0.002*0.1),2) salary_sales_value,
0 salary_fnl_profit
from csx_tmp.temp_new_cust_00 a
where a.cust_id in('115935')
group by a.dist,a.cust_id,a.cust_name,a.work_no,a.sales_name,a.service_user_work_no,a.service_user_name,a.smonth
;



  
--结果表1 


--02客户、销售员逾期系数
--客户当月提成 

drop table csx_tmp.temp_new_cust_salary;
create table csx_tmp.temp_new_cust_salary
as
select 
a.smonth,a.dist,a.cust_id,a.cust_name,
--3月签呈，将以下客户的销售员调整为蒋玲
--if(a.cust_id in('113873','113918','113935','113936','113940'),'80886296',
--    a.work_no) as work_no,
--if(a.cust_id in('113873','113918','113935','113936','113940'),'蒋玲',
--    a.sales_name) as sales_name,
a.work_no,a.sales_name,
a.service_user_work_no,a.service_user_name,
a.sales_value,a.profit,a.profit/abs(a.sales_value) prorate,
a.fnl_profit,a.fnl_profit/abs(a.sales_value) fnl_prorate,
--a.salary,
a.salary_sales_value,
a.salary_fnl_profit,
b.receivable_amount,b.over_amt,
if(a.service_user_work_no<>'','服务管家有提成','服务管家无提成') assigned_type, --分配类别
--b.over_rate cust_over_rate,
c.over_rate sale_over_rate,
d.over_rate service_user_over_rate,
--if(a.salary<0 or c.over_rate is null,a.salary,a.salary*(1-coalesce(if(c.over_rate<=0.5,c.over_rate,1),0)) ) salary_2
--if(z.salary3 is null,if(a.salary<0 or c.over_rate is null,a.salary,a.salary*(1-coalesce(if(c.over_rate<=0.5,c.over_rate,1),0)) ),z.salary3) salary_2
if(z.salary_sales_value is null,if(a.salary_sales_value<0 or c.over_rate is null,
    a.salary_sales_value,a.salary_sales_value*(1-coalesce(if(c.over_rate<=0.5,c.over_rate,1),0)) )
	,z.salary_sales_value)*if(a.service_user_work_no<>'',0.9,1) salary_sales_value_sale,	
if(z.salary_sales_value is null,if(a.salary_sales_value<0 or d.over_rate is null,
    a.salary_sales_value,a.salary_sales_value*(1-coalesce(if(d.over_rate<=0.5,d.over_rate,1),0)) )
	,z.salary_sales_value)*if(a.service_user_work_no<>'',0.1,0) salary_sales_value_service,
	
if(z.salary_fnl_profit is null,if(c.over_rate is null,
    a.salary_fnl_profit,a.salary_fnl_profit*(1-coalesce(if(c.over_rate<=0.5,c.over_rate,1),0)) )
	,z.salary_fnl_profit)*if(a.service_user_work_no<>'',0.8,1) salary_fnl_profit_sale,
if(z.salary_fnl_profit is null,if(d.over_rate is null,
    a.salary_fnl_profit,a.salary_fnl_profit*(1-coalesce(if(d.over_rate<=0.5,d.over_rate,1),0)) )
	,z.salary_fnl_profit)*if(a.service_user_work_no<>'',0.2,0) salary_fnl_profit_service	
from  csx_tmp.temp_new_cust_01 a
left join csx_tmp.temp_cust_over_rate b on b.customer_no=a.cust_id
left join csx_tmp.temp_salesname_over_rate c on c.sales_name=a.sales_name and coalesce(c.work_no,0)=coalesce(a.work_no,0)
left join csx_tmp.temp_service_user_over_rate d on d.service_user_name=a.service_user_name and coalesce(d.service_user_work_no,0)=coalesce(a.service_user_work_no,0)
--1月签呈 调整最终提成--按照奖金包特定值发
left join 
(
select '000000'cust_id, 0 salary_sales_value, 0 salary_fnl_profit
union all
select '000001'cust_id, 0 salary_sales_value, 0 salary_fnl_profit
)z on z.cust_id=a.cust_id
; 

insert overwrite directory '/tmp/raoyanhua/tc_kehu' row format delimited fields terminated by '\t'
select *,
coalesce(salary_sales_value_sale,0)+coalesce(salary_fnl_profit_sale,0),
coalesce(salary_sales_value_service,0)+coalesce(salary_fnl_profit_service,0)
from csx_tmp.temp_new_cust_salary;

--销售员当月提成
insert overwrite directory '/tmp/raoyanhua/tc_xiaoshou' row format delimited fields terminated by '\t'
select smonth,dist,work_no,sales_name,
sum(sales_value)sales_value,
sum(profit)profit,
sum(profit)/abs(sum(sales_value)) prorate,
sum(fnl_profit)fnl_profit,
sum(fnl_profit)/abs(sum(sales_value)) fnl_prorate,
sum(salary_sales_value) salary_sales_value,
sum(salary_fnl_profit) salary_fnl_profit,
sum(receivable_amount)receivable_amount,
sum(over_amt)over_amt,
--sum(salary_1)salary_1,
sale_over_rate,
sum(salary_sales_value_sale)salary_sales_value_sale,
sum(salary_fnl_profit_sale)salary_fnl_profit_sale,
coalesce(sum(salary_sales_value_sale),0)+coalesce(sum(salary_fnl_profit_sale),0) salary_sale
from csx_tmp.temp_new_cust_salary
group by smonth,dist,work_no,sales_name,sale_over_rate;

--服务管家当月提成
insert overwrite directory '/tmp/raoyanhua/tc_fuwuguanjia' row format delimited fields terminated by '\t'
select smonth,dist,service_user_work_no,service_user_name,
sum(sales_value)sales_value,
sum(profit)profit,
sum(profit)/abs(sum(sales_value)) prorate,
sum(fnl_profit)fnl_profit,
sum(fnl_profit)/abs(sum(sales_value)) fnl_prorate,
sum(salary_sales_value) salary_sales_value,
sum(salary_fnl_profit) salary_fnl_profit,
sum(receivable_amount)receivable_amount,
sum(over_amt)over_amt,
--sum(salary_1)salary_1,
service_user_over_rate,
sum(salary_sales_value_service)salary_sales_value_service,
sum(salary_fnl_profit_service)salary_fnl_profit_service,
coalesce(sum(salary_sales_value_service),0)+coalesce(sum(salary_fnl_profit_service),0) salary_service
from csx_tmp.temp_new_cust_salary
group by smonth,dist,service_user_work_no,service_user_name,service_user_over_rate;





/*
-- 大客户提成：月度新客户
select b.sales_province_name,b.customer_no,b.customer_name,b.attribute_desc,b.attribute_name,b.dev_source_name,b.work_no,b.sales_name,b.sign_date,
	a.first_order_date
from
(
select attribute_desc,attribute_name,dev_source_name,customer_no,customer_name,channel_name,sales_name,work_no,sales_province_name,
regexp_replace(split(first_sign_time, ' ')[0], '-', '') as sign_date,estimate_contract_amount*10000 estimate_contract_amount
from csx_dw.dws_crm_w_a_customer
where sdt='current'
and customer_no<>''
and channel_code in('1','7','8')
)b
join
--客户最早销售月 新客月、新客季度
	(select customer_no,
	min(first_order_date) first_order_date
	from csx_dw.dws_crm_w_a_customer_active
	where sdt = 'current'
	group by customer_no
	having min(first_order_date)>='20210401' and min(first_order_date)<'20210501'
	)a on b.customer_no=a.customer_no;






---截至上月销售员的累计销售额
drop table csx_dw.dws_cust_ytd_sale;
create table csx_dw.dws_cust_ytd_sale
as
--insert overwrite directory '/tmp/raoyanhua/linshi01' row format delimited fields terminated by '\t'
select b.work_no,b.sales_name,
sum(a.sales_value)sales_value,
sum(a.profit)profit
from 
(select customer_no,substr(sdt,1,6) smonth,
sum(sales_value) sales_value,
sum(profit)profit
 from csx_dw.dws_sale_r_d_detail
where sdt>='20200101' and sdt<${hiveconf:i_sdate_12}  
and item_channel_code in('1','7')
group by customer_no,substr(sdt,1,6))a 
left join (select * from csx_dw.dws_crm_w_a_customer where sdt=${hiveconf:i_sdate_11}) b on b.customer_no=a.customer_no   --上月最后1日
left join (select distinct customer_no,substr(sdt,1,6) smonth from csx_tmp.tmp_cust_partner2 ) d on d.customer_no=a.customer_no and d.smonth=a.smonth
where d.customer_no is null
group by b.work_no,b.sales_name;




