--20200518将利润中心调整凭证号剔除
--20200525增加更新需剔除的利润中心调整凭证号
--20200527增加字段：对账金额，开票金额
--20200603 临时表的库名调整b2b_tmp.--csx_tmp.
--20210615 账期表 csx_dw.dws_crm_r_a_customer_account_day 改成 csx_dw.dws_crm_w_a_customer_company
--20210730 因权限控制需要，客户关联csx_dw.dws_crm_w_a_customer后再关联csx_dw.dws_sale_w_a_area_belong取销售省区

-- 报表数据相关
--select ${hiveconf:yesterday},${hiveconf:yesterday1},${hiveconf:current_day};
--当天分区、昨日分区、取数截止日期到昨日或指定日期
set current_day =regexp_replace(date_sub(current_date,0),'-','');
set current_day1 =regexp_replace(date_sub(current_date,1),'-','');

set yesterday = regexp_replace(date_sub(current_date,1),'-',''); 
set yesterday1 = date_sub(current_date,1);

--刷历史用，指定刷某一天,如刷上月底最后一天
--set yesterday = regexp_replace(last_day(add_months(date_sub(current_date,1),-1)),'-','');  --上月底最后一天'20210731'
--set yesterday1 =last_day(add_months(date_sub(current_date,1),-1)); --上月底最后一天'2021-07-31'



-- 第一部分 算逾期
--临时表0.1
drop table csx_tmp.temp_account;
create temporary table csx_tmp.temp_account
as 
select a.bukrs,
case when a.kunnr in ('G7150')  then '36'
     when e.province_name is null and substr(f.company_name, 1, 2)='北京' then '1'
	 when e.province_name is null and substr(f.company_name, 1, 2)='上海' then '2'
	 when e.province_name is null and substr(f.company_name, 1, 2)='河北' then '6'
	 when e.province_name is null and substr(f.company_name, 1, 2)='江苏' then '10'
	 when e.province_name is null and substr(f.company_name, 1, 2)='安徽' then '11'
	 when e.province_name is null and substr(f.company_name, 1, 2)='浙江' then '13'
	 when e.province_name is null and substr(f.company_name, 1, 2)='福建' then '15'
	 when e.province_name is null and substr(f.company_name, 1, 3)='BBC' then '16'
	 when e.province_name is null and substr(f.company_name, 1, 2)='广东' then '20'
	 when e.province_name is null and substr(f.company_name, 1, 2)='贵州' then '23'
	 when e.province_name is null and substr(f.company_name, 1, 2)='四川' then '24'
	 when e.province_name is null and substr(f.company_name, 1, 2)='陕西' then '26'
	 when e.province_name is null and substr(f.company_name, 1, 2)='重庆' then '32'
	 when e.province_name is null and substr(f.company_name, 1, 2)='永辉' then '15'
	 else coalesce(e.province_code,d.sales_province_code,'999') end as sales_province_code,					 
if(e.province_name is null and d.sales_province_name is null,
	case when a.kunnr in ('G7150')  then '平台-食百采购'
	     when substr(f.company_name, 1, 2) in('上海', '北京', '重庆') then concat(substr(f.company_name, 1, 2), '市')
		 when substr(f.company_name, 1, 2)= '永辉' then '福建省'
		 else concat(substr(f.company_name, 1, 2), '省') end,
	coalesce(e.province_name,d.sales_province_name,'其他')) as sales_province,
coalesce(e.city_group_name,d.city_group_name,'其他') as sales_city,		
a.kunnr,
c.payment_terms,
c.diff payment_days,
e.customer_name,
a.budat,
case when c.payment_terms like 'Y%' then 
date_add(last_day(concat(substr(a.budat,1,4),'-',substr(a.budat,5,2),'-',substr(a.budat,7,2))),COALESCE(c.diff,0))
else date_add(concat(substr(a.budat,1,4),'-',substr(a.budat,5,2),'-',substr(a.budat,7,2)),COALESCE(c.diff,0)) end edate,
dmbtr,sale,income  
from 
(
--select bukrs,kunnr,budat,dmbtr,
--case when (substr(a.belnr,1,1)<>'1' and substr(a.belnr,1,2)<>'01' and substr(a.belnr,1,3)<>'009') then dmbtr else 0 end sale,
--case when (substr(a.belnr,1,1)='1' or substr(a.belnr,1,2)='01' or substr(a.belnr,1,3)='009')then dmbtr else 0 end income
--from ods_ecc.ecc_ytbcustomer a 
--where sdt=${hiveconf:current_day} and budat<=${hiveconf:yesterday} and mandt='800'  --sdt budat月初刷数据-当前分区刷月底
----and a.hkont = '1122010000'
--and a.hkont like '1122%'
------and 
-------- 剔除利润调整凭证 科目+年度+凭证号+公司代码
------concat_ws('-',hkont ,gjahr,belnr,bukrs) not in (
------	'1122010000-2020-0090526358-1933','1122010000-2020-0090526357-1933','1122010000-2020-0090446438-1933','1122010000-2020-0090446437-1933',
------    '1122010000-2020-0090446436-1933','1122010000-2020-0101042210-2200','1122010000-2020-0100794408-2121','1122010000-2020-0100794407-2121',
------    '1122010000-2020-0100698829-2121','1122010000-2020-0100698828-2121','1122010000-2020-0100698815-2121','1122010000-2020-0100698814-2121',
------    '1122010000-2020-0100698811-2121','1122010000-2020-0100698810-2121','1122010000-2020-0100698807-2121','1122010000-2020-0100698806-2121',
------    '1122010000-2020-0100599788-2202','1122010000-2020-0100387789-2400','1122010000-2020-0100384016-2300','1122010000-2020-0100343582-2403',
------    '1122010000-2020-0100343559-2403','1122010000-2020-0100343558-2403','1122010000-2020-0100339686-2402','1122010000-2020-0100245041-2303',
------    '1122010000-2020-0100154283-2700','1122010000-2020-0100004543-2800','1122010000-2020-0100183238-2700','1122010000-2020-0100404461-2402',
------    '1122010000-2020-0100467273-2400','1122010000-2020-0100468834-2300','1122010000-2020-0100755372-2202','1122010000-2020-0100873656-2121',
------    '1122010000-2020-0101263298-2200','1122010000-2020-0090572072-1933')
--union all
--select bukrs,kunnr,budat,if(shkzg='H',-dmbtr,dmbtr) as dmbtr,
--case when (substr(belnr,1,1)<>'1' and substr(belnr,1,2)<>'01' and substr(belnr,1,3)<>'009') then if(shkzg='H',-dmbtr,dmbtr) else 0 end sale,
--case when (substr(belnr,1,1)='1' or substr(belnr,1,2)='01' or substr(belnr,1,3)='009')then if(shkzg='H',-dmbtr,dmbtr) else 0 end income
--from dw.fin_csx_bsad_fct
--where sdt=${hiveconf:current_day1} and budat<=${hiveconf:yesterday}
--and mandt='800' 
--and hkont like '1122%'

  select distinct concat_ws('-',belnr,kunnr,bukrs,budat,buzei) as id,bukrs,kunnr,buzei,budat,dmbtr,
  case when (substr(belnr,1,1)<>'1' and substr(belnr,1,2)<>'01' and substr(belnr,1,3)<>'009') then dmbtr else 0 end sale,
  case when (substr(belnr,1,1)='1' or substr(belnr,1,2)='01' or substr(belnr,1,3)='009')then dmbtr else 0 end income
  from ods_ecc.ecc_ytbcustomer
  where sdt=${hiveconf:current_day} 
  and budat<=${hiveconf:yesterday} 
  and mandt='800'  --sdt budat月初刷数据-当前分区刷月底
  and hkont like '1122%'
)a 
left join 
( 
  select 
    customer_no,
    company_code,
    payment_terms,
    cast(payment_days as int) diff 
  from csx_dw.dws_crm_w_a_customer_company
  where sdt = 'current'
)c on lpad(a.kunnr, 10, '0') = lpad(c.customer_no, 10, '0') and a.bukrs = c.company_code
left join
 (select * 
from csx_dw.dws_basic_w_a_csx_shop_m where sdt='current'
)d on lpad(a.kunnr,10,'0')=lpad(concat('S', d.shop_id),10,'0')
left join
 (select * 
from csx_dw.dws_crm_w_a_customer where sdt='current' 
)e on lpad(a.kunnr,10,'0')=lpad(e.customer_no,10,'0')
left join (select distinct code as company_code,name as company_name from csx_dw.dws_basic_w_a_company_code where sdt='current')f on a.bukrs = f.company_code;




--临时表0.2 应收
drop table csx_tmp.temp_out;
CREATE temporary table csx_tmp.temp_out
as
select a.*, 
row_number() OVER(PARTITION BY bukrs,kunnr ORDER BY budat asc)rno,
sum(amount)over(PARTITION BY bukrs,kunnr order by budat asc ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING ) sum_sq,
sum(amount)over(PARTITION BY bukrs,kunnr order by budat asc)sum_bq
from (select bukrs,kunnr,cast(budat as int)budat,edate,payment_terms,payment_days,sum(dmbtr)amount
 from csx_tmp.temp_account  a where dmbtr>=0 
 group by bukrs,kunnr,budat,edate,payment_terms,payment_days)a;
 
 
--临时表0.3 已收
 drop table csx_tmp.temp_in;
 CREATE temporary table csx_tmp.temp_in
 as
 select bukrs,kunnr,sum(dmbtr)amount
 from csx_tmp.temp_account  a where dmbtr<0 
 group by bukrs,kunnr;


--临时表0.4 日期金额--逾期用
 drop table csx_tmp.temp_left;
 CREATE temporary table csx_tmp.temp_left
 as
 --已收账款不足应收账款 
 select a.bukrs,a.kunnr,a.budat,a.edate,payment_terms,payment_days,
 case when coalesce(a.sum_sq,0)+b.amount<0 then a.sum_bq+b.amount else a.amount end amount
 ,a.rno,a.sum_bq+b.amount amount_left
 from csx_tmp.temp_out a 
 join csx_tmp.temp_in b on (a.bukrs=b.bukrs and a.kunnr=b.kunnr)
 where a.sum_bq+b.amount>=0
 --已收账款超过应收账款
  union all 
  select a.bukrs,a.kunnr,a.budat,a.edate,payment_terms,payment_days,
  a.sum_bq+b.amount amount
 ,a.rno,a.sum_bq+b.amount amount_left
 from csx_tmp.temp_out a 
 join (select bukrs,kunnr,max(rno)rno_max from csx_tmp.temp_out group by bukrs,kunnr)c 
 on (a.bukrs=c.bukrs and a.kunnr=c.kunnr and a.rno=c.rno_max)
 join csx_tmp.temp_in b on (a.bukrs=b.bukrs and a.kunnr=b.kunnr)
where a.sum_bq+b.amount<0 
--只有应收没有收款
 union all 
 select a.bukrs,a.kunnr,a.budat,a.edate,payment_terms,payment_days,
 a.amount
 ,a.rno,a.sum_bq amount_left
 from csx_tmp.temp_out a 
 left join csx_tmp.temp_in b on (a.bukrs=b.bukrs and a.kunnr=b.kunnr)
 where b.amount is null
--只有预付没有应收款
union all 
select 
a.bukrs,a.kunnr,a.budat,a.edate,payment_terms,payment_days,
  a.amount amount
 ,null rno,a.amount amount_left
from 
(select bukrs,kunnr,cast(budat as int)budat,edate,payment_terms,payment_days,sum(dmbtr)amount
 from csx_tmp.temp_account a where  dmbtr<0
 group by bukrs,kunnr,budat,edate,payment_terms,payment_days)a 
left join (select bukrs,kunnr,sum(amount)amount from csx_tmp.temp_out group by bukrs,kunnr)c 
on (a.bukrs=c.bukrs and a.kunnr=c.kunnr)
where c.amount is null;




-- 第二部分
-- 临时表1 客户信息提取，如部分客户5月没有过机回款记录，但有对账或开票，该月也需展示
drop table csx_tmp.temp_account0;
create temporary table csx_tmp.temp_account0
as 
select distinct
	sales_province_code, 
	sales_province,
	kunnr,
	sales_city,
	bukrs -- 结算公司主体
 from csx_tmp.temp_account;
 
 
-- 临时表2 客户月度合计
drop table csx_tmp.temp_account01;
create temporary table csx_tmp.temp_account01
as
select sales_province_code,sales_province,sales_city,kunnr,bukrs,smonth,
sum(sale) sale,sum(income) income,sum(ac_all) ac_all,sum(kp_amt) kp_amt,sum(dz_amt) dz_amt
from
	(select
	coalesce(a.sales_province_code,b.sales_province_code) sales_province_code,
	coalesce(a.sales_province,b.sales_province)sales_province,
	coalesce(a.sales_city,b.sales_city)sales_city,
	coalesce(a.kunnr,b.kunnr)kunnr,
	coalesce(a.bukrs,b.bukrs)bukrs, -- 结算公司主体
	a.smonth,
	a.sale,a.income,a.ac_all,a.kp_amt,a.dz_amt
	from 
		(select
			a.sales_province_code, 
			a.sales_province,
			coalesce(a.kunnr,b.kunnr,c.kunnr)kunnr,
			a.sales_city,
			coalesce(a.bukrs,b.company_code,c.company_code)bukrs, -- 结算公司主体
			coalesce(a.smonth,b.smonth,c.smonth)smonth,
			a.sale,a.income,a.ac_all,b.kp_amt,c.dz_amt
		from
			(select
				sales_province_code, 
				sales_province,
				kunnr,
				sales_city,
				bukrs, -- 结算公司主体
				substr(budat,1,6)smonth,
				sum(sale)sale,
				sum(income)income,
				sum(sale)+sum(income) ac_all
			 from csx_tmp.temp_account
			group by sales_province_code,sales_province,kunnr,sales_city,bukrs,substr(budat,1,6) )a
		full join            --开票金额
			(select lpad(customer_code,10,'0')kunnr,company_code,substr(sdt,1,6) smonth,
				sum(value_tax_total) kp_amt
			from csx_ods.source_sss_r_d_invoice
			where sdt>'19990101'
			and sdt<=${hiveconf:yesterday} 
			and invoice_state in('2')
			group by customer_code,company_code,substr(sdt,1,6)
			)b on b.kunnr=a.kunnr and b.company_code=a.bukrs and b.smonth=a.smonth
		full join            --对账金额
			(select lpad(customer_code,10,'0')kunnr,company_code,substr(sdt,1,6) smonth,
				sum(statement_amount) dz_amt
			from csx_ods.source_sss_r_d_statement_account
			where sdt>'19990101'
			and sdt<=${hiveconf:yesterday} 
			and sdt <>'__HIVE_DEFAULT_PARTITION__'
			and statement_state in('20','21','30','40','50')
			group by customer_code,company_code,substr(sdt,1,6)
			)c on c.kunnr=a.kunnr and c.company_code=a.bukrs and c.smonth=a.smonth )a
	left join csx_tmp.temp_account0 b on b.kunnr=a.kunnr and b.bukrs=a.bukrs
	where b.kunnr is not null )a
	group by sales_province_code,sales_province,sales_city,kunnr,bukrs,smonth;



------------
-- 临时表3 客户小计 +临时表0.4逾期部分
drop table csx_tmp.temp_account001;
create temporary table csx_tmp.temp_account001
as
select
coalesce(a.sales_province_code,b.sales_province_code) sales_province_code,
coalesce(a.sales_province,b.sales_province)sales_province,
coalesce(a.sales_city,b.sales_city)sales_city,
coalesce(a.kunnr,b.kunnr)kunnr,
coalesce(a.bukrs,b.bukrs)bukrs, -- 结算公司主体
a.sale,a.income,a.kp_amt,a.dz_amt,a.ac_yq
from 
	(select 
	a.sales_province_code,
	a.sales_province,
	a.sales_city,
	coalesce(a.kunnr,d.kunnr,c.kunnr) kunnr,
	coalesce(a.bukrs,d.company_code,c.company_code) bukrs,
	a.sale,a.income,d.kp_amt,c.dz_amt,
	case when b.ac_yq>=0 then b.ac_yq else 0 end ac_yq
	from(
	select sales_province_code,sales_province,sales_city,kunnr,bukrs,
	sum(sale) sale,
	sum(income) income
	--case when round((sum(if(sale>0,if(edate<to_date(current_date),sale,0),sale))+sum(income)),2)<0 then 0
	--	 else round((sum(if(sale>0,if(edate<to_date(current_date),sale,0),sale))+sum(income)),2)
	--	 end ac_yq
	from csx_tmp.temp_account
	group by sales_province_code,sales_province,sales_city,kunnr,bukrs) a
	left join (select kunnr,bukrs,
	 sum(case when datediff(${hiveconf:yesterday1}, concat(substr(budat,1,4),'-',substr(budat,5,2),'-',substr(budat,7,2))) >=0 then amount else 0 end) ac_all,
	 sum(case when edate<${hiveconf:yesterday1} then amount else 0 end) ac_yq
	 --sum(case when edate>=${hiveconf:yesterday1} then amount else 0 end) ac_wdq
	 from csx_tmp.temp_left
	 group by kunnr,bukrs) b on b.kunnr=a.kunnr and b.bukrs=a.bukrs
	left join            --开票金额
		(select lpad(customer_code,10,'0')kunnr,company_code,
			sum(value_tax_total) kp_amt
		from csx_ods.source_sss_r_d_invoice
		where sdt>'19990101'
		and sdt<=${hiveconf:yesterday} --刷数目标日期
		and invoice_state in('2')
		group by customer_code,company_code
		)d on d.kunnr=a.kunnr and d.company_code=a.bukrs 
	left join            --对账金额
		(select lpad(customer_code,10,'0')kunnr,company_code,
			sum(statement_amount) dz_amt
		from csx_ods.source_sss_r_d_statement_account
		where sdt>'19990101'
		and sdt<=${hiveconf:yesterday} --刷数目标日期
		and sdt <>'__HIVE_DEFAULT_PARTITION__'
		and statement_state in('20','21','30','40','50')
		group by customer_code,company_code
		)c on c.kunnr=a.kunnr and c.company_code=a.bukrs )a
left join csx_tmp.temp_account0 b on b.kunnr=a.kunnr and b.bukrs=a.bukrs ;


 
-- 临时表4 客户应收应付统计表 有多余辅助字段
drop table csx_tmp.temp_account02;
create temporary table csx_tmp.temp_account02
as 
select 
a.sales_province_code,
a.sales_province,
a.sales_city,
if(b.channel_name is not null and b1.shop_name is not null,'商超',b.channel_name) as channel,
a.bukrs, -- 结算公司主体
c.company_code, -- 结算公司主体
c.company_name,
a.kunnr,
coalesce(b.customer_name,b1.shop_name) as customer_name,
a.smonth,
a.grouping_id,
coalesce(d.credit_limit,e.credit_limit,f.credit_limit,'-') credit_limit,
coalesce(d.temp_credit_limit,e.temp_credit_limit,f.temp_credit_limit,'-') temp_credit_limit,
'' distribution_channel,
b.first_category_name as first_category,
b.second_category_name as second_category,
b.third_category_name as third_category,
coalesce(d.payment_terms,e.payment_terms,f.payment_terms,'-') payment_terms,
coalesce(d.payment_name,e.payment_name,f.payment_name,'-') payment_name,
b.sales_id,
b.sales_name,
b.first_supervisor_code,
b.first_supervisor_name,
a.sale,a.income,a.ac_all,a.kp_amt,a.dz_amt,
if(a.ac_yq<0,0,a.ac_yq) ac_yq 
from  
(select
sales_province_code, 
sales_province,
kunnr,
sales_city,
bukrs, -- 结算公司主体
smonth,
3 as grouping_id,
sale,income,ac_all,kp_amt,dz_amt,
'' as ac_yq
 from csx_tmp.temp_account01 

union all 
select  -- 客户小计、省区合计
sales_province_code,
sales_province,
kunnr,
sales_city,
bukrs,
case when sales_province is not null and kunnr is null then '合计'
   else '小计' end smonth,
case when sales_province is not null and kunnr is null then 0
   else 2 end grouping_id,
sale,
income,
sale+income ac_all,kp_amt,dz_amt,
ac_yq
from (
select sales_province_code,sales_province,sales_city,kunnr,bukrs,
sum(kp_amt) kp_amt,
sum(dz_amt) dz_amt,
sum(sale) sale,
sum(income) income,
sum(ac_yq) ac_yq 
from csx_tmp.temp_account001
group by sales_province_code,sales_province,sales_city,kunnr,bukrs 
grouping sets((sales_province_code,sales_province),(sales_province_code,sales_province,sales_city,kunnr,bukrs))
) a

union all 
select  -- 城市合计
sales_province_code,
sales_province,
'' kunnr,
sales_city,
'' bukrs,
'合计' smonth,
1 as grouping_id,
sale,
income,
sale+income ac_all,kp_amt,dz_amt,
ac_yq
from (
select sales_province_code,sales_province,sales_city,
sum(kp_amt) kp_amt,
sum(dz_amt) dz_amt,
sum(sale) sale,
sum(income) income,
sum(ac_yq) ac_yq 
from csx_tmp.temp_account001
where sales_city <>''
group by sales_province_code,sales_province,sales_city 
) b
) a
left join 
(
select * 
from csx_dw.dws_crm_w_a_customer 
where sdt=${hiveconf:yesterday} --刷数目标日期
and customer_no<>''
) b on lpad(a.kunnr,10,'0')=lpad(b.customer_no,10,'0')
left join
 (select * 
from csx_dw.dws_basic_w_a_csx_shop_m where sdt='current'
)b1 on lpad(a.kunnr,10,'0')=lpad(concat('S', b1.shop_id),10,'0')
left join -- 结算公司主体
(select 
distinct a.company_code,
a.company_name 
from csx_dw.dws_basic_w_a_csx_shop_m a 
where sdt='current'
) c on a.bukrs=c.company_code
left join -- 客户各月信控额度、临时信控额度、账期类型、账期 csx_dw.dws_crm_w_a_customer_company
(
select distinct a.customer_no,
a.company_code,
a.payment_terms,
a.payment_name,
a.credit_limit,
a.temp_credit_limit,
substr(if(a.sdt='current',regexp_replace(current_date,'-',''),a.sdt),1,6) smonth
from csx_dw.dws_crm_w_a_customer_company a
right join (
select customer_no,
company_code,
substr(if(sdt='current',regexp_replace(current_date,'-',''),sdt),1,6) smonth,
max(if(sdt='current',regexp_replace(current_date,'-',''),sdt)) max_sdt
from csx_dw.dws_crm_w_a_customer_company
group by customer_no,
company_code,
substr(if(sdt='current',regexp_replace(current_date,'-',''),sdt),1,6)
) b on b.customer_no=a.customer_no and b.company_code=a.company_code and b.max_sdt=if(a.sdt='current',regexp_replace(current_date,'-',''),a.sdt)
) d on lpad(d.customer_no,10,'0')=lpad(a.kunnr,10,'0') and d.company_code=a.bukrs and d.smonth=a.smonth
left join -- 客户各月信控额度、临时信控额度、账期类型、账期 csx_dw.dws_crm_w_a_customer_company
(
select distinct a.customer_no,
a.company_code,
a.payment_terms,
a.payment_name,
a.credit_limit,
a.temp_credit_limit,
substr(a.sdt,1,6) smonth
from (
select *
from csx_dw.dws_crm_w_a_customer_company 
where customer_no<>''
)a
right join (
select customer_no,
company_code,
substr(sdt,1,6) smonth,
max(sdt) max_sdt
from csx_dw.dws_crm_w_a_customer_company
where customer_no<>''
group by customer_no,
company_code,
substr(sdt,1,6)
) b on b.customer_no=a.customer_no and b.company_code=a.company_code and b.max_sdt=a.sdt
) e on lpad(e.customer_no,10,'0')=lpad(a.kunnr,10,'0') and e.company_code=a.bukrs and e.smonth=a.smonth
left join -- 客户小计信控额度、临时信控额度、账期类型、账期 
(
select distinct customer_no customer_no,
company_code,
'小计' smonth,
payment_terms,
payment_name,
credit_limit,
temp_credit_limit
from csx_dw.dws_crm_w_a_customer_company 
where sdt='current'
and customer_no<>''
)f on lpad(f.customer_no,10,'0')=lpad(a.kunnr,10,'0') and f.company_code=a.bukrs and f.smonth=a.smonth
order by a.sales_province_code,a.sales_province,a.sales_city,a.kunnr,a.bukrs,a.grouping_id,a.smonth;







set hive.map.aggr = true;
set hive.groupby.skewindata=false;
set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true; -- 开启动态分析
set hive.exec.dynamic.partition.mode=nonstrict; -- 动态分区模式
set hive.exec.max.dynamic.partitions.pernode=10000;
-- 结果表1 客户应收应付统计表
insert overwrite table csx_dw.fixation_report_customer_sale_income1 partition(sdt)
select 
  sales_province_code,
  sales_province,
  sales_city,
  channel,
  kunnr,
  -- bukrs, -- 结算公司主体
  customer_name,
  smonth,
  case when smonth='合计' then '' else credit_limit end credit_limit,
  case when smonth='合计' then '' else temp_credit_limit end temp_credit_limit,
  distribution_channel,
  first_category,
  second_category,
  third_category,
  case when smonth='合计' then '' else payment_terms end payment_terms,
  case when smonth='合计' then '' else payment_name end payment_days,
  company_name as sap_merchant_name,
  company_code as sap_merchant_code, -- 结算公司主体
  sales_id,
  sales_name,
  first_supervisor_code,
  first_supervisor_name,
  round(dz_amt,2) dz_amt,  
  round(kp_amt,2) kp_amt,
  round(sale,2) sale,
  round(income,2) income,
  round(ac_all,2) ac_all,
  round(ac_yq,2) ac_yq,
  ${hiveconf:yesterday}  as sdt 
from csx_tmp.temp_account02;


-- 结果表2 省区应收应付
insert overwrite table csx_dw.fixation_report_province_sale_income1 partition(sdt)
select 
  sales_province_code,
  sales_province,
  sales_city,
  smonth,
  dz_amt, 
  kp_amt,  
  sale,
  income,
  ac_all,
  sdt 
from
(select 
  NO_1,
  sales_province_code,
  sales_province,
  sales_city,
  smonth,
  round(dz_amt,2) dz_amt,  
  round(kp_amt,2) kp_amt,  
  round(sale,2) sale,
  round(income,2) income,
  round(ac_all,2) ac_all,
  ${hiveconf:yesterday}  as sdt 
from(
select
  if(smonth is null, '100000',smonth) NO_1,
  sales_province_code,
  sales_province,
  sales_city,
  if(smonth is null, '合计',smonth)smonth,
  case when sales_province is not null and sales_city is null then 0 
     when sales_province is not null and sales_city is not null then 1
     else 2 end grouping_id,
  sum(sale) sale,
  sum(income) income,
  sum(sale)+sum(income) ac_all,
  sum(kp_amt) kp_amt,
  sum(dz_amt) dz_amt
from csx_tmp.temp_account01
where sales_province in('福建省','重庆市','北京市','江苏省','浙江省')
group by sales_province_code,sales_province,sales_city,smonth 
grouping sets((sales_province_code,sales_province),(sales_province_code,sales_province,sales_city),(sales_province_code,sales_province,smonth),(sales_province_code,sales_province,sales_city,smonth))

union all 
select 
  if(smonth is null, '100000',smonth) NO_1,  
  sales_province_code,
  sales_province,
  sales_city,
  if(smonth is null, '合计',smonth) smonth,
  case when sales_province is not null and sales_city is null then 0 
     when sales_province is not null and sales_city is not null then 1
     else 2 end grouping_id,
  sum(sale) sale,
  sum(income) income,
  sum(sale)+sum(income) ac_all,
  sum(kp_amt) kp_amt,
  sum(dz_amt) dz_amt 
from csx_tmp.temp_account01
where sales_province not in('福建省','重庆市','北京市','江苏省','浙江省')
group by sales_province_code,sales_province,sales_city,smonth 
grouping sets((sales_province_code,sales_province),(sales_province_code,sales_province,smonth))
) a
order by sales_province_code,sales_city,NO_1) a;


insert overwrite table csx_dw.fixation_report_customer_sale_income_output
select
  '' as id,
  *
from csx_dw.fixation_report_customer_sale_income1
where sdt<=regexp_replace(date_sub(current_date,1),'-','')
  and sdt>=regexp_replace(date_sub(current_date,61),'-','')
;

insert overwrite table csx_dw.fixation_report_province_sale_income_output
select
 '' as id,
  *
from csx_dw.fixation_report_province_sale_income1
where sdt<=regexp_replace(date_sub(current_date,1),'-','')
  and sdt>=regexp_replace(date_sub(current_date,61),'-','');