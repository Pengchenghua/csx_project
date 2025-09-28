
--hive 标讯TOP信息回收
drop table if exists csx_tmp.biaoxun_list_top;
create table csx_tmp.biaoxun_list_top(
`id`	string	COMMENT	'ID',
`month`	string	COMMENT	'年月',
`month_0`	string	COMMENT	'月份',
`bid_date`	string	COMMENT	'投标日期',
`province_name`	string	COMMENT	'省区',
`region_name`	string	COMMENT	'大区',
`city_name`	string	COMMENT	'城市',
`original_category_name`	string	COMMENT	'行业（大类）',
`second_category_name`	string	COMMENT	'二级行业',
`zh_category_name`	string	COMMENT	'整合行业',
`item_name`	string	COMMENT	'投标项目',
`item_no`	string	COMMENT	'项目编号',
`original_customer`	string	COMMENT	'客户名称',
`tender_company`	string	COMMENT	'招标公司名称',
`subject_amount`	decimal(20,6)	COMMENT	'标的金额（万）',
`business_type_name`	string	COMMENT	'业务类型',
`previous_work`	string	COMMENT	'前期工作',
`bid_result`	string	COMMENT	'是否中标',
`bid_win_amount`	decimal(20,6)	COMMENT	'中标金额（万）',
`result_reason`	string	COMMENT	'成败原因',
`remarks`	string	COMMENT	'备注',
`customer_business_no`	string	COMMENT	'客户号或商机号',
`customer_no`	string	COMMENT	'客户编号',
`business_number`	string	COMMENT	'商机号'

) COMMENT '标讯TOP信息回收'
row format delimited fields terminated by ','
STORED AS TEXTFILE;


load data inpath '/tmp/raoyanhua/biaoxun_list_top.csv' overwrite into table csx_tmp.biaoxun_list_top;
select * from csx_tmp.biaoxun_list_top;


--标讯对应商机
drop table csx_tmp.tmp_biaoxun_business;
create table csx_tmp.tmp_biaoxun_business
as
select '2' as id,'SJ22070800011' as business_number
union all  select '3' as id,'SJ22070800012' as business_number
union all  select '11' as id,'SJ22032600006' as business_number
union all  select '12' as id,'SJ22032600007' as business_number
union all  select '13' as id,'SJ22032600037' as business_number
union all  select '14' as id,'SJ22032600004' as business_number
union all  select '15' as id,'SJ22032600016' as business_number
union all  select '18' as id,'SJ22062200013' as business_number
union all  select '29' as id,'SJ21091722951' as business_number
union all  select '42' as id,'SJ21091713718' as business_number
union all  select '43' as id,'SJ21091702234' as business_number
union all  select '44' as id,'SJ21091702234' as business_number
union all  select '45' as id,'SJ22011500034' as business_number
union all  select '46' as id,'SJ22011500034' as business_number
union all  select '47' as id,'SJ22041600042' as business_number
union all  select '48' as id,'SJ22051602190' as business_number
union all  select '48' as id,'SJ22060100015' as business_number
union all  select '49' as id,'SJ22050900054' as business_number
union all  select '50' as id,'SJ22041102131' as business_number
union all  select '74' as id,'SJ22020400002' as business_number
union all  select '75' as id,'SJ22032800001' as business_number
union all  select '78' as id,'SJ22070100028' as business_number
union all  select '79' as id,'SJ22052000052' as business_number
union all  select '80' as id,'SJ22052700061' as business_number
union all  select '81' as id,'SJ22040700050' as business_number
union all  select '82' as id,'SJ22070100028' as business_number
union all  select '87' as id,'SJ22070700046' as business_number
union all  select '88' as id,'SJ22042800009' as business_number
union all  select '89' as id,'SJ22052700042' as business_number
union all  select '90' as id,'SJ22063000027' as business_number;


--标讯对应客户
drop table csx_tmp.tmp_biaoxun_customer;
create table csx_tmp.tmp_biaoxun_customer
as
select distinct a.id,a.customer_no,e.first_order_month
from 
(
select '1' as id,'127075' as customer_no
union all  select '4' as id,'128199' as customer_no
union all  select '5' as id,'126984' as customer_no
union all  select '5' as id,'127016' as customer_no
union all  select '5' as id,'126979' as customer_no
union all  select '11' as id,'103145' as customer_no
union all  select '12' as id,'118318' as customer_no
union all  select '13' as id,'118724' as customer_no
union all  select '14' as id,'103207' as customer_no
union all  select '15' as id,'127634' as customer_no
union all  select '18' as id,'128057' as customer_no
union all  select '20' as id,'126465' as customer_no
union all  select '20' as id,'126391' as customer_no
union all  select '20' as id,'126958' as customer_no
union all  select '21' as id,'127670' as customer_no
union all  select '21' as id,'118504' as customer_no
union all  select '22' as id,'126270' as customer_no
union all  select '23' as id,'126293' as customer_no
union all  select '24' as id,'115947' as customer_no
union all  select '25' as id,'128211' as customer_no
union all  select '26' as id,'125815' as customer_no
union all  select '27' as id,'126690' as customer_no
union all  select '28' as id,'111207' as customer_no
union all  select '28' as id,'115537' as customer_no
union all  select '28' as id,'125534' as customer_no
union all  select '30' as id,'108810' as customer_no
union all  select '31' as id,'126319' as customer_no
union all  select '32' as id,'125604' as customer_no
union all  select '33' as id,'127132' as customer_no
union all  select '34' as id,'119213' as customer_no
union all  select '34' as id,'119235' as customer_no
union all  select '35' as id,'111612' as customer_no
union all  select '36' as id,'113054' as customer_no
union all  select '37' as id,'125834' as customer_no
union all  select '38' as id,'117671' as customer_no
union all  select '39' as id,'127082' as customer_no
union all  select '40' as id,'107592' as customer_no
union all  select '41' as id,'111130' as customer_no
union all  select '51' as id,'125412' as customer_no
union all  select '51' as id,'127655' as customer_no
union all  select '51' as id,'125434' as customer_no
union all  select '51' as id,'127499' as customer_no
union all  select '51' as id,'125429' as customer_no
union all  select '51' as id,'127613' as customer_no
union all  select '51' as id,'125428' as customer_no
union all  select '51' as id,'127569' as customer_no
union all  select '52' as id,'125096' as customer_no
union all  select '53' as id,'永辉投标' as customer_no
union all  select '54' as id,'111291' as customer_no
union all  select '54' as id,'118738' as customer_no
union all  select '54' as id,'118943' as customer_no
union all  select '54' as id,'120346' as customer_no
union all  select '54' as id,'120860' as customer_no
union all  select '55' as id,'126041' as customer_no
union all  select '56' as id,'127565' as customer_no
union all  select '56' as id,'127474' as customer_no
union all  select '56' as id,'127550' as customer_no
union all  select '56' as id,'127515' as customer_no
union all  select '57' as id,'已中标需走流程' as customer_no
union all  select '58' as id,'127333' as customer_no
union all  select '58' as id,'127341' as customer_no
union all  select '59' as id,'127375' as customer_no
union all  select '59' as id,'127260' as customer_no
union all  select '60' as id,'113366' as customer_no
union all  select '61' as id,'119841' as customer_no
union all  select '62' as id,'105790' as customer_no
union all  select '63' as id,'125897' as customer_no
union all  select '64' as id,'119663' as customer_no
union all  select '65' as id,'126112' as customer_no
union all  select '66' as id,'126124' as customer_no
union all  select '66' as id,'128005' as customer_no
union all  select '67' as id,'128074' as customer_no
union all  select '67' as id,'128095' as customer_no
union all  select '67' as id,'128083' as customer_no
union all  select '67' as id,'128087' as customer_no
union all  select '67' as id,'128033' as customer_no
union all  select '67' as id,'128090' as customer_no
union all  select '67' as id,'128077' as customer_no
union all  select '67' as id,'128105' as customer_no
union all  select '67' as id,'128084' as customer_no
union all  select '67' as id,'128086' as customer_no
union all  select '67' as id,'128069' as customer_no
union all  select '67' as id,'128110' as customer_no
union all  select '68' as id,'127760' as customer_no
union all  select '68' as id,'127755' as customer_no
union all  select '68' as id,'127751' as customer_no
union all  select '68' as id,'127543' as customer_no
union all  select '68' as id,'127729' as customer_no
union all  select '68' as id,'127698' as customer_no
union all  select '68' as id,'127750' as customer_no
union all  select '68' as id,'127728' as customer_no
union all  select '68' as id,'127739' as customer_no
union all  select '68' as id,'127745' as customer_no
union all  select '68' as id,'127592' as customer_no
union all  select '68' as id,'127743' as customer_no
union all  select '68' as id,'127767' as customer_no
union all  select '68' as id,'127766' as customer_no
union all  select '68' as id,'127746' as customer_no
union all  select '68' as id,'127753' as customer_no
union all  select '68' as id,'127775' as customer_no
union all  select '68' as id,'127754' as customer_no
union all  select '68' as id,'127747' as customer_no
union all  select '68' as id,'127756' as customer_no
union all  select '68' as id,'127553' as customer_no
union all  select '68' as id,'127737' as customer_no
union all  select '69' as id,'125883' as customer_no
union all  select '70' as id,'127002' as customer_no
union all  select '70' as id,'127312' as customer_no
union all  select '70' as id,'127336' as customer_no
union all  select '71' as id,'127969' as customer_no
union all  select '72' as id,'127707' as customer_no
union all  select '73' as id,'128134' as customer_no
union all  select '76' as id,'126579' as customer_no
union all  select '77' as id,'永辉门店合作' as customer_no
union all  select '78' as id,'128290' as customer_no
union all  select '82' as id,'128290' as customer_no
union all  select '83' as id,'124815' as customer_no
union all  select '84' as id,'122660' as customer_no
union all  select '85' as id,'125831' as customer_no
union all  select '86' as id,'115769' as customer_no
union all  select '91' as id,'125720' as customer_no
union all  select '92' as id,'127123' as customer_no
union all  select '93' as id,'127927' as customer_no
union all  select '93' as id,'128050' as customer_no
union all  select '94' as id,'还未建信控' as customer_no

--商机里的客户
union all
select distinct a.id,b.customer_no
from csx_tmp.tmp_biaoxun_business a 
left join 
(
	select b.business_number,c.customer_no
	from
	(
		select 
			id customer_id,		 --	客户ID
			business_number		 --	商机号	 
		from csx_dw.dws_crm_w_a_business_customer
		where sdt='current'
	)b 
	left join 
		(
		select customer_id,customer_no
		from csx_dw.dws_crm_w_a_customer
		where sdt='current' 
		)c on b.customer_id=c.customer_id	
)b on a.business_number=b.business_number
where b.customer_no<>''
)a
--客户首次销售日期，区分老客新标
left join
(
	select customer_no,first_order_date,
	substr(first_order_date,1,6) first_order_month
	from csx_dw.dws_crm_w_a_customer_active
	where sdt = 'current'
)e on e.customer_no=a.customer_no	
;

--标讯对应客户 区分新老客
drop table csx_tmp.tmp_biaoxun_customer_new_old;
create table csx_tmp.tmp_biaoxun_customer_new_old
as
select a.id,a.customer_no,a.first_order_month,b.month,
case when a.first_order_month<b.month then '老客' else '新客' end cust_new_old
from csx_tmp.tmp_biaoxun_customer a 
left join csx_tmp.biaoxun_list_top b on a.id=b.id;


--商机信息
drop table csx_tmp.tmp_business_information;
create temporary table csx_tmp.tmp_business_information
as
select a.*,
	c.customer_no,
	--coalesce(c.customer_no,f.customer_no) customer_no,
	e.first_order_date,	
	f.id as id_cust,
	g.id as id_business,
	coalesce(f.id,g.id) as id
from
	(
	select 
		id customer_id,		 --	客户ID
		business_number,		 --	商机号
		customer_name,		 --	客户名称
		--first_category_code,		 --	一级客户分类编码
		first_category_name,		 --	一级客户分类名称
		--second_category_code,		 --	二级客户分类编码
		second_category_name,		 --	二级客户分类名称
		--third_category_code,		 --	三级客户分类编码
		third_category_name,		 --	三级客户分类名称
		attribute,		 --	商机属性编码
		attribute_desc,		 --	机属性
		sales_region_name region_name,		 --	销售大区名称(业绩划分)
		sales_province_name province_name,		 --	销售归属省区名称
		city_group_name,		 --	城市组名称(业绩划分)
		--sales_id,		 --	主销售员Id
		work_no,		 --	销售员工号
		sales_name,		 --	销售员名称
		--first_supervisor_work_no,first_supervisor_name,
		--third_supervisor_work_no,third_supervisor_name,			
		--business_stage,		 --	阶段状态 1.阶段1 2.阶段2 3.阶段3 4.阶段4 5.阶段5
		--status,		 --	是否有效 0.无效 1.有效
		case business_stage 
		when 1 then '10%商机'
		when 2 then '25%商机'
		when 3 then '50%商机'
		when 4 then '75%商机'
		when 5 then '100%商机' end business_stage,
		case status 
		when 0 then '停止跟进'
		when 1 then '正常跟进' end status,
		estimate_once_amount,		 --	预估一次性配送金额
		estimate_month_amount,		 --	预估月度配送金额
		estimate_contract_amount,		 --	预计合同签约金额
		gross_profit_rate,		 --	预计毛利率
		regexp_replace(substr(expect_sign_time, 1, 10), '-', '') as expect_sign_date,		 --	预计签约时间
		expect_execute_time,		 --	预计履约时间
		contract_cycle,		 --	合同周期
		regexp_replace(substr(sign_time, 1, 10), '-', '') sign_time,		 --	签约时间
		regexp_replace(substr(create_time, 1, 10), '-', '') create_time,		 --	创建时间
		sdt		 
	from csx_dw.dws_crm_w_a_business_customer
	where sdt='current'
	and regexp_replace(substr(create_time, 1, 10), '-', '')>='20210101'
	and (regexp_replace(substr(sign_time, 1, 10), '-', '')>='20210101' or sign_time is null)
	--and status='1'  --是否有效 0.无效 1.有效 (status=0,'停止跟进')
	)a			
left join 
	(
	select customer_id,customer_no
	from csx_dw.dws_crm_w_a_customer
	where sdt='current' 
	and customer_no<>''
	)c on a.customer_id=c.customer_id	
left join
	(
	select customer_no,first_order_date
	from csx_dw.dws_crm_w_a_customer_active
	where sdt = 'current'
	)e on e.customer_no=c.customer_no
left join csx_tmp.tmp_biaoxun_customer_new_old f on f.customer_no=c.customer_no
left join csx_tmp.tmp_biaoxun_business g on g.business_number=a.business_number
where f.id is not null or g.id is not null
;


--top标讯+商机+销售
drop table csx_tmp.tmp_bid_business_sale;
create  table csx_tmp.tmp_bid_business_sale
as
select 
	a.id,
	a.month,
	a.month_0,
	a.bid_date,
	a.province_name,
	a.region_name,
	a.city_name,
	a.original_category_name,
	a.second_category_name,
	a.zh_category_name,
	a.item_name,
	a.item_no,
	a.original_customer,
	a.tender_company,
	a.subject_amount,
	a.business_type_name,
	a.previous_work,
	a.bid_result,
	a.bid_win_amount,
	a.result_reason,
	a.remarks,
	a.customer_business_no,
	a.customer_id,		 --	客户ID
	a.business_number,		 --	商机号
	coalesce(a.customer_no,f.customer_no) customer_no,
	a.customer_name,		 --	客户名称
	a.attribute,		 --	商机属性编码
	a.attribute_desc,		 --	机属性
	a.work_no,		 --	销售员工号
	a.sales_name,		 --	销售员名称
	a.business_stage,
	a.status,
	a.estimate_once_amount,		 --	预估一次性配送金额
	a.estimate_month_amount,		 --	预估月度配送金额
	a.estimate_contract_amount,		 --	预计合同签约金额
	a.gross_profit_rate,		 --	预计毛利率
	a.expect_sign_date,		 --	预计签约时间
	a.expect_execute_time,		 --	预计履约时间
	a.contract_cycle,		 --	合同周期
	a.sign_time,		 --	签约时间
	a.create_time,		 --	创建时间
	a.id_cust,
	a.id_business,
	f.first_order_month,f.cust_new_old,
	c.smonth,
	case when a.month <= c.smonth then c.sales_value end sales_value,
	case when a.month <= c.smonth then c.profit end profit,
	case when a.month <= c.smonth then c.count_day end count_day
from
(
select 
	a.id,
	a.month,
	a.month_0,
	a.bid_date,
	a.province_name,
	a.region_name,
	a.city_name,
	a.original_category_name,
	a.second_category_name,
	a.zh_category_name,
	a.item_name,
	a.item_no,
	a.original_customer,
	a.tender_company,
	a.subject_amount,
	a.business_type_name,
	a.previous_work,
	a.bid_result,
	a.bid_win_amount,
	a.result_reason,
	a.remarks,
	a.customer_business_no,
	b.customer_id,		 --	客户ID
	b.business_number,		 --	商机号
	b.customer_no,
	b.customer_name,		 --	客户名称
	b.attribute,		 --	商机属性编码
	b.attribute_desc,		 --	机属性
	b.work_no,		 --	销售员工号
	b.sales_name,		 --	销售员名称
	b.business_stage,
	b.status,
	b.estimate_once_amount,		 --	预估一次性配送金额
	b.estimate_month_amount,		 --	预估月度配送金额
	b.estimate_contract_amount,		 --	预计合同签约金额
	b.gross_profit_rate,		 --	预计毛利率
	b.expect_sign_date,		 --	预计签约时间
	b.expect_execute_time,		 --	预计履约时间
	b.contract_cycle,		 --	合同周期
	b.sign_time,		 --	签约时间
	b.create_time,		 --	创建时间
	b.id_cust,
	b.id_business
	--b.id,
	--b.first_order_date,
--`(bid_date)?+.+`
--regexp_replace(to_date(from_unixtime(unix_timestamp(bid_date,'yyyy/MM/dd'))), '-', '') bid_date
from csx_tmp.biaoxun_list_top a
left join csx_tmp.tmp_business_information b on a.id=b.id
)a 
left join csx_tmp.tmp_biaoxun_customer_new_old f on f.id=a.id and f.customer_no=a.customer_no
left join
(
select customer_no,substr(sdt,1,6) smonth,
	sum(sales_value) sales_value,   --含税销售额
    sum(profit) profit,   --含税毛利
	count(distinct sdt) count_day
from csx_dw.dws_sale_r_d_detail
where sdt>='20220101'
and channel_code in('1','7','8','9')
group by customer_no,substr(sdt,1,6) 
)c on c.customer_no=f.customer_no
;

--select count(1)
--from csx_tmp.tmp_bid_business_sale;


--top标讯+销售
drop table csx_tmp.tmp_bid_sale;
create  table csx_tmp.tmp_bid_sale
as
select 
	a.id,
	a.month,
	a.month_0,
	a.bid_date,
	a.province_name,
	a.region_name,
	a.city_name,
	a.original_category_name,
	a.second_category_name,
	a.zh_category_name,
	a.item_name,
	a.item_no,
	a.original_customer,
	a.tender_company,
	a.subject_amount,
	a.business_type_name,
	a.previous_work,
	a.bid_result,
	a.bid_win_amount,
	a.result_reason,
	a.remarks,
	a.customer_business_no,
	b.count_business,
	d.count_customer,
	d.count_cust_old,
	c.smonth,
	--c.sales_value,c.profit,c.count_day
	case when a.month <= c.smonth then c.sales_value_old end sales_value_old,
	case when a.month <= c.smonth then c.sales_value end sales_value,
	case when a.month <= c.smonth then c.profit end profit
--`(bid_date)?+.+`
--regexp_replace(to_date(from_unixtime(unix_timestamp(bid_date,'yyyy/MM/dd'))), '-', '') bid_date
from csx_tmp.biaoxun_list_top a
--投标对应客户的销售
left join 
(
	select
		id,smonth,
			sum(sales_value) sales_value,   --含税销售额
			sum(case when cust_new_old='老客' then sales_value end) sales_value_old,
			sum(profit) profit   --含税毛利	
	from
	(
		select 
			distinct b.id,b.month,b.first_order_month,b.cust_new_old,
			c.customer_no,c.smonth,c.sales_value,c.profit,c.count_day
		from csx_tmp.tmp_biaoxun_customer_new_old b
		left join
		(
			select customer_no,substr(sdt,1,6) smonth,
				sum(sales_value) sales_value,   --含税销售额
				sum(profit) profit,   --含税毛利
				count(distinct sdt) count_day
			from csx_dw.dws_sale_r_d_detail
			where sdt>='20220101'
			and channel_code in('1','7','8','9')
			group by customer_no,substr(sdt,1,6) 
		)c on c.customer_no=b.customer_no
	)a
	group by id,smonth
)c on a.id=c.id
--投标对应的商机数
left join 
(
select id,count(business_number) count_business
from csx_tmp.tmp_biaoxun_business
group by id
)b on b.id=a.id
--投标对应的客户数
left join 
(
select id,count(customer_no) count_customer,
count(case when cust_new_old='老客' then customer_no end) count_cust_old
from csx_tmp.tmp_biaoxun_customer_new_old
--统计客户个数时只统计客户编号的，排除文本记录的
where customer_no rlike '[0-9]{6}'
group by id
)d on d.id=a.id
;



--以下客户2022年后无销售
select customer_no,first_order_date,last_order_date
from csx_dw.dws_crm_w_a_customer_active
where sdt = 'current'
and customer_no in('108810','111130','111291','105790','115769');



select *
from csx_tmp.tmp_bid_business_sale;

select *
from csx_tmp.tmp_bid_sale;


select id,count(1)
from csx_tmp.tmp_bid_business_sale
group by id;
