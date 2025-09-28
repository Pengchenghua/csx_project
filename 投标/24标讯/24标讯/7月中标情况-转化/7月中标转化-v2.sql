
--hive 标讯中标转化
drop table if exists csx_tmp.biaoxun_list_inversion;
create table csx_tmp.biaoxun_list_inversion(
`id`	string	COMMENT	'序号',
`business_director`	string	COMMENT	'商机指导人',
`business_principal`	string	COMMENT	'商机负责人',
`bid_principal`	string	COMMENT	'投标负责人',
`region_name`	string	COMMENT	'大区',
`province_name`	string	COMMENT	'省区',
`city_name`	string	COMMENT	'城市',
`month`	string	COMMENT	'年月',
`bid_date`	string	COMMENT	'投标日期',
`original_category_name`	string	COMMENT	'行业',
`second_category_name`	string	COMMENT	'二级行业',
`zh_category_name`	string	COMMENT	'整合行业',
`item_name`	string	COMMENT	'项目名称',
`item_no`	string	COMMENT	'项目编号',
`original_customer`	string	COMMENT	'采购单位',
`tender_company`	string	COMMENT	'招标公司名称',
`tender_company_tell`	string	COMMENT	'招标公司联系电话',
`subject_amount`	decimal(20,6)	COMMENT	'标的金额（万）',
`bid_win_number_max`	string	COMMENT	'最大可中包数/总包数',
`bid_win_amount_max`	decimal(20,6)	COMMENT	'最大可中标金额',
`supply_period`	string	COMMENT	'供应期限（月）',
`business_type_name`	string	COMMENT	'合作类型',
`old_new_customer`	string	COMMENT	'新老客户标识',
`business_type_past`	string	COMMENT	'历史合作类型',
`previous_work`	string	COMMENT	'前期工作',
`cooperation_form`	string	COMMENT	'合作形式',
`bid_company`	string	COMMENT	'投标主体',
`bid_result`	string	COMMENT	'中标情况',
`result_date`	string	COMMENT	'结果公布日期',
`bid_win_amount`	decimal(20,6)	COMMENT	'中标金额（万）',
`result_reason`	string	COMMENT	'成败/流标/废标/弃标原因',
`result`	string	COMMENT	'项目结果',
`bid_open`	string	COMMENT	'开标情况',
`remarks`	string	COMMENT	'备注',
`business_number`	string	COMMENT	'商机号',
`customer_no`	string	COMMENT	'客户号',
`remarks_1`	string	COMMENT	'备注'

) COMMENT '标讯中标转化'
row format delimited fields terminated by ','
STORED AS TEXTFILE;


load data inpath '/tmp/raoyanhua/biaoxun_list_inversion.csv' overwrite into table csx_tmp.biaoxun_list_inversion;
select * from csx_tmp.biaoxun_list_inversion;


--标讯对应商机
drop table csx_tmp.tmp_biaoxun_business;
create table csx_tmp.tmp_biaoxun_business
as
select '1' as id,'SJ21091715114' as business_number
union all  select '2' as id,'SJ22022502127' as business_number
union all  select '3' as id,'SJ22030400001' as business_number
union all  select '4' as id,'SJ22031100002' as business_number
union all  select '6' as id,'SJ22052000026' as business_number
union all  select '8' as id,'SJ22052100076' as business_number
union all  select '9' as id,'SJ22061502137' as business_number
union all  select '10' as id,'SJ22062700005' as business_number
union all  select '11' as id,'SJ22070100061' as business_number
union all  select '12' as id,'SJ22060600022' as business_number
union all  select '13' as id,'SJ22042200032' as business_number
union all  select '14' as id,'SJ22062202154' as business_number
union all  select '15' as id,'SJ22061600047' as business_number
union all  select '16' as id,'SJ22071100002' as business_number
union all  select '17' as id,'SJ22080400003' as business_number
union all  select '18' as id,'SJ22070200042' as business_number
union all  select '19' as id,'SJ22071401131' as business_number
union all  select '20' as id,'SJ21091714962' as business_number
union all  select '21' as id,'SJ22080800043' as business_number
union all  select '22' as id,'SJ22080800041' as business_number
union all  select '23' as id,'SJ22070100095' as business_number
union all  select '24' as id,'SJ22080500097' as business_number
union all  select '27' as id,'SJ22080800037' as business_number
union all  select '28' as id,'SJ22041800057' as business_number
union all  select '29' as id,'SJ22072800057' as business_number
union all  select '30' as id,'SJ22060100013' as business_number
union all  select '31' as id,'SJ22072700024' as business_number
union all  select '32' as id,'SJ22072300003' as business_number
union all  select '33' as id,'SJ22052100083' as business_number
union all  select '34' as id,'SJ22070600013' as business_number
union all  select '35' as id,'SJ22080200035' as business_number
union all  select '36' as id,'SJ22080500096' as business_number
union all  select '37' as id,'SJ22080500092' as business_number
union all  select '38' as id,'SJ22071800011' as business_number
union all  select '39' as id,'SJ21122300090' as business_number
union all  select '45' as id,'SJ22073100037' as business_number
union all  select '48' as id,'SJ22051900011' as business_number
union all  select '50' as id,'SJ22070200020' as business_number
union all  select '51' as id,'SJ22070602150' as business_number
union all  select '52' as id,'SJ22061500004' as business_number
union all  select '54' as id,'SJ22072900015' as business_number
union all  select '57' as id,'SJ21091709364' as business_number
union all  select '59' as id,'SJ22080300022' as business_number
union all  select '61' as id,'SJ22071000010' as business_number
union all  select '62' as id,'SJ22072700027' as business_number
union all  select '63' as id,'SJ22041900068' as business_number
union all  select '64' as id,'SJ22072700028' as business_number
union all  select '66' as id,'SJ22080400014' as business_number
union all  select '67' as id,'SJ22080300019' as business_number
union all  select '69' as id,'SJ22080400029' as business_number
union all  select '70' as id,'SJ22080200039' as business_number
union all  select '73' as id,'SJ22073100038' as business_number
union all  select '74' as id,'SJ22080800074' as business_number
union all  select '75' as id,'SJ22073000048' as business_number
union all  select '78' as id,'SJ21111700094' as business_number
union all  select '81' as id,'SJ21091714914' as business_number
union all  select '82' as id,'SJ21091705098' as business_number
union all  select '86' as id,'SJ22072900013' as business_number
union all  select '87' as id,'SJ21091722326' as business_number
union all  select '88' as id,'SJ22072800040' as business_number
union all  select '89' as id,'SJ21091713946' as business_number
union all  select '93' as id,'SJ22061400006' as business_number
union all  select '94' as id,'SJJ22080200014' as business_number
union all  select '95' as id,'SJ22062202130' as business_number
union all  select '96' as id,'SJ22062000025' as business_number
union all  select '97' as id,'SJ22070800007' as business_number
union all  select '98' as id,'SJ22080500031' as business_number
union all  select '101' as id,'SJ22072800025' as business_number
union all  select '102' as id,'SJ21091709105' as business_number
union all  select '107' as id,'SJ22072800060' as business_number
union all  select '109' as id,'SJ21091708383' as business_number
union all  select '111' as id,'SJ22051000031' as business_number
union all  select '112' as id,'SJ22071401112' as business_number
union all  select '113' as id,'SJ22073000044' as business_number
union all  select '114' as id,'SJ22071300033' as business_number
union all  select '100、105' as id,'SJ22030902146' as business_number
union all  select '25、26' as id,'SJ22080100020' as business_number
union all  select '25、26' as id,'SJ22072200038' as business_number
union all  select '49、53' as id,'SJ22062800024' as business_number
union all  select '90、91' as id,'SJ22052700001' as business_number
union all  select '99、104、106、108' as id,'SJ22072700049' as business_number
union all  select '201' as id,'SJ22080200007' as business_number
union all  select '202' as id,'SJ22080200043' as business_number
union all  select '203' as id,'SJ22062900006' as business_number
union all  select '204' as id,'SJ22030200024' as business_number
union all  select '205' as id,'SJ22051500024' as business_number
union all  select '206' as id,'SJ22080400004' as business_number
union all  select '208' as id,'SJ22070400006' as business_number
union all  select '209' as id,'SJ2207200002' as business_number
union all  select '210' as id,'SJ22070802151' as business_number
union all  select '212' as id,'SJ22072102133' as business_number
union all  select '213' as id,'SJ22080900065' as business_number
union all  select '214' as id,'SJ22072200015' as business_number
union all  select '301' as id,'SJ22081000020' as business_number
;


--标讯对应客户
drop table csx_tmp.tmp_biaoxun_customer;
create table csx_tmp.tmp_biaoxun_customer
as
select distinct a.id,a.customer_no,b.first_sign_date,e.first_order_date,e.first_order_month,
e.normal_first_order_date,e.welfare_first_order_date,e.bbc_first_order_date
from 
(
select '1' as id,'119853' as customer_no
union all  select '3' as id,'128672' as customer_no
union all  select '4' as id,'128357' as customer_no
union all  select '7' as id,'105673' as customer_no
union all  select '9' as id,'128385' as customer_no
union all  select '12' as id,'128656' as customer_no
union all  select '13' as id,'115380' as customer_no
union all  select '15' as id,'120726' as customer_no
union all  select '19' as id,'123698' as customer_no
union all  select '20' as id,'119661' as customer_no
union all  select '24' as id,'119078' as customer_no
union all  select '28' as id,'125105' as customer_no
union all  select '32' as id,'128435' as customer_no
union all  select '38' as id,'128637' as customer_no
union all  select '40' as id,'113423' as customer_no
union all  select '41' as id,'117348' as customer_no
union all  select '42' as id,'124447' as customer_no
union all  select '43' as id,'105283' as customer_no
union all  select '44' as id,'125970' as customer_no
union all  select '48' as id,'128507' as customer_no
union all  select '50' as id,'105941' as customer_no
union all  select '51' as id,'106504' as customer_no
union all  select '52' as id,'128397' as customer_no
union all  select '54' as id,'128597' as customer_no
union all  select '57' as id,'112954' as customer_no
union all  select '62' as id,'119117' as customer_no
union all  select '63' as id,'115769' as customer_no
union all  select '70' as id,'128691' as customer_no
union all  select '76' as id,'107934' as customer_no
union all  select '81' as id,'119604' as customer_no
union all  select '84' as id,'111602' as customer_no
union all  select '86' as id,'128617' as customer_no
union all  select '88' as id,'128620' as customer_no
union all  select '89' as id,'118386' as customer_no
union all  select '93' as id,'128281' as customer_no
union all  select '94' as id,'128649' as customer_no
union all  select '95' as id,'114179' as customer_no
union all  select '96' as id,'128014' as customer_no
union all  select '97' as id,'126561' as customer_no
union all  select '101' as id,'128592' as customer_no
union all  select '102' as id,'112664' as customer_no
union all  select '109' as id,'111885' as customer_no
union all  select '111' as id,'127144' as customer_no
union all  select '112' as id,'128745' as customer_no
union all  select '113' as id,'128618' as customer_no
union all  select '114' as id,'128339' as customer_no
union all  select '100、105' as id,'126084' as customer_no
union all  select '25、26' as id,'113556' as customer_no
union all  select '46、47' as id,'124522' as customer_no
union all  select '49、53' as id,'117217' as customer_no
union all  select '77、79、80' as id,'113396' as customer_no
union all  select '90、91' as id,'119407' as customer_no
union all  select '99、104、106、108' as id,'128616' as customer_no
union all  select '201' as id,'113476' as customer_no
union all  select '207' as id,'120465' as customer_no
union all  select '208' as id,'128594' as customer_no
union all  select '212' as id,'123632' as customer_no

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
--客户信息 首次签约时间
left join 
(
	select customer_id,customer_no,
	regexp_replace(substr(first_sign_time, 1, 10), '-', '') first_sign_date		 --	签约时间
	from csx_dw.dws_crm_w_a_customer
	where sdt='current' 
)b on b.customer_no=a.customer_no
--客户首次销售日期，区分老客新标
left join
(
	select customer_no,first_order_date,normal_first_order_date,welfare_first_order_date,bbc_first_order_date,
	substr(first_order_date,1,6) first_order_month
	from csx_dw.dws_crm_w_a_customer_active
	where sdt = 'current'
)e on e.customer_no=a.customer_no
;

--标讯对应客户 区分新老客
drop table csx_tmp.tmp_biaoxun_customer_new_old;
create table csx_tmp.tmp_biaoxun_customer_new_old
as
select b.id,a.customer_no,a.first_order_date,a.first_sign_date,b.month,
	a.normal_first_order_date,a.welfare_first_order_date,a.bbc_first_order_date,
	regexp_replace(from_unixtime(unix_timestamp(b.bid_date,'yyyy/mm/dd'),'yyyy-mm-dd'), '-', '') bid_date,
	case when a.first_order_date<regexp_replace(from_unixtime(unix_timestamp(b.bid_date,'yyyy/mm/dd'),'yyyy-mm-dd'), '-', '') 
			or a.first_order_date<a.first_sign_date
		then '老客户' 
	--when a.customer_no is null and b.old_new_customer='老客户' then '老客户'	
	else '新客户' end cust_new_old
from csx_tmp.tmp_biaoxun_customer a 
left join csx_tmp.biaoxun_list_inversion b on a.id=b.id;


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
		regexp_replace(regexp_replace(regexp_replace(regexp_replace(contract_cycle,
				'{"input":"', ''), 
				'","radio":"1","radioName":"年"}', '年'),
				'","radio":"2","radioName":"月"}', '月'), 
				'","radio":"3","radioName":"日"}', '日') contract_cycle,		 --	合同周期
		regexp_replace(substr(sign_time, 1, 10), '-', '') sign_time,		 --	签约时间
		regexp_replace(substr(create_time, 1, 10), '-', '') create_time,		 --	创建时间
		sdt		 
	from csx_dw.dws_crm_w_a_business_customer
	where sdt='current'
	and regexp_replace(substr(create_time, 1, 10), '-', '')>='20220301'
	and (regexp_replace(substr(sign_time, 1, 10), '-', '')>='20220301' or sign_time is null)
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
	select customer_no,first_order_date,normal_first_order_date,welfare_first_order_date,bbc_first_order_date
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
	a.id,	--序号
	a.business_director,	--	商机指导人
	a.business_principal,	--	商机负责人
	a.bid_principal,	--	投标负责人
	a.region_name,	--	大区
	a.province_name,	--	省区
	a.city_name,	--	城市
	a.month,	--	年月
	a.bid_date,	--	投标日期
	a.second_category_name,	--	二级行业
	a.zh_category_name,	--	整合行业
	a.item_name,	--	项目名称
	a.item_no,	--	项目编号
	a.original_customer,	--	采购单位
	a.subject_amount,	--	标的金额（万）
	a.bid_win_number_max,	--	最大可中包数/总包数
	a.bid_win_amount_max,	--	最大可中标金额
	a.supply_period,	--	供应期限（月）
	a.business_type_name,	--	合作类型
	a.old_new_customer,	--	新老客户标识
	a.business_type_past,	--	历史合作类型
	a.previous_work,	--	前期工作
	a.cooperation_form,	--	合作形式
	a.bid_company,	--	投标主体
	a.bid_result,	--	中标情况
	a.result_date,	--	结果公布日期
	a.bid_win_amount,	--	中标金额（万）
	--a.business_number,	--	商机号
	--a.customer_no,	--	客户号
	a.remarks_1,	--	备注
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
	f.first_order_date,--f.normal_first_order_date,f.welfare_first_order_date,f.bbc_first_order_date,
	--case when a.business_type_name='日配' and f.normal_first_order_date<a.bid_date then '老客户' 
	--	when a.business_type_name='福利' and (f.welfare_first_order_date<a.bid_date or f.bbc_first_order_date<a.bid_date) then '老客户'
	--	when a.business_type_name='日配、福利' and f.first_order_date<a.bid_date then '老客户'
	--	when coalesce(a.customer_no,f.customer_no) is null and a.old_new_customer='老客户' then '老客户'
	--	else '新客户' end cust_new_old,
	coalesce(f.cust_new_old,a.old_new_customer) cust_new_old,
	c.smonth,
	sum(case when a.bid_date <= c.sdt then c.sales_value end) sales_value,
	sum(case when a.bid_date <= c.sdt then c.rp_sales_value end) rp_sales_value,   --日配含税销售额
	sum(case when a.bid_date <= c.sdt then c.fl_sales_value end) fl_sales_value,   --福利含税销售额
	sum(case when a.bid_date <= c.sdt then c.bbc_sales_value end) bbc_sales_value,   --BBC含税销售额
	sum(case when a.bid_date <= c.sdt then c.bbc_sales_value end) csfws_sales_value,   --城市服务商含税销售额
	sum(case when a.bid_date <= c.sdt then c.bbc_sales_value end) ngdz_sales_value,   --内购大宗含税销售额
	sum(case when a.bid_date <= c.sdt then c.profit end) profit,
	sum(case when a.bid_date <= c.sdt then c.count_day end) count_day
from
(
select 
	a.id,	--序号
	a.business_director,	--	商机指导人
	a.business_principal,	--	商机负责人
	a.bid_principal,	--	投标负责人
	a.region_name,	--	大区
	a.province_name,	--	省区
	a.city_name,	--	城市
	a.month,	--	年月
	regexp_replace(from_unixtime(unix_timestamp(a.bid_date,'yyyy/mm/dd'),'yyyy-mm-dd'), '-', '') bid_date,	--	投标日期
	a.second_category_name,	--	二级行业
	a.zh_category_name,	--	整合行业
	a.item_name,	--	项目名称
	a.item_no,	--	项目编号
	a.original_customer,	--	采购单位
	a.subject_amount,	--	标的金额（万）
	a.bid_win_number_max,	--	最大可中包数/总包数
	a.bid_win_amount_max,	--	最大可中标金额
	a.supply_period,	--	供应期限（月）
	a.business_type_name,	--	合作类型
	a.old_new_customer,	--	新老客户标识
	a.business_type_past,	--	历史合作类型
	a.previous_work,	--	前期工作
	a.cooperation_form,	--	合作形式
	a.bid_company,	--	投标主体
	a.bid_result,	--	中标情况
	a.result_date,	--	结果公布日期
	a.bid_win_amount,	--	中标金额（万）
	--a.business_number,	--	商机号
	--a.customer_no,	--	客户号
	a.remarks_1,	--	备注
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
from csx_tmp.biaoxun_list_inversion a
left join csx_tmp.tmp_business_information b on a.id=b.id
)a 
left join csx_tmp.tmp_biaoxun_customer_new_old f on f.id=a.id --and f.customer_no=a.customer_no
left join
(
select customer_no,sdt,substr(sdt,1,6) smonth,
	sum(sales_value) sales_value,   --含税销售额
	--业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
	sum(case when business_type_code='1' then sales_value end) rp_sales_value,   --日配含税销售额
	sum(case when business_type_code='2' then sales_value end) fl_sales_value,   --福利含税销售额
	sum(case when business_type_code='6' then sales_value end) bbc_sales_value,   --BBC含税销售额
	sum(case when business_type_code='4' then sales_value end) csfws_sales_value,   --城市服务商含税销售额
	sum(case when business_type_code in('3','4') then sales_value end) ngdz_sales_value,   --内购大宗含税销售额
    sum(profit) profit,   --含税毛利
	count(distinct sdt) count_day
from csx_dw.dws_sale_r_d_detail
where sdt>='20220701'
and channel_code in('1','7','8','9')
group by customer_no,sdt,substr(sdt,1,6) 
)c on c.customer_no=f.customer_no
group by
	a.id,	--序号
	a.business_director,	--	商机指导人
	a.business_principal,	--	商机负责人
	a.bid_principal,	--	投标负责人
	a.region_name,	--	大区
	a.province_name,	--	省区
	a.city_name,	--	城市
	a.month,	--	年月
	a.bid_date,	--	投标日期
	a.second_category_name,	--	二级行业
	a.zh_category_name,	--	整合行业
	a.item_name,	--	项目名称
	a.item_no,	--	项目编号
	a.original_customer,	--	采购单位
	a.subject_amount,	--	标的金额（万）
	a.bid_win_number_max,	--	最大可中包数/总包数
	a.bid_win_amount_max,	--	最大可中标金额
	a.supply_period,	--	供应期限（月）
	a.business_type_name,	--	合作类型
	a.old_new_customer,	--	新老客户标识
	a.business_type_past,	--	历史合作类型
	a.previous_work,	--	前期工作
	a.cooperation_form,	--	合作形式
	a.bid_company,	--	投标主体
	a.bid_result,	--	中标情况
	a.result_date,	--	结果公布日期
	a.bid_win_amount,	--	中标金额（万）
	--a.business_number,	--	商机号
	--a.customer_no,	--	客户号
	a.remarks_1,	--	备注
	a.customer_id,		 --	客户ID
	a.business_number,		 --	商机号
	coalesce(a.customer_no,f.customer_no),
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
	f.first_order_date,--f.normal_first_order_date,f.welfare_first_order_date,f.bbc_first_order_date,
	--case when a.business_type_name='日配' and f.normal_first_order_date<a.bid_date then '老客' 
	--	when a.business_type_name='福利' and (f.welfare_first_order_date<a.bid_date or f.bbc_first_order_date<a.bid_date) then '老客'
	--	when a.business_type_name='日配、福利' and f.first_order_date<a.bid_date then '老客'
	--	when coalesce(a.customer_no,f.customer_no) is null and a.old_new_customer='老客' then '老客'
	--	else '新客' end,
	coalesce(f.cust_new_old,a.old_new_customer),
	c.smonth
;

--select count(1)
--from csx_tmp.tmp_bid_business_sale;


--top标讯+销售
drop table csx_tmp.tmp_bid_sale;
create  table csx_tmp.tmp_bid_sale
as
select 
	a.id,	--序号
	a.business_director,	--	商机指导人
	a.business_principal,	--	商机负责人
	a.bid_principal,	--	投标负责人
	a.region_name,	--	大区
	a.province_name,	--	省区
	a.city_name,	--	城市
	a.month,	--	年月
	regexp_replace(from_unixtime(unix_timestamp(a.bid_date,'yyyy/mm/dd'),'yyyy-mm-dd'), '-', '') bid_date,	--	投标日期
	a.second_category_name,	--	二级行业
	a.zh_category_name,	--	整合行业
	a.item_name,	--	项目名称
	a.item_no,	--	项目编号
	a.original_customer,	--	采购单位
	a.subject_amount,	--	标的金额（万）
	a.bid_win_number_max,	--	最大可中包数/总包数
	a.bid_win_amount_max,	--	最大可中标金额
	a.supply_period,	--	供应期限（月）
	a.business_type_name,	--	合作类型
	a.old_new_customer,	--	新老客户标识
	a.business_type_past,	--	历史合作类型
	a.previous_work,	--	前期工作
	a.cooperation_form,	--	合作形式
	a.bid_company,	--	投标主体
	a.bid_result,	--	中标情况
	a.result_date,	--	结果公布日期
	if(a.bid_win_amount=0,e.estimate_contract_amount,a.bid_win_amount) bid_win_amount,	--	中标金额（万）
	e.estimate_contract_amount,		 --	预计合同签约金额
	if(d.id_cust_old>0,'老客户','新客户') id_cust_old,   --标讯新老客标签
	--a.business_number,	--	商机号
	--a.customer_no,	--	客户号
	b.count_business,
	d.count_customer,
	d.count_cust_old,
	c.smonth,
	--c.sales_value,c.profit,c.count_day
	sum(case when regexp_replace(from_unixtime(unix_timestamp(a.bid_date,'yyyy/mm/dd'),'yyyy-mm-dd'), '-', '') <= c.sdt then c.sales_value end) sales_value,
	sum(case when regexp_replace(from_unixtime(unix_timestamp(a.bid_date,'yyyy/mm/dd'),'yyyy-mm-dd'), '-', '') <= c.sdt then c.rp_sales_value end) rp_sales_value,   --日配含税销售额
	sum(case when regexp_replace(from_unixtime(unix_timestamp(a.bid_date,'yyyy/mm/dd'),'yyyy-mm-dd'), '-', '') <= c.sdt then c.fl_sales_value end) fl_sales_value,   --福利含税销售额
	sum(case when regexp_replace(from_unixtime(unix_timestamp(a.bid_date,'yyyy/mm/dd'),'yyyy-mm-dd'), '-', '') <= c.sdt then c.bbc_sales_value end) bbc_sales_value,   --BBC含税销售额	
	sum(case when regexp_replace(from_unixtime(unix_timestamp(a.bid_date,'yyyy/mm/dd'),'yyyy-mm-dd'), '-', '') <= c.sdt then c.csfws_sales_value end) csfws_sales_value,   --城市服务商含税销售额
	sum(case when regexp_replace(from_unixtime(unix_timestamp(a.bid_date,'yyyy/mm/dd'),'yyyy-mm-dd'), '-', '') <= c.sdt then c.ngdz_sales_value end) ngdz_sales_value,   --内购大宗含税销售额
	
	sum(case when regexp_replace(from_unixtime(unix_timestamp(a.bid_date,'yyyy/mm/dd'),'yyyy-mm-dd'), '-', '') <= c.sdt then c.sales_value_old end) sales_value_old,
	sum(case when regexp_replace(from_unixtime(unix_timestamp(a.bid_date,'yyyy/mm/dd'),'yyyy-mm-dd'), '-', '') <= c.sdt then c.rp_sales_value_old end) rp_sales_value_old,   --老客日配含税销售额
	sum(case when regexp_replace(from_unixtime(unix_timestamp(a.bid_date,'yyyy/mm/dd'),'yyyy-mm-dd'), '-', '') <= c.sdt then c.fl_sales_value_old end) fl_sales_value_old,   --老客福利含税销售额
	sum(case when regexp_replace(from_unixtime(unix_timestamp(a.bid_date,'yyyy/mm/dd'),'yyyy-mm-dd'), '-', '') <= c.sdt then c.bbc_sales_value_old end) bbc_sales_value_old,   --老客BBC含税销售额	
	sum(case when regexp_replace(from_unixtime(unix_timestamp(a.bid_date,'yyyy/mm/dd'),'yyyy-mm-dd'), '-', '') <= c.sdt then c.csfws_sales_value_old end) csfws_sales_value_old,   --老客城市服务商含税销售额
	sum(case when regexp_replace(from_unixtime(unix_timestamp(a.bid_date,'yyyy/mm/dd'),'yyyy-mm-dd'), '-', '') <= c.sdt then c.ngdz_sales_value_old end) ngdz_sales_value_old,   --老客内购大宗含税销售额	
	sum(case when regexp_replace(from_unixtime(unix_timestamp(a.bid_date,'yyyy/mm/dd'),'yyyy-mm-dd'), '-', '') <= c.sdt then c.profit end) profit
--`(bid_date)?+.+`
--regexp_replace(to_date(from_unixtime(unix_timestamp(bid_date,'yyyy/MM/dd'))), '-', '') bid_date
from csx_tmp.biaoxun_list_inversion a
--投标对应客户的销售
left join 
(
	select
		id,sdt,smonth,
			sum(sales_value) sales_value,   --含税销售额
			--业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
			sum(rp_sales_value) rp_sales_value,   --日配含税销售额
			sum(fl_sales_value) fl_sales_value,   --福利含税销售额
			sum(bbc_sales_value) bbc_sales_value,   --BBC含税销售额		
			sum(csfws_sales_value) csfws_sales_value,   --城市服务商含税销售额
			sum(ngdz_sales_value) ngdz_sales_value,   --内购大宗含税销售额			
			sum(case when cust_new_old='老客户' then sales_value end) sales_value_old,
			sum(case when cust_new_old='老客户' then rp_sales_value end) rp_sales_value_old,   --老客日配含税销售额
			sum(case when cust_new_old='老客户' then fl_sales_value end) fl_sales_value_old,   --老客福利含税销售额
			sum(case when cust_new_old='老客户' then bbc_sales_value end) bbc_sales_value_old,   --老客BBC含税销售额	
			sum(case when cust_new_old='老客户' then csfws_sales_value end) csfws_sales_value_old,   --老客城市服务商含税销售额
			sum(case when cust_new_old='老客户' then ngdz_sales_value end) ngdz_sales_value_old,   --老客内购大宗含税销售额			
			sum(profit) profit   --含税毛利	
	from
	(
		select distinct
			b.id,b.first_order_date,b.cust_new_old,
			c.customer_no,c.sdt,c.smonth,c.sales_value,c.rp_sales_value,c.fl_sales_value,c.bbc_sales_value,
			c.csfws_sales_value,c.ngdz_sales_value,c.profit,c.count_day
		from csx_tmp.tmp_biaoxun_customer_new_old b
		left join
		(
			select customer_no,sdt,substr(sdt,1,6) smonth,
				sum(sales_value) sales_value,   --含税销售额
				--业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
				sum(case when business_type_code='1' then sales_value end) rp_sales_value,   --日配含税销售额
				sum(case when business_type_code='2' then sales_value end) fl_sales_value,   --福利含税销售额
				sum(case when business_type_code='6' then sales_value end) bbc_sales_value,   --BBC含税销售额	
				sum(case when business_type_code='4' then sales_value end) csfws_sales_value,   --城市服务商含税销售额
				sum(case when business_type_code in('3','4') then sales_value end) ngdz_sales_value,   --内购大宗含税销售额				
				sum(profit) profit,   --含税毛利
				count(distinct sdt) count_day
			from csx_dw.dws_sale_r_d_detail
			where sdt>='20220701'
			and channel_code in('1','7','8','9')
			group by customer_no,sdt,substr(sdt,1,6) 
		)c on c.customer_no=b.customer_no
	)a
	group by id,sdt,smonth
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
count(case when cust_new_old='老客户' then customer_no end) count_cust_old,
count(case when cust_new_old='老客户' then id end) id_cust_old
from csx_tmp.tmp_biaoxun_customer_new_old
--统计客户个数时只统计客户编号的，排除文本记录的
--where customer_no rlike '[0-9]{6}'
group by id
)d on d.id=a.id
--投标对应的商机签约金额
left join 
(
select id,sum(estimate_contract_amount) estimate_contract_amount		 --	预计合同签约金额
from csx_tmp.tmp_business_information
where id_business is not null
group by id
)e on e.id=a.id
group by
	a.id,	--序号
	a.business_director,	--	商机指导人
	a.business_principal,	--	商机负责人
	a.bid_principal,	--	投标负责人
	a.region_name,	--	大区
	a.province_name,	--	省区
	a.city_name,	--	城市
	a.month,	--	年月
	regexp_replace(from_unixtime(unix_timestamp(a.bid_date,'yyyy/mm/dd'),'yyyy-mm-dd'), '-', ''),	--	投标日期
	a.second_category_name,	--	二级行业
	a.zh_category_name,	--	整合行业
	a.item_name,	--	项目名称
	a.item_no,	--	项目编号
	a.original_customer,	--	采购单位
	a.subject_amount,	--	标的金额（万）
	a.bid_win_number_max,	--	最大可中包数/总包数
	a.bid_win_amount_max,	--	最大可中标金额
	a.supply_period,	--	供应期限（月）
	a.business_type_name,	--	合作类型
	a.old_new_customer,	--	新老客户标识
	a.business_type_past,	--	历史合作类型
	a.previous_work,	--	前期工作
	a.cooperation_form,	--	合作形式
	a.bid_company,	--	投标主体
	a.bid_result,	--	中标情况
	a.result_date,	--	结果公布日期
	a.bid_win_amount,	--	中标金额（万）
	e.estimate_contract_amount,		 --	预计合同签约金额
	if(d.id_cust_old>0,'老客户','新客户'),   --标讯新老客标签
	--a.business_number,	--	商机号
	--a.customer_no,	--	客户号
	b.count_business,
	d.count_customer,
	d.count_cust_old,
	c.smonth
;



--以下客户2022年后无销售
select customer_no,first_order_date,last_order_date
from csx_dw.dws_crm_w_a_customer_active
where sdt = 'current'
and customer_no in('108810','111130','111291','105790','115769');



select *
from csx_tmp.tmp_bid_business_sale
order by id;

select *
from csx_tmp.tmp_bid_sale
order by id;


select id,count(1)
from csx_tmp.tmp_bid_business_sale
group by id;
