
--hive 标讯中标转化
drop table if exists csx_tmp.biaoxun_list_inversion_tbb;
create table csx_tmp.biaoxun_list_inversion_tbb(
`id`	string	COMMENT	'序号',
`region_name`	string	COMMENT	'大区',
`province_name`	string	COMMENT	'省区',
`bid_principal_name`	string	COMMENT	'投标_人员姓名',
`bid_principal_number`	string	COMMENT	'投标_人员工号',
`begin_date`	string	COMMENT	'入职日期',
`pos_title`	string	COMMENT	'职务',
`item_name`	string	COMMENT	'项目名称',
`item_no`	string	COMMENT	'项目编号',
`subject_amount`	decimal(20,6)	COMMENT	'标的金额（万）',
`result_date`	string	COMMENT	'中标日期',
`remarks`	string	COMMENT	'备注',
`remarks_1`	string	COMMENT	'备注',
`business_number`	string	COMMENT	'商机号',
`customer_no`	string	COMMENT	'客户号'
) COMMENT '标讯中标转化_投标部'
row format delimited fields terminated by ','
STORED AS TEXTFILE;


load data inpath '/tmp/raoyanhua/biaoxun_list_inversion_tbb.csv' overwrite into table csx_tmp.biaoxun_list_inversion_tbb;
select * from csx_tmp.biaoxun_list_inversion_tbb;


--标讯对应商机
drop table csx_tmp.tmp_biaoxun_business;
create table csx_tmp.tmp_biaoxun_business
as
select '2' as id,'SJ22090900028' as business_number
union all  select '77' as id,'SJ22080300033' as business_number
union all  select '78' as id,'SJ22082300049' as business_number
union all  select '79' as id,'SJ22082400076' as business_number
union all  select '80' as id,'SJ22051400030' as business_number
union all  select '85' as id,'SJ22080300022' as business_number
union all  select '86、87' as id,'SJ22080300019' as business_number
union all  select '89' as id,'SJ22072102137' as business_number
union all  select '90' as id,'SJ22081200046' as business_number
union all  select '92' as id,'SJ22072900022' as business_number
union all  select '93' as id,'SJ21092800030' as business_number
union all  select '94' as id,'SJ22070800020' as business_number
union all  select '98' as id,'SJ22073100038' as business_number
union all  select '99' as id,'SJ22080800074' as business_number
union all  select '100' as id,'SJ22081700011' as business_number
union all  select '101' as id,'SJ22072900013' as business_number
union all  select '102' as id,'SJ22083000079' as business_number
union all  select '122' as id,'SJ22090600002' as business_number
union all  select '132' as id,'SJ22080900065' as business_number
union all  select '133' as id,'SJ22080500096' as business_number
union all  select '136' as id,'SJ22070600013' as business_number
union all  select '139、140' as id,'SJ22080500001' as business_number
union all  select '141' as id,'SJ22082200025' as business_number
union all  select '143' as id,'SJ22082200026' as business_number
union all  select '156' as id,'SJ22080200032' as business_number
;


--标讯对应客户
drop table csx_tmp.tmp_biaoxun_customer;
create table csx_tmp.tmp_biaoxun_customer
as
select distinct a.id,a.customer_no,b.first_sign_date,e.first_order_date,e.first_order_month,
e.normal_first_order_date,e.welfare_first_order_date,e.bbc_first_order_date
from 
(
select '1' as id,'115848' as customer_no
union all  select '3' as id,'120151' as customer_no
union all  select '4' as id,'120307' as customer_no
union all  select '5' as id,'107655' as customer_no
union all  select '6' as id,'124873' as customer_no
union all  select '7、8' as id,'121397' as customer_no
union all  select '9' as id,'128950' as customer_no
union all  select '10' as id,'128748' as customer_no
union all  select '11' as id,'120660' as customer_no
union all  select '12' as id,'129103' as customer_no
union all  select '13' as id,'129207' as customer_no
union all  select '14' as id,'128772' as customer_no
union all  select '15' as id,'128739' as customer_no
union all  select '16' as id,'107738' as customer_no
union all  select '17' as id,'128949' as customer_no
union all  select '18' as id,'120497' as customer_no
union all  select '19' as id,'120344' as customer_no
union all  select '21' as id,'128640' as customer_no
union all  select '22' as id,'113833' as customer_no
union all  select '22' as id,'113861' as customer_no
union all  select '22' as id,'113875' as customer_no
union all  select '22' as id,'114120' as customer_no
union all  select '22' as id,'119037' as customer_no
union all  select '22' as id,'119038' as customer_no
union all  select '22' as id,'119090' as customer_no
union all  select '22' as id,'119107' as customer_no
union all  select '22' as id,'127498' as customer_no
union all  select '22' as id,'127522' as customer_no
union all  select '23' as id,'128618' as customer_no
union all  select '24' as id,'128616' as customer_no
union all  select '25' as id,'129182' as customer_no
union all  select '26' as id,'129370' as customer_no
union all  select '73、74' as id,'119296' as customer_no
union all  select '75' as id,'118121' as customer_no
union all  select '76' as id,'128142' as customer_no
union all  select '81' as id,'107909' as customer_no
union all  select '82' as id,'116753' as customer_no
union all  select '83' as id,'117816' as customer_no
union all  select '84' as id,'128327' as customer_no
union all  select '88' as id,'112766' as customer_no
union all  select '91' as id,'117902' as customer_no
union all  select '95' as id,'122079' as customer_no
union all  select '96' as id,'129175' as customer_no
union all  select '97' as id,'123895' as customer_no
union all  select '103、104、105' as id,'112189' as customer_no
union all  select '110' as id,'117284' as customer_no
union all  select '111' as id,'128777' as customer_no
union all  select '112' as id,'120417' as customer_no
union all  select '113' as id,'128597' as customer_no
union all  select '114' as id,'115210' as customer_no
union all  select '115' as id,'120427' as customer_no
union all  select '116' as id,'128762' as customer_no
union all  select '117' as id,'115191' as customer_no
union all  select '118' as id,'129020' as customer_no
union all  select '119' as id,'129032' as customer_no
union all  select '123、124' as id,'113556' as customer_no
union all  select '126' as id,'113476' as customer_no
union all  select '128' as id,'105687' as customer_no
union all  select '129' as id,'128789' as customer_no
union all  select '129' as id,'128981' as customer_no
union all  select '131' as id,'129022' as customer_no
union all  select '134' as id,'122072' as customer_no
union all  select '135' as id,'128788' as customer_no
union all  select '137' as id,'128703' as customer_no
union all  select '138' as id,'128955' as customer_no
union all  select '142' as id,'129008' as customer_no
union all  select '144' as id,'128886' as customer_no
union all  select '145' as id,'128435' as customer_no
union all  select '146' as id,'114179' as customer_no
union all  select '147' as id,'114910' as customer_no
union all  select '148' as id,'128649' as customer_no
union all  select '149' as id,'119114' as customer_no
union all  select '150' as id,'119099' as customer_no
union all  select '151' as id,'127144' as customer_no
union all  select '152' as id,'128745' as customer_no
union all  select '153' as id,'124788' as customer_no
union all  select '154' as id,'128778' as customer_no
union all  select '155' as id,'123423' as customer_no

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
select b.id,a.customer_no,a.first_order_date,a.first_sign_date,
	a.normal_first_order_date,a.welfare_first_order_date,a.bbc_first_order_date,
	b.result_date,
	case when a.first_order_date<b.result_date
			or a.first_sign_date<b.result_date
		then '老客户' 
	--when a.customer_no is null and b.old_new_customer='老客户' then '老客户'	
	--else '新客户' 
	end cust_new_old
from csx_tmp.tmp_biaoxun_customer a 
left join csx_tmp.biaoxun_list_inversion_tbb b on a.id=b.id;


--商机信息
drop table csx_tmp.tmp_business_information;
create table csx_tmp.tmp_business_information
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
		business_stage business_stage_code,		 --	阶段状态 1.阶段1 2.阶段2 3.阶段3 4.阶段4 5.阶段5
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
		expect_execute_time,
		contract_cycle,		 --	合同周期		--	预计履约时间
		--regexp_replace(regexp_replace(regexp_replace(regexp_replace(contract_cycle,
		--		'{"input":"', ''), 
		--		'","radio":"1","radioName":"年"}', '年'),
		--		'","radio":"2","radioName":"月"}', '月'), 
		--		'","radio":"3","radioName":"日"}', '日') contract_cycle,		 --	合同周期
		regexp_replace(substr(sign_time, 1, 10), '-', '') sign_time,		 --	签约时间
		regexp_replace(substr(create_time, 1, 10), '-', '') create_time,		 --	创建时间
		sdt		 
	from csx_dw.dws_crm_w_a_business_customer
	where sdt='current'
	--and regexp_replace(substr(create_time, 1, 10), '-', '')>='20211201'
	--and (regexp_replace(substr(sign_time, 1, 10), '-', '')>='20211201' or sign_time is null)
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
a.id,	--	序号
a.region_name,	--	大区
a.province_name,	--	省区
a.bid_principal_name,	--	投标_人员姓名
a.bid_principal_number,	--	投标_人员工号
a.begin_date,	--	入职日期
a.pos_title,	--	职务
a.item_name,	--	项目名称
a.item_no,	--	项目编号
a.subject_amount,	--	标的金额（万）
a.result_date,	--	中标日期
a.remarks,	--	备注
a.remarks_1,	--	备注

	a.customer_id,		 --	客户ID
	a.business_number,		 --	商机号
	coalesce(a.customer_no,f.customer_no) customer_no,
	a.customer_name,		 --	客户名称
	a.attribute,		 --	商机属性编码
	a.attribute_desc,		 --	机属性
	a.work_no,		 --	销售员工号
	a.sales_name,		 --	销售员名称
	a.business_stage_code,
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
	--case when a.business_type_name='日配' and f.normal_first_order_date<a.result_date then '老客户' 
	--	when a.business_type_name='福利' and (f.welfare_first_order_date<a.result_date or f.bbc_first_order_date<a.result_date) then '老客户'
	--	when a.business_type_name='日配、福利' and f.first_order_date<a.result_date then '老客户'
	--	when coalesce(a.customer_no,f.customer_no) is null and a.old_new_customer='老客户' then '老客户'
	--	else '新客户' end cust_new_old,
	f.cust_new_old,
	c.smonth,
	sum(case when a.result_date <= c.sdt then c.sales_value end) sales_value,
	sum(case when a.result_date <= c.sdt then c.rp_sales_value end) rp_sales_value,   --日配含税销售额
	sum(case when a.result_date <= c.sdt then c.fl_sales_value end) fl_sales_value,   --福利含税销售额
	sum(case when a.result_date <= c.sdt then c.bbc_sales_value end) bbc_sales_value,   --BBC含税销售额
	sum(case when a.result_date <= c.sdt then c.bbc_sales_value end) csfws_sales_value,   --城市服务商含税销售额
	sum(case when a.result_date <= c.sdt then c.bbc_sales_value end) ngdz_sales_value,   --内购大宗含税销售额
	sum(case when a.result_date <= c.sdt then c.profit end) profit,
	sum(case when a.result_date <= c.sdt then c.count_day end) count_day
from
(
select 
a.id,	--	序号
a.region_name,	--	大区
a.province_name,	--	省区
a.bid_principal_name,	--	投标_人员姓名
a.bid_principal_number,	--	投标_人员工号
a.begin_date,	--	入职日期
a.pos_title,	--	职务
a.item_name,	--	项目名称
a.item_no,	--	项目编号
a.subject_amount,	--	标的金额（万）
a.result_date,	--	中标日期
a.remarks,	--	备注
a.remarks_1,	--	备注
	b.customer_id,		 --	客户ID
	b.business_number,		 --	商机号
	b.customer_no,
	b.customer_name,		 --	客户名称
	b.attribute,		 --	商机属性编码
	b.attribute_desc,		 --	机属性
	b.work_no,		 --	销售员工号
	b.sales_name,		 --	销售员名称
	b.business_stage_code,
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
--`(result_date)?+.+`
--regexp_replace(to_date(from_unixtime(unix_timestamp(result_date,'yyyy/MM/dd'))), '-', '') result_date
from csx_tmp.biaoxun_list_inversion_tbb a
left join csx_tmp.tmp_business_information b on a.id=b.id
)a 
left join csx_tmp.tmp_biaoxun_customer_new_old f on f.id=a.id and f.customer_no=a.customer_no
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
where sdt>='20211201'
and channel_code in('1','7','8','9')
group by customer_no,sdt,substr(sdt,1,6) 
)c on c.customer_no=f.customer_no
group by
a.id,	--	序号
a.region_name,	--	大区
a.province_name,	--	省区
a.bid_principal_name,	--	投标_人员姓名
a.bid_principal_number,	--	投标_人员工号
a.begin_date,	--	入职日期
a.pos_title,	--	职务
a.item_name,	--	项目名称
a.item_no,	--	项目编号
a.subject_amount,	--	标的金额（万）
a.result_date,	--	中标日期
a.remarks,	--	备注
a.remarks_1,	--	备注
	a.customer_id,		 --	客户ID
	a.business_number,		 --	商机号
	coalesce(a.customer_no,f.customer_no),
	a.customer_name,		 --	客户名称
	a.attribute,		 --	商机属性编码
	a.attribute_desc,		 --	机属性
	a.work_no,		 --	销售员工号
	a.sales_name,		 --	销售员名称
	a.business_stage_code,
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
	--case when a.business_type_name='日配' and f.normal_first_order_date<a.result_date then '老客' 
	--	when a.business_type_name='福利' and (f.welfare_first_order_date<a.result_date or f.bbc_first_order_date<a.result_date) then '老客'
	--	when a.business_type_name='日配、福利' and f.first_order_date<a.result_date then '老客'
	--	when coalesce(a.customer_no,f.customer_no) is null and a.old_new_customer='老客' then '老客'
	--	else '新客' end,
	f.cust_new_old,
	c.smonth
;

--select count(1)
--from csx_tmp.tmp_bid_business_sale;

  
--top标讯+销售
drop table csx_tmp.tmp_bid_sale;
create  table csx_tmp.tmp_bid_sale
as
select 
a.id,	--	序号
a.region_name,	--	大区
a.province_name,	--	省区
a.bid_principal_name,	--	投标_人员姓名
a.bid_principal_number,	--	投标_人员工号
a.begin_date,	--	入职日期
a.pos_title,	--	职务
a.item_name,	--	项目名称
a.item_no,	--	项目编号
a.subject_amount,	--	标的金额（万）
a.result_date,	--	中标日期
a.remarks,	--	备注
a.remarks_1,	--	备注
	e.estimate_contract_amount,		 --	预计合同签约金额
	if(d.id_cust_old>0,'老客户','新客户') id_cust_old,   --标讯新老客标签
	b1.business_stage business_stage_max,	
	b.count_business,
	d.count_customer,
	d.count_cust_old,
    f.business_number_list,
    g.customer_no_list,
    g.customer_name_list,	
	c.smonth,
	--c.sales_value,c.profit,c.count_day
	count(distinct case when a.result_date <= c.sdt then c.sdt end) count_days,
	sum(case when a.result_date <= c.sdt then c.sales_value end) sales_value,
	sum(case when a.result_date <= c.sdt then c.rp_sales_value end) rp_sales_value,   --日配含税销售额
	sum(case when a.result_date <= c.sdt then c.fl_sales_value end) fl_sales_value,   --福利含税销售额
	sum(case when a.result_date <= c.sdt then c.bbc_sales_value end) bbc_sales_value,   --BBC含税销售额	
	sum(case when a.result_date <= c.sdt then c.csfws_sales_value end) csfws_sales_value,   --城市服务商含税销售额
	sum(case when a.result_date <= c.sdt then c.ngdz_sales_value end) ngdz_sales_value,   --内购大宗含税销售额
	
	sum(case when a.result_date <= c.sdt then c.sales_value_old end) sales_value_old,
	sum(case when a.result_date <= c.sdt then c.rp_sales_value_old end) rp_sales_value_old,   --老客日配含税销售额
	sum(case when a.result_date <= c.sdt then c.fl_sales_value_old end) fl_sales_value_old,   --老客福利含税销售额
	sum(case when a.result_date <= c.sdt then c.bbc_sales_value_old end) bbc_sales_value_old,   --老客BBC含税销售额	
	sum(case when a.result_date <= c.sdt then c.csfws_sales_value_old end) csfws_sales_value_old,   --老客城市服务商含税销售额
	sum(case when a.result_date <= c.sdt then c.ngdz_sales_value_old end) ngdz_sales_value_old,   --老客内购大宗含税销售额	
	sum(case when a.result_date <= c.sdt then c.profit end) profit
--`(result_date)?+.+`
--regexp_replace(to_date(from_unixtime(unix_timestamp(result_date,'yyyy/MM/dd'))), '-', '') result_date
from csx_tmp.biaoxun_list_inversion_tbb a
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
				sum(case when business_type_code in('3','5') then sales_value end) ngdz_sales_value,   --内购大宗含税销售额				
				sum(profit) profit,   --含税毛利
				count(distinct sdt) count_day
			from csx_dw.dws_sale_r_d_detail
			where sdt>='20211201'
			and channel_code in('1','7','8','9')
			group by customer_no,sdt,substr(sdt,1,6) 
		)c on c.customer_no=b.customer_no
	)a
	group by id,sdt,smonth
)c on a.id=c.id
--投标对应的商机数 最快商机进度
left join 
(
select id,
count(business_number) count_business,
max(business_stage_code) business_stage_max
from csx_tmp.tmp_business_information
group by id
)b on b.id=a.id
left join 
(
select distinct business_stage_code,business_stage
from csx_tmp.tmp_business_information
)b1 on b1.business_stage_code=b.business_stage_max
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
--标讯对应商机列表
left join 
(
  select 
    id,
      concat_ws('：',collect_list(business_number)) as business_number_list
  from (select distinct id, business_number from csx_tmp.tmp_bid_business_sale)a
  group by id
)f on f.id=a.id 
--标讯对客户编号、客户名称列表
left join 
(
  select 
    id,
      concat_ws('：',collect_list(customer_no)) as customer_no_list,
      concat_ws('：',collect_list(customer_name)) as customer_name_list
  from (select distinct id, customer_no,customer_name from csx_tmp.tmp_bid_business_sale)a
  group by id
)g on g.id=a.id 
group by
a.id,	--	序号
a.region_name,	--	大区
a.province_name,	--	省区
a.bid_principal_name,	--	投标_人员姓名
a.bid_principal_number,	--	投标_人员工号
a.begin_date,	--	入职日期
a.pos_title,	--	职务
a.item_name,	--	项目名称
a.item_no,	--	项目编号
a.subject_amount,	--	标的金额（万）
a.result_date,	--	中标日期
a.remarks,	--	备注
a.remarks_1,	--	备注
	e.estimate_contract_amount,		 --	预计合同签约金额
	if(d.id_cust_old>0,'老客户','新客户'),   --标讯新老客标签
	b1.business_stage,	
	b.count_business,
	d.count_customer,
	d.count_cust_old,
    f.business_number_list,
    g.customer_no_list,
    g.customer_name_list,	
	c.smonth	
;



--以下客户2022年后无销售
--select customer_no,first_order_date,last_order_date
--from csx_dw.dws_crm_w_a_customer_active
--where sdt = 'current'
--and customer_no in('108810','111130','111291','105790','115769');



select *
from csx_tmp.tmp_bid_business_sale
order by id;

select *
from csx_tmp.tmp_bid_sale
order by id;


select id,count(1)
from csx_tmp.tmp_bid_business_sale
group by id;
