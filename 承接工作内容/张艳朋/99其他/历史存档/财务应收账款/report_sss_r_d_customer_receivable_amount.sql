
-- 动态分区
set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions =1000;
set hive.exec.max.dynamic.partitions.pernode =1000;

-- 中间结果压缩
set mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
set mapred.output.compression.type=BLOCK;
set parquet.compression=SNAPPY;

-- 启用引号识别
set hive.support.quoted.identifiers=none;

-- 昨日
set yesterday =regexp_replace(date_sub(current_date,1),'-','');
set created_time =from_utc_timestamp(current_timestamp(),'GMT');
set created_by='zhangyanpeng';

drop table if exists csx_tmp.tmp_cust_receivable_amount_00;
create table csx_tmp.tmp_cust_receivable_amount_00
as
select
	coalesce(c.sales_region_code,b.sales_region_code,'999') as region_code,
	coalesce(c.sales_region_name,b.sales_region_name,'其他') as region_name,  
	coalesce(c.province_code,b.sales_province_code,'999') as province_code,					 
	coalesce(c.province_name,b.sales_province_name,'其他') as province_name,
	coalesce(c.city_group_code,b.city_group_code,'999') as city_group_code,
	coalesce(c.city_group_name,b.city_group_name,'其他') as city_group_name, 
	a.smonth, --年月
	a.customer_code as customer_no,
	c.customer_name,
	a.company_code,
	d.company_name,
	max(a.max_overdue_day) as max_overdue_day,--最大逾期天数
	sum(a.tax_sale_amount) as tax_sale_amount, --销售金额
	sum(a.receivable_amount) as receivable_amount, --未核销应收金额
	sum(a.overdue_amount) as overdue_amount, --未核销逾期金额
	sum(a.bad_debt_amount) as bad_debt_amount,--坏账（账款调整金额）	
	sum(a.claim_amount) as claim_amount,	--认领金额（含核销与未核销的，含补救单）
	sum(a.paid_amount) as paid_amount,	--认领核销金额
	sum(a.residual_amount) as residual_amount,	--认领未核销金额
	sum(a.payment_amount) as payment_amount	--单据核销金额
from 
	(
	select
		substr(stat_date,1,6) smonth,customer_code,company_code,max_overdue_day,
		tax_sale_amount,receivable_amount,overdue_amount,bad_debt_amount,
		0 as claim_amount,	--认领金额（含核销与未核销的，含补救单）
		0 as paid_amount,	--认领核销金额
		0 as residual_amount,	--认领未核销金额
		0 as payment_amount	--单据核销金额
	from
		csx_dw.dws_sss_r_d_customer_settle_day_stat --客户开票对账认领核销日维度分布数据
	where 
		sdt=${hiveconf:yesterday}
		--and customer_code='105831'
		
	union all
	
	select
		a.smonth,a.customer_code,a.company_code,
		0 as max_overdue_day,--最大逾期天数
		0 as tax_sale_amount, --销售金额
		0 as receivable_amount, --未核销应收金额
		0 as overdue_amount, --未核销逾期金额
		0 as bad_debt_amount,--坏账（账款调整金额）
		sum(a.claim_amount) as claim_amount,	--认领金额（含核销与未核销的，含补救单）
		sum(a.paid_amount) as paid_amount,	--认领核销金额
		sum(a.residual_amount) as residual_amount,	--认领未核销金额
		sum(b.payment_amount) as payment_amount	--单据核销金额
	from
		(
		select
			claim_bill_no,regexp_replace(substr(posting_time,1,7),'-','') smonth,customer_code,company_code,
			sum(claim_amount) as claim_amount,
			sum(paid_amount) as paid_amount,	
			sum(residual_amount) as residual_amount	
		from 
			csx_dw.dwd_sss_r_d_money_back
		where 
			((sdt>='20200601' and sdt<=${hiveconf:yesterday}) 
			or (sdt='19990101' and regexp_replace(substr(posting_time,1,10),'-','')>='20200601' and regexp_replace(substr(posting_time,1,10),'-','')<=${hiveconf:yesterday}))
			and (paid_amount<>0 or residual_amount<>0) --剔除补救单和对应原单
			--and customer_code='105831'
		group by 
			claim_bill_no,regexp_replace(substr(posting_time,1,7),'-',''),customer_code,company_code  
		)a
		left join
			(	
			select   
				claim_bill_no,customer_code,company_code,
				sum(payment_amount) payment_amount	--核销金额
			from
				csx_dw.dwd_sss_r_d_close_bill_account_record_20200908
			where 
				(regexp_replace(substr(happen_date,1,10),'-','')<=${hiveconf:yesterday} or happen_date='' or happen_date is null)
				and is_deleted =0
				--and customer_code='105831'	
			group by 
				claim_bill_no,customer_code,company_code
			)b on b.claim_bill_no=a.claim_bill_no and b.customer_code=a.customer_code and b.company_code=a.company_code
	group by
		a.smonth,
		a.customer_code,
		a.company_code
		
	union all --加入微信支付订单，认领中无、核销中有
	
	select   
		substr(sdt,1,6) smonth,customer_code,company_code,
		0 as max_overdue_day,--最大逾期天数
		0 as tax_sale_amount, --销售金额
		0 as receivable_amount, --未核销应收金额
		0 as overdue_amount, --未核销逾期金额
		0 as bad_debt_amount,--坏账（账款调整金额）		
		sum(pay_on_line_amount) as claim_amount,
		sum(pay_on_line_amount) as paid_amount,	--认领核销金额
		0 as residual_amount,	--认领未核销金额
		sum(pay_on_line_amount) payment_amount	--单据核销金额
	from
		csx_dw.dwd_sss_r_d_source_bill
	where 
		sdt>='20200601'
		and sdt<=${hiveconf:yesterday}
		--and customer_code='105831'
	group by 
		substr(sdt,1,6),customer_code,company_code		
	) a 
	left join ( select * from csx_dw.dws_basic_w_a_csx_shop_m where sdt='current') b on a.customer_code=concat('S', b.shop_id)
	left join ( select * from csx_dw.dws_crm_w_a_customer where sdt='current') c on a.customer_code=c.customer_no
	left join ( select distinct company_code,company_name from csx_dw.dws_basic_w_a_csx_shop_m where sdt='current')d on a.company_code=d.company_code	
group by 
	coalesce(c.sales_region_code,b.sales_region_code,'999'),
	coalesce(c.sales_region_name,b.sales_region_name,'其他'),  
	coalesce(c.province_code,b.sales_province_code,'999'),					 
	coalesce(c.province_name,b.sales_province_name,'其他'),
	coalesce(c.city_group_code,b.city_group_code,'999'),
	coalesce(c.city_group_name,b.city_group_name,'其他'),
	a.smonth, --年月
	a.customer_code,
	c.customer_name,
	a.company_code,
	d.company_name
;

drop table if exists csx_tmp.tmp_cust_receivable_amount_01;
create table csx_tmp.tmp_cust_receivable_amount_01
as
select 
    region_code,region_name,province_code,province_name,city_group_code,city_group_name,smonth,3 as grouping_id,
    customer_no,customer_name,company_code,company_name,max_overdue_day,tax_sale_amount,receivable_amount,
	overdue_amount,bad_debt_amount,claim_amount,paid_amount,residual_amount,payment_amount	
from 
	csx_tmp.tmp_cust_receivable_amount_00
union all 
  -- 客户小计、省区合计、城市合计
select 
    region_code,region_name,province_code,province_name,coalesce(city_group_code,'-') as city_group_code,coalesce(city_group_name,'-') as city_group_name,
    if(customer_no is null,'合计','小计') smonth, 
    if(customer_no is null,if(city_group_code is null and customer_no is null,0,1),2) grouping_id,
    coalesce(customer_no,'-') as customer_no,
	coalesce(customer_name,'-') as customer_name, 
    coalesce(company_code,'-') as company_code,
    coalesce(company_name,'-') as company_name,
	max(max_overdue_day) max_overdue_day,
   	sum(tax_sale_amount) as tax_sale_amount, --销售金额
	sum(receivable_amount) as receivable_amount, --未核销应收金额
	sum(overdue_amount) as overdue_amount, --未核销逾期金额
	sum(bad_debt_amount) as bad_debt_amount,--坏账（账款调整金额）	
	sum(claim_amount) as claim_amount,	--认领金额（含核销与未核销的，含补救单）
	sum(paid_amount) as paid_amount,	--认领核销金额
	sum(residual_amount) as residual_amount,	--认领未核销金额
	sum(payment_amount) as payment_amount	--单据核销金额
from 
	csx_tmp.tmp_cust_receivable_amount_00
group by 
	region_code,region_name,province_code,province_name,city_group_code,city_group_name,customer_no,customer_name,company_code,company_name
grouping sets(
    (region_code,region_name,province_code,province_name),
    (region_code,region_name,province_code,province_name,city_group_code,city_group_name),
    (region_code,region_name,province_code,province_name,city_group_code,city_group_name,customer_no,customer_name,company_code,company_name))
;


--结果表 客户应收账款
insert overwrite table csx_dw.report_sss_r_d_customer_receivable_amount partition(sdt)

select
	concat_ws('-',${hiveconf:yesterday},a.customer_no,a.company_code,a.province_code,a.city_group_code,a.smonth) as biz_id,
	a.region_code,a.region_name,a.province_code,a.province_name,a.city_group_code,a.city_group_name,d.channel_code,d.channel_name,
	a.smonth,a.grouping_id,a.customer_no,a.customer_name,a.company_code,a.company_name, 
	coalesce(b.payment_terms,c.payment_terms,'-') payment_terms,--账期类型
	coalesce(b.payment_name,c.payment_name,'-') payment_name,
	coalesce(b.payment_days,c.payment_days,'-') payment_days,
	coalesce(b.payment_short_name,c.payment_short_name,'-') payment_short_name, 
	coalesce(b.credit_limit,c.credit_limit,'-') credit_limit,
	coalesce(b.temp_credit_limit,c.temp_credit_limit,'-') temp_credit_limit,
	d.first_category_code,d.first_category_name,d.second_category_code,d.second_category_name,d.third_category_code,d.third_category_name,		
	d.sales_id,d.work_no,d.sales_name,d.first_supervisor_code,d.first_supervisor_work_no,d.first_supervisor_name,d.dev_source_code,
	d.dev_source_name,e.customer_active_status_code,e.customer_active_status_name,a.tax_sale_amount,a.receivable_amount,
	if(a.overdue_amount<0,0,overdue_amount) as overdue_amount,a.bad_debt_amount,a.claim_amount,a.paid_amount,a.residual_amount,a.payment_amount,a.max_overdue_day,
	${hiveconf:created_by} create_by,
	${hiveconf:created_time} create_time,
	${hiveconf:created_time} update_time,
	${hiveconf:yesterday} as sdt -- 统计日期  
from
	(
	select
		region_code,region_name,province_code,province_name,city_group_code,city_group_name,smonth,grouping_id,customer_no,customer_name,company_code,company_name,
		max_overdue_day,tax_sale_amount,receivable_amount,overdue_amount,bad_debt_amount,claim_amount,paid_amount,residual_amount,payment_amount
	from	
		csx_tmp.tmp_cust_receivable_amount_01
	) a 
	left join
		(
		select
			distinct b.customer_no,b.company_code,b.payment_terms,b.payment_name,b.payment_days,b.payment_short_name,b.credit_limit,b.temp_credit_limit,
			substr(if(b.sdt='current',regexp_replace(current_date,'-',''),b.sdt),1,6) smonth
		from
			(
			select 
				customer_no,company_code,
				substr(if(sdt='current',regexp_replace(current_date,'-',''),sdt),1,6) smonth,
				max(if(sdt='current',regexp_replace(current_date,'-',''),sdt)) max_sdt
			from 
				csx_dw.dws_crm_w_a_customer_company
			group by 
				customer_no,company_code,substr(if(sdt='current',regexp_replace(current_date,'-',''),sdt),1,6)
			) a 
			left join csx_dw.dws_crm_w_a_customer_company b 
				on b.customer_no=a.customer_no and b.company_code=a.company_code and a.max_sdt=if(b.sdt='current',regexp_replace(current_date,'-',''),b.sdt)
		) b on b.customer_no=a.customer_no and b.company_code=a.company_code and b.smonth=a.smonth
	left join -- 客户小计信控额度、临时信控额度、账期类型、账期 
		(
		select 
			distinct customer_no customer_no,company_code,'小计' smonth,payment_terms,payment_name,payment_days,payment_short_name,credit_limit,temp_credit_limit
		from 
			csx_dw.dws_crm_w_a_customer_company 
		where 
			sdt='current'
		)c on c.customer_no=a.customer_no and c.company_code=a.company_code and c.smonth=a.smonth
	left join ( select * from csx_dw.dws_crm_w_a_customer where sdt='current') d on a.customer_no=d.customer_no
	left join
		(
		select 
			distinct customer_no,sign_company_code,last_sales_date,last_to_now_days,customer_active_status_code,
			case when  customer_active_status_code = 1 then '活跃客户'
				when customer_active_status_code = 2 then '沉默客户'
				when customer_active_status_code = 3 then '预流失客户'
				when customer_active_status_code = 4 then '流失客户' 
				else '其他'
			end as customer_active_status_name	--客户活跃状态
		from 
			csx_dw.dws_sale_w_a_customer_company_active
		where 
			sdt = 'current'
		)e on a.customer_no=e.customer_no and a.company_code = e.sign_company_code
order by 
	a.province_code,a.province_name,a.city_group_code,a.city_group_name,a.customer_no,a.company_code,a.grouping_id,a.smonth
;


--------------------------------- hive建表语句 -------------------------------
-- csx_dw.report_sss_r_d_customer_receivable_amount  财务应收账款表

--drop table if exists csx_dw.report_sss_r_d_customer_receivable_amount;
--create table csx_dw.report_sss_r_d_customer_receivable_amount(
--`biz_id`                         string              COMMENT    '业务主键',
--`region_code`                    string              COMMENT    '大区编码',
--`region_name`                    string              COMMENT    '大区名称',
--`province_code`                  string              COMMENT    '省区编码',
--`province_name`                  string              COMMENT    '省区名称',
--`city_group_code`                string              COMMENT    '城市组编码',
--`city_group_name`                string              COMMENT    '城市组名称',
--`channel_code`                   string              COMMENT    '渠道编码',
--`channel_name`                   string              COMMENT    '渠道名称',
--`smonth`                         string              COMMENT    '年月',
--`grouping_id`                    string              COMMENT    '区域粒度编码',
--`customer_no`                    string              COMMENT    '客户编号',
--`customer_name`                  string              COMMENT    '客户名称',
--`company_code`                   string              COMMENT    '公司代码',
--`company_name`                   string              COMMENT    '公司名称',
--`payment_terms`                  string              COMMENT    '账期类型',
--`payment_name`                   string              COMMENT    '账期名称',
--`payment_days`                   string              COMMENT    '账期值',
--`payment_short_name`             string              COMMENT    '账期简称',
--`credit_limit`                   decimal(26,6)       COMMENT    '信控额度',
--`temp_credit_limit`              decimal(26,6)       COMMENT    '临时额度',
--`first_category_code`            string              COMMENT    '一级客户分类编码',
--`first_category_name`            string              COMMENT    '一级客户分类名称',
--`second_category_code`           string              COMMENT    '二级客户分类编码',
--`second_category_name`           string              COMMENT    '二级客户分类名称',
--`third_category_code`            string              COMMENT    '三级客户分类编码',
--`third_category_name`            string              COMMENT    '三级客户分类名称',
--`sales_id`                       string              COMMENT    '销售员Id',
--`work_no`                        string              COMMENT    '销售员工号',
--`sales_name`                     string              COMMENT    '销售员',
--`first_supervisor_code`          string              COMMENT    '销售主管编码',
--`first_supervisor_work_no`       string              COMMENT    '销售主管工号',
--`first_supervisor_name`          string              COMMENT    '销售主管姓名',
--`dev_source_code`                string              COMMENT    '开发来源编码(1:自营,2:业务代理人,3:城市服务商,4:内购)',
--`dev_source_name`                string              COMMENT    '开发来源名称',
--`customer_active_status_code`    string              COMMENT    '客户活跃状态编码',
--`customer_active_status_name`    string              COMMENT    '客户活跃状态',
--`tax_sale_amount`                decimal(26,6)       COMMENT    '销售金额',
--`receivable_amount`              decimal(26,6)       COMMENT    '应收金额',
--`overdue_amount`                 decimal(26,6)       COMMENT    '逾期金额',
--`bad_debt_amount`                decimal(26,6)       COMMENT    '坏账金额',
--`claim_amount`                   decimal(26,6)       COMMENT    '认领金额',
--`paid_amount`                    decimal(26,6)       COMMENT    '认领核销金额',
--`residual_amount`                decimal(26,6)       COMMENT    '认领未核销金额',
--`payment_amount`                 decimal(26,6)       COMMENT    '单据核销金额',
--`max_overdue_day`                decimal(26,0)       COMMENT    '最大逾期天数',
--`create_by`                      string              COMMENT    '创建人',
--`create_time`                    timestamp           COMMENT    '创建时间',
--`update_time`                    timestamp           COMMENT    '更新时间'
--
--) COMMENT '财务应收账款表'
--PARTITIONED BY (sdt string COMMENT '日期分区')
--STORED AS TEXTFILE;