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

insert overwrite table csx_dw.report_sss_r_d_customer_accounts partition(sdt)

select
	concat_ws('-',${hiveconf:yesterday},a.customer_no,a.company_code) as biz_id,
	a.customer_no,
	c.customer_name,
	c.channel_code,
	c.channel_name,
	c.attribute,
	c.attribute_desc,
	c.first_category_code,
	c.first_category_name,
	c.second_category_code,
	c.second_category_name,
	c.third_category_code,
	c.third_category_name,
	c.sales_id,
	c.work_no,
	c.sales_name,
	c.first_supervisor_code,
	c.first_supervisor_work_no,
	c.first_supervisor_name,
	c.province_code,
	c.province_name,
	c.city_group_code,
	c.city_group_name,
	a.company_code,
	a.company_name,
	a.payment_terms,
	a.payment_name,
	a.payment_days,
	a.customer_level,
	a.credit_limit,
	a.temp_credit_limit,
	a.temp_begin_time,
	a.temp_end_time,
	a.overdue_amount,
	a.overdue_amount1,
	a.overdue_amount15,
	a.overdue_amount30,
	a.overdue_amount60,
	a.overdue_amount90,
	a.overdue_amount120,
	a.overdue_amount180,
	a.overdue_amount365,
	a.overdue_amount730,
	a.overdue_amount1095,
	a.non_overdue_amount,
	a.receivable_amount,
	a.bad_debt_amount,
	a.max_overdue_day,
	b.last_sales_date,
	b.last_to_now_days,
	b.customer_active_status_code,
	b.customer_active_status,
	${hiveconf:created_by} create_by,
	${hiveconf:created_time} create_time,
	${hiveconf:created_time} update_time,
	${hiveconf:yesterday} as sdt -- 统计日期  	
from 
	(
	select 
		customer_code as customer_no,customer_name,company_code,company_name,payment_terms,payment_name,payment_days,customer_level,credit_limit,temp_credit_limit,temp_begin_time,
		temp_end_time,overdue_amount,overdue_amount1,overdue_amount15,overdue_amount30,overdue_amount60,overdue_amount90,overdue_amount120,overdue_amount180,
		overdue_amount365,overdue_amount730,overdue_amount1095,non_overdue_amount,receivable_amount,bad_debt_amount,max_overdue_day,sdt
	from 
		csx_dw.dws_sss_r_d_customer_settle_detail 
	where 
		sdt=${hiveconf:yesterday}
	) a 
	left join
		(
		select 
			customer_no,sign_company_code,last_sales_date,last_to_now_days,customer_active_status_code,
			case when customer_active_status_code = 1 then '活跃客户'
				when customer_active_status_code = 2 then '沉默客户'
				when customer_active_status_code = 3 then '预流失客户'
				when customer_active_status_code = 4 then '流失客户'
				else '其他'
			end as customer_active_status
		from 
			csx_dw.dws_sale_w_a_customer_company_active
		where 
			sdt =${hiveconf:yesterday}
		) b on a.customer_no= b.customer_no and a.company_code = b.sign_company_code
	left join
		(
		select
			customer_no,customer_name,channel_code,channel_name,attribute,attribute_desc,first_category_code,first_category_name,second_category_code,second_category_name,
			third_category_code,third_category_name,sales_id,work_no,sales_name,first_supervisor_code,first_supervisor_work_no,first_supervisor_name,province_code,province_name,
			city_group_code,city_group_name
		from
			csx_dw.dws_crm_w_a_customer
		where 
			sdt =${hiveconf:yesterday}
		) c on c.customer_no=a.customer_no
;
	

--------------------------------- hive建表语句 -------------------------------
-- csx_dw.report_sss_r_d_customer_accounts  账龄分析和回款统计

--drop table if exists csx_dw.report_sss_r_d_customer_accounts;
--create table csx_dw.report_sss_r_d_customer_accounts(
--`biz_id`                         string              COMMENT    '业务主键',
--`customer_no`                    string              COMMENT    '客户编码',
--`customer_name`                  string              COMMENT    '客户名称',
--`channel_code`                   string              COMMENT    '渠道编码',
--`channel_name`                   string              COMMENT    '渠道名称',
--`attribute_code`                 string              COMMENT    '客户属性编码 ',
--`attribute_name`                 string              COMMENT    '客户属性名称',
--`first_category_code`            string              COMMENT    '企业一级分类名称',
--`first_category_name`            string              COMMENT    '企业一级分类名称',
--`second_category_code`           string              COMMENT    '企业二级分类编码',
--`second_category_name`           string              COMMENT    '企业二级分类名称',
--`third_category_code`            string              COMMENT    '企业三级分类编码',
--`third_category_name`            string              COMMENT    '企业三级分类名称',
--`sales_id`                       string              COMMENT    '销售员ID',
--`work_no`                        string              COMMENT    '销售员OA工号',
--`sales_name`                     string              COMMENT    '销售员名称',
--`sales_supervisor_id`            string              COMMENT    '主管ID',
--`sales_supervisor_work_no`       string              COMMENT    '主管OA工号',
--`sales_supervisor_name`          string              COMMENT    '主管名称',
--`province_code`                  string              COMMENT    '省区编码',
--`province_name`                  string              COMMENT    '省区名称',
--`city_group_code`                string              COMMENT    '城市编码',
--`city_group_name`                string              COMMENT    '城市名称',
--`company_code`                   string              COMMENT    '公司代码',
--`company_name`                   string              COMMENT    '公司代码名称',
--`payment_terms`                  string              COMMENT    '付款条件',
--`payment_name`                   string              COMMENT    '付款条件名称',
--`payment_days`                   int                 COMMENT    '账期值',
--`customer_level`                 string              COMMENT    '客户等级',
--`credit_limit`                   decimal(26,2)       COMMENT    '信控额度',
--`temp_credit_limit`              decimal(26,2)       COMMENT    '临时额度',
--`temp_begin_time`                timestamp           COMMENT    '临时额度起始时间',
--`temp_end_time`                  timestamp           COMMENT    '临时额度截止时间',
--`overdue_amount`                 decimal(15,2)       COMMENT    '逾期金额',
--`overdue_amount1`                decimal(15,2)       COMMENT    '逾期1-15天',
--`overdue_amount15`               decimal(15,2)       COMMENT    '逾期15-30天',
--`overdue_amount30`               decimal(15,2)       COMMENT    '逾期30-60天',
--`overdue_amount60`               decimal(15,2)       COMMENT    '逾期60-90天',
--`overdue_amount90`               decimal(15,2)       COMMENT    '逾期90-120天',
--`overdue_amount120`              decimal(15,2)       COMMENT    '逾期120-180天',
--`overdue_amount180`              decimal(15,2)       COMMENT    '逾期180-365天',
--`overdue_amount365`              decimal(15,2)       COMMENT    '逾期1-2年',
--`overdue_amount730`              decimal(15,2)       COMMENT    '逾期2-3年',
--`overdue_amount1095`             decimal(15,2)       COMMENT    '逾期3年以上',
--`non_overdue_amount`             decimal(15,2)       COMMENT    '未逾期金额',
--`receivable_amount`              decimal(15,2)       COMMENT    '应收账款',
--`bad_debt_amount`                decimal(15,2)       COMMENT    '坏账金额',
--`max_overdue_day`                int                 COMMENT    '最大逾期天数',
--`last_sales_date`                string              COMMENT    '最后销售日期',
--`last_to_now_days`               int                 COMMENT    '未销售天数',
--`customer_active_status_code`    string              COMMENT    '客户标识名称',
--`customer_active_status`         string              COMMENT    '客户标识',
--`create_by`                      string              COMMENT    '创建人',
--`create_time`                    timestamp           COMMENT    '创建时间',
--`update_time`                    timestamp           COMMENT    '更新时间'
--
--) COMMENT '账龄分析和回款统计'
--PARTITIONED BY (sdt string COMMENT '日期分区')
--STORED AS TEXTFILE;


