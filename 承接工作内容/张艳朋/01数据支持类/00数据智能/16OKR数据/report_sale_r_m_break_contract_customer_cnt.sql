-- OKR数据-断约客户 
-- 核心逻辑： 

-- 切换tez计算引擎
set mapred.job.name=report_sale_r_m_break_contract_customer_cnt;
SET hive.execution.engine=tez;
SET tez.queue.name=caishixian;

-- 动态分区
SET hive.exec.parallel=true;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions =1000;
SET hive.exec.max.dynamic.partitions.pernode =1000;

-- 中间结果压缩
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
SET mapred.output.compression.type=BLOCK;
SET parquet.compression=SNAPPY;

-- 启用引号识别
set hive.support.quoted.identifiers=none;

-- 计算日期
set current_day = regexp_replace(date_sub(current_date, 1), '-', '');

-- 当月第一天
set current_start_day = regexp_replace(trunc(date_sub(current_date, 1), 'MM'), '-', '');

-- 目标表
set target_table=csx_tmp.report_sale_r_m_break_contract_customer_cnt;
	
with current_break_contract_customer as 	
(
select
	a.region_code,
	c.region_name,
	a.province_code,
	c.province_name,
	a.city_group_code,
	c.city_group_name,
	count(distinct case when a.min_sdt between '20210101' and '20210331' then a.customer_no else null end) as last_qtr_customer_cnt,
	count(distinct case when a.max_sdt between '20210101' and '20210331' then a.customer_no else null end) as cur_qtr_none_customer_cnt,
	count(distinct case when a.max_sdt between '20210401' and '20210630' then a.customer_no else null end) as cur_qtr_customer_cnt,
	count(distinct case when d.normal_first_order_date between '20210401' and '20210630' then a.customer_no else null end) as cur_qtr_add_customer_cnt
from
	(
	select
		region_code,province_code,city_group_code,customer_no,
		min(sdt) as min_sdt,
		max(sdt) as max_sdt
	from 
		csx_dw.dws_sale_r_d_detail
	where 
		sdt >= '20210101' 
		and sdt <= '20210630'
		and channel_code in ('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
		and business_type_code in ('1') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
		and sales_type !='fanli'
	group by 
		region_code,province_code,city_group_code,customer_no
	) as a
	join -- 客户信息
		(
		select 
			customer_no,customer_name,work_no,sales_name,first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name,
			regexp_replace(split(sign_time,' ')[0],'-','') as sign_date,cooperation_mode_code,cooperation_mode_name
		from 
			csx_dw.dws_crm_w_a_customer
		where 
			sdt='current'
			and cooperation_mode_code='01' -- 合作模式编码(01长期客户,02一次性客户)
		group by 
			customer_no,customer_name,work_no,sales_name,first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name,
			regexp_replace(split(sign_time,' ')[0],'-',''),cooperation_mode_code,cooperation_mode_name
		) b on a.customer_no=b.customer_no
	left join -- 区域信息
		(
		select 
			city_group_code,city_group_name,province_code,province_name,region_code,region_name
		from 
			csx_dw.dws_sale_w_a_area_belong
		group by 
			city_group_code,city_group_name,province_code,province_name,region_code,region_name
		) c on c.city_group_code=a.city_group_code	
	left join -- 最后一次下日配单
		(
		select 
			customer_no,normal_first_order_date,normal_last_order_date
		from 
			csx_dw.dws_crm_w_a_customer_active
		where 
			sdt='current'
		group by 
			customer_no,normal_first_order_date,normal_last_order_date
		) d on d.customer_no=a.customer_no
group by 
	a.region_code,
	c.region_name,
	a.province_code,
	c.province_name,
	a.city_group_code,
	c.city_group_name
)

insert overwrite table ${hiveconf:target_table} partition(quarter)			
			
select
	'' as biz_id,
	region_code,
	region_name,
	province_code,
	province_name,
	city_group_code,
	city_group_name,
	last_qtr_customer_cnt,
	cur_qtr_none_customer_cnt,
	cur_qtr_customer_cnt,
	cur_qtr_add_customer_cnt,
	from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time, -- 更新时间
	'202102' as quarter -- 月份
from
	current_break_contract_customer	
;



INVALIDATE METADATA csx_tmp.report_sale_r_m_break_contract_customer_cnt;	



/*
--------------------------------- hive建表语句 -------------------------------
-- csx_tmp.report_sale_r_m_break_contract_customer_cnt  OKR数据 断约客户

drop table if exists csx_tmp.report_sale_r_m_break_contract_customer_cnt;
create table csx_tmp.report_sale_r_m_break_contract_customer_cnt(
`biz_id`                         string              COMMENT    '业务主键',
`region_code`                    string              COMMENT    '大区编码',
`region_name`                    string              COMMENT    '大区名称',
`province_code`                  string              COMMENT    '省份编码',
`province_name`                  string              COMMENT    '省份名称',
`city_group_code`                string              COMMENT    '城市组编码',
`city_group_name`                string              COMMENT    '城市组名称',
`last_qtr_customer_cnt`          int                 COMMENT    '上季度下单客户数',
`cur_qtr_none_customer_cnt`      int                 COMMENT    '本季度断约客户数',
`cur_qtr_customer_cnt`           int                 COMMENT    '本季度下单客户数',
`cur_qtr_add_customer_cnt`       int                 COMMENT    '本季度新增客户数',
`update_time`                    string              COMMENT    '数据更新时间'
) COMMENT 'zhangyanpeng:OKR数据-断约客户'
PARTITIONED BY (quarter string COMMENT '日期分区')
STORED AS TEXTFILE;

*/	