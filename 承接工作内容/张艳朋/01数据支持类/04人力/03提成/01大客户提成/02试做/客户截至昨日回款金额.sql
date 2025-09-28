--截至XX 客户回款金额、回款已核销金额、回款未核销金额
set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=2000;
set hive.groupby.skewindata=false;
set hive.map.aggr = true;
-- 增加reduce过程
set hive.optimize.sort.dynamic.partition=true;


--set i_sdate_11 ='20201130';
set i_sdate_11 =regexp_replace(date_sub(current_date,1),'-','');




--insert overwrite directory '/tmp/raoyanhua/linshi01' row format delimited fields terminated by '\t'
insert overwrite table csx_tmp.csx_customer_money_back_paid  partition(sdt)
select a.channel,a.sales_province,a.attribute,a.customer_no,a.customer_name,a.sales_name,a.work_no,
	b.company_code,c.company_name,b.claim_amount,b.paid_amount,b.residual_amount,
	d.payment_amount,b.claim_amount-coalesce(d.payment_amount,0) as unpay_amount, --当前使用
	from_utc_timestamp(current_timestamp(),'GMT') write_time,
	${hiveconf:i_sdate_11} as sdt
from 
	(select * 
	from csx_dw.dws_crm_w_a_customer_m_v1 
	where sdt=${hiveconf:i_sdate_11} 
	and customer_no<>''
	--and channel_code in('1','7')
	--and attribute_code <> 5
	) a
left join
( -- 获取本月客户回款金额
  select
    customer_code, -- 客户编码
    company_code, -- 公司代码
    sum(claim_amount) as claim_amount,	--回款金额（未使用，含补救单）
    sum(paid_amount) as paid_amount,	--回款已核销金额
    sum(residual_amount) as residual_amount	--回款未核销金额
  from csx_dw.dwd_sss_r_d_money_back -- sdt以过账日期分区，只看20200601及以后的，该表计算回款只计算已核销金额
  where ((sdt>='20200601' and sdt<=${hiveconf:i_sdate_11}) 
  or (sdt='19990101' and regexp_replace(substr(posting_time,1,10),'-','')>='20200601' and regexp_replace(substr(posting_time,1,10),'-','')<=${hiveconf:i_sdate_11}))
  and regexp_replace(substr(update_time,1,10),'-','')<=${hiveconf:i_sdate_11}  --回款以过账日期为准，但每次已核销金额发生变化更新日期都会变化，此表无法查历史回款已核销金额
  and (paid_amount<>'0' or residual_amount<>'0') --剔除补救单和对应原单
  group by customer_code,company_code
) b on a.customer_no = b.customer_code
left join
( -- 获取公司编码
  select
    code, name as company_name
  from csx_dw.dws_basic_w_a_company_code
  where sdt = 'current'
)c on b.company_code = c.code
left join
(--核销流水明细表中已核销金额
select customer_code,company_code,
	sum(payment_amount) payment_amount
from
	csx_dw.dwd_sss_r_d_close_bill_account_record_20200908
where regexp_replace(substr(posting_time,1,10),'-','') <=${hiveconf:i_sdate_11}
and (regexp_replace(substr(happen_date,1,10),'-','')<=${hiveconf:i_sdate_11} or happen_date='' or happen_date is NULL)
and regexp_replace(substr(paid_time,1,10),'-','') <=${hiveconf:i_sdate_11} 
and is_deleted ='0'
and money_back_id<>'0' --回款关联ID为0是微信支付、-1是退货系统核销
group by customer_code,company_code
)d on d.customer_code = b.customer_code and d.company_code = b.company_code
;




/*
--------------------------------- hive建表语句 -------------------------------
-- csx_tmp.csx_customer_money_back_paid  客户回款与核销金额

drop table if exists csx_tmp.csx_customer_money_back_paid;
create table csx_tmp.csx_customer_money_back_paid(
  `channel_name` string COMMENT  '渠道',
  `sales_province` string COMMENT  '省区',  
  `attribute` string COMMENT '客户属性',
  `customer_no` string COMMENT  '客户编号',
  `customer_name` string COMMENT  '客户名称', 
  `sales_name` string COMMENT  '销售员',  
  `work_no` string COMMENT  '销售员工号',
  `company_code` string COMMENT  '公司代码',
  `company_name` string COMMENT  '公司名称',  
  `claim_amount` decimal(26,6) COMMENT '回款金额',
  `paid_amount` decimal(26,6) COMMENT '回款已核销金额',
  `residual_amount` decimal(26,6) COMMENT '回款未核销金额',
  `payment_amount` decimal(26,6) COMMENT '核销明细_回款已核销金额',
  `unpay_amount` decimal(26,6) COMMENT '核销明细_回款未核销金额',
  `write_time` timestamp comment '更新时间'
) COMMENT '客户回款与核销金额'
PARTITIONED BY (sdt string COMMENT '日期分区')
STORED AS TEXTFILE;

insert overwrite directory '/tmp/raoyanhua/linshi01' row format delimited fields terminated by '\t'
select *
from csx_tmp.csx_customer_money_back_paid
where sdt='20201130'
and channel_name='大客户'
;



