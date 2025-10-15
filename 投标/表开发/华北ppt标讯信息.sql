-- 标讯统计\标讯统计_tmp_bid_info.sql
-- 标讯统计\标讯统计_tmp_bid_info.sql
-- drop table  csx_analyse_tmp.csx_analyse_tmp_bid_info;
create table csx_analyse_tmp.csx_analyse_tmp_bid_info as 
with tmp_bid_info as 
(select
  bid_status_name,
  bid_customer_name,
  performance_region_code,
  performance_region_name,
  performance_province_code,
  performance_province_name,
  performance_city_code,
  performance_city_name,
  owner_province_code,
  owner_province_name,
  customer_province_code,
  customer_province_name,
  business_attribute_code,
  business_attribute_name,
  bid_amount,
  bid_date,
  win_bid_date,
  win_bid_amount,
  SDT,
  supply_deadline,
  case 
    when a.supply_deadline is null then a.win_bid_amount
    when a.supply_deadline <= 12 then a.win_bid_amount
    when a.supply_deadline > 12 then a.win_bid_amount / a.supply_deadline * 12
    else a.win_bid_amount
  end as annual_amount
from
    csx_analyse.csx_analyse_crm_bid_info_df a 
where
  sdt = '20251009'
--   and bid_date >= '2025-09-01'
--   and performance_region_name = '华北大区'
),
tmp_sap_receive_detail as (
        select
          sdt,
          performance_province_name ,
          a.customer_code,
          a.customer_name,
          business_attribute_name
        from
          -- csx_dws.csx_dws_sss_customer_credit_invoice_bill_settle_stat_di
          -- 取SAP
            csx_dws.csx_dws_sap_customer_credit_settle_detail_di a
        where
           sdt = '20251009'
        group by sdt,
          performance_province_name ,
          a.customer_code,
          a.customer_name,
          business_attribute_name
) select a.*,if(b.customer_name is not null ,1,0) as new_cust_flag 
from tmp_bid_info a 
left join tmp_sap_receive_detail b on a.bid_customer_name=b.customer_name and a.business_attribute_name=b.business_attribute_name
;

select distinct supply_deadline from
  csx_analyse_tmp.csx_analyse_tmp_bid_info 



select
  performance_region_name,
  performance_province_name,
  sum(if(bid_status_name='流标（投标后）',1,0))  as loss_bid_cust,
  sum(if(bid_status_name='中标',1,0)) as win_bid_cust,
  sum(if(bid_status_name='中标',coalesce(win_bid_amount,0),0)) win_bid_amount,
 
  new_cust_flag
from
  csx_analyse_tmp.csx_analyse_tmp_bid_info
where
  bid_date >= '2025-09-01'
  group by  new_cust_flag,
   performance_region_name,
  performance_province_name
  ;

select distinct   
  bid_status_name
  from
  csx_analyse_tmp.csx_analyse_tmp_bid_info
where
  bid_date >= '2025-09-01'




drop table data_analysis_prd.csx_analyse_tmp_ppt_bid_info ;
CREATE TABLE data_analysis_prd.csx_analyse_tmp_ppt_bid_info (
  id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '主键id',
  bid_status_name VARCHAR(255) COMMENT '投标状态名称', 
  bid_customer_name VARCHAR(255) COMMENT '投标客户名称', 
  performance_region_code VARCHAR(255) COMMENT '业绩大区编码', 
  performance_region_name VARCHAR(255) COMMENT '业绩大区名称', 
  performance_province_code VARCHAR(255) COMMENT '业绩省份编码', 
  performance_province_name VARCHAR(255) COMMENT '业绩省份名称', 
  performance_city_code VARCHAR(255) COMMENT '业绩城市编码', 
  performance_city_name VARCHAR(255) COMMENT '业绩城市名称', 
  owner_province_code VARCHAR(255) COMMENT '标讯归属省份编码', 
  owner_province_name VARCHAR(255) COMMENT '标讯归属省份名称', 
  customer_province_code VARCHAR(255) COMMENT '客户省份编码', 
  customer_province_name VARCHAR(255) COMMENT '客户省份名称', 
  business_attribute_code INT COMMENT '业务属性编码', 
  business_attribute_name VARCHAR(255) COMMENT '业务属性名称', 
  bid_amount VARCHAR(255) COMMENT '投标金额', 
  bid_date  VARCHAR(255) COMMENT '投标日期', 
  win_bid_date  VARCHAR(255) COMMENT '中标日期', 
  win_bid_amount VARCHAR(255) COMMENT '中标金额', 
  sdt VARCHAR(32) COMMENT '数据时间', 
  supply_deadline INT COMMENT '供应期限', 
  annual_amount VARCHAR(255) comment '年化金额', 
  new_cust_flag int COMMENT '新客户标识',
  INDEX idx_performance_region_code (sdt, performance_region_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='PPT投标信息表';


TRUNCATE TABLE csx_analyse_tmp_ppt_bid_info;

select * from csx_analyse_tmp_ppt_bid_info where bid_date>='2025-09-01' and performance_province_name='北京市'