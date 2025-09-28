--  -- 单据核销流水明细月快照表
select
  *
  -- 单据核销流水明细月快照表
from
    csx_ads.csx_ads_sss_close_bill_account_record_snapshot_mf -- 核销日期分区
where
  smt = '202506'
  and delete_flag = '0'
  and customer_code='127289'
--   and money_back_id=0
  and (close_bill_code like '%2505060612875983%'
      or  close_bill_code like '2505070612882789')

  
  
    select
      *
    from
      csx_dws.csx_dws_sale_detail_di
    where
      sdt >= '20250401' 
      and original_order_code like '%2505070612882789%'
      ;
      

  select * from 
   csx_dws.csx_dws_sss_order_credit_invoice_bill_settle_detail_di
 	where sdt='20250531'
-- 	 and source_bill_no like '%2505060612875679%'
	  and (source_bill_no like '%2505060612875679%'
      or  source_bill_no like '2505070612882789')


      select * from csx_analyse_tmp.csx_analyse_fr_tc_customer_credit_order_unpay_mi




csx_analyse.csx_analyse_fr_tc_customer_credit_order_unpay_mi
  

  csx_analyse.csx_analyse_fr_tc_customer_credit_order_detail

-- 这个单号销售表中查不到
select
  *
from
  csx_dws.csx_dws_sale_detail_di di
where
--   channel_code in('1', '7', '9')
--   and order_channel_detail_code not in ('24', '28') -- 剔除永辉生活、永辉线上
   shipper_code = 'YHCSX'
  and sdt >= '20250101'
  and original_order_code like '%2505190612985631%'


华南大客户：超过3%。
3226部队
环比下降，为什么 ？？
32266部队 计划直送，成本较高，售价在下降，供应商
新客，周毛利率情况 
