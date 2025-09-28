
--客户首次销售日期：B端+BBC
drop table csx_tmp.tmp_cust_sale_0;
create table csx_tmp.tmp_cust_sale_0
as 
select
customer_no,
min(case when sales_value>0 and channel_code in('1','7','9','4','5','6') then sdt end) as all_first_order_date,	--B端+BBC+大宗第一次销售日期
min(case when sales_value>0 and channel_code in('1','7','9') then sdt end) as first_order_date,	--B端+BBC第一次销售日期
min(case when sales_value>0 and channel_code in('1','9') then sdt end) as B_first_order_date,	--B端第一次销售日期
min(case when sales_value>0 and channel_code in('1','9') and business_type_code<>'4' then sdt end) as B_nof_first_order_date,	--B端不含合伙人第一次销售日期
min(case when sales_value>0 and channel_code in('1','7','9') and business_type_code<>'4' then sdt end) as B_nof_BBC_first_order_date,	--B端+BBC不含合伙人第一次销售日期
min(case when sales_value>0 and channel_code in('4','5','6') then sdt end) dazong_first_order_date,  --大宗第一次销售日期
min(case when sales_value>0 and business_type_code='1' then sdt end) normal_first_order_date,	--日配业务第一次销售日期
min(case when sales_value>0 and business_type_code='2' then sdt end) welfare_first_order_date,	--福利业务第一次销售日期
min(case when sales_value>0 and business_type_code='4' then sdt end) fws_first_order_date,	--城市服务商业务第一次销售日期  
min(case when sales_value>0 and business_type_code='6' then sdt end) bbc_first_order_date	--BBC业务第一次销售日期
from
(
  --渠道编号-1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 7.企业购 8.其他 9.业务代理  
  -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超) 
select customer_no,sdt,business_type_code,channel_code,sales_value
from csx_dw.dws_sale_r_d_detail 
where sdt>='20190101'
and channel_code in('1','7','9','4','5','6')
and sales_type<>'fanli'
--and business_type_code='1'
and (order_no not in ('OC200529000043','OC200529000044','OC200529000045','OC200529000046',
  					'OC20111000000021','OC20111000000022','OC20111000000023','OC20111000000024','OC20111000000025') or order_no is null)
)a
group by customer_no;


--结果表1、ARPU
insert overwrite directory '/tmp/raoyanhua/01ARPU' row format delimited fields terminated by '\t'
select smonth,coalesce(province_name,'全国') province_name,
  sum(ripei_excluding_tax_sales) ripei_excluding_tax_sales,
  sum(ripei_excluding_tax_sales_old) ripei_excluding_tax_sales_old,
  sum(ripei_excluding_tax_sales_new) ripei_excluding_tax_sales_new,
  sum(ripei_counts) ripei_counts,
  sum(ripei_counts_old) ripei_counts_old,
  sum(ripei_counts_new) ripei_counts_new,
  sum(ripei_excluding_tax_sales)/sum(ripei_counts) ripei_ARPU,
  sum(ripei_excluding_tax_sales_old)/sum(ripei_counts_old) ripei_ARPU_old,
  sum(ripei_excluding_tax_sales_new)/sum(ripei_counts_new) ripei_ARPU_new,
  
  sum(B_nof_excluding_tax_sales) B_nof_excluding_tax_sales,
  sum(B_nof_excluding_tax_sales_old) B_nof_excluding_tax_sales_old,
  sum(B_nof_excluding_tax_sales_new) B_nof_excluding_tax_sales_new,
  sum(B_nof_counts) B_nof_counts,
  sum(B_nof_counts_old) B_nof_counts_old,
  sum(B_nof_counts_new) B_nof_counts_new,
  sum(B_nof_excluding_tax_sales)/sum(B_nof_counts) B_nof_ARPU,
  sum(B_nof_excluding_tax_sales_old)/sum(B_nof_counts_old) B_nof_ARPU_old,
  sum(B_nof_excluding_tax_sales_new)/sum(B_nof_counts_new) B_nof_ARPU_new,
  
  sum(B_excluding_tax_sales) B_excluding_tax_sales,
  sum(B_excluding_tax_sales_old) B_excluding_tax_sales_old,
  sum(B_excluding_tax_sales_new) B_excluding_tax_sales_new,
  sum(B_counts) B_counts,
  sum(B_counts_old) B_counts_old,
  sum(B_counts_new) B_counts_new,
  sum(B_excluding_tax_sales)/sum(B_counts) B_ARPU,
  sum(B_excluding_tax_sales_old)/sum(B_counts_old) B_ARPU_old,
  sum(B_excluding_tax_sales_new)/sum(B_counts_new) B_ARPU_new,
  
  sum(BBBC_excluding_tax_sales) BBBC_excluding_tax_sales,
  sum(BBBC_excluding_tax_sales_old) BBBC_excluding_tax_sales_old,
  sum(BBBC_excluding_tax_sales_new) BBBC_excluding_tax_sales_new,
  sum(BBBC_counts) BBBC_counts,
  sum(BBBC_counts_old) BBBC_counts_old,
  sum(BBBC_counts_new) BBBC_counts_new,
  sum(BBBC_excluding_tax_sales)/sum(BBBC_counts) BBBC_ARPU,
  sum(BBBC_excluding_tax_sales_old)/sum(BBBC_counts_old) BBBC_ARPU_old,
  sum(BBBC_excluding_tax_sales_new)/sum(BBBC_counts_new) BBBC_ARPU_new
from 
(
  select a.smonth,a.province_name,
    sum(a.ripei_excluding_tax_sales) ripei_excluding_tax_sales,
    sum(case when a.ripei_excluding_tax_sales<>0 and (substr(b.normal_first_order_date,1,6)<>a.smonth or
          (substr(b.first_order_date,1,6)<>a.smonth and substr(b.normal_first_order_date,1,6)=a.smonth) or substr(b.normal_first_order_date,1,6) is null)
        then a.ripei_excluding_tax_sales end) ripei_excluding_tax_sales_old,
    sum(case when a.ripei_excluding_tax_sales<>0 and substr(b.first_order_date,1,6)=a.smonth and substr(b.normal_first_order_date,1,6)=a.smonth then a.ripei_excluding_tax_sales end) ripei_excluding_tax_sales_new,
    count(distinct case when a.ripei_excluding_tax_sales<>0 then a.customer_no end) ripei_counts,
    count(distinct case when a.ripei_excluding_tax_sales<>0 and (substr(b.normal_first_order_date,1,6)<>a.smonth or
            (substr(b.first_order_date,1,6)<>a.smonth and substr(b.normal_first_order_date,1,6)=a.smonth) or substr(b.normal_first_order_date,1,6) is null)
          then a.customer_no end) ripei_counts_old,
    count(distinct case when a.ripei_excluding_tax_sales<>0 and substr(b.first_order_date,1,6)=a.smonth and substr(b.normal_first_order_date,1,6)=a.smonth then a.customer_no end) ripei_counts_new,
    
    sum(a.B_nof_excluding_tax_sales) B_nof_excluding_tax_sales,
    sum(case when a.B_nof_excluding_tax_sales<>0 and (substr(b.B_nof_first_order_date,1,6)<>a.smonth or substr(b.B_nof_first_order_date,1,6) is null) then a.B_nof_excluding_tax_sales end) B_nof_excluding_tax_sales_old,
    sum(case when a.B_nof_excluding_tax_sales<>0 and substr(b.B_nof_first_order_date,1,6)=a.smonth then a.B_nof_excluding_tax_sales end) B_nof_excluding_tax_sales_new,
    count(distinct case when a.B_nof_excluding_tax_sales<>0 then a.customer_no end) B_nof_counts,
    count(distinct case when a.B_nof_excluding_tax_sales<>0 and (substr(b.B_nof_first_order_date,1,6)<>a.smonth or substr(b.B_nof_first_order_date,1,6) is null) then a.customer_no end) B_nof_counts_old,
    count(distinct case when a.B_nof_excluding_tax_sales<>0 and substr(b.B_nof_first_order_date,1,6)=a.smonth then a.customer_no end) B_nof_counts_new,
    
    sum(a.B_excluding_tax_sales) B_excluding_tax_sales,
    sum(case when a.B_excluding_tax_sales<>0 and (substr(b.B_first_order_date,1,6)<>a.smonth or substr(b.B_first_order_date,1,6) is null) then a.B_excluding_tax_sales end) B_excluding_tax_sales_old,
    sum(case when a.B_excluding_tax_sales<>0 and substr(b.B_first_order_date,1,6)=a.smonth then a.B_excluding_tax_sales end) B_excluding_tax_sales_new,
    count(distinct case when a.B_excluding_tax_sales<>0 then a.customer_no end) B_counts,
    count(distinct case when a.B_excluding_tax_sales<>0 and (substr(b.B_first_order_date,1,6)<>a.smonth or substr(b.B_first_order_date,1,6) is null) then a.customer_no end) B_counts_old,
    count(distinct case when a.B_excluding_tax_sales<>0 and substr(b.B_first_order_date,1,6)=a.smonth then a.customer_no end) B_counts_new,
    
    sum(a.BBBC_excluding_tax_sales) BBBC_excluding_tax_sales,
    sum(case when (substr(b.first_order_date,1,6)<>a.smonth or substr(b.first_order_date,1,6) is null) then a.BBBC_excluding_tax_sales end) BBBC_excluding_tax_sales_old,
    sum(case when substr(b.first_order_date,1,6)=a.smonth then a.BBBC_excluding_tax_sales end) BBBC_excluding_tax_sales_new,
    count(distinct case when a.BBBC_excluding_tax_sales<>0 then a.customer_no end) BBBC_counts,
    count(distinct case when a.BBBC_excluding_tax_sales<>0 and (substr(b.first_order_date,1,6)<>a.smonth or substr(b.first_order_date,1,6) is null) then a.customer_no end) BBBC_counts_old,
    count(distinct case when a.BBBC_excluding_tax_sales<>0 and substr(b.first_order_date,1,6)=a.smonth then a.customer_no end) BBBC_counts_new
  from
  (
    select substr(sdt,1,6) smonth,province_name,customer_no,
      sum(excluding_tax_sales) BBBC_excluding_tax_sales,
      sum(case when channel_code in('1','9') then excluding_tax_sales end) B_excluding_tax_sales,
      sum(case when channel_code in('1','9') and business_type_code<>'4' then excluding_tax_sales end) B_nof_excluding_tax_sales,
      sum(case when business_type_code='1' then excluding_tax_sales end) ripei_excluding_tax_sales
    from csx_dw.dws_sale_r_d_detail
    where sdt>='20190101'
    and channel_code in('1','7','9')
    group by substr(sdt,1,6),province_name,customer_no
  )a 
  left join csx_tmp.tmp_cust_sale_0 b on a.customer_no=b.customer_no
  group by a.smonth,a.province_name
)a
group by smonth,province_name
grouping sets (smonth,(smonth,province_name));



--结果表2、客户数
insert overwrite directory '/tmp/raoyanhua/02cust' row format delimited fields terminated by '\t'
select smonth,coalesce(province_name,'全国') province_name,
  sum(ripei_counts) ripei_counts,
  sum(fuli_counts) fuli_counts,
  sum(c_rpfl_counts) c_rpfl_counts,
  sum(B_nof_counts) B_nof_counts,
  sum(BBC_counts) BBC_counts,
  sum(c_B_nof_BBC_counts) c_B_nof_BBC_counts,
  sum(B_nof_BBC_counts) B_nof_BBC_counts,
  sum(fws_counts) fws_counts,
  sum(c_B_nof_BBC_fws_counts) c_B_nof_BBC_fws_counts,
  sum(BBBC_counts) BBBC_counts,
  sum(dazong_counts) dazong_counts,
  sum(all_counts) all_counts,
  sum(B_counts) B_counts,
  
  sum(ripei_counts_new) ripei_counts_new,
  sum(fuli_counts_new) fuli_counts_new,
  sum(c_rpfl_counts_new) c_rpfl_counts_new,
  sum(B_nof_counts_new) B_nof_counts_new,
  sum(BBC_counts_new) BBC_counts_new,
  sum(c_B_nof_BBC_counts_new) c_B_nof_BBC_counts_new,
  sum(B_nof_BBC_counts_new) B_nof_BBC_counts_new,
  sum(fws_counts_new) fws_counts_new,
  sum(c_B_nof_BBC_fws_counts_new) c_B_nof_BBC_fws_counts_new,
  sum(BBBC_counts_new) BBBC_counts_new,
  sum(dazong_counts_new) dazong_counts_new,
  sum(all_counts_new) all_counts_new,
  sum(B_counts_new) B_counts_new 
from 
(  
  select a.smonth,a.province_name,
  --客户数
    count(distinct case when ripei_excluding_tax_sales<>0 then a.customer_no end) ripei_counts,
	count(distinct case when fuli_excluding_tax_sales<>0 then a.customer_no end) fuli_counts,
	count(distinct case when ripei_excluding_tax_sales<>0 and fuli_excluding_tax_sales<>0 then a.customer_no end) c_rpfl_counts,
	
	count(distinct case when B_nof_excluding_tax_sales<>0 then a.customer_no end) B_nof_counts,
	count(distinct case when BBC_excluding_tax_sales<>0 then a.customer_no end) BBC_counts,
	count(distinct case when B_nof_excluding_tax_sales<>0 and BBC_excluding_tax_sales<>0 then a.customer_no end) c_B_nof_BBC_counts,
	
	count(distinct case when B_nof_BBC_excluding_tax_sales<>0 then a.customer_no end) B_nof_BBC_counts,
	count(distinct case when fws_excluding_tax_sales<>0 then a.customer_no end) fws_counts,
	count(distinct case when B_nof_BBC_excluding_tax_sales<>0 and fws_excluding_tax_sales<>0 then a.customer_no end) c_B_nof_BBC_fws_counts,
		
	count(distinct case when BBBC_excluding_tax_sales<>0 then a.customer_no end) BBBC_counts,
	count(distinct case when dazong_excluding_tax_sales<>0 then a.customer_no end) dazong_counts,
	count(distinct case when all_excluding_tax_sales<>0 then a.customer_no end) all_counts,
	count(distinct case when B_excluding_tax_sales<>0 then a.customer_no end) B_counts, --B端含合伙人

  --首次履约客户数
    count(distinct case when substr(b.normal_first_order_date,1,6)=a.smonth and ripei_excluding_tax_sales<>0 then a.customer_no end) ripei_counts_new,
	count(distinct case when substr(b.welfare_first_order_date,1,6)=a.smonth and fuli_excluding_tax_sales<>0 then a.customer_no end) fuli_counts_new,
	count(distinct case when substr(b.normal_first_order_date,1,6)=a.smonth and substr(b.welfare_first_order_date,1,6)=a.smonth and 
	                  ripei_excluding_tax_sales<>0 and fuli_excluding_tax_sales<>0 then a.customer_no end) c_rpfl_counts_new,
	
	count(distinct case when substr(b.b_nof_first_order_date,1,6)=a.smonth and B_nof_excluding_tax_sales<>0 then a.customer_no end) B_nof_counts_new,
	count(distinct case when substr(b.bbc_first_order_date,1,6)=a.smonth and BBC_excluding_tax_sales<>0 then a.customer_no end) BBC_counts_new,
	count(distinct case when substr(b.b_nof_first_order_date,1,6)=a.smonth and substr(b.bbc_first_order_date,1,6)=a.smonth and 
	                  B_nof_excluding_tax_sales<>0 and BBC_excluding_tax_sales<>0 then a.customer_no end) c_B_nof_BBC_counts_new,
	
	count(distinct case when substr(b.B_nof_BBC_first_order_date,1,6)=a.smonth and B_nof_BBC_excluding_tax_sales<>0 then a.customer_no end) B_nof_BBC_counts_new,
	count(distinct case when substr(b.fws_first_order_date,1,6)=a.smonth and fws_excluding_tax_sales<>0 then a.customer_no end) fws_counts_new,
	count(distinct case when substr(b.B_nof_BBC_first_order_date,1,6)=a.smonth and substr(b.fws_first_order_date,1,6)=a.smonth and 
	                  B_nof_BBC_excluding_tax_sales<>0 and fws_excluding_tax_sales<>0 then a.customer_no end) c_B_nof_BBC_fws_counts_new,
		
	count(distinct case when substr(b.first_order_date,1,6)=a.smonth and BBBC_excluding_tax_sales<>0 then a.customer_no end) BBBC_counts_new,
	count(distinct case when substr(b.dazong_first_order_date,1,6)=a.smonth and dazong_excluding_tax_sales<>0 then a.customer_no end) dazong_counts_new,
	count(distinct case when substr(b.all_first_order_date,1,6)=a.smonth and all_excluding_tax_sales<>0 then a.customer_no end) all_counts_new,
	count(distinct case when substr(b.B_first_order_date,1,6)=a.smonth and B_excluding_tax_sales<>0 then a.customer_no end) B_counts_new --B端含合伙人	
	
  from
  (
  --渠道编号-1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 7.企业购 8.其他 9.业务代理  
  -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)  
    select substr(sdt,1,6) smonth,province_name,customer_no,
	  sum(case when business_type_code='1' then excluding_tax_sales end) ripei_excluding_tax_sales,
	  sum(case when business_type_code='2' then excluding_tax_sales end) fuli_excluding_tax_sales,
	  sum(case when business_type_code in('1','2') then excluding_tax_sales end) rpfl_excluding_tax_sales,
	  sum(case when channel_code in('1','9') and business_type_code<>'4' then excluding_tax_sales end) B_nof_excluding_tax_sales,
	  sum(case when business_type_code='6' then excluding_tax_sales end) BBC_excluding_tax_sales,
      sum(case when channel_code in('1','7','9') and business_type_code<>'4' then excluding_tax_sales end) B_nof_BBC_excluding_tax_sales,
	  sum(case when business_type_code='4' then excluding_tax_sales end) fws_excluding_tax_sales,
	  sum(case when channel_code in('1','9') then excluding_tax_sales end) B_excluding_tax_sales,
	  sum(case when channel_code in('1','7','9') then excluding_tax_sales end) BBBC_excluding_tax_sales,
      sum(case when channel_code in('4','5','6') then excluding_tax_sales end) dazong_excluding_tax_sales,
      sum(case when channel_code in('1','7','9','4','5','6') then excluding_tax_sales end) all_excluding_tax_sales
    from csx_dw.dws_sale_r_d_detail
    where sdt>='20190101'
    and channel_code in('1','7','9','4','5','6')
    group by substr(sdt,1,6),province_name,customer_no
  )a 
  left join csx_tmp.tmp_cust_sale_0 b on a.customer_no=b.customer_no
  group by a.smonth,a.province_name
)a
group by smonth,province_name
grouping sets (smonth,(smonth,province_name));







--查数
select a.smonth,a.province_name,a.customer_no,b.first_order_date,
    sum(a.BBBC_excluding_tax_sales) BBBC_excluding_tax_sales,
    sum(case when substr(b.first_order_date,1,6)<>a.smonth then a.BBBC_excluding_tax_sales end) BBBC_excluding_tax_sales_old,
    sum(case when substr(b.first_order_date,1,6)=a.smonth then a.BBBC_excluding_tax_sales end) BBBC_excluding_tax_sales_new,
    count(distinct case when a.BBBC_excluding_tax_sales<>0 then a.customer_no end) BBBC_counts,
    count(distinct case when a.BBBC_excluding_tax_sales<>0 and substr(b.first_order_date,1,6)<>a.smonth then a.customer_no end) BBBC_counts_old,
    count(distinct case when a.BBBC_excluding_tax_sales<>0 and substr(b.first_order_date,1,6)=a.smonth then a.customer_no end) BBBC_counts_new
  from
  (
    select substr(sdt,1,6) smonth,province_name,customer_no,
      sum(excluding_tax_sales) BBBC_excluding_tax_sales,
      sum(case when channel_code in('1','9') then excluding_tax_sales end) B_excluding_tax_sales,
      sum(case when channel_code in('1','9') and business_type_code<>'4' then excluding_tax_sales end) B_nof_excluding_tax_sales,
      sum(case when business_type_code='1' then excluding_tax_sales end) ripei_excluding_tax_sales
    from csx_dw.dws_sale_r_d_detail
    where sdt>='20190101'
    and channel_code in('1','7','9')
    and ((province_name='四川省' and substr(sdt,1,6)='202010') 
      or (province_name='福建省' and substr(sdt,1,6)='201912')
      or (province_name='北京市' and substr(sdt,1,6) in('202012','202101'))
      )
    --and customer_no in('113131')
    group by substr(sdt,1,6),province_name,customer_no
  )a 
  left join csx_tmp.tmp_cust_sale_0 b on a.customer_no=b.customer_no
  where b.first_order_date is null
  group by a.smonth,a.province_name,a.customer_no,b.first_order_date;
  
select province_name,customer_no,customer_name,sdt,business_type_code,business_type_name,channel_code,channel_name,sales_type,sales_value
from csx_dw.dws_sale_r_d_detail 
where sdt>='20190101'
and channel_code in('1','7','9','4','5','6')
--and sales_type<>'fanli'
and customer_no in('113131','103242','105536','106735');
  
  