-- 排查新客商机销售 绩效

		select
			if(to_date(business_sign_time)=to_date(first_business_sign_time),'新商机','老商机') cust_flag,
			business_sign_time,
			regexp_replace(substr(to_date(business_sign_time),1,7),'-','') business_sign_month,
			business_number,
			customer_id,
			customer_code,
			customer_name,
            contract_number,			
			performance_region_name,performance_province_name,performance_city_name,
            business_type_code,
			business_type_name,
			contract_cycle_desc,estimate_contract_amount,
			regexp_replace(to_date(business_sign_time),'-','') business_sign_date,
			to_date(business_sign_time) as business_sign_date_2,
			regexp_replace(to_date(first_business_sign_time),'-','') first_business_sign_date,business_stage
		from 
			csx_dim.csx_dim_crm_business_info
		where 
			sdt='current'
			and business_attribute_code in (1,2,5) --  商机属性编码 1：日配客户 2：福利客户 3：大宗贸易 4：m端 5：bbc 6：内购
			and status=1  --  是否有效 0.无效 1.有效 (status=0,'停止跟进')
			and business_stage=5
			and to_date(business_sign_time) >= '2023-01-01'  and to_date(business_sign_time) < '2024-01-01' 
			and performance_province_name !='平台-B'
			and customer_code in ('235752','121337','128848',
'224109',
'184951',
'124558',
'125719',
'231293',
'121337',
'232490',
'114859',
'115681',
'120872',
'232491',
'233673',
'237398','184951','232490'
)order by customer_code
;

select sdt,
  customer_code,
  customer_name,
  business_type_name,
  sum(sale_qty) sale_qty,
  sum(sale_amt) sale,
  sum(profit) profit
from
       csx_dws.csx_dws_sale_detail_di a
where
      sdt >= '202312001'
      and sdt <= '20231231'
  and a.channel_code in ('1', '7', '9')
 	and customer_code in (
 	'235752',
 	'121337',
-- 	'128848'
-- '224109',
-- '184951',
-- '124558',
-- '125719',
-- '231293',
-- '121337',
-- '232490',
-- '114859',
-- '115681',
   '120872'
-- '232491',
-- '233673',
-- '237398','184951','232490'
)
group by business_type_name,
  customer_code,
  customer_name,
  sdt


  ;

  select 
  a.customer_code as customer_no,
  a.business_number,
  a.business_type_code,
  a.business_sign_time as sign_time,
  a.start_date,
  sdt,
  if(ook=1 and b.customer_code is not null,'${i_sdate}','99990101') as end_date,
  if(ook=1 and b.customer_code is not null,'已核销','未核销') as business_type,
   other_needs_code,
  '${i_sdate}' as sdt
from 
  (
     select
          customer_code,
		  business_number,
		  business_type_code,
		  business_sign_time,
		  start_date,
		  other_needs_code,
		  row_number() over(partition by customer_code,business_type_code,other_needs_code order by business_sign_time) ook
        from
		(
		select
          customer_no as customer_code,
		  business_number,
		  business_type_code,
		  from_unixtime(unix_timestamp(sign_time,'yyyy-MM-dd HH:mm:ss')) as business_sign_time,
		  start_date,
		  other_needs_code
        from csx_analyse.csx_analyse_sale_d_customer_business_number_di
        where  sdt='${i_sdate1}' and start_date>='20210801' and  business_type='未核销'
        union all
        select
           customer_code,
		   business_number,
           cast(business_type_code as string) business_type_code,
           from_unixtime(unix_timestamp(business_sign_time,'yyyy-MM-dd HH:mm:ss')) business_sign_time,
           regexp_replace(substr(business_sign_time,1,10),'-','')  start_date,
		   case when business_type_code=6 and other_needs_code='1' then '餐卡'
		        when business_type_code=6 and (other_needs_code<>'1' or other_needs_code is null) then '非餐卡'
				else '其他'  end  as other_needs_code
        FROM
              csx_dim.csx_dim_crm_business_info       
        WHERE  sdt='${i_sdate}' and business_stage = 5 
             and business_type_code in (1,2,6)  
        	 AND regexp_replace(substr(business_sign_time,1,10),'-','')='${i_sdate}' 
      )a
  )a		
left join   -- 销售表
(
   select   
   sdt,
	a.customer_code,
	a.business_type_code,
    if(a.business_type_code in (1,2),'其他',b.credit_pay_type_name) as credit_pay_type_name
   from    csx_dws.csx_dws_sale_detail_di a
   left join 
      (
	    SELECT 
          id,
          if(credit_pay_type_name='餐卡','餐卡','非餐卡') as credit_pay_type_name
        from csx_dws.csx_dws_bbc_sale_detail_di
        where  sdt='20231107' 
	   )b on a.id=b.id
   where a.sdt='20231107'  
         AND  a.order_channel_code<>4 
         and a.business_type_code in (1,2,6)
   group by customer_code,business_type_code,if(a.business_type_code in (1,2),'其他',b.credit_pay_type_name),sdt
)b on a.customer_code=b.customer_code and a.business_type_code=b.business_type_code and a.other_needs_code=b.credit_pay_type_name
where a.customer_code ='128848'


;




		select
          customer_no as customer_code,
		  business_number,
		  business_type_code,
		  from_unixtime(unix_timestamp(sign_time,'yyyy-MM-dd HH:mm:ss')) as business_sign_time,
		  start_date,
		  other_needs_code,
		  business_type
        from csx_analyse.csx_analyse_sale_d_customer_business_number_di
        where  sdt='20231231' and start_date>='20210801' 
        -- and  business_type='未核销'
        and customer_no in 
		 ('235752','121337','128848',
'224109',
'184951',
'124558',
'125719',
'231293',
'121337',
'232490',
'114859',
'115681',
'120872',
'232491',
'233673',
'237398','184951','232490'
)

;


select
           customer_code,
		   business_number,
           cast(business_type_code as string) business_type_code,
           from_unixtime(unix_timestamp(business_sign_time,'yyyy-MM-dd HH:mm:ss')) business_sign_time,
           regexp_replace(substr(business_sign_time,1,10),'-','')  start_date,
		   case when business_type_code=6 and other_needs_code='1' then '餐卡'
		        when business_type_code=6 and (other_needs_code<>'1' or other_needs_code is null) then '非餐卡'
				else '其他'  end  as other_needs_code,
				other_needs_code
        FROM
              csx_dim.csx_dim_crm_business_info       
        WHERE  sdt='20231231' and business_stage = 5 
             and business_type_code in (1,2,6)  
        	 AND regexp_replace(substr(business_sign_time,1,10),'-','')>='20231101' 
        	 and customer_code='128848'

           ;



     select
          customer_code,
		  business_number,
		  business_type_code,
		  business_sign_time,
		  start_date,
		  other_needs_code,
		  row_number() over(partition by customer_code,business_type_code,other_needs_code order by business_sign_time) ook
        from
		(
		select
          customer_no as customer_code,
		  business_number,
		  business_type_code,
		  from_unixtime(unix_timestamp(sign_time,'yyyy-MM-dd HH:mm:ss')) as business_sign_time,
		  start_date,
		  other_needs_code
        from csx_analyse.csx_analyse_sale_d_customer_business_number_di
        where  sdt='${i_sdate1}' and start_date>='20210801' and  business_type='未核销'
        union all
        select
           customer_code,
		   business_number,
           cast(business_type_code as string) business_type_code,
           from_unixtime(unix_timestamp(business_sign_time,'yyyy-MM-dd HH:mm:ss')) business_sign_time,
           regexp_replace(substr(business_sign_time,1,10),'-','')  start_date,
		   case when business_type_code=6 and other_needs_code='1' then '餐卡'
		        when business_type_code=6 and (other_needs_code<>'1' or other_needs_code is null) then '非餐卡'
				else '其他'  end  as other_needs_code
        FROM
              csx_dim.csx_dim_crm_business_info       
        WHERE  sdt='${i_sdate}' and business_stage = 5 
             and business_type_code in (1,2,6)  
        	 AND regexp_replace(substr(business_sign_time,1,10),'-','')='${i_sdate}' 
      )a
  where  a.customer_code='128848'

  ;




   select   
   sdt,
	a.customer_code,
	a.business_type_code,
    if(a.business_type_code in (1,2),'其他',b.credit_pay_type_name) as credit_pay_type_name
   from  csx_dws.csx_dws_sale_detail_di a
   left join 
      (
	    SELECT 
          id,
          if(credit_pay_type_name='餐卡','餐卡','非餐卡') as credit_pay_type_name
        from csx_dws.csx_dws_bbc_sale_detail_di
        where  sdt>='20231001' 
	   )b on a.id=b.id
   where a.sdt='20231107' and sdt<='20231231'  
         AND  a.order_channel_code<>4 
         and a.business_type_code in (1,2,6)
         and customer_code='128848'
   group by customer_code,business_type_code,if(a.business_type_code in (1,2),'其他',b.credit_pay_type_name),sdt
