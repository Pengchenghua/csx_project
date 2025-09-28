
select
	agreement_dc_code,agreement_dc_name,sign_company_code,sign_company_name,sale_amt
from 
	(
	select 
		sign_company_code,sign_company_name,agreement_dc_code,agreement_dc_name,sum(sale_amt)as sale_amt,sum(profit) as profit
	from 
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt between '20201201' and '20221130'
		and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)	
	group by 
		sign_company_code,sign_company_name,agreement_dc_code,agreement_dc_name
	) a  
	join   
		(
		select 
			company_code,company_name,table_type
		from 
			csx_dim.csx_dim_basic_company
		where 
			sdt='current'
			and table_type=2 -- 1 彩食鲜 2 永辉
		group by 
			company_code,company_name,table_type
		) b on b.company_code=a.sign_company_code
;



select
	smonth,agreement_dc_code,agreement_dc_name,sign_company_code,sign_company_name,sale_amt
from 
	(
	select 
		substr(sdt,1,6) as smonth,sign_company_code,sign_company_name,agreement_dc_code,agreement_dc_name,sum(sale_amt)as sale_amt,sum(profit) as profit
	from 
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt between '20201201' and '20221130'
		and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)	
	group by 
		substr(sdt,1,6),sign_company_code,sign_company_name,agreement_dc_code,agreement_dc_name
	) a  
	join   
		(
		select 
			company_code,company_name,table_type
		from 
			csx_dim.csx_dim_basic_company
		where 
			sdt='current'
			and table_type=2 -- 1 彩食鲜 2 永辉
		group by 
			company_code,company_name,table_type
		) b on b.company_code=a.sign_company_code