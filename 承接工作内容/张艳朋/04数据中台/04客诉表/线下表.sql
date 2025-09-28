select * from 
	(
	select
		id,complaint_no
	from 
		complaint_department_deal_detail
	) a 
	join
		(
		select
			deal_detail_id,
			regexp_replace(plan,'\n|\t|\r|\,|\"|\\\\n','') as plan,
			-- plan,
			responsible_person_name
		from	
			department_deal_detail
		) b on b.deal_detail_id=a.id
		
		
		
	select
		id,complaint_no,responsible_department_name
	from 
		complaint_department_deal_detail where complaint_no in ('KS22101700000012','KS22101700000016','KS22101800000001')