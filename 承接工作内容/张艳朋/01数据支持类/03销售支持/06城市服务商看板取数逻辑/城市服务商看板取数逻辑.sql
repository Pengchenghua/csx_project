select
	id, --自增ID
	supplier_code, --供应商编码
	supplier_name, --供应商名称
	customer_code, --客户编码
	customer_name, --客户名称
	accumulative_amount, --累计回款金额
	closing_date, --截至日期
	tax_code, --工商营业执照号（统一社会信用代码），取自主数据表md_supplier_info，字段名tax_code
	version_no --乐观锁
	*
from
	csx_b2b_settle.settle_customer_accumulative --客户回款
where
	is_delete=0
	
	
	
	
	
	
select
	id, --自增ID
	accumulative_amount, --累计付款金额
	tax_code, --工商营业执照号（统一社会信用代码），取自主数据表md_supplier_info，字段名tax_code
	version_no, --乐观锁
	--*,concat(tax_code,"a")
from
	csx_b2b_settle.settle_supplier_accumulative
where
	is_delete=0 --供应商付款
	
	
	
	
select
	*
from	
	csx_b2b_settle.statement_source_bill
	
	
	
	
SELECT 
	DISTINCT(head.id) AS id,
    head.tax_code AS taxCode,
    (
		SELECT 
			supplier_name
        FROM 
			`settle_supplier_accumulative_item` item
        WHERE 
			item.parent_id = head.id
            AND item.is_deleted = '0'
            AND item.order_no = '0'
    ) AS supplierName,
    (
		SELECT 
			supplier_code
		FROM 
			`settle_supplier_accumulative_item` item
		WHERE 
			item.parent_id = head.id
			AND item.is_deleted = '0'
			AND item.order_no = '0'
	) AS supplierCode1,
    (
		SELECT 
			supplier_code
		FROM 
			`settle_supplier_accumulative_item` item
		WHERE 
			item.parent_id = head.id
			AND item.is_deleted = '0'
			AND item.order_no = '1'
	) AS supplierCode2,
    (
		SELECT 
			supplier_code
		FROM 
			`settle_supplier_accumulative_item` item
		WHERE 
			item.parent_id = head.id
			AND item.is_deleted = '0'
			AND item.order_no = '2'
	) AS supplierCode3,
    (
		SELECT 
			supplier_code
		FROM 
			`settle_supplier_accumulative_item` item
		WHERE 
			item.parent_id = head.id
			AND item.is_deleted = '0'
			AND item.order_no = '3'
	) AS supplierCode4,
    head.accumulative_amount AS accumulativeAmount,
    head.update_by AS updateBy,
    head.update_time AS updateTime
FROM 
	`settle_supplier_accumulative` head
     LEFT JOIN `settle_supplier_accumulative_item` item ON head.id = item.parent_id
WHERE 
	head.is_deleted = '0'
	


select
	head.id,head.tax_code,head.accumulative_amount,
	supplierCode1.supplier_code,supplierCode1.supplier_name,
	supplierCode2.supplier_code,supplierCode3.supplier_code,supplierCode4.supplier_code
from	
	(	
	select
		id,tax_code,accumulative_amount
	from
		csx_b2b_settle.settle_supplier_accumulative
	where
		is_deleted='0'
	) as head
	left join --供应商编码1、名称
	    (
		select 
			parent_id,supplier_code,supplier_name
        from 
			csx_b2b_settle.settle_supplier_accumulative_item
        where 
            is_deleted = '0'
            and order_no = '0'
    ) as supplierCode1 on supplierCode1.parent_id=head.id
	left join --供应商编码2
	    (
		select 
			parent_id,supplier_code,supplier_name
        from 
			csx_b2b_settle.settle_supplier_accumulative_item
        where 
            is_deleted = '0'
            and order_no = '1'
    ) as supplierCode2 on supplierCode2.parent_id=head.id
	left join --供应商编码3
	    (
		select 
			parent_id,supplier_code,supplier_name
        from 
			csx_b2b_settle.settle_supplier_accumulative_item
        where 
            is_deleted = '0'
            and order_no = '2'
    ) as supplierCode3 on supplierCode3.parent_id=head.id
	left join --供应商编码4
	    (
		select 
			parent_id,supplier_code,supplier_name
        from 
			csx_b2b_settle.settle_supplier_accumulative_item
        where 
            is_deleted = '0'
            and order_no = '3'
    ) as supplierCode4 on supplierCode4.parent_id=head.id
	
	
	
--=====================================================================================================================
select
	supplier_payment.supplier_name,
	supplier_payment.supplier_code1,
	coalesce(supplier_payment.supplier_code2,0) as supplier_code2,
	coalesce(supplier_payment.supplier_code3,0) as supplier_code3,
	coalesce(supplier_payment.supplier_code4,0) as supplier_code4,
	coalesce(supplier_payment.accumulative_amount,0) as accumulative_amount,
	coalesce(customer_return.customer_code,'0') as customer_code,
	coalesce(customer_return.customer_name,'') as customer_name,
	coalesce(customer_return.accumulative_amount,0) as accumulative_amount
from
	( -- 客户回款
	select
		id, -- 自增ID
		supplier_code, -- 供应商编码
		supplier_name, -- 供应商名称
		customer_code, -- 客户编码
		customer_name, -- 客户名称
		accumulative_amount, -- 累计回款金额
		closing_date, -- 截至日期
		tax_code, -- 工商营业执照号（统一社会信用代码），取自主数据表md_supplier_info，字段名tax_code
		version_no -- 乐观锁
	from
		csx_b2b_settle.settle_customer_accumulative -- 客户回款表
	where
		is_deleted=0	
	) as customer_return
	left join -- 供应商付款
		(
		select
			head.id,head.tax_code,head.accumulative_amount,
			supplier1.supplier_code as supplier_code1,supplier1.supplier_name,
			supplier2.supplier_code as supplier_code2,supplier3.supplier_code as supplier_code3,supplier4.supplier_code as supplier_code4
		from	
			(	
			select
				id,tax_code,accumulative_amount
			from
				csx_b2b_settle.settle_supplier_accumulative -- 供应商付款表
			where
				is_deleted='0'
			) as head
			left join -- 供应商编码1、名称
				(
				select 
					parent_id,supplier_code,supplier_name
				from 
					csx_b2b_settle.settle_supplier_accumulative_item
				where 
					is_deleted = '0'
					and order_no = '0'
			) as supplier1 on supplier1.parent_id=head.id
			left join -- 供应商编码2
				(
				select 
					parent_id,supplier_code,supplier_name
				from 
					csx_b2b_settle.settle_supplier_accumulative_item
				where 
					is_deleted = '0'
					and order_no = '1'
			) as supplier2 on supplier2.parent_id=head.id
			left join -- 供应商编码3
				(
				select 
					parent_id,supplier_code,supplier_name
				from 
					csx_b2b_settle.settle_supplier_accumulative_item
				where 
					is_deleted = '0'
					and order_no = '2'
			) as supplier3 on supplier3.parent_id=head.id
			left join -- 供应商编码4
				(
				select 
					parent_id,supplier_code,supplier_name
				from 
					csx_b2b_settle.settle_supplier_accumulative_item
				where 
					is_deleted = '0'
					and order_no = '3'
			) as supplier4 on supplier4.parent_id=head.id
		) as supplier_payment on supplier_payment.tax_code=customer_return.tax_code