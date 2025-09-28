	select
		concat("'",base.tax_code) as tax_code,
		supplier_detail.supplier_name,
		supplier_detail.supplier_code_1,
		supplier_detail.supplier_code_2,
		supplier_detail.supplier_code_3,
		supplier_detail.supplier_code_4,
		base.current_supplier_amount,
		base.original_supplier_amount,
		base.supplier_amount,
		base.customer_name,
		base.customer_code,
		base.current_cus_amount,
		base.original_cus_amount,
		base.accumulative_amount
	from
		(
		select
			cus_info.customer_code,
			cus_info.customer_name,
			cus_info.tax_code,
			cus_info.current_cus_amount,
			cus_info.original_cus_amount,
			cus_info.accumulative_amount,
			coalesce(supplier_info.current_supplier_amount,0) as current_supplier_amount,
			coalesce(supplier_info.original_supplier_amount,0) as original_supplier_amount,
			coalesce(supplier_info.accumulative_amount,0) as supplier_amount
		from
			(
			select -- 客户回款
				customer_code,
				tax_code,
				SUBSTRING_INDEX(group_concat(distinct customer_name),',',1) as customer_name,
				coalesce(sum(if(type='本期',accumulative_amount,null)),0) as current_cus_amount,
				coalesce(sum(if(type='期初',accumulative_amount,null)),0) as original_cus_amount,
				coalesce(sum(accumulative_amount),0) as accumulative_amount
			from
				(
				select -- 期初 客户回款
					'期初' as type,
					customer_code, -- 客户编码
					customer_name,
					tax_code, -- 工商营业执照号（统一社会信用代码），取自主数据表md_supplier_info，字段名tax_code
					coalesce(sum(accumulative_amount),0) as accumulative_amount -- 累计回款金额
				from
					csx_b2b_settle.settle_customer_accumulative -- 客户回款表
				where
					is_deleted=0
				group by
					customer_code,customer_name,tax_code
				union -- 本期 客户回款
				select
					'本期' as type,
					a.customer_code,
					a.customer_name,
					c.tax_code,
					coalesce(sum(if(d.customer_code is null or date(a.happen_date)>date(d.closing_date),payment_amount,null)),0) as accumulative_amount
				from
					(
					select
						happen_date, -- 业务发生日期
						posting_time, -- 过账时间
						payment_amount, -- 本次核销金额
						customer_code, -- 客户编码
						customer_name,
						close_bill_no -- 单据类型为来源单就是来源单号/为期初账款就是id
					from
						csx_b2b_sss.sss_close_bill_account_record -- 单据核销流水明细
					where
						is_deleted=0 -- 删除标识
					) as a
					left join
						(
						select
							a.sales_order_code,
							b.tax_code
						from
							(
							select
								supplier_code, -- 供应商编码
								sales_order_code, -- 销售单号
								source_code
							from
								csx_b2b_settle.statement_source_bill -- 来源 入库单 退货单 结算单
							where
								source_code=4 -- 项目合伙人
								and payment_status=4 -- 已付款成功
							) as a
							left join 
								(
								select
									supplier_code, -- 供应商编码
									tax_code -- 税号
								from
									csx_b2b_settle.settle_supplier_base_info -- 供应商信息表
								group by 
									supplier_code,tax_code
								) as b on a.supplier_code=b.supplier_code
						group by
							a.sales_order_code,
							b.tax_code	
						) as c on a.close_bill_no=c.sales_order_code
					left join
						(
						select
							supplier_code, -- 供应商编码
							customer_code, -- 客户编码
							closing_date -- 截至日期
						from
							csx_b2b_settle.settle_customer_accumulative -- 客户回款表
						where
							is_deleted=0
						group by
							supplier_code,customer_code,closing_date
						) as d on d.customer_code=a.customer_code
				where
					c.sales_order_code is not null
				group by
					a.customer_code,
					a.customer_name,
					c.tax_code
				) as t1
			group by 
				customer_code,tax_code
			) as cus_info
			left join
				(
				select -- 供应商付款
					tax_code,
					coalesce(sum(if(type='本期',accumulative_amount,null)),0) as current_supplier_amount,
					coalesce(sum(if(type='期初',accumulative_amount,null)),0) as original_supplier_amount,
					coalesce(sum(accumulative_amount),0) as accumulative_amount
				from
					(
					select -- 期初 供应商付款
						'期初' as type,
						tax_code,
						coalesce(sum(accumulative_amount),0) as accumulative_amount
					from
						csx_b2b_settle.settle_supplier_accumulative -- 供应商付款表
					where
						is_deleted='0'
					group by 
						tax_code
					union -- 本期 供应商付款
					select
						'本期' as type,
						b.tax_code,
						coalesce(sum(if(c.closing_date is null or date(a.happen_date)>date(c.closing_date),statement_amount,null)),0) as accumulative_amount
					from
						(
						select
							happen_date, -- 业务发生日期
							paid_time, -- 付款时间
							statement_amount, -- 对账金额
							company_code, -- 所属公司编码
							supplier_code, -- 供应商编码
							supplier_name, -- 供应商名称
							customer_code, -- 客户编码
							sales_order_code -- 销售单号
						from
							csx_b2b_settle.statement_source_bill -- 来源 入库单 退货单 结算单
						where
							source_code=4 -- 项目合伙人
							and payment_status=4 -- 已付款成功
						) as a
						left join
							(
							select
								supplier_code, -- 供应商编码
								tax_code -- 税号
							from
								csx_b2b_settle.settle_supplier_base_info -- 供应商信息表
							group by 
								supplier_code,tax_code
							) as b on a.supplier_code=b.supplier_code
						left join
							(
							select
								tax_code,
								closing_date
							from
								(
								select
									supplier_code, -- 供应商编码
									customer_code, -- 客户编码
									closing_date -- 截至日期
								from
									csx_b2b_settle.settle_customer_accumulative -- 客户回款表
								where
									is_deleted=0
								group by
									supplier_code,customer_code,closing_date
								) as tmp1
								left join
									(
									select
										supplier_code, -- 供应商编码
										tax_code -- 税号
									from
										csx_b2b_settle.settle_supplier_base_info -- 供应商信息表
									group by 
										supplier_code,tax_code
									) as tmp2 on tmp1.supplier_code=tmp2.supplier_code
							group by 
								tax_code,
								closing_date
							) as c on c.tax_code=b.tax_code
					group by
						b.tax_code
					) as t2
				group by 
					tax_code
				) as supplier_info on supplier_info.tax_code=cus_info.tax_code
		union 
		select
			coalesce(cus_info.customer_code,'') as customer_code,
			coalesce(cus_info.customer_name,'') as customer_name,
			coalesce(supplier_info.tax_code,'') as tax_code,
			coalesce(cus_info.current_cus_amount,0) as current_cus_amount,
			coalesce(cus_info.original_cus_amount,0) as original_cus_amount,
			coalesce(cus_info.accumulative_amount,0) as accumulative_amount,
			coalesce(supplier_info.current_supplier_amount,0) as current_supplier_amount,
			coalesce(supplier_info.original_supplier_amount,0) as original_supplier_amount,
			coalesce(supplier_info.accumulative_amount,0) as supplier_amount
		from
			(
			select -- 客户回款
				customer_code,
				tax_code,
				SUBSTRING_INDEX(group_concat(distinct customer_name),',',1) as customer_name,
				coalesce(sum(if(type='本期',accumulative_amount,null)),0) as current_cus_amount,
				coalesce(sum(if(type='期初',accumulative_amount,null)),0) as original_cus_amount,
				coalesce(sum(accumulative_amount),0) as accumulative_amount
			from
				(
				select -- 期初 客户回款
					'期初' as type,
					customer_code, -- 客户编码
					customer_name,
					tax_code, -- 工商营业执照号（统一社会信用代码），取自主数据表md_supplier_info，字段名tax_code
					coalesce(sum(accumulative_amount),0) as accumulative_amount -- 累计回款金额
				from
					csx_b2b_settle.settle_customer_accumulative -- 客户回款表
				where
					is_deleted=0
				group by
					customer_code,customer_name,tax_code
				union -- 本期 客户回款
				select
					'本期' as type,
					a.customer_code,
					a.customer_name,
					c.tax_code,
					coalesce(sum(if(d.customer_code is null or date(a.happen_date)>date(d.closing_date),payment_amount,null)),0) as accumulative_amount
				from
					(
					select
						happen_date, -- 业务发生日期
						posting_time, -- 过账时间
						payment_amount, -- 本次核销金额
						customer_code, -- 客户编码
						customer_name,
						close_bill_no -- 单据类型为来源单就是来源单号/为期初账款就是id
					from
						csx_b2b_sss.sss_close_bill_account_record -- 单据核销流水明细
					where
						is_deleted=0 -- 删除标识
					) as a
					left join
						(
						select
							a.sales_order_code,
							b.tax_code
						from
							(
							select
								supplier_code, -- 供应商编码
								sales_order_code, -- 销售单号
								source_code
							from
								csx_b2b_settle.statement_source_bill -- 来源 入库单 退货单 结算单
							where
								source_code=4 -- 项目合伙人
								and payment_status=4 -- 已付款成功
							) as a
							left join 
								(
								select
									supplier_code, -- 供应商编码
									tax_code -- 税号
								from
									csx_b2b_settle.settle_supplier_base_info -- 供应商信息表
								group by 
									supplier_code,tax_code
								) as b on a.supplier_code=b.supplier_code
						group by
							a.sales_order_code,
							b.tax_code	
						) as c on a.close_bill_no=c.sales_order_code
					left join
						(
						select
							supplier_code, -- 供应商编码
							customer_code, -- 客户编码
							closing_date -- 截至日期
						from
							csx_b2b_settle.settle_customer_accumulative -- 客户回款表
						where
							is_deleted=0
						group by
							supplier_code,customer_code,closing_date
						) as d on d.customer_code=a.customer_code
				where
					c.sales_order_code is not null
				group by
					a.customer_code,
					a.customer_name,
					c.tax_code
				) as t1
			group by 
				customer_code,tax_code
			) as cus_info
			right join
				(
				select -- 供应商付款
					tax_code,
					coalesce(sum(if(type='本期',accumulative_amount,null)),0) as current_supplier_amount,
					coalesce(sum(if(type='期初',accumulative_amount,null)),0) as original_supplier_amount,
					coalesce(sum(accumulative_amount),0) as accumulative_amount
				from
					(
					select -- 期初 供应商付款
						'期初' as type,
						tax_code,
						coalesce(sum(accumulative_amount),0) as accumulative_amount
					from
						csx_b2b_settle.settle_supplier_accumulative -- 供应商付款表
					where
						is_deleted='0'
					group by 
						tax_code
					union -- 本期 供应商付款
					select
						'本期' as type,
						b.tax_code,
						coalesce(sum(if(c.closing_date is null or date(a.happen_date)>date(c.closing_date),statement_amount,null)),0) as accumulative_amount
					from
						(
						select
							happen_date, -- 业务发生日期
							paid_time, -- 付款时间
							statement_amount, -- 对账金额
							company_code, -- 所属公司编码
							supplier_code, -- 供应商编码
							supplier_name, -- 供应商名称
							customer_code, -- 客户编码
							sales_order_code -- 销售单号
						from
							csx_b2b_settle.statement_source_bill -- 来源 入库单 退货单 结算单
						where
							source_code=4 -- 项目合伙人
							and payment_status=4 -- 已付款成功
						) as a
						left join
							(
							select
								supplier_code, -- 供应商编码
								tax_code -- 税号
							from
								csx_b2b_settle.settle_supplier_base_info -- 供应商信息表
							group by 
								supplier_code,tax_code
							) as b on a.supplier_code=b.supplier_code
						left join
							(
							select
								tax_code,
								closing_date
							from
								(
								select
									supplier_code, -- 供应商编码
									customer_code, -- 客户编码
									closing_date -- 截至日期
								from
									csx_b2b_settle.settle_customer_accumulative -- 客户回款表
								where
									is_deleted=0
								group by
									supplier_code,customer_code,closing_date
								) as tmp1
								left join
									(
									select
										supplier_code, -- 供应商编码
										tax_code -- 税号
									from
										csx_b2b_settle.settle_supplier_base_info -- 供应商信息表
									group by 
										supplier_code,tax_code
									) as tmp2 on tmp1.supplier_code=tmp2.supplier_code
							group by 
								tax_code,
								closing_date
							) as c on c.tax_code=b.tax_code
					group by
						b.tax_code
					) as t2
				group by 
					tax_code
				) as supplier_info on supplier_info.tax_code=cus_info.tax_code
		) as base
		left join
			(
			select
				tax_code,
				supplier_name,
				supplier_code_1,
				if(supplier_code_2=supplier_code_1,'',supplier_code_2) as supplier_code_2,
				if(supplier_code_3=supplier_code_2,'',supplier_code_3) as supplier_code_3,
				if(supplier_code_4=supplier_code_3,'',supplier_code_4) as supplier_code_4
			from
				(
				select
					tax_code,
					SUBSTRING_INDEX(group_concat(distinct supplier_name),',',1) as supplier_name,
					SUBSTRING_INDEX(group_concat(distinct supplier_code),',',1) as supplier_code_1,
					SUBSTRING_INDEX(SUBSTRING_INDEX(group_concat(distinct supplier_code),',',2),',',-1) as supplier_code_2,
					SUBSTRING_INDEX(SUBSTRING_INDEX(group_concat(distinct supplier_code),',',3),',',-1) as supplier_code_3,
					SUBSTRING_INDEX(SUBSTRING_INDEX(group_concat(distinct supplier_code),',',4),',',-1) as supplier_code_4
				from
					csx_b2b_settle.settle_supplier_base_info
				group by
					tax_code
				) as t1
			) as supplier_detail on supplier_detail.tax_code=base.tax_code