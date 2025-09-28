select *  from csx_tmp.report_finance_r_m_inventory_stock_sale
where sdt ='${sdate}'
and (surplus_qty+surplus_amt+loss_qty+loss_amt+qty+amt+surplus_loss_total_qty+surplus_loss_total_amt+sales_value+sales_cost+profit+profit_rate+loss_rate+inv_after_profit_rate) <> 0
${if(len(sq)==0,"","AND province_name in( '"+sq+"') ")}
${if(len(ck)==0,"","AND location_code in( '"+ck+"') ")}
;


select *  from csx_analyse.csx_analyse_fr_sss_finance_inventory_stock_sale_mi
where sdt ='${sdate}'
and (surplus_qty+surplus_amt+loss_qty+loss_amt+qty+amt+surplus_loss_total_qty+surplus_loss_total_amt+sale_amt_no_tax+sale_cost_no_tax+profit_no_tax+profit_no_tax_rate+loss_rate+inv_after_profit_no_tax_rate) <> 0
${if(len(sq)==0,"","AND performance_province_name in( '"+sq+"') ")}
${if(len(ck)==0,"","AND location_code in( '"+ck+"') ")}