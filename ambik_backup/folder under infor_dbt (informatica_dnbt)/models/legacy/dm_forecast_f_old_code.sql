-- customer group and lob (staging)

   -- with from staging hot_ptp
  --- dynamic cols
   --- target cols
    final as (
        select
            ,
                select dateadd('year', -3, date_trunc('year',s.sales_date)) from sales_promo

                -- replace 161 to 165 with incremental sales_Date on batch_update_date in load model
                -- hard code s.sales_date greater than from current_date (3 years back)
                -- sales data and mdp matrix combine and create one stage view
                -- customer group as staging view
                -- promotion data one more view -- stg_oracle__sales-promotions
                -- sales data and mdp matrix combine as one view (stg_oracle__sales_forecast) and add from line 206 conditions here , mdp and sales condition also comes here 
                -- line 173 to 205 comes in load
                -- (++) left outer join
                -- staging (we can have inner join) because already we have data
                -- load (we dont have data in the target so we use left outer join to find numm values)
                and mp.item_id = s.item_id
                and mp.location_id = s.location_id
                and cal.datet = s.sales_date
                and ac.t_ep_hot_abc_class_ep_id = mp.t_ep_hot_abc_class_ep_id
                and s.item_id = lp1.item_id(+)
                and s.location_id = lp1.location_id(+)
                and mp.t_ep_item_ep_id = ti.t_ep_item_ep_id
                -- and nvl(ti.ebs_item_dest_key,0) = msi.inventory_item_id(+)
                and ti.item = msi.segment1(+)
                and msi.organization_id(+) = 82
                and mp.t_ep_organization_ep_id = teo.t_ep_organization_ep_id
                -- and nvl(teo.ebs_org_dest_key,0) = mop.partner_id(+)
                and nvl(teo.organization, 'na') = mop.organization_code(+)
                and mp.t_ep_ebs_demand_class_ep_id = td.t_ep_ebs_demand_class_ep_id
                -- next step right outer join of sales_promo
                and sales_promo.location_id = sales_promo.location_id(+)
                and sales_promo.item_id = sales_promo.item_id(+)
                and sales_promo.sales_date = sales_promo.sales_date(+)
                and mop.sr_tp_id = ord.organization_id(+)
                and ord.set_of_books_id = sob.set_of_books_id(+)
                -- instead of cc and ea replace it with cust_group_lob 
                and mp.t_ep_e1_cust_cat_2_ep_id = cc.t_ep_e1_cust_cat_2_ep_id
                and mp.t_ep_ebs_account_ep_id = ea.t_ep_ebs_account_ep_id
                -- entire sub string replace with joining curtomer_grp_lob (customer_account_number) remove (195 from substr to 198)
                and hca.customer_account_number(+) = substr(
                    ea.ebs_account,
                    regexp_instr(ea.ebs_account, ':', -1, 1) + 1,
                    length(ea.ebs_account) - regexp_instr(ea.ebs_account, ':', -1, 1)
                )
                and fl.lookup_type(+) = 'hot_override_price_tag'
                and substr(ea.ebs_account, regexp_instr(ea.ebs_account, ':', -1) + 1)
                = fl.lookup_code(+)
                and s.sales_date >= trunc(current_date, 'mm')
                
            )
        select *
        from final