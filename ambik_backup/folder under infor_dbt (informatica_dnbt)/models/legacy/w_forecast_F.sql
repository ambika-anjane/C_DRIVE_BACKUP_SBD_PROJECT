with
    hot_ptp as (select * from {{ ref("stg_oracle__hot_ptp_sales_data") }}),



    final as (
        select
            hot_ptp.s_item_id,
            hot_ptp.s_location_id,
            hot_ptp.s_sales_date,
            hot_ptp.s_demand_ly,
            hot_ptp.s_actual_quantity,
            hot_ptp.cogs_sd,
            hot_ptp.sales_base_override,
            hot_ptp.fore_0_calc,
            hot_ptp.s_budget,
            hot_ptp.invoice_price_sd,
            hot_ptp.max_date_diff,
            
            --coalesce(hot_ptp.s_demand_ly,invoice_price_sd,NULL),
             coalesce(hot_ptp.s_demand_ly * invoice_price_sd,0)  last_year_sales_dlr,
           
                

            (
                case
                    when hot_ptp.max_date_diff in (-1, 0)
                    then 0
                    else
                        (
                            case
                                when hot_ptp.sales_base_override is null
                                then
                                    (
                                        case
                                            when hot_ptp.fore_0_calc is null
                                            then 0
                                            else hot_ptp.fore_0_calc
                                        end
                                    )
                                else hot_ptp.sales_base_override
                            end
                        )
                end
            ) base_forecast,
            hot_ptp.incremental_forecast,
            hot_ptp.total_forecast,
            case
                when hot_ptp.s_actual_quantity = 0 or hot_ptp.s_actual_quantity is null
                then
                    (
                        case
                            when hot_ptp.total_forecast is null
                            then 0
                            else hot_ptp.total_forecast
                        end
                    )
                else hot_ptp.s_actual_quantity
            end sales_proj_vol,
            (
                case
                    when
                        hot_ptp.s_actual_quantity = 0
                        or hot_ptp.s_actual_quantity is null
                    then
                        (
                            case
                                when hot_ptp.total_forecast is null
                                then 0
                                else hot_ptp.total_forecast
                            end
                        )
                    else hot_ptp.s_actual_quantity
                end
            ) * (
                case
                    when hot_ptp.invoice_price_sd is null
                    then
                        (
                            case
                                when hot_ptp.s_hot_est_list_price is null
                                then 0
                                else hot_ptp.s_hot_est_list_price
                            end
                        )
                    else hot_ptp.invoice_price_sd
                end
            )
            sales_proj_value,
            (
                case
                    when hot_ptp.max_date_diff in (-1, 0)
                    then 0
                    else
                        (
                            case
                                when hot_ptp.sales_base_override is null
                                then
                                    (
                                        case
                                            when hot_ptp.fore_0_calc is null
                                            then 0
                                            else fore_0_calc
                                        end
                                    )
                                else hot_ptp.sales_base_override
                            end
                        )
                end
            ) volume_base_future,
            (
                case
                    when hot_ptp.max_date_diff in (-1, 0)
                    then 0
                    else
                        (
                            case
                                when hot_ptp.sales_base_override is null
                                then
                                    (
                                        case
                                            when hot_ptp.fore_0_calc is null
                                            then 0
                                            else fore_0_calc
                                        end
                                    )
                                else hot_ptp.sales_base_override
                            end
                        )
                end
            ) * (
                case
                    when hot_ptp.invoice_price_sd is null
                    then
                        (
                            case
                                when hot_ptp.s_hot_est_list_price is null
                                then 0
                                else hot_ptp.s_hot_est_list_price
                            end
                        )
                    else hot_ptp.invoice_price_sd
                end
            ) volume_base_future_value,
            (
                case
                    when
                        (
                            case
                                when hot_ptp.s_enter_fore is null
                                then
                                    (
                                        case
                                            when hot_ptp.s_col_for_over is null
                                            then
                                                (
                                                    case
                                                        when
                                                            hot_ptp.s_manual_stat
                                                            is null
                                                        then
                                                            (
                                                                case
                                                                    when
                                                                        hot_ptp.s_sim_val_1
                                                                        is null
                                                                    then
                                                                        hot_ptp.fore_0_calc
                                                                    else
                                                                        hot_ptp.s_sim_val_1
                                                                end
                                                            )
                                                        else hot_ptp.s_manual_stat
                                                    end
                                                ) * (
                                                    1.00 - (
                                                        case
                                                            when
                                                                hot_ptp.s_trg_cannizn_perc
                                                                = 1
                                                            then
                                                                hot_ptp.s_trg_cannizn_perc
                                                            else 0
                                                        end
                                                    )
                                                )
                                                * (
                                                    1.00 - (
                                                        case
                                                            when
                                                                hot_ptp.s_src_cannizn_perc
                                                                is null
                                                            then 0
                                                            else
                                                                hot_ptp.s_src_cannizn_perc
                                                        end
                                                    )
                                                )
                                                * (
                                                    1.00 + (
                                                        case
                                                            when
                                                                hot_ptp.s_manual_fact
                                                                is null
                                                            then 0
                                                            else hot_ptp.s_manual_fact
                                                        end
                                                    )
                                                )
                                                + (
                                                    case
                                                        when
                                                            hot_ptp.s_fixed_prom is null
                                                        then 0
                                                        else hot_ptp.s_fixed_prom
                                                    end
                                                )
                                                * (
                                                    1.00 - (
                                                        case
                                                            when
                                                                hot_ptp.s_rule_coll
                                                                is null
                                                            then 0
                                                            else hot_ptp.s_rule_coll
                                                        end
                                                    )
                                                )
                                                + (
                                                    case
                                                        when hot_ptp.s_int_cost is null
                                                        then 0
                                                        else hot_ptp.s_int_cost
                                                    end
                                                )
                                                * (
                                                    case
                                                        when hot_ptp.s_rule_coll is null
                                                        then 0
                                                        else hot_ptp.s_rule_coll
                                                    end
                                                )
                                            else hot_ptp.s_col_for_over
                                        end
                                    )
                                else hot_ptp.s_ff
                            end
                        )
                        is null
                    then 0
                    else
                        (
                            case
                                when hot_ptp.s_enter_fore is null
                                then
                                    (
                                        case
                                            when hot_ptp.s_col_for_over is null
                                            then
                                                (
                                                    case
                                                        when
                                                            hot_ptp.s_manual_stat
                                                            is null
                                                        then
                                                            (
                                                                case
                                                                    when
                                                                        hot_ptp.s_sim_val_1
                                                                        is null
                                                                    then
                                                                        hot_ptp.fore_0_calc
                                                                    else
                                                                        hot_ptp.s_sim_val_1
                                                                end
                                                            )
                                                        else hot_ptp.s_manual_stat
                                                    end
                                                ) * (
                                                    1.00 - (
                                                        case
                                                            when
                                                                hot_ptp.s_trg_cannizn_perc
                                                                = 1
                                                            then
                                                                hot_ptp.s_trg_cannizn_perc
                                                            else 0
                                                        end
                                                    )
                                                )
                                                * (
                                                    1.00 - (
                                                        case
                                                            when
                                                                hot_ptp.s_src_cannizn_perc
                                                                is null
                                                            then 0
                                                            else
                                                                hot_ptp.s_src_cannizn_perc
                                                        end
                                                    )
                                                )
                                                * (
                                                    1.00 + (
                                                        case
                                                            when
                                                                hot_ptp.s_manual_fact
                                                                is null
                                                            then 0
                                                            else hot_ptp.s_manual_fact
                                                        end
                                                    )
                                                )
                                                + (
                                                    case
                                                        when
                                                            hot_ptp.s_fixed_prom is null
                                                        then 0
                                                        else hot_ptp.s_fixed_prom
                                                    end
                                                )
                                                * (
                                                    1.00 - (
                                                        case
                                                            when
                                                                hot_ptp.s_rule_coll
                                                                is null
                                                            then 0
                                                            else hot_ptp.s_rule_coll
                                                        end
                                                    )
                                                )
                                                + (
                                                    case
                                                        when hot_ptp.s_int_cost is null
                                                        then 0
                                                        else hot_ptp.s_int_cost
                                                    end
                                                )
                                                * (
                                                    case
                                                        when hot_ptp.s_rule_coll is null
                                                        then 0
                                                        else hot_ptp.s_rule_coll
                                                    end
                                                )
                                            else hot_ptp.s_col_for_over
                                        end
                                    )
                                else hot_ptp.s_ff
                            end
                        )
                end
            )
            * 1 final_forecast,
            (
                case
                    when hot_ptp.s_wcp_override is null
                    then
                        (
                            (
                                (
                                    case
                                        when hot_ptp.s_manual_stat is null
                                        then
                                            (
                                                case
                                                    when hot_ptp.s_sim_val_1 is null
                                                    then hot_ptp.fore_0_calc
                                                    else hot_ptp.s_sim_val_1
                                                end
                                            )
                                        else hot_ptp.s_manual_stat
                                    end
                                ) * (
                                    1.00 + (
                                        case
                                            when hot_ptp.s_manual_fact is null
                                            then 0
                                            else hot_ptp.s_manual_fact
                                        end
                                    )
                                )


                                * (
                                    case
                                        when hot_ptp.s_pct_final_weight is null
                                        then 1
                                        else hot_ptp.s_pct_final_weight
                                    end
                                )



                                + (
                                    case
                                        when hot_ptp.s_sales_override is null
                                        then
                                            (
                                                case
                                                    when
                                                        hot_ptp.s_sales_baseline is null
                                                    then 0
                                                    else hot_ptp.s_sales_baseline
                                                end
                                            )
                                        else hot_ptp.s_sales_override
                                    end
                                )



                                * (
                                    1.00 + (
                                        case
                                            when hot_ptp.s_sales_pct_change is null
                                            then 0
                                            else hot_ptp.s_sales_pct_change
                                        end
                                    )
                                )




                                * (
                                    case
                                        when hot_ptp.s_pct_sales_weight is null
                                        then 0
                                        else hot_ptp.s_pct_sales_weight
                                    end
                                )


                                + (
                                    case
                                        when hot_ptp.s_mktg_override is null
                                        then
                                            (
                                                case
                                                    when hot_ptp.s_mktg_baseline is null
                                                    then 0
                                                    else hot_ptp.s_mktg_baseline
                                                end
                                            )
                                        else hot_ptp.s_mktg_override
                                    end
                                )



                                * (
                                    1.00 + (
                                        case
                                            when hot_ptp.s_mktg_pct_change is null
                                            then 0
                                            else hot_ptp.s_mktg_pct_change
                                        end
                                    )
                                )

                                * (
                                    case
                                        when hot_ptp.s_pct_mktg_weight is null
                                        then 0
                                        else hot_ptp.s_pct_mktg_weight
                                    end
                                )
                            ) / (
                                (
                                    case
                                        when hot_ptp.s_pct_final_weight is null
                                        then 1
                                        else hot_ptp.s_pct_final_weight
                                    end
                                ) + (
                                    case
                                        when hot_ptp.s_pct_sales_weight is null
                                        then 0
                                        else hot_ptp.s_pct_sales_weight
                                    end
                                )
                                + (
                                    case
                                        when hot_ptp.s_pct_mktg_weight is null
                                        then 0
                                        else hot_ptp.s_pct_mktg_weight
                                    end
                                )
                            )
                        )
                    else hot_ptp.s_wcp_override
                end
            )
            * hot_ptp.lp_list_price c20_consensus_forecast_value,
            hot_ptp.s_last_update_date last_update_date,
            (
                case
                    when hot_ptp.s_actual_quantity is null
                    then 0
                    else hot_ptp.s_actual_quantity
                end
            ) * (
                case
                    when hot_ptp.s_hot_invoice_price_over is null
                    then
                        (
                            case
                                hot_ptp.s_hot_hyperion_unit
                                when hot_ptp.invoice_price_sd is null
                                then 0
                                else hot_ptp.invoice_price_sd
                            end
                        )
                    else hot_ptp.s_hot_invoice_price_over
                end
            )
            actual_ttl_value,
            (
                case
                    when hot_ptp.s_ebs_bh_book_qty_bd is null
                    then 0
                    else hot_ptp.s_ebs_bh_book_qty_bd
                end
            ) book_quantity_book_date,
            hot_ptp.s_actual_quantity history,
            hot_ptp.s_pseudo_sale c25_history_override,
            (
                case
                    when hot_ptp.s_demand is null
                    then
                        (
                            case
                                when hot_ptp.s_pseudo_sale is null
                                then hot_ptp.s_actual_quantity
                                else hot_ptp.s_pseudo_sale
                            end
                        ) * (
                            1.00 + (
                                case
                                    when hot_ptp.s_demand_fact is null
                                    then 0
                                    else hot_ptp.s_demand_fact
                                end
                            )
                        )
                    else
                        (
                            hot_ptp.s_demand * (
                                1.00 + (
                                    case
                                        when hot_ptp.s_demand_fact is null
                                        then 0
                                        else hot_ptp.s_demand_fact
                                    end
                                )
                            )
                        )
                end
            )
            adjusted_history,
            hot_ptp.s_sim_val_1 simulation,
            (
                case
                    when hot_ptp.s_sim_val_1 is null
                    then hot_ptp.fore_0_calc
                    else hot_ptp.s_sim_val_1
                end
            ) * (
                case
                    when hot_ptp.s_hot_invoice_price_over is null
                    then
                        (
                            case
                                when hot_ptp.invoice_price_sd is null
                                then 0
                                else hot_ptp.invoice_price_sd
                            end
                        )
                    else hot_ptp.s_hot_invoice_price_over
                end
            ) baseline_forecast__,
            (
                case
                    when hot_ptp.s_sim_val_1 is null
                    then hot_ptp.fore_0_calc
                    else hot_ptp.s_sim_val_1
                end
            )
            baseline_forecast,
            hot_ptp.s_manual_stat c_30_base_overridde,
            (
                case
                    when hot_ptp.s_ebs_bh_book_qty_bd is null
                    then 0
                    else hot_ptp.s_ebs_bh_book_qty_bd
                end
            ) * (
                case
                    when hot_ptp.s_hot_invoice_price_over is null
                    then
                        (
                            case
                                when hot_ptp.invoice_price_sd is null
                                then 0
                                else hot_ptp.invoice_price_sd
                            end
                        )
                    else hot_ptp.s_hot_invoice_price_over
                end
            ) book_quantity_book_date__,
            hot_ptp.s_actual_quantity * (
                case
                    when hot_ptp.s_hot_invoice_price_over is null
                    then
                        (
                            case
                                when hot_ptp.invoice_price_sd is null
                                then 0
                                else hot_ptp.invoice_price_sd
                            end
                        )
                    else hot_ptp.s_hot_invoice_price_over
                end
            ) history__,
            (
                (
                    case
                        when
                            (
                                case
                                    when hot_ptp.s_enter_fore is null
                                    then
                                        (
                                            case
                                                when hot_ptp.s_col_for_over is null
                                                then
                                                    (
                                                        case
                                                            when
                                                                hot_ptp.s_manual_stat
                                                                is null
                                                            then
                                                                (
                                                                    case
                                                                        when
                                                                            hot_ptp.s_sim_val_1
                                                                            is null
                                                                        then
                                                                            hot_ptp.fore_0_calc
                                                                        else
                                                                            hot_ptp.s_sim_val_1
                                                                    end
                                                                )
                                                            else hot_ptp.s_manual_stat
                                                        end
                                                    ) * (
                                                        1.00 - (
                                                            case
                                                                when
                                                                    hot_ptp.s_trg_cannizn_perc
                                                                    = 1
                                                                then
                                                                    hot_ptp.s_trg_cannizn_perc
                                                                else 0
                                                            end
                                                        )
                                                    )
                                                    * (
                                                        1.00 - (
                                                            case
                                                                when
                                                                    hot_ptp.s_src_cannizn_perc
                                                                    is null
                                                                then 0
                                                                else
                                                                    hot_ptp.s_src_cannizn_perc
                                                            end
                                                        )
                                                    )
                                                    * (
                                                        1.00 + (
                                                            case
                                                                when
                                                                    hot_ptp.s_manual_fact
                                                                    is null
                                                                then 0
                                                                else
                                                                    hot_ptp.s_manual_fact
                                                            end
                                                        )
                                                    )
                                                    + (
                                                        case
                                                            when
                                                                hot_ptp.s_fixed_prom
                                                                is null
                                                            then 0
                                                            else hot_ptp.s_fixed_prom
                                                        end
                                                    )
                                                    * (
                                                        1.00 - (
                                                            case
                                                                when
                                                                    hot_ptp.s_rule_coll
                                                                    is null
                                                                then 0
                                                                else hot_ptp.s_rule_coll
                                                            end
                                                        )
                                                    )
                                                    + (
                                                        case
                                                            when
                                                                hot_ptp.s_int_cost
                                                                is null
                                                            then 0
                                                            else hot_ptp.s_int_cost
                                                        end
                                                    )
                                                    * (
                                                        case
                                                            when
                                                                hot_ptp.s_rule_coll
                                                                is null
                                                            then 0
                                                            else hot_ptp.s_rule_coll
                                                        end
                                                    )
                                                else hot_ptp.s_col_for_over
                                            end
                                        )
                                    else hot_ptp.s_ff
                                end
                            )
                            is null
                        then 0
                        else
                            (
                                case
                                    when hot_ptp.s_enter_fore is null
                                    then
                                        (
                                            case
                                                when hot_ptp.s_col_for_over is null
                                                then
                                                    (
                                                        case
                                                            when
                                                                hot_ptp.s_manual_stat
                                                                is null
                                                            then
                                                                (
                                                                    case
                                                                        when
                                                                            hot_ptp.s_sim_val_1
                                                                            is null
                                                                        then
                                                                            hot_ptp.fore_0_calc
                                                                        else
                                                                            hot_ptp.s_sim_val_1
                                                                    end
                                                                )
                                                            else hot_ptp.s_manual_stat
                                                        end
                                                    ) * (
                                                        1.00 - (
                                                            case
                                                                when
                                                                    hot_ptp.s_trg_cannizn_perc
                                                                    = 1
                                                                then
                                                                    hot_ptp.s_trg_cannizn_perc
                                                                else 0
                                                            end
                                                        )
                                                    )
                                                    * (
                                                        1.00 - (
                                                            case
                                                                when
                                                                    hot_ptp.s_src_cannizn_perc
                                                                    is null
                                                                then 0
                                                                else
                                                                    hot_ptp.s_src_cannizn_perc
                                                            end
                                                        )
                                                    )
                                                    * (
                                                        1.00 + (
                                                            case
                                                                when
                                                                    hot_ptp.s_manual_fact
                                                                    is null
                                                                then 0
                                                                else
                                                                    hot_ptp.s_manual_fact
                                                            end
                                                        )
                                                    )
                                                    + (
                                                        case
                                                            when
                                                                hot_ptp.s_fixed_prom
                                                                is null
                                                            then 0
                                                            else hot_ptp.s_fixed_prom
                                                        end
                                                    )
                                                    * (
                                                        1.00 - (
                                                            case
                                                                when
                                                                    hot_ptp.s_rule_coll
                                                                    is null
                                                                then 0
                                                                else hot_ptp.s_rule_coll
                                                            end
                                                        )
                                                    )
                                                    + (
                                                        case
                                                            when
                                                                hot_ptp.s_int_cost
                                                                is null
                                                            then 0
                                                            else hot_ptp.s_int_cost
                                                        end
                                                    )
                                                    * (
                                                        case
                                                            when
                                                                hot_ptp.s_rule_coll
                                                                is null
                                                            then 0
                                                            else hot_ptp.s_rule_coll
                                                        end
                                                    )
                                                else hot_ptp.s_col_for_over
                                            end
                                        )
                                    else hot_ptp.s_ff
                                end
                            )
                    end
                )
                * 1
            ) * (
                case
                    when hot_ptp.s_hot_invoice_price_over is null
                    then
                        (
                            case
                                when hot_ptp.invoice_price_sd is null
                                then 0
                                else hot_ptp.invoice_price_sd
                            end
                        )
                    else hot_ptp.s_hot_invoice_price_over
                end
            )
            final_forecast__,
            (
                case
                    when hot_ptp.s_manual_stat is null
                    then
                        (
                            case
                                when hot_ptp.fore_0_calc is null
                                then 0
                                else hot_ptp.fore_0_calc
                            end
                        )
                    else hot_ptp.s_manual_stat
                end
            ) * (
                case
                    when hot_ptp.s_manual_fact is null then 0 else hot_ptp.s_manual_fact
                end
            ) change_to_base,
            hot_ptp.s_manual_stat manual_stat,
            (
                case
                    when hot_ptp.s_wcp_override is null
                    then
                        (
                            (
                                (
                                    case
                                        when hot_ptp.s_manual_stat is null
                                        then
                                            (
                                                case
                                                    when hot_ptp.s_sim_val_1 is null
                                                    then hot_ptp.fore_0_calc
                                                    else hot_ptp.s_sim_val_1
                                                end
                                            )
                                        else hot_ptp.s_manual_stat
                                    end
                                ) * (
                                    1.00 - (
                                        case
                                            when hot_ptp.s_trg_cannizn_perc = 1
                                            then hot_ptp.s_trg_cannizn_perc
                                            else 0
                                        end
                                    )
                                )
                                * (
                                    1.00 - (
                                        case
                                            when hot_ptp.s_src_cannizn_perc is null
                                            then 0
                                            else hot_ptp.s_src_cannizn_perc
                                        end
                                    )
                                )
                                * (
                                    1.00 + (
                                        case
                                            when hot_ptp.s_manual_fact is null
                                            then 0
                                            else hot_ptp.s_manual_fact
                                        end
                                    )
                                )
                                * (
                                    case
                                        when hot_ptp.s_pct_final_weight is null
                                        then 1
                                        else hot_ptp.s_pct_final_weight
                                    end
                                )
                                + (
                                    case
                                        when hot_ptp.s_sales_override is null
                                        then
                                            (
                                                case
                                                    when
                                                        hot_ptp.s_sales_baseline is null
                                                    then 0
                                                    else hot_ptp.s_sales_baseline
                                                end
                                            )
                                        else hot_ptp.s_sales_override
                                    end
                                )
                                * (
                                    1.00 + (
                                        case
                                            when hot_ptp.s_sales_pct_change is null
                                            then 0
                                            else hot_ptp.s_sales_pct_change
                                        end
                                    )
                                )
                                * (
                                    case
                                        when hot_ptp.s_pct_sales_weight is null
                                        then 0
                                        else hot_ptp.s_pct_sales_weight
                                    end
                                )
                                + (
                                    case
                                        when hot_ptp.s_mktg_override is null
                                        then
                                            (
                                                case
                                                    when hot_ptp.s_mktg_baseline is null
                                                    then 0
                                                    else hot_ptp.s_mktg_baseline
                                                end
                                            )
                                        else hot_ptp.s_mktg_override
                                    end
                                )
                                * (
                                    1.00 + (
                                        case
                                            when hot_ptp.s_mktg_pct_change is null
                                            then 0
                                            else hot_ptp.s_mktg_pct_change
                                        end
                                    )
                                )
                                * (
                                    case
                                        when hot_ptp.s_pct_mktg_weight is null
                                        then 0
                                        else hot_ptp.s_pct_mktg_weight
                                    end
                                )
                            ) / (
                                (
                                    case
                                        when hot_ptp.s_pct_final_weight is null
                                        then 1
                                        else hot_ptp.s_pct_final_weight
                                    end
                                ) + (
                                    case
                                        when hot_ptp.s_pct_sales_weight is null
                                        then 0
                                        else hot_ptp.s_pct_sales_weight
                                    end
                                )
                                + (
                                    case
                                        when hot_ptp.s_pct_mktg_weight is null
                                        then 0
                                        else hot_ptp.s_pct_mktg_weight
                                    end
                                )
                            )
                        )
                    else hot_ptp.s_wcp_override
                end
            )
            c36_consensus_forecast,
            (
                case
                    when hot_ptp.s_demand is null
                    then
                        (
                            case
                                when hot_ptp.s_pseudo_sale is null
                                then hot_ptp.s_actual_quantity
                                else hot_ptp.s_pseudo_sale
                            end
                        ) * (
                            1.00 + (
                                case
                                    when hot_ptp.s_demand_fact is null
                                    then 0
                                    else hot_ptp.s_demand_fact
                                end
                            )
                        )
                    else
                        (
                            hot_ptp.s_demand * (
                                1.00 + (
                                    case
                                        when hot_ptp.s_demand_fact is null
                                        then 0
                                        else hot_ptp.s_demand_fact
                                    end
                                )
                            )
                        )
                end
            ) * (
                case
                    when hot_ptp.s_hot_invoice_price_over is null
                    then
                        (
                            case
                                when hot_ptp.invoice_price_sd is null
                                then 0
                                else hot_ptp.invoice_price_sd
                            end
                        )
                    else hot_ptp.s_hot_invoice_price_over
                end
            ) c37_adjusted_history__,
            (
                case
                    when hot_ptp.max_date_diff in (-1, 0)
                    then 0
                    else
                        (
                            case
                                when hot_ptp.sales_base_override is null
                                then
                                    (
                                        case
                                            when hot_ptp.fore_0_calc is null
                                            then 0
                                            else hot_ptp.fore_0_calc
                                        end
                                    )
                                else hot_ptp.sales_base_override
                            end
                        )
                end
            ) * (
                case
                    when hot_ptp.s_hot_invoice_price_over is null
                    then
                        (
                            case
                                when hot_ptp.invoice_price_sd is null
                                then 0
                                else hot_ptp.invoice_price_sd
                            end
                        )
                    else hot_ptp.s_hot_invoice_price_over
                end
            )
            c38_base_forecast__,
            case
                when hot_ptp.mp_do_fore = 0
                then 'DO NOT FORECAST'
                when hot_ptp.mp_do_fore = 1
                then 'DO FORECAST'
                when hot_ptp.mp_do_fore = 2
                then 'DO ZERO FORECAST'
                else null
            end c39_forecast_flag,
            hot_ptp.hot_budget_review_1 c40_hot_budget_value_review_1,
            hot_ptp.hot_budget_review_2 c41_hot_budget_value_review_2,
            (
                case
                    when hot_ptp.s_hot_future_orders is null
                    then 0
                    else hot_ptp.s_hot_future_orders
                end
            ) c42_hot_future_orders,
            (
                case
                    when hot_ptp.s_hot_future_orders is null
                    then 0
                    else hot_ptp.s_hot_future_orders
                end
            ) * (
                case
                    when hot_ptp.s_hot_invoice_price_over is null
                    then
                        (
                            case
                                when hot_ptp.invoice_price_sd is null
                                then 0
                                else hot_ptp.invoice_price_sd
                            end
                        )
                    else hot_ptp.s_hot_invoice_price_over
                end
            ) c43_hot_future_orders__,
            hot_ptp.s_hot_wtd_ship c44_hot_wtd_ship,
            hot_ptp.one_month_lag_fcst c45_one_month_lag_fcst,
            hot_ptp.two_month_lag_fcst c46_two_month_lag_fcst,
            hot_ptp.three_month_lag_fcst c47_three_month_lag_fcst,
            hot_ptp.four_month_lag_fcst c48_four_month_lag_fcst,
            hot_ptp.s_hot_hyperion_unit c49_hot_hyperion_units,
            hot_ptp.s_hot_hyperion_budget c50_hot_hyperion_budget,
            hot_ptp.hot_budget_review_3 c51_hot_budget_value_review_3,
            hot_ptp.s_hot_hyperion_units_rev1 c52_hot_hyperion_units_rev1,
            hot_ptp.s_hot_hyperion_units_rev2 c53_hot_hyperion_units_rev2,
            hot_ptp.s_hot_hyperion_units_rev3 c54_hot_hyperion_units_rev3,
            hot_ptp.s_hot_hyperion_budget_rev1 c55_hot_hyperion_budget_rev1,
            hot_ptp.s_hot_hyperion_budget_rev2 c56_hot_hyperion_budget_rev2,
            hot_ptp.s_hot_hyperion_budget_rev3 c57_hot_hyperion_budget_rev3,
            hot_ptp.s_hot_hyperion_cogs_rev1 c58_hot_hyperion_cogs_rev1,
            hot_ptp.s_hot_hyperion_cogs_rev2 c59_hot_hyperion_cogs_rev2,
            hot_ptp.s_hot_hyperion_cogs_rev3 c60_hot_hyperion_cogs_rev3,
            hot_ptp.s_hot_targ_fcst_5_lag c61_five_month_lag_fcst,
            hot_ptp.s_hot_targ_fcst_6_lag c62_six_month_lag_fcst,
            hot_ptp.s_hot_targ_fcst_7_lag c63_seven_month_lag_fcst,
            hot_ptp.s_hot_targ_fcst_8_lag c64_eight_month_lag_fcst,
            hot_ptp.s_hot_targ_fcst_9_lag c65_nine_month_lag_fcst,
            hot_ptp.s_hot_targ_fcst_10_lag c66_ten_month_lag_fcst,
            hot_ptp.s_hot_targ_fcst_11_lag c67_eleven_month_lag_fcst,
            hot_ptp.s_hot_targ_fcst_12_lag c68_twelve_month_lag_fcst,
            hot_ptp.mp_hot_abc_flag c69_hot_abc_flag,
            hot_ptp.max_sales_date c70_max_sales_date,
            hot_ptp.s_bat_fcst_1_lag c71_one_month_lag_bat_fcst,
            hot_ptp.s_bat_fcst_2_lag c72_two_month_lag_bat_fcst,
            hot_ptp.s_bat_fcst_3_lag c73_three_month_lag_bat_fcst,
            hot_ptp.s_bat_fcst_4_lag c74_four_month_lag_bat_fcst,
            (
                case
                    when hot_ptp.max_date_diff = 1
                    then null
                    else
                        (
                            (
                                case
                                    when
                                        (
                                            case
                                                when hot_ptp.s_demand is null
                                                then
                                                    (
                                                        case
                                                            when
                                                                hot_ptp.s_pseudo_sale
                                                                is null
                                                            then
                                                                hot_ptp.s_actual_quantity
                                                            else hot_ptp.s_pseudo_sale
                                                        end
                                                    ) * (
                                                        1.00 + (
                                                            case
                                                                when
                                                                    hot_ptp.s_demand_fact
                                                                    is null
                                                                then 0
                                                                else
                                                                    hot_ptp.s_demand_fact
                                                            end
                                                        )
                                                    )
                                                else
                                                    (
                                                        hot_ptp.s_demand * (
                                                            1.00 + (
                                                                case
                                                                    when
                                                                        hot_ptp.s_demand_fact
                                                                        is null
                                                                    then 0
                                                                    else
                                                                        hot_ptp.s_demand_fact
                                                                end
                                                            )
                                                        )
                                                    )
                                            end
                                        )
                                        is null
                                    then 0
                                    else
                                        (
                                            case
                                                when hot_ptp.s_demand is null
                                                then
                                                    (
                                                        case
                                                            when
                                                                hot_ptp.s_pseudo_sale
                                                                is null
                                                            then
                                                                hot_ptp.s_actual_quantity
                                                            else hot_ptp.s_pseudo_sale
                                                        end
                                                    ) * (
                                                        1.00 + (
                                                            case
                                                                when
                                                                    hot_ptp.s_demand_fact
                                                                    is null
                                                                then 0
                                                                else
                                                                    hot_ptp.s_demand_fact
                                                            end
                                                        )
                                                    )
                                                else
                                                    (
                                                        hot_ptp.s_demand * (
                                                            1.00 + (
                                                                case
                                                                    when
                                                                        hot_ptp.s_demand_fact
                                                                        is null
                                                                    then 0
                                                                    else
                                                                        hot_ptp.s_demand_fact
                                                                end
                                                            )
                                                        )
                                                    )
                                            end
                                        )
                                end
                            ) - (
                                case
                                    when hot_ptp.three_month_lag_fcst is null
                                    then 0
                                    else hot_ptp.three_month_lag_fcst
                                end
                            )
                        )
                end
            ) c75_three_mth_error_lag_fcst,
            (
                case
                    when hot_ptp.max_date_diff = 1
                    then null
                    else
                        (
                            (
                                case
                                    when
                                        (
                                            case
                                                when hot_ptp.s_demand is null
                                                then
                                                    (
                                                        case
                                                            when
                                                                hot_ptp.s_pseudo_sale
                                                                is null
                                                            then
                                                                hot_ptp.s_actual_quantity
                                                            else hot_ptp.s_pseudo_sale
                                                        end
                                                    ) * (
                                                        1.00 + (
                                                            case
                                                                when
                                                                    hot_ptp.s_demand_fact
                                                                    is null
                                                                then 0
                                                                else
                                                                    hot_ptp.s_demand_fact
                                                            end
                                                        )
                                                    )
                                                else
                                                    (
                                                        hot_ptp.s_demand * (
                                                            1.00 + (
                                                                case
                                                                    when
                                                                        hot_ptp.s_demand_fact
                                                                        is null
                                                                    then 0
                                                                    else
                                                                        hot_ptp.s_demand_fact
                                                                end
                                                            )
                                                        )
                                                    )
                                            end
                                        )
                                        is null
                                    then 0
                                    else
                                        (
                                            case
                                                when hot_ptp.s_demand is null
                                                then
                                                    (
                                                        case
                                                            when
                                                                hot_ptp.s_pseudo_sale
                                                                is null
                                                            then
                                                                hot_ptp.s_actual_quantity
                                                            else hot_ptp.s_pseudo_sale
                                                        end
                                                    ) * (
                                                        1.00 + (
                                                            case
                                                                when
                                                                    hot_ptp.s_demand_fact
                                                                    is null
                                                                then 0
                                                                else
                                                                    hot_ptp.s_demand_fact
                                                            end
                                                        )
                                                    )
                                                else
                                                    (
                                                        hot_ptp.s_demand * (
                                                            1.00 + (
                                                                case
                                                                    when
                                                                        hot_ptp.s_demand_fact
                                                                        is null
                                                                    then 0
                                                                    else
                                                                        hot_ptp.s_demand_fact
                                                                end
                                                            )
                                                        )
                                                    )
                                            end
                                        )
                                end
                            ) - (
                                case
                                    when hot_ptp.four_month_lag_fcst is null
                                    then 0
                                    else hot_ptp.four_month_lag_fcst
                                end
                            )
                        )
                end
            ) c76_four_mth_error_lag_fcst,
            (
                case
                    when hot_ptp.max_date_diff = 1
                    then null
                    else
                        (
                            (
                                case
                                    when
                                        (
                                            case
                                                when hot_ptp.s_demand is null
                                                then
                                                    (
                                                        case
                                                            when
                                                                hot_ptp.s_pseudo_sale
                                                                is null
                                                            then
                                                                hot_ptp.s_actual_quantity
                                                            else hot_ptp.s_pseudo_sale
                                                        end
                                                    ) * (
                                                        1.00 + (
                                                            case
                                                                when
                                                                    hot_ptp.s_demand_fact
                                                                    is null
                                                                then 0
                                                                else
                                                                    hot_ptp.s_demand_fact
                                                            end
                                                        )
                                                    )
                                                else
                                                    (
                                                        hot_ptp.s_demand * (
                                                            1.00 + (
                                                                case
                                                                    when
                                                                        hot_ptp.s_demand_fact
                                                                        is null
                                                                    then 0
                                                                    else
                                                                        hot_ptp.s_demand_fact
                                                                end
                                                            )
                                                        )
                                                    )
                                            end
                                        )
                                        is null
                                    then 0
                                    else
                                        (
                                            case
                                                when hot_ptp.s_demand is null
                                                then
                                                    (
                                                        case
                                                            when
                                                                hot_ptp.s_pseudo_sale
                                                                is null
                                                            then
                                                                hot_ptp.s_actual_quantity
                                                            else hot_ptp.s_pseudo_sale
                                                        end
                                                    ) * (
                                                        1.00 + (
                                                            case
                                                                when
                                                                    hot_ptp.s_demand_fact
                                                                    is null
                                                                then 0
                                                                else
                                                                    hot_ptp.s_demand_fact
                                                            end
                                                        )
                                                    )
                                                else
                                                    (
                                                        hot_ptp.s_demand * (
                                                            1.00 + (
                                                                case
                                                                    when
                                                                        hot_ptp.s_demand_fact
                                                                        is null
                                                                    then 0
                                                                    else
                                                                        hot_ptp.s_demand_fact
                                                                end
                                                            )
                                                        )
                                                    )
                                            end
                                        )
                                end
                            ) - (
                                case
                                    when hot_ptp.s_bat_fcst_3_lag is null
                                    then 0
                                    else hot_ptp.s_bat_fcst_3_lag
                                end
                            )
                        )
                end
            ) c77_three_mth_error_lag_bat_fc,
            (
                case
                    when hot_ptp.max_date_diff = 1
                    then null
                    else
                        (
                            (
                                case
                                    when
                                        (
                                            case
                                                when hot_ptp.s_demand is null
                                                then
                                                    (
                                                        case
                                                            when
                                                                hot_ptp.s_pseudo_sale
                                                                is null
                                                            then
                                                                hot_ptp.s_actual_quantity
                                                            else hot_ptp.s_pseudo_sale
                                                        end
                                                    ) * (
                                                        1.00 + (
                                                            case
                                                                when
                                                                    hot_ptp.s_demand_fact
                                                                    is null
                                                                then 0
                                                                else
                                                                    hot_ptp.s_demand_fact
                                                            end
                                                        )
                                                    )
                                                else
                                                    (
                                                        hot_ptp.s_demand * (
                                                            1.00 + (
                                                                case
                                                                    when
                                                                        hot_ptp.s_demand_fact
                                                                        is null
                                                                    then 0
                                                                    else
                                                                        hot_ptp.s_demand_fact
                                                                end
                                                            )
                                                        )
                                                    )
                                            end
                                        )
                                        is null
                                    then 0
                                    else
                                        (
                                            case
                                                when hot_ptp.s_demand is null
                                                then
                                                    (
                                                        case
                                                            when
                                                                hot_ptp.s_pseudo_sale
                                                                is null
                                                            then
                                                                hot_ptp.s_actual_quantity
                                                            else hot_ptp.s_pseudo_sale
                                                        end
                                                    ) * (
                                                        1.00 + (
                                                            case
                                                                when
                                                                    hot_ptp.s_demand_fact
                                                                    is null
                                                                then 0
                                                                else
                                                                    hot_ptp.s_demand_fact
                                                            end
                                                        )
                                                    )
                                                else
                                                    (
                                                        hot_ptp.s_demand * (
                                                            1.00 + (
                                                                case
                                                                    when
                                                                        hot_ptp.s_demand_fact
                                                                        is null
                                                                    then 0
                                                                    else
                                                                        hot_ptp.s_demand_fact
                                                                end
                                                            )
                                                        )
                                                    )
                                            end
                                        )
                                end
                            ) - (
                                case
                                    when hot_ptp.s_bat_fcst_4_lag is null
                                    then 0
                                    else hot_ptp.s_bat_fcst_4_lag
                                end
                            )
                        )
                end
            ) c78_four_mth_error_lag_bat_fcs,
            hot_ptp.s_hot_est_list_price c79_hot_est_list_price,
            (
                case
                    when hot_ptp.max_date_diff = 1
                    then null
                    else
                        (
                            (
                                case
                                    when hot_ptp.s_actual_quantity is null
                                    then 0
                                    else hot_ptp.s_actual_quantity
                                end
                            ) - (
                                case
                                    when hot_ptp.one_month_lag_fcst is null
                                    then 0
                                    else hot_ptp.one_month_lag_fcst
                                end
                            )
                        )
                end
            ) c80_one_mth_error_lag_fcst,
            (
                case
                    when hot_ptp.period_diff >= 0
                    then hot_ptp.one_month_lag_fcst
                    else null
                end
            ) c81_disp_fcst_lag_1,
            (
                case
                    when hot_ptp.period_diff = -1
                    then hot_ptp.one_month_lag_fcst
                    else
                        (
                            case
                                when hot_ptp.period_diff >= 0
                                then hot_ptp.two_month_lag_fcst
                                else null
                            end
                        )
                end
            ) c82_disp_fcst_lag_2,
            (
                case
                    when hot_ptp.period_diff = -2
                    then hot_ptp.one_month_lag_fcst
                    when hot_ptp.period_diff = -1
                    then hot_ptp.two_month_lag_fcst
                    else
                        (
                            case
                                when hot_ptp.period_diff >= 0
                                then hot_ptp.three_month_lag_fcst
                                else null
                            end
                        )
                end
            ) c83_disp_fcst_lag_3,
            (
                case
                    when hot_ptp.period_diff = -3
                    then hot_ptp.one_month_lag_fcst
                    when hot_ptp.period_diff = -2
                    then hot_ptp.two_month_lag_fcst
                    when hot_ptp.period_diff = -1
                    then hot_ptp.three_month_lag_fcst
                    else
                        (
                            case
                                when hot_ptp.period_diff >= 0
                                then hot_ptp.four_month_lag_fcst
                                else null
                            end
                        )
                end
            ) c84_disp_fcst_lag_4,
            (
                case
                    when hot_ptp.period_diff = -4
                    then hot_ptp.one_month_lag_fcst
                    when hot_ptp.period_diff = -3
                    then hot_ptp.two_month_lag_fcst
                    when hot_ptp.period_diff = -2
                    then hot_ptp.three_month_lag_fcst
                    when hot_ptp.period_diff = -1
                    then hot_ptp.four_month_lag_fcst
                    else
                        (
                            case
                                when hot_ptp.period_diff >= 0
                                then hot_ptp.s_hot_targ_fcst_5_lag
                                else null
                            end
                        )
                end
            ) c85_disp_fcst_lag_5,
            (
                case
                    when hot_ptp.period_diff = -5
                    then hot_ptp.one_month_lag_fcst
                    when hot_ptp.period_diff = -4
                    then hot_ptp.two_month_lag_fcst
                    when hot_ptp.period_diff = -3
                    then hot_ptp.three_month_lag_fcst
                    when hot_ptp.period_diff = -2
                    then hot_ptp.four_month_lag_fcst
                    when hot_ptp.period_diff = -1
                    then hot_ptp.s_hot_targ_fcst_5_lag
                    else
                        (
                            case
                                when hot_ptp.period_diff >= 0
                                then hot_ptp.s_hot_targ_fcst_6_lag
                                else null
                            end
                        )
                end
            ) c86_disp_fcst_lag_6,
            (
                case
                    when hot_ptp.period_diff = -6
                    then hot_ptp.one_month_lag_fcst
                    when hot_ptp.period_diff = -5
                    then hot_ptp.two_month_lag_fcst
                    when hot_ptp.period_diff = -4
                    then hot_ptp.three_month_lag_fcst
                    when hot_ptp.period_diff = -3
                    then hot_ptp.four_month_lag_fcst
                    when hot_ptp.period_diff = -2
                    then hot_ptp.s_hot_targ_fcst_5_lag
                    when hot_ptp.period_diff = -1
                    then hot_ptp.s_hot_targ_fcst_6_lag
                    else
                        (
                            case
                                when hot_ptp.period_diff >= 0
                                then hot_ptp.s_hot_targ_fcst_7_lag
                                else null
                            end
                        )
                end
            ) c87_disp_fcst_lag_7,
            (
                case
                    when hot_ptp.period_diff = -7
                    then hot_ptp.one_month_lag_fcst
                    when hot_ptp.period_diff = -6
                    then hot_ptp.two_month_lag_fcst
                    when hot_ptp.period_diff = -5
                    then hot_ptp.three_month_lag_fcst
                    when hot_ptp.period_diff = -4
                    then hot_ptp.four_month_lag_fcst
                    when hot_ptp.period_diff = -3
                    then hot_ptp.s_hot_targ_fcst_5_lag
                    when hot_ptp.period_diff = -2
                    then hot_ptp.s_hot_targ_fcst_6_lag
                    when hot_ptp.period_diff = -1
                    then hot_ptp.s_hot_targ_fcst_7_lag
                    else
                        (
                            case
                                when hot_ptp.period_diff >= 0
                                then hot_ptp.s_hot_targ_fcst_8_lag
                                else null
                            end
                        )
                end
            ) c88_disp_fcst_lag_8,

            (
                case
                    when hot_ptp.period_diff = -8
                    then hot_ptp.one_month_lag_fcst
                    when hot_ptp.period_diff = -7
                    then hot_ptp.two_month_lag_fcst
                    when hot_ptp.period_diff = -6
                    then hot_ptp.three_month_lag_fcst
                    when hot_ptp.period_diff = -5
                    then hot_ptp.four_month_lag_fcst
                    when hot_ptp.period_diff = -4
                    then hot_ptp.s_hot_targ_fcst_5_lag
                    when hot_ptp.period_diff = -3
                    then hot_ptp.s_hot_targ_fcst_6_lag
                    when hot_ptp.period_diff = -2
                    then hot_ptp.s_hot_targ_fcst_7_lag
                    when hot_ptp.period_diff = -1
                    then hot_ptp.s_hot_targ_fcst_8_lag
                    else
                        (
                            case
                                when hot_ptp.period_diff >= 0
                                then hot_ptp.s_hot_targ_fcst_9_lag
                                else null
                            end
                        )
                end
            ) c89_disp_fcst_lag_9,
            (
                case
                    when hot_ptp.period_diff = -9
                    then hot_ptp.one_month_lag_fcst
                    when hot_ptp.period_diff = -8
                    then hot_ptp.two_month_lag_fcst
                    when hot_ptp.period_diff = -7
                    then hot_ptp.three_month_lag_fcst
                    when hot_ptp.period_diff = -6
                    then hot_ptp.four_month_lag_fcst
                    when hot_ptp.period_diff = -5
                    then hot_ptp.s_hot_targ_fcst_5_lag
                    when hot_ptp.period_diff = -4
                    then hot_ptp.s_hot_targ_fcst_6_lag
                    when hot_ptp.period_diff = -3
                    then hot_ptp.s_hot_targ_fcst_7_lag
                    when hot_ptp.period_diff = -2
                    then hot_ptp.s_hot_targ_fcst_8_lag
                    when hot_ptp.period_diff = -1
                    then hot_ptp.s_hot_targ_fcst_9_lag
                    else
                        (
                            case
                                when hot_ptp.period_diff >= 0
                                then hot_ptp.s_hot_targ_fcst_10_lag
                                else null
                            end
                        )
                end
            ) c90_disp_fcst_lag_10,
            (
                case
                    when hot_ptp.period_diff = -10
                    then hot_ptp.one_month_lag_fcst
                    when hot_ptp.period_diff = -9
                    then hot_ptp.two_month_lag_fcst
                    when hot_ptp.period_diff = -8
                    then hot_ptp.three_month_lag_fcst
                    when hot_ptp.period_diff = -7
                    then hot_ptp.four_month_lag_fcst
                    when hot_ptp.period_diff = -6
                    then hot_ptp.s_hot_targ_fcst_5_lag
                    when hot_ptp.period_diff = -5
                    then hot_ptp.s_hot_targ_fcst_6_lag
                    when hot_ptp.period_diff = -4
                    then hot_ptp.s_hot_targ_fcst_7_lag
                    when hot_ptp.period_diff = -3
                    then hot_ptp.s_hot_targ_fcst_8_lag
                    when hot_ptp.period_diff = -2
                    then hot_ptp.s_hot_targ_fcst_9_lag
                    when hot_ptp.period_diff = -1
                    then hot_ptp.s_hot_targ_fcst_10_lag
                    else
                        (
                            case
                                when hot_ptp.period_diff >= 0
                                then hot_ptp.s_hot_targ_fcst_11_lag
                                else null
                            end
                        )
                end
            ) c91_disp_fcst_lag_11,
            (
                case
                    when hot_ptp.period_diff = -11
                    then hot_ptp.one_month_lag_fcst
                    when hot_ptp.period_diff = -10
                    then hot_ptp.two_month_lag_fcst
                    when hot_ptp.period_diff = -9
                    then hot_ptp.three_month_lag_fcst
                    when hot_ptp.period_diff = -8
                    then hot_ptp.four_month_lag_fcst
                    when hot_ptp.period_diff = -7
                    then hot_ptp.s_hot_targ_fcst_5_lag
                    when hot_ptp.period_diff = -6
                    then hot_ptp.s_hot_targ_fcst_6_lag
                    when hot_ptp.period_diff = -5
                    then hot_ptp.s_hot_targ_fcst_7_lag
                    when hot_ptp.period_diff = -4
                    then hot_ptp.s_hot_targ_fcst_8_lag
                    when hot_ptp.period_diff = -3
                    then hot_ptp.s_hot_targ_fcst_9_lag
                    when hot_ptp.period_diff = -2
                    then hot_ptp.s_hot_targ_fcst_10_lag
                    when hot_ptp.period_diff = -1
                    then hot_ptp.s_hot_targ_fcst_11_lag
                    when hot_ptp.period_diff = 0
                    then hot_ptp.s_hot_targ_fcst_12_lag
                    else
                        (
                            case
                                when hot_ptp.period_diff < -11
                                then hot_ptp.one_month_lag_fcst
                                else null
                            end
                        )
                end
            ) c92_disp_fcst_lag_12,
            hot_ptp.period_diff c93_period_diff,
            hot_ptp.currency_code c94_currency_code,
            case
                when hot_ptp.mp_hot_do_fore_over = 0
                then 'DO NOT FORECAST'
                when hot_ptp.mp_hot_do_fore_over = 1
                then 'DO FORECAST'
                when hot_ptp.mp_hot_do_fore_over = 2
                then 'DO ZERO FORECAST'
                else null
            end
            c95_forecast_flag_override,
            (
                case
                    when hot_ptp.one_month_lag_fcst is null
                    then 0
                    else hot_ptp.one_month_lag_fcst
                end
            ) * (
                case
                    when hot_ptp.s_hot_invoice_price_over is null
                    then
                        (
                            case
                                when hot_ptp.invoice_price_sd is null
                                then 0
                                else hot_ptp.invoice_price_sd
                            end
                        )
                    else hot_ptp.s_hot_invoice_price_over
                end
            )
            c96_one_month_lag_fcst__,
            (
                case
                    when hot_ptp.two_month_lag_fcst is null
                    then 0
                    else hot_ptp.two_month_lag_fcst
                end
            ) * (
                case
                    when hot_ptp.s_hot_invoice_price_over is null
                    then
                        (
                            case
                                when hot_ptp.invoice_price_sd is null
                                then 0
                                else hot_ptp.invoice_price_sd
                            end
                        )
                    else hot_ptp.s_hot_invoice_price_over
                end
            )
            c97_two_month_lag_fcst__,
            (
                case
                    when hot_ptp.three_month_lag_fcst is null
                    then 0
                    else hot_ptp.three_month_lag_fcst
                end
            ) * (
                case
                    when hot_ptp.s_hot_invoice_price_over is null
                    then
                        (
                            case
                                when hot_ptp.invoice_price_sd is null
                                then 0
                                else hot_ptp.invoice_price_sd
                            end
                        )
                    else hot_ptp.s_hot_invoice_price_over
                end
            )
            c98_three_month_lag_fcst__,
            (
                case
                    when hot_ptp.four_month_lag_fcst is null
                    then 0
                    else hot_ptp.four_month_lag_fcst
                end
            ) * (
                case
                    when hot_ptp.s_hot_invoice_price_over is null
                    then
                        (
                            case
                                when hot_ptp.invoice_price_sd is null
                                then 0
                                else hot_ptp.invoice_price_sd
                            end
                        )
                    else hot_ptp.s_hot_invoice_price_over
                end
            )
            c99_four_month_lag_fcst__,
            hot_ptp.msi_inventory_item_id || '~' || hot_ptp.org_id c100_item_org_id,
            hot_ptp.td_ebs_demand_class c101_demand_class,
            -- hot_ptp.uplift_future c102_uplift_future,
            hot_ptp.s_item_id || '~' || hot_ptp.s_location_id c103_integration_id,
            -- sales_date_wid was taken out from integration_id
            hot_ptp.s_hot_1_lag_fcst_dol c104_one_month_lag_fcst_dol,
            hot_ptp.s_hot_unconstrained_demand c105_hot_unconstrained_demand,
            (hot_ptp.s_hot_unconstrained_demand) * (
                case
                    when hot_ptp.s_hot_invoice_price_over is null
                    then
                        (
                            case
                                when hot_ptp.invoice_price_sd is null
                                then 0
                                else hot_ptp.invoice_price_sd
                            end
                        )
                    else hot_ptp.s_hot_invoice_price_over
                end
            ) c106_hot_unconstrained_demand_,
            hot_ptp.s_hot_invoice_price_over c107_invoice_price_override,
            hot_ptp.msi_sales_account c108_sales_account,
            hot_ptp.org_id c109_org_id,
            -- customer_account_id condition yo check
            -- hot_ptp.customer_account_id c110_customer_account_id, 
            hot_ptp.msi_inventory_item_id c111_inventory_item_id,
            hot_ptp._batch_update_date
        -- replace with bacth_start_time
        from hot_ptp
        where
            hot_ptp._batch_update_date > current_date()
            and hot_ptp._batch_update_date < current_date()
           




    -- addd remaining cols (done)
    -- ad dymanic cols -- ( 2 done)
    -- add join conditiond (done)
    -- customer_account_id condition yo check (done)
    -- add incremental (done)
    -- check target columns and source colums
    -- check nvl conditiond in stage _ (not working) to chk with bala 
    -- check  and conditions in final by renaming stage cols (used for and
    -- conditions) - done
    -- two cols missing in target (cust_acct_id and uplife_future) to check with team
    -- - check
    -- last testing
    -- rename cols in test (with renamed cols in stage)
    -- instead of cc and ea replace it with cust_group_lob 
    -- entire sub string replace with joining curtomer_grp_lob
    -- (customer_account_number) remove (195 from substr to 198)*/
    )
select *
from final 