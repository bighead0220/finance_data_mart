/*
*****************DDL建表语句******************
[FDM]基础层          52张表:	其中2个代码表,1个汇率信息表(无脚本),49个脚本;
[RDM]应用层"MA"  管会31张表:	其中1个汇率信息表(无脚本),30个脚本;
[RDM]应用层"GL"大总账15张表;
[RDM]应用层      报表 5张表;
[ADM]汇总层           3张表;
[APP]前端应用         8张表;
TOTAL:              114张表;         20160924
*/
/*
******************修改记录*******************
0.	CREATE BY wyh AT 20160924 
*/
--create schema f_fdm default include schema privileges;


create table f_fdm.f_org_tellr
(
   etl_date date not null,
   grp_typ numeric(1,0),
   tellr_num varchar(80),
   tellr_nm varchar(100),
   belg_org varchar(80),
   belg_dept varchar(300),
   post_sav_bank_ind varchar(50),
   id_card_num varchar(80),
   birth_dt date,
   gender varchar(50),
   edu_degr varchar(50),
   tellr_post_cd varchar(50),
   tellr_stat_cd varchar(50),
   tellr_typ_cd varchar(50),
   tel varchar(100),
   family_addr varchar(300),
   gradt_acad varchar(300),
   majr varchar(300),
   prtc_workday date,
   in_line_workday date,
   contr_start_dt date,
   contr_termn_dt date,
   contr_term_typ_cd varchar(50),
   rgst_dt date,
   rec_setup_tm date,
   rec_modi_tm date,
   cret varchar(100),
   final_mdfr varchar(100),
   sys_src varchar(50)
)
partition by (((date_part('year', f_org_tellr.etl_date) * 100) + date_part('month', f_org_tellr.etl_date)));

comment on table f_fdm.f_org_tellr is '柜员信息表';



create table f_fdm.f_agt_exp_coll
(
   etl_date date not null,
   grp_typ numeric(1,0),
   agmt_id varchar(80),
   cust_id varchar(80),
   org_num varchar(80),
   cur_cd varchar(50),
   prod_cd varchar(50),
   coll_dt date,
   due_dt date,
   subj_cd varchar(80),
   coll_amt numeric(24,2),
   coll_bal numeric(24,2),
   coll_typ_cd varchar(50),
   agmt_stat_cd varchar(50),
   mth_accm numeric(24,2),
   yr_accm numeric(24,2),
   mth_day_avg_bal numeric(24,2),
   yr_day_avg_bal numeric(24,2),
   sys_src varchar(50)
)
partition by (((date_part('year', f_agt_exp_coll.etl_date) * 100) + date_part('month', f_agt_exp_coll.etl_date)));

comment on table f_fdm.f_agt_exp_coll is '出口托收业务信息表';


create table f_fdm.f_agt_imp_clltn
(
   etl_date date not null,
   grp_typ numeric(1,0),
   agmt_id varchar(80),
   cust_id varchar(80),
   prod_cd varchar(50),
   clltn_dt date,
   due_dt date,
   draft_amt numeric(24,2),
   is_mtg_exchg varchar(2),
   is_adv_money varchar(2),
   is_multi_remtt varchar(2),
   is_ovrs_era_pay varchar(2),
   agmt_stat_cd varchar(50),
   org_num varchar(80),
   cur_cd varchar(50),
   curr_bal numeric(24,2),
   subj_cd varchar(80),
   mth_accm numeric(24,2),
   yr_accm numeric(24,2),
   mth_day_avg_bal numeric(24,2),
   yr_day_avg_bal numeric(24,2),
   sys_src varchar(50)
)
partition by (((date_part('year', f_agt_imp_clltn.etl_date) * 100) + date_part('month', f_agt_imp_clltn.etl_date)));

comment on table f_fdm.f_agt_imp_clltn is '进口代收业务信息表';


create table f_fdm.f_acct_crdt_card_info
(
   etl_date date not null,
   grp_typ numeric(1,0),
   card_num varchar(80),
   pri_card_card_num varchar(80),
   rmb_acct_num varchar(80),
   cust_num varchar(80),
   actv_ind varchar(2),
   chg_card_ind varchar(2),
   crdt_lmt numeric(24,2),
   card_typ_cd varchar(50),
   pri_sec_card_ind varchar(50),
   card_stat_cd varchar(50),
   issu_card_dt date,
   ltst_actv_dt date,
   pin_card_dt date,
   crdt_card_appl_stat_cd varchar(50),
   crdt_card_chk_reasn_cd varchar(50),
   fst_swipe_dt date,
   apprv_dt date,
   aval_lmt numeric(24,2),
   curmth_cash_amt numeric(24,2),
   curmth_consm_amt numeric(24,2),
   curmth_repay_amt numeric(24,2),
   curmth_annl_fee_incom_amt numeric(24,2),
   curmth_atm_draw_comm_fee_incom_amt numeric(24,2),
   curmth_amtbl_pay_comm_fee_incom_amt numeric(24,2),
   curmth_loss_comm_fee_incom_amt numeric(24,2),
   curmth_replm_card_comm_fee_incom_amt numeric(24,2),
   curmth_cntr_cash_comm_fee_incom_amt numeric(24,2),
   curmth_oth_comm_fee_incom_amt numeric(24,2),
   curmth_cro_cnr_tx_serv_cost_incom_amt numeric(24,2),
   curmth_late_chrg_comm_fee_incom_amt numeric(24,2),
   curmth_oth_cash_comm_fee_incom_amt numeric(24,2),
   prmt_org_cd varchar(50),
   sys_src varchar(50)
)
partition by (((date_part('year', f_acct_crdt_card_info.etl_date) * 100) + date_part('month', f_acct_crdt_card_info.etl_date)));

comment on table f_fdm.f_acct_crdt_card_info is '信用卡信息表';


create table f_fdm.f_prd_chrem
(
   etl_date date not null,
   grp_typ numeric(1,0),
   prod_cd varchar(50),
   prod_nm varchar(100),
   cur_cd varchar(50),
   coll_start_dt date,
   coll_end_dt date,
   coll_totl_amt numeric(24,2),
   had_coll_amt numeric(24,2),
   open_mode_ind varchar(2),
   brk_evn_ind varchar(2),
   indep_bal_ind varchar(2),
   prod_cate varchar(50),
   prod_stat varchar(50),
   sys_src varchar(50)
)
partition by (((date_part('year', f_prd_chrem.etl_date) * 100) + date_part('month', f_prd_chrem.etl_date)));

comment on table f_fdm.f_prd_chrem is '理财产品信息表';


create table f_fdm.f_acct_insu_agn
(
   etl_date date not null,
   grp_typ numeric(1,0),
   agmt_id varchar(80),
   insu_corp_cd varchar(50),
   prod_cd varchar(50),
   dpst_acct_num varchar(80),
   cust_id varchar(80),
   paym_mode_cd varchar(50),
   sign_dt date,
   insu_bred_cd varchar(50),
   insu_bred_efft_dt date,
   agmt_stat_cd varchar(50),
   cust_mgr_id varchar(80),
   org_num varchar(80),
   cur_cd varchar(50),
   insu_lot numeric(24,2),
   accm_prem numeric(24,2),
   insu_amt numeric(24,2),
   sys_src varchar(50)
)
partition by (((date_part('year', f_acct_insu_agn.etl_date) * 100) + date_part('month', f_acct_insu_agn.etl_date)));

comment on table f_fdm.f_acct_insu_agn is '保险代销账户信息表';



create table f_fdm.f_loan_corp_contr
(
   etl_date date not null,
   grp_typ numeric(1,0),
   agmt_id varchar(80),
   org_num varchar(80),
   cust_num varchar(80),
   cur_cd varchar(50),
   contr_amt numeric(24,2),
   start_dt date,
   due_dt date,
   agmt_stat_cd varchar(50),
   fst_distr_dt date,
   crdt_typ_cd varchar(50),
   crdt_lmt_id varchar(80),
   guar_mode_cd varchar(50),
   lvrg_mode varchar(50),
   draw_clos_dt_prd date,
   syndic_loan_typ_cd varchar(50),
   loan_drct_inds_cd varchar(50),
   contr_bal numeric(24,2),
   contr_next_accm_distr_amt numeric(24,2),
   contr_next_accm_repay_amt numeric(24,2),
   contr_aval_amt numeric(24,2),
   contr_paid_money_bal numeric(24,2),
   sys_src varchar(50)
)
partition by (((date_part('year', f_loan_corp_contr.etl_date) * 100) + date_part('month', f_loan_corp_contr.etl_date)));

comment on table f_fdm.f_loan_corp_contr is '公司贷款合同信息表';


create table f_fdm.f_cust_ibank
(
   etl_date date not null,
   grp_typ numeric(1,0),
   cust_num varchar(80),
   ibank_cust_legl_nm varchar(300),
   is_crdt_cust_ind varchar(30),
   is_lpr_ind varchar(30),
   cust_typ_cd varchar(50),
   fin_lics_id varchar(80),
   org_org_cd varchar(80),
   inds_typ_cd varchar(50),
   cert_typ_cd varchar(50),
   cert_num varchar(80),
   sys_src varchar(50)
)
partition by (((date_part('year', f_cust_ibank.etl_date) * 100) + date_part('month', f_cust_ibank.etl_date)));

comment on table f_fdm.f_cust_ibank is '同业客户基本信息表';


create table f_fdm.f_loan_indv_contr
(
   etl_date date not null,
   grp_typ numeric(1,0),
   agmt_id varchar(80),
   crdt_lmt_id varchar(80),
   org_num varchar(80),
   cust_num varchar(80),
   cur_cd varchar(50),
   start_dt date,
   due_dt date,
   contr_amt numeric(24,2),
   contr_bal numeric(24,2),
   contr_next_accm_distr_amt numeric(24,2),
   contr_next_accm_repay_amt numeric(24,2),
   contr_aval_amt numeric(24,2),
   contr_paid_money_bal numeric(24,2),
   agmt_stat_cd varchar(50),
   fst_distr_dt date,
   guar_mode_cd varchar(50),
   loan_drct_inds_cd varchar(50),
   sys_src varchar(50)
)
partition by (((date_part('year', f_loan_indv_contr.etl_date) * 100) + date_part('month', f_loan_indv_contr.etl_date)));

comment on table f_fdm.f_loan_indv_contr is '个人贷款合同信息表';


create table f_fdm.f_loan_indv_crdt
(
   etl_date date not null,
   grp_typ numeric(1,0),
   crdt_lmt_id varchar(80),
   cust_num varchar(80),
   belg_org_num varchar(80),
   cur_cd varchar(50),
   prod_cd varchar(50),
   is_can_cir_ind varchar(2),
   efft_dt date,
   invldtn_dt date,
   crdt_amt numeric(24,2),
   aval_lmt numeric(24,2),
   frz_lmt numeric(24,2),
   lmt_stat_cd varchar(50),
   guar_mode_cd varchar(50),
   majr_guar_mode_cd varchar(50),
   draw_clos_dt_prd date,
   sys_src varchar(50)
)
partition by (((date_part('year', f_loan_indv_crdt.etl_date) * 100) + date_part('month', f_loan_indv_crdt.etl_date)));

comment on table f_fdm.f_loan_indv_crdt is '个人授信额度信息表';


create table f_fdm.f_loan_corp_dubil_info
(
   etl_date date not null,
   grp_typ numeric(1,0),
   agmt_id varchar(80),
   cust_num varchar(80),
   org_num varchar(80),
   cur_cd varchar(50),
   prod_cd varchar(50),
   distr_dt date,
   st_int_dt date,
   due_dt date,
   payoff_dt date,
   wrtoff_dt date,
   loan_orgnl_amt numeric(24,2),
   exec_int_rate numeric(12,6),
   bmk_int_rate numeric(12,6),
   flt_ratio numeric(12,6),
   basis numeric(12,6),
   ovrd_int_rate numeric(12,6),
   int_base_cd varchar(50),
   cmpd_int_calc_mode_cd varchar(50),
   pre_chrg_int varchar(50),
   int_rate_attr_cd varchar(50),
   int_rate_adj_mode_cd varchar(50),
   amtbl_loan_ind varchar(2),
   repay_mode_cd varchar(50),
   repay_prd_cd varchar(50),
   orgnl_term numeric(24,0),
   orgnl_term_corp_cd varchar(50),
   rprc_prd numeric(24,0),
   rprc_prd_corp_cd varchar(50),
   last_rprc_day date,
   next_rprc_day date,
   next_pay_amt numeric(24,2),
   last_pay_day date,
   next_pay_day date,
   four_cls_cls varchar(50),
   fiv_cls varchar(50),
   agmt_stat_cd varchar(50),
   contr_agmt_id varchar(80),
   asst_secu_ind varchar(2),
   prin_subj varchar(80),
   curr_bal numeric(24,2),
   norm_bal numeric(24,2),
   slug_bal numeric(24,2),
   bad_debt_bal numeric(24,2),
   wrtoff_prin numeric(24,2),
   int_subj varchar(80),
   today_provs_int numeric(24,2),
   curmth_provs_int numeric(24,2),
   accm_provs_int numeric(24,2),
   today_chrg_int numeric(24,2),
   curmth_recvd_int numeric(24,2),
   accm_recvd_int numeric(24,2),
   int_adj_amt numeric(24,2),
   mth_accm numeric(24,2),
   yr_accm numeric(24,2),
   mth_day_avg_bal numeric(24,2),
   yr_day_avg_bal numeric(24,2),
   opr_org_num varchar(80),
   opr_tellr_num varchar(80),
   is_corp_cnstr_hous_loan varchar(2),
   free_int_ind varchar(2),
   free_int_prd numeric(24,0),
   expd_ind varchar(50),
   expd_due_dt date,
   loan_deval_prep_bal numeric(24,2),
   loan_deval_prep_amt numeric(24,2),
   ovrd_days numeric(24,0),
   ovrd_prin numeric(24,2),
   ovrd_int numeric(24,2),
   adv_money_ind varchar(2),
   adv_money_amt numeric(24,2),
   adv_money_bal numeric(24,2),
   sys_src varchar(50)
)
partition by (((date_part('year', f_loan_corp_dubil_info.etl_date) * 100) + date_part('month', f_loan_corp_dubil_info.etl_date)));

comment on table f_fdm.f_loan_corp_dubil_info is '公司贷款借据信息表';


create table f_fdm.f_acct_lpr_od
(
   etl_date date not null,
   grp_typ numeric(1,0),
   agmt_id varchar(80),
   cust_num varchar(80),
   prod_cd varchar(50),
   st_int_dt date,
   due_dt date,
   rprc_prd numeric(24,0),
   rprc_prd_corp_cd varchar(50),
   int_base_cd varchar(50),
   cmpd_int_calc_mode_cd varchar(50),
   pre_chrg_int varchar(50),
   int_rate_attr_cd varchar(50),
   orgnl_term numeric(24,0),
   orgnl_term_corp_cd varchar(50),
   init_lmt numeric(24,2),
   agmt_stat_cd varchar(50),
   org_num varchar(80),
   cur_cd varchar(50),
   exec_int_rate numeric(12,6),
   bmk_int_rate numeric(12,6),
   basic_diff numeric(12,6),
   last_rprc_day date,
   next_rprc_day date,
   prin_subj varchar(80),
   curr_bal numeric(24,2),
   int_subj varchar(80),
   today_provs_int numeric(24,2),
   curmth_provs_int numeric(24,2),
   accm_provs_int numeric(24,2),
   today_chrg_int numeric(24,2),
   curmth_recvd_int numeric(24,2),
   accm_recvd_int numeric(24,2),
   int_adj_amt numeric(24,2),
   deval_prep_bal numeric(24,2),
   deval_prep_amt numeric(24,2),
   mth_accm numeric(24,2),
   yr_accm numeric(24,2),
   mth_day_avg_bal numeric(24,2),
   yr_day_avg_bal numeric(24,2),
   sys_src varchar(50)
)
partition by (((date_part('year', f_acct_lpr_od.etl_date) * 100) + date_part('month', f_acct_lpr_od.etl_date)));

comment on table f_fdm.f_acct_lpr_od is '法人透支账户信息表';


create table f_fdm.f_loan_guar_contr
(
   etl_date date not null,
   grp_typ numeric(1,0),
   guar_agmt_id varchar(300),
   cust_num varchar(80),
   cur_cd varchar(50),
   loan_contr_agmt_id varchar(80),
   guar_contr_st_dt date,
   guar_contr_stp_dt date,
   guar_amt numeric(24,2),
   guar_typ_cd varchar(50),
   guar_ratio numeric(12,6),
   guartr_id varchar(600),
   guartr_nm varchar(600),
   guar_claim_amt numeric(24,2),
   agmt_stat_cd varchar(50),
   sys_src varchar(50)
)
partition by (((date_part('year', f_loan_guar_contr.etl_date) * 100) + date_part('month', f_loan_guar_contr.etl_date)));

comment on table f_fdm.f_loan_guar_contr is '贷款担保合同信息表';


create table f_fdm.f_agt_bill_discount
(
   etl_date date not null,
   grp_typ numeric(1,0),
   agmt_id varchar(80),
   cust_num varchar(80),
   org_num varchar(80),
   prod_cd varchar(50),
   acct_stat_cd varchar(50),
   fiv_cls_cd varchar(50),
   bill_id varchar(80),
   discnt_dt date,
   due_dt date,
   int_rate numeric(12,6),
   cur_cd varchar(50),
   prin_subj varchar(80),
   discnt_amt numeric(24,2),
   discnt_int numeric(24,2),
   int_subj varchar(80),
   curmth_amtbl_int numeric(24,2),
   int_adj_subj varchar(80),
   int_adj_amt numeric(24,2),
   curr_bal numeric(24,2),
   mth_accm numeric(24,2),
   yr_accm numeric(24,2),
   mth_day_avg_bal numeric(24,2),
   yr_day_avg_bal numeric(24,2),
   discnt_cate_cd varchar(50),
   loan_deval_prep_bal numeric(24,2),
   loan_deval_prep_amt numeric(24,2),
   sys_src varchar(50)
)
partition by (((date_part('year', f_agt_bill_discount.etl_date) * 100) + date_part('month', f_agt_bill_discount.etl_date)));

comment on table f_fdm.f_agt_bill_discount is '票据贴现信息表';


create table f_fdm.f_agt_acptn_info
(
   etl_date date not null,
   grp_typ numeric(1,0),
   biz_id varchar(80),
   bill_num varchar(80),
   drawr_cust_id varchar(80),
   org_num varchar(80),
   cur_cd varchar(50),
   prod_cd varchar(50),
   acpt_dt date,
   pay_dt date,
   subj_cd varchar(80),
   acpt_amt numeric(24,2),
   acpt_bal numeric(24,2),
   adv_money_amt numeric(24,2),
   margn_acct_num varchar(80),
   margn_cur varchar(50),
   margn_cfl_amt numeric(24,2),
   margn_ratio numeric(12,6),
   acct_stat_cd varchar(50),
   fiv_cls_cd varchar(50),
   deval_prep_bal numeric(24,2),
   deval_prep_amt numeric(24,2),
   mth_accm numeric(24,2),
   yr_accm numeric(24,2),
   mth_day_avg_bal numeric(24,2),
   yr_day_avg_bal numeric(24,2),
   sys_src varchar(50)
)
partition by (((date_part('year', f_agt_acptn_info.etl_date) * 100) + date_part('month', f_agt_acptn_info.etl_date)));

comment on table f_fdm.f_agt_acptn_info is '承兑汇票信息表';


create table f_fdm.f_acct_tbond_agn
(
   etl_date date not null,
   grp_typ numeric(1,0),
   agmt_id varchar(80),
   tbond_cd varchar(50),
   cust_num varchar(80),
   org_num varchar(80),
   cur_cd varchar(50),
   subj_cd varchar(80),
   buy_amt numeric(24,2),
   curr_bal numeric(24,2),
   actl_pmt_int numeric(24,2),
   tbond_int_rate numeric(12,6),
   int_rate_attr_cd varchar(50),
   term_prd_cnt numeric(1,0),
   term_prd_typ_cd varchar(50),
   tbond_cust_typ_cd varchar(50),
   is_adv_cash_ind varchar(2),
   bond_typ_cd varchar(50),
   agmt_stat_cd varchar(50),
   st_int_dt date,
   open_dt date,
   due_dt date,
   termn_dt date,
   cust_mgr_id varchar(80),
   mth_accm numeric(24,2),
   yr_accm numeric(24,2),
   mth_day_avg_bal numeric(24,2),
   yr_day_avg_bal numeric(24,2),
   sys_src varchar(50)
)
partition by (((date_part('year', f_acct_tbond_agn.etl_date) * 100) + date_part('month', f_acct_tbond_agn.etl_date)));

comment on table f_fdm.f_acct_tbond_agn is '国债代销账户信息表';


create table f_fdm.f_fnc_exchg_rate
(
   etl_date date not null,
   grp_typ numeric(1,0),
   orgnl_cur_cd varchar(50),
   convt_cur_cd varchar(50),
   efft_day date,
   exchg_rate_val numeric(12,6),
   sys_src varchar(50)
)
partition by (((date_part('year', f_fnc_exchg_rate.etl_date) * 100) + date_part('month', f_fnc_exchg_rate.etl_date)));

comment on table f_fdm.f_fnc_exchg_rate is '汇率信息表';


create table f_fdm.f_fnc_subj_taxrate
(
   etl_date date not null,
   grp_typ numeric(1,0),
   subj varchar(80),
   subj_nm varchar(100),
   org_typ varchar(50),
   incom_expns_typ varchar(50),
   tax_attr varchar(50),
   tax_rate numeric(12,6),
   sys_src varchar(50)
)
partition by (((date_part('year', f_fnc_subj_taxrate.etl_date) * 100) + date_part('month', f_fnc_subj_taxrate.etl_date)));

comment on table f_fdm.f_fnc_subj_taxrate is '科目税率信息表';


create table f_fdm.f_loan_mrtg_prop
(
   etl_date date not null,
   grp_typ numeric(1,0),
   pldg_id varchar(80),
   guar_contr_agmt_id varchar(300),
   pldg_typ_cd varchar(50),
   pldg_nm varchar(600),
   cur_cd varchar(50),
   pldg_rgst_val numeric(24,2),
   pldg_estim_val numeric(24,2),
   pldg_estim_dt date,
   had_mtg_val numeric(24,2),
   atrer_id varchar(80),
   atrer_nm varchar(300),
   mtg_usg_cd varchar(50),
   psbc_cfm_val numeric(24,2),
   val_cfm_dt date,
   cret_dpst_acct_num varchar(80),
   sys_src varchar(50)
)
partition by (((date_part('year', f_loan_mrtg_prop.etl_date) * 100) + date_part('month', f_loan_mrtg_prop.etl_date)));

comment on table f_fdm.f_loan_mrtg_prop is '贷款抵质押物信息表';


create table f_fdm.f_acct_chrem
(
   etl_date date not null,
   grp_typ numeric(1,0),
   mdl_biz_tx_acct_num varchar(80),
   contr_id varchar(80),
   prod_cd varchar(50),
   cust_id varchar(80),
   org_num varchar(80),
   cur_cd varchar(50),
   dpst_acct_num varchar(80),
   open_acct_dt date,
   prod_typ_cd varchar(50),
   prin_subj_cd varchar(80),
   prin_bal numeric(24,2),
   chrem_lot numeric(24,2),
   net_worth numeric(24,2),
   amt numeric(24,2),
   actl_prft_amt numeric(24,2),
   stop_to_today_accm_int numeric(24,2),
   stop_to_last_day_accm_int numeric(24,2),
   prft_subj_cd varchar(80),
   today_prft numeric(24,2),
   curmth_prft numeric(24,2),
   mth_accm numeric(24,2),
   yr_accm numeric(24,2),
   mth_day_avg_bal numeric(24,2),
   yr_day_avg_bal numeric(24,2),
   cust_mgr_id varchar(80),
   sys_src varchar(50)
)
partition by (((date_part('year', f_acct_chrem.etl_date) * 100) + date_part('month', f_acct_chrem.etl_date)));

comment on table f_fdm.f_acct_chrem is '理财账户信息表';


create table f_fdm.f_loan_corp_crdt
(
   etl_date date,
   grp_typ numeric(1,0),
   crdt_lmt_id varchar(80),
   cust_num varchar(80),
   belg_org_num varchar(80),
   cur_cd varchar(50),
   is_can_cir_ind varchar(2),
   efft_dt date,
   invldtn_dt date,
   crdt_amt numeric(24,2),
   aval_lmt numeric(24,2),
   frz_lmt numeric(24,2),
   lmt_stat_cd varchar(50),
   sys_src varchar(50)
);


create table f_fdm.f_cust_corp
(
   etl_date date,
   grp_typ numeric(1,0),
   cust_num varchar(80),
   cust_nm varchar(200),
   cust_org_org_cd varchar(50),
   belg_org_num varchar(80),
   open_acct_dt date,
   cust_typ_cd varchar(50),
   cust_inds_cd varchar(50),
   cust_size_cd varchar(50),
   rgst_cap_cur_cd varchar(50),
   rgst_cap numeric(24,2),
   ltst_rat_rslt varchar(50),
   cust_crdt_lvl_cd varchar(50),
   is_lst_corp_ind varchar(2),
   is_rltv_corp_ind varchar(2),
   is_and_sign_bkcp_agmt_ind varchar(2),
   is_grp_cust_ind varchar(2),
   corp_grp_prnt_corp_cust_num varchar(80),
   corp_grp_prnt_corp_nm varchar(400),
   is_vip varchar(2),
   corp_own_typ_cd varchar(50),
   cust_stat_cd varchar(50),
   corp_charc_cd varchar(50),
   cust_impt_lvl_cd varchar(50),
   cust_mgr_id varchar(80),
   strgy_cust_ind varchar(2),
   pltf_ind varchar(2),
   prtnr_ind varchar(2),
   ovrs_cust_rgst_num varchar(80),
   cert_typ_cd varchar(50),
   cert_num varchar(80),
   sys_src varchar(50)
);



create table f_fdm.f_agt_ftp_info
(
   etl_date date not null,
   grp_typ numeric(1,0),
   acct_num varchar(80),
   org_num varchar(80),
   cust_num varchar(80),
   cur_cd varchar(50),
   prod_cd varchar(50),
   prin_subj varchar(80),
   adj_befr_ftp_prc numeric(24,2),
   adj_befr_ftp_tran_incom_expns numeric(24,2),
   adj_post_ftp_prc numeric(24,2),
   adj_post_ftp_tran_incom_expns numeric(24,2),
   data_source varchar(50),
   sys_src varchar(50)
);



create table f_fdm.f_dpst_indv_acct
(
   etl_date date not null,
   grp_typ numeric(1,0),
   agmt_id varchar(80),
   cust_num varchar(80),
   org_num varchar(80),
   cur_cd varchar(50),
   prod_cd varchar(50),
   dpst_cate_cd varchar(50),
   cash_ind_cd varchar(50),
   st_int_dt date,
   due_dt date,
   open_acct_day date,
   clos_acct_day date,
   exec_int_rate numeric(12,6),
   bmk_int_rate numeric(12,6),
   basis numeric(12,6),
   int_base_cd varchar(50),
   cmpd_int_calc_mode_cd varchar(50),
   is_nt_int_ind varchar(2),
   pre_chrg_int varchar(50),
   int_rate_attr_cd varchar(50),
   orgnl_term numeric(24,0),
   orgnl_term_corp_cd varchar(50),
   rprc_prd numeric(24,0),
   rprc_prd_corp_cd varchar(50),
   last_rprc_day date,
   next_rprc_day date,
   is_autornw varchar(2),
   last_autornw_dt date,
   agmt_stat_cd varchar(50),
   prin_subj varchar(80),
   curr_bal numeric(24,2),
   int_subj varchar(80),
   today_provs_int numeric(24,2),
   curmth_provs_int numeric(24,2),
   accm_provs_int numeric(24,2),
   today_int_pay numeric(24,2),
   curmth_paid_int numeric(24,2),
   accm_paid_int numeric(24,2),
   int_adj_amt numeric(24,2),
   mth_accm numeric(24,2),
   yr_accm numeric(24,2),
   mth_day_avg_bal numeric(24,2),
   yr_day_avg_bal numeric(24,2),
   sys_src varchar(50)
)
partition by (((date_part('year', f_dpst_indv_acct.etl_date) * 100) + date_part('month', f_dpst_indv_acct.etl_date)));


create table f_fdm.f_fnc_accti2gl
(
   etl_date date not null,
   grp_typ numeric(1,0),
   src_sys_subj_cd varchar(50),
   majr_gl_subj_cd varchar(50),
   majr_gl_dtl_subj_cd varchar(50),
   majr_gl_prod_cd varchar(50),
   majr_gl_line_cd varchar(50),
   st_use_dt date,
   invldtn_dt date,
   sys_src varchar(50)
)
partition by (((date_part('year', f_fnc_accti2gl.etl_date) * 100) + date_part('month', f_fnc_accti2gl.etl_date)));

comment on table f_fdm.f_fnc_accti2gl is '科目对照关系表';


create table f_fdm.f_cap_biz_provs_info
(
   etl_date date not null,
   agmt_id varchar(80),
   cur_cd varchar(50),
   org_num varchar(80),
   provs_dt date,
   due_dt date,
   prin_subj varchar(80),
   deval_prep_bal numeric(24,2),
   sys_src varchar(50)
)
partition by (((date_part('year', f_cap_biz_provs_info.etl_date) * 100) + date_part('month', f_cap_biz_provs_info.etl_date)));

comment on table f_fdm.f_cap_biz_provs_info is '资金业务拨备信息表';


create table f_fdm.cd_rf_std_cd_tran_ref
(
   data_pltf_src_tab_nm varchar(100),
   data_pltf_src_fld_nm varchar(100),
   src_cd varchar(50),
   src_cd_desc varchar(100),
   tgt_cd_typ_cn_desc varchar(100),
   tgt_cd varchar(50),
   tgt_cd_desc varchar(100),
   modi_dt date,
   modi_reasn varchar(100)
);


create table f_fdm.cd_cd_table
(
   cd_typ_encd varchar(80),
   cd_typ_cn_desc varchar(100),
   cd varchar(50),
   cd_desc varchar(100),
   memo varchar(300),
   cd_load_mode varchar(100),
   modi_dt date,
   modi_reasn varchar(100)
);


create table f_fdm.f_agt_cap_offer
(
   grp_typ numeric(1,0),
   etl_date date,
   agmt_id varchar(80),
   tx_cnt_pty_cust_num varchar(80),
   ibank_offer_drct_cd varchar(50),
   prod_cd varchar(50),
   tx_comb_cd varchar(50),
   st_int_dt date,
   due_dt date,
   int_base_cd varchar(50),
   cmpd_int_calc_mode_cd varchar(50),
   int_rate_attr_cd varchar(50),
   orgnl_term numeric(24,0),
   orgnl_term_corp_cd varchar(50),
   rprc_prd numeric(24,0),
   rprc_prd_corp_cd varchar(50),
   org_num varchar(80),
   cust_acct_num varchar(80),
   cur_cd varchar(50),
   curr_int_rate numeric(12,6),
   bmk_int_rate numeric(12,6),
   basis numeric(12,6),
   last_rprc_day date,
   next_rprc_day date,
   prin_subj varchar(80),
   curr_bal numeric(24,2),
   int_subj varchar(80),
   today_provs_int numeric(24,2),
   curmth_provs_int numeric(24,2),
   accm_provs_int numeric(24,2),
   today_acpt_pay_int numeric(24,2),
   curmth_recvd_int_pay numeric(24,2),
   accm_recvd_int_pay numeric(24,2),
   mth_accm numeric(24,2),
   yr_accm numeric(24,2),
   mth_day_avg_bal numeric(24,2),
   yr_day_avg_bal numeric(24,2),
   sys_src varchar(50)
);


create table f_fdm.f_dpst_corp_acct
(
   etl_date date not null,
   grp_typ numeric(1,0),
   cust_acct_num varchar(80),
   sub_acct varchar(80),
   org_num varchar(80),
   cur_cd varchar(50),
   cust_num varchar(80),
   prod_cd varchar(50),
   dpst_cate_cd varchar(50),
   st_int_dt date,
   due_dt date,
   open_acct_day date,
   clos_acct_day date,
   exec_int_rate numeric(12,6),
   bmk_int_rate numeric(12,6),
   basis numeric(12,6),
   int_rate_adj_mode_cd varchar(50),
   adj_prd_typ_cd varchar(50),
   int_base_cd varchar(50),
   cmpd_int_calc_mode_cd varchar(50),
   pre_chrg_int varchar(50),
   int_rate_attr_cd varchar(50),
   orgnl_term numeric(24,0),
   orgnl_term_corp_cd varchar(50),
   rprc_prd numeric(24,0),
   rprc_prd_corp_cd varchar(50),
   last_rprc_day date,
   next_rprc_day date,
   is_autornw varchar(2),
   last_autornw_dt date,
   is_cash_mgmt_acct varchar(2),
   agmt_stat_cd varchar(50),
   prin_subj varchar(80),
   curr_bal numeric(24,2),
   int_subj varchar(80),
   today_provs_int numeric(24,2),
   curmth_provs_int numeric(24,2),
   accm_provs_int numeric(24,2),
   today_int_pay numeric(24,2),
   curmth_paid_int numeric(24,2),
   accm_paid_int numeric(24,2),
   int_adj_amt numeric(24,2),
   mth_accm numeric(24,2),
   yr_accm numeric(24,2),
   mth_day_avg_bal numeric(24,2),
   yr_day_avg_bal numeric(24,2),
   sys_src varchar(50)
)
partition by (((date_part('year', f_dpst_corp_acct.etl_date) * 100) + date_part('month', f_dpst_corp_acct.etl_date)));


create table f_fdm.f_agt_cap_stor
(
   etl_date date not null,
   grp_typ numeric(1,0),
   agmt_id varchar(80),
   tx_cnt_pty_cust_num varchar(80),
   org_num varchar(80),
   cust_acct_num varchar(80),
   acct_stat_cd varchar(50),
   cur_cd varchar(50),
   prod_cd varchar(50),
   tx_comb_cd varchar(50),
   st_int_dt date,
   due_dt date,
   int_base_cd varchar(50),
   cmpd_int_calc_mode_cd varchar(50),
   pre_chrg_int varchar(50),
   int_rate_attr_cd varchar(50),
   rprc_prd numeric(24,0),
   rprc_prd_corp_cd varchar(50),
   last_rprc_day date,
   next_rprc_day date,
   int_pay_freq varchar(100),
   orgnl_term numeric(24,0),
   orgnl_term_corp_cd varchar(50),
   bmk_int_rate numeric(12,6),
   curr_int_rate numeric(12,6),
   basis numeric(12,6),
   prin_subj varchar(80),
   curr_bal numeric(24,2),
   int_subj varchar(80),
   today_provs_int numeric(24,2),
   curmth_provs_int numeric(24,2),
   accm_provs_int numeric(24,2),
   today_acpt_pay_int numeric(24,2),
   curmth_recvd_int_pay numeric(24,2),
   accm_recvd_int_pay numeric(24,2),
   int_adj_subj varchar(80),
   int_adj_amt numeric(24,2),
   mth_accm numeric(24,2),
   yr_accm numeric(24,2),
   mth_day_avg_bal numeric(24,2),
   yr_day_avg_bal numeric(24,2),
   sys_src varchar(50)
)
partition by (((date_part('year', f_agt_cap_stor.etl_date) * 100) + date_part('month', f_agt_cap_stor.etl_date)));


create table f_fdm.f_agt_cap_bond_buy_back
(
   etl_date date not null,
   grp_typ numeric(1,0),
   agmt_id varchar(80),
   org_num varchar(80),
   bond_cd varchar(80),
   cur_cd varchar(50),
   prod_cd varchar(50),
   tx_comb_cd varchar(50),
   tx_cnt_pty_cust_num varchar(80),
   biz_drct_ind varchar(50),
   tx_day date,
   st_int_dt date,
   due_dt date,
   exec_int_rate numeric(12,6),
   prin_subj varchar(80),
   buy_back_amt numeric(24,2),
   int_subj varchar(80),
   buy_back_int numeric(24,2),
   self_biz_agent_cust_ind varchar(2),
   mth_accm numeric(24,2),
   yr_accm numeric(24,2),
   mth_day_avg_bal numeric(24,2),
   yr_day_avg_bal numeric(24,2),
   sys_src varchar(50)
)
partition by (((date_part('year', f_agt_cap_bond_buy_back.etl_date) * 100) + date_part('month', f_agt_cap_bond_buy_back.etl_date)));


create table f_fdm.f_cap_bond_inves
(
   etl_date date not null,
   grp_typ numeric(1,0),
   agmt_id varchar(80),
   org_num varchar(80),
   cur_cd varchar(50),
   bond_cd varchar(50),
   tx_tool_cls varchar(50),
   tx_comb_cd varchar(50),
   tx_cnt_pty_cust_num varchar(80),
   prod_cd varchar(50),
   bond_typ_cd varchar(50),
   bond_issur varchar(300),
   bond_issu_dt date,
   st_int_dt date,
   due_dt date,
   int_base_cd varchar(50),
   cmpd_int_calc_mode_cd varchar(50),
   int_pay_freq_cd varchar(50),
   int_rate_attr_cd varchar(50),
   orgnl_term numeric(24,0),
   orgnl_term_corp_cd varchar(50),
   rprc_prd numeric(24,0),
   rprc_prd_corp_cd varchar(50),
   last_rprc_day date,
   next_rprc_day date,
   curr_int_rate numeric(12,6),
   bmk_int_rate numeric(12,6),
   basis numeric(12,6),
   prin_subj varchar(80),
   buy_cost numeric(24,2),
   book_bal numeric(24,2),
   mkt_val numeric(24,2),
   deval_prep_bal numeric(24,2),
   int_subj varchar(80),
   today_provs_int numeric(24,2),
   curmth_provs_int numeric(24,2),
   accm_provs_int numeric(24,2),
   today_chrg_int numeric(24,2),
   curmth_recvd_int numeric(24,2),
   accm_recvd_int numeric(24,2),
   int_adj_subj varchar(80),
   today_int_adj_amt numeric(24,2),
   curmth_int_adj_amt numeric(24,2),
   accm_int_adj_amt numeric(24,2),
   valtn_prft_loss_subj varchar(80),
   today_valtn_prft_loss_amt numeric(24,2),
   curmth_valtn_prft_loss_amt numeric(24,2),
   accm_valtn_prft_loss_amt numeric(24,2),
   biz_prc_diff_prft_subj varchar(80),
   today_biz_prc_diff_amt numeric(24,2),
   curmth_biz_prc_diff_amt numeric(24,2),
   accm_biz_prc_diff_amt numeric(24,2),
   comm_fee_subj varchar(80),
   today_comm_fee_amt numeric(24,2),
   curmth_comm_fee_amt numeric(24,2),
   accm_comm_fee_amt numeric(24,2),
   mth_accm numeric(24,2),
   yr_accm numeric(24,2),
   mth_day_avg_bal numeric(24,2),
   yr_day_avg_bal numeric(24,2),
   sys_src varchar(50)
)
partition by (((date_part('year', f_cap_bond_inves.etl_date) * 100) + date_part('month', f_cap_bond_inves.etl_date)));


create table f_fdm.f_cap_raw_tx
(
   etl_date date not null,
   grp_typ numeric(1,0),
   agmt_id varchar(80),
   cust_acct_num varchar(80),
   cust_num varchar(80),
   org_num varchar(80),
   prod_cd varchar(50),
   cur_cd varchar(50),
   cnt_pty_cur_cd varchar(50),
   cap_tx_inves_comb_cd varchar(50),
   st_int_dt date,
   due_dt date,
   stl_dt date,
   int_base_cd varchar(50),
   cnt_pty_int_base_cd varchar(50),
   cmpd_int_calc_mode_cd varchar(50),
   pre_chrg_int varchar(50),
   int_pay_freq varchar(100),
   int_rate_attr_cd varchar(50),
   cnt_pty_int_rate_attr_cd varchar(50),
   orgnl_term numeric(24,0),
   orgnl_term_corp_cd varchar(50),
   exchg_rate numeric(12,6),
   curr_int_rate numeric(12,6),
   cnt_pty_curr_int_rate numeric(12,6),
   bmk_int_rate numeric(12,6),
   basis numeric(12,6),
   prin_subj varchar(80),
   notnl_prin numeric(24,2),
   cnt_pty_prin_subj varchar(80),
   cnt_pty_notnl_prin numeric(24,2),
   net_val numeric(24,2),
   curr_val numeric(24,2),
   valtn_prft_loss_subj varchar(80),
   today_valtn_prft_loss_amt numeric(24,2),
   curmth_valtn_prft_loss_amt numeric(24,2),
   paybl_int_subj varchar(80),
   today_paybl_int numeric(24,2),
   curmth_paybl_int numeric(24,2),
   accm_paybl_int numeric(24,2),
   recvbl_int_subj varchar(80),
   today_recvbl_int numeric(24,2),
   curmth_recvbl_int numeric(24,2),
   accm_recvbl_int numeric(24,2),
   biz_prc_diff_prft_subj varchar(80),
   today_biz_prc_diff_amt numeric(24,2),
   curmth_biz_prc_diff_amt numeric(24,2),
   accm_biz_prc_diff_amt numeric(24,2),
   mth_accm numeric(24,2),
   yr_accm numeric(24,2),
   mth_day_avg_bal numeric(24,2),
   yr_day_avg_bal numeric(24,2),
   sys_src varchar(50)
)
partition by (((date_part('year', f_cap_raw_tx.etl_date) * 100) + date_part('month', f_cap_raw_tx.etl_date)));


create table f_fdm.f_evt_insu_comm_fee
(
   etl_date date not null,
   grp_typ numeric(1,0),
   insu_corp_cd varchar(50),
   life_insur_comm_fee numeric(24,2),
   prop_insur_comm_fee numeric(24,2),
   renewal_comm_fee numeric(24,2),
   realtm_clltn_comm_fee numeric(24,2),
   bat_comm_fee numeric(24,2),
   oth_comm_fee numeric(24,2),
   comm_fee_sum numeric(24,2),
   sys_src varchar(50)
)
partition by (((date_part('year', f_evt_insu_comm_fee.etl_date) * 100) + date_part('month', f_evt_insu_comm_fee.etl_date)));


create table f_fdm.f_cust_insu
(
   etl_date date not null,
   grp_typ numeric(1,0),
   insu_corp_cd varchar(50),
   insu_corp_typ varchar(100),
   online_ind varchar(2),
   bat_tx_stl_mode varchar(50),
   cn_comnt varchar(300),
   cn_shtn varchar(100),
   en_nm varchar(300),
   en_abbr varchar(100),
   corp_addr varchar(600),
   zip_cd varchar(80),
   corp_tel varchar(80),
   corp_fax varchar(80),
   eml_addr varchar(100),
   rgst_cap numeric(24,2),
   corp_rat varchar(100),
   divid_corp_qty numeric(24,0),
   co_form varchar(100),
   contcr varchar(100),
   contcr_tel varchar(80),
   sys_src varchar(50)
)
partition by (((date_part('year', f_cust_insu.etl_date) * 100) + date_part('month', f_cust_insu.etl_date)));


create table f_fdm.f_agt_lc
(
   etl_date date not null,
   grp_typ numeric(1,0),
   agmt_id varchar(80),
   cust_num varchar(80),
   org_num varchar(80),
   cur_cd varchar(50),
   prod_cd varchar(50),
   loan_contr_id varchar(80),
   happ_dt date,
   due_dt date,
   subj_cd varchar(80),
   lc_amt numeric(24,2),
   lc_bal numeric(24,2),
   agmt_stat_cd varchar(50),
   contr_id varchar(80),
   issu_bank_cd varchar(50),
   issu_bank_nm varchar(300),
   advis_bank_cd varchar(50),
   advis_bank_nm varchar(300),
   nego_pay_line_cd varchar(50),
   nego_pay_line_nm varchar(300),
   margn_ratio numeric(12,6),
   margn_acct_num varchar(100),
   margn_amt numeric(24,2),
   fill_tab_dept varchar(300),
   guar_mode_cd varchar(50),
   mth_accm numeric(24,2),
   yr_accm numeric(24,2),
   mth_day_avg_bal numeric(24,2),
   yr_day_avg_bal numeric(24,2),
   sys_src varchar(50)
)
partition by (((date_part('year', f_agt_lc.etl_date) * 100) + date_part('month', f_agt_lc.etl_date)));

comment on table f_fdm.f_agt_lc is '信用证信息表';


create table f_fdm.f_acct_fund_agn
(
   etl_date date not null,
   grp_typ numeric(1,0),
   agmt_id varchar(80),
   prod_cd varchar(50),
   biz_typ_cd varchar(50),
   entrs_corp_cd varchar(80),
   dpst_acct varchar(80),
   cust_id varchar(80),
   org_num varchar(50),
   cur_cd varchar(50),
   open_acct_dt date,
   subj_cd varchar(50),
   fund_lot numeric(24,2),
   fund_net_worth numeric(24,2),
   fund_amt numeric(24,2),
   cust_mgr_id varchar(80),
   mth_accm numeric(24,2),
   yr_accm numeric(24,2),
   mth_day_avg_bal numeric(24,2),
   yr_day_avg_bal numeric(24,2),
   sys_src varchar(50)
)
partition by (((date_part('year', f_acct_fund_agn.etl_date) * 100) + date_part('month', f_acct_fund_agn.etl_date)));


create table f_fdm.f_acct_prec_metal
(
   etl_date date not null,
   grp_typ numeric(1,0),
   agmt_id varchar(80),
   prod_cd varchar(50),
   dpst_acct_num varchar(80),
   cust_id varchar(80),
   org_num varchar(80),
   cur_cd varchar(50),
   open_acct_dt date,
   prin_subj varchar(80),
   prec_metal_lot numeric(24,2),
   curr_mkt_val numeric(24,2),
   prec_metal_amt numeric(24,2),
   acct_bal numeric(24,2),
   cust_mgr_id varchar(80),
   mth_accm numeric(24,2),
   yr_accm numeric(24,2),
   mth_day_avg_bal numeric(24,2),
   yr_day_avg_bal numeric(24,2),
   sys_src varchar(50)
)
partition by (((date_part('year', f_acct_prec_metal.etl_date) * 100) + date_part('month', f_acct_prec_metal.etl_date)));


create table f_fdm.f_loan_indv_dubil
(
   etl_date date not null,
   grp_typ numeric(1,0),
   agmt_id varchar(80),
   cust_num varchar(80),
   org_num varchar(80),
   cur_cd varchar(50),
   prod_cd varchar(50),
   distr_dt date,
   st_int_dt date,
   due_dt date,
   payoff_dt date,
   wrtoff_dt date,
   loan_orgnl_amt numeric(24,2),
   exec_int_rate numeric(12,6),
   bmk_int_rate numeric(12,6),
   flt_ratio numeric(12,6),
   basis numeric(12,6),
   ovrd_int_rate numeric(12,6),
   int_base_cd varchar(50),
   cmpd_int_calc_mode_cd varchar(50),
   pre_chrg_int varchar(50),
   int_rate_attr_cd varchar(50),
   int_rate_adj_mode_cd varchar(50),
   repay_mode_cd varchar(50),
   repay_prd_cd varchar(50),
   orgnl_term numeric(24,0),
   orgnl_term_corp_cd varchar(50),
   rprc_prd numeric(24,0),
   rprc_prd_corp_cd varchar(50),
   last_rprc_day date,
   next_rprc_day date,
   next_pay_amt numeric(24,2),
   last_pay_day date,
   next_pay_day date,
   four_cls_cls varchar(50),
   fiv_cls varchar(50),
   agmt_stat_cd varchar(50),
   contr_agmt_id varchar(80),
   asst_secuz_ind varchar(2),
   prin_subj varchar(80),
   curr_bal numeric(24,2),
   norm_bal numeric(24,2),
   slug_bal numeric(24,2),
   bad_debt_bal numeric(24,2),
   wrtoff_prin numeric(24,2),
   int_subj varchar(80),
   today_provs_int numeric(24,2),
   curmth_provs_int numeric(24,2),
   accm_provs_int numeric(24,2),
   today_chrg_int numeric(24,2),
   curmth_recvd_int numeric(24,2),
   accm_recvd_int numeric(24,2),
   int_adj_amt numeric(24,2),
   mth_accm numeric(24,2),
   yr_accm numeric(24,2),
   mth_day_avg_bal numeric(24,2),
   yr_day_avg_bal numeric(24,2),
   opr_org_num varchar(80),
   opr_tellr_num varchar(80),
   free_int_ind varchar(2),
   free_int_prd numeric(24,0),
   expd_ind varchar(50),
   expd_due_dt date,
   int_rate_typ_cd varchar(50),
   loan_typ varchar(50),
   is_loan_sbsd_ind varchar(1),
   is_farm_ind varchar(2),
   is_spec_loan varchar(2),
   is_acrd_fin_rvn_farm_std varchar(50),
   is_setup_inds_loan varchar(2),
   spec_biz_typ varchar(50),
   ovrd_days numeric(24,0),
   ovrd_prin numeric(24,2),
   ovrd_int numeric(24,2),
   adv_money_ind varchar(2),
   adv_money_amt numeric(24,2),
   adv_money_bal numeric(24,2),
   loan_deval_prep_bal numeric(24,2),
   loan_deval_prep_amt numeric(24,2),
   sys_src varchar(50)
)
partition by (((date_part('year', f_loan_indv_dubil.etl_date) * 100) + date_part('month', f_loan_indv_dubil.etl_date)));

comment on table f_fdm.f_loan_indv_dubil is '个人贷款借据信息表';


create table f_fdm.f_cust_indv
(
   etl_date date not null,
   grp_typ numeric(1,0),
   cust_num varchar(80),
   cust_nm varchar(100),
   nation_cd varchar(50),
   gender_cd varchar(50),
   birth_dt date,
   carr_cd varchar(50),
   marrg_stat_cd varchar(50),
   open_acct_org_num varchar(80),
   belg_org_num varchar(80),
   open_acct_dt date,
   cust_typ_cd varchar(50),
   cust_lvl_cd varchar(50),
   crdt_card_cust_lvl_cd varchar(50),
   is_vip varchar(30),
   cust_hi_card_hold_lvl_cd varchar(50),
   is_pros_vip_ind varchar(30),
   fin_asst_totl_amt numeric(24,2),
   emp_ind varchar(30),
   cust_stat_cd varchar(50),
   cust_mgr_id varchar(80),
   cust_div_grp varchar(50),
   cert_typ_cd varchar(50),
   cert_num varchar(80),
   sys_src varchar(50)
)
partition by (((date_part('year', f_cust_indv.etl_date) * 100) + date_part('month', f_cust_indv.etl_date)));

comment on table f_fdm.f_cust_indv is '个人客户基本信息表';


create table f_fdm.f_agt_bill_info
(
   etl_date date not null,
   grp_typ numeric(1,0),
   flow_id varchar(80),
   bill_num varchar(80),
   par_amt numeric(24,2),
   draw_day date,
   due_dt date,
   drawr_cust_id varchar(80),
   draw_cust_nm varchar(300),
   draw_line_id varchar(80),
   acptr_cust_id varchar(300),
   acpt_line_cust_id varchar(80),
   bill_attr_cd varchar(50),
   bill_typ_cd varchar(50),
   cur_cd varchar(50),
   bill_src_cd varchar(50),
   recvr_cust_id varchar(80),
   bill_stat_cd varchar(50),
   sys_src varchar(50)
)
partition by (((date_part('year', f_agt_bill_info.etl_date) * 100) + date_part('month', f_agt_bill_info.etl_date)));

comment on table f_fdm.f_agt_bill_info is '票据信息表';



create table f_fdm.f_agt_asst_consv
(
   etl_date date not null,
   grp_typ numeric(1,0),
   asst_consv_id varchar(80),
   orgnl_agmt_id varchar(80),
   cust_id varchar(80),
   org_id varchar(80),
   wrtoff_dt date,
   wrtoff_prin numeric(24,2),
   wrtoff_in_bal_int numeric(24,2),
   wrtoff_norm_int numeric(24,2),
   wrtoff_arr_int numeric(24,2),
   wrtoff_pnsh_int numeric(24,2),
   wrtoff_post_retr_dt date,
   wrtoff_post_retr_prin numeric(24,2),
   wrtoff_post_retr_int numeric(24,2),
   wrtoff_post_retr_accrd_int numeric(24,2),
   sys_src varchar(50)
)
partition by (((date_part('year', f_agt_asst_consv.etl_date) * 100) + date_part('month', f_agt_asst_consv.etl_date)));


create table f_fdm.f_fnc_subj_info
(
   etl_date date not null,
   grp_typ numeric(1,0),
   subj_cd varchar(80),
   subj_nm varchar(100),
   subj_hrcy varchar(50),
   subj_cate varchar(50),
   subj_stat varchar(50),
   dtl_subj_ind varchar(1),
   sys_src varchar(50)
)
partition by (((date_part('year', f_fnc_subj_info.etl_date) * 100) + date_part('month', f_fnc_subj_info.etl_date)));

comment on table f_fdm.f_fnc_subj_info is '科目信息表';


create table f_fdm.f_fnc_gl_info
(
   etl_date date not null,
   grp_typ numeric(1,0),
   org_cd varchar(50),
   cur_cd varchar(50),
   duty_ctr_cd varchar(50),
   account_cd varchar(50),
   dtl_subj_cd varchar(50),
   prod_cd varchar(50),
   biz_line_cd varchar(50),
   inn_reco_cd varchar(50),
   business_unit_cd varchar(50),
   spare_cd varchar(50),
   today_debit_amt numeric(24,2),
   today_crdt_amt numeric(24,2),
   curmth_accm_debit_amt numeric(24,2),
   curmth_accm_crdt_amt numeric(24,2),
   debit_bal numeric(24,2),
   crdt_bal numeric(24,2),
   curr_debit_avg_bal numeric(24,2),
   curr_crdt_avg_bal numeric(24,2),
   term_ind numeric(24,2),
   sys_src varchar(50)
)
partition by (((date_part('year', f_fnc_gl_info.etl_date) * 100) + date_part('month', f_fnc_gl_info.etl_date)));

comment on table f_fdm.f_fnc_gl_info is '总账信息表';


create table f_fdm.f_acct_crdt_amtbl
(
   etl_date date not null,
   grp_typ numeric(1,0),
   agmt_id varchar(80),
   amtbl_pay_ordr_num numeric(24,0),
   card_num varchar(80),
   cust_num varchar(80),
   cur_cd varchar(50),
   crdt_card_acct_typ_cd varchar(50),
   prin_subj varchar(80),
   int_subj varchar(80),
   comm_fee_subj varchar(80),
   amtbl_pay_stat_cd varchar(50),
   amtbl_pay_typ_cd varchar(50),
   amtbl_amt numeric(24,2),
   totl_amtbl_mths numeric(24,0),
   amtbl_int numeric(24,2),
   amtbl_pay_comm_fee numeric(24,2),
   st_int_dt date,
   amtbl_pay_int_rate numeric(12,6),
   amtbl_pay_fee_rate numeric(12,6),
   remn_un_ret_prin numeric(24,2),
   remn_un_ret_fee numeric(24,2),
   remn_un_ret_int numeric(24,2),
   amtbl_pay_prd_cnt numeric(24,0),
   amtbl_dt date,
   sys_src varchar(50)
)
partition by (((date_part('year', f_acct_crdt_amtbl.etl_date) * 100) + date_part('month', f_acct_crdt_amtbl.etl_date)));

comment on table f_fdm.f_acct_crdt_amtbl is '信用卡账户分期信息表';


create table f_fdm.f_evt_tx_info
(
   etl_date date not null,
   grp_typ numeric(1,0),
   tx_dt date,
   acct_num varchar(80),
   org_num varchar(80),
   tx_typ_cd varchar(50),
   tx_chnl_cd varchar(50),
   cur_cd varchar(50),
   tx_amt numeric(24,2),
   tx_cnt numeric(24,0),
   sys_src varchar(50)
)
partition by (((date_part('year', f_evt_tx_info.etl_date) * 100) + date_part('month', f_evt_tx_info.etl_date)));

comment on table f_fdm.f_evt_tx_info is '交易信息表';


create table f_fdm.f_acct_crdt_info
(
   etl_date date not null,
   grp_typ numeric(1,0),
   agmt_id varchar(80),
   card_num varchar(80),
   cust_id varchar(80),
   open_acct_org_num varchar(80),
   prod_cd varchar(50),
   stmt_day numeric(24,0),
   open_acct_dt date,
   crdt_lmt numeric(24,2),
   repay_acct_num_1 varchar(80),
   repay_acct_num_2 varchar(80),
   repay_acct_num_3 varchar(80),
   repay_acct_num_4 varchar(80),
   repay_day date,
   pre_brw_cash_ratio numeric(12,6),
   amtbl_pay_lmt numeric(24,2),
   org_num varchar(80),
   cur_cd varchar(50),
   curr_int_rate numeric(12,6),
   int_base_cd varchar(50),
   cmpd_int_calc_mode_cd varchar(50),
   acct_stat_cd varchar(50),
   prin_subj_od varchar(80),
   prin_subj_dpst varchar(80),
   int_subj varchar(80),
   curr_bal numeric(24,2),
   ovrd_stat varchar(50),
   ovrd_dt date,
   tranfm_od_dt date,
   remn_ovrd_prin numeric(24,2),
   remn_ovrd_int numeric(24,2),
   spl_pay_bal numeric(24,2),
   wrtoff_ind varchar(30),
   wrtoff_dt date,
   wrtoff_prin numeric(24,2),
   wrtoff_comm_fee numeric(24,2),
   wrtoff_late_chrg numeric(24,2),
   wrtoff_off_bal_int numeric(24,2),
   wrtoff_in_bal_int numeric(24,2),
   today_provs_int numeric(24,2),
   curmth_provs_int numeric(24,2),
   accm_provs_int numeric(24,2),
   today_chrg_int numeric(24,2),
   curmth_recvd_int numeric(24,2),
   accm_recvd_int numeric(24,2),
   mth_accm numeric(24,2),
   yr_accm numeric(24,2),
   int_adj_amt numeric(24,2),
   loan_deval_prep_bal numeric(24,2),
   loan_deval_prep_amt numeric(24,2),
   mth_day_avg_bal numeric(24,2),
   yr_day_avg_bal numeric(24,2),
   sys_src varchar(50)
)
partition by (((date_part('year', f_acct_crdt_info.etl_date) * 100) + date_part('month', f_acct_crdt_info.etl_date)));

comment on table f_fdm.f_acct_crdt_info is '信用卡账户信息表';


create table f_fdm.f_agt_guarantee
(
   etl_date date,
   grp_typ numeric(1,0),
   agmt_id varchar(80),
   cust_num varchar(80),
   org_num varchar(80),
   cur_cd varchar(50),
   prod_cd varchar(50),
   loan_contr_id varchar(80),
   open_dt date,
   due_dt date,
   subj_cd varchar(80),
   guar_amt numeric(24,2),
   guar_bal numeric(24,2),
   agmt_stat_cd varchar(50),
   guar_mode_cd varchar(50),
   guar_typ_cd varchar(50),
   guar_charc_cd varchar(50),
   margn_ratio numeric(12,6),
   margn_acct_num varchar(100),
   margn_amt numeric(24,2),
   fill_tab_dept varchar(300),
   mth_accm numeric(24,2),
   yr_accm numeric(24,2),
   mth_day_avg_bal numeric(24,2),
   yr_day_avg_bal numeric(24,2),
   sys_src varchar(50)
);


create table f_fdm.f_evt_fxstl_fex
(
   etl_date date,
   grp_typ numeric(1,0),
   tx_dt date,
   ent_acct_acct_num varchar(80),
   out_acct_acct_num varchar(80),
   tx_org varchar(80),
   cust_num varchar(80),
   prod_cd varchar(50),
   tx_seq_num varchar(80),
   tx_typ varchar(80),
   buy_cur varchar(80),
   buy_amt numeric(24,2),
   sell_cur varchar(80),
   sell_amt numeric(24,2),
   exchg_rate numeric(12,6),
   buy_amt_to_rmb numeric(24,2),
   sell_amt_to_rmb numeric(24,2),
   biz_prc_diff numeric(24,2),
   sight_fwd_ind varchar(30),
   tx_chnl_cd varchar(50),
   sys_src varchar(50)
);


create table f_fdm.f_evt_comfee_commsn
(
   etl_date date,
   grp_typ numeric(1,0),
   tx_dt date,
   acct_num varchar(80),
   org_num varchar(80),
   cur_cd varchar(50),
   chrg_mode varchar(50),
   tx_chnl varchar(50),
   cust_num varchar(80),
   chrg_proj varchar(50),
   comm_fee_subj varchar(80),
   comm_fee_amt numeric(24,2),
   tx_cnt numeric(24,0),
   sys_src varchar(50)
);


create table f_fdm.f_org_info
(
   etl_date date not null,
   grp_typ numeric(1,0),
   org_cd varchar(50),
   org_nm varchar(300),
   org_hrcy varchar(50),
   org_sht_nm varchar(300),
   admin_lvl varchar(50),
   org_typ varchar(50),
   org_func varchar(100),
   org_attr varchar(50),
   org_stat_cd varchar(50),
   regn_attr varchar(50),
   admin_div_cd varchar(50),
   org_cnt numeric(24,0),
   pers_cnt numeric(24,0),
   up_org_cd varchar(80),
   up_org_nm varchar(300),
   is_dtl_org varchar(2),
   st_use_dt date,
   invldtn_dt date,
   econ_regn_attr_cd varchar(50),
   is_nt_indpd varchar(30),
   brch_biz_area numeric(24,0),
   post_biz_area numeric(24,0),
   biz_area_prop varchar(30),
   offi_area_prop varchar(30),
   offi_area numeric(24,0),
   func_prtn varchar(100),
   leas_matr_tm varchar(30),
   seat_cnt numeric(1,0),
   zip_cd varchar(80),
   addr varchar(300),
   sys_src varchar(50)
)
partition by (((date_part('year', f_org_info.etl_date) * 100) + date_part('month', f_org_info.etl_date)));

create table f_rdm.f_rpt_item_and_rpt_rel
(
    rpt_id varchar(80) not null,
    rpt_item_ordr_num numeric(24,0),
    rpt_item_encd varchar(80),
    rpt_item_nm varchar(100),
    show_typ varchar(2),
    show_up varchar(2)
);

comment on table f_rdm.f_rpt_item_and_rpt_rel is '报表项与报表关系表';


create table f_rdm.f_hq_and_line_leadr_data_model
(
    etl_dt varchar(8) not null,
    rpt_item_encd varchar(80),
    dumy_org_cd varchar(80),
    cur_cd varchar(50),
    freq varchar(1),
    curr_val numeric(24,2),
    pre_prd_val numeric(24,2),
    same_prd_val numeric(24,2),
    y_accm_val numeric(24,2)
);

comment on table f_rdm.f_hq_and_line_leadr_data_model is '总行及报行领导数据模型表';

create table f_rdm.f_ibank_data_model
(
    etl_dt varchar(8) not null,
    rpt_item_encd varchar(80),
    ibank_cd varchar(50),
    cur_cd varchar(50),
    freq varchar(1),
    curr_val numeric(24,2),
    pre_prd_val numeric(24,2),
    same_prd_val numeric(24,2),
    y_accm_val numeric(24,2)
);

comment on table f_rdm.f_ibank_data_model is '同业数据模型表';

create table f_rdm.f_indx_qry_data_model_tab
(
    etl_dt varchar(8) not null,
    org_cd varchar(80),
    indx_item_id varchar(80),
    cur_cd varchar(50),
    indx_item_nm varchar(100),
    org_nm varchar(100),
    up_org_cd varchar(80),
    up_org_nm varchar(100),
    curr_val numeric(24,2)
);

comment on table f_rdm.f_indx_qry_data_model_tab is '机构维度指标信息查询报表数据模型表';


create table f_rdm.f_comb_dim_indx_qry_data_model
(
    etl_dt varchar(8) not null,
    org_cd varchar(80),
    org_nm varchar(300),
    up_org_cd varchar(50),
    up_org_nm varchar(300),
    fst_lvl_prod_cd varchar(50),
    sec_prod_cd varchar(50),
    thd_cls_prod_cd varchar(50),
    four_cls_prod_cd varchar(50),
    end_lvl_prod_cd varchar(50),
    fst_lvl_line_cd varchar(50),
    sec_line_cd varchar(50),
    cur_cd varchar(50),
    fst_lvl_prod_nm varchar(300),
    sec_prod_nm varchar(300),
    thd_cls_prod_nm varchar(300),
    four_cls_prod_nm varchar(300),
    end_lvl_prod_nm varchar(300),
    fst_lvl_line_nm varchar(300),
    sec_line_nm varchar(300),
    yield_asst_tm_point_bal numeric(24,2),
    int_pay_liab_tm_point_bal numeric(24,2),
    biz_incom numeric(24,2),
    non_biz_net_incom numeric(24,2),
    int_net_incom numeric(24,2),
    int_incom numeric(24,2),
    int_expns numeric(24,2),
    non_int_expns numeric(24,2),
    non_int_net_incom numeric(24,2),
    non_int_incom numeric(24,2),
    comm_fee_and_commsn_net_incom numeric(24,2),
    comm_fee_and_commsn_incom numeric(24,2),
    comm_fee_and_commsn_expns numeric(24,2),
    inves_prft numeric(24,2),
    val_chg_prft_loss numeric(24,2),
    exchg_prft_loss numeric(24,2),
    oth_biz_incom numeric(24,2),
    non_biz_incom numeric(24,2),
    biz_cost numeric(24,2),
    biz_drct_cost numeric(24,2),
    biz_indrct_cost numeric(24,2),
    biz_tax_and_addt numeric(24,2),
    biz_and_mgmt_drct_cost numeric(24,2),
    biz_and_mgmt_indrct_cost numeric(24,2),
    drct_cost numeric(24,2),
    manu_cost numeric(24,2),
    mkt_dev_fee numeric(24,2),
    org_mov_fee numeric(24,2),
    deprt_and_amtbl_fee numeric(24,2),
    tax_insu_and_cost numeric(24,2),
    majr_serv_fee numeric(24,2),
    oth_fee numeric(24,2),
    indrct_cost numeric(24,2),
    post_agent_cost numeric(24,2),
    asst_deval_loss numeric(24,2),
    oth_biz_cost numeric(24,2),
    non_biz_expns numeric(24,2),
    incom_tax_fee numeric(24,2),
    biz_margn numeric(24,2),
    biz_margn_drct_cost numeric(24,2),
    biz_margn_indrct_cost numeric(24,2),
    margn_totl_amt numeric(24,2),
    margn_totl_amt_drct_cost numeric(24,2),
    margn_totl_amt_indrct_cost numeric(24,2),
    net_margn numeric(24,2),
    net_margn_drct_cost numeric(24,2),
    net_margn_indrct_cost numeric(24,2),
    econ_cap numeric(24,2),
    crdt_risk_econ_cap numeric(24,2),
    opr_risk_econ_cap numeric(24,2),
    mkt_risk_econ_cap numeric(24,2),
    econ_cap_lmt numeric(24,2),
    risk_wght_asst numeric(24,2),
    yield_asst_extnl_int_incom numeric(24,2),
    yield_asst_ftp_int_expns numeric(24,2),
    yield_asst_day_avg_bal numeric(24,2),
    int_pay_liab_ftp_int_incom numeric(24,2),
    int_pay_liab_extnl_int_expns numeric(24,2),
    int_pay_liab_day_avg_bal numeric(24,2),
    econ_add_val_drct_cost numeric(24,2),
    econ_add_val_indrct_cost numeric(24,2),
    econ_cap_cost numeric(24,2),
    biz_and_mgmt_cost numeric(24,2),
    econ_cap_ocup numeric(24,2)
);

comment on table f_rdm.f_comb_dim_indx_qry_data_model is '组合维度指标查询数据模型';



--create schema f_rdm default include schema privileges;


create table f_rdm.ma_org_info
(
   etl_date date not null,
   org_cd varchar(50),
   org_nm varchar(300),
   org_hrcy varchar(50),
   org_sht_nm varchar(300),
   admin_lvl varchar(50),
   org_func varchar(100),
   org_attr varchar(50),
   org_stat_cd varchar(50),
   admin_div_cd varchar(50),
   econ_regn_attr_cd varchar(50),
   regn_attr varchar(50),
   org_cnt numeric(24,0),
   pers_cnt numeric(1,0),
   up_org_cd varchar(80),
   up_org_nm varchar(300),
   is_dtl_org varchar(1),
   st_use_dt date,
   invldtn_dt date,
   area numeric(24,2),
   seat_cnt numeric(1,0)
)
partition by (((date_part('year', ma_org_info.etl_date) * 100) + date_part('month', ma_org_info.etl_date)));

comment on table f_rdm.ma_org_info is '机构';


create table f_rdm.ma_cust_indv
(
   etl_date date not null,
   cust_num varchar(80),
   cust_nm varchar(100),
   nation_cd varchar(50),
   gender varchar(50),
   birth_dt date,
   carr_cd varchar(50),
   marrg_stat_cd varchar(50),
   belg_org_num varchar(80),
   open_acct_dt date,
   cust_typ_cd varchar(50),
   cust_lvl_cd varchar(50),
   crdt_card_cust_lvl_cd varchar(50),
   is_vip varchar(30),
   aum numeric(24,2),
   cust_div_grp varchar(50)
)
partition by (((date_part('year', ma_cust_indv.etl_date) * 100) + date_part('month', ma_cust_indv.etl_date)));

comment on table f_rdm.ma_cust_indv is '个人客户';


create table f_rdm.ma_cust_ibank
(
   etl_date date not null,
   cust_num varchar(80),
   ibank_cust_legl_nm varchar(300),
   is_crdt_cust_ind varchar(30),
   is_lpr_ind varchar(30),
   cust_typ_cd varchar(50),
   fin_lics_id varchar(80),
   org_org_cd varchar(80),
   inds_typ_cd varchar(50)
)
partition by (((date_part('year', ma_cust_ibank.etl_date) * 100) + date_part('month', ma_cust_ibank.etl_date)));

comment on table f_rdm.ma_cust_ibank is '同业客户';


create table f_rdm.ma_lpr_od
(
   etl_date date not null,
   lpr_od_acct_num varchar(80),
   org_num varchar(80),
   cust_num varchar(80),
   cur_cd varchar(50),
   prod_cd varchar(50),
   st_int_dt date,
   due_dt date,
   curr_int_rate numeric(10,6),
   bmk_int_rate numeric(10,6),
   basic_diff numeric(10,6),
   int_days varchar(50),
   cmpd_int_calc_mode_cd varchar(50),
   int_mode_cd varchar(50),
   int_rate_attr_cd varchar(50),
   orgnl_term varchar(8),
   orgnl_term_corp_cd varchar(50),
   rprc_prd numeric(15,0),
   rprc_prd_corp_cd varchar(50),
   last_rprc_day date,
   next_rprc_day date,
   sys_src varchar(50),
   acct_stat_cd varchar(50),
   prin_subj varchar(80),
   curr_bal numeric(24,2),
   int_subj varchar(80),
   today_provs_int numeric(24,2),
   curmth_provs_int numeric(24,2),
   accm_provs_int numeric(24,2),
   today_chrg_int numeric(24,2),
   curmth_recvd_int numeric(24,2),
   accm_recvd_int numeric(24,2),
   int_adj_amt numeric(24,2),
   mth_day_avg_bal numeric(24,2),
   yr_day_avg_bal numeric(24,2),
   ftp_price numeric(24,2),
   ftp_tranfm_expns numeric(24,2),
   prosp_loss numeric(24,2),
   deval_prep_bal numeric(24,2),
   deval_prep_amt numeric(24,2),
   crdt_risk_econ_cap numeric(24,2),
   mkt_risk_econ_cap numeric(24,2),
   opr_risk_econ_cap numeric(24,2),
   crdt_risk_econ_cap_cost numeric(24,2),
   mkt_risk_econ_cap_cost numeric(24,2),
   opr_risk_econ_cap_cost numeric(24,2)
)
partition by (((date_part('year', ma_lpr_od.etl_date) * 100) + date_part('month', ma_lpr_od.etl_date)));

comment on table f_rdm.ma_lpr_od is '法人透支';


create table f_rdm.ma_loan
(
   etl_date date not null,
   loan_acct varchar(80),
   org_num varchar(80),
   cust_num varchar(80),
   cur_cd varchar(50),
   prod_cd varchar(50),
   st_int_dt date,
   due_dt date,
   ori_int_rate numeric(10,6),
   curr_int_rate numeric(10,6),
   bmk_int_rate numeric(10,6),
   basic_diff numeric(10,6),
   ovrd_int_rate numeric(10,6),
   loan_orgnl_amt numeric(24,2),
   int_days varchar(50),
   cmpd_int_calc_mode_cd varchar(50),
   int_mode_cd varchar(50),
   int_rate_attr_cd varchar(50),
   repay_mode varchar(50),
   repay_prd varchar(50),
   repay_prd_corp varchar(1),
   next_pay_amt numeric(24,2),
   last_pay_day date,
   next_pay_day date,
   orgnl_term varchar(8),
   orgnl_term_corp_cd varchar(50),
   rprc_prd numeric(15,0),
   rprc_prd_corp_cd varchar(50),
   last_rprc_day date,
   next_rprc_day date,
   loan_drct varchar(50),
   fiv_cls varchar(50),
   loan_stat varchar(50),
   sys_src varchar(50),
   lvrg_flg varchar(50),
   is_repay_plan varchar(1),
   loan_deval_prep_bal numeric(24,2),
   loan_deval_prep_amt numeric(24,2),
   acct_stat_cd varchar(50),
   prin_subj varchar(80),
   curr_bal numeric(24,2),
   int_subj varchar(80),
   today_provs_int numeric(24,2),
   curmth_provs_int numeric(24,2),
   accm_provs_int numeric(24,2),
   today_chrg_int numeric(24,2),
   curmth_recvd_int numeric(24,2),
   accm_recvd_int numeric(24,2),
   int_adj_amt numeric(24,2),
   mth_day_avg_bal numeric(24,2),
   yr_day_avg_bal numeric(24,2),
   ftp_price numeric(24,2),
   ftp_tranfm_expns numeric(24,2),
   prosp_loss numeric(24,2),
   crdt_risk_econ_cap numeric(24,2),
   mkt_risk_econ_cap numeric(24,2),
   opr_risk_econ_cap numeric(24,2),
   crdt_risk_econ_cap_cost numeric(24,2),
   mkt_risk_econ_cap_cost numeric(24,2),
   opr_risk_econ_cap_cost numeric(24,2)
)
partition by (((date_part('year', ma_loan.etl_date) * 100) + date_part('month', ma_loan.etl_date)));

comment on table f_rdm.ma_loan is '贷款';


create table f_rdm.ma_bill_discount
(
   etl_date date not null,
   discnt_acct_num varchar(80),
   bill_num varchar(80),
   org_num varchar(80),
   cust_num varchar(80),
   cur_cd varchar(50),
   prod_cd varchar(50),
   st_int_dt date,
   due_dt date,
   curr_int_rate numeric(10,6),
   int_days varchar(50),
   cmpd_int_calc_mode_cd varchar(50),
   int_rate_attr_cd varchar(50),
   repay_mode varchar(50),
   repay_prd varchar(50),
   repay_prd_corp varchar(1),
   next_pay_amt numeric(24,2),
   last_pay_day date,
   next_pay_day date,
   orgnl_term varchar(8),
   orgnl_term_corp_cd varchar(50),
   rprc_prd numeric(15,0),
   rprc_prd_corp_cd varchar(50),
   last_rprc_day date,
   next_rprc_day date,
   fiv_cls varchar(50),
   loan_stat varchar(50),
   sys_src varchar(50),
   loan_deval_prep_bal numeric(24,2),
   loan_deval_prep_amt numeric(24,2),
   acct_stat_cd varchar(50),
   par_subj varchar(50),
   par_amt numeric(24,2),
   int_adj_subj varchar(80),
   int_adj_amt numeric(24,2),
   int_subj varchar(80),
   curmth_amtbl_int numeric(24,2),
   mth_day_avg_bal numeric(24,2),
   ftp_price numeric(24,2),
   ftp_tranfm_expns numeric(24,2),
   prosp_loss numeric(24,2),
   crdt_risk_econ_cap numeric(24,2),
   mkt_risk_econ_cap numeric(24,2),
   opr_risk_econ_cap numeric(24,2),
   crdt_risk_econ_cap_cost numeric(24,2),
   mkt_risk_econ_cap_cost numeric(24,2),
   opr_risk_econ_cap_cost numeric(24,2)
)
partition by (((date_part('year', ma_bill_discount.etl_date) * 100) + date_part('month', ma_bill_discount.etl_date)));

comment on table f_rdm.ma_bill_discount is '票据贴现';


create table f_rdm.ma_pstv_buy_back
(
   etl_date date not null,
   buy_back_id varchar(80),
   org_num varchar(80),
   tx_comb varchar(80),
   cust_num varchar(80),
   acct_typ varchar(50),
   cur_cd varchar(50),
   prod_cd varchar(50),
   st_int_dt date,
   due_dt date,
   curr_int_rate numeric(10,6),
   bmk_int_rate numeric(10,6),
   basic_diff numeric(10,6),
   int_days varchar(50),
   cmpd_int_calc_mode_cd varchar(50),
   int_rate_attr_cd varchar(50),
   orgnl_term varchar(8),
   orgnl_term_corp_cd varchar(50),
   rprc_prd numeric(15,0),
   rprc_prd_corp_cd varchar(50),
   last_rprc_day date,
   next_rprc_day date,
   sys_src varchar(50),
   prin_subj varchar(80),
   curr_bal numeric(24,2),
   int_subj varchar(80),
   today_provs_int numeric(24,2),
   curmth_provs_int numeric(24,2),
   accm_provs_int numeric(24,2),
   today_int_pay numeric(24,2),
   curmth_paid_int numeric(24,2),
   accm_paid_int numeric(24,2),
   int_adj_amt numeric(24,2),
   mth_day_avg_bal numeric(24,2),
   yr_day_avg_bal numeric(24,2),
   ftp_price numeric(24,2),
   ftp_tranfm_incom numeric(24,2),
   exchg_prft_loss_subj varchar(80),
   fair_val_chg_subj varchar(80)
)
partition by (((date_part('year', ma_pstv_buy_back.etl_date) * 100) + date_part('month', ma_pstv_buy_back.etl_date)));

comment on table f_rdm.ma_pstv_buy_back is '资金业务-正回购';

 

create table f_rdm.ma_borrowing
(
   etl_date date not null,
   ibank_offer_id varchar(80),
   org_num varchar(80),
   tx_comb varchar(80),
   cust_num varchar(80),
   acct_typ varchar(50),
   cur_cd varchar(50),
   prod_cd varchar(50),
   st_int_dt date,
   due_dt date,
   curr_int_rate numeric(10,6),
   bmk_int_rate numeric(10,6),
   basic_diff numeric(10,6),
   int_days varchar(50),
   cmpd_int_calc_mode_cd varchar(50),
   int_rate_attr_cd varchar(50),
   orgnl_term varchar(8),
   orgnl_term_corp_cd varchar(50),
   rprc_prd numeric(15,0),
   rprc_prd_corp_cd varchar(50),
   last_rprc_day date,
   next_rprc_day date,
   sys_src varchar(50),
   prin_subj varchar(80),
   curr_bal numeric(24,2),
   int_subj varchar(80),
   today_provs_int numeric(24,2),
   curmth_provs_int numeric(24,2),
   accm_provs_int numeric(24,2),
   today_int_pay numeric(24,2),
   curmth_paid_int numeric(24,2),
   accm_paid_int numeric(24,2),
   int_adj_amt numeric(24,2),
   mth_day_avg_bal numeric(24,2),
   yr_day_avg_bal numeric(24,2),
   ftp_price numeric(24,2),
   ftp_tranfm_incom numeric(24,2),
   prosp_loss numeric(24,2),
   deval_prep_bal numeric(24,2),
   deval_prep_amt numeric(24,2),
   crdt_risk_econ_cap numeric(24,2),
   mkt_risk_econ_cap numeric(24,2),
   opr_risk_econ_cap numeric(24,2),
   crdt_risk_econ_cap_cost numeric(24,2),
   mkt_risk_econ_cap_cost numeric(24,2),
   opr_risk_econ_cap_cost numeric(24,2),
   exchg_prft_loss_subj varchar(80),
   fair_val_chg_subj varchar(80)
)
partition by (((date_part('year', ma_borrowing.etl_date) * 100) + date_part('month', ma_borrowing.etl_date)));

comment on table f_rdm.ma_borrowing is '资金业务-拆入';


create table f_rdm.ma_lending
(
   etl_date date not null,
   ibank_offer_id varchar(80),
   org_num varchar(80),
   tx_comb varchar(80),
   cust_num varchar(80),
   acct_typ varchar(50),
   cur_cd varchar(50),
   prod_cd varchar(50),
   st_int_dt date,
   due_dt date,
   curr_int_rate numeric(10,6),
   bmk_int_rate numeric(10,6),
   basic_diff numeric(10,6),
   int_days varchar(50),
   cmpd_int_calc_mode_cd varchar(50),
   int_rate_attr_cd varchar(50),
   orgnl_term varchar(8),
   orgnl_term_corp_cd varchar(50),
   rprc_prd numeric(15,0),
   rprc_prd_corp_cd varchar(50),
   last_rprc_day date,
   next_rprc_day date,
   sys_src varchar(50),
   prin_subj varchar(80),
   curr_bal numeric(24,2),
   int_subj varchar(80),
   today_provs_int numeric(24,2),
   curmth_provs_int numeric(24,2),
   accm_provs_int numeric(24,2),
   today_chrg_int numeric(24,2),
   curmth_recvd_int numeric(24,2),
   accm_recvd_int numeric(24,2),
   int_adj_amt numeric(24,2),
   mth_day_avg_bal numeric(24,2),
   yr_day_avg_bal numeric(24,2),
   ftp_price numeric(24,2),
   ftp_tranfm_expns numeric(24,2),
   prosp_loss numeric(24,2),
   deval_prep_bal numeric(24,2),
   deval_prep_amt numeric(24,2),
   crdt_risk_econ_cap numeric(24,2),
   mkt_risk_econ_cap numeric(24,2),
   opr_risk_econ_cap numeric(24,2),
   crdt_risk_econ_cap_cost numeric(24,2),
   mkt_risk_econ_cap_cost numeric(24,2),
   opr_risk_econ_cap_cost numeric(24,2),
   exchg_prft_loss_subj varchar(80),
   fair_val_chg_subj varchar(80)
)
partition by (((date_part('year', ma_lending.etl_date) * 100) + date_part('month', ma_lending.etl_date)));

comment on table f_rdm.ma_lending is '资金业务-拆出';


create table f_rdm.ma_acptn
(
   etl_date date not null,
   biz_id varchar(80),
   org_num varchar(80),
   cust_num varchar(80),
   cur_cd varchar(50),
   prod_cd varchar(50),
   st_int_dt date,
   due_dt date,
   fiv_cls varchar(50),
   sys_src varchar(50),
   loan_deval_prep_bal numeric(24,2),
   loan_deval_prep_amt numeric(24,2),
   acct_stat varchar(50),
   prin_subj varchar(80),
   acptn_amt numeric(24,2),
   acptn_bal numeric(24,2),
   mth_day_avg_bal numeric(24,2),
   yr_day_avg_bal numeric(24,2),
   prosp_loss numeric(24,2),
   crdt_risk_econ_cap numeric(24,2),
   mkt_risk_econ_cap numeric(24,2),
   opr_risk_econ_cap numeric(24,2),
   crdt_risk_econ_cap_cost numeric(24,2),
   mkt_risk_econ_cap_cost numeric(24,2),
   opr_risk_econ_cap_cost numeric(24,2),
   margn_acct_num varchar(80)
)
partition by (((date_part('year', ma_acptn.etl_date) * 100) + date_part('month', ma_acptn.etl_date)));

comment on table f_rdm.ma_acptn is '承兑汇票';


create table f_rdm.ma_guar
(
   etl_date date not null,
   biz_id varchar(80),
   org_num varchar(80),
   cust_num varchar(80),
   cur_cd varchar(50),
   prod_cd varchar(50),
   st_int_dt date,
   due_dt date,
   fiv_cls varchar(50),
   sys_src varchar(50),
   loan_deval_prep_bal numeric(24,2),
   loan_deval_prep_amt numeric(24,2),
   acct_stat_cd varchar(50),
   prin_subj varchar(80),
   guar_amt numeric(24,2),
   guar_bal numeric(24,2),
   mth_day_avg_bal numeric(24,2),
   yr_day_avg_bal numeric(24,2),
   prosp_loss numeric(24,2),
   crdt_risk_econ_cap numeric(24,2),
   mkt_risk_econ_cap numeric(24,2),
   opr_risk_econ_cap numeric(24,2),
   crdt_risk_econ_cap_cost numeric(24,2),
   mkt_risk_econ_cap_cost numeric(24,2),
   opr_risk_econ_cap_cost numeric(24,2),
   margn_acct_num varchar(80)
)
partition by (((date_part('year', ma_guar.etl_date) * 100) + date_part('month', ma_guar.etl_date)));

comment on table f_rdm.ma_guar is '保函';


create table f_rdm.ma_lc
(
   etl_date date not null,
   biz_id varchar(80),
   org_num varchar(80),
   cust_num varchar(80),
   cur_cd varchar(50),
   prod_cd varchar(50),
   st_int_dt date,
   due_dt date,
   fiv_cls varchar(50),
   sys_src varchar(50),
   loan_deval_prep_bal numeric(24,2),
   loan_deval_prep_amt numeric(24,2),
   acct_stat_cd varchar(50),
   prin_subj varchar(80),
   lc_amt numeric(24,2),
   lc_bal numeric(24,2),
   issu_bank_cd varchar(50),
   issu_bank_nm varchar(300),
   advis_bank_cd varchar(50),
   advis_bank_nm varchar(300),
   nego_pay_line_cd varchar(50),
   nego_pay_line_nm varchar(300),
   mth_day_avg_bal numeric(24,2),
   yr_day_avg_bal numeric(24,2),
   prosp_loss numeric(24,2),
   crdt_risk_econ_cap numeric(24,2),
   mkt_risk_econ_cap numeric(24,2),
   opr_risk_econ_cap numeric(24,2),
   crdt_risk_econ_cap_cost numeric(24,2),
   mkt_risk_econ_cap_cost numeric(24,2),
   opr_risk_econ_cap_cost numeric(24,2),
   margn_acct_num varchar(80)
)
partition by (((date_part('year', ma_lc.etl_date) * 100) + date_part('month', ma_lc.etl_date)));

comment on table f_rdm.ma_lc is '信用证';


create table f_rdm.ma_chrem
(
   etl_date date not null,
   cust_chrem_acct varchar(80),
   chrem_acct_contr_org varchar(50),
   cur_natl_std varchar(50),
   prod_id varchar(80),
   prod_series varchar(50),
   folders varchar(80),
   prin_curr_bal numeric(24,2),
   accm_bal_m numeric(24,2),
   accm_bal_y numeric(24,2),
   prin_subj varchar(80),
   upto_to_today_accm_int numeric(24,2),
   stop_to_last_day_accm_int numeric(24,2),
   today_int numeric(24,2),
   curmth_int_accm numeric(24,2),
   int_subj varchar(80),
   src_sys varchar(50),
   mth_day_avg_bal numeric(24,2),
   yr_day_avg_bal numeric(24,2),
   ftp_price numeric(24,2),
   ftp_tranfm_incom numeric(24,2)
)
partition by (((date_part('year', ma_chrem.etl_date) * 100) + date_part('month', ma_chrem.etl_date)));

comment on table f_rdm.ma_chrem is '理财';


create table f_rdm.ma_tax_rate
(
   etl_date date not null,
   subj varchar(80),
   subj_nm varchar(100),
   org_typ varchar(50),
   incom_expns_typ varchar(50),
   tax_attr varchar(50),
   tax_rate numeric(10,6)
)
partition by (((date_part('year', ma_tax_rate.etl_date) * 100) + date_part('month', ma_tax_rate.etl_date)));

comment on table f_rdm.ma_tax_rate is '税率';


create table f_rdm.ma_exchg_rate
(
   etl_date date not null,
   orgnl_cur varchar(50),
   convt_cur varchar(50),
   efft_day date,
   exchg_rate_val numeric(10,6)
)
partition by (((date_part('year', ma_exchg_rate.etl_date) * 100) + date_part('month', ma_exchg_rate.etl_date)));

comment on table f_rdm.ma_exchg_rate is '汇率';


create table f_rdm.ma_comm_fee_sum
(
   etl_date date not null,
   tx_dt date,
   acct_num varchar(80),
   org_num varchar(80),
   cur_cd varchar(50),
   cust_num varchar(80),
   prod_cd varchar(50),
   tx_typ varchar(80),
   comm_fee_subj varchar(80),
   comm_fee_amt numeric(24,2),
   chrg_typ varchar(50),
   tx_chnl varchar(50),
   sys_src varchar(50)
)
partition by (((date_part('year', ma_comm_fee_sum.etl_date) * 100) + date_part('month', ma_comm_fee_sum.etl_date)));

comment on table f_rdm.ma_comm_fee_sum is '手续费汇总';


create table f_rdm.ma_mth_crdt_cust
(
   etl_date date not null,
   org_num varchar(80),
   cust_num varchar(80),
   crdt_amt numeric(24,2),
   sys_src varchar(50)
)
partition by (((date_part('year', ma_mth_crdt_cust.etl_date) * 100) + date_part('month', ma_mth_crdt_cust.etl_date)));

comment on table f_rdm.ma_mth_crdt_cust is '月授信客户表';


create table f_rdm.ma_mth_crdt_card_cnt_sum
(
   etl_date date not null,
   org_num varchar(80),
   actv_ind varchar(1),
   stl_crdt_card_cnt numeric(37,15),
   new_incrs_crdt_card_cnt numeric(37,15),
   sys_src varchar(50)
)
partition by (((date_part('year', ma_mth_crdt_card_cnt_sum.etl_date) * 100) + date_part('month', ma_mth_crdt_card_cnt_sum.etl_date)));

comment on table f_rdm.ma_mth_crdt_card_cnt_sum is '月信用卡数汇总表';


create table f_rdm.ma_opr_line_pers_cnt
(
   etl_date date not null,
   org_num varchar(80),
   dept_num varchar(80),
   dept_nm varchar(100),
   post_encd varchar(80),
   post_nm varchar(100),
   pers_cnt numeric(1,0),
   sys_src varchar(50)
)
partition by (((date_part('year', ma_opr_line_pers_cnt.etl_date) * 100) + date_part('month', ma_opr_line_pers_cnt.etl_date)));

comment on table f_rdm.ma_opr_line_pers_cnt is '经营条线人数表';


create table f_rdm.ma_ibank_info
(
   etl_date date not null,
   acct_num varchar(80),
   org_num varchar(80),
   cust_num varchar(80),
   cur_cd varchar(50),
   prod_cd varchar(50),
   st_int_dt date,
   due_dt date,
   ori_int_rate numeric(10,6),
   curr_int_rate numeric(10,6),
   bmk_int_rate numeric(10,6),
   basic_diff numeric(10,6),
   int_days varchar(50),
   cmpd_int_calc_mode_cd varchar(50),
   int_mode_cd varchar(50),
   int_rate_attr_cd varchar(50),
   orgnl_term varchar(8),
   orgnl_term_corp_cd varchar(50),
   rprc_prd numeric(15,0),
   rprc_prd_corp_cd varchar(50),
   last_rprc_day date,
   next_rprc_day date,
   sys_src varchar(50),
   acct_stat varchar(50),
   prin_subj varchar(80),
   curr_bal numeric(24,2),
   int_subj varchar(80),
   today_provs_int numeric(24,2),
   curmth_provs_int numeric(24,2),
   accm_provs_int numeric(24,2),
   today_acpt_pay_int numeric(24,2),
   curmth_recvd_int_pay numeric(24,2),
   accm_recvd_int_pay numeric(24,2),
   int_adj_amt numeric(24,2),
   mth_day_avg_bal numeric(24,2),
   yr_day_avg_bal numeric(24,2),
   ftp_price numeric(24,2),
   ftp_tranfm_expns numeric(24,2),
   prosp_loss numeric(24,2),
   deval_prep_bal numeric(24,2),
   deval_prep_amt numeric(24,2),
   crdt_risk_econ_cap numeric(24,2),
   mkt_risk_econ_cap numeric(24,2),
   opr_risk_econ_cap numeric(24,2),
   crdt_risk_econ_cap_cost numeric(24,2),
   mkt_risk_econ_cap_cost numeric(24,2),
   opr_risk_econ_cap_cost numeric(24,2)
)
partition by (((date_part('year', ma_ibank_info.etl_date) * 100) + date_part('month', ma_ibank_info.etl_date)));

comment on table f_rdm.ma_ibank_info is '同业信息';


create table f_rdm.ma_mth_tx_cnt_sum
(
   etl_date date not null,
   acct_num varchar(80),
   org_num varchar(80),
   cur_cd varchar(50),
   tx_typ varchar(80),
   tx_chnl varchar(50),
   tx_cnt numeric(24,0),
   tx_amt numeric(24,2),
   sys_src varchar(50)
)
partition by (((date_part('year', ma_mth_tx_cnt_sum.etl_date) * 100) + date_part('month', ma_mth_tx_cnt_sum.etl_date)));

comment on table f_rdm.ma_mth_tx_cnt_sum is '月交易笔数汇总表';


create table f_rdm.ma_loan_and_tm_dpst_rcpt_rel
(
   etl_date date not null,
   loan_acct varchar(80),
   impw_tm_dpst_rcpt_acct_num varchar(80),
   sys_src varchar(50)
)
partition by (((date_part('year', ma_loan_and_tm_dpst_rcpt_rel.etl_date) * 100) + date_part('month', ma_loan_and_tm_dpst_rcpt_rel.etl_date)));

comment on table f_rdm.ma_loan_and_tm_dpst_rcpt_rel is '贷款与定期存单关系';


create table f_rdm.ma_cust_corp
(
   etl_date date not null,
   cust_num varchar(80),
   cust_nm varchar(200),
   cust_org_org_cd varchar(50),
   belg_org_num varchar(80),
   ovrs_cust_rgst_num varchar(80),
   open_acct_dt date,
   cust_typ_cd varchar(50),
   cust_inds_cd varchar(50),
   cust_size_cd varchar(50),
   rgst_cap numeric(24,2),
   cust_rat varchar(50),
   is_grp_cust_ind varchar(1),
   corp_grp_prnt_corp_nm varchar(400),
   corp_grp_prnt_corp_cust_num varchar(80),
   is_vip varchar(30),
   strgy_cust_ind varchar(1),
   pltf_ind varchar(1),
   prtnr_ind varchar(1)
);


create table f_rdm.ma_bond_etc_inves
(
   etl_date date,
   acct_num varchar(80),
   org_num varchar(80),
   dept_ent varchar(50),
   bond_cd varchar(50),
   tx_cnt_pty_cust_num varchar(80),
   acct_typ varchar(50),
   tx_comb varchar(80),
   bond_typ varchar(50),
   bond_issur varchar(80),
   cur_cd varchar(50),
   prod_cd varchar(50),
   st_int_dt date,
   due_dt date,
   curr_int_rate numeric(12,6),
   bmk_int_rate numeric(12,6),
   basic_diff numeric(12,6),
   int_days varchar(50),
   cmpd_int_calc_mode_cd varchar(50),
   int_rate_attr_cd varchar(50),
   orgnl_term numeric(24,0),
   orgnl_term_corp_cd varchar(50),
   rprc_prd numeric(24,0),
   rprc_prd_corp_cd varchar(50),
   last_rprc_day date,
   next_rprc_day date,
   sys_src varchar(50),
   prin_subj varchar(80),
   book_bal numeric(24,2),
   buy_cost numeric(24,2),
   mkt_val numeric(24,2),
   int_subj varchar(80),
   today_provs_int numeric(24,2),
   curmth_provs_int numeric(24,2),
   today_chrg_int numeric(24,2),
   curmth_recvd_int numeric(24,2),
   accm_recvd_int numeric(24,2),
   valtn_prft_loss_subj varchar(80),
   today_valtn_prft_loss_amt numeric(24,2),
   curmth_valtn_prft_loss_amt numeric(24,2),
   biz_prc_diff_prft_subj varchar(80),
   today_biz_prc_diff_amt numeric(24,2),
   curmth_biz_prc_diff_amt numeric(24,2),
   comm_fee_subj varchar(80),
   today_comm_fee_amt numeric(24,2),
   curmth_comm_fee_happ numeric(24,2),
   mth_day_avg_bal numeric(24,2),
   yr_day_avg_bal numeric(24,2),
   ftp_price numeric(24,2),
   ftp_tranfm_expns numeric(24,2),
   prosp_loss numeric(24,2),
   crdt_risk_econ_cap numeric(24,2),
   mkt_risk_econ_cap numeric(24,2),
   opr_risk_econ_cap numeric(24,2),
   crdt_risk_econ_cap_cost numeric(24,2),
   mkt_risk_econ_cap_cost numeric(24,2),
   opr_risk_econ_cap_cost numeric(24,2)
);


create table f_rdm.ma_deriv_tx
(
   etl_date date,
   tx_id varchar(80),
   acct_num varchar(80),
   org_num varchar(80),
   cust_num varchar(80),
   prod_cd varchar(50),
   tx_comb varchar(80),
   cur_cd varchar(50),
   cnt_pty_cur varchar(50),
   st_int_dt date,
   due_dt date,
   exchg_rate numeric(12,6),
   curr_int_rate numeric(12,6),
   cnt_pty_curr_int_rate numeric(12,6),
   bmk_int_rate numeric(12,6),
   basic_diff numeric(12,6),
   int_days varchar(50),
   cnt_pty_int_days varchar(50),
   cmpd_int_calc_mode_cd varchar(50),
   pre_chrg_int varchar(50),
   int_pay_freq varchar(100),
   int_rate_attr_cd varchar(50),
   cnt_pty_int_rate_attr varchar(50),
   orgnl_term numeric(24,0),
   orgnl_term_corp_cd varchar(50),
   sys_src varchar(50),
   prin_subj varchar(80),
   notnl_prin numeric(24,2),
   cnt_pty_prin_subj varchar(80),
   cnt_pty_notnl_prin numeric(24,2),
   net_val numeric(24,2),
   prec_metal_amt numeric(24,2),
   paybl_int_subj varchar(80),
   today_paybl_int numeric(24,2),
   curmth_paybl_int numeric(24,2),
   recvbl_int_subj varchar(80),
   today_recvbl_int numeric(24,2),
   curmth_recvbl_int numeric(24,2),
   valtn_prft_loss_subj varchar(80),
   today_valtn_prft_loss_amt numeric(24,2),
   curmth_valtn_prft_loss_amt numeric(24,2),
   biz_prc_diff_prft_subj varchar(80),
   today_biz_prc_diff_amt numeric(24,2),
   curmth_biz_prc_diff_amt numeric(24,2),
   mth_day_avg_bal numeric(24,2),
   yr_day_avg_bal numeric(24,2),
   ftp_price numeric(24,2),
   ftp_tranfm_expns numeric(24,2),
   prosp_loss numeric(24,2),
   crdt_risk_econ_cap numeric(24,2),
   mkt_risk_econ_cap numeric(24,2),
   opr_risk_econ_cap numeric(24,2),
   crdt_risk_econ_cap_cost numeric(24,2),
   mkt_risk_econ_cap_cost numeric(24,2),
   opr_risk_econ_cap_cost numeric(24,2)
);


create table f_rdm.ma_buy_back
(
   etl_date date not null,
   buy_back_id varchar(80),
   org_num varchar(80),
   tx_comb varchar(80),
   cust_num varchar(80),
   acct_typ varchar(50),
   cur_cd varchar(50),
   prod_cd varchar(50),
   st_int_dt date,
   due_dt date,
   curr_int_rate numeric(10,6),
   bmk_int_rate numeric(10,6),
   basic_diff numeric(10,6),
   int_days varchar(50),
   cmpd_int_calc_mode_cd varchar(50),
   int_rate_attr_cd varchar(50),
   orgnl_term numeric(24,0),
   orgnl_term_corp_cd varchar(50),
   rprc_prd numeric(15,0),
   rprc_prd_corp_cd varchar(50),
   last_rprc_day date,
   next_rprc_day date,
   sys_src varchar(50),
   prin_subj varchar(80),
   curr_bal numeric(24,2),
   int_subj varchar(80),
   today_provs_int numeric(24,2),
   curmth_provs_int numeric(24,2),
   accm_provs_int numeric(24,2),
   today_chrg_int numeric(24,2),
   curmth_recvd_int numeric(24,2),
   accm_recvd_int numeric(24,2),
   int_adj_amt numeric(24,2),
   mth_day_avg_bal numeric(24,2),
   yr_day_avg_bal numeric(24,2),
   ftp_price numeric(24,2),
   ftp_tranfm_expns numeric(24,2),
   prosp_loss numeric(24,2),
   deval_prep_bal numeric(24,2),
   deval_prep_amt numeric(24,2),
   crdt_risk_econ_cap numeric(24,2),
   mkt_risk_econ_cap numeric(24,2),
   opr_risk_econ_cap numeric(24,2),
   crdt_risk_econ_cap_cost numeric(24,2),
   mkt_risk_econ_cap_cost numeric(24,2),
   opr_risk_econ_cap_cost numeric(24,2),
   exchg_prft_loss_subj varchar(80),
   fair_val_chg_subj varchar(80)
)
partition by (((date_part('year', ma_buy_back.etl_date) * 100) + date_part('month', ma_buy_back.etl_date)));

comment on table f_rdm.ma_buy_back is '资金业务-逆回购';


create table f_rdm.gl_cust_info
(
   etl_date date not null,
   cust_id varchar(80),
   cust_nm varchar(200),
   cust_inds_typ_cd varchar(50),
   sys_src varchar(50)
)
partition by (((date_part('year', gl_cust_info.etl_date) * 100) + date_part('month', gl_cust_info.etl_date)));


create table f_rdm.gl_loan_dubil
(
   etl_date date not null,
   contr_id varchar(80),
   dubil_id varchar(80),
   fst_lvl_brch varchar(50),
   cust_id varchar(80),
   loan_bal numeric(24,2),
   due_dt date,
   is_nt_ovrd varchar(5),
   prin_ovrd_days numeric(24,0),
   int_ovrd_days numeric(24,0),
   wrtoff_dt date,
   wrtoff_prin numeric(24,2),
   wrtoff_in_bal_int numeric(24,2),
   wrtoff_norm_int numeric(24,2),
   wrtoff_arr_int numeric(24,2),
   wrtoff_pnsh_int numeric(24,2),
   wrtoff_post_retr_dt date,
   wrtoff_post_retr_prin numeric(24,2),
   wrtoff_post_retr_int numeric(24,2),
   wrtoff_post_retr_accrd_int numeric(24,2),
   fiv_cls varchar(50),
   resv_provs_amt numeric(24,2),
   cur_cd varchar(50),
   next_rprc_day date,
   loan_typ varchar(50),
   sys_src varchar(50),
   prin_subj varchar(80)
)
partition by (((date_part('year', gl_loan_dubil.etl_date) * 100) + date_part('month', gl_loan_dubil.etl_date)));


create table f_rdm.gl_loan_contr
(
   etl_date date not null,
   contr_id varchar(80),
   contr_amt numeric(24,2),
   contr_bal numeric(24,2),
   contr_aval_amt numeric(24,2),
   contr_end_prd date,
   sys_src varchar(50)
)
partition by (((date_part('year', gl_loan_contr.etl_date) * 100) + date_part('month', gl_loan_contr.etl_date)));


create table f_rdm.gl_guar_contr
(
   etl_date date not null,
   loan_contr_agmt_id varchar(80),
   guar_contr_st_dt date,
   guar_contr_stp_dt date,
   guar_amt numeric(24,2),
   guar_typ_cd varchar(50),
   guar_agmt_id varchar(300),
   sys_src varchar(50)
)
partition by (((date_part('year', gl_guar_contr.etl_date) * 100) + date_part('month', gl_guar_contr.etl_date)));


create table f_rdm.gl_mrtg_prop
(
   etl_date date not null,
   pldg_id varchar(80),
   guar_agmt_id varchar(300),
   pldg_typ_cd varchar(50),
   pldg_rgst_val numeric(24,2),
   pldg_estim_val numeric(24,2),
   sys_src varchar(50)
)
partition by (((date_part('year', gl_mrtg_prop.etl_date) * 100) + date_part('month', gl_mrtg_prop.etl_date)));


create table f_rdm.gl_crdt_card
(
   etl_date date not null,
   prod_nm varchar(100),
   pri_card_card_num varchar(80),
   fst_lvl_brch varchar(50),
   cust_id varchar(80),
   crdt_lmt numeric(24,2),
   od_prin numeric(24,2),
   fiv_cls varchar(50),
   is_nt_ovrd varchar(5),
   ovrd_prin_amt numeric(24,2),
   ovrd_int_amt numeric(24,2),
   resv_provs numeric(24,2),
   ovrd_days numeric(24,0),
   wrtoff_dt date,
   wrtoff_prin numeric(24,2),
   wrtoff_late_chrg numeric(24,2),
   wrtoff_comm_fee numeric(24,2),
   wrtoff_in_bal_int numeric(24,2),
   wrtoff_off_bal_int numeric(24,2),
   wrtoff_post_repay_dt date,
   wrtoff_post_repay_amt numeric(24,2),
   sys_src varchar(50),
   cur_cd varchar(50)
)
partition by (((date_part('year', gl_crdt_card.etl_date) * 100) + date_part('month', gl_crdt_card.etl_date)));


create table f_rdm.gl_lc_guar_draft
(
   etl_date date not null,
   biz_id varchar(80),
   biz_cate varchar(50),
   fst_lvl_brch varchar(50),
   applr_id varchar(80),
   due_dt date,
   cur_cd varchar(50),
   end_tm_orgnl_book_amt numeric(24,2),
   rmb_book_amt numeric(24,2),
   sys_src varchar(50)
)
partition by (((date_part('year', gl_lc_guar_draft.etl_date) * 100) + date_part('month', gl_lc_guar_draft.etl_date)));


create table f_rdm.gl_dpst
(
   etl_date date not null,
   acct_num varchar(50),
   sub_acct_num varchar(50),
   accti_pltf_subj_num varchar(50),
   fst_lvl_brch varchar(50),
   cust_cd varchar(50),
   cur_cd varchar(50),
   book_orgnl_amt numeric(24,2),
   stbl_int_rate varchar(30),
   sys_src varchar(50),
   prin_subj varchar(80)
)
partition by (((date_part('year', gl_dpst.etl_date) * 100) + date_part('month', gl_dpst.etl_date)));


create table f_rdm.gl_ibank
(
   etl_date date not null,
   bank_num varchar(50),
   bank_sub_acct varchar(50),
   biz_cate varchar(50),
   fst_lvl_brch varchar(50),
   due_dt date,
   orgnl_cur varchar(50),
   orgnl_bal numeric(24,2),
   sys_src varchar(50),
   term numeric(24,0),
   prin_subj varchar(80),
   cust_id varchar(80)
)
partition by (((date_part('year', gl_ibank.etl_date) * 100) + date_part('month', gl_ibank.etl_date)));


create table f_rdm.gl_cap_ibank_offer
(
   etl_date date not null,
   tx_num varchar(50),
   biz_typ varchar(30),
   tx_tools varchar(50),
   due_dt date,
   remn_prin numeric(24,2),
   fst_lvl_brch varchar(50),
   orgnl_cur varchar(50),
   orgnl_bal numeric(24,2),
   sys_src varchar(50),
   term numeric(24,0),
   prin_subj varchar(80)
)
partition by (((date_part('year', gl_cap_ibank_offer.etl_date) * 100) + date_part('month', gl_cap_ibank_offer.etl_date)));


create table f_rdm.gl_inves
(
   etl_date date not null,
   tx_num varchar(50),
   acct_nm varchar(100),
   tx_tool_cls varchar(50),
   issu_day date,
   due_dt date,
   remn_par numeric(24,2),
   recvbl_int_bal numeric(24,2),
   int_adj_bal numeric(24,2),
   val_chg_bal numeric(24,2),
   deval_prep_bal numeric(24,2),
   sys_src varchar(50),
   cur_cd varchar(50),
   prin_subj varchar(80)
)
partition by (((date_part('year', gl_inves.etl_date) * 100) + date_part('month', gl_inves.etl_date)));


create table f_rdm.gl_buy_back_bond
(
   etl_date date not null,
   tx_num varchar(50),
   tx_sht_nm varchar(100),
   due_dt date,
   dlvy_amt numeric(24,2),
   book_val numeric(24,2),
   cur_cd varchar(50),
   sys_src varchar(50),
   term numeric(24,0),
   prin_subj varchar(80)
)
partition by (((date_part('year', gl_buy_back_bond.etl_date) * 100) + date_part('month', gl_buy_back_bond.etl_date)));


create table f_rdm.gl_buy_back_bill
(
   etl_date date not null,
   tx_id varchar(80),
   bill_num varchar(80),
   book_val numeric(24,2),
   tx_cnt_pty_id varchar(50),
   discnt_cate varchar(30),
   bill_typ varchar(30),
   buy_dt date,
   sell_dt date,
   buy_back_dt date,
   rtn_sell_dt date,
   sell_int_rate numeric(10,6),
   buy_int_rate numeric(10,6),
   par_amt numeric(24,2),
   recv_amt numeric(24,2),
   actl_pmt_amt numeric(24,2),
   sys_src varchar(50),
   term numeric(24,0),
   cur_cd varchar(50),
   prin_subj varchar(80)
)
partition by (((date_part('year', gl_buy_back_bill.etl_date) * 100) + date_part('month', gl_buy_back_bill.etl_date)));


create table f_rdm.gl_agent
(
   etl_date date not null,
   insu_corp_cd varchar(50),
   insu_comm_fee numeric(24,2),
   presrv_comm_fee numeric(24,2),
   rwl_prd_comm_fee numeric(24,2),
   realtm_clltn_comm_fee numeric(24,2),
   bat_comm_fee numeric(24,2),
   oth_comm_fee numeric(24,2),
   comm_fee_sum numeric(24,2),
   sys_src varchar(50)
)
partition by (((date_part('year', gl_agent.etl_date) * 100) + date_part('month', gl_agent.etl_date)));


create table f_rdm.gl_derive
(
   etl_date date not null,
   tx_num varchar(50),
   prod_nm varchar(100),
   net_prc_mkt_val numeric(24,2),
   notnl_prin_amt numeric(24,2),
   cur_cd varchar(50),
   sys_src varchar(50),
   prin_subj varchar(80)
)
partition by (((date_part('year', gl_derive.etl_date) * 100) + date_part('month', gl_derive.etl_date)));


create table f_rdm.ma_accti2gl
(
   etl_date date not null,
   src_sys_subj_cd varchar(50),
   majr_gl_subj_cd varchar(50),
   majr_gl_dtl_subj_cd varchar(50),
   majr_gl_prod_cd varchar(50),
   majr_gl_line_cd varchar(50),
   st_use_dt date,
   invldtn_dt date,
   sys_src varchar(50)
)
partition by (((date_part('year', ma_accti2gl.etl_date) * 100) + date_part('month', ma_accti2gl.etl_date)));


create table f_rdm.ma_subj_info
(
   etl_date date not null,
   subj_cd varchar(80),
   subj_nm varchar(100),
   subj_hrcy varchar(50),
   subj_cate varchar(50),
   subj_stat varchar(50),
   dtl_subj_int varchar(1),
   sys_src varchar(50)
)
partition by (((date_part('year', ma_subj_info.etl_date) * 100) + date_part('month', ma_subj_info.etl_date)));

comment on table f_rdm.ma_subj_info is '科目信息';


create table f_rdm.ma_gl
(
   etl_date date not null,
   org_cd varchar(50),
   cur_cd varchar(50),
   duty_ctr_cd varchar(50),
   account_cd varchar(50),
   dtl_subj_cd varchar(50),
   prod_cd varchar(50),
   biz_line_cd varchar(50),
   inn_reco_cd varchar(50),
   business_unit_cd varchar(50),
   spare_cd varchar(50),
   today_debit_amt numeric(24,2),
   today_crdt_amt numeric(24,2),
   debit_bal numeric(24,2),
   crdt_bal numeric(24,2),
   curmth_accm_debit_amt numeric(24,2),
   curmth_accm_crdt_amt numeric(24,2),
   term_ind numeric(24,2),
   sys_src varchar(50)
)
partition by (((date_part('year', ma_gl.etl_date) * 100) + date_part('month', ma_gl.etl_date)));

comment on table f_rdm.ma_gl is '总账';


create table f_rdm.ma_crdt_od
(
   etl_date date not null,
   acct_num varchar(80),
   card_num varchar(80),
   org_num varchar(80),
   cust_num varchar(80),
   cur_cd varchar(50),
   prod_cd varchar(50),
   st_int_dt date,
   due_dt date,
   curr_int_rate numeric(12,6),
   bmk_int_rate numeric(12,6),
   basic_diff numeric(12,6),
   int_days varchar(50),
   cmpd_int_calc_mode_cd varchar(50),
   sys_src varchar(50),
   acct_stat_cd varchar(50),
   prin_subj_od varchar(80),
   spl_pay_bal numeric(24,2),
   prin_subj_dpst varchar(80),
   curr_bal numeric(24,2),
   int_subj varchar(80),
   today_provs_int numeric(24,2),
   curmth_provs_int numeric(24,2),
   accm_provs_int numeric(24,2),
   today_chrg_int numeric(24,2),
   curmth_recvd_int numeric(24,2),
   accm_recvd_int numeric(24,2),
   int_adj_amt numeric(24,2),
   mth_day_avg_bal numeric(24,2),
   yr_day_avg_bal numeric(24,2),
   prosp_loss numeric(24,2),
   deval_prep_bal numeric(24,2),
   deval_prep_amt numeric(24,2),
   crdt_risk_econ_cap numeric(24,2),
   mkt_risk_econ_cap numeric(24,2),
   opr_risk_econ_cap numeric(24,2),
   crdt_risk_econ_cap_cost numeric(24,2),
   mkt_risk_econ_cap_cost numeric(24,2),
   opr_risk_econ_cap_cost numeric(24,2),
   prmt_org_num varchar(50)
)
partition by (((date_part('year', ma_crdt_od.etl_date) * 100) + date_part('month', ma_crdt_od.etl_date)));

comment on table f_rdm.ma_crdt_od is '信用卡透支';


create table f_rdm.ma_dpst
(
   etl_date date,
   dpst_acct_num varchar(80),
   org_num varchar(80),
   cust_num varchar(80),
   cur_cd varchar(50),
   prod_cd varchar(50),
   st_int_dt date,
   due_dt date,
   ori_int_rate numeric(10,6),
   curr_int_rate numeric(10,6),
   bmk_int_rate numeric(10,6),
   basic_diff numeric(10,6),
   int_days varchar(50),
   cmpd_int_calc_mode_cd varchar(50),
   int_mode_cd varchar(50),
   int_rate_attr_cd varchar(50),
   orgnl_term varchar(8),
   orgnl_term_corp_cd varchar(50),
   rprc_prd int,
   rprc_prd_corp_cd varchar(50),
   last_rprc_day date,
   next_rprc_day date,
   sys_src varchar(50),
   is_autornw varchar(2),
   last_autornw_dt date,
   acct_stat_cd varchar(50),
   prin_subj varchar(80),
   curr_bal numeric(24,2),
   int_subj varchar(80),
   today_provs_int numeric(24,2),
   curmth_provs_int numeric(24,2),
   accm_provs_int numeric(24,2),
   today_int_pay numeric(24,2),
   curmth_paid_int numeric(24,2),
   accm_paid_int numeric(24,2),
   int_adj_amt numeric(24,2),
   mth_day_avg_bal numeric(24,2),
   yr_day_avg_bal numeric(24,2),
   ftp_price numeric(24,2),
   ftp_tranfm_incom numeric(24,2)
);

comment on table f_fdm.f_loan_corp_crdt is '公司授信额度信息表';
comment on table f_fdm.f_cust_corp is '公司客户基本信息表';
comment on table f_fdm.f_agt_ftp_info is 'ftp信息表';
comment on table f_fdm.f_dpst_indv_acct is '个人存款账户信息表';
comment on table f_fdm.cd_rf_std_cd_tran_ref is '需转换代码表';
comment on table f_fdm.cd_cd_table is '代码表';
comment on table f_fdm.f_agt_cap_offer is '资金拆借信息表';
comment on table f_fdm.f_dpst_corp_acct is '公司存款账户信息表';
comment on table f_fdm.f_agt_cap_stor is '资金存放信息表';
comment on table f_fdm.f_agt_cap_bond_buy_back is '资金债券回购业务信息表';
comment on table f_fdm.f_cap_bond_inves is '资金债券投资信息表';
comment on table f_fdm.f_cap_raw_tx is '资金衍生交易信息表';
comment on table f_fdm.f_evt_insu_comm_fee is '保险公司手续费信息表';
comment on table f_fdm.f_cust_insu is '保险公司基本信息表';
comment on table f_fdm.f_acct_fund_agn is '基金代销账户信息表';
comment on table f_fdm.f_acct_prec_metal is '贵金属账户信息表';
comment on table f_fdm.f_agt_asst_consv is '资产保全信息表';
comment on table f_fdm.f_agt_guarantee is '保函信息表';
comment on table f_fdm.f_evt_fxstl_fex is '结售汇及外汇买卖交易信息表';
comment on table f_fdm.f_evt_comfee_commsn is '中间业务收入信息表';
comment on table f_fdm.f_org_info is '本行机构信息表';
comment on table f_rdm.ma_cust_corp is '公司客户';
comment on table f_rdm.ma_bond_etc_inves is '资金业务-债券等投资';
comment on table f_rdm.ma_deriv_tx is '资金业务-衍生交易';
comment on table f_rdm.gl_cust_info is '客户信息';
comment on table f_rdm.gl_loan_dubil is '贷款借据';
comment on table f_rdm.gl_loan_contr is '贷款合同';
comment on table f_rdm.gl_guar_contr is '担保合同';
comment on table f_rdm.gl_mrtg_prop is '抵质押物';
comment on table f_rdm.gl_crdt_card is '信用卡';
comment on table f_rdm.gl_lc_guar_draft is '信保汇（信用证 保函 汇票）';
comment on table f_rdm.gl_dpst is '存款';
comment on table f_rdm.gl_ibank is '同业 ';
comment on table f_rdm.gl_cap_ibank_offer is '资金拆借';
comment on table f_rdm.gl_inves is '投资';
comment on table f_rdm.gl_buy_back_bond is '回购反售-债券';
comment on table f_rdm.gl_buy_back_bill is '回购反售-票据';
comment on table f_rdm.gl_agent is '代理';
comment on table f_rdm.gl_derive is '衍生';
comment on table f_rdm.ma_accti2gl is '科目对照关系';
comment on table f_rdm.ma_dpst is '存款';






--create schema f_adm default include schema privileges;

create table f_adm.c_biz_line_dim_info
(
    etl_date date not null,
    biz_line_cd varchar(50),
    biz_line_nm varchar(100),
    biz_line_hrcy varchar(100),
    up_biz_line varchar(100),
    is_nt_leaf_node varchar(2),
    data_src varchar(50)
)
partition by (((date_part('year', c_biz_line_dim_info.etl_date) * 100) + date_part('month', c_biz_line_dim_info.etl_date)));

comment on table f_adm.c_biz_line_dim_info is '业务条线维度信息';

create table f_adm.c_prod_dim_info
(
    etl_date date not null,
    prod_cd varchar(50),
    prod_nm varchar(100),
    prod_hrcy varchar(100),
    up_prod varchar(100),
    is_nt_leaf_node varchar(2),
    data_src varchar(50)
)
partition by (((date_part('year', c_prod_dim_info.etl_date) * 100) + date_part('month', c_prod_dim_info.etl_date)));

comment on table f_adm.c_prod_dim_info is '产品维度信息';

create table f_adm.c_ibank_indx_addt_rcrd_templt
(
    data_mth char(6),
    freq varchar(1),
    ibank_org_en_sht_nm varchar(100),
    ibank_org_cn_nm varchar(100),
    indx_encd varchar(80),
    curr_val numeric(24,2),
    pre_prd_val numeric(24,2),
    last_yr_prd_val numeric(24,2),
    is_valid varchar(2),
    operation_date date,
    approval_status varchar(2),
    user_id varchar(80),
    user_name varchar(100),
    action_status varchar(100),
    is_approval varchar(100)
);

comment on table f_adm.c_ibank_indx_addt_rcrd_templt is '同业指标补录模板';







--create schema f_app default include schema privileges;
--***********************************************************************
-- f_app schema建表语句
-- author: hgf
-- date: 20160915
--***********************************************************************
drop table if exists f_app.t_index_info cascade;
create table f_app.t_index_info  (
    index_no    varchar(8)    not null,
    index_name    varchar(100)    ,
    index_typ1    varchar(4)    ,
    index_typ2    varchar(4)    ,
    create_date    date    ,
    valid_date    date    ,
    invldtn_date    date    ,
    index_frequency    varchar(4)    ,
    matn_stat    varchar(2)    ,
    dim    varchar(100)    ,
    frml_calc    varchar(2000)    ,
    data_src    varchar(4)    ,
    index_def    varchar(2000)    ,
    index_accuracy    varchar(4)    ,
    index_unit    varchar(30)    ,
    extend    varchar(1)    ,
    org    varchar(30)    ,
    is_commit    varchar(1)    ,
    stdby1    varchar(300)    ,
    stdby2    varchar(300)    ,
    stdby3    varchar(300)
    );
alter table f_app.t_index_info add constraint pk_t_index_info primary key (index_no);

--create projection f_app.t_index_info_super as select * from  f_app.t_index_info;


comment on table f_app.t_index_info is '指标表';
--comment on column f_app.t_index_info_super.index_no is '指标编号';
--comment on column f_app.t_index_info_super.index_name is '指标名称';
--comment on column f_app.t_index_info_super.index_typ1 is '一级分类';
--comment on column f_app.t_index_info_super.index_typ2 is '二级分类';
--comment on column f_app.t_index_info_super.create_date is '创建日期';
--comment on column f_app.t_index_info_super.valid_date is '生效日期';
--comment on column f_app.t_index_info_super.invldtn_date is '失效日期';
--comment on column f_app.t_index_info_super.index_frequency is '频度';
--comment on column f_app.t_index_info_super.matn_stat is '维护状态';
--comment on column f_app.t_index_info_super.dim is '适用维度';
--comment on column f_app.t_index_info_super.frml_calc is '公式计算';
--comment on column f_app.t_index_info_super.data_src is '数据来源';
--comment on column f_app.t_index_info_super.index_def is '指标定义';
--comment on column f_app.t_index_info_super.index_accuracy is '指标值精度';
--comment on column f_app.t_index_info_super.index_unit is '指标值单位';
--comment on column f_app.t_index_info_super.extend is '是否派生';
--comment on column f_app.t_index_info_super.org is '机构';
--comment on column f_app.t_index_info_super.is_commit is '发布状态';
--comment on column f_app.t_index_info_super.stdby1 is '备用字段1';
--comment on column f_app.t_index_info_super.stdby2 is '备用字段2';
--comment on column f_app.t_index_info_super.stdby3 is '备用字段3';

drop table if exists f_app.t_index_review cascade;
create table f_app.t_index_review  (
    index_no    varchar(8)    not null,
    index_name    varchar(100)    not null,
    user_id    varchar(32)    ,
    review_date    timestamp(6)    not null,
    review_opinion    varchar(300)
    );
alter table f_app.t_index_review add constraint pk_t_index_review primary key (index_no,index_name,review_date);

--create projection f_app.t_index_review_super as select * from  f_app.t_index_review;


comment on table f_app.t_index_review is '指标审批意见表';
--comment on column f_app.t_index_review_super.index_no is '指标编号';
--comment on column f_app.t_index_review_super.index_name is '指标名称';
--comment on column f_app.t_index_review_super.user_id is '审批人id';
--comment on column f_app.t_index_review_super.review_date is '审批日期';
--comment on column f_app.t_index_review_super.review_opinion is '审批意见';

drop table if exists f_app.t_hand_review_record cascade;
create table f_app.t_hand_review_record  (
    id          identity(1,1),
    menu_id    varchar(32)    ,
    menu_name    varchar(100)    ,
    count    varchar(12)    ,
    handle_date    date    ,
    user_id    varchar(32)    ,
    user_name    varchar(100)    ,
    action_status    varchar(12)    ,
    entity_id    varchar(12)
    );
alter table f_app.t_hand_review_record add constraint pk_t_hand_review_record primary key (id);

--create projection f_app.t_hand_review_record_super as select * from  f_app.t_hand_review_record;


comment on table f_app.t_hand_review_record is '补录审批清单表';
--comment on column f_app.t_hand_review_record_super.id is 'id';
--comment on column f_app.t_hand_review_record_super.menu_id is '菜单id';
--comment on column f_app.t_hand_review_record_super.menu_name is '菜单name';
--comment on column f_app.t_hand_review_record_super.count is '条数';
--comment on column f_app.t_hand_review_record_super.handle_date is '操作日期';
--comment on column f_app.t_hand_review_record_super.user_id is '提交人id';
--comment on column f_app.t_hand_review_record_super.user_name is '提交人名称';
--comment on column f_app.t_hand_review_record_super.action_status is '动作类型';
--comment on column f_app.t_hand_review_record_super.entity_id is '实体id';

drop table if exists f_app.t_hand_review cascade;
create table f_app.t_hand_review  (
    unionprimarykey    varchar(100)    not null,
    user_id    varchar(32)    ,
    review_date    timestamp(6)    ,
    review_opinion    varchar(300)    ,
    entity_id    varchar(32)
    );
alter table f_app.t_hand_review add constraint pk_t_hand_review primary key (unionprimarykey);

--create projection f_app.t_hand_review_super as select * from  f_app.t_hand_review;


comment on table f_app.t_hand_review is '补录审批意见表';
--comment on column f_app.t_hand_review_super.unionprimarykey is '主键';
--comment on column f_app.t_hand_review_super.user_id is '用户id';
--comment on column f_app.t_hand_review_super.review_date is '审批日期';
--comment on column f_app.t_hand_review_super.review_opinion is '审批意见';
--comment on column f_app.t_hand_review_super.entity_id is '实体id';

drop table if exists f_app.t_rd_validate_rule cascade;
create table f_app.t_rd_validate_rule  (
    rule_id    varchar(12)    not null,
    rule_name    varchar(200)    ,
    tab_en_name    varchar(50)    not null,
    tab_cn_name    varchar(500)    not null,
    field_en_name    varchar(100)    not null,
    field_cn_name    varchar(1000)    not null,
    validation_rule    varchar(1000)    not null,
    rule_type    varchar(1)    not null,
    rule_desc    varchar(3000)    ,
    validate_flag    varchar(10)    not null,
    action_type    varchar(100)    ,
    record_status    varchar(1)    ,
    review_flag    varchar(1)    ,
    reviewer    varchar(10)    ,
    review_timestamp    date(10)    ,
    review_reason    varchar(300)    ,
    create_user    varchar(50)    ,
    create_time    timestamp    ,
    last_update_user    varchar(50)    ,
    last_update_time    timestamp
    );
alter table f_app.t_rd_validate_rule add constraint pk_t_rd_validate_rule primary key (rule_id);

--create projection f_app.t_rd_validate_rule_super as select * from  f_app.t_rd_validate_rule;


comment on table f_app.t_rd_validate_rule is '数据校验表';
--comment on column f_app.t_rd_validate_rule_super.rule_id is '规则编号';
--comment on column f_app.t_rd_validate_rule_super.rule_name is '规则名称';
--comment on column f_app.t_rd_validate_rule_super.tab_en_name is '表英文名';
--comment on column f_app.t_rd_validate_rule_super.tab_cn_name is '表中文名';
--comment on column f_app.t_rd_validate_rule_super.field_en_name is '字段英文名';
--comment on column f_app.t_rd_validate_rule_super.field_cn_name is '字段中文名';
--comment on column f_app.t_rd_validate_rule_super.validation_rule is '校验规则';
--comment on column f_app.t_rd_validate_rule_super.rule_type is '规则类型';
--comment on column f_app.t_rd_validate_rule_super.rule_desc is '规则描述';
--comment on column f_app.t_rd_validate_rule_super.validate_flag is '验证标识';
--comment on column f_app.t_rd_validate_rule_super.action_type is '动作类型';
--comment on column f_app.t_rd_validate_rule_super.record_status is '数据类型';
--comment on column f_app.t_rd_validate_rule_super.review_flag is '审批状态';
--comment on column f_app.t_rd_validate_rule_super.reviewer is '审批人';
--comment on column f_app.t_rd_validate_rule_super.review_timestamp is '审批时间';
--comment on column f_app.t_rd_validate_rule_super.review_reason is '审批意见';
--comment on column f_app.t_rd_validate_rule_super.create_user is '创建用户';
--comment on column f_app.t_rd_validate_rule_super.create_time is '创建时间';
--comment on column f_app.t_rd_validate_rule_super.last_update_user is '更新用户';
--comment on column f_app.t_rd_validate_rule_super.last_update_time is '更新时间';

drop table if exists f_app.t_rd_exception cascade;
create table f_app.t_rd_exception  (
    rule_type    varchar(1)    not null,
    tab_en_name    varchar(50)    not null,
    tabcnname    varchar(500)    not null,
    rule_id    varchar(12)    not null,
    check_time    timestamp    ,
    field_en_name    varchar(50)    ,
    fieldcnname    varchar(1000)    ,
    work_dt    date    ,
    exception_msg    varchar(1000)    ,
    tab_pk_value    varchar(1000)    ,
    field_value    varchar(1000)    not null
    );
alter table f_app.t_rd_exception add constraint pk_t_rd_exception primary key (rule_id);

--create projection f_app.t_rd_exception_super as select * from  f_app.t_rd_exception;


comment on table f_app.t_rd_exception is '数据校验异常报告表';
--comment on column f_app.t_rd_exception_super.rule_type is '规则类型';
--comment on column f_app.t_rd_exception_super.tab_en_name is '表名称';
--comment on column f_app.t_rd_exception_super.tabcnname is '表中文名称';
--comment on column f_app.t_rd_exception_super.rule_id is '规则编号';
--comment on column f_app.t_rd_exception_super.check_time is '检验时间';
--comment on column f_app.t_rd_exception_super.field_en_name is '字段英文名称';
--comment on column f_app.t_rd_exception_super.fieldcnname is '字段中文名称';
--comment on column f_app.t_rd_exception_super.work_dt is '数据日期';
--comment on column f_app.t_rd_exception_super.exception_msg is '异常提示信息';
--comment on column f_app.t_rd_exception_super.tab_pk_value is '表主键值';
--comment on column f_app.t_rd_exception_super.field_value is '异常值';

drop table if exists f_app.app_logic_pk cascade;
create table f_app.app_logic_pk  (
    table_name    varchar(500)    ,
    column_cn_name    varchar(100)    ,
    column_name    varchar(100)    ,
    seq    number(18)
    );

--create projection f_app.app_logic_pk_super as select * from  f_app.app_logic_pk;


comment on table f_app.app_logic_pk is '基础层主键表';
--comment on column f_app.app_logic_pk_super.table_name is '表名';
--comment on column f_app.app_logic_pk_super.column_cn_name is '列中文名';
--comment on column f_app.app_logic_pk_super.column_name is '列名';
--comment on column f_app.app_logic_pk_super.seq is '序号';

drop table if exists f_app.f_fdm_tab_field cascade;
create table f_app.f_fdm_tab_field  (
    tabenname    varchar2(50)    ,
    tabcnname    varchar2(50)    ,
    fieldenname    varchar2(50)    ,
    fieldcnname    varchar2(50)
    );

--create projection f_app.f_fdm_tab_field_super as select * from  f_app.f_fdm_tab_field;


comment on table f_app.f_fdm_tab_field is '基础层表及字段';
--comment on column f_app.f_fdm_tab_field_super.tabenname is '表英文名';
--comment on column f_app.f_fdm_tab_field_super.tabcnname is '表中文名';
--comment on column f_app.f_fdm_tab_field_super.fieldenname is '字段英文名';
--comment on column f_app.f_fdm_tab_field_super.fieldcnname is '字段中文名';
