
/*
Author             :zhangwj
Function           :个人贷款借据信息表
Load method        :
Source table       :dw_sdata.pcs_005_tb_sup_loan_info              --主档表,
                    dw_sdata.pcs_006_tb_csm_customer               --客户总表,
                    dw_sdata.pcs_006_tb_lon_loan_duebill           --贷款借据表,
                    dw_sdata.pcs_006_tb_lon_loan                   --贷款台帐主信息表,
                    dw_sdata.pcs_006_tb_lon_loan_contract          --合同,
                    dw_sdata.pcs_001_tb_pub_loanratedic            --基准利率表,
                    dw_sdata.pcs_005_tb_sup_intr_rate_adjust       --利率调整表,
                    dw_sdata.pcs_005_tb_sup_repayment_plan         --分期贷款还款计划表,
                    dw_sdata.pcs_005_tb_sup_repayment_info         --还款登记簿,
                    dw_sdata.pcs_005_tb_sup_water_a                --当日流水表,
                    dw_sdata.pcs_005_tb_sup_debt_info_n            --分期贷款明细登记表,
                    dw_sdata.pcs_005_tb_sup_account_info           --分户,
                    dw_sdata.pcs_005_tb_sup_prin_plan_a            --还款方式还本计划表,
                    dw_sdata.pcs_005_tb_sup_water_c                --历史流水表,
                    dw_sdata.pcs_006_tb_abs_loan_info,
                    dw_sdata.acc_003_t_acc_assets_ledger           --资产类客户账户分户账,
                    dw_sdata.acc_003_t_accdata_last_item_no        --科目转换对照表,
                    dw_sdata.pcs_006_tb_lon_org_manage             --贷款业务机构管理表
Destination Table  :f_fdm.f_loan_indv_dubil
Frequency          :D
Modify history list:Created by zhangwj at 2016-4-25 16:10 v1.0
                    Changed by zhangwj at 2016-5-25 10:12 v1.1   大数据贴源层表名修改，表为拉链表或流水表，与之保持一致
                    Changed by zhangwj at 2016-5-25 14:12 v1.2   修改“当日计提利息 ”、“当月计提利息”、“累计计提利息”赋值规则
                    Changed by zhangwj at 2016-5-30 14:55 v1.3   汇总字段“当日收息”
                    Changed by zhangwj at 2016-6-13 14:55 v1.4   1.修改“计息方式代码”为“是否先收息”,并修改映射规则,2.修改字段‘还款周期单位代码’ 改为‘还款周期代码’3.删除字段‘贷款投向行业代码’
                    Changed by zhangwj at 2016-6-14 10:55 v1.5   修改“利率属性代码”的映射规则  
                    Changed by zhangwj at 2016-6-14 13:55 v1.6   新增月积数、年积数、月日均余额、年日均余额
                   :Modify  by liudongyan 20160714将临时表4里面的dw_sdata.pcs_005_tb_sup_water_a(当日流水表)改为dw_sdata.pcs_005_tb_sup_water_c（历史流水表）
                    Modify  by xumingaho 20160724将 T6表 psc_005_tb_sup_intr_rate_adjust改为psc_001_tb_sup_intr_rate_adjust;去掉T14,T15,T16盨¿禝¡⹡nd start_dt<=ETL 加馗¥央d_dt
                   modified by gln 20160922 累计计提、累计结�0
                   modified by zhangliang 20160923 将月基数null改为0
-------------------------------------------逻辑说明---------------------------------------------
业务逻辑说明
-------------------------------------------逻辑说明END------------------------------------------
*/
/*数据回退区*/
Delete /* +direct */ from  f_fdm.f_loan_indv_dubil
where etl_date='$TXDATE'::date
/*数据回退勥nd*/
;
/*临时表创建区*/
create local temporary table tt_f_loan_indv_dubil_temp_1
on commit preserve rows as
select
      t51.loan_id
      ,t52.base_rate
from
      (select
             loan_id
             ,(case
               when app_op_id='e821'
               and  province_num='54'
               then '3'
               when app_op_id='e821'
               and  province_num<>'54'
               then '2'
               when app_op_id<>'e821'
               and  province_num='54'
               then '4'
               when app_op_id<>'e821'
               and  province_num<>'54'
               then '1'
               end
               )    as rate_type                                                         --利率类型
             ,(case
               when loan_length>'0'
               and  loan_length <='6'
               then '6'
               when loan_length>'6'
               and  loan_length <='12'
               then '12'
               when loan_length>'12'
               and  loan_length <='36'
               then '36'
               when loan_length>'36'
               and  loan_length <='60'
               then '60'
               when loan_length>'60'
               and  loan_length <='9999'
               then '9999'
               end
               )    as  loan_length                                                       --利率档期
      from   dw_sdata.pcs_006_tb_lon_loan                                                 --贷款台帐主信息表
      where  start_dt<='$TXDATE'::date
      and    end_dt>'$TXDATE'::date
      ) t51
left join    dw_sdata.pcs_001_tb_pub_loanratedic t52                                      --基准利率表
on           t51.rate_type=t52.rate_type
and          t51.loan_length=t52.loan_length
and          t52.delflag ='0'                                                             --未删除
and          t52.rate_state ='2'                                                          --未删除
and          t52.start_dt<='$TXDATE'::date
and          t52.end_dt>'$TXDATE'::date
;
create local temporary table tt_f_loan_indv_dubil_temp_2                       --贷款利率调整表
on commit preserve rows as
select
      due_num
      ,max(itr_date) as itr_date
from  dw_sdata.pcs_001_tb_sup_intr_rate_adjust                                            --利率调整表
where fin_flg='1'
and   fin_date <='$TXDATE'
and   start_dt<='$TXDATE'::date
and   end_dt>'$TXDATE'::date
group by due_num
;
create local temporary table tt_f_loan_indv_dubil_temp_3
on commit preserve rows as
select
      due_num
      ,max(rcv_date)  as rcv_date
      from dw_sdata.pcs_005_tb_sup_repayment_info                                        --还款登记簿
where rcv_date<='$TXDATE'
--and   start_dt<='$TXDATE'::date
--and   end_dt>'$TXDATE'::date
group by due_num                                                                          --还款日期
;
create local temporary table tt_f_loan_indv_dubil_temp_4
on commit preserve rows as
SELECT DUE_NUM,sum(AMT_INCUR) as AMT_INCUR FROM (
              (SELECT  DUE_NUM,AMT_INCUR FROM DW_SDATA.pcs_005_tb_sup_water_c
               WHERE ACC_TYP='05' AND BRW_LGO='D' and  sup_date='$TXDATE')
               union  all
               (SELECT  DUE_NUM,AMT_INCUR FROM DW_SDATA.pcs_005_tb_sup_water_c
               WHERE ACC_TYP='20' AND BRW_LGO='C' and  sup_date='$TXDATE')
                )  tmp
  GROUP BY DUE_NUM;
create local temporary table tt_f_loan_indv_dubil_temp_5
on commit preserve rows as
select                                                                                    --主还款方式分期：01,02,03,04时(分期），逾期本金、逾期利息逻辑
      a1.due_num
      ,a2.overdue_amt                                                                     --逾期本金
      ,a2.overdue_rate                                                                    --逾期利息
from  dw_sdata.pcs_005_tb_sup_loan_info a1                                                --主档
left join
        (select
               due_num
               ,sum(rcv_prn-pad_up_prn) as  overdue_amt
               ,sum(rcv_dft_itr_in-pad_up_dft_itr_in+rcv_dft_itr_out-pad_up_dft_itr_out) as  overdue_rate
        from   dw_sdata.pcs_005_tb_sup_debt_info_n                                        --分期贷款明细登记表
        where  '$TXDATE'> end_date                                               --限定统计范围
        and     start_dt<='$TXDATE'::date
        and     end_dt>'$TXDATE'::date
        group by due_num
        )  a2
on      a1.due_num=a2.due_num
where   a1.prm_pay_typ in ('01','02','03','04')
and     a1.start_dt<='$TXDATE'::date
and     a1.end_dt>'$TXDATE'::date
union
select                                                                                    --主要还款方式非分期：非分期11，12，13，15 时，逾期本金、逾期利息逻辑
      b1.due_num
      ,(b2.nor_bal_01+b2.dvl_bal_02)        as overdue_amt                                --逾期本金
      ,(b2.in_dft_bal_07+b2.out_nor_bal_09) as overdue_rate                               --逾期利息
from  dw_sdata.pcs_005_tb_sup_loan_info b1                                                         --主档
left join dw_sdata.pcs_005_tb_sup_account_info  b2                                        --分户
on        b1.due_num=b2.due_num
and       b2.start_dt<='$TXDATE'::date
and       b2.end_dt>'$TXDATE'::date
left join dw_sdata.pcs_006_tb_lon_loan_duebill  b3                                         --贷款借据表
on        b1.due_num=b3.duebill_no
and       b3.start_dt<='$TXDATE'::date
and       b3.end_dt>'$TXDATE'::date
left join dw_sdata.pcs_006_tb_lon_loan          b4                                          --贷款台帐主信息表
on        b3.loan_id=b4.loan_id
and       b4.start_dt<='$TXDATE'::date
and       b4.end_dt>'$TXDATE'::date
where     b1.prm_pay_typ in ('11','12','13','15')
and       '$TXDATE'>b1.end_date                                                       --限定统计范围
and       b1.start_dt<='$TXDATE'::date
and       b1.end_dt>'$TXDATE'::date
union
select                                                                                    --主要还款方式非分期：非分期14 时，逾期本金、逾期利息逻辑
      c1.due_num
      ,c3.overdue_amt                                                                     --逾期本金
      ,(c2.in_dft_bal_07+c2.out_nor_bal_09) as overdue_rate                               --逾期利息
from  dw_sdata.pcs_005_tb_sup_loan_info  c1                                              --主档
left join dw_sdata.pcs_005_tb_sup_account_info  c2                                       --分户
on        c1.due_num=c2.due_num
and       c2.start_dt<='$TXDATE'::date
and       c2.end_dt>'$TXDATE'::date
left join
        (select
               due_num
               ,sum(prn_bal) as overdue_amt
        from   dw_sdata.pcs_005_tb_sup_prin_plan_a                                         --还款方式还本计划表
        where  '$TXDATE'> end_date                                                 --限定统计范围
        and    start_dt<='$TXDATE'::date
        and    end_dt>'$TXDATE'::date
        group by  due_num
        ) c3
on      c1.due_num =c3.due_num
where   c1.prm_pay_typ ='14'
and     c1.start_dt<='$TXDATE'::date
and     c1.end_dt>'$TXDATE'::date
;
create local temporary table tt_f_loan_indv_dubil_temp_6
on commit preserve rows as
select
      due_num                                                                              --借据号
      ,sum(amt_incur)as amt_incur                                                          --发生额
from  dw_sdata.pcs_005_tb_sup_water_c                                                     --历史流水表
where sup_date between '$MONTHBGNDAY' and '$TXDATE'
and   acc_typ='14'                                                                           --账户类型
--and   etl_dt='$TXDATE'::date
group by due_num
;
/*临时表创建区END*/

/*数据回退区*/
delete /* +direct */ from f_fdm.f_loan_indv_dubil
where  etl_date = '$TXDATE'
;
/*数据回退区END*/

/*数据处理区*/
insert /* +direct */  into f_fdm.f_loan_indv_dubil
      (grp_typ                                                                              --组别
      ,etl_date                                                                             --数据日期
      ,agmt_id                                                                              --协议编号
      ,cust_num                                                                             --客户号
      ,org_num                                                                              --机构号
      ,cur_cd                                                                               --货币代码
      ,prod_cd                                                                              --产品代码
      ,distr_dt                                                                             --放款日期
      ,st_int_dt                                                                            --起息日
      ,due_dt                                                                               --到期日
      ,payoff_dt                                                                            --结清日期
      ,wrtoff_dt                                                                            --核销日期
      ,loan_orgnl_amt                                                                       --贷款原始金额
      ,exec_int_rate                                                                        --执行利率
      ,bmk_int_rate                                                                         --基准利率
      ,flt_ratio                                                                            --浮动比例
      ,basis                                                                                --基差
      ,ovrd_int_rate                                                                        --逾期利率
      ,int_base_cd                                                                          --计息基础代码
      ,cmpd_int_calc_mode_cd                                                                --复利计算方式代码
      ,pre_chrg_int                                                                         --是否先收息
      ,int_rate_attr_cd                                                                     --利率属性代码
      ,int_rate_adj_mode_cd                                                                 --利率调整方式代码
      ,repay_mode_cd                                                                        --还款方式代码
      ,repay_prd_cd                                                                         --还款周期代码
      ,orgnl_term                                                                           --原始期限
      ,orgnl_term_corp_cd                                                                   --原始期限单位代码
      ,rprc_prd                                                                             --重定价周期    --字段长度小于实际数据
      ,rprc_prd_corp_cd                                                                     --重定价周期单位代码
      ,last_rprc_day                                                                        --上次重定价日
      ,next_rprc_day                                                                        --下次重定价日
      ,next_pay_amt                                                                         --下次付款金额
      ,last_pay_day                                                                         --上次付款日
      ,next_pay_day                                                                         --下次付款日
      ,four_cls_cls                                                                         --四级分类
      ,fiv_cls                                                                              --五级分类
      ,agmt_stat_cd                                                                         --协议状态代码
      ,contr_agmt_id                                                                        --合同协议编号
      ,asst_secuz_ind                                                                       --资产证券化标识
      ,prin_subj                                                                            --本金科目
      ,curr_bal                                                                             --当前余额
      ,norm_bal                                                                             --正常余额
      ,slug_bal                                                                             --呆滞余额
      ,bad_debt_bal                                                                         --呆账余额
      ,wrtoff_prin                                                                          --核销本金
      ,int_subj                                                                             --利息科目
      ,today_provs_int                                                                      --当日计提利息
      ,curmth_provs_int                                                                     --当月计提利息
      ,accm_provs_int                                                                       --累计计提利息
      ,today_chrg_int                                                                       --当日收息
      ,curmth_recvd_int                                                                     --当月已收息
      ,accm_recvd_int                                                                       --累计已收息
      ,int_adj_amt                                                                          --利息调整金额
      ,mth_accm                                                                             --月积数
      ,yr_accm                                                                              --年积数
      ,mth_day_avg_bal                                                                      --月日均余额
      ,yr_day_avg_bal                                                                       --年日均余额
      ,opr_org_num                                                                          --经办机构号
      ,opr_tellr_num                                                                        --经办柜员号
      ,free_int_ind                                                                         --免息标志
      ,free_int_prd                                                                         --免息周期
      ,expd_ind                                                                             --展期标志
      ,expd_due_dt                                                                          --展期到期日
      ,int_rate_typ_cd                                                                      --利率类型代码
      ,loan_typ                                                                             --贷款类型
      ,is_loan_sbsd_ind                                                                     --是否贴息标志
      ,is_farm_ind                                                                          --是否农户标志
      ,is_spec_loan                                                                         --是否特定贷款
      ,is_acrd_fin_rvn_farm_std                                                             --是否符合财税农户标准
      ,is_setup_inds_loan                                                                   --是否创业贷款
      ,spec_biz_typ                                                                         --特色业务类型
      ,ovrd_days                                                                            --逾期天数
      ,ovrd_prin                                                                            --逾期本金
      ,ovrd_int                                                                             --逾期利息
      ,adv_money_ind                                                                        --垫款标志
      ,adv_money_amt                                                                        --垫款金额
      ,adv_money_bal                                                                        --垫款余额
      ,loan_deval_prep_bal                                                                  --贷款减值准备余额
      ,loan_deval_prep_amt                                                                  --贷款减值准备发生额
      ,sys_src                                                                              --系统来源
      )
 select
       1                                                                                    as  grp_typ     --组别
       ,'$TXDATE'::date                                                            as  etl_date    --数据日期
       ,t.due_num                                                                           as  agmt_id     --协议编号
       ,coalesce(t3.cus_no,'')                                                              as  cust_num    --客户号
       ,t.opn_dep                                                                           as  org_num     --机构号
       ,coalesce(t40.Tgt_Cd,'')                                                             as  cur_cd      --货币代码
       ,coalesce(t2.app_op_id,'')                                                           as  prod_cd     --产品代码
       ,to_date(t.beg_date,'yyyymmdd')                                                      as  distr_dt    --放款日期
       ,to_date(t.beg_itr_date,'yyyymmdd')                                                  as  st_int_dt   --起息日
       ,to_date(t.end_date,'yyyymmdd')                                                      as  due_dt      --到期日
       ,coalesce(t1.close_date,'$MINDATE' :: date )                                     as  payoff_dt   --结清日期
       ,coalesce(t1.cancel_date,'$MINDATE' :: date )                                    as  wrtoff_dt   --核销日期
       ,coalesce(t2.loan_contract_amount,0)                                                 as  loan_orgnl_amt  --贷款原始金额
       ,t.nor_itr_rate                                                                      as  exec_int_rate --执行利率
       ,coalesce(t5.base_rate,0)                                                            as  bmk_int_rate --基准利率
       ,coalesce(t2.floating_ratio,0)                                                       as  flt_ratio    --浮动比例
       ,coalesce(t5.base_rate,0)-t.nor_itr_rate                                             as  basis        --基差
       ,t.del_itr_rate                                                                      as  ovrd_int_rate --逾期利率
       ,coalesce(case 
                     when  t.prm_pay_typ in ('01','02','03','04') and t.itr_rate_way='1' 
                     then '10'   --30/360
                     when  t.prm_pay_typ in ('01','02','03','04') and t.itr_rate_way='2' 
                     then '11' --30/365  
                     when  t.prm_pay_typ not in ('01','02','03','04') and t.itr_rate_way='1' 
                     then '1' --实际/360   
                     when  t.prm_pay_typ not in ('01','02','03','04') and t.itr_rate_way='2' 
                     then '4' --实际/365  
                     end,'')                                                                as  int_base_cd  --计息基础代码
       ,'1'                                                                                 as  cmpd_int_calc_mode_cd   --复利计算方式代码
       ,coalesce(case 
                     when t.prm_pay_typ||t.ast_pay_typ ='0110' 
                     then '1' 
                     else '0'
                     end
                 ,'')                                                                      as  pre_chrg_int    --是否先收息
       ,coalesce(case 
                     when  t2.rate_adjust_kind in ('1','3','5') 
                     then  '3'--定期
                     when  t2.rate_adjust_kind='9' 
                     then  '1'--固定 
                     else  '4'--不定期
                     end,'')                                                              as  int_rate_attr_cd --利率属性代码 
       ,coalesce(t50.Tgt_Cd,'')                                                           as  int_rate_adj_mode_cd --利率调整方式代码
       ,coalesce(t60.Tgt_Cd,'')                                                           as  repay_mode_cd    --还款方式代码
       ,t.caspan                                                                          as  repay_prd_cd    --还款周期代码
       ,coalesce(cast(t4.loan_length as numeric(24,0)),0)                                 as  orgnl_term       --原始期限
       ,'M'                                                                               as orgnl_term_corp_cd    --原始期限单位代码
       ,(case
         when t2.rate_adjust_kind='1'
         then 1
         when t2.rate_adjust_kind='2'
         then
              (case
               when  t.caspan=1
               then  1
               when  t.caspan=2
               then  3
               when  t.caspan=3
               then  1
               when  t.caspan=6
               then  6
               when  t.caspan=7
               then  15
               when  t.caspan=8
               then  7
               when  t.caspan=9
               then  14
               else  -1
               --'@'||t.caspan   20160822
               end
               )
         when  t2.rate_adjust_kind='3'
         then  1
         when  t2.rate_adjust_kind='4'
         then  0  --modify at 20160822
         when  t2.rate_adjust_kind='5'
         then  3
         else (case when  t.end_date is null
               then  0  --modify at 20160822
               else  to_date(t.end_date,'yyyymmdd')-to_date(t.beg_itr_date,'yyyymmdd') end )
         end
         )                                                                                   as rprc_prd      --重定价周期
       ,(case
         when t2.rate_adjust_kind='1'
         then 'Y'
         when t2.rate_adjust_kind='2'
         then
             (case
              when  t.caspan=1
              then  'M'
              when  t.caspan=2
              then  'M'
              when  t.caspan=3
              then  'Y'
              when  t.caspan=6
              then  'M'
              when  t.caspan=7
              then  'D'
              when  t.caspan=8
              then  'D'
              when  t.caspan=9
              then  'D'
              else  ''
              end
              )
         when t2.rate_adjust_kind='3'
         then 'Y'
         when t2.rate_adjust_kind='4'
         then ''  -- modify at 20160822
         when t2.rate_adjust_kind='5'
         then 'M'
         else (case when t.end_date is null
               then '' --modify at 20160822
               else 'D' end )
         end
         )                                                                                   as  rprc_prd_corp_cd    --重定价周期单位代码
       ,(case
         when t6.due_num is not null
         then to_date(t6.itr_date,'yyyymmdd')
         else to_date(t.beg_itr_date,'yyyymmdd')
         end
         )                                                                                   as  last_rprc_day        --上次重定价日
       ,case
       when (t11.dvl_bal_02>0  or t1.overdue_days>0)
       then '$MAXDATE'::date
       when t.sts in ('7','9')
       then to_date(t.end_date,'yyyymmdd')
       else (case
                 when t2.rate_adjust_kind='1'  and add_months(to_date(case
                                                                          when t6.due_num is not null
                                                                          then t6.itr_date
                                                                          else t.beg_itr_date
                                                                          end,'yyyymmdd'),12) < to_date(t.end_date,'yyyymmdd')
                 then add_months(to_date(case
                                             when t6.due_num is not null
                                             then t6.itr_date
                                             else t.beg_itr_date
                                             end,'yyyymmdd'),12)
                when t2.rate_adjust_kind='2'  and  to_date(t.next_prov_date,'yyyymmdd')<to_date(t.end_date,'yyyymmdd')
                then to_date(t.next_prov_date,'yyyymmdd')
                when t2.rate_adjust_kind='3'  and to_date(year('$TXDATE'::date)+1||'0101','yyyymmdd')<to_date(t.end_date,'yyyymmdd')
                then to_date(year('$TXDATE'::date)+1||'0101','yyyymmdd')
                when t2.rate_adjust_kind='4' and '$TXDATE'::date+1<to_date(t.end_date,'yyyymmdd')
                then '$TXDATE'::date+1
                when t2.rate_adjust_kind='5'  and add_months(to_date(case
                                                                         when t6.due_num is not null
                                                                         then t6.itr_date
                                                                         else t.beg_itr_date
                                                                         end,'yyyymmdd'),3) < to_date(t.end_date,'yyyymmdd')
                then add_months(to_date(case
                                            when t6.due_num is not null
                                            then t6.itr_date
                                            else t.beg_itr_date
                                            end,'yyyymmdd'),3)
                else to_date(t.end_date,'yyyymmdd')
                end)
       end                                                                                    as  next_rprc_day --下次重定价日
       ,case
        when t.prm_pay_typ||t.ast_pay_typ = '0100'
        then coalesce(t7.curr_prj_prn ,0)                                                                 --本期推算本金
        when t.prm_pay_typ||t.ast_pay_typ in ('0200','0210','0220','0230')
        then coalesce(t7.curr_prj_itr,0)+coalesce(t7.curr_prj_prn,0)                                      --(本期推算利息+本期推算本金)
        else 0
        end                                                                                   as  next_pay_amt   --下次付款金额
       ,coalesce(to_date(t8.rcv_date,'yyyymmdd') ,'$MINDATE'::date)                                 as  last_pay_day   --上次付款日 --修改最小日期变量by liudongyan
       ,to_date(t.next_prov_date,'yyyymmdd')                                                  as  next_pay_day   --下次付款日
       ,coalesce(t2.loan_level_four_class,'')                                                 as  four_cls_cls   --四级分类
       ,coalesce(t2.loan_level_five_class ,'')                                                as  fiv_cls        --五级分类
       ,coalesce(t70.Tgt_Cd ,'')                                                              as  agmt_stat_cd   --协议状态代码
       ,t.con_no                                                                              as  contr_agmt_id  --合同协议编号
       ,(case
         when t9.duebill_no is not  null
         then '1'
         else '0'
         end
         )                                                                                    as  contr_agmt_id   --合同协议编号
       ,coalesce(t10.itm_no,'')                                                               as  asst_secuz_ind  --资产证券化标识
       ,coalesce(t10.bal,0)                                                                   as  prin_subj       --本金科目
       ,coalesce(t11.nor_bal_01,0)                                                            as  curr_bal        --当前余额
       ,coalesce(t1.duebill_balance_dull,0)                                                   as  norm_bal        --正常余额
       ,coalesce(t1.duebill_balance_bad ,0)                                                   as  slug_bal        --呆滞余额
       ,coalesce(t11.oft_prn_bal_12,0)                                                        as  wrtoff_prin     --核销本金
       ,coalesce(t12.itm_no,'')                                                               as  int_subj        --利息科目
       ,coalesce(t13.amt_incur,0)                                                             as  today_provs_int --当日计提利息
       ,coalesce(t20.amt_incur,0)                                                             as  curmth_provs_int --当月计提利息
       ,0                                                                                     as  accm_provs_int  --累计计提利息
       ,coalesce(t14.amt,0)                                                                   as  today_chrg_int   --当日收息
       ,coalesce(t15.amt,0)                                                                   as  curmth_recvd_int  --当月已收息
       ,0                                                                                     as  accm_recvd_int    --累计已收息
       ,coalesce(t11.nor_itr_adj_bal_03,0)                                                    as  int_adj_amt       --待映射  --利息调整金额
       ,0                                                                                  as  mth_accm          --待映射  --月积数
       ,0                                                                                  as  yr_accm           --待映射  --年积数
       ,0                                                                                  as  mth_day_avg_bal   --待映射  --月日均余额
       ,0                                                                                  as  yr_day_avg_bal    --待映射  --年日均余额
       ,coalesce(t17.cur_org_id,'')                                                           as  opr_org_num       --经办机构号
       ,coalesce(t17.own_user_id ,'')                                                         as  opr_tellr_num     --经办柜员号
       ,t.itr_fre_flg                                                                         as  free_int_ind      --免息标志
       ,coalesce(t.itr_fre_cyl,0)                                                             as  free_int_prd      --免息周期
       ,case 
            when t2.extension_flag in ('0','1') 
            then '0' 
            else '1' 
            end                                                                               as  expd_ind          --展期标志
       ,coalesce(t2.extension_date,'$MINDATE' :: date )                                                as  expd_due_dt       --展期到期日
       ,case
        when t2.app_op_id='e821' and t2.province_num='54'
        then '3'                                                                                                 --西藏公积金贷款
        when t2.app_op_id='e821' and t2.province_num<>'54'
        then '2'                                                                                                 --公积金贷款
        when t2.app_op_id<>'e821' and t2.province_num='54'
        then '4'                                                                                                 --西藏普通人民币贷款
        when t2.app_op_id<>'e821' and t2.province_num<>'54'
        then '1'                                                                                                 --普通人民币贷款
        end                                                                                                as  int_rate_typ_cd    --利率类型代码
       ,coalesce(t2.loan_type,'')                                                                          as  loan_typ           --贷款类型
       ,coalesce(t2.is_discount ,'')                                                                       as  is_loan_sbsd_ind   --是否贴息标志
       ,coalesce(t2.is_farmer ,'')                                                                         as  is_farm_ind        --是否农户标志
       ,coalesce(t2.is_design_loan,'')                                                                     as  is_spec_loan       --是否特定贷款
       ,coalesce(t2.is_fit_farmer_stand,'')                                                                as  is_acrd_fin_rvn_farm_std  --是否符合财税农户标准
       ,coalesce(t2.is_carve_out_loan,'')                                                                  as  is_setup_inds_loan --是否创业贷款
       ,coalesce(t2.characteristic_app_type,'')                                                            as  spec_biz_typ       --特色业务类型
       ,coalesce(t1.overdue_days,0)                                                                        as  ovrd_days          --逾期天数
       ,coalesce(t18.overdue_amt,0)                                                                        as  ovrd_prin          --逾期本金
       ,coalesce(t18.overdue_rate,0)                                                                       as  ovrd_int           --逾期利息
       ,coalesce(t2.advance_flag,'')                                                                       as  adv_money_ind      --垫款标志
       ,coalesce(t2.advance_amount,0)                                                                      as  adv_money_amt      --垫款金额
       ,coalesce(t2.advance_balance,0)                                                                     as  adv_money_bal      --垫款余额
       ,coalesce(t11.ipr_pvs_bal_14,0)                                                                     as  loan_deval_prep_bal --贷款减值准备余额
       ,coalesce(t19.amt_incur ,0)                                                                         as  loan_deval_prep_amt --贷款减值准备发生额
       ,'PCS'                                                                                as  sys_src            --系统来源
 from  dw_sdata.pcs_005_tb_sup_loan_info                t                                 --主档表
 left join dw_sdata.pcs_006_tb_lon_loan_duebill        t1                                --借据
 on        t.due_num=t1.duebill_no
 and t1.duebill_status<>'4'
 and       t1.start_dt<='$TXDATE'::date
 and       t1.end_dt>'$TXDATE'::date
 left join dw_sdata.pcs_006_tb_lon_loan                t2                                --台账
 on        t1.loan_id=t2.loan_id
 and       t2.start_dt<='$TXDATE'::date
 and       t2.end_dt>'$TXDATE'::date
  left join dw_sdata.pcs_006_tb_csm_customer            t3                                --客户总表
 on        t2.cus_id=t3.cus_id
 and       t3.start_dt<='$TXDATE'::date
 and       t3.end_dt>'$TXDATE'::date
 left join (select * from
               (
                select loan_id,LOAN_CONTRACT_AMOUNT,LOAN_LENGTH
                       ,row_number()over(partition by loan_id order by update_time desc ) rn 
                 from  dw_sdata.pcs_006_tb_lon_loan_contract  
                where start_dt<='$TXDATE'::date and '$TXDATE'::date<end_dt
             )q  where rn = 1    
            )T4
 ON T1.LOAN_ID=T4.LOAN_ID 
 left join tt_f_loan_indv_dubil_temp_1                     t5
 on        t1.loan_id=t5.loan_id
 left join tt_f_loan_indv_dubil_temp_2                     t6                                --贷款利率调整表
 on        t.due_num=t6.due_num                                                          --借据号关联
 left join dw_sdata.pcs_005_tb_sup_repayment_plan      t7                                --分期贷款还款计划表
 on        t.due_num=t7.due_num
 and       t7.start_dt<='$TXDATE'::date
 and       t7.end_dt>'$TXDATE'::date
 left join tt_f_loan_indv_dubil_temp_3                     t8                                --还款登记簿
 on        t.due_num=t8.due_num
 left join dw_sdata.pcs_006_tb_abs_loan_info           t9
 on        t.due_num=t9.duebill_no
 and       t9.is_abs ='1'
 and       t9.start_dt<='$TXDATE'::date
 and       t9.end_dt>'$TXDATE'::date
 left join dw_sdata.acc_003_t_acc_assets_ledger        t10                                 --资产类客户账户分户账
 on        t.due_num=t10.acc
 and       t10.sys_code ='99340000000'
 and       t10.start_dt<='$TXDATE'::date
 and       t10.end_dt>'$TXDATE'::date
 left join dw_sdata.pcs_005_tb_sup_account_info        t11                                 --分户账
 on        t.due_num=t11.due_num
 and       t11.start_dt<='$TXDATE'::date
 and       t11.end_dt>'$TXDATE'::date
 left join dw_sdata.acc_003_t_accdata_last_item_no     t12                                  --科目转换对照表
 on        t10.itm_no= t12.amt_itm
 and       t12.first_itm='18'
 and       t12.start_dt<='$TXDATE'::date
 and       t12.end_dt>'$TXDATE'::date
 left join tt_f_loan_indv_dubil_temp_4                     t13
 on        t.due_num=t13.due_num
 left join
          (select
                 due_num
                 ,sum(pad_up_nor_itr_in+pad_up_dft_itr_in
                 +pad_up_pns_itr_in +pad_up_nor_itr_out+pad_up_dft_itr_out
                 +pad_up_pns_itr_out ) as amt
          from   dw_sdata.pcs_005_tb_sup_repayment_info
          where  rcv_date ='$TXDATE'
          group by due_num
          )  t14                                  --还款登记簿
 on       t.due_num=t14.due_num
 left join
          (select
                 due_num
                 ,sum(pad_up_nor_itr_in+pad_up_dft_itr_in+pad_up_pns_itr_in +pad_up_nor_itr_out+pad_up_dft_itr_out +pad_up_pns_itr_out) as amt
          from   dw_sdata.pcs_005_tb_sup_repayment_info
          where  month(rcv_date) =month('$TXDATE'::date)
          and rcv_date <='$TXDATE'
          group by due_num
          )  t15
 on t.due_num=t15.due_num
-- left join
--          (select
--                 due_num
--                 ,sum(pad_up_nor_itr_in+pad_up_dft_itr_in+pad_up_pns_itr_in +pad_up_nor_itr_out+pad_up_dft_itr_out) as amt
--          from   dw_sdata.pcs_005_tb_sup_repayment_info
--          where  etl_dt<='$TXDATE'::date
--          group by due_num
--          )  t16
-- on t.due_num=t16.due_num
 left join dw_sdata.pcs_006_tb_lon_org_manage          t17                               --贷款业务机构管理表
 on        t1.loan_id=t17.loan_id
 and       t17.start_dt<='$TXDATE'::date
 and       t17.end_dt>'$TXDATE'::date
 left join tt_f_loan_indv_dubil_temp_5                     t18
 on        t.due_num=t18.due_num
 left join tt_f_loan_indv_dubil_temp_6                     t19
 on        t.due_num=t19.due_num
 left join
        (SELECT DUE_NUM,sum(AMT_INCUR) as AMT_INCUR FROM (
              (SELECT  DUE_NUM,AMT_INCUR FROM DW_SDATA.pcs_005_tb_sup_water_c   --历史流水表 账户类型(表内）
               WHERE ACC_TYP='05' AND BRW_LGO='D' and  month(sup_date::date) =month('$TXDATE'::date)
                and  sup_date<='$TXDATE')
               union  all
               (SELECT  DUE_NUM,AMT_INCUR FROM DW_SDATA.pcs_005_tb_sup_water_c  --历史流水表账户类型(表外）
               WHERE ACC_TYP='20' AND BRW_LGO='C' and  month(sup_date::date) =month('$TXDATE'::date)
               and  sup_date<='$TXDATE' )
                )  tmp
  GROUP BY DUE_NUM ) t20
 on      t.due_num=t20.due_num
 --left join
 --        (SELECT DUE_NUM,sum(AMT_INCUR) as AMT_INCUR FROM (
 --             (SELECT  DUE_NUM,AMT_INCUR FROM DW_SDATA.pcs_005_tb_sup_water_c   --历史流水表 账户类型(表内）
 --              WHERE ACC_TYP='05' AND BRW_LGO='D' and  etl_dt<='$TXDATE'::date)
 --              union  all
 --              (SELECT  DUE_NUM,AMT_INCUR FROM DW_SDATA.pcs_005_tb_sup_water_c  --历史流水表账户类型(表外）
 --              WHERE ACC_TYP='20' AND BRW_LGO='C' and  etl_dt<='$TXDATE'::date)
 --               )  tmp
 -- GROUP BY DUE_NUM ) t21
 --on      t.due_num=t21.due_num
  LEFT JOIN  F_FDM.CD_RF_STD_CD_TRAN_REF T40 
     ON  T.CURR_COD=T40.SRC_CD                      
    AND  T40.DATA_PLTF_SRC_TAB_NM = 'PCS_005_TB_SUP_LOAN_INFO' 
    AND  T40.Data_Pltf_Src_Fld_Nm ='CURR_COD'
 LEFT JOIN  F_FDM.CD_RF_STD_CD_TRAN_REF T50
     ON  T2.RATE_ADJUST_KIND=T50.SRC_CD                      
    AND  T50.DATA_PLTF_SRC_TAB_NM = 'PCS_006_TB_LON_LOAN'
    AND  T50.Data_Pltf_Src_Fld_Nm ='RATE_ADJUST_KIND'
LEFT JOIN  F_FDM.CD_RF_STD_CD_TRAN_REF T60 
     ON  T2.REPAY_KIND=T60.SRC_CD                      
    AND  T60.DATA_PLTF_SRC_TAB_NM = 'PCS_006_TB_LON_LOAN'
    AND  T60.Data_Pltf_Src_Fld_Nm ='REPAY_KIND'   
LEFT JOIN  F_FDM.CD_RF_STD_CD_TRAN_REF T70 
     ON  T.STS=T70.SRC_CD                      
    AND  T70.DATA_PLTF_SRC_TAB_NM = 'PCS_005_TB_SUP_LOAN_INFO'
    AND  T70.Data_Pltf_Src_Fld_Nm ='STS'   
 where     t.start_dt<='$TXDATE'::date
 and       t.end_dt>'$TXDATE'::date
 ;
 /*月积数、年积数、月日均余额、年日均余额临时表创建区*/
 
create local temporary table tt_f_loan_indv_dubil_yjs
on commit preserve rows as
select 
      t.agmt_id
      ,(case 
            when '$TXDATE'= '$MONTHBGNDAY' 
            then t.curr_bal
            else t.curr_bal+coalesce(t1.mth_accm,0)
            end
       )                                                                      as mth_accm  --月积数
      ,(case 
            when  '$TXDATE' = '$YEARBGNDAY' 
            then t.curr_bal
            else t.curr_bal+coalesce(t1.yr_accm,0)
            end
       )                                                                      as yr_accm   --年积数
      ,(case 
            when '$TXDATE' = '$MONTHBGNDAY' 
            then t.curr_bal
            else t.curr_bal+coalesce(t1.mth_accm,0)
            end
       )/('$TXDATE'::date-'$MONTHBGNDAY'::date+1)               as mth_day_avg_bal  --月日均余额
      ,(case 
           when '$TXDATE' = '$YEARBGNDAY' 
           then t.curr_bal
           else t.curr_bal+coalesce(t1.yr_accm,0)
           end
       )/('$TXDATE'::date-'$YEARBGNDAY'::date+1)                    as Yr_Day_Avg_Bal   --年日均余额
from  f_fdm.f_loan_indv_dubil     t
left join f_fdm.f_loan_indv_dubil t1
on        t.agmt_Id=t1.agmt_Id
and       t1.etl_date='$TXDATE'::date-1
where     t.etl_date='$TXDATE'::date
;
/*月积数、年积数、月日均余额、年日均余额临时表创建区END*/
/*更新月积数、年积数、月日均余额、年日均余额*/
update f_fdm.f_loan_indv_dubil t
set mth_accm=t1.mth_accm 
,yr_accm=t1.yr_accm
,mth_day_avg_bal=t1.mth_day_avg_bal
,Yr_Day_Avg_Bal=t1.Yr_Day_Avg_Bal
from tt_f_loan_indv_dubil_yjs t1
where  t.agmt_Id=t1.agmt_Id
and t.etl_date='$TXDATE'::date
;
/*数据处理区END*/


 COMMIT;

