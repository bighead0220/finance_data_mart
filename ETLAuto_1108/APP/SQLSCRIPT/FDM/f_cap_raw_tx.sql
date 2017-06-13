/*
Author             :魏银辉
Function           :资金衍生交易信息表
Load method        :INSERT
Source table       :cos_000_swdeals T1
                   :cd_cd_table T2
                   :ecf_002_t01_cust_info_T T3
                   :cos_000_cflows Tmp_T4
Destination Table  :f_Cap_Raw_TX  资金衍生交易信息表
Frequency          :D
Modify history list:Created by 魏银辉
                   :
 modified by wyh 20160912 修改机构代码，交易对手客户号
 modified by zhangliang 20161009 修改t5,变更223
----------------------------------问题说明--------------------------
1.代码表没有数据，（FDM085）
    inner join的逻辑是否可靠   --可靠，需筛选产品范围
2.cos_000_cflows这张表原先是 流水表，改成拉链表
3.cos_000_gl_entry 是一张流水表，但是从这里面累计金额，要去掉ETL日期限制

-------------------------------------------逻辑说明---------------------------------------------
业务逻辑说明
-------------------------------------------逻辑说明END------------------------------------------
*/
/*临时表创建区*/

DROP TABLE IF EXISTS f_Cap_Raw_TX_tmp_t5 cascade;

create local temporary table if not exists f_Cap_Raw_TX_tmp_t5             --本金科目、利息科目、投资收益科目及对应的金额
on commit preserve rows as
select
                deal_no
                --927015001/927005
                ,max(case when map_code like '927015001%' or map_code like '927005%'
                          then map_code end)    as p_prin_acc  --本金科目
                ,sum(case when map_code like '927015001%' or map_code like '927005%'
                          then amount else 0 end)                    as p_prin_amount --名义本金
                --927015002(货币掉期类业务)
                ,max(case when map_code like '927015002%' then map_code end)  as r_prin_acc  --对方本金科目
                ,sum(case when map_code like '927015002%' then amount else 0 end)                  as r_prin_amount --对方名义本金
                --2530:应付衍生工具利息
                ,max(case when map_code like '2530%' then map_code end)    as p_accr_acc   --应付利息科目
                ,sum(case when map_code like '2530%' and gl_date='$TXDATE'::date 
                          then amount else 0 end)                    as p_accr_amount_d --当日应付利息
                ,sum(case when map_code like '2530%' and to_char(gl_date,'yyyymm')=to_char('$TXDATE'::date,'yyyymm')
                          then amount else 0 end)                    as p_accr_amount_m --当月应付利息
                ,sum(case when map_code like '2530%'
                          then amount else 0 end)                    as p_accr_amount_t --累计应付利息
                 --1380:应收衍生工具利息
                ,max(case when map_code like '1380%' then map_code end)    as r_accr_acc   --应收利息科目
                ,sum(case when map_code like '1380%' and gl_date='$TXDATE'::date 
                          then amount else 0 end)                    as r_accr_amount_d --当日应收利息
                ,sum(case when map_code like '1380%' and to_char(gl_date,'yyyymm')=to_char('$TXDATE'::date,'yyyymm')
                          then amount else 0 end)                    as r_accr_amount_m --当月应收利息
                ,sum(case when map_code like '1380%' 
                          then amount else 0 end)                    as r_accr_amount_t --累计应收利息          
                --5705:投资收益
                ,max(case when map_code like '5705%' then map_code end)    as inv_acc   --买卖价差收益科目
                ,sum(case when map_code like '5705%' and gl_date='$TXDATE'::date 
                          then amount else 0 end)                    as inv_acc_d --当日买卖价差金额
                ,sum(case when map_code like '5705%' and to_char(gl_date,'yyyymm')=to_char('$TXDATE'::date,'yyyymm')
                          then amount else 0 end)                    as inv_acc_m --当月买卖价差金额
                ,sum(case when map_code like '5705%'
                          then amount else 0 end)                    as inv_acc_t --累计买卖价差金额
      from      dw_sdata.cos_000_qta_gl_accounting --账务信息
     where     ret_code='000000'  --会计处理平台处理成功
       and     gl_date<='$TXDATE'::date
    group by   deal_no
;
/*临时表创建区END*/
/*数据回退区*/
DELETE/* +direct */ from f_fdm.f_Cap_Raw_TX
where etl_date='$TXDATE'::date
;
/*数据回退区END*/
/*数据处理区*/
INSERT INTO f_fdm.f_Cap_Raw_TX
(
        grp_typ                             --组别
        ,etl_date                           --数据日期
        ,agmt_id                            --协议编号
        ,cust_acct_num                      --客户账号
        ,cust_num                           --客户号
        ,org_num                            --机构号
        ,prod_cd                            --产品代码
        ,cur_cd                             --货币代码
        ,cnt_pty_cur_cd                     --对方货币代码
        ,cap_tx_inves_comb_cd               --资金交易投资组合代码
        ,st_int_dt                          --起息日
        ,due_dt                             --到期日
        ,stl_dt                             --结算日期
        ,int_base_cd                        --计息基础代码
        ,cnt_pty_int_base_cd                --对方计息基础代码
        ,cmpd_int_calc_mode_cd              --复利计算方式代码
        ,Pre_Chrg_Int                       --是否先收息
        ,int_pay_freq                       --付息频率
        ,int_rate_attr_cd                   --利率属性代码
        ,cnt_pty_int_rate_attr_cd           --对方利率属性代码
        ,orgnl_term                         --原始期限
        ,orgnl_term_corp_cd                 --原始期限单位代码
        ,exchg_rate                         --汇率
        ,curr_int_rate                      --当前利率
        ,cnt_pty_curr_int_rate              --对方当前利率
        ,bmk_int_rate                       --基准利率
        ,Basis                              --基差
        ,prin_subj                          --本金科目
        ,notnl_prin                         --名义本金
        ,cnt_pty_prin_subj                  --对方本金科目
        ,cnt_pty_notnl_prin                 --对方名义本金
        ,Net_Val                            --净现值
        ,curr_val                           --当前价值
        ,valtn_prft_loss_subj               --估值损益科目
        ,today_valtn_prft_loss_amt          --当日估值损益金额
        ,curmth_valtn_prft_loss_amt         --当月估值损益金额
        ,paybl_int_subj                     --应付利息科目
        ,today_paybl_int                    --当日应付利息
        ,curmth_paybl_int                   --当月应付利息
        ,accm_paybl_int                     --累计应付利息
        ,recvbl_int_subj                    --应收利息科目
        ,today_recvbl_int                   --当日应收利息
        ,curmth_recvbl_int                  --当月应收利息
        ,accm_recvbl_int                    --累计应收利息
        ,biz_prc_diff_prft_subj             --买卖价差收益科目
        ,today_biz_prc_diff_amt             --当日买卖价差金额
        ,curmth_biz_prc_diff_amt            --当月买卖价差金额
        ,accm_biz_prc_diff_amt              --累计买卖价差金额
        ,mth_accm                           --月积数
        ,yr_accm                            --年积数
        ,mth_day_avg_bal                    --月日均余额
        ,yr_day_avg_bal                     --年日均余额
        ,sys_src                            --系统来源
)
SELECT
        '1'                                 AS grp_typ
        ,'$TXDATE'::date           AS etl_date
        ,T.DEAL_NO                          AS agmt_id
        ,coalesce(T4.BANK_ACC,'')                        AS cust_acct_num
        ,coalesce(T3.ecif_cust_no,'@'||t.cparty)                    AS cust_num
        ,coalesce(T_org_2.name,'')                         AS org_num
        ,coalesce(T2.Cd,'@'||T.sectype)             AS prod_cd
        ,coalesce(T_CD08.TGT_CD,'@'||T.ccy)         AS cur_cd
        ,coalesce(T_CD09.TGT_CD,'@'||T.ccy2)        AS cnt_pty_cur_cd
        ,T.entity													          AS cap_tx_inves_comb_cd
        ,to_date(T.settle_dt,'YYYYMMDD')    AS st_int_dt
        ,to_date(T.cur_mat_dt,'YYYYMMDD')   AS due_dt
        ,to_date(T.cur_mat_dt,'YYYYMMDD')   AS stl_dt
        ,coalesce(T_CD14.TGT_CD,'@'||T1.p_int_days) AS int_base_cd
        ,coalesce(T_CD15.TGT_CD,'@'||T1.r_int_days) AS cnt_pty_int_base_cd
        ,'1'                                AS cmpd_int_calc_mode_cd             --单利
        ,'0'                                AS Pre_Chrg_Int
        ,coalesce(T_CD18.TGT_CD,'@'||T1.P_FREQ)     AS int_pay_freq
        ,(case
                when T1.p_fixfloat = 'FIXED' THEN '1'
                WHEN T1.p_fixfloat = 'FLOATING'  THEN '4'  --按不定期浮动利率
                else ''
         end
         )                                  AS int_rate_attr_cd
        ,(case
                when T1.r_fixfloat = 'FIXED' THEN '1'
                WHEN T1.r_fixfloat = 'FLOATING'  THEN '4'  --按不定期浮动利率
                else ''
         end
         )                                  AS cnt_pty_int_rate_attr_cd
        ,to_date(T.mature_dt,'YYYYMMDD') - to_date(T.settle_dt,'YYYYMMDD')      AS orgnl_term
        ,'D'                                AS orgnl_term_corp_cd
        ,(case
                when T1.cross_ccy='Y' then T1.exch_rate
                else 0
         end
         )                                  AS exchg_rate   --货币掉期业务有值
        ,coalesce(T1.p_rate,0)                          AS curr_int_rate
        ,coalesce(T1.r_rate,0)                          AS cnt_pty_curr_int_rate
        ,'0'                                AS bmk_int_rate
        ,'0'                                AS Basis
        ,coalesce(T5.p_prin_acc,'')                      AS prin_subj
        ,coalesce(abs(T5.p_prin_amount),0)              AS notnl_prin
        ,coalesce(T5.r_prin_acc,'')                      AS cnt_pty_prin_subj
        ,coalesce(abs(T5.r_prin_amount),0)              AS cnt_pty_notnl_prin
        ,'0'                                AS Net_Val
        ,'0'                                AS curr_val
        ,''                                 AS valtn_prft_loss_subj
        ,'0'                                AS today_valtn_prft_loss_amt
        ,'0'                                AS curmth_valtn_prft_loss_amt
        ,coalesce(T5.p_accr_acc,'')                      AS paybl_int_subj
        ,coalesce(abs(T5.p_accr_amount_d),0)            AS today_paybl_int
        ,coalesce(abs(T5.p_accr_amount_m),0)            AS curmth_paybl_int
        ,coalesce(abs(T5.p_accr_amount_t),0)            AS accm_paybl_int
        ,coalesce(T5.r_accr_acc,'')                      AS recvbl_int_subj
        ,coalesce(abs(T5.r_accr_amount_d),0)            AS today_recvbl_int
        ,coalesce(abs(T5.r_accr_amount_m),0)            AS curmth_recvbl_int
        ,coalesce(abs(T5.r_accr_amount_t),0)            AS accm_recvbl_int
        ,coalesce(T5.inv_acc,'')                         AS biz_prc_diff_prft_subj
        ,coalesce(abs(T5.inv_acc_d),0)                  AS today_biz_prc_diff_amt
        ,coalesce(abs(T5.inv_acc_m),0)                  AS curmth_biz_prc_diff_amt
        ,coalesce(abs(T5.inv_acc_t),0)                  AS accm_biz_prc_diff_amt
        ,0.00                               AS mth_accm
        ,0.00                               AS yr_accm
        ,0.00                               AS mth_day_avg_bal
        ,0.00                               AS yr_day_avg_bal
        ,'COS'                              AS sys_src
from    dw_sdata.cos_000_deals T --交易信息主表
INNER JOIN dw_sdata.cos_000_swdeals T1 --掉期交易
ON      T1.DEAL_NO = T.DEAL_NO
and     T1.start_dt <= '$TXDATE'::date
and     T1.end_dt > '$TXDATE'::date
INNER JOIN f_fdm.cd_cd_table T2 --代码表（财务数据集市基础层）
ON      T.sectype = T2.Cd
and     T2.Cd_Typ_Encd='FDM085' --资金衍生产品代码
LEFT JOIN dw_sdata.ecf_002_t01_cust_info_T T3   --同业客户基本信息
ON      T.cparty=T3.trans_emt_no --(交易对手编号)
AND     T3.start_dt <= '$TXDATE'::date
and     T3.end_dt > '$TXDATE'::date
LEFT JOIN (select
                    distinct DEAL_NO
                    ,bank_acc
           from dw_sdata.cos_000_cflows
           where (DEAL_NO,FLOW_NO) in (
                                        SELECT
                                                DEAL_NO
                                                ,MAX(FLOW_NO)
                                        FROM dw_sdata.cos_000_cflows
                                        where start_dt <= '$TXDATE'::date
                                        and end_dt > '$TXDATE'::date
                                        GROUP BY DEAL_NO
                                      )
           and start_dt <= '$TXDATE'::date
           and end_dt > '$TXDATE'::date
           )    T4
ON         T1.DEAL_NO=T4.DEAL_NO
LEFT JOIN f_Cap_Raw_TX_tmp_t5 T5
ON  T.DEAL_NO = T5.DEAL_NO
LEFT JOIN f_fdm.CD_RF_STD_CD_TRAN_REF T_CD08
ON      T.ccy = T_CD08.SRC_CD                       --源代码值相同
AND     T_CD08.DATA_PLTF_SRC_TAB_NM = upper('cos_000_deals') --数据平台源表主干名
AND     T_CD08.Data_Pltf_Src_Fld_Nm =upper('ccy')
LEFT JOIN f_fdm.CD_RF_STD_CD_TRAN_REF T_CD09
ON      T.ccy2 = T_CD09.SRC_CD                       --源代码值相同
AND     T_CD09.DATA_PLTF_SRC_TAB_NM =upper('cos_000_deals') --数据平台源表主干名
AND     T_CD09.Data_Pltf_Src_Fld_Nm =upper('ccy2')
LEFT JOIN f_fdm.CD_RF_STD_CD_TRAN_REF T_CD14
ON      T1.p_int_days = T_CD14.SRC_CD                       --源代码值相同
AND     T_CD14.DATA_PLTF_SRC_TAB_NM = upper('cos_000_swdeals') --数据平台源表主干名
AND     T_CD14.Data_Pltf_Src_Fld_Nm =upper('p_int_days')
LEFT JOIN f_fdm.CD_RF_STD_CD_TRAN_REF T_CD15
ON      T1.r_int_days = T_CD15.SRC_CD                       --源代码值相同
AND     T_CD15.DATA_PLTF_SRC_TAB_NM = upper('cos_000_swdeals') --数据平台源表主干名
AND     T_CD15.Data_Pltf_Src_Fld_Nm =upper('r_int_days')
LEFT JOIN f_fdm.CD_RF_STD_CD_TRAN_REF T_CD18
ON      T1.P_FREQ = T_CD18.SRC_CD                       --源代码值相同
AND     T_CD18.DATA_PLTF_SRC_TAB_NM = upper('cos_000_swdeals') --数据平台源表主干名
AND     T_CD18.Data_Pltf_Src_Fld_Nm =upper('P_FREQ')
left join dw_sdata.cos_000_bustruct T_org_1
on t.entity = T_org_1.thekey
and T_org_1.start_dt <= '$txdate'::date
and T_org_1.end_dt > '$txdate'::date
left join dw_sdata.cos_000_anacode T_org_2
on T_org_1.analyse04 = T_org_2.thekey 
and T_org_1.start_dt <= '$txdate'::date
and T_org_1.end_dt > '$txdate'::date                  --modified 20160912
where   T.start_dt <= '$TXDATE'::DATE
and     T.end_dt > '$TXDATE'::DATE  ----modified by zmx
;
 /*月积数、年积数、月日均余额、年日均余额临时表创建区*/

create local temporary table if not exists f_Cap_Raw_TX_tmp_yjs
on commit preserve rows as
select
      t.agmt_id
      ,(case
            when '$TXDATE'= '$MONTHBGNDAY'
            then t.notnl_prin
            else t.notnl_prin+coalesce(t1.mth_accm,0)
            end
       )                                                                      as mth_accm  --月积数
      ,(case
            when  '$TXDATE' = '$YEARBGNDAY'
            then t.notnl_prin
            else t.notnl_prin+coalesce(t1.yr_accm,0)
            end
       )                                                                      as yr_accm   --年积数
      ,(case
            when '$TXDATE' = '$MONTHBGNDAY'
            then t.notnl_prin
            else t.notnl_prin+coalesce(t1.mth_accm,0)
            end
       )/('$TXDATE'::date-'$MONTHBGNDAY'::date+1)               as mth_day_avg_bal  --月日均余额
      ,(case
           when '$TXDATE' = '$YEARBGNDAY'
           then t.notnl_prin
           else t.notnl_prin+coalesce(t1.yr_accm,0)
           end
       )/('$TXDATE'::date-'$YEARBGNDAY'::date+1)                    as Yr_Day_Avg_Bal   --年日均余额
from  f_fdm.f_Cap_Raw_TX     t
left join f_fdm.f_Cap_Raw_TX t1
on         t.agmt_id= t1.agmt_id
and        t1.etl_date='$TXDATE'::date-1
where     t.etl_date='$TXDATE'::date
;
/*月积数、年积数、月日均余额、年日均余额临时表创建区END*/
/*更新月积数、年积数、月日均余额、年日均余额*/
update f_fdm.f_Cap_Raw_TX   t
set mth_accm=t1.mth_accm
,yr_accm=t1.yr_accm
,mth_day_avg_bal=t1.mth_day_avg_bal
,Yr_Day_Avg_Bal=t1.Yr_Day_Avg_Bal
from  f_Cap_Raw_TX_tmp_yjs    t1
where t.agmt_id= t1.agmt_id
and   t.etl_date='$TXDATE'::date
;
/*数据处理区END*/
COMMIT;
