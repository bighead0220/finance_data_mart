/*
Author             :zhangwj
Function           :交易信息表
Load method        :
Source table       :dw_sdata.pcs_005_tb_sup_water_c,dw_sdata.lcs_000_pk_fix_dtl,dw_sdata.lcs_000_pk_fc_fix_leg,dw_sdata.ccs_006_japfg0,dw_sdata.cbs_001_amdtl_corp,dw_sdata.cbs_001_ammst_corp,dw_sdata.cbs_001_amdtl_secu,dw_sdata.cbs_001_ammst_sec,dw_sdata.ccb_000_event
Destination Table  :f_fdm.f_evt_tx_info
Frequency          :D
Modify history list:Created by zhangwj at 2016-5-10 15:53 v1.0
                    Changed by zhangwj at 2016-5-25 10:12 v1.1   大数据贴源层表名修改，表为拉链表或流水表，与之保持一致
                   :Modify  by xuminghao at 2016-7-14 10:12 v1.2  根据模型更改
                   :Modify  by xsh 将JAGOUSCOD修改为JAG0USCOD;group by 中增加T.tx_code
                   :Modify by weiyinhui at 2016-8-11 15:11:23 组别1 添加筛选条件
                   :Modified by wyh at 20160830 修改组别2下的group by 条件
                    Modified by wyh at 20160927 组别3 group by (PK)修改

-------------------------------------------逻辑说明---------------------------------------------
业务逻辑说明
-------------------------------------------逻辑说明END------------------------------------------
*/

/*临时表创建区*/

/*临时表创建区END*/

/*数据回退区*/
delete /* +direct */ from f_fdm.f_evt_tx_info
where etl_date = '$TXDATE'
;
/*数据回退区END*/

/*数据处理区*/
insert /* +direct */  into f_fdm.f_evt_tx_info
     (grp_typ                                   --组别
     ,etl_date                                  --数据日期
     ,tx_dt                                     --交易日期
     ,acct_num                                  --账号
     ,org_num                                   --机构号
     ,tx_typ_cd                                 --交易类型代码
     ,tx_chnl_cd                                --交易渠道代码
     ,cur_cd                                    --货币代码
     ,tx_amt                                    --交易金额
     ,tx_cnt                                    --交易笔数
     ,sys_src                                   --系统来源
     )
 select
       1                                                 as  grp_typ      --组别
       ,'$TXDATE'::date                         as  etl_date     --数据日期
       ,to_date(t.sup_date,'yyyymmdd')                   as  tx_dt        --交易日期
       ,coalesce(t.due_num,'')                           as  acct_num     --账号
       ,coalesce(t.trn_dep,'')                           as  org_num      --机构号
       ,case when trn_cod='B1101' and acc_typ in('01','02') then '1' --放款
             when trn_cod in ('B1102','B1301','FN_BatRepay_cop_001','DBM_bat_repayment_sh','DBM_bat_repayment') and acc_typ='ZZ' then '2' --还款
             when trn_cod in('DAF_inner_disc_chg','DBM_bat_disc_repayment','B1106') and acc_typ='ZZ' then '201' --还款(贴息扣款)
       else ''
       end                                               as  tx_typ_cd    --交易类型代码
       ,NVL(T5.TGT_CD,'@'||t.tran_from)                  as  tx_chnl_cd   --交易渠道代码
       ,NVL(T4.TGT_CD,'@'||t.curr_cod)                   as  cur_cd       --货币代码
       ,coalesce(sum(t.amt_incur),0)                     as  tx_amt       --交易金额
       ,count(1)                                         as  tx_cnt       --交易笔数
       ,'PCS'                                            as  sys_src      --系统来源
 from  DW_SDATA.PCS_005_TB_SUP_WATER_C t                 --历史流水表
 LEFT JOIN  F_Fdm.CD_RF_STD_CD_TRAN_REF T4
     ON  T.CURR_COD=T4.SRC_CD
    AND  T4.DATA_PLTF_SRC_TAB_NM = 'PCS_005_TB_SUP_WATER_C'
    AND  T4.DAtA_PLTF_SRC_FLD_NM ='CURR_COD'
 LEFT JOIN  F_Fdm.CD_RF_STD_CD_TRAN_REF T5
   ON T.TRAN_FROM=T5.SRC_CD
   AND  T5.DATA_PLTF_SRC_TAB_NM = 'PCS_005_TB_SUP_WATER_C'
   AND  T5.DATA_PLTF_SRC_FLD_NM ='TRAN_FROM'
 where t.recall<>'1'                                      --0：正常1：撤消
 AND   (
        (trn_cod='B1101' and acc_typ in('01','02'))--放款
        or (trn_cod in('B1102','B1301','FN_BatRepay_cop_001','DBM_bat_repayment_sh','DBM_bat_repayment',
        'DAF_inner_disc_chg','DBM_bat_disc_repayment','B1106') and acc_typ='ZZ')--还款
        )
 and   t.etl_dt='$TXDATE'::date
 group by t.sup_date,t.due_num,t.trn_dep,t.trn_cod,t.tran_from,t.curr_cod,t.acc_typ,t5.Tgt_cd,t4.Tgt_cd
  union all
 select
       2                                                 as  grp_typ       --组别
       ,'$TXDATE'::date                         as  etl_date      --数据日期
       ,to_date(substr(t.tran_time,1,8),'yyyymmdd')      as  tx_dt         --交易日期
       ,coalesce(t.acc,'')                               as  acct_num      --账号
       ,coalesce(t.tran_inst,'')                         as  org_num       --机构号
       ,coalesce(t.tran_code,'')                         as  tx_typ_cd     --交易类型代码
       ,coalesce(null,'')                                as  tx_chnl_cd    --交易渠道代码
       ,case when T1.curr_type is null then '156' --默认‘人民币’
           else coalesce(t1.curr_type,'')
            end                                          as  cur_cd        --货币代码
       ,coalesce(sum(t.amt),0)                           as  tx_amt        --交易金额
       ,count(1)                                         as  tx_cnt        --交易笔数
       ,'LCS'                                            as  sys_src       --系统来源
 from  dw_sdata.lcs_000_pk_fix_dtl t                     --定期交易明细表
 left join dw_sdata.lcs_000_pk_fc_fix_leg t1             --定期分户登记薄（外币）
 on        t.acc=t1.acc
 and       t1.start_dt<='$TXDATE'::date
 and       t1.end_dt>'$TXDATE'::date
 where     t.etl_dt='$TXDATE'::date
 group by  substr(t.tran_time,1,8),t.acc,t.tran_inst,t.tran_code,t1.curr_type           --修改group by 条件 20160830
 union all
 select
       3                                                 as  grp_typ       --组别
       ,'$TXDATE'::date                         as  etl_date      --数据日期
       ,to_date(substr(t.timestamp,1,8),'yyyymmdd')      as  tx_dt         --交易日期
       ,coalesce(t.acc,'')                               as  acct_num      --账号
       ,coalesce(t.tran_inst,'')                         as  org_num       --机构号
       ,coalesce(t.tran_code,'')                         as  tx_typ_cd     --交易类型代码
       ,coalesce(null,'')                                as  tx_chnl_cd    --交易渠道代码
       ,case when T1.curr_type is null then '156' --默认‘人民币’
        else coalesce(t1.curr_type,'')
        end                                              as  cur_cd        --货币代码
       ,coalesce(sum(t.amt),0)                           as  tx_amt        --交易金额
       ,count(1)                                         as  tx_cnt        --交易笔数
       ,'LCS'                                            as  sys_src       --系统来源
 from  dw_sdata.lcs_000_qry_cdm_dtl t                    --活期交易明细表
 left join  dw_sdata.lcs_000_pk_fc_fix_leg t1            --活期分户登记薄（外币）
 on         t.acc=t1.acc
 and        t1.start_dt<='$TXDATE'::date
 and        t1.end_dt>'$TXDATE'::date
 where   
 t.etl_dt='$TXDATE'::date
 group by   substr(t.timestamp,1,8),t.acc,t.tran_inst,t.tran_code,t1.curr_type --modified at 20160927
 union all
 select
       4                                                as  grp_typ        --组别
       ,'$TXDATE'::date                        as  etl_date       --数据日期
       ,to_date(t.jag0date,'yyyymmdd')                  as  tx_dt          --交易日期
       ,coalesce(t.jag0duebno,'')                       as  acct_num       --账号
       ,coalesce(t.jag0dpnok,'')                        as  org_num        --机构号
       ,coalesce(t.jag0uscod,'')                        as  tx_typ_cd      --交易类型代码
       ,NVL(T4.TGT_CD,'@'||t.jag0trfrom)                as  tx_chnl_cd     --交易渠道代码
       ,NVL(T5.TGT_CD,'@'||t.jag0cur)                   as  cur_cd         --货币代码
       ,coalesce(sum(t.jag0amt),0)                      as  tx_amt         --交易金额
       ,count(1)                                        as  tx_cnt         --交易笔数
       ,'CCS'                                           as  sys_src        --系统来源
 FROM  DW_SDATA.CCS_006_JAPFG0  t--柜面账务类交易对账主表
 LEFT JOIN  F_FDM.CD_RF_STD_CD_TRAN_REF T4
  ON  T.JAG0USCOD=T4.SRC_CD
    AND  T4.DATA_PLTF_SRC_TAB_NM = 'CCS_006_JAPFG0'
    AND  T4.DATA_PLTF_SRC_FLD_NM ='T.JAGOUSCOD'
  LEFT JOIN  F_FDM.CD_RF_STD_CD_TRAN_REF T5
   ON  T.JAG0CUR=T5.SRC_CD
    AND  T5.DATA_PLTF_SRC_TAB_NM = 'CCS_006_JAPFG0'
    AND  T5.DATA_PLTF_SRC_FLD_NM ='JAG0CUR'
 where t.jag0fltype in ('A','B')
 and   T.jag0date= '$TXDATE'
 and   t.start_dt<='$TXDATE'::date
 and   t.end_dt>'$TXDATE'::date
 group by t.jag0date,t.jag0duebno,t.jag0dpnok,t.jag0uscod,t.jag0trfrom,t.jag0cur,t5.Tgt_cd,t4.Tgt_cd
 union all
 select
       5                                                as  grp_typ        --组别
       ,'$TXDATE'::date                        as  etl_date       --数据日期
       ,to_date(t.tx_date,'yyyymmdd')                   as  tx_dt          --交易日期
       ,coalesce(t.cust_acct,'')                        as  acct_num       --账号
       ,coalesce(t.unit_code,'')                        as  org_num        --机构号
       ,coalesce(T.tx_code,'')                          as  tx_typ_cd      --交易类型代码
       ,coalesce(('CBS_'||T.chnl_code),'')              as  tx_chnl_cd     --交易渠道代码
       ,coalesce(t1.cur_code,'')                        as  cur_cd         --货币代码
       ,coalesce(sum(t.amount),0)                       as  tx_amt         --交易金额
       ,count(1)                                        as  tx_cnt         --交易笔数
       ,'CBS'                                           as  sys_src        --系统来源
 from  dw_sdata.cbs_001_amdtl_corp t                    --单位分户明细账
 left join  dw_sdata.cbs_001_ammst_corp t1              --单位分户账
 on         t.inter_acct=t1.inter_acct
 and        t1.start_dt<='$TXDATE'::date
 and        t1.end_dt>'$TXDATE'::date
 where      t.valid_flag='1'
 and        t.etl_dt='$TXDATE'::date
 group by   t.tx_date,t.cust_acct,t.unit_code,t.chnl_code,t1.cur_code,T.tx_code
 union all
 select
       6                                                as  grp_typ        --组别
       ,'$TXDATE'::date                        as  etl_date       --数据日期
       ,to_date(t.tx_date,'yyyymmdd')                   as  tx_dt          --交易日期
       ,coalesce(t.cust_acct,'')                        as  acct_num       --账号
       ,coalesce(t.unit_code,'')                        as  org_num        --机构号
       ,coalesce(T.tx_code,'')                          as  tx_typ_cd      --交易类型代码
       ,coalesce(( 'CBS_'||T.chnl_code),'')             as  tx_chnl_cd     --交易渠道代码
       ,coalesce(t1.cur_code,'')                        as  cur_cd         --货币代码
       ,coalesce(sum(t.amount),0)                       as  tx_amt         --交易金额
       ,count(1)                                        as  tx_cnt         --交易笔数
       ,'CBS'                                           as  sys_src        --系统来源
 from  dw_sdata.cbs_001_amdtl_secu t                    --保证金分户明细
 left join  dw_sdata.cbs_001_ammst_secu t1              --单位保证金分户账
 on         t.inter_acct=t1.inter_acct
 and        t1.start_dt<='$TXDATE'::date
 and        t1.end_dt>'$TXDATE'::date
 where      t.valid_flag='1'
 and        t.etl_dt='$TXDATE'::date
 group by   t.tx_date,t.cust_acct,t.unit_code,t.chnl_code,t1.cur_code,T.tx_code
 union all
 select
       7                                                as  grp_typ         --组别
       ,'$TXDATE'::date                        as  etl_date        --数据日期
       ,to_date(to_char(t.inp_date),'yyyymmdd')         as  tx_dt           --交易日期
       ,coalesce(to_char(t.acctnbr),'')                 as  acct_num        --账号
       ,'1199523Q'                                      as  org_num         --机构号
       ,coalesce(to_char(t.trans_type),'')              as  tx_typ_cd       --交易类型代码
       ,coalesce(t.pipe1,'')                            as  tx_chnl_cd      --交易渠道代码
       ,coalesce(to_char(t.currncy_cd),'')              as  cur_cd          --货币代码
       ,SUM(T.BILL_AMT)                                 as  tx_amt          --交易金额          --此映射规则暂时存在问题
       ,count(1)                                        as  tx_cnt          --交易笔数
       ,'CCB'                                           as  sys_src         --系统来源
 from  dw_sdata.ccb_000_event t                         --账户交易流水表
 where etl_dt='$TXDATE'::date
  and t.REV_IND<>'1'
 group by t.inp_date,t.acctnbr,t.trans_type,t.pipe1,t.currncy_cd
 ;
/*数据处理区END*/
 COMMIT;
