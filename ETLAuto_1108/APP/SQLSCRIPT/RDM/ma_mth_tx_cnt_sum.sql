/*
Author             :liudongyan
Function           :月交易笔数汇总表
Load method        :INSERT
Source table       :f_evt_tx_info                   
Destination Table  :ma_mth_tx_cnt_sum 月交易笔数汇总表 
Frequency          :M
Modify history list:Created by liudongyan 2016/5/18 10:19:34
                   :Modify  by
*/

-------------------------------------------逻辑说明---------------------------------------------
/* 

*/
-------------------------------------------逻辑说明END------------------------------------------
/*月末跑批控制语句*/
SELECT COUNT(1) 
FROM dual
where '$MONTHENDDAY'='$TXDATE';
.IF ActivityCount <= 0 THEN .GOTO SCRIPT_END;
/*月末跑批控制语句END*/
/*临时表创建区*/
/*临时表创建区END*/
/*数据回退区*/
DELETE FROM f_rdm.ma_mth_tx_cnt_sum
WHERE  to_char(etl_date,'yyyymm')=substr( '$TXDATE',1,6) ;
/*数据回退区END*/
/*数据处理区*/
INSERT INTO f_rdm.ma_mth_tx_cnt_sum
(
      etl_date                              --数据日期
     ,Acct_Num                              --帐号
     ,Org_Num                               --机构号
     ,Cur_Cd                                --币种
     ,TX_Typ                                --交易类型
     ,TX_Chnl                               --交易渠道
     ,TX_Cnt                                --交易笔数
     ,TX_Amt                                --交易金额
     ,sys_src                               --系统来源
 
)
SELECT
     '$TXDATE'::date                as etl_date
     ,Acct_Num                               as Acct_Num
     ,Org_Num                                as Org_Num
     ,Cur_Cd                                 as Cur_Cd
     ,TX_Typ_Cd                              as TX_Typ
     ,TX_Chnl_Cd                             as TX_Chnl
     ,sum(TX_Cnt)                            as TX_Cnt
     ,sum(TX_Amt)                            as TX_Amt
     ,Sys_Src                                as sys_src
FROM  f_fdm.f_evt_tx_info --交易信息表
WHERE to_char(tx_dt,'yyyymm')=substr('$TXDATE',1,6) 
GROUP BY Acct_Num,
         Org_Num,Cur_Cd,
         TX_Typ_Cd,
         TX_Chnl_Cd,
         Sys_Src
;
/*数据处理区END*/

COMMIT;
