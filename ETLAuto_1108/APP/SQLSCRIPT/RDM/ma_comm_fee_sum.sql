/*
Author             :liudongyan
Function           :手续费汇总
Load method        :INSERT
Source table       :f_evt_comfee_commsn 中间业务收入信息表
Destination Table  :ma_comm_fee_sum   手续费汇总
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
DELETE FROM f_rdm.ma_comm_fee_sum
WHERE  etl_date=  '$TXDATE'::date ;
/*数据回退区END*/
/*数据处理区*/
INSERT INTO f_rdm.ma_comm_fee_sum 
(
      etl_date                              --数据日期
     ,Acct_Num                              --帐号
     ,Org_Num                               --机构号
     ,Cur_Cd                                --币种
     ,cust_num                              --客户号
     ,prod_cd                               --产品代码
     ,TX_Typ                                --交易类型
     ,comm_fee_subj                         --手续费科目
     ,Comm_Fee_Amt                          --手续费金额
     ,chrg_typ                              --收费类型
     ,TX_Chnl                               --交易渠道
     ,sys_src                               --系统来源
 
)
SELECT
      '$TXDATE'::date                    as etl_date
     ,Acct_Num                                as Acct_Num
     ,Org_Num                                 as Org_Num
     ,Cur_Cd                                  as Cur_Cd
     ,Cust_Num                                as cust_num
     ,NULL                                   as prod_cd
     ,Chrg_Proj                               as TX_Typ
     ,Comm_Fee_Subj                           as comm_fee_subj
     ,sum(Comm_Fee_Amt)                            as Comm_Fee_Amt
     ,Chrg_Mode                               as chrg_typ
     ,TX_Chnl                                 as TX_Chnl
     ,Sys_Src                                 as sys_src

FROM  f_fdm.f_evt_comfee_commsn
WHERE month(etl_date::date) =month('$TXDATE'::date)
group by 
 Acct_Num,
 Org_Num,
 Cur_Cd,
 cust_num,
 TX_Typ,
 comm_fee_subj,
 chrg_typ,
 TX_Chnl,
 sys_src
;
/*数据处理区END*/

COMMIT;
