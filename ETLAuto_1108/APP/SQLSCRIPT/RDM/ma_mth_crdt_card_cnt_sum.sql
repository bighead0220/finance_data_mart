/*
Author             :liudongyan
Function           :月信用卡数汇总表
Load method        :INSERT
Source table       :f_acct_crdt_card_info                   
Destination Table  :ma_mth_crdt_card_cnt_sum 月信用卡数汇总表
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
DELETE FROM f_rdm.ma_mth_crdt_card_cnt_sum
WHERE  etl_date='$TXDATE'::date  ;
/*数据回退区END*/
/*数据处理区*/
INSERT INTO f_rdm.ma_mth_crdt_card_cnt_sum
(
      etl_date                              --数据日期
     ,Org_Num                               --机构号
     ,Actv_Ind                              --激活标志
     ,stl_crdt_card_cnt                     --结存信用卡数
     ,new_incrs_crdt_card_cnt               --新增信用卡数
     ,sys_src                               --系统来源
 
)
SELECT
     '$TXDATE'::date                      as etl_date
     ,T.Prmt_Org_Cd                                as Org_Num
     ,T.Actv_Ind                                   as Actv_Ind
     ,T.NUM                                        as stl_crdt_card_cnt
     ,T1.NUM1                                      as new_incrs_crdt_card_cnt
     ,T.Sys_Src                                    as sys_src

FROM (select Actv_Ind,
             Prmt_Org_Cd, 
             Sys_Src,
             count(distinct Card_Num)  as NUM
        from f_fdm.f_acct_crdt_card_info  --信用卡信息表 
       where etl_date='$TXDATE'::date 
       group by Actv_Ind,
                Prmt_Org_Cd ,
                Sys_Src
      )             T 
LEFT JOIN (select Actv_Ind,
                  Prmt_Org_Cd, 
                  Sys_Src,
                  count(distinct Card_Num)   as NUM1
             from f_fdm.f_acct_crdt_card_info     
            where to_char(Issu_Card_Dt,'yyyymm')=substr( '$TXDATE',1,6) 
            group by Actv_Ind,
                     Prmt_Org_Cd,
                     Sys_Src
          )         T1 
ON   T.Actv_Ind=T1.Actv_Ind
AND  T.Prmt_Org_Cd=T1.Prmt_Org_Cd
;
/*数据处理区END*/

COMMIT;
