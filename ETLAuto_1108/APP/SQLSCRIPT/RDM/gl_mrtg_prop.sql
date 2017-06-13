/*
Author             :liudongyan
Function           :抵质押物
Load method        :INSERT
Source table       :f_loan_mrtg_prop	贷款抵质押物信息表
Destination Table  :GL_Mrtg_Prop	抵质押物
Frequency          :M
Modify history list:Created by liudongyan 2016/5/18 10:19:34
                    Modify by 魏银辉 at 2016-8-8 15:50:13 新增筛选条件
                    modified by wyh at 20161011 增加月末跑批控制语句
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
DELETE FROM f_rdm.GL_Mrtg_Prop
WHERE  etl_date=  '$TXDATE'::date ;
/*数据回退区END*/

/*数据处理区*/
INSERT INTO f_rdm.GL_Mrtg_Prop
(
     etl_date        --数据日期
     ,pldg_id        --抵押物编号
     ,guar_agmt_id   --担保协议编号
     ,pldg_typ_cd    --担保物类型代码
     ,pldg_rgst_val  --抵押物登记价值
     ,pldg_estim_val --抵押物评估价值
     ,sys_src        --系统来源
)
SELECT
    '$TXDATE'::date        as etl_date
    ,T.pldg_id                        as pldg_id
    ,T.guar_contr_agmt_id             as guar_agmt_id
    ,T.pldg_typ_cd                    as pldg_typ_cd
    ,T.pldg_rgst_val                  as pldg_rgst_val
    ,T.pldg_estim_val                 as pldg_estim_val
    ,T.sys_src                        as sys_src
FROM  f_fdm.f_loan_mrtg_prop T	--贷款抵质押物信息表
WHERE T.guar_contr_agmt_id in (
                        select
                                guar_agmt_id
                        from f_rdm.GL_Guar_Contr
                        where etl_date = '$TXDATE'::date
                      )
AND T.etl_date = '$TXDATE'::date
;
/*数据处理区END*/

COMMIT;
