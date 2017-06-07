/*
Author             :魏银辉
Function           :代理
Load method        :INSERT
Source table       :f_fdm.insu_corp_comm_fee_info_tab
Destination Table  :GL_AGENT 代理
Frequency          :M
Modify history list:Created by 魏银辉 at 2016-8-9 10:02:06
                    modified by wyh at 20161011 增加月末跑批控制语句
                    modified by wyh at 20161014 修改业务逻辑，增加GROUP BY 条件
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
DELETE FROM f_rdm.GL_AGENT
WHERE  etl_date= '$TXDATE'::date ;
/*数据回退区END*/

/*数据处理区*/
INSERT INTO f_rdm.GL_AGENT
(
        etl_date                --数据日期
        ,Insu_Corp_Cd           --保险公司代码
        ,insu_comm_fee          --寿险手续费
        ,presrv_comm_fee        --财险手续费
        ,rwl_prd_comm_fee       --续期手续费
        ,realtm_clltn_comm_fee  --实时代收手续费
        ,bat_comm_fee           --批量手续费
        ,oth_comm_fee           --其他手续费
        ,comm_fee_sum           --手续费合计
        ,sys_src                --系统来源
)
SELECT
        '$TXDATE'       AS  etl_date              
        ,insu_corp_cd            AS  Insu_Corp_Cd
        ,sum(life_insur_comm_fee)     AS  insu_comm_fee
        ,sum(prop_insur_comm_fee)     AS  presrv_comm_fee
        ,sum(renewal_comm_fee)        AS  rwl_prd_comm_fee
        ,sum(realtm_clltn_comm_fee)   AS  realtm_clltn_comm_fee
        ,sum(bat_comm_fee)            AS  bat_comm_fee
        ,sum(oth_comm_fee)            AS  oth_comm_fee
        ,sum(comm_fee_sum)            AS  comm_fee_sum
        ,max(sys_src)                 AS  sys_src
FROM f_fdm.f_evt_insu_comm_fee T
where --T.etl_date= '$TXDATE'::date     modified by wyh at 20161014
substr(to_char(T.etl_date,'YYYYMMDD'),1,6) = substr('$TXDATE',1,6)
group by t.insu_corp_cd
;

/*数据处理区END*/

COMMIT;
