/*
Author             :liudongyan
Function           :同业客户
Load method        :INSERT
Source table       :f_cust_ibank 同业客户基本信息表
Destination Table  :ma_cust_ibank   同业客户
Frequency          :M
Modify history list:Created by liudongyan 2016/5/18 10:19:34
                   :Modify  by
*/

-------------------------------------------逻辑说明---------------------------------------------
/* 

*/
-------------------------------------------逻辑说明END------------------------------------------
SELECT COUNT(1) 
FROM dual
where '$MONTHENDDAY'='$TXDATE';
.IF ActivityCount <= 0 THEN .GOTO SCRIPT_END;
/*月末跑批控制语句END*/
/*临时表创建区*/
/*临时表创建区END*/
/*数据回退区*/
DELETE FROM f_rdm.ma_cust_ibank
WHERE  etl_date= '$TXDATE'::date ;
/*数据回退区END*/
/*数据处理区*/
INSERT INTO f_rdm.ma_cust_ibank
(
        etl_date                              --数据日期
        ,cust_num                             --客户号
        ,ibank_cust_legl_nm                   --同业客户法定名称
        ,is_crdt_cust_ind                     --是否授信客户标志
        ,is_lpr_ind                           --是否法人标志
        ,cust_typ_cd                          --客户类型代码
        ,fin_lics_id                          --金融许可证编号
        ,org_org_cd                           --组织机构代码
       ,inds_typ_cd                           --行业类型代码

)
SELECT
        '$TXDATE'::date               as etl_date
        ,cust_num                              as cust_num
        ,ibank_cust_legl_nm                    as ibank_cust_legl_nm
        ,is_crdt_cust_ind                      as is_crdt_cust_ind
        ,is_lpr_ind                            as is_lpr_ind
        ,cust_typ_cd                           as cust_typ_cd
        ,fin_lics_id                           as fin_lics_id
        ,org_org_cd                            as org_org_cd
        ,inds_typ_cd                           as inds_typ_cd

FROM  f_fdm.f_cust_ibank
WHERE etl_date = '$TXDATE'::date
;
/*数据处理区END*/

COMMIT;
