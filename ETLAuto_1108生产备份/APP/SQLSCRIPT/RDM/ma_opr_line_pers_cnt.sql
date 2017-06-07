/*
Author             :liudongyan
Function           :经营条线人数表
Load method        :INSERT
Source table       :f_org_line_perscnt                 
Destination Table  :ma_opr_line_pers_cnt 经营条线人数表
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
DELETE FROM f_rdm.ma_opr_line_pers_cnt
WHERE  etl_date='$TXDATE'::date  ;
/*数据回退区END*/

/*数据处理区*/
INSERT INTO f_rdm.ma_opr_line_pers_cnt
(
      etl_date                              --数据日期
     ,Org_Num                               --机构号
     ,Dept_Num                              --部门号
     ,Dept_Nm                               --部门名称
     ,Post_Encd                             --岗位编码
     ,Post_Nm                               --岗位名称
     ,pers_cnt                              --人数
     ,sys_src                               --系统来源 
)
SELECT
      '$TXDATE'::date                     as etl_date
     ,Org_Num                                      as Org_Num
     ,Dept_Num                                     as Dept_Num
     ,Dept_Nm                                      as Dept_Nm
     ,Post_Encd                                    as Post_Encd
     ,Post_Nm                                      as Post_Nm
     ,Pers_Cnt                                     as pers_cnt
     ,Sys_Src                                      as sys_src
 FROM f_fdm.f_org_line_perscnt --经营条线人数信息 
WHERE etl_date='$TXDATE'::date
;
/*数据处理区END*/

COMMIT;
