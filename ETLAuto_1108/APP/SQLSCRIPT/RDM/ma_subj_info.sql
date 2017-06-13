/*
Author             :liudongyan
Function           :科目信息
Load method        :INSERT
Source table       :f_fnc_subj_info 
Destination Table  :ma_subj_info   科目信息
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
DELETE FROM f_rdm.ma_subj_info
WHERE  etl_date= '$TXDATE'::date ;
/*数据回退区END*/

/*数据处理区*/
INSERT INTO f_rdm.ma_subj_info 
(
       etl_date                              --数据日期
      ,Subj_Cd                          --科目代码  
      ,Subj_Nm                               --科目名称
      ,Subj_Hrcy                             --科目层级
      ,Subj_Cate                             --科目类别
      ,Subj_Stat                             --科目状态
      ,Sys_Src                               --系统来源    
)
SELECT
       '$TXDATE'::date               as etl_date
      ,Subj_Cd                                as Subj_Cd
      ,Subj_Nm                                as Subj_Nm
      ,Subj_Hrcy                              as Subj_Hrcy
      ,Subj_Cate                              as Subj_Cate
      ,Subj_Stat                              as Subj_Stat
      ,Sys_Src                                as Sys_Src   
FROM  f_fdm.f_fnc_subj_info --科目信息表
WHERE etl_date =  '$TXDATE'::date
;
/*数据处理区END*/

COMMIT;
