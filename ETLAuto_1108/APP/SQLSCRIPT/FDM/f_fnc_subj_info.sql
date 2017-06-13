/*
Author             :Liuxz
Function           :
Load method        :
Source table       :dw_sdata.acc_000_t_mng_pa_itm
Destination Table  :f_fdm.f_fnc_subj_info
Frequency          :D
Modify history list:Created by Liuxz at 2016-05-04 11:21:19.809000
                   :Modify  by liuxz 20160714 注释组别1
                    modified by zmx 修改码值
		    modified by lxz 20160902 修改科目类别代码转换条件 
*/


-------------------------------------------逻辑说明---------------------------------------------
/*业务逻辑说明
科目信息表
*/
-------------------------------------------逻辑说明END------------------------------------------
/*数据回退区*/
delete from f_fdm.f_fnc_subj_info
where etl_date='$TXDATE'::date
;
/*数据回退区END*/
/*数据处理区*/
/*
insert into f_fdm.f_fnc_subj_info
(grp_typ      --组别
,etl_date      --数据日期
,Subj_Cd      --科目代码
,Subj_Nm      --科目名称
,Subj_Hrcy      --科目层级
,Subj_Cate      --科目类别
,Subj_Stat      --科目状态
,Sys_Src      --系统来源
)
select  '1'                                              as grp_typ        --组别
        ,'$TXDATE'::date                        as etl_date       --数据日期
        ,T.subjcode                                      as Subj_Cd        --科目代码
        ,T.subjname                                      as Subj_Nm        --科目名称
        ,T.subjlev                                       as Subj_Hrcy      --科目层级
        ,T.pk_subjtype                                   as Subj_Cate      --科目类别
        ,T.sealflag                                      as Subj_Stat      --科目状态
        ,''                                              as Sys_Src        --系统来源
from    dw_sdata.BD_ACCSUBJ T   --不存在
;
*/
insert /* +direct */ into f_fdm.f_fnc_subj_info
(grp_typ                              --组别
,etl_date                             --数据日期
,Subj_Cd                              --科目代码
,Subj_Nm                              --科目名称
,Subj_Hrcy                            --科目层级
,Subj_Cate                            --科目类别
,Subj_Stat                            --科目状态
,dtl_subj_ind                                     --明细科目标识
,Sys_Src                              --系统来源
)
select  '2'                                               as  grp_typ       --组别
        ,'$TXDATE'::date                         as etl_date       --数据日期
        ,T.ITM_NO                                         as Subj_Cd        --科目代码
        ,T.ITM_NAME                                       as Subj_Nm        --科目名称
        ,T.ITM_LVL                                        as Subj_Hrcy      --科目层级
        ,coalesce(T1.TGT_CD,'@'||T.ITM_PSN)                                        as Subj_Cate      --科目类别
        ,T.ITM_STAT                                       as Subj_Stat      --科目状态
        ,T.ENDITM_FLAG                                    AS  dtl_subj_ind           --明细科目标识
        ,'ACC'                                            as Sys_Src        --系统来源
from    dw_sdata.acc_000_t_mng_pa_itm T
left join   f_fdm.CD_RF_STD_CD_TRAN_REF T1 --需转换代码表
  on T.ITM_PSN=T1.SRC_CD
 and T1.DATA_PLTF_SRC_TAB_NM = 'ACC_000_T_MNG_PA_ITM' --数据平台源表主干名
 and T1.Data_Pltf_Src_Fld_Nm ='ITM_PSN'     
WHERE   T.START_DT<='$TXDATE'::date
AND     T.END_DT>'$TXDATE'::date
;
/*数据处理区END*/

commit;

