/*
Author             :Liuxz
Function           :
Load method        :
Source table       :dw_sdata.fss_001_gf_basic,dw_sdata.fss_001_gf_ydaddmx
Destination Table  :f_fdm.f_prd_chrem
Frequency          :D
Modify history list:Created by Liuxz at 2016-04-28 09:05:58.728000
                   :Modify  by Liuxz 2016-05-25 更改贴源层表县
                    modified by Liuxz 20160708 修改字段中文名'开放式标识'和'保本标识'为'是否开放式标识' '是否保本标识'，并修改相应的映射规则 (变更记录105)
                    modified by liudongyan 20160711修改贴源层表名dw_sdata.FSS_GF_YDADDMX_0215 （测试脚本）
                    modified by xuminghao 20160712 Coll_Start_Dt，Coll_End_Dt的case when里添加::date
                    modified by zmx 20160830 变更很大，根据模型，select及主表从表，关联条件都进行了变更
*/


-------------------------------------------逻辑说明---------------------------------------------
/*业务逻辑说明
理财产品信息表
*/
-------------------------------------------逻辑说明END------------------------------------------
/*创建临时表区*/
/*创建临时表区END*/
/*数据回退区*/
delete /* +direct */ from f_fdm.f_prd_chrem
where etl_date='$TXDATE'::date
;
/*数据回退区END*/
/*数据处理区*/
insert /* +direct */ into f_fdm.f_prd_chrem
        (Grp_Typ                           --组别
        ,etl_date                          --数据日期
        ,Prod_Cd                           --产品代码
        ,Prod_Nm                           --产品名称
        ,Cur_Cd                            --货币代码
        ,Coll_Start_Dt                     --募集开始日期
        ,Coll_End_Dt                       --募集结束日期
        ,Coll_Totl_Amt                     --募集总额
        ,Had_Coll_Amt                      --已募集金额
        ,Open_Mode_Ind                     --是否开放式标识
        ,Brk_Evn_Ind                       --是否保本标识
        ,Indep_Bal_Ind                     --自主平衡标识
        ,Prod_Cate                         --产品类别
        ,Prod_Stat                         --产品状使
        ,Sys_Src                           --系统来源
        )
select  '1'                                                              as Grp_Typ            --组别
        ,'$TXDATE'::date                                        as etl_date           --数据日期
        ,T.prod_code                                                     as Prod_Cd            --产品代码
        ,T.prod_name                                                   as Prod_Nm            --产品名称
        ,'156'                                                           as Cur_Cd             --货币代码
        ,coalesce(t1.raising_begin_date::date,'$MINDATE'::date)      --募集开始日期
        ,coalesce(t1.raising_end_date::date,'$MINDATE'::date)        --募集结束日期
        ,t1.plan_issue_amt                                                    as Coll_Totl_Amt      --募集总额
        ,0.00              as Had_Coll_Amt       --已募集金额
        ,t.prod_oper_model                                                       as Open_Mode_Ind      --是否开放式标识
        ,t.profit_type                                                             as Brk_Evn_Ind        --是否保本标识
        ,t.is_auto_balance                                                              as Indep_Bal_Ind      --自主平衡标识
        ,t.profit_type                                                   as Prod_Cate          --产品类别
        ,t.prod_status                                                     as Prod_Stat          --产品状使
        ,'FSS'                                                           as Sys_Src            --系统来源
from    dw_sdata.fss_001_gf_prod_base_info T
left join   dw_sdata.fss_001_gf_prod_buy_info T1 
on      T.prod_code=T1.prod_code
and     T1.start_dt<='$TXDATE'::date
and     T1.end_dt>'$TXDATE'::date
WHERE   T.START_DT<='$TXDATE'::date
AND     T.END_DT>'$TXDATE'::date
;
/*数据处理区END*/
commit
;
