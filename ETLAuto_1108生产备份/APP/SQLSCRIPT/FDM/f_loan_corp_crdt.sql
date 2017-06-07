/*
Author             :刘东燕
Function           :公司授信额度信息表
Load method        :INSERT
Source table       :dw_sdata.ccs_004_tb_crd_credit_limit,dw_sdata.ccs_004_tb_crd_product_limit                  
Destination Table  :f_loan_corp_crdt  公司授信额度信息表
Frequency          :D
Modify history list:Created by刘东燕2016年4月19日10:05:55
                   :Modify  by
-------------------------------------------逻辑说明---------------------------------------------
业务逻辑说明
-------------------------------------------逻辑说明END------------------------------------------
*/
/*数据回退区*/
DELETE/* +direct */ from f_fdm.f_loan_corp_crdt
where etl_date='$TXDATE'::date
;
/*数据回退区END*/
/*数据处理区*/
INSERT INTO f_fdm.f_loan_corp_crdt
(
        Grp_Typ                                  --组别
       ,etl_date                                 --数据日期
       ,Crdt_Lmt_Id                              --授信额度编号
       ,Cust_Num                                 --客户号
       ,Belg_Org_Num                             --所属机构号
       ,Cur_Cd                                   --货币代码
       ,Is_Can_Cir_Ind                           --是否可循环标志
       ,Efft_Dt                                  --生效日期
       ,Invldtn_Dt                               --失效日期
       ,Crdt_Amt                                 --授信金额
       ,Aval_Lmt                                 --可用额度
       ,Frz_Lmt                                  --冻结额度
       ,Lmt_Stat_Cd                              --额度状态代码
       ,Sys_Src                                  --系统来源


)
SELECT
        '1'                                           as Grp_Typ
        ,'$TXDATE'::date                     as etl_date
        ,coalesce(T.credit_limit_num,'')              as Crdt_Lmt_Id
        ,coalesce(T.customer_num,'')                  as Cust_Num
        ,coalesce(T.branch,'')                        as Belg_Org_Num
        ,coalesce(T.currency,'')                                   as Cur_Cd
        ,coalesce(T.circular,'')                                   as Is_Can_Cir_Ind
        ,coalesce(T.start_date,'$MINDATE'::DATE)                                 as Efft_Dt
        ,coalesce(T.end_date,'$MINDATE'::DATE)                                   as Invldtn_Dt
        ,coalesce(T.approve_credit_limit,0)           as Crdt_Amt
        ,coalesce(T.approved_free_limit,0)            as Aval_Lmt
        ,coalesce(T.freezed_limit,0)                  as Frz_Lmt
        ,coalesce(T.state,'')                                      as Lmt_Stat_Cd
       ,'CCS'                                         as Sys_Src
         
FROM    dw_sdata.ccs_004_tb_crd_credit_limit   AS T                  
WHERE   T.start_dt<='$TXDATE'::date
AND     T.end_dt>'$TXDATE'::date

;
/*数据处理区END*/
COMMIT;

