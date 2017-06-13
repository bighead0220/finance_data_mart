/*
Author             :Liuxz
Function           :
Load method        :
Source table       :dw_sdata.ecf_001_t01_cust_info,dw_sdata.ecf_001_t02_cust_acct_rel,dw_sdata.ecf_001_t01_cust_card_level,dw_sdata.ecf_001_t01_cust_extend_info,dw_sdata.ecf_001_t09_asset_assess,dw_sdata.ecf_004_t01_cust_info,dw_sdata.ecf_004_t01_cust_extend_info
Destination Table  :f_fdm.f_cust_indv
Frequency          :D
Modify history list:Created by Liuxz at 2016-04-15 17:34:07.758000
                   :Modify  by Liuxz 2016-05-25 更改贴源层表名
                    modify  by Liuxz 2016-06-03 变更记录46
                    modify by liuxz 20160617 第一组普通个人客户：修改字段‘国籍代码’、‘性别代码’、‘职业代码’、‘婚姻状态代码’、‘客户等级代码’、‘证件类型代码’取数规则 
                    modified by liuxz 20160704 证件代码需转换代码开发
                    modified by liuxz 20160714 取消'是' '否'
                     modified by liuxz 20160718 修改整体格式
                    modified by wyh 20160826 修改coalesce,修改主表;
                    modified by wyh 20160826 增加数据重复处理区SQL;
					modified by wyh at 20161014 1.将ecf_001_t01_cust_extend_info进行 ROW_NUMBER处理
					                            2.last_updated_ts 替换为 updated_ts 
                    modified by zmx 20161014 修改t3
                    modified by wyh 20161019 修改ROW_NUMBER逻辑:order by UPDATED_TS desc,last_updated_ts desc    
*/


-------------------------------------------逻辑说明---------------------------------------------
/*业务逻辑说明
个人客户基本信息表
*/
-------------------------------------------逻辑说明END------------------------------------------
/*临时表创建区*/
create local temporary table if not exists tt_f_cust_indv_op_date  
on commit preserve rows as
select   party_id
	,min(op_date) as op_date 
from dw_sdata.ecf_001_t02_cust_acct_rel 
where START_DT<='$TXDATE'::date  and END_DT>'$TXDATE'::date 
group by party_id
order by party_id
segmented by hash(party_id) ALL NODES;  

create local temporary table if not exists tt_f_cust_indv_num_grp1
on commit preserve rows as
select * from (
select *,row_number() over(partition by ECIF_CUST_NO  order by UPDATED_TS desc,last_updated_ts desc) as num
from dw_sdata.ecf_001_t01_cust_info
where --UPDATED_TS = '99991231 00:00:00:000000'            --  modified by wyh at 20161019
--and 
START_DT<='$TXDATE'::DATE
and end_dt >'$TXDATE') t
where t.num=1
order by party_id
segmented by hash(party_id) ALL NODES;

/*临时表创建区END*/

/*数据回退区*/

DELETE /* +direct */ from f_fdm.f_cust_indv
where etl_date = '$TXDATE'::date
;
/*数据回退区END*/ 

/*数据处理区*/
/*组别2*/
insert /* +direct */ into f_fdm.f_cust_indv
(
Grp_Typ                                                    --组别
,ETL_Date                                                  --数据日期
,Cust_Num                                                  --客户号
,Cust_Nm                                                   --客户名称
,Nation_Cd                                                 --国籍代码
,Gender_Cd                                                 --性别代码
,Birth_Dt                                                  --出生日期
,Carr_Cd                                                   --职业代码
,Marrg_Stat_Cd                                             --婚姻状态代码
,Open_Acct_Org_Num                                         --开户机构号
,Belg_Org_Num                                              --所属机构号
,Open_Acct_Dt                                              --开户日期
,Cust_Typ_Cd                                               --客户类型代码
,Cust_Lvl_Cd                                               --客户等级代码
,Crdt_Card_Cust_Lvl_Cd                                     --信用卡客户等级代码
,Is_VIP                                                    --是否VIP
,Cust_Hi_Card_Hold_Lvl_Cd                                  --客户最高持卡级别代码
,Is_Pros_VIP_Ind                                           --是否潜在VIP标志
,Fin_Asst_Totl_Amt                                         --金融资产总额
,Emp_Ind                                                   --员工标志
,Cust_Stat_Cd                                              --客户状态代码
,Cust_Mgr_Id                                               --客户经理编号
,Cust_Div_Grp                                              --客户分群
,Cert_Typ_Cd                                               --证件类型代码
,Cert_Num                                                  --证件号码
,Sys_Src                                                   --系统来源
)
select  '2'                                                     as grp_typ                       --组别
,'$TXDATE'::date                                       as ETL_Date                      --数据日期
--,t.party_id
,coalesce(t.ECIF_CUST_NO,'')                                                 as Cust_Num                      --客户号
,coalesce(t.PARTY_NAME,'')                                                   as Cust_Nm                       --客户名称
,coalesce(t.NAT,'')                                                          as Nation_Cd                     --国籍代码
,coalesce(t.GENDER,'')                                                       as Gender_Cd                     --性别代码
,coalesce(t.BIRTH_DATE::date,'$MINDATE'::DATE)                                             as Birth_Dt                      --出生日期
,coalesce(t.PRFSSN,'')                                                       as Carr_Cd                       --职业代码
,coalesce(t.MARITAL_STATE,'')                                                as Marrg_Stat_Cd                 --婚姻状态代码
,coalesce(t.OPEN_ORG,'')                                                     as Open_Acct_Org_Num             --开户机构号
,coalesce(t.OWN_ORG,'')                                                      as Belg_Org_Num                  --所属机构号
,coalesce(to_date(t1.OP_DATE,'YYYYMMDD'),'$MINDATE'::DATE)                                 as Open_Acct_Dt                  --开户日期
,''                                                             as Cust_Typ_Cd                   --客户类型代码
,coalesce(t.CUST_LEVEL,'')                                                   as Cust_Lvl_Cd                   --客户等级代码
,''                                                             as Crdt_Card_Cust_Lvl_Cd         --信用卡客户等级代码
,t.IS_VIP_FLAG                                                as Is_VIP                        --是否VIP
,coalesce(t2.CUST_CARD_LEVEL,'')                                             as Cust_Hi_Card_Hold_Lvl_Cd      --客户最高持卡级别代码
,coalesce(to_char(t3.WILL_VIP_FLAG),'')                                               as Is_Pros_VIP_Ind               --是否潜在VIP标志
--,null -- modify by dhy 20160802 t4.FIN_ASSET_AMT 
,0                                              as Fin_Asst_Totl_Amt             --金融资产总额
,coalesce(t3.FI_EMP_FLAG,'')                                                 as Emp_Ind                       --员工标志
,coalesce(t.CUST_STATE,'')                                                   as Cust_Stat_Cd                  --客户状态代码
,coalesce(t3.MANAGER_CODE,'')                                                as Cust_Mgr_Id                   --客户经理编号
,''                                                             as Cust_Div_Grp                  --客户分群
,coalesce(T5.TGT_CD,'@'||T.CERT_TYPE)                           as Cert_Typ_Cd                   --证件类型代码
,coalesce(T.CERT_NO,'')                                                      as Cert_Num                      --证件号码
,'ECF'                                                          as Sys_Src                       --系统来源
from (select *,row_number() over(partition by ECIF_CUST_NO  order by updated_ts desc ,last_updated_ts desc) as num
from dw_sdata.ecf_004_t01_cust_info
where --UPDATED_TS = '99991231 00:00:00:000000'
--and                              -- modified by wyh at 20161019
START_DT<='$TXDATE'::DATE
and end_dt >'$TXDATE') t           -- modified by wyh at 20160826
left join tt_f_cust_indv_op_date T1
--left join  ( select party_id,min(op_date) as op_date from dw_sdata.ecf_001_t02_cust_acct_rel where START_DT<='$TXDATE'::date  and END_DT>'$TXDATE'::date group by party_id ) t1  
on t.party_id=t1.party_id
--left join (select party_id,max(CUST_CARD_LEVEL) as CUST_CARD_LEVEL from  dw_sdata.ecf_001_T01_CUST_CARD_LEVEL where  START_DT<='$TXDATE'::date AND END_DT>'$TXDATE'::date group by party_id ) t2    
left join (SELECT party_id, 
        cust_card_level ,
        START_DT,
        END_DT,
        ROW_NUMBER() OVER(PARTITION BY party_id ORDER BY updated_ts desc ,last_updated_ts DESC) AS NUM   -- modified by wyh at 20161019
 FROM dw_sdata.ecf_001_t01_cust_card_level 
 WHERE START_DT<='$TXDATE'::date 
   and '$TXDATE'::date < end_dt
 )T2 
on t.party_id=t2.party_id
and t2.num = 1
left join   (SELECT party_id, 
          WILL_VIP_FLAG,
        MANAGER_CODE,
        FI_EMP_FLAG,       
        START_DT,   --modified by zmx 20161014
        END_DT,
        ROW_NUMBER() OVER(PARTITION BY party_id ORDER BY updated_ts desc , last_updated_ts DESC) AS NUM  -- modified by wyh at 20161019
 FROM dw_sdata.ecf_001_t01_cust_extend_info 
 WHERE START_DT<='$TXDATE'::date  and END_DT>'$TXDATE'::date)
t3
on       t.party_id=t3.party_id
and      t3.NUM = 1
AND      T3.START_DT<='$TXDATE'::date
AND      T3.END_DT>'$TXDATE'::date
left join f_fdm.CD_RF_STD_CD_TRAN_REF T5
on       CERT_TYPE=T5.SRC_CD
and      T5.DATA_PLTF_SRC_TAB_NM = 'ECF_004_T01_CUST_INFO' 
and      T5.Data_Pltf_Src_Fld_Nm ='CERT_TYPE'
where t.num = 1   --modified by wyh at 20160826
--and t.UPDATED_TS = '99991231 00:00:00:000000'
--AND   T.START_DT<='$TXDATE'::date
--AND   T.END_DT>'$TXDATE'::date
;
/*组别1*/

insert /* +direct */ into f_fdm.f_cust_indv
(
Grp_Typ                                  --组别
,ETL_Date                                --数据日期
,Cust_Num                                --客户号
,Cust_Nm                                 --客户名称
,Nation_Cd                               --国籍代码
,Gender_Cd                               --性别代码
,Birth_Dt                                --出生日期
,Carr_Cd                                 --职业代码
,Marrg_Stat_Cd                           --婚姻状态代码
,Open_Acct_Org_Num                       --开户机构号
,Belg_Org_Num                            --所属机构号
,Open_Acct_Dt                            --开户日期
,Cust_Typ_Cd                             --客户类型代码
,Cust_Lvl_Cd                             --客户等级代码
,Crdt_Card_Cust_Lvl_Cd                   --信用卡客户等级代码
,Is_VIP                                  --是否VIP
,Cust_Hi_Card_Hold_Lvl_Cd                --客户最高持卡级别代码
,Is_Pros_VIP_Ind                         --是否潜在VIP标志
,Fin_Asst_Totl_Amt                       --金融资产总额
,Emp_Ind                                 --员工标志
,Cust_Stat_Cd                            --客户状态代码
,Cust_Mgr_Id                             --客户经理编号
,Cust_Div_Grp                            --客户分群
,Cert_Typ_Cd                             --证件类型代码
,Cert_Num                                --证件号码
,Sys_Src                                 --系统来源
)             
select  '1'                                                 as grp_typ                       --组别
,'$TXDATE'::date                                   as ETL_Date                      --数据日期
--,t.party_id
,coalesce(t.ECIF_CUST_NO,'')                                            as Cust_Num                      --客户号
,coalesce(t.PARTY_NAME,'')                                              as Cust_Nm                       --客户名称
,coalesce(substr(t.NAT,5),'')                                           as Nation_Cd                     --国籍代码
,coalesce(substr(t.GENDER,5),'')                                        as Gender_Cd                     --性别代码
,'$MINDATE'::DATE 													                           as Birth_Dt
--t.BIRTH_DATE::date                                        as Birth_Dt                      --出生日期
,coalesce(substr(t.PRFSSN,5),'')                                                  as Carr_Cd                       --职业代码
,coalesce(substr(t.MARITAL_STATE,5),'')                                           as Marrg_Stat_Cd                 --婚姻状态代码
,coalesce(t.OPEN_ORG,'')                                                as Open_Acct_Org_Num             --开户机构号
,coalesce(t.OWN_ORG,'')                                                 as Belg_Org_Num                  --所属机构号
,coalesce(to_date(t1.OP_DATE,'YYYYMMDD'),'$MINDATE'::DATE)                             as Open_Acct_Dt                  --开户日期
,''                                                         as Cust_Typ_Cd                   --客户类型代码
,coalesce(substr(t.CUST_LEVEL,5),'')                                              as Cust_Lvl_Cd                   --客户等级代码
,''                                                         as Crdt_Card_Cust_Lvl_Cd         --信用卡客户等级代码
,coalesce(T.IS_VIP_FLAG,'')                                                    as Is_VIP                        --是否VIP
,coalesce(t2.CUST_CARD_LEVEL,'')                                         as Cust_Hi_Card_Hold_Lvl_Cd      --客户最高持卡级别代码
,coalesce(to_char(t3.WILL_VIP_FLAG),'')                                        as Is_Pros_VIP_Ind               --是否潜在VIP标志   --modified by wyh
--,null 
,0                                           as Fin_Asst_Totl_Amt--modify by dhy 20160802 t4.FIN_ASSET_AMT             --金融资产总额
,coalesce(t3.FI_EMP_FLAG,'')                                                    as Emp_Ind                       --员工标志
,coalesce(t.CUST_STATE,'')                                              as Cust_Stat_Cd                  --客户状态代码
,coalesce(t3.MANAGER_CODE,'')                                            as Cust_Mgr_Id                   --客户经理编号
,''                                                         as Cust_Div_Grp                  --客户分群
,coalesce(t5.TGT_CD,'@'||substr(T.CERT_TYPE,5),'')             as Cert_Typ_Cd                   --证件类型代码
,coalesce(T.CERT_NO,'')                                                  as Cert_Num                      --证件号码
,'ECF'                                                      as Sys_Src                       --系统来源
--from    (select *,row_number() over(partition by ECIF_CUST_NO  order by updated_ts desc) as num 
--from dw_sdata.ecf_001_t01_cust_info 
--where UPDATED_TS = '99991231 00:00:00:000000'
--and START_DT<='$TXDATE'::DATE
--and end_dt >'$TXDATE') t -- modified by wyh at 20160826
from tt_f_cust_indv_num_grp1 t
left join tt_f_cust_indv_op_date T1
--left join  ( select party_id,min(op_date) as op_date from dw_sdata.ecf_001_t02_cust_acct_rel where START_DT<='$TXDATE'::date  and END_DT>'$TXDATE'::date group by party_id ) t1
on       t.party_id=t1.party_id
--left join (select party_id,max(CUST_CARD_LEVEL) as CUST_CARD_LEVEL from dw_sdata.ecf_001_T01_CUST_CARD_LEVEL where  START_DT<='$TXDATE'::date AND END_DT>'$TXDATE'::date group by party_id ) t2
left join -- modify by dhy 20160802 
(SELECT party_id, 
        cust_card_level ,
        START_DT,
        END_DT,
        ROW_NUMBER() OVER(PARTITION BY party_id ORDER BY updated_ts desc , last_updated_ts DESC) AS NUM  -- modified by wyh at 20161019
 FROM dw_sdata.ecf_001_t01_cust_card_level 
 WHERE START_DT<='$TXDATE'::date 
   and '$TXDATE'::date < end_dt
 )T2
on       t.party_id=t2.party_id
and t2.num = 1
left join   (SELECT party_id, 
        WILL_VIP_FLAG,
        MANAGER_CODE,
        FI_EMP_FLAG,
        START_DT,
        END_DT,
        ROW_NUMBER() OVER(PARTITION BY party_id ORDER BY updated_ts desc , last_updated_ts DESC) AS NUM   -- modified by wyh at 20161019
 FROM dw_sdata.ecf_001_t01_cust_extend_info 
 WHERE START_DT<='$TXDATE'::date  and END_DT>'$TXDATE'::date) t3
on       t.party_id=t3.party_id
and      t3.num = 1
AND      T3.START_DT<='$TXDATE'::date
AND      T3.END_DT>'$TXDATE'::date
left join f_fdm.CD_RF_STD_CD_TRAN_REF T5
on       substr(T.CERT_TYPE,5)=T5.SRC_CD
and      T5.DATA_PLTF_SRC_TAB_NM = 'ECF_001_T01_CUST_INFO' 
and      T5.Data_Pltf_Src_Fld_Nm ='CERT_TYPE'
where (not exists
(select 1
from f_fdm.f_cust_indv T0
where t.ECIF_CUST_NO = T0.cust_num
and T0.etl_date = '$TXDATE'::date
))
--and t.num = 1
--where t.num = 1 
--t.UPDATED_TS = '99991231 00:00:00:000000'
--AND  T.START_DT<='$TXDATE'::date  --modified by wyh
--AND  T.END_DT>'$TXDATE'::date
;

/*数据处理区END*/
--MODIFIED BY WYH AT 20160826
/*重复数据处理区*/
--DELETE /* +direct */ from f_fdm.f_cust_indv
--where cust_num in
--(
--select Cust_Num 
--from f_fdm.f_cust_indv
--where etl_date = '$TXDATE'::date 
--group by 1
--having count(1) <> 1 
--)  
--and Grp_Typ = 1
--and etl_date = '$TXDATE'::date;
/*重复数据处理区END*/
commit
;

