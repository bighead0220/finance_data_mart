 /*
Author             :dhy
Function           :
Load method        :
Source table       :dw_sdata.OGS_000_TBL_BRH_KEY_INFO,dw_sdata.OGS_000_TBL_FINC_BRH_MEM_INFO,dw_sdata.ogs_tbl_finc_brh_field_info,dw_sdata.ogs_TBL_FINC_BRH_BASIC_INFO,dw_sdata.OGS_TBL_POST_BRH_INFO
Destination Table  :f_fdm.f_org_info
Frequency          :D
Modify history list:Created by xsh 20160729    
										modified by liuxz 20160823 增加关联代码转换表T2               
                    modified by zhangliang at 20160912 修改了字段映射规则，增加8个字段，删除1个字段，变更编号#200;                 
*/

-------------------------------------------逻辑说明---------------------------------------------
/*
 
*/
-------------------------------------------逻辑说明END---------------------------------------
/*临时表创建区*/
CREATE local TEMP TABLE IF NOT EXISTS  tt_brh_num 
(
 brh_code varchar(30),
 cnt numeric
)ON COMMIT PRESERVE ROWS;   --lxz: delete -> preserve 20150513
;
/*临时表创建


/*数据回退区*/
delete from  f_fdm.f_org_info where etl_date ='$TXDATE'::date ;

/*数据回退区END*/
/*数据处理区*/
-- 加工
insert into tt_brh_num 
select trim(brh_code), sum(cnt)
  from (select brh_code, count(1) cnt -- 5 级别
          from dw_sdata.OGS_000_TBL_BRH_KEY_INFO a
         where brh_type = '01'
           and brh_lvl = '05'
           and a.start_dt<='$TXDATE'::date 
           and a.end_dt>'$TXDATE'::date 
         group by 1
        union all
        select up_brh_code as brh_code, count(1) -- 4 级
          from dw_sdata.OGS_000_TBL_BRH_KEY_INFO a
         where a.brh_type = '01'
           and a.brh_lvl = '05'
           and a.start_dt<='$TXDATE'::date 
           and a.end_dt>'$TXDATE'::date 
         group by 1
        union all
        select p.up_brh_code as brh_code, sum(b.cnt) -- 3 级别 
          from 
              (select * from dw_sdata.OGS_000_TBL_BRH_KEY_INFO a
               where a.start_dt<='$TXDATE'::date 
               and a.end_dt>'$TXDATE'::date )p
          left join 
                    (select up_brh_code as brh_code, count(1) cnt -- 4 级
                       from dw_sdata.OGS_000_TBL_BRH_KEY_INFO a
                      where a.brh_type = '01'
                        and a.brh_lvl = '05'
                        and a.start_dt<='$TXDATE'::date 
                        and a.end_dt>'$TXDATE'::date 
                      group by 1) b
            on p.brh_code = b.brh_code
         where p.brh_type = '01'
           and p.brh_lvl <> '05'
           and p.start_dt<='$TXDATE'::date 
           and p.end_dt>'$TXDATE'::date 
         group by 1                  
        union all
        select q.up_brh_code, sum(a.cnt)
           from 
            (select * from dw_sdata.OGS_000_TBL_BRH_KEY_INFO t --机构关键信息表
             where t.start_dt<='$TXDATE'::date 
             and t.end_dt>'$TXDATE'::date )q
            left join                     
                      (select t.up_brh_code brh_code, sum(b.cnt) cnt -- 3 级别
                       from 
                         (select * from dw_sdata.OGS_000_TBL_BRH_KEY_INFO a 
                           where a.start_dt<='$TXDATE'::date 
                           and a.end_dt>'$TXDATE'::date )t 
                                              
                       left join 
                       
                                 (select up_brh_code as brh_code, count(1) cnt -- 4 级
                                   from dw_sdata.OGS_000_TBL_BRH_KEY_INFO a
                                  where a.brh_type = '01'
                                    and a.brh_lvl = '05'
                                    and a.start_dt<='$TXDATE'::date 
                                    and a.end_dt>'$TXDATE'::date 
                                  group by 1) b
                         on t.brh_code = b.brh_code
                      where t.brh_type = '01'
                        and t.brh_lvl <> '05'
                      group by 1) a
            on q.brh_code = a.brh_code
         where q.brh_type = '01'
           and q.brh_lvl in ('02', '03')         
         group by 1
        ) tt
 group by 1
 ; 

;

-- 插入
insert/*+ direct */ into f_fdm.f_org_info
(
etl_date
,Grp_Typ
,org_cd
,org_nm
,org_hrcy
,org_sht_nm
,admin_lvl
,org_typ
,org_func
,org_attr
,org_stat_cd
,regn_attr
,admin_div_cd
,org_cnt
,pers_cnt
,up_org_cd
,up_org_nm
,is_dtl_org
,st_use_dt
,invldtn_dt
,econ_regn_attr_cd
,is_nt_indpd
,brch_biz_area
,post_biz_area
,biz_area_prop
,offi_area_prop
,offi_area
,func_prtn
,leas_matr_tm
,seat_cnt
,zip_cd
,addr
,sys_src
) 
 
select
'$TXDATE'::date                                                  as ETL_Date                      --数据日期      
,'1'                                                             as grp_typ                       --组别            
,t.BRH_CODE                                                      as org_cd                        --机构代码        
,t.BRH_NAME                                                      as org_nm                        --机构名称        
,t.BRH_LVL                                                       as org_hrcy                      --机构层级        
,t.BRH_SHT_NAME                                                  as org_sht_nm                    --机构简称        
,''                                                              as admin_lvl                     --行政级别        
,coalesce(a.tgt_cd,'@'||t.BRH_TYPE)                              as org_typ                       --机构类型        
,t.BRH_ABLI                                                      as org_func                      --机构职能        
,t.BRH_ATTR                                                      as org_attr                      --机构属性        
,t.BRH_STAT                                                      as org_stat_cd                   --机构状态代码    
,t.AREA_ATTR                                                     as regn_attr                     --地域属性        
,t.RGLM_CODE                                                     as admin_div_cd                  --行政区划代码    
,d.cnt                                                           as org_cnt                       --机构数         (未获取)
,t1.GUA_NUM                                                      as pers_cnt                      --人数            
,t.UP_BRH_CODE                                                   as up_org_cd                     --上级机构代码    
,coalesce(t2.BRH_NAME,'')                                        as up_org_nm                     --上级机构名称    
,case when t.BRH_LVL='05' then '1' else '0'  end                 as is_dtl_org                    --是否明细机构    
,null                                                            as st_use_dt                     --启用日期        
,null                                                            as invldtn_dt                    --失效日期        
,null                                                            as econ_regn_attr_cd             --经济区域属性代码
,T3.INDP_SHOP -- 是否独立门面
,T3.NET_BUS_AREA -- 网点营业面积
,T3.POST_BUS_AREA -- 邮政营业面积
,T3.BUS_FIELD_PROPT -- 营业场地所有权
,T3.OFFICE_FIELD_PROPT-- 办公场地所有权
,T3.OFFICE_AREA-- 办公面积
,T3.FUNC_SUB_AREA -- 功能分区
,T3.HIRE_END_DT -- 租赁到期时间
--,0/*t2.NET_BUS_AREA*/                                            as area                          --面积            (待有表放开)      
,T3.CASH_BORD_NUM 
 + T3.NO_CASH_BORD_NUM 
 + T3.CASH_BORD_FACT_NUM 
 + T3.VIP_CASH_BORD_NUM  /*t2.CASH_BORD_NUM*/                                           as seat_cnt                      --台席数          (待有表放开)  
,t4.POST_CODE                                                    as zip_cd                        --邮政编码        (待有表放开)
,t.BRH_ADDR                                                      as addr                          --地址            
,'OGS'                                                           as sys_src                       --系统来源        
 from dw_sdata.ogs_000_tbl_brh_key_info T
left join dw_sdata.ogs_000_tbl_finc_brh_mem_info T1
on T.BRH_CODE=T1.BRH_CODE
and T1.START_DT<='$TXDATE'::date
and T1.end_dt>'$TXDATE'::date
LEFT JOIN f_fdm.CD_RF_STD_CD_TRAN_REF a
ON      T.BRH_TYPE = a.SRC_CD                       --源代码值相同
AND     a.DATA_PLTF_SRC_TAB_NM =upper('ogs_000_tbl_brh_key_info')--数据平台源表主干名
AND     a.Data_Pltf_Src_Fld_Nm =upper('BRH_TYPE')
left join dw_sdata.ogs_000_tbl_brh_key_info T2
on t.up_brh_code=t2.brh_code
and T2.START_DT<='$TXDATE'::date
and T2.end_dt > '$TXDATE'::date
left join  dw_sdata.OGS_000_TBL_FINC_BRH_FIELD_INFO T3    --modified 20160912
on T.BRH_CODE=T3.BRH_CODE 
and T3.START_DT<='$TXDATE'::date
and T3.end_dt > '$TXDATE'::date
left join (select brh_code,post_code 
             from dw_sdata.ogs_000_tbl_finc_brh_basic_info
             where START_DT<='$TXDATE'::date
               and end_dt > '$TXDATE'::date) T4
on t.brh_code=t4.brh_code
left join tt_brh_num d 
    on t.brh_code = d.brh_code 
where T.START_DT<='$TXDATE'::date 
  and T.end_dt > '$TXDATE'::date
;
/*数据处理区END*/

