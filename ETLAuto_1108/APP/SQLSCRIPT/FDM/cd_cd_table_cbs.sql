/*
Author             :朱明香
Function           :代码表整理
Load method        :INSERT
Source table       :CBS_001_PMCTL_KIND_TYPE,CBS_001_SMCTL_CHNL_CODE                            
Destination Table  :CD_CD_TABLE
Frequency          :
Modify history list:
                   :modified by wyh at 20160929 delete
-------------------------------------------逻辑说明---------------------------------------------
业务逻辑说明
-------------------------------------------逻辑说明END------------------------------------------
*/
/*临时表创建区*/

/*临时表创建区END*/
/*数据回退区*/
DELETE FROM f_fdm.CD_CD_TABLE
WHERE  Cd_Typ_Encd  in ('FDM051','FDM071') ---------------- modified by wyh at 20160929
;
/*数据回退区END*/
/*数据处理区*/
INSERT INTO f_fdm.CD_CD_TABLE
	      (Cd_Typ_Encd												--代码类型编码
		  	,Cd_Typ_Cn_Desc									    --代码类型中文描述
		 		,Cd                    							--代码值
				,Cd_Desc						                --代码描述
				,Memo							                  --备注
				,Cd_Load_Mode											  --代码加载方式
				,Modi_Dt									          --修改日期
				,Modi_Reasn								          --修改原因
				)
SELECT	'FDM051'														AS	Cd_Typ_Encd
				,'存款类别'													AS	Cd_Typ_Cn_Desc
				,DEP_CODE														AS	Cd
				,KINDTYPE_NAME											AS	Cd_Desc
				,''																	AS	Memo
				,'2'										            AS	Cd_Load_Mode 										 
				,'$TXDATE'::date 					AS	Modi_Dt
				,''																	AS	Modi_Reasn
FROM		dw_sdata.CBS_001_PMCTL_KIND_TYPE 
WHERE		START_DT <= '$TXDATE'::date 
  AND	  '$TXDATE'::date < END_DT
;
INSERT INTO f_fdm.CD_CD_TABLE
	      (Cd_Typ_Encd												--代码类型编码
		  	,Cd_Typ_Cn_Desc									    --代码类型中文描述
		 		,Cd                    							--代码值
				,Cd_Desc						                --代码描述
				,Memo							                  --备注
				,Cd_Load_Mode											  --代码加载方式
				,Modi_Dt									          --修改日期
				,Modi_Reasn								          --修改原因
				)
SELECT	'FDM071'														AS	Cd_Typ_Encd
				,'交易渠道代码'											AS	Cd_Typ_Cn_Desc
				,'CBS_'||CHNL_CODE									AS	Cd
				,CHNL_NAME													AS	Cd_Desc
				,''																	AS	Memo
				,'2'										            AS	Cd_Load_Mode										
				,'$TXDATE'::date 					AS	Modi_Dt
				,''																	AS	Modi_Reasn
FROM 		dw_sdata.CBS_001_SMCTL_CHNL_CODE 
WHERE 	START_DT <= '$TXDATE'::date 
  AND 	'$TXDATE'::date < END_DT
;

/*数据处理区END*/
COMMIT;

