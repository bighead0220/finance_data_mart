/*
Author             :朱明香
Function           :代码表整理
Load method        :INSERT
Source table       :PCS_001_TB_PUB_APPLICATIONCONFIG,PCS_001_TC_PUB_DICSUB,PCS_006_TB_AST_ASSETTYPE                              
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
WHERE Cd_Typ_Encd  IN ('FDM027','FDM041'
,'FDM040','FDM053','FDM037')
;                                   ---------------------------- modified by  wyh at 20160929
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
SELECT	'FDM027'														AS	Cd_Typ_Encd
				,'个人贷款业务种类ID'								AS	Cd_Typ_Cn_Desc
				,APP_OP_ID													AS	Cd
				,APP_OP_NAME												AS	Cd_Desc
				,''																	AS	Memo
				,'2'										            AS	Cd_Load_Mode 
				,'$TXDATE'::date 					AS	Modi_Dt
				,''																	AS	Modi_Reasn
FROM  	dw_sdata.PCS_001_TB_PUB_APPLICATIONCONFIG
WHERE 	APP_OP_TYPE = '3' 
  AND 	start_dt <= '$TXDATE'::date
  AND 	end_dt > '$TXDATE'::date
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
SELECT	'FDM041'														AS	Cd_Typ_Encd
				,'额度循环标志'											AS	Cd_Typ_Cn_Desc
				,subdic_cd													AS	Cd
				,subdic_name												AS	Cd_Desc
				,''																	AS	Memo
				,'2'										            AS	Cd_Load_Mode 
				,'$TXDATE'::date 					AS	Modi_Dt
				,''																	AS	Modi_Reasn
FROM 		dw_sdata.PCS_001_TC_PUB_DICSUB 
WHERE		BIGCLASS_CD = 'CYCLE_FLAG' 
  AND 	START_DT <= '$TXDATE'::date 
  AND 	'$TXDATE'::date < END_DT 
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
SELECT	'FDM040'														AS	Cd_Typ_Encd
				,'额度业务种类代码'									AS	Cd_Typ_Cn_Desc
				,APP_OP_ID													AS	Cd
				,APP_OP_NAME												AS	Cd_Desc
				,''																	AS	Memo
				,'2'										            AS	Cd_Load_Mode 
				,'$TXDATE'::date 					AS	Modi_Dt
				,''																	AS	Modi_Reasn
FROM  	dw_sdata.PCS_001_TB_PUB_APPLICATIONCONFIG 
WHERE 	APP_OP_TYPE = '2' 
  AND	  START_DT <= '$TXDATE'::date 
  AND   '$TXDATE'::date < END_DT
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
SELECT	'FDM053'														AS	Cd_Typ_Encd
				,'个贷担保物类型代码'								AS	Cd_Typ_Cn_Desc
				,'PCS_'||ASSETTYPE_ID								AS	Cd
				,ASSETTYPE_NAME											AS	Cd_Desc
				,''																	AS	Memo
				,'2'										            AS	Cd_Load_Mode 
				,'$TXDATE'::date 					AS	Modi_Dt
				,''																	AS	Modi_Reasn
FROM  	dw_sdata.PCS_006_TB_AST_ASSETTYPE 
WHERE 	START_DT <= '$TXDATE'::date 
  and 	'$TXDATE'::date < END_DT
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
SELECT	'FDM037'														AS	Cd_Typ_Encd
				,'个人贷款特色业务类型'							AS	Cd_Typ_Cn_Desc
				,subdic_cd													AS	Cd
				,subdic_name												AS	Cd_Desc
				,''																	AS	Memo
				,'2'										            AS	Cd_Load_Mode 
				,'$TXDATE'::date 					AS	Modi_Dt
				,''																	AS	Modi_Reasn
FROM	  dw_sdata.PCS_001_TC_PUB_DICSUB
WHERE	  BIGCLASS_CD = 'CHARACTERISTIC_APP_TYPE'
 ;


/*数据处理区END*/
COMMIT;

