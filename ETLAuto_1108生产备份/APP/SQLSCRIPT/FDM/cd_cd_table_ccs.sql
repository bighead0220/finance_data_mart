/*
Author             :朱明香
Function           :代码表整理
Load method        :INSERT
Source table       :CCS_004_TB_SYS_PRODUCT,CCS_004_TP_CMN_CODES  							                          
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
WHERE  Cd_Typ_Encd  in ('FDM038','FDM045','FDM044','FDM054')-----modified by wyh at 20160929
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
SELECT	'FDM038'														AS	Cd_Typ_Encd
				,'授信品种代码'											AS	Cd_Typ_Cn_Desc
				,PRODUCT_CD													AS	Cd
				,PRODUCT_NAME												AS	Cd_Desc
				,''																	AS	Memo
				,'2'										            AS	Cd_Load_Mode										 
				,'$TXDATE'::date 					AS	Modi_Dt
				,''																	AS	Modi_Reasn
FROM 		dw_sdata.CCS_004_TB_SYS_PRODUCT 
WHERE 	PRODUCT_LEVEL = '3'
  AND   START_DT <= '$TXDATE'::date 
  AND   '$TXDATE'::date < END_DT;

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
SELECT	'FDM045'														AS	Cd_Typ_Encd
				,'银团贷款类型'											AS	Cd_Typ_Cn_Desc
				,CODE_CD														AS	Cd
				,CODE_NAME													AS	Cd_Desc
				,''																	AS	Memo
				,'2'										            AS	Cd_Load_Mode										 
				,'$TXDATE'::date 					AS	Modi_Dt
				,''																	AS	Modi_Reasn
FROM 		dw_sdata.CCS_004_TP_CMN_CODES 
WHERE	 	CODE_TYPE_CD = 'BankCreditTypeCd'
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
SELECT	'FDM044'														AS	Cd_Typ_Encd
				,'融资模式'													AS	Cd_Typ_Cn_Desc
				,CODE_CD														AS	Cd
				,CODE_NAME													AS	Cd_Desc
				,''																	AS	Memo
				,'2'										            AS	Cd_Load_Mode										 
				,'$TXDATE'::date 					AS	Modi_Dt
				,''																	AS	Modi_Reasn
FROM 		dw_sdata.CCS_004_TP_CMN_CODES  
WHERE 	CODE_TYPE_CD = 'FinancingModelCd' 
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
SELECT	'FDM054'														AS	Cd_Typ_Encd
				,'公贷担保物类型代码'								AS	Cd_Typ_Cn_Desc
				,'CCS_'||CODE_CD										AS	Cd
				,CODE_NAME													AS	Cd_Desc
				,''																	AS	Memo
				,'2'										            AS	Cd_Load_Mode										 
				,'$TXDATE'::date 					AS	Modi_Dt
				,''																	AS	Modi_Reasn
FROM 		dw_sdata.CCS_004_TP_CMN_CODES 
WHERE 	CODE_TYPE_CD = 'CollateralTypeCd' 
  AND 	START_DT <= '$TXDATE'::date 
  AND 	'$TXDATE'::date < END_DT
;

/*数据处理区END*/
COMMIT;

