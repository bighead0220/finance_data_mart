/*
Author             :朱明香
Function           :代码表整理
Load method        :INSERT
Source table       :CCB_000_PRMCD,CCB_000_CLOCD,CCB_000_CODES,CCB_000_TRDEF                     
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
WHERE  Cd_Typ_Encd  in('FDM056','FDM057','FDM058','FDM099')------modified by wyh at 20160929
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
SELECT	'FDM056'														AS	Cd_Typ_Encd
				,'卡片种类代码'											AS	Cd_Typ_Cn_Desc
				,PRODUCT														AS	Cd
				,PROD_DESC													AS	Cd_Desc
				,''																	AS	Memo
				,'2'										            AS	Cd_Load_Mode										 
				,'$TXDATE'::date 					AS	Modi_Dt
				,''																	AS	Modi_Reasn
FROM 		dw_sdata.CCB_000_PRMCD 
WHERE 	START_DT <= '$TXDATE'::date 
  AND	 	'$TXDATE'::date < END_DT
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
SELECT	'FDM057'														AS	Cd_Typ_Encd
				,'信用卡/账户状态代码'							AS	Cd_Typ_Cn_Desc
				,CLOSE_CD														AS	Cd
				,CLOSE_DESC													AS	Cd_Desc
				,''																	AS	Memo
				,'2'										            AS	Cd_Load_Mode										 
				,'$TXDATE'::date 					AS	Modi_Dt
				,''																	AS	Modi_Reasn
FROM 		dw_sdata.CCB_000_CLOCD 
WHERE		START_DT <= '$TXDATE'::date 
 	AND 	'$TXDATE'::date  < END_DT
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
SELECT	'FDM058'														AS	Cd_Typ_Encd
				,'信用卡审核原因代码'								AS	Cd_Typ_Cn_Desc
				,SUBSTR(CODE_VALUE,2)								AS	Cd
				,VALUE_DEF													AS	Cd_Desc
				,''																	AS	Memo
				,'2'										            AS	Cd_Load_Mode										 
				,'$TXDATE'::date 					AS	Modi_Dt
				,''																	AS	Modi_Reasn
FROM 	 	dw_sdata.CCB_000_CODES 
WHERE 	CODE_TYPE = 'APDEC' 
  AND	 	START_DT <= '$TXDATE'::date 
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
SELECT	'FDM099'														AS	Cd_Typ_Encd
				,'交易类型代码'											AS	Cd_Typ_Cn_Desc
				,TRANS_TYPE||'_'||O_R_FLAG						AS	Cd
				,DESC_PRINT													AS	Cd_Desc
				,''																	AS	Memo
				,'2'										            AS	Cd_Load_Mode										 
				,'$TXDATE'::date 					AS	Modi_Dt
				,''																	AS	Modi_Reasn
FROM 		dw_sdata.CCB_000_TRDEF 
WHERE 	START_DT <= '$TXDATE'::date 
  AND 	'$TXDATE'::date < END_DT
;

/*数据处理区END*/
COMMIT;

