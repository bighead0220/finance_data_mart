/*
Author             :朱明香
Function           :代码表整理
Load method        :INSERT
Source table       :ECF_003_CODE_STRING                    
Destination Table  :CD_CD_TABLE
Frequency          :
Modify history list:
                   :modified by wyh at 20160929 delete
                    MODIFIED BY ZMX AT 20161008 添加AND   CODE_FLAG = '1'  
-------------------------------------------逻辑说明---------------------------------------------
业务逻辑说明
-------------------------------------------逻辑说明END------------------------------------------
*/
/*临时表创建区*/

/*临时表创建区END*/
/*数据回退区*/
DELETE FROM f_fdm.CD_CD_TABLE
WHERE Cd_Typ_Encd  IN ('FDM015','FDM016','FDM020'
,'FDM021','FDM022','FDM023','FDM024'
,'FDM014','FDM018','FDM026')               ------------------------modified by wyh at 20160929
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
SELECT	'FDM015'														AS	Cd_Typ_Encd
				,'行业种类代码'											AS	Cd_Typ_Cn_Desc
				,CODE_VALUE													AS	Cd
				,CODE_DESC													AS	Cd_Desc
				,''																	AS	Memo
				,'2'										            AS	Cd_Load_Mode										 
				,'$TXDATE'::date 					AS	Modi_Dt
				,''																	AS	Modi_Reasn
FROM 		dw_sdata.ECF_003_CODE_STRING 
WHERE 	CODE_TYPE = 'C903' 
  AND 	START_DT <= '$TXDATE'::date 
  AND 	'$TXDATE'::date < END_DT 
  AND   CODE_FLAG = '1'   
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
SELECT	'FDM016'														AS	Cd_Typ_Encd
				,'企业规模'													AS	Cd_Typ_Cn_Desc
				,CODE_VALUE													AS	Cd
				,CODE_DESC													AS	Cd_Desc
				,''																	AS	Memo
				,'2'										            AS	Cd_Load_Mode										 
				,'$TXDATE'::date 					AS	Modi_Dt
				,''																	AS	Modi_Reasn
FROM 		dw_sdata.ECF_003_CODE_STRING 
WHERE		CODE_TYPE = 'C105' 
  AND 	START_DT <= '$TXDATE'::date 
  AND 	'$TXDATE'::date < END_DT
  AND   CODE_FLAG = '1' 
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
SELECT  'FDM020'														AS	Cd_Typ_Encd
				,'企业性质代码'											AS	Cd_Typ_Cn_Desc
				,CODE_VALUE													AS	Cd
				,CODE_DESC													AS	Cd_Desc
				,''																	AS	Memo
				,'2'										            AS	Cd_Load_Mode										 
				,'$TXDATE'::date 					AS	Modi_Dt
				,''																	AS	Modi_Reasn
FROM 	  dw_sdata.ECF_003_CODE_STRING 
WHERE   CODE_TYPE = 'C103' 
  AND START_DT <= '$TXDATE'::date 
  AND '$TXDATE'::date < END_DT
  AND   CODE_FLAG = '1' 
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
SELECT	'FDM021'														AS	Cd_Typ_Encd
				,'国籍代码'													AS	Cd_Typ_Cn_Desc
				,CODE_VALUE													AS	Cd
				,CODE_DESC													AS	Cd_Desc
				,''																	AS	Memo
				,'2'										            AS	Cd_Load_Mode										 
				,'$TXDATE'::date 					AS	Modi_Dt
				,''																	AS	Modi_Reasn
FROM dw_sdata.ECF_003_CODE_STRING 
WHERE CODE_TYPE = 'C902' 
  AND START_DT <= '$TXDATE'::date 
  AND '$TXDATE'::date < END_DT
  AND   CODE_FLAG = '1' 
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
SELECT	'FDM022'														AS	Cd_Typ_Encd
				,'职业代码'													AS	Cd_Typ_Cn_Desc
				,CODE_VALUE													AS	Cd
				,CODE_DESC													AS	Cd_Desc
				,''																	AS	Memo
				,'2'										            AS	Cd_Load_Mode										 
				,'$TXDATE'::date 					AS	Modi_Dt
				,''																	AS	Modi_Reasn
FROM 		dw_sdata.ECF_003_CODE_STRING 
WHERE 	CODE_TYPE = 'C005' 
  AND 	START_DT <= '$TXDATE'::date 
  AND 	'$TXDATE'::date < END_DT
  AND   CODE_FLAG = '1' 
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
SELECT	'FDM023'														AS	Cd_Typ_Encd
				,'婚姻状态'													AS	Cd_Typ_Cn_Desc
				,CODE_VALUE													AS	Cd
				,CODE_DESC													AS	Cd_Desc
				,''																	AS	Memo
				,'2'										            AS	Cd_Load_Mode										 
				,'$TXDATE'::date 					AS	Modi_Dt
				,''																	AS	Modi_Reasn
FROM 		dw_sdata.ECF_003_CODE_STRING 
WHERE 	CODE_TYPE = 'C002' 
  and 	START_DT <= '$TXDATE'::date 
  and 	'$TXDATE'::date < END_DT
  AND   CODE_FLAG = '1' 
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
SELECT	'FDM024'														AS	Cd_Typ_Encd
				,'客户等级代码'											AS	Cd_Typ_Cn_Desc
				,CODE_VALUE													AS	Cd
				,CODE_DESC													AS	Cd_Desc
				,''																	AS	Memo
				,'2'										            AS	Cd_Load_Mode										 
				,'$TXDATE'::date 					AS	Modi_Dt
				,''																	AS	Modi_Reasn
FROM		 dw_sdata.ECF_003_CODE_STRING 
WHERE 	CODE_TYPE = 'C013' 
  and 	START_DT <= '$TXDATE'::date 
  and 	'$TXDATE'::date < END_DT
  AND   CODE_FLAG = '1' 
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
SELECT	'FDM014'														AS	Cd_Typ_Encd
				,'公司客户类型代码'									AS	Cd_Typ_Cn_Desc
				,CODE_VALUE													AS	Cd
				,CODE_DESC													AS	Cd_Desc
				,''																	AS	Memo
				,'2'										            AS	Cd_Load_Mode										 
				,'$TXDATE'::date 					AS	Modi_Dt
				,''																	AS	Modi_Reasn
FROM 		dw_sdata.ECF_003_CODE_STRING 
WHERE 	CODE_TYPE = 'C101' 
  AND 	START_DT <= '$TXDATE'::date 
  AND 	'$TXDATE'::date < END_DT
  AND   CODE_FLAG = '1' 
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
SELECT	'FDM018'														AS	Cd_Typ_Encd
				,'企业信用等级代码'									AS	Cd_Typ_Cn_Desc
				,CODE_VALUE													AS	Cd
				,CODE_DESC													AS	Cd_Desc
				,''																	AS	Memo
				,'2'										            AS	Cd_Load_Mode										 
				,'$TXDATE'::date 					AS	Modi_Dt
				,''																	AS	Modi_Reasn
FROM 		dw_sdata.ECF_003_CODE_STRING 
WHERE	  CODE_TYPE = 'C107' 
  AND		START_DT <= '$TXDATE'::date 
  AND 	'$TXDATE'::date < END_DT
  AND   CODE_FLAG = '1' 
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
SELECT	'FDM026'														AS	Cd_Typ_Encd
				,'同业客户类型代码'									AS	Cd_Typ_Cn_Desc
				,CODE_VALUE													AS	Cd
				,CODE_DESC													AS	Cd_Desc
				,''																	AS	Memo
				,'2'										            AS	Cd_Load_Mode										 
				,'$TXDATE'::date 					AS	Modi_Dt
				,''																	AS	Modi_Reasn
FROM 		dw_sdata.ECF_003_CODE_STRING 
WHERE 	CODE_TYPE = 'C201' 
  AND  	START_DT <= '$TXDATE'::date 
  AND 	'$TXDATE'::date < END_DT
  AND   CODE_FLAG = '1' 
;

/*数据处理区END*/
COMMIT;

