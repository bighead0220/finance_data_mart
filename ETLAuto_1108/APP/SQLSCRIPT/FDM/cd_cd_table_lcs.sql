/*
Author             :朱明香
Function           :代码表整理
Load method        :INSERT
Source table       :LCS_000_CARD_PUB_SVT,LCS_000_CARD_PUB_FC_SVT                             
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
WHERE  Cd_Typ_Encd  IN ('FDM047','FDM048')
;----------------------------modified by wyh at 20160929
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
SELECT	'FDM047'														AS	Cd_Typ_Encd
				,'储种（本币）'											AS	Cd_Typ_Cn_Desc
				,SVT_NO															AS	Cd
				,SVT_NAME														AS	Cd_Desc
				,''																	AS	Memo
				,'2'										            AS	Cd_Load_Mode 
				,'$TXDATE'::date 					AS	Modi_Dt
				,''																	AS	Modi_Reasn
FROM 		dw_sdata.LCS_000_CARD_PUB_SVT 
WHERE 	START_DT <= '$TXDATE'::date 
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
SELECT	DISTINCT 
				'FDM048'														AS	Cd_Typ_Encd
				,'储种（外币）'											AS	Cd_Typ_Cn_Desc
				,SVT_NO										AS	Cd
				,SVT_NAME														AS	Cd_Desc
				,''																	AS	Memo
				,'2'										            AS	Cd_Load_Mode 
				,'$TXDATE'::date 					AS	Modi_Dt
				,''																	AS	Modi_Reasn
FROM 		dw_sdata.LCS_000_CARD_PUB_FC_SVT 
WHERE 	START_DT <= '$TXDATE'::date 
  AND 	'$TXDATE'::date < END_DT
;

/*数据处理区END*/
COMMIT;

