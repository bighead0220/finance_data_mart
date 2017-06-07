/*
Author             :朱明香
Function           :代码表整理
Load method        :INSERT
Source table       :cos_000_sectype，cos_000_bustruct                                  
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
WHERE  Cd_Typ_Encd  in ('FDM083','FDM084',
'FDM085','FDM087','FDM088')                  ---------------modified by wyh at 20160929
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
SELECT	'FDM083'														AS	Cd_Typ_Encd
				,'资金拆借产品代码'											AS	Cd_Typ_Cn_Desc
				,thekey													AS	Cd
				,name													AS	Cd_Desc
				,''																	AS	Memo
				,'2'										            AS	Cd_Load_Mode										 
				,'$TXDATE'::date 					AS	Modi_Dt
				,''																	AS	Modi_Reasn
FROM 		dw_sdata.cos_000_sectype 
WHERE 	name like '%拆借%' and thelevel='T' 
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
SELECT	'FDM084'														AS	Cd_Typ_Encd
				,'资金债券回购产品代码'											AS	Cd_Typ_Cn_Desc
				,thekey													AS	Cd
				,name													AS	Cd_Desc
				,''																	AS	Memo
				,'2'										            AS	Cd_Load_Mode										 
				,'$TXDATE'::date 					AS	Modi_Dt
				,''																	AS	Modi_Reasn
FROM 		dw_sdata.cos_000_sectype 
WHERE 	thekey in ('2690','2691') 
   AND 	start_dt <= '$TXDATE'::date 
  AND 	end_dt > '$TXDATE'::date 
   union
SELECT	'FDM084'														AS	Cd_Typ_Encd
				,'资金债券回购产品代码'											AS	Cd_Typ_Cn_Desc
				,thekey													AS	Cd
				,name													AS	Cd_Desc
				,''																	AS	Memo
				,'2'										            AS	Cd_Load_Mode										 
				,'$TXDATE'::date 					AS	Modi_Dt
				,''																	AS	Modi_Reasn
FROM 		dw_sdata.cos_000_sectype 
   where transtype='RA' 
   and name like '%回购%' 
   and thelevel='T' 
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
SELECT	'FDM085'														AS	Cd_Typ_Encd
				,'资金衍生产品代码'											AS	Cd_Typ_Cn_Desc
				,thekey													AS	Cd
				,name													AS	Cd_Desc
				,''																	AS	Memo
				,'2'										            AS	Cd_Load_Mode										 
				,'$TXDATE'::date 					AS	Modi_Dt
				,''																	AS	Modi_Reasn
FROM 		dw_sdata.cos_000_sectype 
WHERE 	transtype in ('SC','SI') 
  and    thelevel='T' 
  AND 	start_dt <= '$TXDATE'::date 
  AND 	end_dt > '$TXDATE'::date  --SC：掉期类-货币，SI：掉期类-利率
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
SELECT distinct
       	'FDM087'														AS	Cd_Typ_Encd
				,'资金债券类型代码'											AS	Cd_Typ_Cn_Desc
				,t3.thekey													AS	Cd
				,t3.name													AS	Cd_Desc
				,''																	AS	Memo
				,'2'										            AS	Cd_Load_Mode										 
				,'$TXDATE'::date 					AS	Modi_Dt
				,''																	AS	Modi_Reasn
from dw_sdata.cos_000_sectype t1  
inner join f_fdm.cd_cd_table t2
        on t1.thekey=t2.cd
       and t2.Cd_Typ_Encd='FDM086'
inner join dw_sdata.cos_000_sectype t3  
        on t1.owner=t3.thekey
       and T3.start_dt <= '$TXDATE'::date 
        and T3.end_dt > '$TXDATE'::date 
where T1.start_dt <= '$TXDATE'::date  
and T1.end_dt > '$TXDATE'::date 
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
SELECT	'FDM088'														AS	Cd_Typ_Encd
				,'资金交易组合代码'											AS	Cd_Typ_Cn_Desc
				,thekey													AS	Cd
				,name													AS	Cd_Desc
				,''																	AS	Memo
				,'2'										            AS	Cd_Load_Mode										 
				,'$TXDATE'::date 					AS	Modi_Dt
				,''																	AS	Modi_Reasn
FROM 		dw_sdata.cos_000_bustruct 
WHERE   start_dt <= '$TXDATE'::date 
  AND 	end_dt > '$TXDATE'::date  
;


/*数据处理区END*/
COMMIT;

