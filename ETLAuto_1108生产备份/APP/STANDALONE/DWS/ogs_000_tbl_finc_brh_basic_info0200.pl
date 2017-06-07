#!/usr/bin/perl

use strict;     # Declare using Perl strict syntax
use DBI;        # If you are using other package, declare here

# ------------ Variable Section ------------
my ${AUTO_HOME} = $ENV{"AUTO_HOME"};

my ${WML_DB} = $ENV{"AUTO_WML_DB"};
if ( !defined(${WML_DB}) ) {
    ${WML_DB} = "WML";
}
my ${WTL_DB} = $ENV{"AUTO_WTL_DB"};
if ( !defined(${WTL_DB}) ) {
    ${WTL_DB} = "WTL";
}
my ${WMLVIEW_DB} = $ENV{"AUTO_WMLVIEW_DB"};
if ( !defined(${WMLVIEW_DB}) ) {
    ${WMLVIEW_DB} = "WMLVIEW";
}
my ${WTLVIEW_DB} = $ENV{"AUTO_WTLVIEW_DB"};
if ( !defined(${WTLVIEW_DB}) ) {
    ${WTLVIEW_DB} = "WTLVIEW";
}

my ${NULL_DATE} = "1900-01-02";
my ${MIN_DATE} = "1900-01-01";
my ${MAX_DATE} = "2100-12-31";

my ${LOGON_FILE} = "${AUTO_HOME}/etc/VERTICA_LOGON";
my ${LOGON_STR};
my ${CONTROL_FILE};
my ${TX_DATE};
my ${TX_DATE_YYYYMMDD};
my ${TX_MON_DAY_MMDD};

# ------------ VSQL function ------------
sub run_vsql_command
{
  #my $rc = open(VSQL, "${LOGON_STR}");
  my $rc = open(VSQL, "|vsql -h 22.224.65.171 -p 5433 -d CPCIMDB_TEST -U dwtrans -w dwtrans2016");

  unless ($rc) {
      print "Could not invoke VSQL command
";
      return -1;
  }

# ------ Below are VSQL scripts ----------
  print VSQL <<ENDOFINPUT;

\\set ON_ERROR_STOP on

--Step0:
DELETE FROM dw_sdata.OGS_000_TBL_FINC_BRH_BASIC_INFO WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.OGS_000_TBL_FINC_BRH_BASIC_INFO SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_356 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.OGS_000_TBL_FINC_BRH_BASIC_INFO WHERE 1=0;

--Step2:
INSERT  INTO T_356 (
  BRH_CODE,
  PRE_DEAL_ID,
  PRE_APRV_NO,
  OPEN_APRV_NO,
  OPEN_APRV_DT,
  FINC_LICS_NO,
  LICS_ADDR,
  BRH_STYLE,
  PBC_BRH_LVL,
  SET_DT,
  OPEN_DT,
  BUS_ACS_DT,
  POST_CODE,
  MNG_DUTY_NAME,
  MNG_DUTY_TEL,
  BUS_TM,
  NET_DUTY_NAME,
  NET_DUTY_TEL,
  BLG_PBC_NAME,
  BLG_PBC_CODE,
  BRH_EMAIL,
  VIP_SERV,
  PROXY_POST_BUS,
  SELF_SERV_TYPE,
  PAY_NET_FLAG,
  AGR_NET_FLAG,
  FORE_COST_ER,
  FORE_PEPO_QUAR_BALA_PD,
  FORE_QUAR_BUS_PROFIT,
  FORE_FIPR_QUAR_BALA,
  SA_EXCHG_BRH_CODE,
  SA_EXCHG_BRH_NAME,
  SA_SELF_BANK_CODE,
  SA_SELF_BANK_NAME,
  SIGN_BANK_PRO,
  SAVE_FLAG,
  DRAW_FLAG,
  PAY_FLAG,
  NEAR_BUILD,
  LAST_UPD_TM,
  LAST_UPD_OPR,
  LAST_UPD_TXN,
  RESV1,
  start_dt,
  end_dt)
SELECT
  N.BRH_CODE,
  N.PRE_DEAL_ID,
  N.PRE_APRV_NO,
  N.OPEN_APRV_NO,
  N.OPEN_APRV_DT,
  N.FINC_LICS_NO,
  N.LICS_ADDR,
  N.BRH_STYLE,
  N.PBC_BRH_LVL,
  N.SET_DT,
  N.OPEN_DT,
  N.BUS_ACS_DT,
  N.POST_CODE,
  N.MNG_DUTY_NAME,
  N.MNG_DUTY_TEL,
  N.BUS_TM,
  N.NET_DUTY_NAME,
  N.NET_DUTY_TEL,
  N.BLG_PBC_NAME,
  N.BLG_PBC_CODE,
  N.BRH_EMAIL,
  N.VIP_SERV,
  N.PROXY_POST_BUS,
  N.SELF_SERV_TYPE,
  N.PAY_NET_FLAG,
  N.AGR_NET_FLAG,
  N.FORE_COST_ER,
  N.FORE_PEPO_QUAR_BALA_PD,
  N.FORE_QUAR_BUS_PROFIT,
  N.FORE_FIPR_QUAR_BALA,
  N.SA_EXCHG_BRH_CODE,
  N.SA_EXCHG_BRH_NAME,
  N.SA_SELF_BANK_CODE,
  N.SA_SELF_BANK_NAME,
  N.SIGN_BANK_PRO,
  N.SAVE_FLAG,
  N.DRAW_FLAG,
  N.PAY_FLAG,
  N.NEAR_BUILD,
  N.LAST_UPD_TM,
  N.LAST_UPD_OPR,
  N.LAST_UPD_TXN,
  N.RESV1,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(BRH_CODE, '' ) AS BRH_CODE ,
  COALESCE(PRE_DEAL_ID, '' ) AS PRE_DEAL_ID ,
  COALESCE(PRE_APRV_NO, '' ) AS PRE_APRV_NO ,
  COALESCE(OPEN_APRV_NO, '' ) AS OPEN_APRV_NO ,
  COALESCE(OPEN_APRV_DT, '' ) AS OPEN_APRV_DT ,
  COALESCE(FINC_LICS_NO, '' ) AS FINC_LICS_NO ,
  COALESCE(LICS_ADDR, '' ) AS LICS_ADDR ,
  COALESCE(BRH_STYLE, '' ) AS BRH_STYLE ,
  COALESCE(PBC_BRH_LVL, '' ) AS PBC_BRH_LVL ,
  COALESCE(SET_DT, '' ) AS SET_DT ,
  COALESCE(OPEN_DT, '' ) AS OPEN_DT ,
  COALESCE(BUS_ACS_DT, '' ) AS BUS_ACS_DT ,
  COALESCE(POST_CODE, '' ) AS POST_CODE ,
  COALESCE(MNG_DUTY_NAME, '' ) AS MNG_DUTY_NAME ,
  COALESCE(MNG_DUTY_TEL, '' ) AS MNG_DUTY_TEL ,
  COALESCE(BUS_TM, '' ) AS BUS_TM ,
  COALESCE(NET_DUTY_NAME, '' ) AS NET_DUTY_NAME ,
  COALESCE(NET_DUTY_TEL, '' ) AS NET_DUTY_TEL ,
  COALESCE(BLG_PBC_NAME, '' ) AS BLG_PBC_NAME ,
  COALESCE(BLG_PBC_CODE, '' ) AS BLG_PBC_CODE ,
  COALESCE(BRH_EMAIL, '' ) AS BRH_EMAIL ,
  COALESCE(VIP_SERV, '' ) AS VIP_SERV ,
  COALESCE(PROXY_POST_BUS, '' ) AS PROXY_POST_BUS ,
  COALESCE(SELF_SERV_TYPE, '' ) AS SELF_SERV_TYPE ,
  COALESCE(PAY_NET_FLAG, '' ) AS PAY_NET_FLAG ,
  COALESCE(AGR_NET_FLAG, '' ) AS AGR_NET_FLAG ,
  COALESCE(FORE_COST_ER, '' ) AS FORE_COST_ER ,
  COALESCE(FORE_PEPO_QUAR_BALA_PD, 0 ) AS FORE_PEPO_QUAR_BALA_PD ,
  COALESCE(FORE_QUAR_BUS_PROFIT, 0 ) AS FORE_QUAR_BUS_PROFIT ,
  COALESCE(FORE_FIPR_QUAR_BALA, 0 ) AS FORE_FIPR_QUAR_BALA ,
  COALESCE(SA_EXCHG_BRH_CODE, '' ) AS SA_EXCHG_BRH_CODE ,
  COALESCE(SA_EXCHG_BRH_NAME, '' ) AS SA_EXCHG_BRH_NAME ,
  COALESCE(SA_SELF_BANK_CODE, '' ) AS SA_SELF_BANK_CODE ,
  COALESCE(SA_SELF_BANK_NAME, '' ) AS SA_SELF_BANK_NAME ,
  COALESCE(SIGN_BANK_PRO, '' ) AS SIGN_BANK_PRO ,
  COALESCE(SAVE_FLAG, '' ) AS SAVE_FLAG ,
  COALESCE(DRAW_FLAG, '' ) AS DRAW_FLAG ,
  COALESCE(PAY_FLAG, '' ) AS PAY_FLAG ,
  COALESCE(NEAR_BUILD, '' ) AS NEAR_BUILD ,
  COALESCE(LAST_UPD_TM, '' ) AS LAST_UPD_TM ,
  COALESCE(LAST_UPD_OPR, '' ) AS LAST_UPD_OPR ,
  COALESCE(LAST_UPD_TXN, '' ) AS LAST_UPD_TXN ,
  COALESCE(RESV1, '' ) AS RESV1 
 FROM  dw_tdata.OGS_000_TBL_FINC_BRH_BASIC_INFO_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  BRH_CODE ,
  PRE_DEAL_ID ,
  PRE_APRV_NO ,
  OPEN_APRV_NO ,
  OPEN_APRV_DT ,
  FINC_LICS_NO ,
  LICS_ADDR ,
  BRH_STYLE ,
  PBC_BRH_LVL ,
  SET_DT ,
  OPEN_DT ,
  BUS_ACS_DT ,
  POST_CODE ,
  MNG_DUTY_NAME ,
  MNG_DUTY_TEL ,
  BUS_TM ,
  NET_DUTY_NAME ,
  NET_DUTY_TEL ,
  BLG_PBC_NAME ,
  BLG_PBC_CODE ,
  BRH_EMAIL ,
  VIP_SERV ,
  PROXY_POST_BUS ,
  SELF_SERV_TYPE ,
  PAY_NET_FLAG ,
  AGR_NET_FLAG ,
  FORE_COST_ER ,
  FORE_PEPO_QUAR_BALA_PD ,
  FORE_QUAR_BUS_PROFIT ,
  FORE_FIPR_QUAR_BALA ,
  SA_EXCHG_BRH_CODE ,
  SA_EXCHG_BRH_NAME ,
  SA_SELF_BANK_CODE ,
  SA_SELF_BANK_NAME ,
  SIGN_BANK_PRO ,
  SAVE_FLAG ,
  DRAW_FLAG ,
  PAY_FLAG ,
  NEAR_BUILD ,
  LAST_UPD_TM ,
  LAST_UPD_OPR ,
  LAST_UPD_TXN ,
  RESV1 
 FROM dw_sdata.OGS_000_TBL_FINC_BRH_BASIC_INFO 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.BRH_CODE = T.BRH_CODE
WHERE
(T.BRH_CODE IS NULL)
 OR N.PRE_DEAL_ID<>T.PRE_DEAL_ID
 OR N.PRE_APRV_NO<>T.PRE_APRV_NO
 OR N.OPEN_APRV_NO<>T.OPEN_APRV_NO
 OR N.OPEN_APRV_DT<>T.OPEN_APRV_DT
 OR N.FINC_LICS_NO<>T.FINC_LICS_NO
 OR N.LICS_ADDR<>T.LICS_ADDR
 OR N.BRH_STYLE<>T.BRH_STYLE
 OR N.PBC_BRH_LVL<>T.PBC_BRH_LVL
 OR N.SET_DT<>T.SET_DT
 OR N.OPEN_DT<>T.OPEN_DT
 OR N.BUS_ACS_DT<>T.BUS_ACS_DT
 OR N.POST_CODE<>T.POST_CODE
 OR N.MNG_DUTY_NAME<>T.MNG_DUTY_NAME
 OR N.MNG_DUTY_TEL<>T.MNG_DUTY_TEL
 OR N.BUS_TM<>T.BUS_TM
 OR N.NET_DUTY_NAME<>T.NET_DUTY_NAME
 OR N.NET_DUTY_TEL<>T.NET_DUTY_TEL
 OR N.BLG_PBC_NAME<>T.BLG_PBC_NAME
 OR N.BLG_PBC_CODE<>T.BLG_PBC_CODE
 OR N.BRH_EMAIL<>T.BRH_EMAIL
 OR N.VIP_SERV<>T.VIP_SERV
 OR N.PROXY_POST_BUS<>T.PROXY_POST_BUS
 OR N.SELF_SERV_TYPE<>T.SELF_SERV_TYPE
 OR N.PAY_NET_FLAG<>T.PAY_NET_FLAG
 OR N.AGR_NET_FLAG<>T.AGR_NET_FLAG
 OR N.FORE_COST_ER<>T.FORE_COST_ER
 OR N.FORE_PEPO_QUAR_BALA_PD<>T.FORE_PEPO_QUAR_BALA_PD
 OR N.FORE_QUAR_BUS_PROFIT<>T.FORE_QUAR_BUS_PROFIT
 OR N.FORE_FIPR_QUAR_BALA<>T.FORE_FIPR_QUAR_BALA
 OR N.SA_EXCHG_BRH_CODE<>T.SA_EXCHG_BRH_CODE
 OR N.SA_EXCHG_BRH_NAME<>T.SA_EXCHG_BRH_NAME
 OR N.SA_SELF_BANK_CODE<>T.SA_SELF_BANK_CODE
 OR N.SA_SELF_BANK_NAME<>T.SA_SELF_BANK_NAME
 OR N.SIGN_BANK_PRO<>T.SIGN_BANK_PRO
 OR N.SAVE_FLAG<>T.SAVE_FLAG
 OR N.DRAW_FLAG<>T.DRAW_FLAG
 OR N.PAY_FLAG<>T.PAY_FLAG
 OR N.NEAR_BUILD<>T.NEAR_BUILD
 OR N.LAST_UPD_TM<>T.LAST_UPD_TM
 OR N.LAST_UPD_OPR<>T.LAST_UPD_OPR
 OR N.LAST_UPD_TXN<>T.LAST_UPD_TXN
 OR N.RESV1<>T.RESV1
;

--Step3:
UPDATE dw_sdata.OGS_000_TBL_FINC_BRH_BASIC_INFO P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_356
WHERE P.End_Dt=DATE('2100-12-31')
AND P.BRH_CODE=T_356.BRH_CODE
;

--Step4:
INSERT  INTO dw_sdata.OGS_000_TBL_FINC_BRH_BASIC_INFO SELECT * FROM T_356;

COMMIT;

ENDOFINPUT

  close(VSQL);

  my $RET_CODE = $? >> 8;

  if ( $RET_CODE == 0 ) {
      return 0;
  }
  else {
      return 1;
  }
}

# ------------ main function ------------
sub main
{
   my $ret;
   open(LOGONFILE_H, "${LOGON_FILE}");
   ${LOGON_STR} = <LOGONFILE_H>;
   close(LOGONFILE_H);
   
   # Get the decoded logon string
   my($user,$passwd) = split(',',${LOGON_STR}); 
   #my $decodepasswd = `${AUTO_HOME}/bin/IceCode.exe -d "$passwd" "$user"`;                     
   #${LOGON_STR} = "|vsql -h 192.168.2.44 -p 5433 -d CPCIMDB_TEST -U ".$user." -w ".$decodepasswd;

   # Call vsql command to load data
   $ret = run_vsql_command();

   print "run_vsql_command() = $ret";
   return $ret;
}

# ------------ program section ------------
if ( $#ARGV < 0 ) {
   print "Usage: [perl ������ Control_File] (Control_File format: dir.jobnameYYYYMMDD or sysname_jobname_YYYYMMDD.dir) 
";
   print "
";
   exit(1);
}

# Get the first argument
${CONTROL_FILE} = $ARGV[0];

if (${CONTROL_FILE} =~/[0-9]{8}($|\.)/) {
   ${TX_DATE_YYYYMMDD} = substr($&,0,8);
}
else{
   print "Usage: [perl ������ Control_File] (Control_File format: dir.jobnameYYYYMMDD or sysname_jobname_YYYYMMDD.dir) 
";
   print "
";
   exit(1);
}

${TX_MON_DAY_MMDD} = substr(${TX_DATE_YYYYMMDD}, length(${TX_DATE_YYYYMMDD})-4,4);
${TX_DATE} = substr(${TX_DATE_YYYYMMDD}, 0, 4)."-".substr(${TX_DATE_YYYYMMDD}, 4, 2)."-".substr(${TX_DATE_YYYYMMDD}, 6, 2);
open(STDERR, ">&STDOUT");

my $ret = main();

exit($ret);
