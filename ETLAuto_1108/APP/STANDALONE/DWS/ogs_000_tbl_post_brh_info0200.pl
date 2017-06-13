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
DELETE FROM dw_sdata.OGS_000_TBL_POST_BRH_INFO WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.OGS_000_TBL_POST_BRH_INFO SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_351 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.OGS_000_TBL_POST_BRH_INFO WHERE 1=0;

--Step2:
INSERT  INTO T_351 (
  BRH_CODE,
  NET_FLAG,
  POST_CODE,
  NET_TEL,
  SET_DT,
  OPEN_DT,
  BUS_ACS_DT,
  BUS_TM,
  DUTY_NAME,
  DUTY_TEL,
  ONLI_NAME,
  ONLI_TEL,
  AGENT_PRT_BRH_CODE,
  BANK_ADDR_FLAG,
  SA_FINC_BRH_CODE,
  SA_FINC_BRH_NAME,
  RMT_AUTH_FLAG,
  BRH_MEM_NUM,
  SEAT_NUM,
  MON_DEV_FLAG,
  ONLI_CHK_DEV,
  TEM_MAG_TYPE,
  TEM_ID,
  IP_ADDR,
  AGT_FLAG,
  LAST_UPD_TM,
  LAST_UPD_OPR,
  LAST_UPD_TXN,
  RESV1,
  start_dt,
  end_dt)
SELECT
  N.BRH_CODE,
  N.NET_FLAG,
  N.POST_CODE,
  N.NET_TEL,
  N.SET_DT,
  N.OPEN_DT,
  N.BUS_ACS_DT,
  N.BUS_TM,
  N.DUTY_NAME,
  N.DUTY_TEL,
  N.ONLI_NAME,
  N.ONLI_TEL,
  N.AGENT_PRT_BRH_CODE,
  N.BANK_ADDR_FLAG,
  N.SA_FINC_BRH_CODE,
  N.SA_FINC_BRH_NAME,
  N.RMT_AUTH_FLAG,
  N.BRH_MEM_NUM,
  N.SEAT_NUM,
  N.MON_DEV_FLAG,
  N.ONLI_CHK_DEV,
  N.TEM_MAG_TYPE,
  N.TEM_ID,
  N.IP_ADDR,
  N.AGT_FLAG,
  N.LAST_UPD_TM,
  N.LAST_UPD_OPR,
  N.LAST_UPD_TXN,
  N.RESV1,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(BRH_CODE, '' ) AS BRH_CODE ,
  COALESCE(NET_FLAG, '' ) AS NET_FLAG ,
  COALESCE(POST_CODE, '' ) AS POST_CODE ,
  COALESCE(NET_TEL, '' ) AS NET_TEL ,
  COALESCE(SET_DT, '' ) AS SET_DT ,
  COALESCE(OPEN_DT, '' ) AS OPEN_DT ,
  COALESCE(BUS_ACS_DT, '' ) AS BUS_ACS_DT ,
  COALESCE(BUS_TM, '' ) AS BUS_TM ,
  COALESCE(DUTY_NAME, '' ) AS DUTY_NAME ,
  COALESCE(DUTY_TEL, '' ) AS DUTY_TEL ,
  COALESCE(ONLI_NAME, '' ) AS ONLI_NAME ,
  COALESCE(ONLI_TEL, '' ) AS ONLI_TEL ,
  COALESCE(AGENT_PRT_BRH_CODE, '' ) AS AGENT_PRT_BRH_CODE ,
  COALESCE(BANK_ADDR_FLAG, '' ) AS BANK_ADDR_FLAG ,
  COALESCE(SA_FINC_BRH_CODE, '' ) AS SA_FINC_BRH_CODE ,
  COALESCE(SA_FINC_BRH_NAME, '' ) AS SA_FINC_BRH_NAME ,
  COALESCE(RMT_AUTH_FLAG, '' ) AS RMT_AUTH_FLAG ,
  COALESCE(BRH_MEM_NUM, 0 ) AS BRH_MEM_NUM ,
  COALESCE(SEAT_NUM, 0 ) AS SEAT_NUM ,
  COALESCE(MON_DEV_FLAG, '' ) AS MON_DEV_FLAG ,
  COALESCE(ONLI_CHK_DEV, '' ) AS ONLI_CHK_DEV ,
  COALESCE(TEM_MAG_TYPE, '' ) AS TEM_MAG_TYPE ,
  COALESCE(TEM_ID, '' ) AS TEM_ID ,
  COALESCE(IP_ADDR, '' ) AS IP_ADDR ,
  COALESCE(AGT_FLAG, '' ) AS AGT_FLAG ,
  COALESCE(LAST_UPD_TM, '' ) AS LAST_UPD_TM ,
  COALESCE(LAST_UPD_OPR, '' ) AS LAST_UPD_OPR ,
  COALESCE(LAST_UPD_TXN, '' ) AS LAST_UPD_TXN ,
  COALESCE(RESV1, '' ) AS RESV1 
 FROM  dw_tdata.OGS_000_TBL_POST_BRH_INFO_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  BRH_CODE ,
  NET_FLAG ,
  POST_CODE ,
  NET_TEL ,
  SET_DT ,
  OPEN_DT ,
  BUS_ACS_DT ,
  BUS_TM ,
  DUTY_NAME ,
  DUTY_TEL ,
  ONLI_NAME ,
  ONLI_TEL ,
  AGENT_PRT_BRH_CODE ,
  BANK_ADDR_FLAG ,
  SA_FINC_BRH_CODE ,
  SA_FINC_BRH_NAME ,
  RMT_AUTH_FLAG ,
  BRH_MEM_NUM ,
  SEAT_NUM ,
  MON_DEV_FLAG ,
  ONLI_CHK_DEV ,
  TEM_MAG_TYPE ,
  TEM_ID ,
  IP_ADDR ,
  AGT_FLAG ,
  LAST_UPD_TM ,
  LAST_UPD_OPR ,
  LAST_UPD_TXN ,
  RESV1 
 FROM dw_sdata.OGS_000_TBL_POST_BRH_INFO 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.BRH_CODE = T.BRH_CODE
WHERE
(T.BRH_CODE IS NULL)
 OR N.NET_FLAG<>T.NET_FLAG
 OR N.POST_CODE<>T.POST_CODE
 OR N.NET_TEL<>T.NET_TEL
 OR N.SET_DT<>T.SET_DT
 OR N.OPEN_DT<>T.OPEN_DT
 OR N.BUS_ACS_DT<>T.BUS_ACS_DT
 OR N.BUS_TM<>T.BUS_TM
 OR N.DUTY_NAME<>T.DUTY_NAME
 OR N.DUTY_TEL<>T.DUTY_TEL
 OR N.ONLI_NAME<>T.ONLI_NAME
 OR N.ONLI_TEL<>T.ONLI_TEL
 OR N.AGENT_PRT_BRH_CODE<>T.AGENT_PRT_BRH_CODE
 OR N.BANK_ADDR_FLAG<>T.BANK_ADDR_FLAG
 OR N.SA_FINC_BRH_CODE<>T.SA_FINC_BRH_CODE
 OR N.SA_FINC_BRH_NAME<>T.SA_FINC_BRH_NAME
 OR N.RMT_AUTH_FLAG<>T.RMT_AUTH_FLAG
 OR N.BRH_MEM_NUM<>T.BRH_MEM_NUM
 OR N.SEAT_NUM<>T.SEAT_NUM
 OR N.MON_DEV_FLAG<>T.MON_DEV_FLAG
 OR N.ONLI_CHK_DEV<>T.ONLI_CHK_DEV
 OR N.TEM_MAG_TYPE<>T.TEM_MAG_TYPE
 OR N.TEM_ID<>T.TEM_ID
 OR N.IP_ADDR<>T.IP_ADDR
 OR N.AGT_FLAG<>T.AGT_FLAG
 OR N.LAST_UPD_TM<>T.LAST_UPD_TM
 OR N.LAST_UPD_OPR<>T.LAST_UPD_OPR
 OR N.LAST_UPD_TXN<>T.LAST_UPD_TXN
 OR N.RESV1<>T.RESV1
;

--Step3:
UPDATE dw_sdata.OGS_000_TBL_POST_BRH_INFO P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_351
WHERE P.End_Dt=DATE('2100-12-31')
AND P.BRH_CODE=T_351.BRH_CODE
;

--Step4:
INSERT  INTO dw_sdata.OGS_000_TBL_POST_BRH_INFO SELECT * FROM T_351;

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
   print "Usage: [perl 程序名 Control_File] (Control_File format: dir.jobnameYYYYMMDD or sysname_jobname_YYYYMMDD.dir) 
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
   print "Usage: [perl 程序名 Control_File] (Control_File format: dir.jobnameYYYYMMDD or sysname_jobname_YYYYMMDD.dir) 
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
