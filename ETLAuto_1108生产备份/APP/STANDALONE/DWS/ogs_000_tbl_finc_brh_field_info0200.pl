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
DELETE FROM dw_sdata.OGS_000_TBL_FINC_BRH_FIELD_INFO WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.OGS_000_TBL_FINC_BRH_FIELD_INFO SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_357 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.OGS_000_TBL_FINC_BRH_FIELD_INFO WHERE 1=0;

--Step2:
INSERT  INTO T_357 (
  BRH_CODE,
  INDP_SHOP,
  NET_BUS_AREA,
  POST_BUS_AREA,
  BUS_FIELD_PROPT,
  OFFICE_FIELD_PROPT,
  OFFICE_AREA,
  LAST_BUILD_DT,
  LAST_BUILD_TYPE,
  FUNC_SUB_AREA,
  BLG_ECON_FUNC_AREA,
  HIRE_END_DT,
  FINC_ROOM_NUM,
  BALK_DOOR,
  CREDIT_CENTER_FLAG,
  SALE_DEPT_SM_POST,
  VIP_CASH_BORD_NUM,
  CASH_BORD_NUM,
  NO_CASH_BORD_NUM,
  CASH_BORD_FACT_NUM,
  LAST_UPD_TM,
  LAST_UPD_OPR,
  LAST_UPD_TXN,
  RESV1,
  start_dt,
  end_dt)
SELECT
  N.BRH_CODE,
  N.INDP_SHOP,
  N.NET_BUS_AREA,
  N.POST_BUS_AREA,
  N.BUS_FIELD_PROPT,
  N.OFFICE_FIELD_PROPT,
  N.OFFICE_AREA,
  N.LAST_BUILD_DT,
  N.LAST_BUILD_TYPE,
  N.FUNC_SUB_AREA,
  N.BLG_ECON_FUNC_AREA,
  N.HIRE_END_DT,
  N.FINC_ROOM_NUM,
  N.BALK_DOOR,
  N.CREDIT_CENTER_FLAG,
  N.SALE_DEPT_SM_POST,
  N.VIP_CASH_BORD_NUM,
  N.CASH_BORD_NUM,
  N.NO_CASH_BORD_NUM,
  N.CASH_BORD_FACT_NUM,
  N.LAST_UPD_TM,
  N.LAST_UPD_OPR,
  N.LAST_UPD_TXN,
  N.RESV1,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(BRH_CODE, '' ) AS BRH_CODE ,
  COALESCE(INDP_SHOP, '' ) AS INDP_SHOP ,
  COALESCE(NET_BUS_AREA, 0 ) AS NET_BUS_AREA ,
  COALESCE(POST_BUS_AREA, 0 ) AS POST_BUS_AREA ,
  COALESCE(BUS_FIELD_PROPT, '' ) AS BUS_FIELD_PROPT ,
  COALESCE(OFFICE_FIELD_PROPT, '' ) AS OFFICE_FIELD_PROPT ,
  COALESCE(OFFICE_AREA, 0 ) AS OFFICE_AREA ,
  COALESCE(LAST_BUILD_DT, '' ) AS LAST_BUILD_DT ,
  COALESCE(LAST_BUILD_TYPE, '' ) AS LAST_BUILD_TYPE ,
  COALESCE(FUNC_SUB_AREA, '' ) AS FUNC_SUB_AREA ,
  COALESCE(BLG_ECON_FUNC_AREA, '' ) AS BLG_ECON_FUNC_AREA ,
  COALESCE(HIRE_END_DT, '' ) AS HIRE_END_DT ,
  COALESCE(FINC_ROOM_NUM, 0 ) AS FINC_ROOM_NUM ,
  COALESCE(BALK_DOOR, '' ) AS BALK_DOOR ,
  COALESCE(CREDIT_CENTER_FLAG, '' ) AS CREDIT_CENTER_FLAG ,
  COALESCE(SALE_DEPT_SM_POST, '' ) AS SALE_DEPT_SM_POST ,
  COALESCE(VIP_CASH_BORD_NUM, 0 ) AS VIP_CASH_BORD_NUM ,
  COALESCE(CASH_BORD_NUM, 0 ) AS CASH_BORD_NUM ,
  COALESCE(NO_CASH_BORD_NUM, 0 ) AS NO_CASH_BORD_NUM ,
  COALESCE(CASH_BORD_FACT_NUM, 0 ) AS CASH_BORD_FACT_NUM ,
  COALESCE(LAST_UPD_TM, '' ) AS LAST_UPD_TM ,
  COALESCE(LAST_UPD_OPR, '' ) AS LAST_UPD_OPR ,
  COALESCE(LAST_UPD_TXN, '' ) AS LAST_UPD_TXN ,
  COALESCE(RESV1, '' ) AS RESV1 
 FROM  dw_tdata.OGS_000_TBL_FINC_BRH_FIELD_INFO_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  BRH_CODE ,
  INDP_SHOP ,
  NET_BUS_AREA ,
  POST_BUS_AREA ,
  BUS_FIELD_PROPT ,
  OFFICE_FIELD_PROPT ,
  OFFICE_AREA ,
  LAST_BUILD_DT ,
  LAST_BUILD_TYPE ,
  FUNC_SUB_AREA ,
  BLG_ECON_FUNC_AREA ,
  HIRE_END_DT ,
  FINC_ROOM_NUM ,
  BALK_DOOR ,
  CREDIT_CENTER_FLAG ,
  SALE_DEPT_SM_POST ,
  VIP_CASH_BORD_NUM ,
  CASH_BORD_NUM ,
  NO_CASH_BORD_NUM ,
  CASH_BORD_FACT_NUM ,
  LAST_UPD_TM ,
  LAST_UPD_OPR ,
  LAST_UPD_TXN ,
  RESV1 
 FROM dw_sdata.OGS_000_TBL_FINC_BRH_FIELD_INFO 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.BRH_CODE = T.BRH_CODE
WHERE
(T.BRH_CODE IS NULL)
 OR N.INDP_SHOP<>T.INDP_SHOP
 OR N.NET_BUS_AREA<>T.NET_BUS_AREA
 OR N.POST_BUS_AREA<>T.POST_BUS_AREA
 OR N.BUS_FIELD_PROPT<>T.BUS_FIELD_PROPT
 OR N.OFFICE_FIELD_PROPT<>T.OFFICE_FIELD_PROPT
 OR N.OFFICE_AREA<>T.OFFICE_AREA
 OR N.LAST_BUILD_DT<>T.LAST_BUILD_DT
 OR N.LAST_BUILD_TYPE<>T.LAST_BUILD_TYPE
 OR N.FUNC_SUB_AREA<>T.FUNC_SUB_AREA
 OR N.BLG_ECON_FUNC_AREA<>T.BLG_ECON_FUNC_AREA
 OR N.HIRE_END_DT<>T.HIRE_END_DT
 OR N.FINC_ROOM_NUM<>T.FINC_ROOM_NUM
 OR N.BALK_DOOR<>T.BALK_DOOR
 OR N.CREDIT_CENTER_FLAG<>T.CREDIT_CENTER_FLAG
 OR N.SALE_DEPT_SM_POST<>T.SALE_DEPT_SM_POST
 OR N.VIP_CASH_BORD_NUM<>T.VIP_CASH_BORD_NUM
 OR N.CASH_BORD_NUM<>T.CASH_BORD_NUM
 OR N.NO_CASH_BORD_NUM<>T.NO_CASH_BORD_NUM
 OR N.CASH_BORD_FACT_NUM<>T.CASH_BORD_FACT_NUM
 OR N.LAST_UPD_TM<>T.LAST_UPD_TM
 OR N.LAST_UPD_OPR<>T.LAST_UPD_OPR
 OR N.LAST_UPD_TXN<>T.LAST_UPD_TXN
 OR N.RESV1<>T.RESV1
;

--Step3:
UPDATE dw_sdata.OGS_000_TBL_FINC_BRH_FIELD_INFO P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_357
WHERE P.End_Dt=DATE('2100-12-31')
AND P.BRH_CODE=T_357.BRH_CODE
;

--Step4:
INSERT  INTO dw_sdata.OGS_000_TBL_FINC_BRH_FIELD_INFO SELECT * FROM T_357;

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
