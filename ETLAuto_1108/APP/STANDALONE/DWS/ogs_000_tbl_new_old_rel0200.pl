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
DELETE FROM dw_sdata.OGS_000_TBL_NEW_OLD_REL WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.OGS_000_TBL_NEW_OLD_REL SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_311 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.OGS_000_TBL_NEW_OLD_REL WHERE 1=0;

--Step2:
INSERT  INTO T_311 (
  SEQ_NUM,
  SYS_TYPE,
  BRH_CODE,
  BRH_SV_NEW_CODE,
  USE_FLAG,
  CHG_DT,
  NEW_OLD_FIELD0,
  NEW_OLD_FIELD1,
  NEW_OLD_FIELD2,
  NEW_OLD_FIELD3,
  NEW_OLD_FIELD4,
  FILE_NAME,
  LAST_UPD_TM,
  LAST_UPD_OPR,
  LAST_UPD_TXN,
  start_dt,
  end_dt)
SELECT
  N.SEQ_NUM,
  N.SYS_TYPE,
  N.BRH_CODE,
  N.BRH_SV_NEW_CODE,
  N.USE_FLAG,
  N.CHG_DT,
  N.NEW_OLD_FIELD0,
  N.NEW_OLD_FIELD1,
  N.NEW_OLD_FIELD2,
  N.NEW_OLD_FIELD3,
  N.NEW_OLD_FIELD4,
  N.FILE_NAME,
  N.LAST_UPD_TM,
  N.LAST_UPD_OPR,
  N.LAST_UPD_TXN,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(SEQ_NUM, '' ) AS SEQ_NUM ,
  COALESCE(SYS_TYPE, '' ) AS SYS_TYPE ,
  COALESCE(BRH_CODE, '' ) AS BRH_CODE ,
  COALESCE(BRH_SV_NEW_CODE, '' ) AS BRH_SV_NEW_CODE ,
  COALESCE(USE_FLAG, '' ) AS USE_FLAG ,
  COALESCE(CHG_DT, '' ) AS CHG_DT ,
  COALESCE(NEW_OLD_FIELD0, '' ) AS NEW_OLD_FIELD0 ,
  COALESCE(NEW_OLD_FIELD1, '' ) AS NEW_OLD_FIELD1 ,
  COALESCE(NEW_OLD_FIELD2, '' ) AS NEW_OLD_FIELD2 ,
  COALESCE(NEW_OLD_FIELD3, '' ) AS NEW_OLD_FIELD3 ,
  COALESCE(NEW_OLD_FIELD4, '' ) AS NEW_OLD_FIELD4 ,
  COALESCE(FILE_NAME, '' ) AS FILE_NAME ,
  COALESCE(LAST_UPD_TM, '' ) AS LAST_UPD_TM ,
  COALESCE(LAST_UPD_OPR, '' ) AS LAST_UPD_OPR ,
  COALESCE(LAST_UPD_TXN, '' ) AS LAST_UPD_TXN 
 FROM  dw_tdata.OGS_000_TBL_NEW_OLD_REL_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  SEQ_NUM ,
  SYS_TYPE ,
  BRH_CODE ,
  BRH_SV_NEW_CODE ,
  USE_FLAG ,
  CHG_DT ,
  NEW_OLD_FIELD0 ,
  NEW_OLD_FIELD1 ,
  NEW_OLD_FIELD2 ,
  NEW_OLD_FIELD3 ,
  NEW_OLD_FIELD4 ,
  FILE_NAME ,
  LAST_UPD_TM ,
  LAST_UPD_OPR ,
  LAST_UPD_TXN 
 FROM dw_sdata.OGS_000_TBL_NEW_OLD_REL 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.SEQ_NUM = T.SEQ_NUM AND N.SYS_TYPE = T.SYS_TYPE AND N.BRH_CODE = T.BRH_CODE
WHERE
(T.SEQ_NUM IS NULL AND T.SYS_TYPE IS NULL AND T.BRH_CODE IS NULL)
 OR N.BRH_SV_NEW_CODE<>T.BRH_SV_NEW_CODE
 OR N.USE_FLAG<>T.USE_FLAG
 OR N.CHG_DT<>T.CHG_DT
 OR N.NEW_OLD_FIELD0<>T.NEW_OLD_FIELD0
 OR N.NEW_OLD_FIELD1<>T.NEW_OLD_FIELD1
 OR N.NEW_OLD_FIELD2<>T.NEW_OLD_FIELD2
 OR N.NEW_OLD_FIELD3<>T.NEW_OLD_FIELD3
 OR N.NEW_OLD_FIELD4<>T.NEW_OLD_FIELD4
 OR N.FILE_NAME<>T.FILE_NAME
 OR N.LAST_UPD_TM<>T.LAST_UPD_TM
 OR N.LAST_UPD_OPR<>T.LAST_UPD_OPR
 OR N.LAST_UPD_TXN<>T.LAST_UPD_TXN
;

--Step3:
UPDATE dw_sdata.OGS_000_TBL_NEW_OLD_REL P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_311
WHERE P.End_Dt=DATE('2100-12-31')
AND P.SEQ_NUM=T_311.SEQ_NUM
AND P.SYS_TYPE=T_311.SYS_TYPE
AND P.BRH_CODE=T_311.BRH_CODE
;

--Step4:
INSERT  INTO dw_sdata.OGS_000_TBL_NEW_OLD_REL SELECT * FROM T_311;

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
