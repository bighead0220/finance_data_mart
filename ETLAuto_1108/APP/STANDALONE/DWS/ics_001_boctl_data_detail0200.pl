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
DELETE FROM dw_sdata.ICS_001_BOCTL_DATA_DETAIL WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.ICS_001_BOCTL_DATA_DETAIL SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_221 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.ICS_001_BOCTL_DATA_DETAIL WHERE 1=0;

--Step2:
INSERT  INTO T_221 (
  INSU_DATE,
  INSU_CODE,
  POL_ID,
  DATE_KIND,
  APPF_NO,
  CLR_DATE,
  FILE_NAME,
  INSU_SUM,
  TX_AMT,
  FLAG,
  PAY_NO,
  NEXT_DATE,
  NEXT_AMT,
  NOW_DATE,
  POL_AMT,
  POH_NAME,
  INSU_KIND,
  POH_CERT_ID,
  STATE,
  DEAL_FLAG,
  PARA_1,
  PARA_2,
  PARA_3,
  PARA_4,
  PARA_5,
  start_dt,
  end_dt)
SELECT
  N.INSU_DATE,
  N.INSU_CODE,
  N.POL_ID,
  N.DATE_KIND,
  N.APPF_NO,
  N.CLR_DATE,
  N.FILE_NAME,
  N.INSU_SUM,
  N.TX_AMT,
  N.FLAG,
  N.PAY_NO,
  N.NEXT_DATE,
  N.NEXT_AMT,
  N.NOW_DATE,
  N.POL_AMT,
  N.POH_NAME,
  N.INSU_KIND,
  N.POH_CERT_ID,
  N.STATE,
  N.DEAL_FLAG,
  N.PARA_1,
  N.PARA_2,
  N.PARA_3,
  N.PARA_4,
  N.PARA_5,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(INSU_DATE, '' ) AS INSU_DATE ,
  COALESCE(INSU_CODE, '' ) AS INSU_CODE ,
  COALESCE(POL_ID, '' ) AS POL_ID ,
  COALESCE(DATE_KIND, '' ) AS DATE_KIND ,
  COALESCE(APPF_NO, '' ) AS APPF_NO ,
  COALESCE(CLR_DATE, '' ) AS CLR_DATE ,
  COALESCE(FILE_NAME, '' ) AS FILE_NAME ,
  COALESCE(INSU_SUM, 0 ) AS INSU_SUM ,
  COALESCE(TX_AMT, 0 ) AS TX_AMT ,
  COALESCE(FLAG, '' ) AS FLAG ,
  COALESCE(PAY_NO, 0 ) AS PAY_NO ,
  COALESCE(NEXT_DATE, '' ) AS NEXT_DATE ,
  COALESCE(NEXT_AMT, 0 ) AS NEXT_AMT ,
  COALESCE(NOW_DATE, '' ) AS NOW_DATE ,
  COALESCE(POL_AMT, 0 ) AS POL_AMT ,
  COALESCE(POH_NAME, '' ) AS POH_NAME ,
  COALESCE(INSU_KIND, '' ) AS INSU_KIND ,
  COALESCE(POH_CERT_ID, '' ) AS POH_CERT_ID ,
  COALESCE(STATE, '' ) AS STATE ,
  COALESCE(DEAL_FLAG, '' ) AS DEAL_FLAG ,
  COALESCE(PARA_1, '' ) AS PARA_1 ,
  COALESCE(PARA_2, '' ) AS PARA_2 ,
  COALESCE(PARA_3, '' ) AS PARA_3 ,
  COALESCE(PARA_4, '' ) AS PARA_4 ,
  COALESCE(PARA_5, '' ) AS PARA_5 
 FROM  dw_tdata.ICS_001_BOCTL_DATA_DETAIL_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  INSU_DATE ,
  INSU_CODE ,
  POL_ID ,
  DATE_KIND ,
  APPF_NO ,
  CLR_DATE ,
  FILE_NAME ,
  INSU_SUM ,
  TX_AMT ,
  FLAG ,
  PAY_NO ,
  NEXT_DATE ,
  NEXT_AMT ,
  NOW_DATE ,
  POL_AMT ,
  POH_NAME ,
  INSU_KIND ,
  POH_CERT_ID ,
  STATE ,
  DEAL_FLAG ,
  PARA_1 ,
  PARA_2 ,
  PARA_3 ,
  PARA_4 ,
  PARA_5 
 FROM dw_sdata.ICS_001_BOCTL_DATA_DETAIL 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.INSU_DATE = T.INSU_DATE AND N.INSU_CODE = T.INSU_CODE AND N.POL_ID = T.POL_ID AND N.DATE_KIND = T.DATE_KIND AND N.APPF_NO = T.APPF_NO
WHERE
(T.INSU_DATE IS NULL AND T.INSU_CODE IS NULL AND T.POL_ID IS NULL AND T.DATE_KIND IS NULL AND T.APPF_NO IS NULL)
 OR N.CLR_DATE<>T.CLR_DATE
 OR N.FILE_NAME<>T.FILE_NAME
 OR N.INSU_SUM<>T.INSU_SUM
 OR N.TX_AMT<>T.TX_AMT
 OR N.FLAG<>T.FLAG
 OR N.PAY_NO<>T.PAY_NO
 OR N.NEXT_DATE<>T.NEXT_DATE
 OR N.NEXT_AMT<>T.NEXT_AMT
 OR N.NOW_DATE<>T.NOW_DATE
 OR N.POL_AMT<>T.POL_AMT
 OR N.POH_NAME<>T.POH_NAME
 OR N.INSU_KIND<>T.INSU_KIND
 OR N.POH_CERT_ID<>T.POH_CERT_ID
 OR N.STATE<>T.STATE
 OR N.DEAL_FLAG<>T.DEAL_FLAG
 OR N.PARA_1<>T.PARA_1
 OR N.PARA_2<>T.PARA_2
 OR N.PARA_3<>T.PARA_3
 OR N.PARA_4<>T.PARA_4
 OR N.PARA_5<>T.PARA_5
;

--Step3:
UPDATE dw_sdata.ICS_001_BOCTL_DATA_DETAIL P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_221
WHERE P.End_Dt=DATE('2100-12-31')
AND P.INSU_DATE=T_221.INSU_DATE
AND P.INSU_CODE=T_221.INSU_CODE
AND P.POL_ID=T_221.POL_ID
AND P.DATE_KIND=T_221.DATE_KIND
AND P.APPF_NO=T_221.APPF_NO
;

--Step4:
INSERT  INTO dw_sdata.ICS_001_BOCTL_DATA_DETAIL SELECT * FROM T_221;

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
