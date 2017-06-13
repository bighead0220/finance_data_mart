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
DELETE FROM dw_sdata.PCS_001_TC_PUB_DICINDUSTRY_FOR_CUS WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.PCS_001_TC_PUB_DICINDUSTRY_FOR_CUS SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_385 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.PCS_001_TC_PUB_DICINDUSTRY_FOR_CUS WHERE 1=0;

--Step2:
INSERT  INTO T_385 (
  INDUSTRY_ID,
  INDUSTRY_NO,
  INDUSTRY_NAME,
  PRO_NO,
  INDUSTRY_ORDER,
  INDUSTRY_VERSION,
  INDUSTRY_DESC,
  DELFLAG,
  CREATE_TIME,
  UPDATE_TIME,
  TRUNC_NO,
  start_dt,
  end_dt)
SELECT
  N.INDUSTRY_ID,
  N.INDUSTRY_NO,
  N.INDUSTRY_NAME,
  N.PRO_NO,
  N.INDUSTRY_ORDER,
  N.INDUSTRY_VERSION,
  N.INDUSTRY_DESC,
  N.DELFLAG,
  N.CREATE_TIME,
  N.UPDATE_TIME,
  N.TRUNC_NO,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(INDUSTRY_ID, '' ) AS INDUSTRY_ID ,
  COALESCE(INDUSTRY_NO, '' ) AS INDUSTRY_NO ,
  COALESCE(INDUSTRY_NAME, '' ) AS INDUSTRY_NAME ,
  COALESCE(PRO_NO, '' ) AS PRO_NO ,
  COALESCE(INDUSTRY_ORDER, '' ) AS INDUSTRY_ORDER ,
  COALESCE(INDUSTRY_VERSION, '' ) AS INDUSTRY_VERSION ,
  COALESCE(INDUSTRY_DESC, '' ) AS INDUSTRY_DESC ,
  COALESCE(DELFLAG, '' ) AS DELFLAG ,
  COALESCE(CREATE_TIME,'4999-12-31 00:00:00' ) AS CREATE_TIME ,
  COALESCE(UPDATE_TIME,'4999-12-31 00:00:00' ) AS UPDATE_TIME ,
  COALESCE(TRUNC_NO, 0 ) AS TRUNC_NO 
 FROM  dw_tdata.PCS_001_TC_PUB_DICINDUSTRY_FOR_CUS_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  INDUSTRY_ID ,
  INDUSTRY_NO ,
  INDUSTRY_NAME ,
  PRO_NO ,
  INDUSTRY_ORDER ,
  INDUSTRY_VERSION ,
  INDUSTRY_DESC ,
  DELFLAG ,
  CREATE_TIME ,
  UPDATE_TIME ,
  TRUNC_NO 
 FROM dw_sdata.PCS_001_TC_PUB_DICINDUSTRY_FOR_CUS 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.INDUSTRY_ID = T.INDUSTRY_ID
WHERE
(T.INDUSTRY_ID IS NULL)
 OR N.INDUSTRY_NO<>T.INDUSTRY_NO
 OR N.INDUSTRY_NAME<>T.INDUSTRY_NAME
 OR N.PRO_NO<>T.PRO_NO
 OR N.INDUSTRY_ORDER<>T.INDUSTRY_ORDER
 OR N.INDUSTRY_VERSION<>T.INDUSTRY_VERSION
 OR N.INDUSTRY_DESC<>T.INDUSTRY_DESC
 OR N.DELFLAG<>T.DELFLAG
 OR N.CREATE_TIME<>T.CREATE_TIME
 OR N.UPDATE_TIME<>T.UPDATE_TIME
 OR N.TRUNC_NO<>T.TRUNC_NO
;

--Step3:
UPDATE dw_sdata.PCS_001_TC_PUB_DICINDUSTRY_FOR_CUS P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_385
WHERE P.End_Dt=DATE('2100-12-31')
AND P.INDUSTRY_ID=T_385.INDUSTRY_ID
;

--Step4:
INSERT  INTO dw_sdata.PCS_001_TC_PUB_DICINDUSTRY_FOR_CUS SELECT * FROM T_385;

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
