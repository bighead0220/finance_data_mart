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
DELETE FROM dw_sdata.PCS_001_TB_PUB_LOANRATEDIC WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.PCS_001_TB_PUB_LOANRATEDIC SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_313 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.PCS_001_TB_PUB_LOANRATEDIC WHERE 1=0;

--Step2:
INSERT  INTO T_313 (
  RATE_TYPE,
  BASE_RATE,
  LOAN_LENGTH,
  RATE_STATE,
  EFFECT_DATE,
  RATE_DESC,
  CREATE_TIME,
  UPDATE_TIME,
  BASERATE_ID,
  BASERATE_NO,
  DELFLAG,
  TRUNC_NO,
  start_dt,
  end_dt)
SELECT
  N.RATE_TYPE,
  N.BASE_RATE,
  N.LOAN_LENGTH,
  N.RATE_STATE,
  N.EFFECT_DATE,
  N.RATE_DESC,
  N.CREATE_TIME,
  N.UPDATE_TIME,
  N.BASERATE_ID,
  N.BASERATE_NO,
  N.DELFLAG,
  N.TRUNC_NO,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(RATE_TYPE, '' ) AS RATE_TYPE ,
  COALESCE(BASE_RATE, 0 ) AS BASE_RATE ,
  COALESCE(LOAN_LENGTH, 0 ) AS LOAN_LENGTH ,
  COALESCE(RATE_STATE, '' ) AS RATE_STATE ,
  COALESCE(EFFECT_DATE,DATE('4999-12-31') ) AS EFFECT_DATE ,
  COALESCE(RATE_DESC, '' ) AS RATE_DESC ,
  COALESCE(CREATE_TIME,'4999-12-31 00:00:00' ) AS CREATE_TIME ,
  COALESCE(UPDATE_TIME,'4999-12-31 00:00:00' ) AS UPDATE_TIME ,
  COALESCE(BASERATE_ID, '' ) AS BASERATE_ID ,
  COALESCE(BASERATE_NO, '' ) AS BASERATE_NO ,
  COALESCE(DELFLAG, '' ) AS DELFLAG ,
  COALESCE(TRUNC_NO, 0 ) AS TRUNC_NO 
 FROM  dw_tdata.PCS_001_TB_PUB_LOANRATEDIC_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  RATE_TYPE ,
  BASE_RATE ,
  LOAN_LENGTH ,
  RATE_STATE ,
  EFFECT_DATE ,
  RATE_DESC ,
  CREATE_TIME ,
  UPDATE_TIME ,
  BASERATE_ID ,
  BASERATE_NO ,
  DELFLAG ,
  TRUNC_NO 
 FROM dw_sdata.PCS_001_TB_PUB_LOANRATEDIC 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.BASERATE_ID = T.BASERATE_ID
WHERE
(T.BASERATE_ID IS NULL)
 OR N.RATE_TYPE<>T.RATE_TYPE
 OR N.BASE_RATE<>T.BASE_RATE
 OR N.LOAN_LENGTH<>T.LOAN_LENGTH
 OR N.RATE_STATE<>T.RATE_STATE
 OR N.EFFECT_DATE<>T.EFFECT_DATE
 OR N.RATE_DESC<>T.RATE_DESC
 OR N.CREATE_TIME<>T.CREATE_TIME
 OR N.UPDATE_TIME<>T.UPDATE_TIME
 OR N.BASERATE_NO<>T.BASERATE_NO
 OR N.DELFLAG<>T.DELFLAG
 OR N.TRUNC_NO<>T.TRUNC_NO
;

--Step3:
UPDATE dw_sdata.PCS_001_TB_PUB_LOANRATEDIC P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_313
WHERE P.End_Dt=DATE('2100-12-31')
AND P.BASERATE_ID=T_313.BASERATE_ID
;

--Step4:
INSERT  INTO dw_sdata.PCS_001_TB_PUB_LOANRATEDIC SELECT * FROM T_313;

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
