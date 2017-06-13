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
DELETE FROM dw_sdata.COS_000_CHART_ACC WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.COS_000_CHART_ACC SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_150 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.COS_000_CHART_ACC WHERE 1=0;

--Step2:
INSERT  INTO T_150 (
  CHART_ACC_ID,
  COA_CODE,
  COA_NAME,
  COA_MASK_ID,
  GENERATED,
  CONSOLIDATE,
  SET_OF_ACCOUNTS_ID,
  CCY,
  ACCOUNT_TYPE,
  USED_BY_BA,
  REVAL,
  REVAL_LOSS_CHART_ACC_ID,
  REVAL_GAIN_CHART_ACC_ID,
  REVAL_TOT,
  REVAL_DT,
  MAN_POST,
  MASK_COUNTRY_ID,
  MASK_LOCATION_ID,
  MASK_CPARTY_ID,
  MASK_ACCEPTOR_ID,
  IN_USE,
  HASHKEY,
  COLLISION,
  STAMP,
  DATE_CREATE,
  start_dt,
  end_dt)
SELECT
  N.CHART_ACC_ID,
  N.COA_CODE,
  N.COA_NAME,
  N.COA_MASK_ID,
  N.GENERATED,
  N.CONSOLIDATE,
  N.SET_OF_ACCOUNTS_ID,
  N.CCY,
  N.ACCOUNT_TYPE,
  N.USED_BY_BA,
  N.REVAL,
  N.REVAL_LOSS_CHART_ACC_ID,
  N.REVAL_GAIN_CHART_ACC_ID,
  N.REVAL_TOT,
  N.REVAL_DT,
  N.MAN_POST,
  N.MASK_COUNTRY_ID,
  N.MASK_LOCATION_ID,
  N.MASK_CPARTY_ID,
  N.MASK_ACCEPTOR_ID,
  N.IN_USE,
  N.HASHKEY,
  N.COLLISION,
  N.STAMP,
  N.DATE_CREATE,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(CHART_ACC_ID, 0 ) AS CHART_ACC_ID ,
  COALESCE(COA_CODE, '' ) AS COA_CODE ,
  COALESCE(COA_NAME, '' ) AS COA_NAME ,
  COALESCE(COA_MASK_ID, 0 ) AS COA_MASK_ID ,
  COALESCE(GENERATED, 0 ) AS GENERATED ,
  COALESCE(CONSOLIDATE, 0 ) AS CONSOLIDATE ,
  COALESCE(SET_OF_ACCOUNTS_ID, 0 ) AS SET_OF_ACCOUNTS_ID ,
  COALESCE(CCY, '' ) AS CCY ,
  COALESCE(ACCOUNT_TYPE, 0 ) AS ACCOUNT_TYPE ,
  COALESCE(USED_BY_BA, 0 ) AS USED_BY_BA ,
  COALESCE(REVAL, 0 ) AS REVAL ,
  COALESCE(REVAL_LOSS_CHART_ACC_ID, 0 ) AS REVAL_LOSS_CHART_ACC_ID ,
  COALESCE(REVAL_GAIN_CHART_ACC_ID, 0 ) AS REVAL_GAIN_CHART_ACC_ID ,
  COALESCE(REVAL_TOT, 0 ) AS REVAL_TOT ,
  COALESCE(REVAL_DT, '' ) AS REVAL_DT ,
  COALESCE(MAN_POST, 0 ) AS MAN_POST ,
  COALESCE(MASK_COUNTRY_ID, 0 ) AS MASK_COUNTRY_ID ,
  COALESCE(MASK_LOCATION_ID, 0 ) AS MASK_LOCATION_ID ,
  COALESCE(MASK_CPARTY_ID, 0 ) AS MASK_CPARTY_ID ,
  COALESCE(MASK_ACCEPTOR_ID, 0 ) AS MASK_ACCEPTOR_ID ,
  COALESCE(IN_USE, 0 ) AS IN_USE ,
  COALESCE(HASHKEY, 0 ) AS HASHKEY ,
  COALESCE(COLLISION, 0 ) AS COLLISION ,
  COALESCE(STAMP, 0 ) AS STAMP ,
  COALESCE(DATE_CREATE, '' ) AS DATE_CREATE 
 FROM  dw_tdata.COS_000_CHART_ACC_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  CHART_ACC_ID ,
  COA_CODE ,
  COA_NAME ,
  COA_MASK_ID ,
  GENERATED ,
  CONSOLIDATE ,
  SET_OF_ACCOUNTS_ID ,
  CCY ,
  ACCOUNT_TYPE ,
  USED_BY_BA ,
  REVAL ,
  REVAL_LOSS_CHART_ACC_ID ,
  REVAL_GAIN_CHART_ACC_ID ,
  REVAL_TOT ,
  REVAL_DT ,
  MAN_POST ,
  MASK_COUNTRY_ID ,
  MASK_LOCATION_ID ,
  MASK_CPARTY_ID ,
  MASK_ACCEPTOR_ID ,
  IN_USE ,
  HASHKEY ,
  COLLISION ,
  STAMP ,
  DATE_CREATE 
 FROM dw_sdata.COS_000_CHART_ACC 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.CHART_ACC_ID = T.CHART_ACC_ID
WHERE
(T.CHART_ACC_ID IS NULL)
 OR N.COA_CODE<>T.COA_CODE
 OR N.COA_NAME<>T.COA_NAME
 OR N.COA_MASK_ID<>T.COA_MASK_ID
 OR N.GENERATED<>T.GENERATED
 OR N.CONSOLIDATE<>T.CONSOLIDATE
 OR N.SET_OF_ACCOUNTS_ID<>T.SET_OF_ACCOUNTS_ID
 OR N.CCY<>T.CCY
 OR N.ACCOUNT_TYPE<>T.ACCOUNT_TYPE
 OR N.USED_BY_BA<>T.USED_BY_BA
 OR N.REVAL<>T.REVAL
 OR N.REVAL_LOSS_CHART_ACC_ID<>T.REVAL_LOSS_CHART_ACC_ID
 OR N.REVAL_GAIN_CHART_ACC_ID<>T.REVAL_GAIN_CHART_ACC_ID
 OR N.REVAL_TOT<>T.REVAL_TOT
 OR N.REVAL_DT<>T.REVAL_DT
 OR N.MAN_POST<>T.MAN_POST
 OR N.MASK_COUNTRY_ID<>T.MASK_COUNTRY_ID
 OR N.MASK_LOCATION_ID<>T.MASK_LOCATION_ID
 OR N.MASK_CPARTY_ID<>T.MASK_CPARTY_ID
 OR N.MASK_ACCEPTOR_ID<>T.MASK_ACCEPTOR_ID
 OR N.IN_USE<>T.IN_USE
 OR N.HASHKEY<>T.HASHKEY
 OR N.COLLISION<>T.COLLISION
 OR N.STAMP<>T.STAMP
 OR N.DATE_CREATE<>T.DATE_CREATE
;

--Step3:
UPDATE dw_sdata.COS_000_CHART_ACC P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_150
WHERE P.End_Dt=DATE('2100-12-31')
AND P.CHART_ACC_ID=T_150.CHART_ACC_ID
;

--Step4:
INSERT  INTO dw_sdata.COS_000_CHART_ACC SELECT * FROM T_150;

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
