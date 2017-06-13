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
DELETE FROM dw_sdata.CBS_001_FSRGT_CUST_LIMIT WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.CBS_001_FSRGT_CUST_LIMIT SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_365 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.CBS_001_FSRGT_CUST_LIMIT WHERE 1=0;

--Step2:
INSERT  INTO T_365 (
  CUST_ID,
  CUST_NAME,
  CONTRACT,
  CUR_CODE,
  OVRD_LIMIT,
  I_FLAG,
  OVRD_INTCODE,
  FLOAT_IRATE,
  OVRD_IRATE,
  START_DATE,
  END_DATE,
  ADD_DAYS,
  CONC_OVRD_DAYS,
  SIGN_DATE,
  OPERATOR,
  CHECKER,
  AUTH_OPR,
  UNSIGN_DATE,
  UNSIGN_OPR,
  UNSIGN_CHECKER,
  UNSIGN_AUTH_OPR,
  FIELD1,
  FIELD2,
  VALID_FLAG,
  start_dt,
  end_dt)
SELECT
  N.CUST_ID,
  N.CUST_NAME,
  N.CONTRACT,
  N.CUR_CODE,
  N.OVRD_LIMIT,
  N.I_FLAG,
  N.OVRD_INTCODE,
  N.FLOAT_IRATE,
  N.OVRD_IRATE,
  N.START_DATE,
  N.END_DATE,
  N.ADD_DAYS,
  N.CONC_OVRD_DAYS,
  N.SIGN_DATE,
  N.OPERATOR,
  N.CHECKER,
  N.AUTH_OPR,
  N.UNSIGN_DATE,
  N.UNSIGN_OPR,
  N.UNSIGN_CHECKER,
  N.UNSIGN_AUTH_OPR,
  N.FIELD1,
  N.FIELD2,
  N.VALID_FLAG,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(CUST_ID, '' ) AS CUST_ID ,
  COALESCE(CUST_NAME, '' ) AS CUST_NAME ,
  COALESCE(CONTRACT, '' ) AS CONTRACT ,
  COALESCE(CUR_CODE, '' ) AS CUR_CODE ,
  COALESCE(OVRD_LIMIT, 0 ) AS OVRD_LIMIT ,
  COALESCE(I_FLAG, '' ) AS I_FLAG ,
  COALESCE(OVRD_INTCODE, '' ) AS OVRD_INTCODE ,
  COALESCE(FLOAT_IRATE, 0 ) AS FLOAT_IRATE ,
  COALESCE(OVRD_IRATE, 0 ) AS OVRD_IRATE ,
  COALESCE(START_DATE, '' ) AS START_DATE ,
  COALESCE(END_DATE, '' ) AS END_DATE ,
  COALESCE(ADD_DAYS, 0 ) AS ADD_DAYS ,
  COALESCE(CONC_OVRD_DAYS, 0 ) AS CONC_OVRD_DAYS ,
  COALESCE(SIGN_DATE, '' ) AS SIGN_DATE ,
  COALESCE(OPERATOR, '' ) AS OPERATOR ,
  COALESCE(CHECKER, '' ) AS CHECKER ,
  COALESCE(AUTH_OPR, '' ) AS AUTH_OPR ,
  COALESCE(UNSIGN_DATE, '' ) AS UNSIGN_DATE ,
  COALESCE(UNSIGN_OPR, '' ) AS UNSIGN_OPR ,
  COALESCE(UNSIGN_CHECKER, '' ) AS UNSIGN_CHECKER ,
  COALESCE(UNSIGN_AUTH_OPR, '' ) AS UNSIGN_AUTH_OPR ,
  COALESCE(FIELD1, '' ) AS FIELD1 ,
  COALESCE(FIELD2, 0 ) AS FIELD2 ,
  COALESCE(VALID_FLAG, '' ) AS VALID_FLAG 
 FROM  dw_tdata.CBS_001_FSRGT_CUST_LIMIT_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  CUST_ID ,
  CUST_NAME ,
  CONTRACT ,
  CUR_CODE ,
  OVRD_LIMIT ,
  I_FLAG ,
  OVRD_INTCODE ,
  FLOAT_IRATE ,
  OVRD_IRATE ,
  START_DATE ,
  END_DATE ,
  ADD_DAYS ,
  CONC_OVRD_DAYS ,
  SIGN_DATE ,
  OPERATOR ,
  CHECKER ,
  AUTH_OPR ,
  UNSIGN_DATE ,
  UNSIGN_OPR ,
  UNSIGN_CHECKER ,
  UNSIGN_AUTH_OPR ,
  FIELD1 ,
  FIELD2 ,
  VALID_FLAG 
 FROM dw_sdata.CBS_001_FSRGT_CUST_LIMIT 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.CUST_ID = T.CUST_ID
WHERE
(T.CUST_ID IS NULL)
 OR N.CUST_NAME<>T.CUST_NAME
 OR N.CONTRACT<>T.CONTRACT
 OR N.CUR_CODE<>T.CUR_CODE
 OR N.OVRD_LIMIT<>T.OVRD_LIMIT
 OR N.I_FLAG<>T.I_FLAG
 OR N.OVRD_INTCODE<>T.OVRD_INTCODE
 OR N.FLOAT_IRATE<>T.FLOAT_IRATE
 OR N.OVRD_IRATE<>T.OVRD_IRATE
 OR N.START_DATE<>T.START_DATE
 OR N.END_DATE<>T.END_DATE
 OR N.ADD_DAYS<>T.ADD_DAYS
 OR N.CONC_OVRD_DAYS<>T.CONC_OVRD_DAYS
 OR N.SIGN_DATE<>T.SIGN_DATE
 OR N.OPERATOR<>T.OPERATOR
 OR N.CHECKER<>T.CHECKER
 OR N.AUTH_OPR<>T.AUTH_OPR
 OR N.UNSIGN_DATE<>T.UNSIGN_DATE
 OR N.UNSIGN_OPR<>T.UNSIGN_OPR
 OR N.UNSIGN_CHECKER<>T.UNSIGN_CHECKER
 OR N.UNSIGN_AUTH_OPR<>T.UNSIGN_AUTH_OPR
 OR N.FIELD1<>T.FIELD1
 OR N.FIELD2<>T.FIELD2
 OR N.VALID_FLAG<>T.VALID_FLAG
;

--Step3:
UPDATE dw_sdata.CBS_001_FSRGT_CUST_LIMIT P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_365
WHERE P.End_Dt=DATE('2100-12-31')
AND P.CUST_ID=T_365.CUST_ID
;

--Step4:
INSERT  INTO dw_sdata.CBS_001_FSRGT_CUST_LIMIT SELECT * FROM T_365;

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
