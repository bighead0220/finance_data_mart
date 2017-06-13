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
DELETE FROM dw_sdata.FSS_001_GF_PROD_NETVALUE_INFO WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.FSS_001_GF_PROD_NETVALUE_INFO SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_362 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.FSS_001_GF_PROD_NETVALUE_INFO WHERE 1=0;

--Step2:
INSERT  INTO T_362 (
PROD_CODE,
NET_VALUE_DATE,
NET_VALUE,
PROFIT_RATE,
TEN_THOUSANDTH_PROFIT,
VALID_STATUS,
LATEST_MODIFY_DATE,
LATEST_MODIFY_TIME,
LATEST_OPER_CODE,
START_DT,
END_DT)
SELECT
N.PROD_CODE,
N.NET_VALUE_DATE,
N.NET_VALUE,
N.PROFIT_RATE,
N.TEN_THOUSANDTH_PROFIT,
N.VALID_STATUS,
N.LATEST_MODIFY_DATE,
N.LATEST_MODIFY_TIME,
N.LATEST_OPER_CODE,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
COALESCE(PROD_CODE,'') AS PROD_CODE,
COALESCE(NET_VALUE_DATE,'') AS NET_VALUE_DATE,
COALESCE(NET_VALUE,0) AS NET_VALUE,
COALESCE(PROFIT_RATE,0) AS PROFIT_RATE,
COALESCE(TEN_THOUSANDTH_PROFIT,0) AS TEN_THOUSANDTH_PROFIT,
COALESCE(VALID_STATUS,'') AS VALID_STATUS,
COALESCE(LATEST_MODIFY_DATE,'') AS LATEST_MODIFY_DATE,
COALESCE(LATEST_MODIFY_TIME,'') AS LATEST_MODIFY_TIME,
COALESCE(LATEST_OPER_CODE,'') AS LATEST_OPER_CODE,
 FROM  dw_tdata.FSS_001_GF_PROD_NETVALUE_INFO_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
PROD_CODE,
NET_VALUE_DATE,
NET_VALUE,
PROFIT_RATE,
TEN_THOUSANDTH_PROFIT,
VALID_STATUS,
LATEST_MODIFY_DATE,
LATEST_MODIFY_TIME,
LATEST_OPER_CODE
 FROM dw_sdata.FSS_001_GF_PROD_NETVALUE_INFO 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.PROD_CODE = T.PROD_CODE AND N.NET_VALUE_DATE = T.NET_VALUE_DATE AND N.VALID_STATUS = T.VALID_STATUS
WHERE
(T.PROD_CODE IS NULL AND T.NET_VALUE_DATE IS NULL AND T.VALID_STATUS IS NULL)
OR N.PROD_CODE<>T.PROD_CODE
OR N.NET_VALUE_DATE<>T.NET_VALUE_DATE
OR N.NET_VALUE<>T.NET_VALUE
OR N.PROFIT_RATE<>T.PROFIT_RATE
OR N.TEN_THOUSANDTH_PROFIT<>T.TEN_THOUSANDTH_PROFIT
OR N.VALID_STATUS<>T.VALID_STATUS
OR N.LATEST_MODIFY_DATE<>T.LATEST_MODIFY_DATE
OR N.LATEST_MODIFY_TIME<>T.LATEST_MODIFY_TIME
OR N.LATEST_OPER_CODE<>T.LATEST_OPER_CODE
;

--Step3:
UPDATE dw_sdata.FSS_001_GF_PROD_NETVALUE_INFO P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_362
WHERE P.End_Dt=DATE('2100-12-31')
AND P.PROD_CODE=T_362.PROD_CODE
AND P.NET_VALUE_DATE=T_362.NET_VALUE_DATE
AND P.VALID_STATUS=T_362.VALID_STATUS
;

--Step4:
INSERT  INTO dw_sdata.FSS_001_GF_PROD_NETVALUE_INFO SELECT * FROM T_362;

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
