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
DELETE FROM dw_sdata.FPS_000_COMMISSIONINF WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.FPS_000_COMMISSIONINF SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_171 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.FPS_000_COMMISSIONINF WHERE 1=0;

--Step2:
INSERT  INTO T_171 (
  INSTCODE,
  COMMISSION_SWITCH,
  COMMISSION,
  COMMISSION_FLG,
  COMMISSION_RATE,
  COMMISSION_MIN,
  COMMISSION_MAX,
  YINLIAN_REC,
  CUSTOMER_REC,
  INST_REC,
  start_dt,
  end_dt)
SELECT
  N.INSTCODE,
  N.COMMISSION_SWITCH,
  N.COMMISSION,
  N.COMMISSION_FLG,
  N.COMMISSION_RATE,
  N.COMMISSION_MIN,
  N.COMMISSION_MAX,
  N.YINLIAN_REC,
  N.CUSTOMER_REC,
  N.INST_REC,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(INSTCODE, '' ) AS INSTCODE ,
  COALESCE(COMMISSION_SWITCH, '' ) AS COMMISSION_SWITCH ,
  COALESCE(COMMISSION, '' ) AS COMMISSION ,
  COALESCE(COMMISSION_FLG, '' ) AS COMMISSION_FLG ,
  COALESCE(COMMISSION_RATE, '' ) AS COMMISSION_RATE ,
  COALESCE(COMMISSION_MIN, '' ) AS COMMISSION_MIN ,
  COALESCE(COMMISSION_MAX, '' ) AS COMMISSION_MAX ,
  COALESCE(YINLIAN_REC, '' ) AS YINLIAN_REC ,
  COALESCE(CUSTOMER_REC, '' ) AS CUSTOMER_REC ,
  COALESCE(INST_REC, '' ) AS INST_REC 
 FROM  dw_tdata.FPS_000_COMMISSIONINF_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  INSTCODE ,
  COMMISSION_SWITCH ,
  COMMISSION ,
  COMMISSION_FLG ,
  COMMISSION_RATE ,
  COMMISSION_MIN ,
  COMMISSION_MAX ,
  YINLIAN_REC ,
  CUSTOMER_REC ,
  INST_REC 
 FROM dw_sdata.FPS_000_COMMISSIONINF 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.INSTCODE = T.INSTCODE
WHERE
(T.INSTCODE IS NULL)
 OR N.COMMISSION_SWITCH<>T.COMMISSION_SWITCH
 OR N.COMMISSION<>T.COMMISSION
 OR N.COMMISSION_FLG<>T.COMMISSION_FLG
 OR N.COMMISSION_RATE<>T.COMMISSION_RATE
 OR N.COMMISSION_MIN<>T.COMMISSION_MIN
 OR N.COMMISSION_MAX<>T.COMMISSION_MAX
 OR N.YINLIAN_REC<>T.YINLIAN_REC
 OR N.CUSTOMER_REC<>T.CUSTOMER_REC
 OR N.INST_REC<>T.INST_REC
;

--Step3:
UPDATE dw_sdata.FPS_000_COMMISSIONINF P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_171
WHERE P.End_Dt=DATE('2100-12-31')
AND P.INSTCODE=T_171.INSTCODE
;

--Step4:
INSERT  INTO dw_sdata.FPS_000_COMMISSIONINF SELECT * FROM T_171;

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
