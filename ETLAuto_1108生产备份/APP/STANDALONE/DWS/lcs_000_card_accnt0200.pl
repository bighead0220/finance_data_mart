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
DELETE FROM dw_sdata.LCS_000_CARD_ACCNT WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.LCS_000_CARD_ACCNT SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_244 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.LCS_000_CARD_ACCNT WHERE 1=0;

--Step2:
INSERT  INTO T_244 (
  CARD_NO,
  CARD_MEDIUM,
  CARD_TYPE,
  CARD_CLASS,
  CSTM_NAME,
  APP_DATE,
  OPEN_DATE,
  OPEN_INST,
  DUE_DATE,
  DAC,
  MAX_SUB_ACC,
  PWD,
  B_FLAG,
  B_FLAG_INFO,
  start_dt,
  end_dt)
SELECT
  N.CARD_NO,
  N.CARD_MEDIUM,
  N.CARD_TYPE,
  N.CARD_CLASS,
  N.CSTM_NAME,
  N.APP_DATE,
  N.OPEN_DATE,
  N.OPEN_INST,
  N.DUE_DATE,
  N.DAC,
  N.MAX_SUB_ACC,
  N.PWD,
  N.B_FLAG,
  N.B_FLAG_INFO,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(CARD_NO, '' ) AS CARD_NO ,
  COALESCE(CARD_MEDIUM, '' ) AS CARD_MEDIUM ,
  COALESCE(CARD_TYPE, '' ) AS CARD_TYPE ,
  COALESCE(CARD_CLASS, '' ) AS CARD_CLASS ,
  COALESCE(CSTM_NAME, '' ) AS CSTM_NAME ,
  COALESCE(APP_DATE, 0 ) AS APP_DATE ,
  COALESCE(OPEN_DATE, 0 ) AS OPEN_DATE ,
  COALESCE(OPEN_INST, '' ) AS OPEN_INST ,
  COALESCE(DUE_DATE, 0 ) AS DUE_DATE ,
  COALESCE(DAC, '' ) AS DAC ,
  COALESCE(MAX_SUB_ACC, 0 ) AS MAX_SUB_ACC ,
  COALESCE(PWD, '0' ) AS PWD ,
  COALESCE(B_FLAG, 0 ) AS B_FLAG ,
  COALESCE(B_FLAG_INFO, 0 ) AS B_FLAG_INFO 
 FROM  dw_tdata.LCS_000_CARD_ACCNT_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  CARD_NO ,
  CARD_MEDIUM ,
  CARD_TYPE ,
  CARD_CLASS ,
  CSTM_NAME ,
  APP_DATE ,
  OPEN_DATE ,
  OPEN_INST ,
  DUE_DATE ,
  DAC ,
  MAX_SUB_ACC ,
  PWD ,
  B_FLAG ,
  B_FLAG_INFO 
 FROM dw_sdata.LCS_000_CARD_ACCNT 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.CARD_NO = T.CARD_NO
WHERE
(T.CARD_NO IS NULL)
 OR N.CARD_MEDIUM<>T.CARD_MEDIUM
 OR N.CARD_TYPE<>T.CARD_TYPE
 OR N.CARD_CLASS<>T.CARD_CLASS
 OR N.CSTM_NAME<>T.CSTM_NAME
 OR N.APP_DATE<>T.APP_DATE
 OR N.OPEN_DATE<>T.OPEN_DATE
 OR N.OPEN_INST<>T.OPEN_INST
 OR N.DUE_DATE<>T.DUE_DATE
 OR N.DAC<>T.DAC
 OR N.MAX_SUB_ACC<>T.MAX_SUB_ACC
 OR N.PWD<>T.PWD
 OR N.B_FLAG<>T.B_FLAG
 OR N.B_FLAG_INFO<>T.B_FLAG_INFO
;

--Step3:
UPDATE dw_sdata.LCS_000_CARD_ACCNT P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_244
WHERE P.End_Dt=DATE('2100-12-31')
AND P.CARD_NO=T_244.CARD_NO
;

--Step4:
INSERT  INTO dw_sdata.LCS_000_CARD_ACCNT SELECT * FROM T_244;

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
