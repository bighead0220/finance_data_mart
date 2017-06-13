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
  my $rc = open(VSQL, "|vsql -h 22.224.65.171 -p 5433 -d CPCIMDB_TEST -U dwpsbc -w dwpsbc2016");

  unless ($rc) {
      print "Could not invoke VSQL command
";
      return -1;
  }

# ------ Below are VSQL scripts ----------
  print VSQL <<ENDOFINPUT;

\\set ON_ERROR_STOP on

--Step0:
DELETE FROM dw_sdata.LCS_000_PK_FIX_LEG WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.LCS_000_PK_FIX_LEG SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_294 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.LCS_000_PK_FIX_LEG WHERE 1=0;

--Step2:
INSERT  INTO T_294 (
  ACC,
  SVT_NO,
  BAL,
  AVAL_BAL,
  BGN_INT_DATE,
  DUE_DATE,
  AUTO_TSF_FLAG,
  ACCU,
  KPINT,
  B_FLAG,
  start_dt,
  end_dt)
SELECT
  N.ACC,
  N.SVT_NO,
  N.BAL,
  N.AVAL_BAL,
  N.BGN_INT_DATE,
  N.DUE_DATE,
  N.AUTO_TSF_FLAG,
  N.ACCU,
  N.KPINT,
  N.B_FLAG,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(ACC, '' ) AS ACC ,
  COALESCE(SVT_NO, '' ) AS SVT_NO ,
  COALESCE(BAL, 0 ) AS BAL ,
  COALESCE(AVAL_BAL, 0 ) AS AVAL_BAL ,
  COALESCE(BGN_INT_DATE, 0 ) AS BGN_INT_DATE ,
  COALESCE(DUE_DATE, 0 ) AS DUE_DATE ,
  COALESCE(AUTO_TSF_FLAG, 0 ) AS AUTO_TSF_FLAG ,
  COALESCE(ACCU, 0 ) AS ACCU ,
  COALESCE(KPINT, 0 ) AS KPINT ,
  COALESCE(B_FLAG, 0 ) AS B_FLAG 
 FROM  dw_tdata.LCS_000_PK_FIX_LEG_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  ACC ,
  SVT_NO ,
  BAL ,
  AVAL_BAL ,
  BGN_INT_DATE ,
  DUE_DATE ,
  AUTO_TSF_FLAG ,
  ACCU ,
  KPINT ,
  B_FLAG 
 FROM dw_sdata.LCS_000_PK_FIX_LEG 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.ACC = T.ACC
WHERE
(T.ACC IS NULL)
 OR N.SVT_NO<>T.SVT_NO
 OR N.BAL<>T.BAL
 OR N.AVAL_BAL<>T.AVAL_BAL
 OR N.BGN_INT_DATE<>T.BGN_INT_DATE
 OR N.DUE_DATE<>T.DUE_DATE
 OR N.AUTO_TSF_FLAG<>T.AUTO_TSF_FLAG
 OR N.ACCU<>T.ACCU
 OR N.KPINT<>T.KPINT
 OR N.B_FLAG<>T.B_FLAG
;

--Step3:
UPDATE dw_sdata.LCS_000_PK_FIX_LEG P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_294
WHERE P.End_Dt=DATE('2100-12-31')
AND P.ACC=T_294.ACC
;

--Step4:
INSERT  INTO dw_sdata.LCS_000_PK_FIX_LEG SELECT * FROM T_294;

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
   #${LOGON_STR} = "|vsql -h 192.168.2.44 -p 5433 -d CPCIMDB -U ".$user." -w ".$decodepasswd;

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
