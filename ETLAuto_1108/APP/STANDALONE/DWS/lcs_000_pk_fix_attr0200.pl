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
DELETE FROM dw_sdata.LCS_000_PK_FIX_ATTR WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.LCS_000_PK_FIX_ATTR SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_290 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.LCS_000_PK_FIX_ATTR WHERE 1=0;

--Step2:
INSERT  INTO T_290 (
  ACC,
  CRDR_RECD,
  DRAW_NUM,
  CTRL_AMT,
  B_BREAK_FLAG,
  MON_AMT,
  LST_TRAN_DATE,
  KPINT,
  DAC,
  start_dt,
  end_dt)
SELECT
  N.ACC,
  N.CRDR_RECD,
  N.DRAW_NUM,
  N.CTRL_AMT,
  N.B_BREAK_FLAG,
  N.MON_AMT,
  N.LST_TRAN_DATE,
  N.KPINT,
  N.DAC,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(ACC, '' ) AS ACC ,
  COALESCE(CRDR_RECD, '' ) AS CRDR_RECD ,
  COALESCE(DRAW_NUM, 0 ) AS DRAW_NUM ,
  COALESCE(CTRL_AMT, 0 ) AS CTRL_AMT ,
  COALESCE(B_BREAK_FLAG, 0 ) AS B_BREAK_FLAG ,
  COALESCE(MON_AMT, 0 ) AS MON_AMT ,
  COALESCE(LST_TRAN_DATE, 0 ) AS LST_TRAN_DATE ,
  COALESCE(KPINT, 0 ) AS KPINT ,
  COALESCE(DAC, '' ) AS DAC 
 FROM  dw_tdata.LCS_000_PK_FIX_ATTR_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  ACC ,
  CRDR_RECD ,
  DRAW_NUM ,
  CTRL_AMT ,
  B_BREAK_FLAG ,
  MON_AMT ,
  LST_TRAN_DATE ,
  KPINT ,
  DAC 
 FROM dw_sdata.LCS_000_PK_FIX_ATTR 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.ACC = T.ACC
WHERE
(T.ACC IS NULL)
 OR N.CRDR_RECD<>T.CRDR_RECD
 OR N.DRAW_NUM<>T.DRAW_NUM
 OR N.CTRL_AMT<>T.CTRL_AMT
 OR N.B_BREAK_FLAG<>T.B_BREAK_FLAG
 OR N.MON_AMT<>T.MON_AMT
 OR N.LST_TRAN_DATE<>T.LST_TRAN_DATE
 OR N.KPINT<>T.KPINT
 OR N.DAC<>T.DAC
;

--Step3:
UPDATE dw_sdata.LCS_000_PK_FIX_ATTR P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_290
WHERE P.End_Dt=DATE('2100-12-31')
AND P.ACC=T_290.ACC
;

--Step4:
INSERT  INTO dw_sdata.LCS_000_PK_FIX_ATTR SELECT * FROM T_290;

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
