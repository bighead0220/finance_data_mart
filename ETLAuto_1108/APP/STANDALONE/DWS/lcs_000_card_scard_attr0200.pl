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
DELETE FROM dw_sdata.LCS_000_CARD_SCARD_ATTR WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.LCS_000_CARD_SCARD_ATTR SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_263 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.LCS_000_CARD_SCARD_ATTR WHERE 1=0;

--Step2:
INSERT  INTO T_263 (
  CARD_NO,
  MAIN_CARD_NO,
  CARD_SER,
  CTRL_FLAG,
  CYCLE,
  DRAW_PER_LIMIT,
  CYCLE_AMT,
  ADJUST_DATE,
  CUR_CYCLE_AMT,
  N_FLAG,
  DAC,
  start_dt,
  end_dt)
SELECT
  N.CARD_NO,
  N.MAIN_CARD_NO,
  N.CARD_SER,
  N.CTRL_FLAG,
  N.CYCLE,
  N.DRAW_PER_LIMIT,
  N.CYCLE_AMT,
  N.ADJUST_DATE,
  N.CUR_CYCLE_AMT,
  N.N_FLAG,
  N.DAC,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(CARD_NO, '' ) AS CARD_NO ,
  COALESCE(MAIN_CARD_NO, '' ) AS MAIN_CARD_NO ,
  COALESCE(CARD_SER, 0 ) AS CARD_SER ,
  COALESCE(CTRL_FLAG, '' ) AS CTRL_FLAG ,
  COALESCE(CYCLE, '' ) AS CYCLE ,
  COALESCE(DRAW_PER_LIMIT, 0 ) AS DRAW_PER_LIMIT ,
  COALESCE(CYCLE_AMT, 0 ) AS CYCLE_AMT ,
  COALESCE(ADJUST_DATE, 0 ) AS ADJUST_DATE ,
  COALESCE(CUR_CYCLE_AMT, 0 ) AS CUR_CYCLE_AMT ,
  COALESCE(N_FLAG, 0 ) AS N_FLAG ,
  COALESCE(DAC, '' ) AS DAC 
 FROM  dw_tdata.LCS_000_CARD_SCARD_ATTR_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  CARD_NO ,
  MAIN_CARD_NO ,
  CARD_SER ,
  CTRL_FLAG ,
  CYCLE ,
  DRAW_PER_LIMIT ,
  CYCLE_AMT ,
  ADJUST_DATE ,
  CUR_CYCLE_AMT ,
  N_FLAG ,
  DAC 
 FROM dw_sdata.LCS_000_CARD_SCARD_ATTR 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.CARD_NO = T.CARD_NO
WHERE
(T.CARD_NO IS NULL)
 OR N.MAIN_CARD_NO<>T.MAIN_CARD_NO
 OR N.CARD_SER<>T.CARD_SER
 OR N.CTRL_FLAG<>T.CTRL_FLAG
 OR N.CYCLE<>T.CYCLE
 OR N.DRAW_PER_LIMIT<>T.DRAW_PER_LIMIT
 OR N.CYCLE_AMT<>T.CYCLE_AMT
 OR N.ADJUST_DATE<>T.ADJUST_DATE
 OR N.CUR_CYCLE_AMT<>T.CUR_CYCLE_AMT
 OR N.N_FLAG<>T.N_FLAG
 OR N.DAC<>T.DAC
;

--Step3:
UPDATE dw_sdata.LCS_000_CARD_SCARD_ATTR P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_263
WHERE P.End_Dt=DATE('2100-12-31')
AND P.CARD_NO=T_263.CARD_NO
;

--Step4:
INSERT  INTO dw_sdata.LCS_000_CARD_SCARD_ATTR SELECT * FROM T_263;

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
