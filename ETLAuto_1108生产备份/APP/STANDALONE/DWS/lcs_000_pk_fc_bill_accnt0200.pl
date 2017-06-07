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
DELETE FROM dw_sdata.LCS_000_PK_FC_BILL_ACCNT WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.LCS_000_PK_FC_BILL_ACCNT SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_286 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.LCS_000_PK_FC_BILL_ACCNT WHERE 1=0;

--Step2:
INSERT  INTO T_286 (
  PK_ACC,
  PK_NO,
  CSTM_NAME,
  PK_BAL,
  CURR_TYPE,
  CH_TYPE,
  OPEN_DATE,
  OPEN_INST,
  PWD,
  B_FLAG,
  PART_DRAW_NUM,
  DAC,
  B_FLAG_INFO,
  start_dt,
  end_dt)
SELECT
  N.PK_ACC,
  N.PK_NO,
  N.CSTM_NAME,
  N.PK_BAL,
  N.CURR_TYPE,
  N.CH_TYPE,
  N.OPEN_DATE,
  N.OPEN_INST,
  N.PWD,
  N.B_FLAG,
  N.PART_DRAW_NUM,
  N.DAC,
  N.B_FLAG_INFO,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(PK_ACC, '' ) AS PK_ACC ,
  COALESCE(PK_NO, 0 ) AS PK_NO ,
  COALESCE(CSTM_NAME, '' ) AS CSTM_NAME ,
  COALESCE(PK_BAL, 0 ) AS PK_BAL ,
  COALESCE(CURR_TYPE, '' ) AS CURR_TYPE ,
  COALESCE(CH_TYPE, '' ) AS CH_TYPE ,
  COALESCE(OPEN_DATE, 0 ) AS OPEN_DATE ,
  COALESCE(OPEN_INST, '' ) AS OPEN_INST ,
  COALESCE(PWD, '0' ) AS PWD ,
  COALESCE(B_FLAG, 0 ) AS B_FLAG ,
  COALESCE(PART_DRAW_NUM, 0 ) AS PART_DRAW_NUM ,
  COALESCE(DAC, '' ) AS DAC ,
  COALESCE(B_FLAG_INFO, 0 ) AS B_FLAG_INFO 
 FROM  dw_tdata.LCS_000_PK_FC_BILL_ACCNT_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  PK_ACC ,
  PK_NO ,
  CSTM_NAME ,
  PK_BAL ,
  CURR_TYPE ,
  CH_TYPE ,
  OPEN_DATE ,
  OPEN_INST ,
  PWD ,
  B_FLAG ,
  PART_DRAW_NUM ,
  DAC ,
  B_FLAG_INFO 
 FROM dw_sdata.LCS_000_PK_FC_BILL_ACCNT 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.PK_ACC = T.PK_ACC
WHERE
(T.PK_ACC IS NULL)
 OR N.PK_NO<>T.PK_NO
 OR N.CSTM_NAME<>T.CSTM_NAME
 OR N.PK_BAL<>T.PK_BAL
 OR N.CURR_TYPE<>T.CURR_TYPE
 OR N.CH_TYPE<>T.CH_TYPE
 OR N.OPEN_DATE<>T.OPEN_DATE
 OR N.OPEN_INST<>T.OPEN_INST
 OR N.PWD<>T.PWD
 OR N.B_FLAG<>T.B_FLAG
 OR N.PART_DRAW_NUM<>T.PART_DRAW_NUM
 OR N.DAC<>T.DAC
 OR N.B_FLAG_INFO<>T.B_FLAG_INFO
;

--Step3:
UPDATE dw_sdata.LCS_000_PK_FC_BILL_ACCNT P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_286
WHERE P.End_Dt=DATE('2100-12-31')
AND P.PK_ACC=T_286.PK_ACC
;

--Step4:
INSERT  INTO dw_sdata.LCS_000_PK_FC_BILL_ACCNT SELECT * FROM T_286;

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
