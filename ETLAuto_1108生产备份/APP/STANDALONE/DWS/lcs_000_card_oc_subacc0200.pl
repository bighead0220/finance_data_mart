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
DELETE FROM dw_sdata.LCS_000_CARD_OC_SUBACC WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.LCS_000_CARD_OC_SUBACC SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_254 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.LCS_000_CARD_OC_SUBACC WHERE 1=0;

--Step2:
INSERT  INTO T_254 (
  CARD_NO,
  SUB_ACC,
  INN_ACC,
  CURR_TYPE,
  CH_TYPE,
  ACC_TYPE,
  OPEN_INST,
  OPEN_DATE,
  B_FLAG,
  PART_DRAW_NUM,
  DAC,
  start_dt,
  end_dt)
SELECT
  N.CARD_NO,
  N.SUB_ACC,
  N.INN_ACC,
  N.CURR_TYPE,
  N.CH_TYPE,
  N.ACC_TYPE,
  N.OPEN_INST,
  N.OPEN_DATE,
  N.B_FLAG,
  N.PART_DRAW_NUM,
  N.DAC,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(CARD_NO, '' ) AS CARD_NO ,
  COALESCE(SUB_ACC, 0 ) AS SUB_ACC ,
  COALESCE(INN_ACC, '' ) AS INN_ACC ,
  COALESCE(CURR_TYPE, '' ) AS CURR_TYPE ,
  COALESCE(CH_TYPE, '' ) AS CH_TYPE ,
  COALESCE(ACC_TYPE, 0 ) AS ACC_TYPE ,
  COALESCE(OPEN_INST, '' ) AS OPEN_INST ,
  COALESCE(OPEN_DATE, 0 ) AS OPEN_DATE ,
  COALESCE(B_FLAG, 0 ) AS B_FLAG ,
  COALESCE(PART_DRAW_NUM, 0 ) AS PART_DRAW_NUM ,
  COALESCE(DAC, '' ) AS DAC 
 FROM  dw_tdata.LCS_000_CARD_OC_SUBACC_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  CARD_NO ,
  SUB_ACC ,
  INN_ACC ,
  CURR_TYPE ,
  CH_TYPE ,
  ACC_TYPE ,
  OPEN_INST ,
  OPEN_DATE ,
  B_FLAG ,
  PART_DRAW_NUM ,
  DAC 
 FROM dw_sdata.LCS_000_CARD_OC_SUBACC 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.CARD_NO = T.CARD_NO AND N.SUB_ACC = T.SUB_ACC
WHERE
(T.CARD_NO IS NULL AND T.SUB_ACC IS NULL)
 OR N.INN_ACC<>T.INN_ACC
 OR N.CURR_TYPE<>T.CURR_TYPE
 OR N.CH_TYPE<>T.CH_TYPE
 OR N.ACC_TYPE<>T.ACC_TYPE
 OR N.OPEN_INST<>T.OPEN_INST
 OR N.OPEN_DATE<>T.OPEN_DATE
 OR N.B_FLAG<>T.B_FLAG
 OR N.PART_DRAW_NUM<>T.PART_DRAW_NUM
 OR N.DAC<>T.DAC
;

--Step3:
UPDATE dw_sdata.LCS_000_CARD_OC_SUBACC P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_254
WHERE P.End_Dt=DATE('2100-12-31')
AND P.CARD_NO=T_254.CARD_NO
AND P.SUB_ACC=T_254.SUB_ACC
;

--Step4:
INSERT  INTO dw_sdata.LCS_000_CARD_OC_SUBACC SELECT * FROM T_254;

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
