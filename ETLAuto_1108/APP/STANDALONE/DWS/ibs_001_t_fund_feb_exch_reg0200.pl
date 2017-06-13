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
DELETE FROM dw_sdata.IBS_001_T_FUND_FEB_EXCH_REG WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.IBS_001_T_FUND_FEB_EXCH_REG SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_218 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.IBS_001_T_FUND_FEB_EXCH_REG WHERE 1=0;

--Step2:
INSERT  INTO T_218 (
  TRAN_DATE,
  CLT_SEQNO,
  SERINO,
  TRAN_NO,
  SUB_CODE,
  TRAN_TIME,
  TRAN_ABBR,
  CURR_TYPE1,
  CURR_TYPE2,
  DOL_PRC1,
  DOL_PRC2,
  FC_RATION1,
  FC_RATION2,
  AMT1,
  AMT2,
  DOL_AMT1,
  DOL_AMT2,
  FLAG,
  start_dt,
  end_dt)
SELECT
  N.TRAN_DATE,
  N.CLT_SEQNO,
  N.SERINO,
  N.TRAN_NO,
  N.SUB_CODE,
  N.TRAN_TIME,
  N.TRAN_ABBR,
  N.CURR_TYPE1,
  N.CURR_TYPE2,
  N.DOL_PRC1,
  N.DOL_PRC2,
  N.FC_RATION1,
  N.FC_RATION2,
  N.AMT1,
  N.AMT2,
  N.DOL_AMT1,
  N.DOL_AMT2,
  N.FLAG,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(TRAN_DATE, 0 ) AS TRAN_DATE ,
  COALESCE(CLT_SEQNO, '' ) AS CLT_SEQNO ,
  COALESCE(SERINO, 0 ) AS SERINO ,
  COALESCE(TRAN_NO, '' ) AS TRAN_NO ,
  COALESCE(SUB_CODE, '' ) AS SUB_CODE ,
  COALESCE(TRAN_TIME, '' ) AS TRAN_TIME ,
  COALESCE(TRAN_ABBR, '' ) AS TRAN_ABBR ,
  COALESCE(CURR_TYPE1, '' ) AS CURR_TYPE1 ,
  COALESCE(CURR_TYPE2, '' ) AS CURR_TYPE2 ,
  COALESCE(DOL_PRC1, 0 ) AS DOL_PRC1 ,
  COALESCE(DOL_PRC2, 0 ) AS DOL_PRC2 ,
  COALESCE(FC_RATION1, 0 ) AS FC_RATION1 ,
  COALESCE(FC_RATION2, 0 ) AS FC_RATION2 ,
  COALESCE(AMT1, 0 ) AS AMT1 ,
  COALESCE(AMT2, 0 ) AS AMT2 ,
  COALESCE(DOL_AMT1, 0 ) AS DOL_AMT1 ,
  COALESCE(DOL_AMT2, 0 ) AS DOL_AMT2 ,
  COALESCE(FLAG, '' ) AS FLAG 
 FROM  dw_tdata.IBS_001_T_FUND_FEB_EXCH_REG_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  TRAN_DATE ,
  CLT_SEQNO ,
  SERINO ,
  TRAN_NO ,
  SUB_CODE ,
  TRAN_TIME ,
  TRAN_ABBR ,
  CURR_TYPE1 ,
  CURR_TYPE2 ,
  DOL_PRC1 ,
  DOL_PRC2 ,
  FC_RATION1 ,
  FC_RATION2 ,
  AMT1 ,
  AMT2 ,
  DOL_AMT1 ,
  DOL_AMT2 ,
  FLAG 
 FROM dw_sdata.IBS_001_T_FUND_FEB_EXCH_REG 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.TRAN_DATE = T.TRAN_DATE AND N.CLT_SEQNO = T.CLT_SEQNO AND N.SERINO = T.SERINO
WHERE
(T.TRAN_DATE IS NULL AND T.CLT_SEQNO IS NULL AND T.SERINO IS NULL)
 OR N.TRAN_NO<>T.TRAN_NO
 OR N.SUB_CODE<>T.SUB_CODE
 OR N.TRAN_TIME<>T.TRAN_TIME
 OR N.TRAN_ABBR<>T.TRAN_ABBR
 OR N.CURR_TYPE1<>T.CURR_TYPE1
 OR N.CURR_TYPE2<>T.CURR_TYPE2
 OR N.DOL_PRC1<>T.DOL_PRC1
 OR N.DOL_PRC2<>T.DOL_PRC2
 OR N.FC_RATION1<>T.FC_RATION1
 OR N.FC_RATION2<>T.FC_RATION2
 OR N.AMT1<>T.AMT1
 OR N.AMT2<>T.AMT2
 OR N.DOL_AMT1<>T.DOL_AMT1
 OR N.DOL_AMT2<>T.DOL_AMT2
 OR N.FLAG<>T.FLAG
;

--Step3:
UPDATE dw_sdata.IBS_001_T_FUND_FEB_EXCH_REG P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_218
WHERE P.End_Dt=DATE('2100-12-31')
AND P.TRAN_DATE=T_218.TRAN_DATE
AND P.CLT_SEQNO=T_218.CLT_SEQNO
AND P.SERINO=T_218.SERINO
;

--Step4:
INSERT  INTO dw_sdata.IBS_001_T_FUND_FEB_EXCH_REG SELECT * FROM T_218;

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
