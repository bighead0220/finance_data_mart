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
DELETE FROM dw_sdata.CBS_001_PMCTL_KIND_TYPE WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.CBS_001_PMCTL_KIND_TYPE SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_81 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.CBS_001_PMCTL_KIND_TYPE WHERE 1=0;

--Step2:
INSERT  INTO T_81 (
  DEP_CODE,
  KINDTYPE_NAME,
  CUR_CODE,
  FLAG,
  DEP_TERM,
  MIN_DEP_AMT,
  MIN_DW_AMT,
  MIN_KEEP_AMT,
  B_IRATE_CODE,
  C_IRATE_CODE,
  D_IRATE_CODE,
  TERM_LIMIT,
  FINANCE_FLAG,
  BANK_FLAG,
  OVDR_FLAG,
  VALID_FLAG,
  FLOAT_FLAG,
  MTHD_FLAG,
  start_dt,
  end_dt)
SELECT
  N.DEP_CODE,
  N.KINDTYPE_NAME,
  N.CUR_CODE,
  N.FLAG,
  N.DEP_TERM,
  N.MIN_DEP_AMT,
  N.MIN_DW_AMT,
  N.MIN_KEEP_AMT,
  N.B_IRATE_CODE,
  N.C_IRATE_CODE,
  N.D_IRATE_CODE,
  N.TERM_LIMIT,
  N.FINANCE_FLAG,
  N.BANK_FLAG,
  N.OVDR_FLAG,
  N.VALID_FLAG,
  N.FLOAT_FLAG,
  N.MTHD_FLAG,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(DEP_CODE, '' ) AS DEP_CODE ,
  COALESCE(KINDTYPE_NAME, '' ) AS KINDTYPE_NAME ,
  COALESCE(CUR_CODE, '' ) AS CUR_CODE ,
  COALESCE(FLAG, '' ) AS FLAG ,
  COALESCE(DEP_TERM, '' ) AS DEP_TERM ,
  COALESCE(MIN_DEP_AMT, 0 ) AS MIN_DEP_AMT ,
  COALESCE(MIN_DW_AMT, 0 ) AS MIN_DW_AMT ,
  COALESCE(MIN_KEEP_AMT, 0 ) AS MIN_KEEP_AMT ,
  COALESCE(B_IRATE_CODE, '' ) AS B_IRATE_CODE ,
  COALESCE(C_IRATE_CODE, '' ) AS C_IRATE_CODE ,
  COALESCE(D_IRATE_CODE, '' ) AS D_IRATE_CODE ,
  COALESCE(TERM_LIMIT, 0 ) AS TERM_LIMIT ,
  COALESCE(FINANCE_FLAG, '' ) AS FINANCE_FLAG ,
  COALESCE(BANK_FLAG, '' ) AS BANK_FLAG ,
  COALESCE(OVDR_FLAG, '' ) AS OVDR_FLAG ,
  COALESCE(VALID_FLAG, '' ) AS VALID_FLAG ,
  COALESCE(FLOAT_FLAG, '' ) AS FLOAT_FLAG ,
  COALESCE(MTHD_FLAG, '' ) AS MTHD_FLAG 
 FROM  dw_tdata.CBS_001_PMCTL_KIND_TYPE_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  DEP_CODE ,
  KINDTYPE_NAME ,
  CUR_CODE ,
  FLAG ,
  DEP_TERM ,
  MIN_DEP_AMT ,
  MIN_DW_AMT ,
  MIN_KEEP_AMT ,
  B_IRATE_CODE ,
  C_IRATE_CODE ,
  D_IRATE_CODE ,
  TERM_LIMIT ,
  FINANCE_FLAG ,
  BANK_FLAG ,
  OVDR_FLAG ,
  VALID_FLAG ,
  FLOAT_FLAG ,
  MTHD_FLAG 
 FROM dw_sdata.CBS_001_PMCTL_KIND_TYPE 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.DEP_CODE = T.DEP_CODE
WHERE
(T.DEP_CODE IS NULL)
 OR N.KINDTYPE_NAME<>T.KINDTYPE_NAME
 OR N.CUR_CODE<>T.CUR_CODE
 OR N.FLAG<>T.FLAG
 OR N.DEP_TERM<>T.DEP_TERM
 OR N.MIN_DEP_AMT<>T.MIN_DEP_AMT
 OR N.MIN_DW_AMT<>T.MIN_DW_AMT
 OR N.MIN_KEEP_AMT<>T.MIN_KEEP_AMT
 OR N.B_IRATE_CODE<>T.B_IRATE_CODE
 OR N.C_IRATE_CODE<>T.C_IRATE_CODE
 OR N.D_IRATE_CODE<>T.D_IRATE_CODE
 OR N.TERM_LIMIT<>T.TERM_LIMIT
 OR N.FINANCE_FLAG<>T.FINANCE_FLAG
 OR N.BANK_FLAG<>T.BANK_FLAG
 OR N.OVDR_FLAG<>T.OVDR_FLAG
 OR N.VALID_FLAG<>T.VALID_FLAG
 OR N.FLOAT_FLAG<>T.FLOAT_FLAG
 OR N.MTHD_FLAG<>T.MTHD_FLAG
;

--Step3:
UPDATE dw_sdata.CBS_001_PMCTL_KIND_TYPE P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_81
WHERE P.End_Dt=DATE('2100-12-31')
AND P.DEP_CODE=T_81.DEP_CODE
;

--Step4:
INSERT  INTO dw_sdata.CBS_001_PMCTL_KIND_TYPE SELECT * FROM T_81;

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
