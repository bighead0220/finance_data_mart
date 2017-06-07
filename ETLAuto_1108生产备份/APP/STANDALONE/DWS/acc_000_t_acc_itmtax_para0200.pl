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
DELETE FROM dw_sdata.ACC_000_T_ACC_ITMTAX_PARA WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.ACC_000_T_ACC_ITMTAX_PARA SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_345 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.ACC_000_T_ACC_ITMTAX_PARA WHERE 1=0;

--Step2:
INSERT  INTO T_345 (
  ITM_NO,
  INST_TYPE,
  IN_OUT_TYPE,
  TAX_ATTR,
  TAX_RATE,
  VAT_ITM_NO,
  START_FLAG,
  START_DATE,
  END_DATE,
  TLR_NO,
  AUTH_NO,
  TRAN_INST,
  TRAN_TIME,
  start_dt,
  end_dt)
SELECT
  N.ITM_NO,
  N.INST_TYPE,
  N.IN_OUT_TYPE,
  N.TAX_ATTR,
  N.TAX_RATE,
  N.VAT_ITM_NO,
  N.START_FLAG,
  N.START_DATE,
  N.END_DATE,
  N.TLR_NO,
  N.AUTH_NO,
  N.TRAN_INST,
  N.TRAN_TIME,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(ITM_NO, '' ) AS ITM_NO ,
  COALESCE(INST_TYPE, '' ) AS INST_TYPE ,
  COALESCE(IN_OUT_TYPE, '' ) AS IN_OUT_TYPE ,
  COALESCE(TAX_ATTR, '' ) AS TAX_ATTR ,
  COALESCE(TAX_RATE, 0 ) AS TAX_RATE ,
  COALESCE(VAT_ITM_NO, '' ) AS VAT_ITM_NO ,
  COALESCE(START_FLAG, '' ) AS START_FLAG ,
  COALESCE(START_DATE, '' ) AS START_DATE ,
  COALESCE(END_DATE, '' ) AS END_DATE ,
  COALESCE(TLR_NO, '' ) AS TLR_NO ,
  COALESCE(AUTH_NO, '' ) AS AUTH_NO ,
  COALESCE(TRAN_INST, '' ) AS TRAN_INST ,
  COALESCE(TRAN_TIME, '' ) AS TRAN_TIME 
 FROM  dw_tdata.ACC_000_T_ACC_ITMTAX_PARA_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  ITM_NO ,
  INST_TYPE ,
  IN_OUT_TYPE ,
  TAX_ATTR ,
  TAX_RATE ,
  VAT_ITM_NO ,
  START_FLAG ,
  START_DATE ,
  END_DATE ,
  TLR_NO ,
  AUTH_NO ,
  TRAN_INST ,
  TRAN_TIME 
 FROM dw_sdata.ACC_000_T_ACC_ITMTAX_PARA 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.ITM_NO = T.ITM_NO AND N.INST_TYPE = T.INST_TYPE AND N.START_DATE = T.START_DATE
WHERE
(T.ITM_NO IS NULL AND T.INST_TYPE IS NULL AND T.START_DATE IS NULL)
 OR N.IN_OUT_TYPE<>T.IN_OUT_TYPE
 OR N.TAX_ATTR<>T.TAX_ATTR
 OR N.TAX_RATE<>T.TAX_RATE
 OR N.VAT_ITM_NO<>T.VAT_ITM_NO
 OR N.START_FLAG<>T.START_FLAG
 OR N.END_DATE<>T.END_DATE
 OR N.TLR_NO<>T.TLR_NO
 OR N.AUTH_NO<>T.AUTH_NO
 OR N.TRAN_INST<>T.TRAN_INST
 OR N.TRAN_TIME<>T.TRAN_TIME
;

--Step3:
UPDATE dw_sdata.ACC_000_T_ACC_ITMTAX_PARA P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_345
WHERE P.End_Dt=DATE('2100-12-31')
AND P.ITM_NO=T_345.ITM_NO
AND P.INST_TYPE=T_345.INST_TYPE
AND P.START_DATE=T_345.START_DATE
;

--Step4:
INSERT  INTO dw_sdata.ACC_000_T_ACC_ITMTAX_PARA SELECT * FROM T_345;

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
