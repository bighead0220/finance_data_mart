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
DELETE FROM dw_sdata.ACC_000_T_INT_ENTRY_CFG WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.ACC_000_T_INT_ENTRY_CFG SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_369 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.ACC_000_T_INT_ENTRY_CFG WHERE 1=0;

--Step2:
INSERT  INTO T_369 (
  N_TRAN_TYPE,
  ACC_ITM,
  DR_ITM,
  CR_ITM,
  ENTRY_CODE,
  ENTRY_NAME,
  ENTRY_ATTR,
  TRAN_CODE,
  REG_DATE,
  LAST_TLR,
  LAST_INST,
  start_dt,
  end_dt)
SELECT
  N.N_TRAN_TYPE,
  N.ACC_ITM,
  N.DR_ITM,
  N.CR_ITM,
  N.ENTRY_CODE,
  N.ENTRY_NAME,
  N.ENTRY_ATTR,
  N.TRAN_CODE,
  N.REG_DATE,
  N.LAST_TLR,
  N.LAST_INST,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(N_TRAN_TYPE, 0 ) AS N_TRAN_TYPE ,
  COALESCE(ACC_ITM, '' ) AS ACC_ITM ,
  COALESCE(DR_ITM, '' ) AS DR_ITM ,
  COALESCE(CR_ITM, '' ) AS CR_ITM ,
  COALESCE(ENTRY_CODE, '' ) AS ENTRY_CODE ,
  COALESCE(ENTRY_NAME, '' ) AS ENTRY_NAME ,
  COALESCE(ENTRY_ATTR, '' ) AS ENTRY_ATTR ,
  COALESCE(TRAN_CODE, '' ) AS TRAN_CODE ,
  COALESCE(REG_DATE, '' ) AS REG_DATE ,
  COALESCE(LAST_TLR, '' ) AS LAST_TLR ,
  COALESCE(LAST_INST, '' ) AS LAST_INST 
 FROM  dw_tdata.ACC_000_T_INT_ENTRY_CFG_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  N_TRAN_TYPE ,
  ACC_ITM ,
  DR_ITM ,
  CR_ITM ,
  ENTRY_CODE ,
  ENTRY_NAME ,
  ENTRY_ATTR ,
  TRAN_CODE ,
  REG_DATE ,
  LAST_TLR ,
  LAST_INST 
 FROM dw_sdata.ACC_000_T_INT_ENTRY_CFG 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.N_TRAN_TYPE = T.N_TRAN_TYPE AND N.ACC_ITM = T.ACC_ITM
WHERE
(T.N_TRAN_TYPE IS NULL AND T.ACC_ITM IS NULL)
 OR N.DR_ITM<>T.DR_ITM
 OR N.CR_ITM<>T.CR_ITM
 OR N.ENTRY_CODE<>T.ENTRY_CODE
 OR N.ENTRY_NAME<>T.ENTRY_NAME
 OR N.ENTRY_ATTR<>T.ENTRY_ATTR
 OR N.TRAN_CODE<>T.TRAN_CODE
 OR N.REG_DATE<>T.REG_DATE
 OR N.LAST_TLR<>T.LAST_TLR
 OR N.LAST_INST<>T.LAST_INST
;

--Step3:
UPDATE dw_sdata.ACC_000_T_INT_ENTRY_CFG P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_369
WHERE P.End_Dt=DATE('2100-12-31')
AND P.N_TRAN_TYPE=T_369.N_TRAN_TYPE
AND P.ACC_ITM=T_369.ACC_ITM
;

--Step4:
INSERT  INTO dw_sdata.ACC_000_T_INT_ENTRY_CFG SELECT * FROM T_369;

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
