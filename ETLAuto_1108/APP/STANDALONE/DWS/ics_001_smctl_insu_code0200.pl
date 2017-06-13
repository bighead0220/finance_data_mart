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
DELETE FROM dw_sdata.ICS_001_SMCTL_INSU_CODE WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.ICS_001_SMCTL_INSU_CODE SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_352 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.ICS_001_SMCTL_INSU_CODE WHERE 1=0;

--Step2:
INSERT  INTO T_352 (
  INSU_CODE,
  INSU_TYPE,
  ONLINE_FLAG,
  POL_BAL_MODE,
  INSU_CH_NAME,
  INSU_CH_AB,
  INSU_EN_NAME,
  INSU_EN_AB,
  INSU_COMP_ADDR,
  INSU_PSCD,
  INSU_TEL,
  INSU_FAX,
  INSU_EMAIL,
  INSU_REG_BAL,
  INSU_LEVEL,
  INSU_SUB_NUM,
  INSU_COOP_FORM,
  INSU_LINKMAN,
  INSU_LINKMAN_TEL,
  start_dt,
  end_dt)
SELECT
  N.INSU_CODE,
  N.INSU_TYPE,
  N.ONLINE_FLAG,
  N.POL_BAL_MODE,
  N.INSU_CH_NAME,
  N.INSU_CH_AB,
  N.INSU_EN_NAME,
  N.INSU_EN_AB,
  N.INSU_COMP_ADDR,
  N.INSU_PSCD,
  N.INSU_TEL,
  N.INSU_FAX,
  N.INSU_EMAIL,
  N.INSU_REG_BAL,
  N.INSU_LEVEL,
  N.INSU_SUB_NUM,
  N.INSU_COOP_FORM,
  N.INSU_LINKMAN,
  N.INSU_LINKMAN_TEL,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(INSU_CODE, '' ) AS INSU_CODE ,
  COALESCE(INSU_TYPE, '' ) AS INSU_TYPE ,
  COALESCE(ONLINE_FLAG, '' ) AS ONLINE_FLAG ,
  COALESCE(POL_BAL_MODE, '' ) AS POL_BAL_MODE ,
  COALESCE(INSU_CH_NAME, '' ) AS INSU_CH_NAME ,
  COALESCE(INSU_CH_AB, '' ) AS INSU_CH_AB ,
  COALESCE(INSU_EN_NAME, '' ) AS INSU_EN_NAME ,
  COALESCE(INSU_EN_AB, '' ) AS INSU_EN_AB ,
  COALESCE(INSU_COMP_ADDR, '' ) AS INSU_COMP_ADDR ,
  COALESCE(INSU_PSCD, '' ) AS INSU_PSCD ,
  COALESCE(INSU_TEL, '' ) AS INSU_TEL ,
  COALESCE(INSU_FAX, '' ) AS INSU_FAX ,
  COALESCE(INSU_EMAIL, '' ) AS INSU_EMAIL ,
  COALESCE(INSU_REG_BAL, 0 ) AS INSU_REG_BAL ,
  COALESCE(INSU_LEVEL, '' ) AS INSU_LEVEL ,
  COALESCE(INSU_SUB_NUM, 0 ) AS INSU_SUB_NUM ,
  COALESCE(INSU_COOP_FORM, '' ) AS INSU_COOP_FORM ,
  COALESCE(INSU_LINKMAN, '' ) AS INSU_LINKMAN ,
  COALESCE(INSU_LINKMAN_TEL, '' ) AS INSU_LINKMAN_TEL 
 FROM  dw_tdata.ICS_001_SMCTL_INSU_CODE_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  INSU_CODE ,
  INSU_TYPE ,
  ONLINE_FLAG ,
  POL_BAL_MODE ,
  INSU_CH_NAME ,
  INSU_CH_AB ,
  INSU_EN_NAME ,
  INSU_EN_AB ,
  INSU_COMP_ADDR ,
  INSU_PSCD ,
  INSU_TEL ,
  INSU_FAX ,
  INSU_EMAIL ,
  INSU_REG_BAL ,
  INSU_LEVEL ,
  INSU_SUB_NUM ,
  INSU_COOP_FORM ,
  INSU_LINKMAN ,
  INSU_LINKMAN_TEL 
 FROM dw_sdata.ICS_001_SMCTL_INSU_CODE 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.INSU_CODE = T.INSU_CODE
WHERE
(T.INSU_CODE IS NULL)
 OR N.INSU_TYPE<>T.INSU_TYPE
 OR N.ONLINE_FLAG<>T.ONLINE_FLAG
 OR N.POL_BAL_MODE<>T.POL_BAL_MODE
 OR N.INSU_CH_NAME<>T.INSU_CH_NAME
 OR N.INSU_CH_AB<>T.INSU_CH_AB
 OR N.INSU_EN_NAME<>T.INSU_EN_NAME
 OR N.INSU_EN_AB<>T.INSU_EN_AB
 OR N.INSU_COMP_ADDR<>T.INSU_COMP_ADDR
 OR N.INSU_PSCD<>T.INSU_PSCD
 OR N.INSU_TEL<>T.INSU_TEL
 OR N.INSU_FAX<>T.INSU_FAX
 OR N.INSU_EMAIL<>T.INSU_EMAIL
 OR N.INSU_REG_BAL<>T.INSU_REG_BAL
 OR N.INSU_LEVEL<>T.INSU_LEVEL
 OR N.INSU_SUB_NUM<>T.INSU_SUB_NUM
 OR N.INSU_COOP_FORM<>T.INSU_COOP_FORM
 OR N.INSU_LINKMAN<>T.INSU_LINKMAN
 OR N.INSU_LINKMAN_TEL<>T.INSU_LINKMAN_TEL
;

--Step3:
UPDATE dw_sdata.ICS_001_SMCTL_INSU_CODE P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_352
WHERE P.End_Dt=DATE('2100-12-31')
AND P.INSU_CODE=T_352.INSU_CODE
;

--Step4:
INSERT  INTO dw_sdata.ICS_001_SMCTL_INSU_CODE SELECT * FROM T_352;

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
