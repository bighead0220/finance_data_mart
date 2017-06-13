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
DELETE FROM dw_sdata.CCB_000_APAD WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.CCB_000_APAD SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_85 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.CCB_000_APAD WHERE 1=0;

--Step2:
INSERT  INTO T_85 (
  APP_JDAY,
  APP_SEQ,
  BANK,
  ADDRESS1_A,
  ADDRESS2_A,
  ADDRESS3_A,
  ADDRESS4_A,
  ADDRESS5_A,
  ADDRTYPE_A,
  MAIL_TO,
  MAIL_TO2,
  OSEA_F_A,
  POSTCODE_A,
  STATE_C_A,
  INTR_CNBR,
  INTR_NAME,
  INTR_NBR,
  INTR_RECOM,
  INTR_RL,
  AB_SOURCE,
  AB_PHONE,
  APP_DAY,
  start_dt,
  end_dt)
SELECT
  N.APP_JDAY,
  N.APP_SEQ,
  N.BANK,
  N.ADDRESS1_A,
  N.ADDRESS2_A,
  N.ADDRESS3_A,
  N.ADDRESS4_A,
  N.ADDRESS5_A,
  N.ADDRTYPE_A,
  N.MAIL_TO,
  N.MAIL_TO2,
  N.OSEA_F_A,
  N.POSTCODE_A,
  N.STATE_C_A,
  N.INTR_CNBR,
  N.INTR_NAME,
  N.INTR_NBR,
  N.INTR_RECOM,
  N.INTR_RL,
  N.AB_SOURCE,
  N.AB_PHONE,
  N.APP_DAY,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(APP_JDAY, 0 ) AS APP_JDAY ,
  COALESCE(APP_SEQ, 0 ) AS APP_SEQ ,
  COALESCE(BANK, 0 ) AS BANK ,
  COALESCE(ADDRESS1_A, '' ) AS ADDRESS1_A ,
  COALESCE(ADDRESS2_A, '' ) AS ADDRESS2_A ,
  COALESCE(ADDRESS3_A, '' ) AS ADDRESS3_A ,
  COALESCE(ADDRESS4_A, '' ) AS ADDRESS4_A ,
  COALESCE(ADDRESS5_A, '' ) AS ADDRESS5_A ,
  COALESCE(ADDRTYPE_A, '' ) AS ADDRTYPE_A ,
  COALESCE(MAIL_TO, '' ) AS MAIL_TO ,
  COALESCE(MAIL_TO2, '' ) AS MAIL_TO2 ,
  COALESCE(OSEA_F_A, '' ) AS OSEA_F_A ,
  COALESCE(POSTCODE_A, '' ) AS POSTCODE_A ,
  COALESCE(STATE_C_A, '' ) AS STATE_C_A ,
  COALESCE(INTR_CNBR, '' ) AS INTR_CNBR ,
  COALESCE(INTR_NAME, '' ) AS INTR_NAME ,
  COALESCE(INTR_NBR, '' ) AS INTR_NBR ,
  COALESCE(INTR_RECOM, '' ) AS INTR_RECOM ,
  COALESCE(INTR_RL, '' ) AS INTR_RL ,
  COALESCE(AB_SOURCE, '' ) AS AB_SOURCE ,
  COALESCE(AB_PHONE, '' ) AS AB_PHONE ,
  COALESCE(APP_DAY, 0 ) AS APP_DAY 
 FROM  dw_tdata.CCB_000_APAD_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  APP_JDAY ,
  APP_SEQ ,
  BANK ,
  ADDRESS1_A ,
  ADDRESS2_A ,
  ADDRESS3_A ,
  ADDRESS4_A ,
  ADDRESS5_A ,
  ADDRTYPE_A ,
  MAIL_TO ,
  MAIL_TO2 ,
  OSEA_F_A ,
  POSTCODE_A ,
  STATE_C_A ,
  INTR_CNBR ,
  INTR_NAME ,
  INTR_NBR ,
  INTR_RECOM ,
  INTR_RL ,
  AB_SOURCE ,
  AB_PHONE ,
  APP_DAY 
 FROM dw_sdata.CCB_000_APAD 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.APP_SEQ = T.APP_SEQ AND N.BANK = T.BANK AND N.APP_DAY = T.APP_DAY
WHERE
(T.APP_SEQ IS NULL AND T.BANK IS NULL AND T.APP_DAY IS NULL)
 OR N.APP_JDAY<>T.APP_JDAY
 OR N.ADDRESS1_A<>T.ADDRESS1_A
 OR N.ADDRESS2_A<>T.ADDRESS2_A
 OR N.ADDRESS3_A<>T.ADDRESS3_A
 OR N.ADDRESS4_A<>T.ADDRESS4_A
 OR N.ADDRESS5_A<>T.ADDRESS5_A
 OR N.ADDRTYPE_A<>T.ADDRTYPE_A
 OR N.MAIL_TO<>T.MAIL_TO
 OR N.MAIL_TO2<>T.MAIL_TO2
 OR N.OSEA_F_A<>T.OSEA_F_A
 OR N.POSTCODE_A<>T.POSTCODE_A
 OR N.STATE_C_A<>T.STATE_C_A
 OR N.INTR_CNBR<>T.INTR_CNBR
 OR N.INTR_NAME<>T.INTR_NAME
 OR N.INTR_NBR<>T.INTR_NBR
 OR N.INTR_RECOM<>T.INTR_RECOM
 OR N.INTR_RL<>T.INTR_RL
 OR N.AB_SOURCE<>T.AB_SOURCE
 OR N.AB_PHONE<>T.AB_PHONE
;

--Step3:
UPDATE dw_sdata.CCB_000_APAD P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_85
WHERE P.End_Dt=DATE('2100-12-31')
AND P.APP_SEQ=T_85.APP_SEQ
AND P.BANK=T_85.BANK
AND P.APP_DAY=T_85.APP_DAY
;

--Step4:
INSERT  INTO dw_sdata.CCB_000_APAD SELECT * FROM T_85;

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
