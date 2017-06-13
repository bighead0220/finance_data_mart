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
DELETE FROM dw_sdata.CCB_000_SCODE WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.CCB_000_SCODE SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_103 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.CCB_000_SCODE WHERE 1=0;

--Step2:
INSERT  INTO T_103 (
  BANK,
  CODE_TYPE,
  CODE_VALUE,
  BRIEF,
  VALUE_DEF,
  SHORT_DEF,
  VAL_A1,
  start_dt,
  end_dt)
SELECT
  N.BANK,
  N.CODE_TYPE,
  N.CODE_VALUE,
  N.BRIEF,
  N.VALUE_DEF,
  N.SHORT_DEF,
  N.VAL_A1,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(BANK, 0 ) AS BANK ,
  COALESCE(CODE_TYPE, '' ) AS CODE_TYPE ,
  COALESCE(CODE_VALUE, '' ) AS CODE_VALUE ,
  COALESCE(BRIEF, '' ) AS BRIEF ,
  COALESCE(VALUE_DEF, '' ) AS VALUE_DEF ,
  COALESCE(SHORT_DEF, '' ) AS SHORT_DEF ,
  COALESCE(VAL_A1, '' ) AS VAL_A1 
 FROM  dw_tdata.CCB_000_SCODE_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  BANK ,
  CODE_TYPE ,
  CODE_VALUE ,
  BRIEF ,
  VALUE_DEF ,
  SHORT_DEF ,
  VAL_A1 
 FROM dw_sdata.CCB_000_SCODE 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.BANK = T.BANK AND N.CODE_TYPE = T.CODE_TYPE AND N.CODE_VALUE = T.CODE_VALUE
WHERE
(T.BANK IS NULL AND T.CODE_TYPE IS NULL AND T.CODE_VALUE IS NULL)
 OR N.BRIEF<>T.BRIEF
 OR N.VALUE_DEF<>T.VALUE_DEF
 OR N.SHORT_DEF<>T.SHORT_DEF
 OR N.VAL_A1<>T.VAL_A1
;

--Step3:
UPDATE dw_sdata.CCB_000_SCODE P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_103
WHERE P.End_Dt=DATE('2100-12-31')
AND P.BANK=T_103.BANK
AND P.CODE_TYPE=T_103.CODE_TYPE
AND P.CODE_VALUE=T_103.CODE_VALUE
;

--Step4:
INSERT  INTO dw_sdata.CCB_000_SCODE SELECT * FROM T_103;

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
