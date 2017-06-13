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
DELETE FROM dw_sdata.ISS_001_IM_LCREJECTPAY WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.ISS_001_IM_LCREJECTPAY SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_242 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.ISS_001_IM_LCREJECTPAY WHERE 1=0;

--Step2:
INSERT  INTO T_242 (
  REJECTPAYNO,
  TXNSERIALNO,
  LCNO,
  ABNO,
  SENDBANKNO,
  SENDBANKSWIFTCODE,
  SENDBANKNAMEADDR,
  RECVBANKNO,
  RECVBANKSWIFTCODE,
  RECVBANKNAMEADDR,
  REJECTDATE,
  REJECTAMT,
  REJECTFORM,
  EXPRESSMAILMODE,
  MEMO,
  CHARGEUNDERTAKER,
  MAILCODE,
  start_dt,
  end_dt)
SELECT
  N.REJECTPAYNO,
  N.TXNSERIALNO,
  N.LCNO,
  N.ABNO,
  N.SENDBANKNO,
  N.SENDBANKSWIFTCODE,
  N.SENDBANKNAMEADDR,
  N.RECVBANKNO,
  N.RECVBANKSWIFTCODE,
  N.RECVBANKNAMEADDR,
  N.REJECTDATE,
  N.REJECTAMT,
  N.REJECTFORM,
  N.EXPRESSMAILMODE,
  N.MEMO,
  N.CHARGEUNDERTAKER,
  N.MAILCODE,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(REJECTPAYNO, '' ) AS REJECTPAYNO ,
  COALESCE(TXNSERIALNO, '' ) AS TXNSERIALNO ,
  COALESCE(LCNO, '' ) AS LCNO ,
  COALESCE(ABNO, '' ) AS ABNO ,
  COALESCE(SENDBANKNO, '' ) AS SENDBANKNO ,
  COALESCE(SENDBANKSWIFTCODE, '' ) AS SENDBANKSWIFTCODE ,
  COALESCE(SENDBANKNAMEADDR, '' ) AS SENDBANKNAMEADDR ,
  COALESCE(RECVBANKNO, '' ) AS RECVBANKNO ,
  COALESCE(RECVBANKSWIFTCODE, '' ) AS RECVBANKSWIFTCODE ,
  COALESCE(RECVBANKNAMEADDR, '' ) AS RECVBANKNAMEADDR ,
  COALESCE(REJECTDATE,DATE('4999-12-31') ) AS REJECTDATE ,
  COALESCE(REJECTAMT, 0 ) AS REJECTAMT ,
  COALESCE(REJECTFORM, '' ) AS REJECTFORM ,
  COALESCE(EXPRESSMAILMODE, '' ) AS EXPRESSMAILMODE ,
  COALESCE(MEMO, '' ) AS MEMO ,
  COALESCE(CHARGEUNDERTAKER, '' ) AS CHARGEUNDERTAKER ,
  COALESCE(MAILCODE, '' ) AS MAILCODE 
 FROM  dw_tdata.ISS_001_IM_LCREJECTPAY_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  REJECTPAYNO ,
  TXNSERIALNO ,
  LCNO ,
  ABNO ,
  SENDBANKNO ,
  SENDBANKSWIFTCODE ,
  SENDBANKNAMEADDR ,
  RECVBANKNO ,
  RECVBANKSWIFTCODE ,
  RECVBANKNAMEADDR ,
  REJECTDATE ,
  REJECTAMT ,
  REJECTFORM ,
  EXPRESSMAILMODE ,
  MEMO ,
  CHARGEUNDERTAKER ,
  MAILCODE 
 FROM dw_sdata.ISS_001_IM_LCREJECTPAY 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.REJECTPAYNO = T.REJECTPAYNO AND N.TXNSERIALNO = T.TXNSERIALNO
WHERE
(T.REJECTPAYNO IS NULL AND T.TXNSERIALNO IS NULL)
 OR N.LCNO<>T.LCNO
 OR N.ABNO<>T.ABNO
 OR N.SENDBANKNO<>T.SENDBANKNO
 OR N.SENDBANKSWIFTCODE<>T.SENDBANKSWIFTCODE
 OR N.SENDBANKNAMEADDR<>T.SENDBANKNAMEADDR
 OR N.RECVBANKNO<>T.RECVBANKNO
 OR N.RECVBANKSWIFTCODE<>T.RECVBANKSWIFTCODE
 OR N.RECVBANKNAMEADDR<>T.RECVBANKNAMEADDR
 OR N.REJECTDATE<>T.REJECTDATE
 OR N.REJECTAMT<>T.REJECTAMT
 OR N.REJECTFORM<>T.REJECTFORM
 OR N.EXPRESSMAILMODE<>T.EXPRESSMAILMODE
 OR N.MEMO<>T.MEMO
 OR N.CHARGEUNDERTAKER<>T.CHARGEUNDERTAKER
 OR N.MAILCODE<>T.MAILCODE
;

--Step3:
UPDATE dw_sdata.ISS_001_IM_LCREJECTPAY P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_242
WHERE P.End_Dt=DATE('2100-12-31')
AND P.REJECTPAYNO=T_242.REJECTPAYNO
AND P.TXNSERIALNO=T_242.TXNSERIALNO
;

--Step4:
INSERT  INTO dw_sdata.ISS_001_IM_LCREJECTPAY SELECT * FROM T_242;

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
