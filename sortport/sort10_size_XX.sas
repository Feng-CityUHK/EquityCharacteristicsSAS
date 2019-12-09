
/* ********************************************************************************* */
/* MACRO 1            */
/* Add port_label to firm_ratio            */
/* Don't mind the '25', it also works for 6 sorts            */

%MACRO FINRATIO_firm_add_ind_25 (begdate=, enddate=, in=, portfolio_label=, out=);
data datainput;
set &in;
where "&begdate"d<=public_date<="&enddate"d;
run;

/* proc export data=datainput outfile="datainput.csv" */
/* dbms=csv replace; */

/* 25 portfolios */

/* merge new industry and financial ratios table */
/* by permno and date */
proc sql;
create table dataoutput_25
  as select a.*, b.myind25 as FFI25_desc
  from datainput a left join &portfolio_label b
  on a.permno=b.permno and a.public_date=b.mdate;
quit;

/* proc export data=dataoutput_25 outfile="dataoutput_25.csv" */
/* dbms=csv replace; */

proc sql;
    create table &out as
        select *
        from dataoutput_25
        where not missing(FFI25_desc); /* hexin */
quit;

%mend FINRATIO_firm_add_ind_25;


/* ********************************************************************************* */
/* MACRO's for bm inv op mom1m mom12m mom60m             */

/* ********************************************************************************* */
/* portfolio sorts */

%MACRO DIVIDE_PORTFOLIO_10_ACC (begdate=, enddate=, table_out=);

/************************ Part 1: Compustat ****************************/
/* Compustat XpressFeed Variables:                                     */
/* AT      = Total Assets                                              */
/* PSTKL   = Preferred Stock Liquidating Value                         */
/* TXDITC  = Deferred Taxes and Investment Tax Credit                  */
/* PSTKRV  = Preferred Stock Redemption Value                          */
/* seq     =                                   */
/* PSTK    = Preferred/Preference Stock (Capital) - Total              */

/* In calculating Book Equity, incorporate Preferred Stock (PS) values */
/*  use the redemption value of PS, or the liquidation value           */
/*    or the par value (in that order) (FF,JFE, 1993, p. 8)            */
/* USe Balance Sheet Deferred Taxes TXDITC if available                */
/* Flag for number of years in Compustat (<2 likely backfilled data)   */

%let vars = AT PSTKL TXDITC PSTKRV seq PSTK ACT LCT NP IB OANCF;
data comp;
  set comp.funda
  (keep= gvkey datadate &vars indfmt datafmt popsrc consol);
  by gvkey datadate;
  where indfmt='INDL' and datafmt='STD' and popsrc='D' and consol='C'
  and datadate >='01Jan1900'd;
 /* Two years of accounting data before 1962 */
  PS = coalesce(PSTKRV,PSTKL,PSTK,0);
  if missing(TXDITC) then TXDITC = 0 ;
  BE = seq + TXDITC - PS ;
  if BE<=0 then BE=.;
  year = year(datadate);
  label BE='Book Value of Equity FYear t-1' ;
  ACC=((ACT-LCT+NP)-(lag(ACT)-lag(LCT)+lag(NP)))/(10*BE);
  if missing(NP) then ACC=((ACT-LCT)-(lag(ACT)-lag(LCT)))/(10*BE);
  if missing(ACT) or missing(LCT) then ACC=(IB-OANCF)/(10*BE);
  drop indfmt datafmt popsrc consol ps &vars;
  retain count;
  if first.gvkey then count=1;
  else count = count+1;
run;

/************************ Part 2: CRSP **********************************/
/* Create a CRSP Subsample with Monthly Stock and Event Variables       */
/* This procedure creates a SAS dataset named "CRSP_M"                  */
/* Restrictions will be applied later                                   */
/* Select variables from the CRSP monthly stock and event datasets      */
%let msevars=ticker ncusip shrcd exchcd;
%let msfvars =  prc ret retx shrout cfacpr cfacshr;

%include '/wrds/crsp/samples/crspmerge.sas';

%crspmerge(s=m,start=01jan1900,end=31dec2018,
sfvars=&msfvars,sevars=&msevars,filters=exchcd in (1,2,3));

/* CRSP_M is sorted by date and permno and has historical returns     */
/* as well as historical share codes and exchange codes               */
/* Add CRSP delisting returns */
proc sql; create table crspm2
 as select a.*, b.dlret,
  sum(1,ret)*sum(1,dlret)-1 as retadj "Return adjusted for delisting",
  abs(a.prc)*a.shrout as MEq 'Market Value of Equity'
 from Crsp_m a left join crsp.msedelist(where=(missing(dlret)=0)) b
 on a.permno=b.permno and
    intnx('month',a.date,0,'E')=intnx('month',b.DLSTDT,0,'E')
 order by a.date, a.permco, MEq;
quit;

/* There are cases when the same firm (permco) has two or more         */
/* securities (permno) at same date. For the purpose of ME for         */
/* the firm, we aggregated all ME for a given permco, date. This       */
/* aggregated ME will be assigned to the Permno with the largest ME    */
data crspm2a (drop = Meq); set crspm2;
  by date permco Meq;
  retain ME;
  if first.permco and last.permco then do;
    ME=meq;
  output; /* most common case where a firm has a unique permno*/
  end;
  else do ;
    if  first.permco then ME=meq;
    else ME=sum(meq,ME);
    if last.permco then output;
  end;
run;

/* There should be no duplicates*/
proc sort data=crspm2a nodupkey; by permno date;run;

/* The next step does 2 things:                                        */
/* - Create weights for later calculation of VW returns.               */
/*   Each firm's monthly return RET t willl be weighted by             */
/*   ME(t-1) = ME(t-2) * (1 + RETX (t-1))                              */
/*     where RETX is the without-dividend return.                      */
/* - Create a File with December t-1 Market Equity (ME)                */
data crspm3 (keep=permno date retadj weight_port ME exchcd shrcd cumretx)
decme (keep = permno date ME rename=(me=DEC_ME) )  ;
     set crspm2a;
 by permno date;
 retain weight_port cumretx me_base;
 Lpermno=lag(permno);
 LME=lag(me);
     if first.permno then do;
     LME=me/(1+retx); cumretx=sum(1,retx); me_base=LME;weight_port=.;end;
     else do;
     if month(date)=7 then do;
        weight_port= LME;
        me_base=LME; /* lag ME also at the end of June */
        cumretx=sum(1,retx);
     end;
     else do;
        if LME>0 then weight_port=cumretx*me_base;
        else weight_port=.;
        cumretx=cumretx*sum(1,retx);
     end; end;
output crspm3;
if month(date)=12 and ME>0 then output decme;
run;

/* Create a file with data for each June with ME from previous December */
proc sql;
  create table crspjune as
  select a.*, b.DEC_ME
  from crspm3 (where=(month(date)=6)) as a, decme as b
  where a.permno=b.permno and
  intck('month',b.date,a.date)=6;
quit;

/***************   Part 3: Merging CRSP and Compustat ***********/
/* Add Permno to Compustat sample */
proc sql;
  create table ccm1 as
  select a.*, b.lpermno as permno, b.linkprim
  from comp as a, crsp.ccmxpf_linktable as b
  where a.gvkey=b.gvkey
  and substr(b.linktype,1,1)='L' and linkprim in ('P','C')
  and (intnx('month',intnx('year',a.datadate,0,'E'),6,'E') >= b.linkdt)
  and (b.linkenddt >= intnx('month',intnx('year',a.datadate,0,'E'),6,'E')
  or missing(b.linkenddt))
  order by a.datadate, permno, b.linkprim desc;
quit;

/*  Cleaning Compustat Data for no relevant duplicates                      */
/*  Eliminating overlapping matching : few cases where different gvkeys     */
/*  for same permno-date --- some of them are not 'primary' matches in CCM  */
/*  Use linkprim='P' for selecting just one gvkey-permno-date combination   */
data ccm1a; set ccm1;
  by datadate permno descending linkprim;
  if first.permno;
run;

/* Sanity Check -- No Duplicates */
proc sort data=ccm1a nodupkey; by permno year datadate; run;

/* 2. However, there other type of duplicates within the year                */
/* Some companiess change fiscal year end in the middle of the calendar year */
/* In these cases, there are more than one annual record for accounting data */
/* We will be selecting the last annual record in a given calendar year      */
data ccm2a ; set ccm1a;
  by permno year datadate;
  if last.year;
run;

/* Sanity Check -- No Duplicates */
proc sort data=ccm2a nodupkey; by permno datadate; run;

/* Finalize Compustat Sample                              */
/* Merge CRSP with Compustat data, at June of every year  */
/* Match fiscal year ending calendar year t-1 with June t */
proc sql; create table ccm2_june as
  select a.*, b.BE, (1000*b.BE)/a.DEC_ME as BEME, b.acc, b.count,
  b.datadate,
  intck('month',b.datadate, a.date) as dist
  from crspjune a, ccm2a b
  where a.permno=b.permno and intnx('month',a.date,0,'E')=
  intnx('month',intnx('year',b.datadate,0,'E'),6,'E')
  order by a.date;
quit;

/************************ Part 4: Size and Book to Market Portfolios ***/
/* Forming Portolio by ME and BEME as of each June t                   */
/* Calculate NYSE Breakpoints for Market Equity (ME) and               */
/* Book-to-Market (BEME)                                               */
proc univariate data=ccm2_june noprint;
  where exchcd=1 and shrcd in (10,11) and me>0 and count>=2;
  var ACC; * ME is Market Equity at the end of June;
  by date; /*at june;*/
  output out=nyse_breaks pctlpre=ACC pctlpts= 10 to 90 by 10; /* HX ADD */
run;

proc export data=nyse_breaks outfile="nyse_breaks.csv"
dbms=csv replace;run;

/* Use Breakpoints to classify stock only at end of all June's */
/* HX MODI */
proc sql;
  create table ccm3_june as
  select a.*,
  b.acc10,b.acc20,b.acc30, b.acc40,b.acc50, b.acc60,b.acc70, b.acc80,b.acc90
  from ccm2_june as a, nyse_breaks as b
  where a.date=b.date;
quit;

/* Create portfolios as of June                       */
/* SIZE Portfolios          : S[mall] or B[ig]        */
/* Book-to-market Portfolios: L[ow], M[edium], H[igh] */
/* HX MODI  10 times 10*/
data june ; set ccm3_june;
 If me>0 and count>=2 then do;
 positivebeme=1;
 if acc <= acc10 then           accport = 'ACC0' ;
 else if acc10 < acc <= acc20 then accport = 'ACC1' ;
 else if acc20 < acc <= acc30 then accport = 'ACC2' ;
 else if acc30 < acc <= acc40 then accport = 'ACC3' ;
 else if acc40 < acc <= acc50 then accport = 'ACC4' ;
 else if acc50 < acc <= acc60 then accport = 'ACC5' ;
 else if acc60 < acc <= acc70 then accport = 'ACC6' ;
 else if acc70 < acc <= acc80 then accport = 'ACC7' ;
 else if acc80 < acc <= acc90 then accport = 'ACC8' ;
 else if acc  > acc90 then          accport = 'ACC9' ;
 else accport='';
end;
else positivebeme=0;
if cmiss(accport)=0 then nonmissport=1; else nonmissport=0;
keep permno date accport positivebeme exchcd shrcd nonmissport;
run;

/* HX ADD */
data myjune; set june;
myind25 = catx('_', accport);
run;

data myjune; set myjune;
sortdate = intnx('month',date,0,'E');
nextmon = intnx('month',date,1);
nextmonend = intnx('month',nextmon,0,'E');
run;

/* proc export data=myjune outfile="myjune_25.csv" */
/* dbms=csv replace; */

/* populate annual data to monthly */
%populate(inset=myjune, outset=mymonth, datevar=nextmonend, idvar=permno, forward_max=12);

/* Identifying each month the securities of              */
/* Buy and hold June portfolios from July t to June t+1  */
proc sql; create table ccm4 as
 select a.*, b.accport, b.date as portdate format MMDDYYD10.,
        b.positivebeme , b.nonmissport
 from crspm3 as a, june as b
 where a.permno=b.permno and  1 <= intck('month',b.date,a.date) <= 12
 order by date, accport;
quit;

/*************** Part 5: Calculating Fama-French Factors  **************/
/* Calculate monthly time series of weighted average portfolio returns */
proc means data=ccm4 noprint;
 where weight_port>0 and exchcd in (1,2,3)
      and shrcd in (10,11) and nonmissport=1;
 by date accport;
 var retadj;
 weight weight_port;
 output out=vwret (drop= _type_ _freq_ ) mean=vwret n=n_firms;
run;

/*proc export data=vwret outfile="vwret_10_acc.csv"*/
/*dbms=csv replace;*/

/* Monthly Factor Returns: SMB and HML */
proc transpose data=vwret(keep=date accport vwret)
 out=vwret2 (drop=_name_ _label_);
 by date ;
 ID accport;
 Var vwret;
run;

proc export data=vwret2 outfile="vwret2_10_acc.csv"
dbms=csv replace;

/* Number of Firms */
proc transpose data=vwret(keep=date accport n_firms)
               out=vwret3 (drop=_name_ _label_) prefix=n_;
by date ;
ID accport;
Var n_firms;
run;

proc export data=vwret3 outfile="vwret3_10_acc.csv"
dbms=csv replace;



proc sort data=mymonth nodupkey out=&table_out;
where "&begdate"d<=mdate<="&enddate"d;
by mdate permno;run;

%mend DIVIDE_PORTFOLIO_10_ACC;



%MACRO DIVIDE_PORTFOLIO_10_BETA (begdate=, enddate=, table_out=);

/************************ Part 1: Compustat ****************************/
/* Compustat XpressFeed Variables:                                     */
/* AT      = Total Assets                                              */
/* PSTKL   = Preferred Stock Liquidating Value                         */
/* TXDITC  = Deferred Taxes and Investment Tax Credit                  */
/* PSTKRV  = Preferred Stock Redemption Value                          */
/* seq     = Common/Ordinary Equity - Total                            */
/* PSTK    = Preferred/Preference Stock (Capital) - Total              */

/* In calculating Book Equity, incorporate Preferred Stock (PS) values */
/*  use the redemption value of PS, or the liquidation value           */
/*    or the par value (in that order) (FF,JFE, 1993, p. 8)            */
/* USe Balance Sheet Deferred Taxes TXDITC if available                */
/* Flag for number of years in Compustat (<2 likely backfilled data)   */

%let vars = AT PSTKL TXDITC PSTKRV seq PSTK ;
data comp;
  set comp.funda
  (keep= gvkey datadate &vars indfmt datafmt popsrc consol);
  by gvkey datadate;
  where indfmt='INDL' and datafmt='STD' and popsrc='D' and consol='C'
  and datadate >='01Jan1900'd;
 /* Two years of accounting data before 1962 */
  PS = coalesce(PSTKRV,PSTKL,PSTK,0);
  if missing(TXDITC) then TXDITC = 0 ;
  BE = seq + TXDITC - PS ;
  if BE<0 then BE=.;
  year = year(datadate);
  label BE='Book Value of Equity FYear t-1' ;
  drop indfmt datafmt popsrc consol ps &vars;
  retain count;
  if first.gvkey then count=1;
  else count = count+1;
run;

/************************ Part 2: CRSP **********************************/
/* Create a CRSP Subsample with Monthly Stock and Event Variables       */
/* This procedure creates a SAS dataset named "CRSP_M"                  */
/* Restrictions will be applied later                                   */
/* Select variables from the CRSP monthly stock and event datasets      */
%let msevars=ticker ncusip shrcd exchcd;
%let msfvars =  prc ret retx shrout cfacpr cfacshr;

%include '/wrds/crsp/samples/crspmerge.sas';

%crspmerge(s=m,start=01jan1900,end=31dec2018,
sfvars=&msfvars,sevars=&msevars,filters=exchcd in (1,2,3));

/* CRSP_M is sorted by date and permno and has historical returns     */
/* as well as historical share codes and exchange codes               */
/* Add CRSP delisting returns */
proc sql; create table crspm2
 as select a.*, b.dlret,
  sum(1,ret)*sum(1,dlret)-1 as retadj "Return adjusted for delisting",
  abs(a.prc)*a.shrout as MEq 'Market Value of Equity'
 from Crsp_m a left join crsp.msedelist(where=(missing(dlret)=0)) b
 on a.permno=b.permno and
    intnx('month',a.date,0,'E')=intnx('month',b.DLSTDT,0,'E')
 order by a.date, a.permco, MEq;
quit;

/* There are cases when the same firm (permco) has two or more         */
/* securities (permno) at same date. For the purpose of ME for         */
/* the firm, we aggregated all ME for a given permco, date. This       */
/* aggregated ME will be assigned to the Permno with the largest ME    */
data crspm2a (drop = Meq); set crspm2;
  by date permco Meq;
  retain ME;
  if first.permco and last.permco then do;
    ME=meq;
  output; /* most common case where a firm has a unique permno*/
  end;
  else do ;
    if  first.permco then ME=meq;
    else ME=sum(meq,ME);
    if last.permco then output;
  end;
run;

proc sql;
	create table dcrsp
	as select permno,date,ret
	from crsp.msf
	quit;

proc sql;
    create table ddcrsp as
    select a.*, (a.ret-b.rf) as ERET, b.mktrf from
    dcrsp a left join ff.factors_daily b
    on
    a.date = b.date;
    quit;

proc sort data=ddcrsp; by permno date; run;


/*proc export data=work.ddcrsp outfile="ddcrsp.csv" */
/*dbms=csv replace;run;*/


%macro RRLOOP(year1=1958, year2=2018, in_ds=ddcrsp, out_ds = work.out_ds);

%local date1 date2 date1f date2f yy mm;

/*Extra step to be sure to start with clean, null datasets for appending*/

proc datasets nolist lib=work;
  delete all_ds oreg_ds1;
run;

%do yy = &year1 %to &year2;
  %do mm = 1 %to 12;

%let date2= %sysfunc(mdy(&mm,1,&yy));
%let date2 = %sysfunc (intnx(month,&date2,0,end));
%let date1 = %sysfunc (intnx(month,&date2,-60,end));

/*An extra step to be sure the loop starts with a clean (empty) dataset for combining results*/
proc datasets nolist lib=work;
  delete oreg_ds1;
run;


/*Regression model estimation -- creates output set with residual*/
proc reg noprint data=&in_ds outest=oreg_ds1 edf;
  where date between &date1 and &date2;
  model ERET = mktrf;
  by permno;
run;

/*Store DATE1 and DATE2 as dataset variables;*/
data oreg_ds1;
  set oreg_ds1;
  date1=&date1;
  date2=&date2;
  date=&date2;
  rename mktrf=beta;
  nobs= _p_ + _edf_;
  if nobs>=24;
  format date1 date2 yymmdd10.;
run;

/*Append loop results to dataset with all date1-date2 observations*/
proc datasets lib=work;
  append base=all_ds data=oreg_ds1;
run;

 %end;   /*MM month loop*/

 %end;  /*YY year loop*/

/*Save results in final dataset*/
data &out_ds;
  set all_ds;
run;

%mend RRLOOP;

%RRLOOP (year1= 1963, year2= 2018,  in_ds=ddcrsp, out_ds=work.out_ds);

/*proc export data=work.out_ds outfile="out_mergedate.csv" */
/*dbms=csv replace;run;*/

proc sort data=work.out_ds nodupkey; by permno date; run;

proc sql;
create table crspm2aa as
select a.*, b.* from
crspm2a a left join out_ds b
on
a.permno = b.permno
and year(a.date) = year(b.date)
and month(a.date) = month(b.date)
and day(a.date)>= day(b.date)-3
and day(a.date)<= day(b.date)
order by a.permno, a.date;
quit;



/* There should be no duplicates*/
proc sort data=crspm2aa nodupkey; by permno date;run;



/* proc export data=crspm2a outfile="crspm2a.csv" */
/* dbms=csv replace;run; */

/* The next step does 2 things:                                        */
/* - Create weights for later calculation of VW returns.               */
/*   Each firm's monthly return RET t willl be weighted by             */
/*   ME(t-1) = ME(t-2) * (1 + RETX (t-1))                              */
/*     where RETX is the without-dividend return.                      */
/* - Create a File with December t-1 Market Equity (ME)                */
data crspm3 (keep=permno date retadj weight_port ME beta exchcd sh rcd cumretx)
decme (keep = permno date ME rename=(me=DEC_ME) )  ;
     set crspm2aa;
 by permno date;
 retain weight_port cumretx me_base;
 Lpermno=lag(permno);
 LME=lag(me);
     if first.permno then do;
     LME=me/(1+retx); cumretx=sum(1,retx); me_base=LME;weight_port=.;end;
     else do;
     if month(date)=7 then do;
        weight_port= LME;
        me_base=LME; /* lag ME also at the end of June */
        cumretx=sum(1,retx);
     end;
     else do;
        if LME>0 then weight_port=cumretx*me_base;
        else weight_port=.;
        cumretx=cumretx*sum(1,retx);
     end; end;
output crspm3;
if month(date)=12 and ME>0 then output decme;
run;

/* Create a file with data for each June with ME from previous December */
proc sql;
  create table crspjune as
  select a.*, b.DEC_ME
  from crspm3 (where=(month(date)=6)) as a, decme as b
  where a.permno=b.permno and
  intck('month',b.date,a.date)=6;
quit;

/***************   Part 3: Merging CRSP and Compustat ***********/
/* Add Permno to Compustat sample */
proc sql;
  create table ccm1 as
  select a.*, b.lpermno as permno, b.linkprim
  from comp as a, crsp.ccmxpf_linktable as b
  where a.gvkey=b.gvkey
  and substr(b.linktype,1,1)='L' and linkprim in ('P','C')
  and (intnx('month',intnx('year',a.datadate,0,'E'),6,'E') >= b.linkdt)
  and (b.linkenddt >= intnx('month',intnx('year',a.datadate,0,'E'),6,'E')
  or missing(b.linkenddt))
  order by a.datadate, permno, b.linkprim desc;
quit;

/*  Cleaning Compustat Data for no relevant duplicates                      */
/*  Eliminating overlapping matching : few cases where different gvkeys     */
/*  for same permno-date --- some of them are not 'primary' matches in CCM  */
/*  Use linkprim='P' for selecting just one gvkey-permno-date combination   */
data ccm1a; set ccm1;
  by datadate permno descending linkprim;
  if first.permno;
run;

/* Sanity Check -- No Duplicates */
proc sort data=ccm1a nodupkey; by permno year datadate; run;

/* 2. However, there other type of duplicates within the year                */
/* Some companiess change fiscal year end in the middle of the calendar year */
/* In these cases, there are more than one annual record for accounting data */
/* We will be selecting the last annual record in a given calendar year      */
data ccm2a ; set ccm1a;
  by permno year datadate;
  if last.year;
run;

/* Sanity Check -- No Duplicates */
proc sort data=ccm2a nodupkey; by permno datadate; run;

/* Finalize Compustat Sample                              */
/* Merge CRSP with Compustat data, at June of every year  */
/* Match fiscal year ending calendar year t-1 with June t */
proc sql; create table ccm2_june as
  select a.*, b.BE, (1000*b.BE)/a.DEC_ME as BEME, b.count,
  b.datadate,
  intck('month',b.datadate, a.date) as dist
  from crspjune a, ccm2a b
  where a.permno=b.permno and intnx('month',a.date,0,'E')=
  intnx('month',intnx('year',b.datadate,0,'E'),6,'E')
  order by a.date;
quit;

/* proc export data=ccm2_june outfile="ccm2_june.csv" */
/* dbms=csv replace;run; */

/* ********************************************************************************* */
/* ******************* CREATE MONTHLY BIG CCM_CRSP ********************************* */
/*  ccm2_june comes from original script                                             */
/*  merge crspm2a with ccm2_june                                                     */
/*  BE information of June paste to crspm2a for July year t to June year t+1         */
/*                                                                                   */
/*                                                                                   */
/*                                                                                   */
proc sql;
create table ccm_month as
select a.*, b.count, b.beme, b.date as june_date, b.datadate as annual_info_date from
crspm2aa a left join ccm2_june b
on
a.permno = b.permno
and 1<=intck('month', intnx('month',b.date,0,'E'),  intnx('month',a.date,0,'E') )<=12
order by a.permno, a.date;
quit;

/* proc export data=ccm_month outfile="ccm_month.csv" */
/* dbms=csv replace;run; */

/************************ Part 4: Size and Book to Market Portfolios ***/
/* Forming Portolio by ME and BEME as of each June t                   */
/* Calculate NYSE Breakpoints for Market Equity (ME) and               */
/* Book-to-Market (BEME)                                               */
proc sort data=ccm_month; by date permno; run;

proc univariate data=ccm_month noprint;
  where exchcd=1 and shrcd in (10,11);
  var beta;
  by date;
  output out=nyse_breaks pctlpre=beta pctlpts=10 to 90 by 10; /* HX ADD */
run;

/* proc export data=nyse_breaks outfile="nyse_breaks.csv" */
/* dbms=csv replace;run; */

/* Use Breakpoints to classify stock only at end of all June's */
/* HX MODI */
proc sql;
  create table ccm3 as
  select a.*,
  b.beta10, b.beta20,b.beta30,b.beta40,b.beta50,b.beta60,b.beta70,b.beta80,b.beta90
  from ccm_month as a, nyse_breaks as b
  where a.date=b.date;
quit;

/* Create portfolios as of June                       */
/* SIZE Portfolios          : S[mall] or B[ig]        */
/* Book-to-market Portfolios: L[ow], M[edium], H[igh] */
/* HX MODI  10 times 10*/
data all_month; set ccm3;
 If me>0 then do;
 positiveme = 1;

 if  beta <= beta10 then              betaport = 'BETA0' ;
 else if beta10 < beta <= beta20 then betaport = 'BETA1' ;
 else if beta20 < beta <= beta30 then betaport = 'BETA2' ;
 else if beta30 < beta <= beta40 then betaport = 'BETA3' ;
 else if beta40 < beta <= beta50 then betaport = 'BETA4' ;
 else if beta50 < beta <= beta60 then betaport = 'BETA5' ;
 else if beta60 < beta <= beta70 then betaport = 'BETA6' ;
 else if beta70 < beta <= beta80 then betaport = 'BETA7' ;
 else if beta80 < beta <= beta90 then betaport = 'BETA8' ;
 else if beta  > beta90 then          betaport = 'BETA9' ;
 else betaport='';
 end;
 else positiveme = 0;
if cmiss(betaport)=0 then nonmissport=1; else nonmissport=0;
keep permno date retadj betaport exchcd shrcd nonmissport me;
run;

/* HX ADD */
data all_month; set all_month;
myind = catx('_', betaport);
run;

/* ********************************************************************************* */
/* lag port labels one month */
proc sort data=all_month; by permno date; run;

data ccm4; set all_month;
by permno date;
mdate = intnx('month', date,0,'E');
format mdate worddatx19.;
mport = lag(betaport);
weight_port = lag(me);
myind25 = lag(myind);
if first.permno then mport=.;
if first.permno then weight_port=.;
if first.permno then myind25=.;
run;

/* ********************************************************************************* */
/* mymonth */

data mymonth; set ccm4;
keep mdate permno myind25;
run;

/* proc export data=mymonth outfile="mymonth.csv" */
/* dbms=csv replace; */

/*************** Part 5: Calculating Fama-French Factors  **************/
/* Calculate monthly time series of weighted average portfolio returns */
proc sort data=ccm4; by date mport; run;

proc means data=ccm4 noprint;
 where weight_port>0 and exchcd in (1,2,3)
      and shrcd in (10,11) and nonmissport=1;
 by date mport;
 var retadj;
 weight weight_port;
 output out=vwret (drop= _type_ _freq_ ) mean=vwret n=n_firms;
run;

/*proc export data=vwret outfile="vwret_25_beta.csv"*/
/*dbms=csv replace;*/

/* Monthly Factor Returns: SMB and HML */
proc transpose data=vwret(keep=date mport vwret)
 out=vwret2 (drop=_name_ _label_);
 by date ;
 ID mport;
 Var vwret;
run;

proc export data=vwret2 outfile="vwret2_10_beta.csv"
dbms=csv replace;

/* Number of Firms */
proc transpose data=vwret(keep=date mport n_firms)
               out=vwret3 (drop=_name_ _label_) prefix=n_;
by date ;
ID mport;
Var n_firms;
run;

proc export data=vwret3 outfile="vwret3_10_beta.csv"
dbms=csv replace;


proc sort data=mymonth nodupkey out=&table_out;
where "&begdate"d<=mdate<="&enddate"d;
by mdate permno;run;

%mend DIVIDE_PORTFOLIO_10_BETA;


%MACRO DIVIDE_PORTFOLIO_10_BM (begdate=, enddate=, table_out=);

/************************ Part 1: Compustat ****************************/
/* Compustat XpressFeed Variables:                                     */
/* AT      = Total Assets                                              */
/* PSTKL   = Preferred Stock Liquidating Value                         */
/* TXDITC  = Deferred Taxes and Investment Tax Credit                  */
/* PSTKRV  = Preferred Stock Redemption Value                          */
/* seq     =     hgghggiuhhiuhkjhjhkj                            */
/* PSTK    = Preferred/Preference Stock (Capital) - Total              */

/* In calculating Book Equity, incorporate Preferred Stock (PS) values */
/*  use the redemption value of PS, or the liquidation value           */
/*    or the par value (in that order) (FF,JFE, 1993, p. 8)            */
/* USe Balance Sheet Deferred Taxes TXDITC if available                */
/* Flag for number of years in Compustat (<2 likely backfilled data)   */


%let vars = AT PSTKL TXDITC PSTKRV seq PSTK ;
data comp;
  set comp.funda
  (keep= gvkey datadate &vars indfmt datafmt popsrc consol);
  by gvkey datadate;
  where indfmt='INDL' and datafmt='STD' and popsrc='D' and consol='C'
  and datadate >='01Jan1900'd;
 /* Two years of accounting data before 1962 */
  PS = coalesce(PSTKRV,PSTKL,PSTK,0);
  if missing(TXDITC) then TXDITC = 0 ;
  BE = seq + TXDITC - PS ;
  if BE<0 then BE=.;
  year = year(datadate);
  label BE='Book Value of Equity FYear t-1' ;
  drop indfmt datafmt popsrc consol ps &vars;
  retain count;
  if first.gvkey then count=1;
  else count = count+1;
run;

/************************ Part 2: CRSP **********************************/
/* Create a CRSP Subsample with Monthly Stock and Event Variables       */
/* This procedure creates a SAS dataset named "CRSP_M"                  */
/* Restrictions will be applied later                                   */
/* Select variables from the CRSP monthly stock and event datasets      */
%let msevars=ticker ncusip shrcd exchcd;
%let msfvars =  prc ret retx shrout cfacpr cfacshr;

%include '/wrds/crsp/samples/crspmerge.sas';

%crspmerge(s=m,start=01jan1900,end=31dec2018,
sfvars=&msfvars,sevars=&msevars,filters=exchcd in (1,2,3));

/* CRSP_M is sorted by date and permno and has historical returns     */
/* as well as historical share codes and exchange codes               */
/* Add CRSP delisting returns */
proc sql; create table crspm2
 as select a.*, b.dlret,
  sum(1,ret)*sum(1,dlret)-1 as retadj "Return adjusted for delisting",
  abs(a.prc)*a.shrout as MEq 'Market Value of Equity'
 from Crsp_m a left join crsp.msedelist(where=(missing(dlret)=0)) b
 on a.permno=b.permno and
    intnx('month',a.date,0,'E')=intnx('month',b.DLSTDT,0,'E')
 order by a.date, a.permco, MEq;
quit;

/* There are cases when the same firm (permco) has two or more         */
/* securities (permno) at same date. For the purpose of ME for         */
/* the firm, we aggregated all ME for a given permco, date. This       */
/* aggregated ME will be assigned to the Permno with the largest ME    */
data crspm2a (drop = Meq); set crspm2;
  by date permco Meq;
  retain ME;
  if first.permco and last.permco then do;
    ME=meq;
  output; /* most common case where a firm has a unique permno*/
  end;
  else do ;
    if  first.permco then ME=meq;
    else ME=sum(meq,ME);
    if last.permco then output;
  end;
run;

/* There should be no duplicates*/
proc sort data=crspm2a nodupkey; by permno date;run;

/* The next step does 2 things:                                        */
/* - Create weights for later calculation of VW returns.               */
/*   Each firm's monthly return RET t willl be weighted by             */
/*   ME(t-1) = ME(t-2) * (1 + RETX (t-1))                              */
/*     where RETX is the without-dividend return.                      */
/* - Create a File with December t-1 Market Equity (ME)                */
data crspm3 (keep=permno date retadj weight_port ME exchcd shrcd cumretx)
decme (keep = permno date ME rename=(me=DEC_ME) )  ;
     set crspm2a;
 by permno date;
 retain weight_port cumretx me_base;
 Lpermno=lag(permno);
 LME=lag(me);
     if first.permno then do;
     LME=me/(1+retx); cumretx=sum(1,retx); me_base=LME;weight_port=.;end;
     else do;
     if month(date)=7 then do;
        weight_port= LME;
        me_base=LME; /* lag ME also at the end of June */
        cumretx=sum(1,retx);
     end;
     else do;
        if LME>0 then weight_port=cumretx*me_base;
        else weight_port=.;
        cumretx=cumretx*sum(1,retx);
     end; end;
output crspm3;
if month(date)=12 and ME>0 then output decme;
run;

/* Create a file with data for each June with ME from previous December */
proc sql;
  create table crspjune as
  select a.*, b.DEC_ME
  from crspm3 (where=(month(date)=6)) as a, decme as b
  where a.permno=b.permno and
  intck('month',b.date,a.date)=6;
quit;

/***************   Part 3: Merging CRSP and Compustat ***********/
/* Add Permno to Compustat sample */
proc sql;
  create table ccm1 as
  select a.*, b.lpermno as permno, b.linkprim
  from comp as a, crsp.ccmxpf_linktable as b
  where a.gvkey=b.gvkey
  and substr(b.linktype,1,1)='L' and linkprim in ('P','C')
  and (intnx('month',intnx('year',a.datadate,0,'E'),6,'E') >= b.linkdt)
  and (b.linkenddt >= intnx('month',intnx('year',a.datadate,0,'E'),6,'E')
  or missing(b.linkenddt))
  order by a.datadate, permno, b.linkprim desc;
quit;

/*  Cleaning Compustat Data for no relevant duplicates                      */
/*  Eliminating overlapping matching : few cases where different gvkeys     */
/*  for same permno-date --- some of them are not 'primary' matches in CCM  */
/*  Use linkprim='P' for selecting just one gvkey-permno-date combination   */
data ccm1a; set ccm1;
  by datadate permno descending linkprim;
  if first.permno;
run;

/* Sanity Check -- No Duplicates */
proc sort data=ccm1a nodupkey; by permno year datadate; run;

/* 2. However, there other type of duplicates within the year                */
/* Some companiess change fiscal year end in the middle of the calendar year */
/* In these cases, there are more than one annual record for accounting data */
/* We will be selecting the last annual record in a given calendar year      */
data ccm2a ; set ccm1a;
  by permno year datadate;
  if last.year;
run;

/* Sanity Check -- No Duplicates */
proc sort data=ccm2a nodupkey; by permno datadate; run;

/* Finalize Compustat Sample                              */
/* Merge CRSP with Compustat data, at June of every year  */
/* Match fiscal year ending calendar year t-1 with June t */
proc sql; create table ccm2_june as
  select a.*, b.BE, (1000*b.BE)/a.DEC_ME as BEME, b.count,
  b.datadate,
  intck('month',b.datadate, a.date) as dist
  from crspjune a, ccm2a b
  where a.permno=b.permno and intnx('month',a.date,0,'E')=
  intnx('month',intnx('year',b.datadate,0,'E'),6,'E')
  order by a.date;
quit;

/************************ Part 4: Size and Book to Market Portfolios ***/
/* Forming Portolio by ME and BEME as of each June t                   */
/* Calculate NYSE Breakpoints for Market Equity (ME) and               */
/* Book-to-Market (BEME)                                               */
proc univariate data=ccm2_june noprint;
  where exchcd=1 and beme>0 and shrcd in (10,11) and me>0 and count>=2;
  var BEME; * ME is Market Equity at the end of June;
  by date; /*at june;*/
  output out=nyse_breaks pctlpre=BEME pctlpts= 10 to 90 by 10; /* HX ADD */
run;

/*proc export data=nyse_breaks outfile="bp.csv" */
/*dbms=csv replace; */


/* Use Breakpoints to classify stock only at end of all June's */
/* HX MODI */
proc sql;
  create table ccm3_june as
  select a.*,
  b.beme10,b.beme20,b.beme30, b.beme40,b.beme50, b.beme60,b.beme70, b.beme80,b.beme90
  from ccm2_june as a, nyse_breaks as b
  where a.date=b.date;
quit;

/* Create portfolios as of June                       */
/* SIZE Portfolios          : S[mall] or B[ig]        */
/* Book-to-market Portfolios: L[ow], M[edium], H[igh] */
/* HX MODI  10 times 10*/
data june ; set ccm3_june;
 If beme>0 and me>0 and count>=2 then do;
 positivebeme=1;

 if 0 < beme <= beme10 then           btmport = 'BM0' ;
 else if beme10 < beme <= beme20 then btmport = 'BM1' ;
 else if beme20 < beme <= beme30 then btmport = 'BM2' ;
 else if beme30 < beme <= beme40 then btmport = 'BM3' ;
 else if beme40 < beme <= beme50 then btmport = 'BM4' ;
 else if beme50 < beme <= beme60 then btmport = 'BM5' ;
 else if beme60 < beme <= beme70 then btmport = 'BM6' ;
 else if beme70 < beme <= beme80 then btmport = 'BM7' ;
 else if beme80 < beme <= beme90 then btmport = 'BM8' ;
 else if beme  > beme90 then          btmport = 'BM9' ;
 else btmport='';
end;
else positivebeme=0;
if cmiss(btmport)=0 then nonmissport=1; else nonmissport=0;
keep permno date btmport positivebeme exchcd shrcd nonmissport;
run;

/*proc export data=ccm3_june outfile="ccm3june.csv" */
/*dbms=csv replace; */

/* HX ADD */
data myjune; set june;
myind25 = catx('_', btmport);
run;

data myjune; set myjune;
sortdate = intnx('month',date,0,'E');
nextmon = intnx('month',date,1);
nextmonend = intnx('month',nextmon,0,'E');
run;

/* proc export data=myjune outfile="myjune_25.csv" */
/* dbms=csv replace; */

/* populate annual data to monthly */
%populate(inset=myjune, outset=mymonth, datevar=nextmonend, idvar=permno, forward_max=12);

/* Identifying each month the securities of              */
/* Buy and hold June portfolios from July t to June t+1  */
proc sql; create table ccm4 as
 select a.*, b.btmport, b.date as portdate format MMDDYYD10.,
        b.positivebeme , b.nonmissport
 from crspm3 as a, june as b
 where a.permno=b.permno and  1 <= intck('month',b.date,a.date) <= 12
 order by date, btmport;
quit;

/*************** Part 5: Calculating Fama-French Factors  **************/
/* Calculate monthly time series of weighted average portfolio returns */
proc means data=ccm4 noprint;
 where weight_port>0 and positivebeme=1 and exchcd in (1,2,3)
      and shrcd in (10,11) and nonmissport=1;
 by date btmport;
 var retadj;
 weight weight_port;
 output out=vwret (drop= _type_ _freq_ ) mean=vwret n=n_firms;
run;

/*proc export data=vwret outfile="vwret_6_BM.csv"*/
/*dbms=csv replace;*/

/* Monthly Factor Returns: SMB and HML */
proc transpose data=vwret(keep=date btmport vwret)
 out=vwret2 (drop=_name_ _label_);
 by date ;
 ID btmport;
 Var vwret;
run;

proc export data=vwret2 outfile="vwret2_10_BM.csv"
dbms=csv replace;

/* Number of Firms */
proc transpose data=vwret(keep=date btmport n_firms)
               out=vwret3 (drop=_name_ _label_) prefix=n_;
by date ;
ID btmport;
Var n_firms;
run;

proc export data=vwret3 outfile="vwret3_10_BM.csv"
dbms=csv replace;

proc sort data=mymonth nodupkey out=&table_out;
where "&begdate"d<=mdate<="&enddate"d;
by mdate permno;run;

%mend DIVIDE_PORTFOLIO_10_BM;


%MACRO DIVIDE_PORTFOLIO_10_CFP (begdate=, enddate=, table_out=);


/************************ Part 1: Compustat ****************************/
/* Compustat XpressFeed Variables:                                     */
/* AT      = Total Assets                                              */
/* PSTKL   = Preferred Stock Liquidating Value                         */
/* TXDITC  = Deferred Taxes and Investment Tax Credit                  */
/* PSTKRV  = Preferred Stock Redemption Value                          */
/* seq     = Common/Ordinary Equity - Total                            */
/* PSTK    = Preferred/Preference Stock (Capital) - Total              */

/* In calculating Book Equity, incorporate Preferred Stock (PS) values */
/*  use the redemption value of PS, or the liquidation value           */
/*    or the par value (in that order) (FF,JFE, 1993, p. 8)            */
/* USe Balance Sheet Deferred Taxes TXDITC if available                */
/* Flag for number of years in Compustat (<2 likely backfilled data)   */

%let vars = AT PSTKL TXDITC PSTKRV seq PSTK IB DP;
data comp;
  set comp.funda
  (keep= gvkey datadate &vars indfmt datafmt popsrc consol);
  by gvkey datadate;
  where indfmt='INDL' and datafmt='STD' and popsrc='D' and consol='C'
  and datadate >='01Jan1900'd;

  PS = coalesce(PSTKRV,PSTKL,PSTK,0);
  if missing(TXDITC) then TXDITC = 0 ;
  BE = seq + TXDITC - PS ;
  if BE<0 then BE=.;
  year = year(datadate);
  label BE='Book Value of Equity FYear t-1' ;

  CF=IB+DP;
  if CF<=0 then delete;

  drop indfmt datafmt popsrc consol ps &vars;
  retain count;
  if first.gvkey then count=1;
  else count = count+1;
run;

/************************ Part 2: CRSP **********************************/
/* Create a CRSP Subsample with Monthly Stock and Event Variables       */
/* This procedure creates a SAS dataset named "CRSP_M"                  */
/* Restrictions will be applied later                                   */
/* Select variables from the CRSP monthly stock and event datasets      */
%let msevars=ticker ncusip shrcd exchcd;
%let msfvars =  prc ret retx shrout cfacpr cfacshr;

%include '/wrds/crsp/samples/crspmerge.sas';

%crspmerge(s=m,start=01jan1900,end=31dec2018,
sfvars=&msfvars,sevars=&msevars,filters=exchcd in (1,2,3));

/* CRSP_M is sorted by date and permno and has historical returns     */
/* as well as historical share codes and exchange codes               */
/* Add CRSP delisting returns */
proc sql; create table crspm2
 as select a.*, b.dlret,
  sum(1,ret)*sum(1,dlret)-1 as retadj "Return adjusted for delisting",
  abs(a.prc)*a.shrout as MEq 'Market Value of Equity'
 from Crsp_m a left join crsp.msedelist(where=(missing(dlret)=0)) b
 on a.permno=b.permno and
    intnx('month',a.date,0,'E')=intnx('month',b.DLSTDT,0,'E')
 order by a.date, a.permco, MEq;
quit;

/* There are cases when the same firm (permco) has two or more         */
/* securities (permno) at same date. For the purpose of ME for         */
/* the firm, we aggregated all ME for a given permco, date. This       */
/* aggregated ME will be assigned to the Permno with the largest ME    */
data crspm2a (drop = Meq); set crspm2;
  by date permco Meq;
  retain ME;
  if first.permco and last.permco then do;
    ME=meq;
  output; /* most common case where a firm has a unique permno*/
  end;
  else do ;
    if  first.permco then ME=meq;
    else ME=sum(meq,ME);
    if last.permco then output;
  end;
run;

/* There should be no duplicates*/
proc sort data=crspm2a nodupkey; by permno date;run;

/* The next step does 2 things:                                        */
/* - Create weights for later calculation of VW returns.               */
/*   Each firm's monthly return RET t willl be weighted by             */
/*   ME(t-1) = ME(t-2) * (1 + RETX (t-1))                              */
/*     where RETX is the without-dividend return.                      */
/* - Create a File with December t-1 Market Equity (ME)                */
data crspm3 (keep=permno date retadj weight_port ME exchcd shrcd cumretx)
decme (keep = permno date ME rename=(me=DEC_ME) )  ;
     set crspm2a;
 by permno date;
 retain weight_port cumretx me_base;
 Lpermno=lag(permno);
 LME=lag(me);
     if first.permno then do;
     LME=me/(1+retx); cumretx=sum(1,retx); me_base=LME;weight_port=.;end;
     else do;
     if month(date)=7 then do;
        weight_port= LME;
        me_base=LME; /* lag ME also at the end of June */
        cumretx=sum(1,retx);
     end;
     else do;
        if LME>0 then weight_port=cumretx*me_base;
        else weight_port=.;
        cumretx=cumretx*sum(1,retx);
     end; end;
output crspm3;
if month(date)=12 and ME>0 then output decme;
run;

/* Create a file with data for each June with ME from previous December */
proc sql;
  create table crspjune as
  select a.*, b.DEC_ME
  from crspm3 (where=(month(date)=6)) as a, decme as b
  where a.permno=b.permno and
  intck('month',b.date,a.date)=6;
quit;

/***************   Part 3: Merging CRSP and Compustat ***********/
/* Add Permno to Compustat sample */
proc sql;
  create table ccm1 as
  select a.*, b.lpermno as permno, b.linkprim
  from comp as a, crsp.ccmxpf_linktable as b
  where a.gvkey=b.gvkey
  and substr(b.linktype,1,1)='L' and linkprim in ('P','C')
  and (intnx('month',intnx('year',a.datadate,0,'E'),6,'E') >= b.linkdt)
  and (b.linkenddt >= intnx('month',intnx('year',a.datadate,0,'E'),6,'E')
  or missing(b.linkenddt))
  order by a.datadate, permno, b.linkprim desc;
quit;

/*  Cleaning Compustat Data for no relevant duplicates                      */
/*  Eliminating overlapping matching : few cases where different gvkeys     */
/*  for same permno-date --- some of them are not 'primary' matches in CCM  */
/*  Use linkprim='P' for selecting just one gvkey-permno-date combination   */
data ccm1a; set ccm1;
  by datadate permno descending linkprim;
  if first.permno;
run;

/* Sanity Check -- No Duplicates */
proc sort data=ccm1a nodupkey; by permno year datadate; run;

/* 2. However, there other type of duplicates within the year                */
/* Some companiess change fiscal year end in the middle of the calendar year */
/* In these cases, there are more than one annual record for accounting data */
/* We will be selecting the last annual record in a given calendar year      */
data ccm2a ; set ccm1a;
  by permno year datadate;
  if last.year;
run;

/* Sanity Check -- No Duplicates */
proc sort data=ccm2a nodupkey; by permno datadate; run;

/* Finalize Compustat Sample                              */
/* Merge CRSP with Compustat data, at June of every year  */
/* Match fiscal year ending calendar year t-1 with June t */
proc sql; create table ccm2_june as
  select a.*, b.BE, (1000*b.BE)/a.DEC_ME as BEME, (1000*b.CF)/a.DEC_ME as CFP,b.count,
  b.datadate,
  intck('month',b.datadate, a.date) as dist
  from crspjune a, ccm2a b
  where a.permno=b.permno and intnx('month',a.date,0,'E')=
  intnx('month',intnx('year',b.datadate,0,'E'),6,'E')
  order by a.date;
quit;

/************************ Part 4: Size and Book to Market Portfolios ***/
/* Forming Portolio by ME and BEME as of each June t                   */
/* Calculate NYSE Breakpoints for Market Equity (ME) and               */
/* Book-to-Market (BEME)                                               */
proc univariate data=ccm2_june noprint;
  where exchcd=1 and shrcd in (10,11) and me>0 and count>=2;
  var CFP; * ME is Market Equity at the end of June;
  by date; /*at june;*/
  output out=nyse_breaks pctlpre=CFP pctlpts=10 to 90 by 10; /* HX ADD */
run;

proc export data=nyse_breaks outfile="nyse_breaks.csv"
dbms=csv replace;run;

/* Use Breakpoints to classify stock only at end of all June's */
/* HX MODI */
proc sql;
  create table ccm3_june as
  select a.*,
  b.cfp10,b.cfp20,b.cfp30, b.cfp40,b.cfp50, b.cfp60,b.cfp70, b.cfp80,b.cfp90
  from ccm2_june as a, nyse_breaks as b
  where a.date=b.date;
quit;

/* Create portfolios as of June                       */
/* SIZE Portfolios          : S[mall] or B[ig]        */
/* Operating Profitability Portfolios: R[obust], M[edium], W[eak] */
/* HX MODI  10 times 10*/
data june ; set ccm3_june;
 If me>0 and count>=2 then do;
 positivebeme=1;

 if              cfp <= cfp10 then cfpport = 'CFP0' ;
 else if cfp10 < cfp <= cfp20 then cfpport = 'CFP1' ;
 else if cfp20 < cfp <= cfp30 then cfpport = 'CFP2' ;
 else if cfp30 < cfp <= cfp40 then cfpport = 'CFP3' ;
 else if cfp40 < cfp <= cfp50 then cfpport = 'CFP4' ;
 else if cfp50 < cfp <= cfp60 then cfpport = 'CFP5' ;
 else if cfp60 < cfp <= cfp70 then cfpport = 'CFP6' ;
 else if cfp70 < cfp <= cfp80 then cfpport = 'CFP7' ;
 else if cfp80 < cfp <= cfp90 then cfpport = 'CFP8' ;
 else if         cfp  > cfp90 then cfpport = 'CFP9' ;
 else cfpport='';
end;
else positivebeme=0;
if cmiss(cfpport)=0 then nonmissport=1; else nonmissport=0;
keep permno date cfpport positivebeme exchcd shrcd nonmissport;
run;

/* HX ADD */
data myjune; set june;
myind25 = catx('_', cfpport);
run;

data myjune; set myjune;
sortdate = intnx('month',date,0,'E');
nextmon = intnx('month',date,1);
nextmonend = intnx('month',nextmon,0,'E');
run;

/* proc export data=myjune outfile="myjune_25.csv" */
/* dbms=csv replace; */

/* populate annual data to monthly */
%populate(inset=myjune, outset=mymonth, datevar=nextmonend, idvar=permno, forward_max=12);

/* Identifying each month the securities of              */
/* Buy and hold June portfolios from July t to June t+1  */
proc sql; create table ccm4 as
 select a.*, b.cfpport, b.date as portdate format MMDDYYD10.,
        b.positivebeme , b.nonmissport
 from crspm3 as a, june as b
 where a.permno=b.permno and  1 <= intck('month',b.date,a.date) <= 12
 order by date, cfpport;
quit;

proc export data=ccm4 outfile="ccm4.csv"
dbms=csv replace;run;

/*************** Part 5: Calculating Fama-French Factors  **************/
/* Calculate monthly time series of weighted average portfolio returns */
proc means data=ccm4 noprint;
 where weight_port>0 and positivebeme=1 and exchcd in (1,2,3)
      and shrcd in (10,11) and nonmissport=1;
 by date cfpport;
 var retadj;
 weight weight_port;
 output out=vwret (drop= _type_ _freq_ ) mean=vwret n=n_firms;
run;

/*proc export data=vwret outfile="vwret_6_cfp.csv"*/
/*dbms=csv replace;*/

/* Monthly Factor Returns: SMB and RMW */
proc transpose data=vwret(keep=date cfpport vwret)
 out=vwret2 (drop=_name_ _label_);
 by date ;
 ID cfpport;
 Var vwret;
run;

proc export data=vwret2 outfile="vwret2_10_cfp.csv"
dbms=csv replace;

/* Number of Firms */
proc transpose data=vwret(keep=date cfpport n_firms)
               out=vwret3 (drop=_name_ _label_) prefix=n_;
by date ;
ID cfpport;
Var n_firms;
run;

proc export data=vwret3 outfile="vwret3_10_cfp.csv"
dbms=csv replace;

proc sort data=mymonth nodupkey out=&table_out;
where "&begdate"d<=mdate<="&enddate"d;
by mdate permno;run;

%mend DIVIDE_PORTFOLIO_10_CFP;



%MACRO DIVIDE_PORTFOLIO_10_DY (begdate=, enddate=, table_out=);

/************************ Part 1: Compustat ****************************/
/* Compustat XpressFeed Variables:                                     */
/* AT      = Total Assets                                              */
/* PSTKL   = Preferred Stock Liquidating Value                         */
/* TXDITC  = Deferred Taxes and Investment Tax Credit                  */
/* PSTKRV  = Preferred Stock Redemption Value                          */
/* seq     = Common/Ordinary Equity - Total                            */
/* PSTK    = Preferred/Preference Stock (Capital) - Total              */

/* In calculating Book Equity, incorporate Preferred Stock (PS) values */
/*  use the redemption value of PS, or the liquidation value           */
/*    or the par value (in that order) (FF,JFE, 1993, p. 8)            */
/* USe Balance Sheet Deferred Taxes TXDITC if available                */
/* Flag for number of years in Compustat (<2 likely backfilled data)   */

%let vars = AT PSTKL TXDITC PSTKRV seq PSTK ;
data comp;
  set comp.funda
  (keep= gvkey datadate &vars indfmt datafmt popsrc consol);
  by gvkey datadate;
  where indfmt='INDL' and datafmt='STD' and popsrc='D' and consol='C'
  and datadate >='01Jan1959'd;
 /* Two years of accounting data before 1962 */
  PS = coalesce(PSTKRV,PSTKL,PSTK,0);
  if missing(TXDITC) then TXDITC = 0 ;
  BE = seq + TXDITC - PS ;
  if BE<0 then BE=.;
  year = year(datadate);
  label BE='Book Value of Equity FYear t-1' ;
  drop indfmt datafmt popsrc consol ps &vars;
  retain count;
  if first.gvkey then count=1;
  else count = count+1;
run;

/************************ Part 2: CRSP **********************************/
/* Create a CRSP Subsample with Monthly Stock and Event Variables       */
/* This procedure creates a SAS dataset named "CRSP_M"                  */
/* Restrictions will be applied later                                   */
/* Select variables from the CRSP monthly stock and event datasets      */
%let msevars=ticker ncusip shrcd exchcd;
%let msfvars =  prc ret retx shrout cfacpr cfacshr;

%include '/wrds/crsp/samples/crspmerge.sas';

%crspmerge(s=m,start=01jan1955,end=31dec2018,
sfvars=&msfvars,sevars=&msevars,filters=exchcd in (1,2,3));

/* CRSP_M is sorted by date and permno and has historical returns     */
/* as well as historical share codes and exchange codes               */
/* Add CRSP delisting returns */
proc sql; create table crspm2
 as select a.*, b.dlret,
  sum(1,ret)*sum(1,dlret)-1 as retadj "Return adjusted for delisting",
  abs(a.prc)*a.shrout as MEq 'Market Value of Equity'
 from Crsp_m a left join crsp.msedelist(where=(missing(dlret)=0)) b
 on a.permno=b.permno and
    intnx('month',a.date,0,'E')=intnx('month',b.DLSTDT,0,'E')
 order by a.date, a.permco, MEq;
quit;

/* There are cases when the same firm (permco) has two or more         */
/* securities (permno) at same date. For the purpose of ME for         */
/* the firm, we aggregated all ME for a given permco, date. This       */
/* aggregated ME will be assigned to the Permno with the largest ME    */
data crspm2a (drop = Meq); set crspm2;
  by date permco Meq;
  retain ME;
  if first.permco and last.permco then do;
    ME=meq;
  output; /* most common case where a firm has a unique permno*/
  end;
  else do ;
    if  first.permco then ME=meq;
    else ME=sum(meq,ME);
    if last.permco then output;
  end;
run;

%macro ttm12(var); (&var + lag1(&var) + lag2(&var) + lag3(&var) + lag4(&var) + lag5(&var)
+ lag6(&var) + lag7(&var) + lag8(&var) + lag9(&var) + lag10(&var) + lag11(&var)) %mend;

proc sql;
	create table mcrsp
	as select permno,date,ret,retx
	from crsp.msf;
	quit;

proc sql;
create table mcrsp2 as
select a.ME, b.* from
crspm2a a left join mcrsp b
on
a.permno = b.permno
and a.date = b.date
order by b.permno, b.date;
quit;

data mcrsp2; set mcrsp2;
by permno date;
lag_1_ret = lag(ret);
lag_2_ret = lag2(ret);
lag_3_ret = lag3(ret);
lag_4_ret = lag4(ret);
lag_5_ret = lag5(ret);
lag_6_ret = lag6(ret);
lag_7_ret = lag7(ret);
lag_8_ret = lag8(ret);
lag_9_ret = lag9(ret);
lag_10_ret = lag10(ret);
lag_11_ret = lag11(ret);
if permno ne lag(permno) then lag_1_ret=.;
if permno ne lag2(permno) then lag_2_ret=.;
if permno ne lag3(permno) then lag_3_ret=.;
if permno ne lag4(permno) then lag_4_ret=.;
if permno ne lag5(permno) then lag_5_ret=.;
if permno ne lag6(permno) then lag_6_ret=.;
if permno ne lag7(permno) then lag_7_ret=.;
if permno ne lag8(permno) then lag_8_ret=.;
if permno ne lag9(permno) then lag_9_ret=.;
if permno ne lag10(permno) then lag_10_ret=.;
if permno ne lag11(permno) then lag_11_ret=.;
if nmiss(lag_1_ret,lag_2_ret,lag_3_ret,lag_4_ret,lag_5_ret,lag_6_ret,lag_7_ret,lag_8_ret,lag_9_ret,lag_10_ret,lag_11_ret)<=4 then
dvt = %ttm12((ret-retx)*lag(ME));
run;

proc sql;
create table crspm2aa as
select a.*, b.* from
crspm2a a left join mcrsp2 b
on
a.permno = b.permno
and a.date = b.date
order by a.permno, a.date;
quit;


/* There should be no duplicates*/
proc sort data=crspm2aa nodupkey; by permno date;run;



/* The next step does 2 things:                                        */
/* - Create weights for later calculation of VW returns.               */
/*   Each firm's monthly return RET t willl be weighted by             */
/*   ME(t-1) = ME(t-2) * (1 + RETX (t-1))                              */
/*     where RETX is the without-dividend return.                      */
/* - Create a File with December t-1 Market Equity (ME)                */
data crspm3 (keep=permno date retadj weight_port ME ret dvt exchcd sh rcd cumretx)
decme (keep = permno date ME rename=(me=DEC_ME) )  ;
     set crspm2aa;
 by permno date;
 retain weight_port cumretx me_base;
 Lpermno=lag(permno);
 LME=lag(me);
     if first.permno then do;
     LME=me/(1+retx); cumretx=sum(1,retx); me_base=LME;weight_port=.;end;
     else do;
     if month(date)=7 then do;
        weight_port= LME;
        me_base=LME; /* lag ME also at the end of June */
        cumretx=sum(1,retx);
     end;
     else do;
        if LME>0 then weight_port=cumretx*me_base;
        else weight_port=.;
        cumretx=cumretx*sum(1,retx);
     end; end;
output crspm3;
if month(date)=12 and ME>0 then output decme;
run;

/* Create a file with data for each June with ME from previous December */
proc sql;
  create table crspjune as
  select a.*, b.DEC_ME
  from crspm3 (where=(month(date)=6)) as a, decme as b
  where a.permno=b.permno and
  intck('month',b.date,a.date)=6;
quit;

proc sort data =crspjune;by permno date;run;

data crspjune; set crspjune;
by permno date;
lagme = lag(ME);
if first.permno then lagme=.;
run;

/*proc export data=crspjune outfile="crspjune.csv"*/
/*dbms=csv replace;*/

/***************   Part 3: Merging CRSP and Compustat ***********/
/* Add Permno to Compustat sample */
proc sql;
  create table ccm1 as
  select a.*, b.lpermno as permno, b.linkprim
  from comp as a, crsp.ccmxpf_linktable as b
  where a.gvkey=b.gvkey
  and substr(b.linktype,1,1)='L' and linkprim in ('P','C')
  and (intnx('month',intnx('year',a.datadate,0,'E'),6,'E') >= b.linkdt)
  and (b.linkenddt >= intnx('month',intnx('year',a.datadate,0,'E'),6,'E')
  or missing(b.linkenddt))
  order by a.datadate, permno, b.linkprim desc;
quit;

/*  Cleaning Compustat Data for no relevant duplicates                      */
/*  Eliminating overlapping matching : few cases where different gvkeys     */
/*  for same permno-date --- some of them are not 'primary' matches in CCM  */
/*  Use linkprim='P' for selecting just one gvkey-permno-date combination   */
data ccm1a; set ccm1;
  by datadate permno descending linkprim;
  if first.permno;
run;

/* Sanity Check -- No Duplicates */
proc sort data=ccm1a nodupkey; by permno year datadate; run;

/* 2. However, there other type of duplicates within the year                */
/* Some companiess change fiscal year end in the middle of the calendar year */
/* In these cases, there are more than one annual record for accounting data */
/* We will be selecting the last annual record in a given calendar year      */
data ccm2a ; set ccm1a;
  by permno year datadate;
  if last.year;
run;

/* Sanity Check -- No Duplicates */
proc sort data=ccm2a nodupkey; by permno datadate; run;

/* Finalize Compustat Sample                              */
/* Merge CRSP with Compustat data, at June of every year  */
/* Match fiscal year ending calendar year t-1 with June t */
proc sql; create table ccm2_june as
  select a.*, b.BE, a.dvt/a.lagme as DY, b.count,
  b.datadate,
  intck('month',b.datadate, a.date) as dist
  from crspjune a, ccm2a b
  where a.permno=b.permno and intnx('month',a.date,0,'E')=
  intnx('month',intnx('year',b.datadate,0,'E'),6,'E')
  order by a.date;
quit;



/* ********************************************************************************* */
/* ******************* CREATE MONTHLY BIG CCM_CRSP ********************************* */
/*  ccm2_june comes from original script                                             */
/*  merge crspm2a with ccm2_june                                                     */
/*  BE information of June paste to crspm2a for July year t to June year t+1         */
/*                                                                                   */
/*                                                                                   */
/*                                                                                   */
proc sql;
create table ccm_month as
select a.*, b.count, b.dy, b.date as june_date, b.datadate as annual_info_date from
crspm2aa a left join ccm2_june b
on
a.permno = b.permno
and 1<=intck('month', intnx('month',b.date,0,'E'),  intnx('month',a.date,0,'E') )<=12
order by a.permno, a.date;
quit;


/************************ Part 4: Size and Book to Market Portfolios ***/
/* Forming Portolio by ME and BEME as of each June t                   */
/* Calculate NYSE Breakpoints for Market Equity (ME) and               */
/* Book-to-Market (BEME)                                               */

data ccm_month;set ccm_month;
if not missing(DY);
if DY>0;
run;

proc sort data=ccm_month; by date permno; run;

proc univariate data=ccm_month noprint;
  where exchcd=1 and shrcd in (10,11) and me>0 and count>=2;
  var DY; * ME is Market Equity at the end of June;
  by date; /*at june;*/
  output out=nyse_breaks pctlpre=DY pctlpts= 10 to 90 by 10; /* HX ADD */
run;


/* Use Breakpoints to classify stock only at end of all June's */
/* HX MODI */
proc sql;
  create table ccm3 as
  select a.*,
  b.dy10, b.dy20, b.dy30, b.dy40, b.dy50, b.dy60, b.dy70, b.dy80, b.dy90
  from ccm_month as a, nyse_breaks as b
  where a.date=b.date;
quit;

/* Create portfolios as of June                       */
/* SIZE Portfolios          : S[mall] or B[ig]        */
/* Operating Profitability Portfolios: R[obust], M[edium], W[eak] */
/* HX MODI  10 times 10*/
data all_month ; set ccm3;
 If me>0 and count>=2 then do;
 positiveme=1;

 if        0 <= dy <= dy10 then dyport = 'DY0' ;
 else if dy10 < dy <= dy20 then dyport = 'DY1' ;
 else if dy20 < dy <= dy30 then dyport = 'DY2' ;
 else if dy30 < dy <= dy40 then dyport = 'DY3' ;
 else if dy40 < dy <= dy50 then dyport = 'DY4' ;
 else if dy50 < dy <= dy60 then dyport = 'DY5' ;
 else if dy60 < dy <= dy70 then dyport = 'DY6' ;
 else if dy70 < dy <= dy80 then dyport = 'DY7' ;
 else if dy80 < dy <= dy90 then dyport = 'DY8' ;
 else if        dy  > dy90 then dyport = 'DY9' ;
 else dyport='';
end;
else positiveme=0;
if cmiss(dyport)=0 then nonmissport=1; else nonmissport=0;
keep permno date retadj dyport exchcd shrcd nonmissport me;
run;

/* HX ADD */
data all_month; set all_month;
myind = catx('_', dyport);
run;

/* ********************************************************************************* */
/* lag port labels one month */
proc sort data=all_month; by permno date; run;

data ccm4; set all_month;
by permno date;
mdate = intnx('month', date,0,'E');
format mdate worddatx19.;
mport = lag(dyport);
weight_port = lag(me);
myind25 = lag(myind);
if first.permno then mport=.;
if first.permno then weight_port=.;
if first.permno then myind25=.;
run;

/*proc export data=ccm4 outfile="ccm4.csv"*/
/*dbms=csv replace;*/

/* ********************************************************************************* */
/* mymonth */

data mymonth; set ccm4;
keep mdate permno myind25;
run;


/*proc export data=mymonth outfile="mymonth.csv"*/
/*dbms=csv replace;*/


/*************** Part 5: Calculating Fama-French Factors  **************/
/* Calculate monthly time series of weighted average portfolio returns */
proc sort data=ccm4; by date mport; run;

proc means data=ccm4 noprint;
 where weight_port>0 and exchcd in (1,2,3)
      and shrcd in (10,11) and nonmissport=1;
 by date mport;
 var retadj;
 weight weight_port;
 output out=vwret (drop= _type_ _freq_ ) mean=vwret n=n_firms;
run;

proc transpose data=vwret(keep=date mport vwret)
 out=vwret2 (drop=_name_ _label_);
 by date ;
 ID mport;
 Var vwret;
run;

proc export data=vwret2 outfile="vwret2_10_DY.csv"
dbms=csv replace;

/* Number of Firms */
proc transpose data=vwret(keep=date mport n_firms)
               out=vwret3 (drop=_name_ _label_) prefix=n_;
by date ;
ID mport;
Var n_firms;
run;

proc export data=vwret3 outfile="vwret3_10_DY.csv"
dbms=csv replace;

proc sort data=mymonth nodupkey out=&table_out;
where "&begdate"d<=mdate<="&enddate"d;
by mdate permno;run;

%mend DIVIDE_PORTFOLIO_10_DY;


%MACRO DIVIDE_PORTFOLIO_10_EP (begdate=, enddate=, table_out=);


/************************ Part 1: Compustat ****************************/
/* Earnings to price ratio                                             */
/* Compustat XpressFeed Variables:                                     */
/* AT      = Total Assets                                              */
/* PSTKL   = Preferred Stock Liquidating Value                         */
/* TXDITC  = Deferred Taxes and Investment Tax Credit                  */
/* PSTKRV  = Preferred Stock Redemption Value                          */
/* seq     =     hgghggiuhhiuhkjhjhkj                            */
/* PSTK    = Preferred/Preference Stock (Capital) - Total              */

/* In calculating Book Equity, incorporate Preferred Stock (PS) values */
/*  use the redemption value of PS, or the liquidation value           */
/*    or the par value (in that order) (FF,JFE, 1993, p. 8)            */
/* USe Balance Sheet Deferred Taxes TXDITC if available                */
/* Flag for number of years in Compustat (<2 likely backfilled data)   */

%let vars = AT PSTKL TXDITC PSTKRV seq PSTK;
data comp;
  set comp.funda
  (keep= gvkey datadate &vars indfmt datafmt popsrc consol IB);
  by gvkey datadate;
  where indfmt='INDL' and datafmt='STD' and popsrc='D' and consol='C'
  and datadate >='01Jan1900'd;
 /* Two years of accounting data before 1962 */
  year = year(datadate);

  label IB='Imcome before extraordinary items of Equity FYear t-1' ;
  drop indfmt datafmt popsrc consol ps &vars;
  retain count;
  if first.gvkey then count=1;
  else count = count+1;
run;

/************************ Part 2: CRSP **********************************/
/* Create a CRSP Subsample with Monthly Stock and Event Variables       */
/* This procedure creates a SAS dataset named "CRSP_M"                  */
/* Restrictions will be applied later                                   */
/* Select variables from the CRSP monthly stock and event datasets      */
%let msevars=ticker ncusip shrcd exchcd;
%let msfvars =  prc ret retx shrout cfacpr cfacshr;

%include '/wrds/crsp/samples/crspmerge.sas';

%crspmerge(s=m,start=01jan1900,end=31dec2018,
sfvars=&msfvars,sevars=&msevars,filters=exchcd in (1,2,3));

/* CRSP_M is sorted by date and permno and has historical returns     */
/* as well as historical share codes and exchange codes               */
/* Add CRSP delisting returns */
proc sql; create table crspm2
 as select a.*, b.dlret,
  sum(1,ret)*sum(1,dlret)-1 as retadj "Return adjusted for delisting",
  abs(a.prc)*a.shrout as MEq 'Market Value of Equity'
 from Crsp_m a left join crsp.msedelist(where=(missing(dlret)=0)) b
 on a.permno=b.permno and
    intnx('month',a.date,0,'E')=intnx('month',b.DLSTDT,0,'E')
 order by a.date, a.permco, MEq;
quit;

/* There are cases when the same firm (permco) has two or more         */
/* securities (permno) at same date. For the purpose of ME for         */
/* the firm, we aggregated all ME for a given permco, date. This       */
/* aggregated ME will be assigned to the Permno with the largest ME    */
data crspm2a (drop = Meq); set crspm2;
  by date permco Meq;
  retain ME;
  if first.permco and last.permco then do;
    ME=meq;
  output; /* most common case where a firm has a unique permno*/
  end;
  else do ;
    if  first.permco then ME=meq;
    else ME=sum(meq,ME);
    if last.permco then output;
  end;
run;

/* There should be no duplicates*/
proc sort data=crspm2a nodupkey; by permno date;run;

/* The next step does 2 things:                                        */
/* - Create weights for later calculation of VW returns.               */
/*   Each firm's monthly return RET t willl be weighted by             */
/*   ME(t-1) = ME(t-2) * (1 + RETX (t-1))                              */
/*     where RETX is the without-dividend return.                      */
/* - Create a File with December t-1 Market Equity (ME)                */
data crspm3 (keep=permno date retadj weight_port ME exchcd shrcd cumretx)
decme (keep = permno date ME rename=(me=DEC_ME) )  ;
     set crspm2a;
 by permno date;
 retain weight_port cumretx me_base;
 Lpermno=lag(permno);
 LME=lag(me);
     if first.permno then do;
     LME=me/(1+retx); cumretx=sum(1,retx); me_base=LME;weight_port=.;end;
     else do;
     if month(date)=7 then do;
        weight_port= LME;
        me_base=LME; /* lag ME also at the end of June */
        cumretx=sum(1,retx);
     end;
     else do;
        if LME>0 then weight_port=cumretx*me_base;
        else weight_port=.;
        cumretx=cumretx*sum(1,retx);
     end; end;
output crspm3;
if month(date)=12 and ME>0 then output decme;
run;

/* Create a file with data for each June with ME from previous December */
proc sql;
  create table crspjune as
  select a.*, b.DEC_ME
  from crspm3 (where=(month(date)=6)) as a, decme as b
  where a.permno=b.permno and
  intck('month',b.date,a.date)=6;
quit;

/***************   Part 3: Merging CRSP and Compustat ***********/
/* Add Permno to Compustat sample */
proc sql;
  create table ccm1 as
  select a.*, b.lpermno as permno, b.linkprim
  from comp as a, crsp.ccmxpf_linktable as b
  where a.gvkey=b.gvkey
  and substr(b.linktype,1,1)='L' and linkprim in ('P','C')
  and (intnx('month',intnx('year',a.datadate,0,'E'),6,'E') >= b.linkdt)
  and (b.linkenddt >= intnx('month',intnx('year',a.datadate,0,'E'),6,'E')
  or missing(b.linkenddt))
  order by a.datadate, permno, b.linkprim desc;
quit;

/*  Cleaning Compustat Data for no relevant duplicates                      */
/*  Eliminating overlapping matching : few cases where different gvkeys     */
/*  for same permno-date --- some of them are not 'primary' matches in CCM  */
/*  Use linkprim='P' for selecting just one gvkey-permno-date combination   */
data ccm1a; set ccm1;
  by datadate permno descending linkprim;
  if first.permno;
run;

/* Sanity Check -- No Duplicates */
proc sort data=ccm1a nodupkey; by permno year datadate; run;

/* 2. However, there other type of duplicates within the year                */
/* Some companiess change fiscal year end in the middle of the calendar year */
/* In these cases, there are more than one annual record for accounting data */
/* We will be selecting the last annual record in a given calendar year      */
data ccm2a ; set ccm1a;
  by permno year datadate;
  if last.year;
run;

/* Sanity Check -- No Duplicates */
proc sort data=ccm2a nodupkey; by permno datadate; run;

/* Finalize Compustat Sample                              */
/* Merge CRSP with Compustat data, at June of every year  */
/* Match fiscal year ending calendar year t-1 with June t */
proc sql; create table ccm2_june as
  select a.*, b.IB, (b.IB)/a.DEC_ME as EP, b.count,
  b.datadate,
  intck('month',b.datadate, a.date) as dist
  from crspjune a, ccm2a b
  where a.permno=b.permno and intnx('month',a.date,0,'E')=
  intnx('month',intnx('year',b.datadate,0,'E'),6,'E')
  order by a.date;
quit;

/************************ Part 4: Size and Earnings to Price Portfolios ***/
/* Forming Portolio by ME and BEME as of each June t                   */
/* Calculate NYSE Breakpoints for Market Equity (ME) and               */
/* Book-to-Market (BEME)                                               */
proc univariate data=ccm2_june noprint;
  where exchcd=1 and EP>0 and shrcd in (10,11) and me>0 and count>=2;
  var EP; * ME is Market Equity at the end of June;
  by date; /*at june;*/
  output out=nyse_breaks pctlpre=EP pctlpts= 10 to 90 by 10; /* HX ADD */
run;


/* Use Breakpoints to classify stock only at end of all June's */
/* HX MODI */
proc sql;
  create table ccm3_june as
  select a.*,
  b.ep10,b.ep20,b.ep30, b.ep40,b.ep50, b.ep60,b.ep70, b.ep80,b.ep90
  from ccm2_june as a, nyse_breaks as b
  where a.date=b.date;
quit;


/* Create portfolios as of June                       */
/* SIZE Portfolios          : S[mall] or B[ig]        */
/* Book-to-market Portfolios: L[ow], M[edium], H[igh] */
/* HX MODI  10 times 10*/
data june ; set ccm3_june;
 If ep>0 and me>0 and count>=2 then do;
 /* In French Website, ep<0 all falls into one portfolio, I simply discard ep<0 stocks */
 positiveep=1;
 if 0 < ep <= ep10 then         epport = 'EP0' ;
 else if ep10 < ep <= ep20 then epport = 'EP1' ;
 else if ep20 < ep <= ep30 then epport = 'EP2' ;
 else if ep30 < ep <= ep40 then epport = 'EP3' ;
 else if ep40 < ep <= ep50 then epport = 'EP4' ;
 else if ep50 < ep <= ep60 then epport = 'EP5' ;
 else if ep60 < ep <= ep70 then epport = 'EP6' ;
 else if ep70 < ep <= ep80 then epport = 'EP7' ;
 else if ep80 < ep <= ep90 then epport = 'EP8' ;
 else if ep  > ep90 then        epport = 'EP9' ;
 else epport='';
end;
else positiveep=0;
if cmiss(epport)=0 then nonmissport=1; else nonmissport=0;
keep permno date epport positiveep exchcd shrcd nonmissport;
run;

/* HX ADD */
data myjune; set june;
myind25 = catx('_', epport);
run;

data myjune; set myjune;
sortdate = intnx('month',date,0,'E');
nextmon = intnx('month',date,1);
nextmonend = intnx('month',nextmon,0,'E');
run;

/* proc export data=myjune outfile="myjune_25.csv" */
/* dbms=csv replace; */

/* populate annual data to monthly */
%populate(inset=myjune, outset=mymonth, datevar=nextmonend, idvar=permno, forward_max=12);

/* Identifying each month the securities of              */
/* Buy and hold June portfolios from July t to June t+1  */
proc sql; create table ccm4 as
 select a.*,b.epport, b.date as portdate format MMDDYYD10.,
        b.positiveep , b.nonmissport
 from crspm3 as a, june as b
 where a.permno=b.permno and  1 <= intck('month',b.date,a.date) <= 12
 order by date,epport;
quit;

/*************** Part 5: Calculating Fama-French Factors  **************/
/* Calculate monthly time series of weighted average portfolio returns */
proc means data=ccm4 noprint;
 where weight_port>0 and positiveep=1 and exchcd in (1,2,3)
      and shrcd in (10,11) and nonmissport=1;
 by date epport;
 var retadj;
 weight weight_port;
 output out=vwret (drop= _type_ _freq_ ) mean=vwret n=n_firms;
run;

/*proc export data=vwret outfile="vwret_6_EP.csv"*/
/*dbms=csv replace;*/

/* Monthly Factor Returns: SMB and HML */
proc transpose data=vwret(keep=date epport vwret)
 out=vwret2 (drop=_name_ _label_);
 by date ;
 ID epport;
 Var vwret;
run;

proc export data=vwret2 outfile="vwret2_10_EP.csv"
dbms=csv replace;

/* Number of Firms */
proc transpose data=vwret(keep=date epport n_firms)
               out=vwret3 (drop=_name_ _label_) prefix=n_;
by date ;
ID epport;
Var n_firms;
run;

proc export data=vwret3 outfile="vwret3_10_EP.csv"
dbms=csv replace;


proc sort data=mymonth nodupkey out=&table_out;
where "&begdate"d<=mdate<="&enddate"d;
by mdate permno;run;

%mend DIVIDE_PORTFOLIO_10_EP;


%MACRO DIVIDE_PORTFOLIO_10_INV (begdate=, enddate=, table_out=);

/************************ Part 1: Compustat ****************************/
/* Compustat XpressFeed Variables:                                     */
/* AT      = Total Assets                                              */
/* PSTKL   = Preferred Stock Liquidating Value                         */
/* TXDITC  = Deferred Taxes and Investment Tax Credit                  */
/* PSTKRV  = Preferred Stock Redemption Value                          */
/* seq     =                                   */
/* PSTK    = Preferred/Preference Stock (Capital) - Total              */

/* In calculating Book Equity, incorporate Preferred Stock (PS) values */
/*  use the redemption value of PS, or the liquidation value           */
/*    or the par value (in that order) (FF,JFE, 1993, p. 8)            */
/* USe Balance Sheet Deferred Taxes TXDITC if available                */
/* Flag for number of years in Compustat (<2 likely backfilled data)   */

%let vars = AT PSTKL TXDITC PSTKRV seq PSTK ;
data comp;
  set comp.funda
  (keep= gvkey datadate &vars indfmt datafmt popsrc consol);
  by gvkey datadate;
  where indfmt='INDL' and datafmt='STD' and popsrc='D' and consol='C'
  and datadate >='01Jan1900'd;
 /* Two years of accounting data before 1962 */
  PS = coalesce(PSTKRV,PSTKL,PSTK,0);
  if missing(TXDITC) then TXDITC = 0 ;
  BE = seq + TXDITC - PS ;
  if BE<0 then BE=.;
  year = year(datadate);
  label BE='Book Value of Equity FYear t-1' ;
  LAT=lag(AT);
  if missing(AT) or missing(LAT) then delete;
  INV=-(LAT-AT)/LAT;
  drop indfmt datafmt popsrc consol ps &vars;
  retain count;
  if first.gvkey then count=1;
  else count = count+1;
run;

/************************ Part 2: CRSP **********************************/
/* Create a CRSP Subsample with Monthly Stock and Event Variables       */
/* This procedure creates a SAS dataset named "CRSP_M"                  */
/* Restrictions will be applied later                                   */
/* Select variables from the CRSP monthly stock and event datasets      */
%let msevars=ticker ncusip shrcd exchcd;
%let msfvars =  prc ret retx shrout cfacpr cfacshr;

%include '/wrds/crsp/samples/crspmerge.sas';

%crspmerge(s=m,start=01jan1900,end=31dec2018,
sfvars=&msfvars,sevars=&msevars,filters=exchcd in (1,2,3));

/* CRSP_M is sorted by date and permno and has historical returns     */
/* as well as historical share codes and exchange codes               */
/* Add CRSP delisting returns */
proc sql; create table crspm2
 as select a.*, b.dlret,
  sum(1,ret)*sum(1,dlret)-1 as retadj "Return adjusted for delisting",
  abs(a.prc)*a.shrout as MEq 'Market Value of Equity'
 from Crsp_m a left join crsp.msedelist(where=(missing(dlret)=0)) b
 on a.permno=b.permno and
    intnx('month',a.date,0,'E')=intnx('month',b.DLSTDT,0,'E')
 order by a.date, a.permco, MEq;
quit;

/* There are cases when the same firm (permco) has two or more         */
/* securities (permno) at same date. For the purpose of ME for         */
/* the firm, we aggregated all ME for a given permco, date. This       */
/* aggregated ME will be assigned to the Permno with the largest ME    */
data crspm2a (drop = Meq); set crspm2;
  by date permco Meq;
  retain ME;
  if first.permco and last.permco then do;
    ME=meq;
  output; /* most common case where a firm has a unique permno*/
  end;
  else do ;
    if  first.permco then ME=meq;
    else ME=sum(meq,ME);
    if last.permco then output;
  end;
run;

/* There should be no duplicates*/
proc sort data=crspm2a nodupkey; by permno date;run;

/* The next step does 2 things:                                        */
/* - Create weights for later calculation of VW returns.               */
/*   Each firm's monthly return RET t willl be weighted by             */
/*   ME(t-1) = ME(t-2) * (1 + RETX (t-1))                              */
/*     where RETX is the without-dividend return.                      */
/* - Create a File with December t-1 Market Equity (ME)                */
data crspm3 (keep=permno date retadj weight_port ME exchcd shrcd cumretx)
decme (keep = permno date ME rename=(me=DEC_ME) )  ;
     set crspm2a;
 by permno date;
 retain weight_port cumretx me_base;
 Lpermno=lag(permno);
 LME=lag(me);
     if first.permno then do;
     LME=me/(1+retx); cumretx=sum(1,retx); me_base=LME;weight_port=.;end;
     else do;
     if month(date)=7 then do;
        weight_port= LME;
        me_base=LME; /* lag ME also at the end of June */
        cumretx=sum(1,retx);
     end;
     else do;
        if LME>0 then weight_port=cumretx*me_base;
        else weight_port=.;
        cumretx=cumretx*sum(1,retx);
     end; end;
output crspm3;
if month(date)=12 and ME>0 then output decme;
run;

/* Create a file with data for each June with ME from previous December */
proc sql;
  create table crspjune as
  select a.*, b.DEC_ME
  from crspm3 (where=(month(date)=6)) as a, decme as b
  where a.permno=b.permno and
  intck('month',b.date,a.date)=6;
quit;

/***************   Part 3: Merging CRSP and Compustat ***********/
/* Add Permno to Compustat sample */
proc sql;
  create table ccm1 as
  select a.*, b.lpermno as permno, b.linkprim
  from comp as a, crsp.ccmxpf_linktable as b
  where a.gvkey=b.gvkey
  and substr(b.linktype,1,1)='L' and linkprim in ('P','C')
  and (intnx('month',intnx('year',a.datadate,0,'E'),6,'E') >= b.linkdt)
  and (b.linkenddt >= intnx('month',intnx('year',a.datadate,0,'E'),6,'E')
  or missing(b.linkenddt))
  order by a.datadate, permno, b.linkprim desc;
quit;

/*  Cleaning Compustat Data for no relevant duplicates                      */
/*  Eliminating overlapping matching : few cases where different gvkeys     */
/*  for same permno-date --- some of them are not 'primary' matches in CCM  */
/*  Use linkprim='P' for selecting just one gvkey-permno-date combination   */
data ccm1a; set ccm1;
  by datadate permno descending linkprim;
  if first.permno;
run;

/* Sanity Check -- No Duplicates */
proc sort data=ccm1a nodupkey; by permno year datadate; run;

/* 2. However, there other type of duplicates within the year                */
/* Some companiess change fiscal year end in the middle of the calendar year */
/* In these cases, there are more than one annual record for accounting data */
/* We will be selecting the last annual record in a given calendar year      */
data ccm2a ; set ccm1a;
  by permno year datadate;
  if last.year;
run;

/* Sanity Check -- No Duplicates */
proc sort data=ccm2a nodupkey; by permno datadate; run;

/* Finalize Compustat Sample                              */
/* Merge CRSP with Compustat data, at June of every year  */
/* Match fiscal year ending calendar year t-1 with June t */
proc sql; create table ccm2_june as
  select a.*, b.BE, (1000*b.BE)/a.DEC_ME as BEME, b.inv, b.count,
  b.datadate,
  intck('month',b.datadate, a.date) as dist
  from crspjune a, ccm2a b
  where a.permno=b.permno and intnx('month',a.date,0,'E')=
  intnx('month',intnx('year',b.datadate,0,'E'),6,'E')
  order by a.date;
quit;

/************************ Part 4: Size and Book to Market Portfolios ***/
/* Forming Portolio by ME and BEME as of each June t                   */
/* Calculate NYSE Breakpoints for Market Equity (ME) and               */
/* Book-to-Market (BEME)                                               */
proc univariate data=ccm2_june noprint;
  where exchcd=1 and shrcd in (10,11) and me>0 and count>=2;
  var INV; * ME is Market Equity at the end of June;
  by date; /*at june;*/
  output out=nyse_breaks pctlpre=INV pctlpts= 10 to 90 by 10; /* HX ADD */
run;


/* Use Breakpoints to classify stock only at end of all June's */
/* HX MODI */
proc sql;
  create table ccm3_june as
  select a.*,
  b.inv10,b.inv20,b.inv30, b.inv40,b.inv50, b.inv60,b.inv70, b.inv80,b.inv90
  from ccm2_june as a, nyse_breaks as b
  where a.date=b.date;
quit;

/* Create portfolios as of June                       */
/* SIZE Portfolios          : S[mall] or B[ig]        */
/* Book-to-market Portfolios: L[ow], M[edium], H[igh] */
/* HX MODI  10 times 10*/
data june ; set ccm3_june;
 If me>0 and count>=2 then do;
 positivebeme=1;

 if              inv <= inv10 then invport = 'INV0' ;
 else if inv10 < inv <= inv20 then invport = 'INV1' ;
 else if inv20 < inv <= inv30 then invport = 'INV2' ;
 else if inv30 < inv <= inv40 then invport = 'INV3' ;
 else if inv40 < inv <= inv50 then invport = 'INV4' ;
 else if inv50 < inv <= inv60 then invport = 'INV5' ;
 else if inv60 < inv <= inv70 then invport = 'INV6' ;
 else if inv70 < inv <= inv80 then invport = 'INV7' ;
 else if inv80 < inv <= inv90 then invport = 'INV8' ;
 else if         inv  > inv90 then invport = 'INV9' ;
 else invport='';
end;
else positivebeme=0;
if cmiss(invport)=0 then nonmissport=1; else nonmissport=0;
keep permno date invport positivebeme exchcd shrcd nonmissport;
run;

/* HX ADD */
data myjune; set june;
myind25 = catx('_', invport);
run;

data myjune; set myjune;
sortdate = intnx('month',date,0,'E');
nextmon = intnx('month',date,1);
nextmonend = intnx('month',nextmon,0,'E');
run;

/* proc export data=myjune outfile="myjune_25.csv" */
/* dbms=csv replace; */

/* populate annual data to monthly */
%populate(inset=myjune, outset=mymonth, datevar=nextmonend, idvar=permno, forward_max=12);

/* Identifying each month the securities of              */
/* Buy and hold June portfolios from July t to June t+1  */
proc sql; create table ccm4 as
 select a.*, b.invport, b.date as portdate format MMDDYYD10.,
        b.positivebeme , b.nonmissport
 from crspm3 as a, june as b
 where a.permno=b.permno and  1 <= intck('month',b.date,a.date) <= 12
 order by date, invport;
quit;

/*************** Part 5: Calculating Fama-French Factors  **************/
/* Calculate monthly time series of weighted average portfolio returns */
proc means data=ccm4 noprint;
 where weight_port>0 and positivebeme=1 and exchcd in (1,2,3)
      and shrcd in (10,11) and nonmissport=1;
 by date invport;
 var retadj;
 weight weight_port;
 output out=vwret (drop= _type_ _freq_ ) mean=vwret n=n_firms;
run;

/*proc export data=vwret outfile="vwret_6_inv.csv"*/
/*dbms=csv replace;*/

/* Monthly Factor Returns: SMB and HML */
proc transpose data=vwret(keep=date invport vwret)
 out=vwret2 (drop=_name_ _label_);
 by date ;
 ID invport;
 Var vwret;
run;

proc export data=vwret2 outfile="vwret2_10_inv.csv"
dbms=csv replace;

/* Number of Firms */
proc transpose data=vwret(keep=date invport n_firms)
               out=vwret3 (drop=_name_ _label_) prefix=n_;
by date ;
ID invport;
Var n_firms;
run;

proc export data=vwret3 outfile="vwret3_10_inv.csv"
dbms=csv replace;

proc sort data=mymonth nodupkey out=&table_out;
where "&begdate"d<=mdate<="&enddate"d;
by mdate permno;run;

%mend DIVIDE_PORTFOLIO_10_INV;


%MACRO DIVIDE_PORTFOLIO_10_MOM1M (begdate=, enddate=, table_out=);

/************************ Part 1: Compustat ****************************/
/* Compustat XpressFeed Variables:                                     */
/* AT      = Total Assets                                              */
/* PSTKL   = Preferred Stock Liquidating Value                         */
/* TXDITC  = Deferred Taxes and Investment Tax Credit                  */
/* PSTKRV  = Preferred Stock Redemption Value                          */
/* seq     = Common/Ordinary Equity - Total                            */
/* PSTK    = Preferred/Preference Stock (Capital) - Total              */

/* In calculating Book Equity, incorporate Preferred Stock (PS) values */
/*  use the redemption value of PS, or the liquidation value           */
/*    or the par value (in that order) (FF,JFE, 1993, p. 8)            */
/* USe Balance Sheet Deferred Taxes TXDITC if available                */
/* Flag for number of years in Compustat (<2 likely backfilled data)   */

%let vars = AT PSTKL TXDITC PSTKRV seq PSTK ;
data comp;
  set comp.funda
  (keep= gvkey datadate &vars indfmt datafmt popsrc consol);
  by gvkey datadate;
  where indfmt='INDL' and datafmt='STD' and popsrc='D' and consol='C'
  and datadate >='01Jan1900'd;
 /* Two years of accounting data before 1962 */
  PS = coalesce(PSTKRV,PSTKL,PSTK,0);
  if missing(TXDITC) then TXDITC = 0 ;
  BE = seq + TXDITC - PS ;
  if BE<0 then BE=.;
  year = year(datadate);
  label BE='Book Value of Equity FYear t-1' ;
  drop indfmt datafmt popsrc consol ps &vars;
  retain count;
  if first.gvkey then count=1;
  else count = count+1;
run;

/************************ Part 2: CRSP **********************************/
/* Create a CRSP Subsample with Monthly Stock and Event Variables       */
/* This procedure creates a SAS dataset named "CRSP_M"                  */
/* Restrictions will be applied later                                   */
/* Select variables from the CRSP monthly stock and event datasets      */
%let msevars=ticker ncusip shrcd exchcd;
%let msfvars =  prc ret retx shrout cfacpr cfacshr;

%include '/wrds/crsp/samples/crspmerge.sas';

%crspmerge(s=m,start=01jan1900,end=31dec2018,
sfvars=&msfvars,sevars=&msevars,filters=exchcd in (1,2,3));

/* CRSP_M is sorted by date and permno and has historical returns     */
/* as well as historical share codes and exchange codes               */
/* Add CRSP delisting returns */
proc sql; create table crspm2
 as select a.*, b.dlret,
  sum(1,ret)*sum(1,dlret)-1 as retadj "Return adjusted for delisting",
  abs(a.prc)*a.shrout as MEq 'Market Value of Equity'
 from Crsp_m a left join crsp.msedelist(where=(missing(dlret)=0)) b
 on a.permno=b.permno and
    intnx('month',a.date,0,'E')=intnx('month',b.DLSTDT,0,'E')
 order by a.date, a.permco, MEq;
quit;

/* There are cases when the same firm (permco) has two or more         */
/* securities (permno) at same date. For the purpose of ME for         */
/* the firm, we aggregated all ME for a given permco, date. This       */
/* aggregated ME will be assigned to the Permno with the largest ME    */
data crspm2a (drop = Meq); set crspm2;
  by date permco Meq;
  retain ME;
  if first.permco and last.permco then do;
    ME=meq;
  output; /* most common case where a firm has a unique permno*/
  end;
  else do ;
    if  first.permco then ME=meq;
    else ME=sum(meq,ME);
    if last.permco then output;
  end;
run;

/* There should be no duplicates*/
proc sort data=crspm2a nodupkey; by permno date;run;

/* add mom12m */
data crspm2a; set crspm2a;
by permno date;
mom1m = ret;
if first.permno then mom1m=.;
/* this is mom characteristic*/
/* sort then label next month firms */
mom12m=  (   (1+lag1(ret))*(1+lag2(ret))*(1+lag3(ret))*(1+lag4(ret))*(1+lag5(ret))*(1+lag6(ret))*
	(1+lag7(ret))*(1+lag8(ret))*(1+lag9(ret))*(1+lag10(ret))*(1+lag11(ret))   ) - 1;
if permno ne lag12(permno) then mom12m=.;
/* we have ME already in about 10 lines before*/

/* ********************************************************************************* */
/* we can add more filtering here, refer to French website */
run;
/* end mom12m */

/* proc export data=crspm2a outfile="crspm2a.csv" */
/* dbms=csv replace;run; */

/* The next step does 2 things:                                        */
/* - Create weights for later calculation of VW returns.               */
/*   Each firm's monthly return RET t willl be weighted by             */
/*   ME(t-1) = ME(t-2) * (1 + RETX (t-1))                              */
/*     where RETX is the without-dividend return.                      */
/* - Create a File with December t-1 Market Equity (ME)                */
data crspm3 (keep=permno date retadj weight_port ME mom1m mom12m exchcd shrcd cumretx)
decme (keep = permno date ME rename=(me=DEC_ME) )  ;
     set crspm2a;
 by permno date;
 retain weight_port cumretx me_base;
 Lpermno=lag(permno);
 LME=lag(me);
     if first.permno then do;
     LME=me/(1+retx); cumretx=sum(1,retx); me_base=LME;weight_port=.;end;
     else do;
     if month(date)=7 then do;
        weight_port= LME;
        me_base=LME; /* lag ME also at the end of June */
        cumretx=sum(1,retx);
     end;
     else do;
        if LME>0 then weight_port=cumretx*me_base;
        else weight_port=.;
        cumretx=cumretx*sum(1,retx);
     end; end;
output crspm3;
if month(date)=12 and ME>0 then output decme;
run;

/* Create a file with data for each June with ME from previous December */
proc sql;
  create table crspjune as
  select a.*, b.DEC_ME
  from crspm3 (where=(month(date)=6)) as a, decme as b
  where a.permno=b.permno and
  intck('month',b.date,a.date)=6;
quit;

/***************   Part 3: Merging CRSP and Compustat ***********/
/* Add Permno to Compustat sample */
proc sql;
  create table ccm1 as
  select a.*, b.lpermno as permno, b.linkprim
  from comp as a, crsp.ccmxpf_linktable as b
  where a.gvkey=b.gvkey
  and substr(b.linktype,1,1)='L' and linkprim in ('P','C')
  and (intnx('month',intnx('year',a.datadate,0,'E'),6,'E') >= b.linkdt)
  and (b.linkenddt >= intnx('month',intnx('year',a.datadate,0,'E'),6,'E')
  or missing(b.linkenddt))
  order by a.datadate, permno, b.linkprim desc;
quit;

/*  Cleaning Compustat Data for no relevant duplicates                      */
/*  Eliminating overlapping matching : few cases where different gvkeys     */
/*  for same permno-date --- some of them are not 'primary' matches in CCM  */
/*  Use linkprim='P' for selecting just one gvkey-permno-date combination   */
data ccm1a; set ccm1;
  by datadate permno descending linkprim;
  if first.permno;
run;

/* Sanity Check -- No Duplicates */
proc sort data=ccm1a nodupkey; by permno year datadate; run;

/* 2. However, there other type of duplicates within the year                */
/* Some companiess change fiscal year end in the middle of the calendar year */
/* In these cases, there are more than one annual record for accounting data */
/* We will be selecting the last annual record in a given calendar year      */
data ccm2a ; set ccm1a;
  by permno year datadate;
  if last.year;
run;

/* Sanity Check -- No Duplicates */
proc sort data=ccm2a nodupkey; by permno datadate; run;

/* Finalize Compustat Sample                              */
/* Merge CRSP with Compustat data, at June of every year  */
/* Match fiscal year ending calendar year t-1 with June t */
proc sql; create table ccm2_june as
  select a.*, b.BE, (1000*b.BE)/a.DEC_ME as BEME, b.count,
  b.datadate,
  intck('month',b.datadate, a.date) as dist
  from crspjune a, ccm2a b
  where a.permno=b.permno and intnx('month',a.date,0,'E')=
  intnx('month',intnx('year',b.datadate,0,'E'),6,'E')
  order by a.date;
quit;

/* proc export data=ccm2_june outfile="ccm2_june.csv" */
/* dbms=csv replace;run; */

/* ********************************************************************************* */
/* ******************* CREATE MONTHLY BIG CCM_CRSP ********************************* */
/*  ccm2_june comes from original script                                             */
/*  merge crspm2a with ccm2_june                                                     */
/*  BE information of June paste to crspm2a for July year t to June year t+1         */
/*                                                                                   */
/*                                                                                   */
/*                                                                                   */
proc sql;
create table ccm_month as
select a.*, b.count, b.beme, b.date as june_date, b.datadate as annual_info_date from
crspm2a a left join ccm2_june b
on
a.permno = b.permno
and 1<=intck('month', intnx('month',b.date,0,'E'),  intnx('month',a.date,0,'E') )<=12
order by a.permno, a.date;
quit;

/* proc export data=ccm_month outfile="ccm_month.csv" */
/* dbms=csv replace;run; */

/************************ Part 4: Size and Book to Market Portfolios ***/
/* Forming Portolio by ME and BEME as of each June t                   */
/* Calculate NYSE Breakpoints for Market Equity (ME) and               */
/* Book-to-Market (BEME)                                               */
proc sort data=ccm_month; by date permno; run;

proc univariate data=ccm_month noprint;
  where exchcd=1 and shrcd in (10,11);
  var mom1m;
  by date;
  output out=nyse_breaks pctlpre=mom1m pctlpts= 10 to 90 by 10; /* HX ADD */
run;

proc export data=nyse_breaks outfile="nyse_breaks.csv"
dbms=csv replace;run;

/* Use Breakpoints to classify stock only at end of all June's */
/* HX MODI */
proc sql;
  create table ccm3 as
  select a.*,
  b.mom1m10, b.mom1m20,b.mom1m30,b.mom1m40,b.mom1m50,b.mom1m60,b.mom1m70,b.mom1m80,b.mom1m90
  from ccm_month as a, nyse_breaks as b
  where a.date=b.date;
quit;

/* Create portfolios as of June                       */
/* SIZE Portfolios          : S[mall] or B[ig]        */
/* Book-to-market Portfolios: L[ow], M[edium], H[igh] */
/* HX MODI  10 times 10*/
data all_month; set ccm3;
 If me>0 then do;
 positiveme = 1;
 if  mom1m <= mom1m10 then           momport = 'MOM1M0' ;
 else if mom1m10 < mom1m <= mom1m20 then momport = 'MOM1M1' ;
 else if mom1m20 < mom1m <= mom1m30 then momport = 'MOM1M2' ;
 else if mom1m30 < mom1m <= mom1m40 then momport = 'MOM1M3' ;
 else if mom1m40 < mom1m <= mom1m50 then momport = 'MOM1M4' ;
 else if mom1m50 < mom1m <= mom1m60 then momport = 'MOM1M5' ;
 else if mom1m60 < mom1m <= mom1m70 then momport = 'MOM1M6' ;
 else if mom1m70 < mom1m <= mom1m80 then momport = 'MOM1M7' ;
 else if mom1m80 < mom1m <= mom1m90 then momport = 'MOM1M8' ;
 else if mom1m  > mom1m90 then          momport = 'MOM1M9' ;
 else momport='';
 end;
 else positiveme = 0;
if cmiss(momport)=0 then nonmissport=1; else nonmissport=0;
keep permno date retadj momport exchcd shrcd nonmissport me;
run;

/* HX ADD */
data all_month; set all_month;
myind = catx('_', momport);
run;

/* ********************************************************************************* */
/* lag port labels one month */
proc sort data=all_month; by permno date; run;

data ccm4; set all_month;
by permno date;
mdate = intnx('month', date,0,'E');
format mdate worddatx19.;
mport = lag(momport);
weight_port = lag(me);
myind25 = lag(myind);
if first.permno then mport=.;
if first.permno then weight_port=.;
if first.permno then myind25=.;
run;

/* ********************************************************************************* */
/* mymonth */

data mymonth; set ccm4;
keep mdate permno myind25;
run;

/* proc export data=mymonth outfile="mymonth.csv" */
/* dbms=csv replace; */

/*************** Part 5: Calculating Fama-French Factors  **************/
/* Calculate monthly time series of weighted average portfolio returns */
proc sort data=ccm4; by date mport; run;

/*proc export data=ccm4 outfile="ccm4.csv" */
/*dbms=csv replace;run; */

proc means data=ccm4 noprint;
 where weight_port>0 and exchcd in (1,2,3)
      and shrcd in (10,11);
 by date mport;
 var retadj;
 weight weight_port;
 output out=vwret (drop= _type_ _freq_ ) mean=vwret n=n_firms;
run;

proc export data=vwret outfile="vwret_6_MOM1M.csv"
dbms=csv replace;

/* Monthly Factor Returns: SMB and HML */
proc transpose data=vwret(keep=date mport vwret)
 out=vwret2 (drop=_name_ _label_);
 by date ;
 ID mport;
 Var vwret;
run;

proc export data=vwret2 outfile="vwret2_10_MOM1M.csv"
dbms=csv replace;

/* Number of Firms */
proc transpose data=vwret(keep=date mport n_firms)
               out=vwret3 (drop=_name_ _label_) prefix=n_;
by date ;
ID mport;
Var n_firms;
run;

proc export data=vwret3 outfile="vwret3_10_MOM1M.csv"
dbms=csv replace;

proc sort data=mymonth nodupkey out=&table_out;
where "&begdate"d<=mdate<="&enddate"d;
by mdate permno;run;

%mend DIVIDE_PORTFOLIO_10_MOM1M;



%MACRO DIVIDE_PORTFOLIO_10_MOM12M (begdate=, enddate=, table_out=);

/************************ Part 1: Compustat ****************************/
/* Compustat XpressFeed Variables:                                     */
/* AT      = Total Assets                                              */
/* PSTKL   = Preferred Stock Liquidating Value                         */
/* TXDITC  = Deferred Taxes and Investment Tax Credit                  */
/* PSTKRV  = Preferred Stock Redemption Value                          */
/* seq     = Common/Ordinary Equity - Total                            */
/* PSTK    = Preferred/Preference Stock (Capital) - Total              */

/* In calculating Book Equity, incorporate Preferred Stock (PS) values */
/*  use the redemption value of PS, or the liquidation value           */
/*    or the par value (in that order) (FF,JFE, 1993, p. 8)            */
/* USe Balance Sheet Deferred Taxes TXDITC if available                */
/* Flag for number of years in Compustat (<2 likely backfilled data)   */

%let vars = AT PSTKL TXDITC PSTKRV seq PSTK ;
data comp;
  set comp.funda
  (keep= gvkey datadate &vars indfmt datafmt popsrc consol);
  by gvkey datadate;
  where indfmt='INDL' and datafmt='STD' and popsrc='D' and consol='C'
  and datadate >='01Jan1900'd;
 /* Two years of accounting data before 1962 */
  PS = coalesce(PSTKRV,PSTKL,PSTK,0);
  if missing(TXDITC) then TXDITC = 0 ;
  BE = seq + TXDITC - PS ;
  if BE<0 then BE=.;
  year = year(datadate);
  label BE='Book Value of Equity FYear t-1' ;
  drop indfmt datafmt popsrc consol ps &vars;
  retain count;
  if first.gvkey then count=1;
  else count = count+1;
run;

/************************ Part 2: CRSP **********************************/
/* Create a CRSP Subsample with Monthly Stock and Event Variables       */
/* This procedure creates a SAS dataset named "CRSP_M"                  */
/* Restrictions will be applied later                                   */
/* Select variables from the CRSP monthly stock and event datasets      */
%let msevars=ticker ncusip shrcd exchcd;
%let msfvars =  prc ret retx shrout cfacpr cfacshr;

%include '/wrds/crsp/samples/crspmerge.sas';

%crspmerge(s=m,start=01jan1900,end=31dec2018,
sfvars=&msfvars,sevars=&msevars,filters=exchcd in (1,2,3));

/* CRSP_M is sorted by date and permno and has historical returns     */
/* as well as historical share codes and exchange codes               */
/* Add CRSP delisting returns */
proc sql; create table crspm2
 as select a.*, b.dlret,
  sum(1,ret)*sum(1,dlret)-1 as retadj "Return adjusted for delisting",
  abs(a.prc)*a.shrout as MEq 'Market Value of Equity'
 from Crsp_m a left join crsp.msedelist(where=(missing(dlret)=0)) b
 on a.permno=b.permno and
    intnx('month',a.date,0,'E')=intnx('month',b.DLSTDT,0,'E')
 order by a.date, a.permco, MEq;
quit;

data crspm2; set crspm2;
if ret=. then ret=-99;
run;

/* There are cases when the same firm (permco) has two or more         */
/* securities (permno) at same date. For the purpose of ME for         */
/* the firm, we aggregated all ME for a given permco, date. This       */
/* aggregated ME will be assigned to the Permno with the largest ME    */
data crspm2a (drop = Meq); set crspm2;
  by date permco Meq;
  retain ME;
  if first.permco and last.permco then do;
    ME=meq;
  output; /* most common case where a firm has a unique permno*/
  end;
  else do ;
    if  first.permco then ME=meq;
    else ME=sum(meq,ME);
    if last.permco then output;
  end;
run;

/* There should be no duplicates*/
proc sort data=crspm2a nodupkey; by permno date;run;

/* add mom12m */
data crspm2a; set crspm2a;
by permno date;
mom1m = ret;
if first.permno then mom1m=.;
/* this is mom characteristic*/
/* sort then label next month firms */
mom12m=  (   (1+lag1(ret))*(1+lag2(ret))*(1+lag3(ret))*(1+lag4(ret))*(1+lag5(ret))*(1+lag6(ret))*
	(1+lag7(ret))*(1+lag8(ret))*(1+lag9(ret))*(1+lag10(ret))*(1+lag11(ret))   ) - 1;
if permno ne lag11(permno) then mom12m=.;
/* we have ME already in about 10 lines before*/

/* ********************************************************************************* */
/* we can add more filtering here, refer to French website */
run;
/* end mom12m */

/* proc export data=crspm2a outfile="crspm2a.csv" */
/* dbms=csv replace;run; */

/* The next step does 2 things:                                        */
/* - Create weights for later calculation of VW returns.               */
/*   Each firm's monthly return RET t willl be weighted by             */
/*   ME(t-1) = ME(t-2) * (1 + RETX (t-1))                              */
/*     where RETX is the without-dividend return.                      */
/* - Create a File with December t-1 Market Equity (ME)                */
data crspm3 (keep=permno date retadj weight_port ME mom12m exchcd shrcd cumretx)
decme (keep = permno date ME rename=(me=DEC_ME) )  ;
     set crspm2a;
 by permno date;
 retain weight_port cumretx me_base;
 Lpermno=lag(permno);
 LME=lag(me);
     if first.permno then do;
     LME=me/(1+retx); cumretx=sum(1,retx); me_base=LME;weight_port=.;end;
     else do;
     if month(date)=7 then do;
        weight_port= LME;
        me_base=LME; /* lag ME also at the end of June */
        cumretx=sum(1,retx);
     end;
     else do;
        if LME>0 then weight_port=cumretx*me_base;
        else weight_port=.;
        cumretx=cumretx*sum(1,retx);
     end; end;
output crspm3;
if month(date)=12 and ME>0 then output decme;
run;

/* Create a file with data for each June with ME from previous December */
proc sql;
  create table crspjune as
  select a.*, b.DEC_ME
  from crspm3 (where=(month(date)=6)) as a, decme as b
  where a.permno=b.permno and
  intck('month',b.date,a.date)=6;
quit;

/***************   Part 3: Merging CRSP and Compustat ***********/
/* Add Permno to Compustat sample */
proc sql;
  create table ccm1 as
  select a.*, b.lpermno as permno, b.linkprim
  from comp as a, crsp.ccmxpf_linktable as b
  where a.gvkey=b.gvkey
  and substr(b.linktype,1,1)='L' and linkprim in ('P','C')
  and (intnx('month',intnx('year',a.datadate,0,'E'),6,'E') >= b.linkdt)
  and (b.linkenddt >= intnx('month',intnx('year',a.datadate,0,'E'),6,'E')
  or missing(b.linkenddt))
  order by a.datadate, permno, b.linkprim desc;
quit;

/*  Cleaning Compustat Data for no relevant duplicates                      */
/*  Eliminating overlapping matching : few cases where different gvkeys     */
/*  for same permno-date --- some of them are not 'primary' matches in CCM  */
/*  Use linkprim='P' for selecting just one gvkey-permno-date combination   */
data ccm1a; set ccm1;
  by datadate permno descending linkprim;
  if first.permno;
run;

/* Sanity Check -- No Duplicates */
proc sort data=ccm1a nodupkey; by permno year datadate; run;

/* 2. However, there other type of duplicates within the year                */
/* Some companiess change fiscal year end in the middle of the calendar year */
/* In these cases, there are more than one annual record for accounting data */
/* We will be selecting the last annual record in a given calendar year      */
data ccm2a ; set ccm1a;
  by permno year datadate;
  if last.year;
run;

/* Sanity Check -- No Duplicates */
proc sort data=ccm2a nodupkey; by permno datadate; run;

/* Finalize Compustat Sample                              */
/* Merge CRSP with Compustat data, at June of every year  */
/* Match fiscal year ending calendar year t-1 with June t */
proc sql; create table ccm2_june as
  select a.*, b.BE, (1000*b.BE)/a.DEC_ME as BEME, b.count,
  b.datadate,
  intck('month',b.datadate, a.date) as dist
  from crspjune a, ccm2a b
  where a.permno=b.permno and intnx('month',a.date,0,'E')=
  intnx('month',intnx('year',b.datadate,0,'E'),6,'E')
  order by a.date;
quit;

/* proc export data=ccm2_june outfile="ccm2_june.csv" */
/* dbms=csv replace;run; */

/* ********************************************************************************* */
/* ******************* CREATE MONTHLY BIG CCM_CRSP ********************************* */
/*  ccm2_june comes from original script                                             */
/*  merge crspm2a with ccm2_june                                                     */
/*  BE information of June paste to crspm2a for July year t to June year t+1         */
/*                                                                                   */
/*                                                                                   */
/*                                                                                   */
proc sql;
create table ccm_month as
select a.*, b.count, b.beme, b.date as june_date, b.datadate as annual_info_date from
crspm2a a left join ccm2_june b
on
a.permno = b.permno
and 1<=intck('month', intnx('month',b.date,0,'E'),  intnx('month',a.date,0,'E') )<=12
order by a.permno, a.date;
quit;

/* proc export data=ccm_month outfile="ccm_month.csv" */
/* dbms=csv replace;run; */

/************************ Part 4: Size and Book to Market Portfolios ***/
/* Forming Portolio by ME and BEME as of each June t                   */
/* Calculate NYSE Breakpoints for Market Equity (ME) and               */
/* Book-to-Market (BEME)                                               */
proc sort data=ccm_month; by date permno; run;

proc univariate data=ccm_month noprint;
  where exchcd=1 and shrcd in (10,11);
  var mom12m;
  by date;
  output out=nyse_breaks pctlpre=mom12m pctlpts= 10 to 90 by 10; /* HX ADD */
run;

/* proc export data=nyse_breaks outfile="nyse_breaks.csv" */
/* dbms=csv replace;run; */

/* Use Breakpoints to classify stock only at end of all June's */
/* HX MODI */
proc sql;
  create table ccm3 as
  select a.*,
  b.mom12m10, b.mom12m20,b.mom12m30,b.mom12m40,b.mom12m50,b.mom12m60,b.mom12m70,b.mom12m80,b.mom12m90
  from ccm_month as a, nyse_breaks as b
  where a.date=b.date;
quit;

/* Create portfolios as of June                       */
/* SIZE Portfolios          : S[mall] or B[ig]        */
/* Book-to-market Portfolios: L[ow], M[edium], H[igh] */
/* HX MODI  10 times 10*/
data all_month; set ccm3;
 If me>0 and count>=1 then do;
 positiveme = 1;
 if  mom12m <= mom12m10 then           momport = 'MOM12M0' ;
 else if mom12m10 < mom12m <= mom12m20 then momport = 'MOM12M1' ;
 else if mom12m20 < mom12m <= mom12m30 then momport = 'MOM12M2' ;
 else if mom12m30 < mom12m <= mom12m40 then momport = 'MOM12M3' ;
 else if mom12m40 < mom12m <= mom12m50 then momport = 'MOM12M4' ;
 else if mom12m50 < mom12m <= mom12m60 then momport = 'MOM12M5' ;
 else if mom12m60 < mom12m <= mom12m70 then momport = 'MOM12M6' ;
 else if mom12m70 < mom12m <= mom12m80 then momport = 'MOM12M7' ;
 else if mom12m80 < mom12m <= mom12m90 then momport = 'MOM12M8' ;
 else if mom12m  > mom12m90 then          momport = 'MOM12M9' ;
 else momport='';
 end;
 else positiveme = 0;
if cmiss(momport)=0 then nonmissport=1; else nonmissport=0;
keep permno date retadj momport exchcd shrcd nonmissport me;
run;

/* HX ADD */
data all_month; set all_month;
myind = catx('_', momport);
run;

/* ********************************************************************************* */
/* lag port labels one month */
proc sort data=all_month; by permno date; run;

data ccm4; set all_month;
by permno date;
mdate = intnx('month', date,0,'E');
format mdate worddatx19.;
mport = lag(momport);
weight_port = lag(me);
myind25 = lag(myind);
if first.permno then mport=.;
if first.permno then weight_port=.;
if first.permno then myind25=.;
run;

/* ********************************************************************************* */
/* mymonth */

data mymonth; set ccm4;
keep mdate permno myind25;
run;

/* proc export data=mymonth outfile="mymonth.csv" */
/* dbms=csv replace; */

/*************** Part 5: Calculating Fama-French Factors  **************/
/* Calculate monthly time series of weighted average portfolio returns */
proc sort data=ccm4; by date mport; run;

proc means data=ccm4 noprint;
 where weight_port>0 and exchcd in (1,2,3)
      and shrcd in (10,11) and nonmissport=1;
 by date mport;
 var retadj;
 weight weight_port;
 output out=vwret (drop= _type_ _freq_ ) mean=vwret n=n_firms;
run;

/*proc export data=vwret outfile="vwret_6_MOM12M.csv"*/
/*dbms=csv replace;*/

/* Monthly Factor Returns: SMB and HML */
proc transpose data=vwret(keep=date mport vwret)
 out=vwret2 (drop=_name_ _label_);
 by date ;
 ID mport;
 Var vwret;
run;

proc export data=vwret2 outfile="vwret2_10_MOM12M.csv"
dbms=csv replace;

/* Number of Firms */
proc transpose data=vwret(keep=date mport n_firms)
               out=vwret3 (drop=_name_ _label_) prefix=n_;
by date ;
ID mport;
Var n_firms;
run;

proc export data=vwret3 outfile="vwret3_10_MOM12M.csv"
dbms=csv replace;

proc sort data=mymonth nodupkey out=&table_out;
where "&begdate"d<=mdate<="&enddate"d;
by mdate permno;run;

%mend DIVIDE_PORTFOLIO_10_MOM12M;



%MACRO DIVIDE_PORTFOLIO_10_MOM60M (begdate=, enddate=, table_out=);


/************************ Part 1: Compustat ****************************/
/* Compustat XpressFeed Variables:                                     */
/* AT      = Total Assets                                              */
/* PSTKL   = Preferred Stock Liquidating Value                         */
/* TXDITC  = Deferred Taxes and Investment Tax Credit                  */
/* PSTKRV  = Preferred Stock Redemption Value                          */
/* seq     = Common/Ordinary Equity - Total                            */
/* PSTK    = Preferred/Preference Stock (Capital) - Total              */

/* In calculating Book Equity, incorporate Preferred Stock (PS) values */
/*  use the redemption value of PS, or the liquidation value           */
/*    or the par value (in that order) (FF,JFE, 1993, p. 8)            */
/* USe Balance Sheet Deferred Taxes TXDITC if available                */
/* Flag for number of years in Compustat (<2 likely backfilled data)   */

%let vars = AT PSTKL TXDITC PSTKRV seq PSTK ;
data comp;
  set comp.funda
  (keep= gvkey datadate &vars indfmt datafmt popsrc consol);
  by gvkey datadate;
  where indfmt='INDL' and datafmt='STD' and popsrc='D' and consol='C'
  and datadate >='01Jan1900'd;
 /* Two years of accounting data before 1962 */
  PS = coalesce(PSTKRV,PSTKL,PSTK,0);
  if missing(TXDITC) then TXDITC = 0 ;
  BE = seq + TXDITC - PS ;
  if BE<0 then BE=.;
  year = year(datadate);
  label BE='Book Value of Equity FYear t-1' ;
  drop indfmt datafmt popsrc consol ps &vars;
  retain count;
  if first.gvkey then count=1;
  else count = count+1;
run;

/************************ Part 2: CRSP **********************************/
/* Create a CRSP Subsample with Monthly Stock and Event Variables       */
/* This procedure creates a SAS dataset named "CRSP_M"                  */
/* Restrictions will be applied later                                   */
/* Select variables from the CRSP monthly stock and event datasets      */
%let msevars=ticker ncusip shrcd exchcd;
%let msfvars =  prc ret retx shrout cfacpr cfacshr;

%include '/wrds/crsp/samples/crspmerge.sas';

%crspmerge(s=m,start=01jan1900,end=31dec2018,
sfvars=&msfvars,sevars=&msevars,filters=exchcd in (1,2,3));

/* CRSP_M is sorted by date and permno and has historical returns     */
/* as well as historical share codes and exchange codes               */
/* Add CRSP delisting returns */
proc sql; create table crspm2
 as select a.*, b.dlret,
  sum(1,ret)*sum(1,dlret)-1 as retadj "Return adjusted for delisting",
  abs(a.prc)*a.shrout as MEq 'Market Value of Equity'
 from Crsp_m a left join crsp.msedelist(where=(missing(dlret)=0)) b
 on a.permno=b.permno and
    intnx('month',a.date,0,'E')=intnx('month',b.DLSTDT,0,'E')
 order by a.date, a.permco, MEq;
quit;

data crspm2; set crspm2;
if ret=. then ret=-99;
run;

/* There are cases when the same firm (permco) has two or more         */
/* securities (permno) at same date. For the purpose of ME for         */
/* the firm, we aggregated all ME for a given permco, date. This       */
/* aggregated ME will be assigned to the Permno with the largest ME    */
data crspm2a (drop = Meq); set crspm2;
  by date permco Meq;
  retain ME;
  if first.permco and last.permco then do;
    ME=meq;
  output; /* most common case where a firm has a unique permno*/
  end;
  else do ;
    if  first.permco then ME=meq;
    else ME=sum(meq,ME);
    if last.permco then output;
  end;
run;

/* There should be no duplicates*/
proc sort data=crspm2a nodupkey; by permno date;run;

/* add mom12m */
data crspm2a; set crspm2a;
by permno date;
mom1m = ret;
if first.permno then mom1m=.;
/* this is mom characteristic*/
/* sort then label next month firms */
mom12m=  (   (1+lag1(ret))*(1+lag2(ret))*(1+lag3(ret))*(1+lag4(ret))*(1+lag5(ret))*(1+lag6(ret))*
	(1+lag7(ret))*(1+lag8(ret))*(1+lag9(ret))*(1+lag10(ret))*(1+lag11(ret))   ) - 1;
if permno ne lag11(permno) then mom12m=.;
mom60m=(   (1+lag12(ret))*(1+lag13(ret))*(1+lag14(ret))*(1+lag15(ret))*(1+lag16(ret))*(1+lag17(ret))*(1+lag18(ret))   *
  (1+lag19(ret))*(1+lag20(ret))*(1+lag21(ret))*(1+lag22(ret))*(1+lag23(ret))*(1+lag24(ret))*
  (1+lag25(ret))*(1+lag26(ret))*(1+lag27(ret))*(1+lag28(ret))*(1+lag29(ret))*(1+lag30(ret))     *
  (1+lag31(ret))*(1+lag32(ret))*(1+lag33(ret))*(1+lag34(ret))*(1+lag35(ret))*(1+lag36(ret))     *
  (1+lag37(ret))*(1+lag38(ret))*(1+lag39(ret))*(1+lag40(ret))     *
  (1+lag41(ret))*(1+lag42(ret))*(1+lag43(ret))*(1+lag44(ret))*(1+lag45(ret))*(1+lag46(ret))     *
  (1+lag47(ret))*(1+lag48(ret))*(1+lag49(ret))*(1+lag50(ret))     *
  (1+lag51(ret))*(1+lag52(ret))*(1+lag53(ret))*(1+lag54(ret))*(1+lag55(ret))*(1+lag56(ret))     *
  (1+lag57(ret))*(1+lag58(ret))*(1+lag59(ret))
  ) - 1;
if permno ne lag59(permno) then mom60m=.;
/* we have ME already in about 10 lines before*/

/* ********************************************************************************* */
/* we can add more filtering here, refer to French website */
run;
/* end mom12m */

/* proc export data=crspm2a outfile="crspm2a.csv" */
/* dbms=csv replace;run; */

/* The next step does 2 things:                                        */
/* - Create weights for later calculation of VW returns.               */
/*   Each firm's monthly return RET t willl be weighted by             */
/*   ME(t-1) = ME(t-2) * (1 + RETX (t-1))                              */
/*     where RETX is the without-dividend return.                      */
/* - Create a File with December t-1 Market Equity (ME)                */
data crspm3 (keep=permno date retadj weight_port ME mom60m mom12m exchcd shrcd cumretx)
decme (keep = permno date ME rename=(me=DEC_ME) )  ;
     set crspm2a;
 by permno date;
 retain weight_port cumretx me_base;
 Lpermno=lag(permno);
 LME=lag(me);
     if first.permno then do;
     LME=me/(1+retx); cumretx=sum(1,retx); me_base=LME;weight_port=.;end;
     else do;
     if month(date)=7 then do;
        weight_port= LME;
        me_base=LME; /* lag ME also at the end of June */
        cumretx=sum(1,retx);
     end;
     else do;
        if LME>0 then weight_port=cumretx*me_base;
        else weight_port=.;
        cumretx=cumretx*sum(1,retx);
     end; end;
output crspm3;
if month(date)=12 and ME>0 then output decme;
run;

/* Create a file with data for each June with ME from previous December */
proc sql;
  create table crspjune as
  select a.*, b.DEC_ME
  from crspm3 (where=(month(date)=6)) as a, decme as b
  where a.permno=b.permno and
  intck('month',b.date,a.date)=6;
quit;

/***************   Part 3: Merging CRSP and Compustat ***********/
/* Add Permno to Compustat sample */
proc sql;
  create table ccm1 as
  select a.*, b.lpermno as permno, b.linkprim
  from comp as a, crsp.ccmxpf_linktable as b
  where a.gvkey=b.gvkey
  and substr(b.linktype,1,1)='L' and linkprim in ('P','C')
  and (intnx('month',intnx('year',a.datadate,0,'E'),6,'E') >= b.linkdt)
  and (b.linkenddt >= intnx('month',intnx('year',a.datadate,0,'E'),6,'E')
  or missing(b.linkenddt))
  order by a.datadate, permno, b.linkprim desc;
quit;

/*  Cleaning Compustat Data for no relevant duplicates                      */
/*  Eliminating overlapping matching : few cases where different gvkeys     */
/*  for same permno-date --- some of them are not 'primary' matches in CCM  */
/*  Use linkprim='P' for selecting just one gvkey-permno-date combination   */
data ccm1a; set ccm1;
  by datadate permno descending linkprim;
  if first.permno;
run;

/* Sanity Check -- No Duplicates */
proc sort data=ccm1a nodupkey; by permno year datadate; run;

/* 2. However, there other type of duplicates within the year                */
/* Some companiess change fiscal year end in the middle of the calendar year */
/* In these cases, there are more than one annual record for accounting data */
/* We will be selecting the last annual record in a given calendar year      */
data ccm2a ; set ccm1a;
  by permno year datadate;
  if last.year;
run;

/* Sanity Check -- No Duplicates */
proc sort data=ccm2a nodupkey; by permno datadate; run;

/* Finalize Compustat Sample                              */
/* Merge CRSP with Compustat data, at June of every year  */
/* Match fiscal year ending calendar year t-1 with June t */
proc sql; create table ccm2_june as
  select a.*, b.BE, (1000*b.BE)/a.DEC_ME as BEME, b.count,
  b.datadate,
  intck('month',b.datadate, a.date) as dist
  from crspjune a, ccm2a b
  where a.permno=b.permno and intnx('month',a.date,0,'E')=
  intnx('month',intnx('year',b.datadate,0,'E'),6,'E')
  order by a.date;
quit;

/* proc export data=ccm2_june outfile="ccm2_june.csv" */
/* dbms=csv replace;run; */

/* ********************************************************************************* */
/* ******************* CREATE MONTHLY BIG CCM_CRSP ********************************* */
/*  ccm2_june comes from original script                                             */
/*  merge crspm2a with ccm2_june                                                     */
/*  BE information of June paste to crspm2a for July year t to June year t+1         */
/*                                                                                   */
/*                                                                                   */
/*                                                                                   */
proc sql;
create table ccm_month as
select a.*, b.count, b.beme, b.date as june_date, b.datadate as annual_info_date from
crspm2a a left join ccm2_june b
on
a.permno = b.permno
and 1<=intck('month', intnx('month',b.date,0,'E'),  intnx('month',a.date,0,'E') )<=12
order by a.permno, a.date;
quit;

/* proc export data=ccm_month outfile="ccm_month.csv" */
/* dbms=csv replace;run; */

/************************ Part 4: Size and Book to Market Portfolios ***/
/* Forming Portolio by ME and BEME as of each June t                   */
/* Calculate NYSE Breakpoints for Market Equity (ME) and               */
/* Book-to-Market (BEME)                                               */
proc sort data=ccm_month; by date permno; run;

proc univariate data=ccm_month noprint;
  where exchcd=1 and shrcd in (10,11);
  var mom60m;
  by date;
  output out=nyse_breaks pctlpre=mom60m pctlpts= 10 to 90 by 10; /* HX ADD */
run;

/* proc export data=nyse_breaks outfile="nyse_breaks.csv" */
/* dbms=csv replace;run; */

/* Use Breakpoints to classify stock only at end of all June's */
/* HX MODI */
proc sql;
  create table ccm3 as
  select a.*,
  b.mom60m10, b.mom60m20,b.mom60m30,b.mom60m40,b.mom60m50,b.mom60m60,b.mom60m70,b.mom60m80,b.mom60m90
  from ccm_month as a, nyse_breaks as b
  where a.date=b.date;
quit;

/* Create portfolios as of June                       */
/* SIZE Portfolios          : S[mall] or B[ig]        */
/* Book-to-market Portfolios: L[ow], M[edium], H[igh] */
/* HX MODI  10 times 10*/
data all_month; set ccm3;
 If me>0 then do;
 positiveme = 1;
 if mom60m <= mom60m10 then           momport = 'MOM60M0' ;
 else if mom60m10 < mom60m <= mom60m20 then momport = 'MOM60M1' ;
 else if mom60m20 < mom60m <= mom60m30 then momport = 'MOM60M2' ;
 else if mom60m30 < mom60m <= mom60m40 then momport = 'MOM60M3' ;
 else if mom60m40 < mom60m <= mom60m50 then momport = 'MOM60M4' ;
 else if mom60m50 < mom60m <= mom60m60 then momport = 'MOM60M5' ;
 else if mom60m60 < mom60m <= mom60m70 then momport = 'MOM60M6' ;
 else if mom60m70 < mom60m <= mom60m80 then momport = 'MOM60M7' ;
 else if mom60m80 < mom60m <= mom60m90 then momport = 'MOM60M8' ;
 else if mom60m  > mom60m90 then          momport = 'MOM60M9' ;
 else momport='';
 end;
 else positiveme=0;
if cmiss(momport)=0 then nonmissport=1; else nonmissport=0;
keep permno date retadj momport exchcd shrcd nonmissport me;
run;

/* HX ADD */
data all_month; set all_month;
myind = catx('_', momport);
run;

/* ********************************************************************************* */
/* lag port labels one month */
proc sort data=all_month; by permno date; run;

data ccm4; set all_month;
by permno date;
mdate = intnx('month', date,0,'E');
format mdate worddatx19.;
mport = lag(momport);
weight_port = lag(me);
myind25 = lag(myind);
if first.permno then mport=.;
if first.permno then weight_port=.;
if first.permno then myind25=.;
run;

/* ********************************************************************************* */
/* mymonth */

data mymonth; set ccm4;
keep mdate permno myind25;
run;

/* proc export data=mymonth outfile="mymonth.csv" */
/* dbms=csv replace; */

/*************** Part 5: Calculating Fama-French Factors  **************/
/* Calculate monthly time series of weighted average portfolio returns */
proc sort data=ccm4; by date mport; run;

proc means data=ccm4 noprint;
 where weight_port>0 and exchcd in (1,2,3)
      and shrcd in (10,11) and nonmissport=1;
 by date mport;
 var retadj;
 weight weight_port;
 output out=vwret (drop= _type_ _freq_ ) mean=vwret n=n_firms;
run;

/*proc export data=vwret outfile="vwret_6_MOM60M.csv"*/
/*dbms=csv replace;*/

/* Monthly Factor Returns: SMB and HML */
proc transpose data=vwret(keep=date mport vwret)
 out=vwret2 (drop=_name_ _label_);
 by date ;
 ID mport;
 Var vwret;
run;

proc export data=vwret2 outfile="vwret2_10_MOM60M.csv"
dbms=csv replace;

/* Number of Firms */
proc transpose data=vwret(keep=date mport n_firms)
               out=vwret3 (drop=_name_ _label_) prefix=n_;
by date ;
ID mport;
Var n_firms;
run;

proc export data=vwret3 outfile="vwret3_10_MOM60M.csv"
dbms=csv replace;

proc sort data=mymonth nodupkey out=&table_out;
where "&begdate"d<=mdate<="&enddate"d;
by mdate permno;run;

%mend DIVIDE_PORTFOLIO_10_MOM60M;


%MACRO DIVIDE_PORTFOLIO_10_NI (begdate=, enddate=, table_out=);

/************************ Part 1: Compustat ****************************/
/* Compustat XpressFeed Variables:                                     */
/* AT      = Total Assets                                              */
/* PSTKL   = Preferred Stock Liquidating Value                         */
/* TXDITC  = Deferred Taxes and Investment Tax Credit                  */
/* PSTKRV  = Preferred Stock Redemption Value                          */
/* seq     =                                   */
/* PSTK    = Preferred/Preference Stock (Capital) - Total              */

/* In calculating Book Equity, incorporate Preferred Stock (PS) values */
/*  use the redemption value of PS, or the liquidation value           */
/*    or the par value (in that order) (FF,JFE, 1993, p. 8)            */
/* USe Balance Sheet Deferred Taxes TXDITC if available                */
/* Flag for number of years in Compustat (<2 likely backfilled data)   */

%let vars = AT PSTKL TXDITC PSTKRV seq PSTK CSHO AJEX;
data comp;
  set comp.funda
  (keep= gvkey datadate &vars indfmt datafmt popsrc consol);
  by gvkey datadate;
  where indfmt='INDL' and datafmt='STD' and popsrc='D' and consol='C'
  and datadate >='01Jan1900'd;
 /* Two years of accounting data before 1962 */
  PS = coalesce(PSTKRV,PSTKL,PSTK,0);
  if missing(TXDITC) then TXDITC = 0 ;
  BE = seq + TXDITC - PS ;
  if BE<=0 then BE=.;
  year = year(datadate);
  label BE='Book Value of Equity FYear t-1' ;
  NI=log(CSHO*AJEX)-log(lag(CSHO)*lag(AJEX));
  if missing(CSHO) then delete;
  if NI<=0 then delete;
  drop indfmt datafmt popsrc consol ps &vars;
  retain count;
  if first.gvkey then count=1;
  else count = count+1;
run;

/************************ Part 2: CRSP **********************************/
/* Create a CRSP Subsample with Monthly Stock and Event Variables       */
/* This procedure creates a SAS dataset named "CRSP_M"                  */
/* Restrictions will be applied later                                   */
/* Select variables from the CRSP monthly stock and event datasets      */
%let msevars=ticker ncusip shrcd exchcd;
%let msfvars =  prc ret retx shrout cfacpr cfacshr;

%include '/wrds/crsp/samples/crspmerge.sas';

%crspmerge(s=m,start=01jan1900,end=31dec2018,
sfvars=&msfvars,sevars=&msevars,filters=exchcd in (1,2,3));

/* CRSP_M is sorted by date and permno and has historical returns     */
/* as well as historical share codes and exchange codes               */
/* Add CRSP delisting returns */
proc sql; create table crspm2
 as select a.*, b.dlret,
  sum(1,ret)*sum(1,dlret)-1 as retadj "Return adjusted for delisting",
  abs(a.prc)*a.shrout as MEq 'Market Value of Equity'
 from Crsp_m a left join crsp.msedelist(where=(missing(dlret)=0)) b
 on a.permno=b.permno and
    intnx('month',a.date,0,'E')=intnx('month',b.DLSTDT,0,'E')
 order by a.date, a.permco, MEq;
quit;

/* There are cases when the same firm (permco) has two or more         */
/* securities (permno) at same date. For the purpose of ME for         */
/* the firm, we aggregated all ME for a given permco, date. This       */
/* aggregated ME will be assigned to the Permno with the largest ME    */
data crspm2a (drop = Meq); set crspm2;
  by date permco Meq;
  retain ME;
  if first.permco and last.permco then do;
    ME=meq;
  output; /* most common case where a firm has a unique permno*/
  end;
  else do ;
    if  first.permco then ME=meq;
    else ME=sum(meq,ME);
    if last.permco then output;
  end;
run;

/* There should be no duplicates*/
proc sort data=crspm2a nodupkey; by permno date;run;

/* The next step does 2 things:                                        */
/* - Create weights for later calculation of VW returns.               */
/*   Each firm's monthly return RET t willl be weighted by             */
/*   ME(t-1) = ME(t-2) * (1 + RETX (t-1))                              */
/*     where RETX is the without-dividend return.                      */
/* - Create a File with December t-1 Market Equity (ME)                */
data crspm3 (keep=permno date retadj weight_port ME exchcd shrcd cumretx)
decme (keep = permno date ME rename=(me=DEC_ME) )  ;
     set crspm2a;
 by permno date;
 retain weight_port cumretx me_base;
 Lpermno=lag(permno);
 LME=lag(me);
     if first.permno then do;
     LME=me/(1+retx); cumretx=sum(1,retx); me_base=LME;weight_port=.;end;
     else do;
     if month(date)=7 then do;
        weight_port= LME;
        me_base=LME; /* lag ME also at the end of June */
        cumretx=sum(1,retx);
     end;
     else do;
        if LME>0 then weight_port=cumretx*me_base;
        else weight_port=.;
        cumretx=cumretx*sum(1,retx);
     end; end;
output crspm3;
if month(date)=12 and ME>0 then output decme;
run;

/* Create a file with data for each June with ME from previous December */
proc sql;
  create table crspjune as
  select a.*, b.DEC_ME
  from crspm3 (where=(month(date)=6)) as a, decme as b
  where a.permno=b.permno and
  intck('month',b.date,a.date)=6;
quit;

/***************   Part 3: Merging CRSP and Compustat ***********/
/* Add Permno to Compustat sample */
proc sql;
  create table ccm1 as
  select a.*, b.lpermno as permno, b.linkprim
  from comp as a, crsp.ccmxpf_linktable as b
  where a.gvkey=b.gvkey
  and substr(b.linktype,1,1)='L' and linkprim in ('P','C')
  and (intnx('month',intnx('year',a.datadate,0,'E'),6,'E') >= b.linkdt)
  and (b.linkenddt >= intnx('month',intnx('year',a.datadate,0,'E'),6,'E')
  or missing(b.linkenddt))
  order by a.datadate, permno, b.linkprim desc;
quit;

/*  Cleaning Compustat Data for no relevant duplicates                      */
/*  Eliminating overlapping matching : few cases where different gvkeys     */
/*  for same permno-date --- some of them are not 'primary' matches in CCM  */
/*  Use linkprim='P' for selecting just one gvkey-permno-date combination   */
data ccm1a; set ccm1;
  by datadate permno descending linkprim;
  if first.permno;
run;

/* Sanity Check -- No Duplicates */
proc sort data=ccm1a nodupkey; by permno year datadate; run;

/* 2. However, there other type of duplicates within the year                */
/* Some companiess change fiscal year end in the middle of the calendar year */
/* In these cases, there are more than one annual record for accounting data */
/* We will be selecting the last annual record in a given calendar year      */
data ccm2a ; set ccm1a;
  by permno year datadate;
  if last.year;
run;

/* Sanity Check -- No Duplicates */
proc sort data=ccm2a nodupkey; by permno datadate; run;

/* Finalize Compustat Sample                              */
/* Merge CRSP with Compustat data, at June of every year  */
/* Match fiscal year ending calendar year t-1 with June t */
proc sql; create table ccm2_june as
  select a.*, b.BE, (1000*b.BE)/a.DEC_ME as BEME, b.NI, b.count,
  b.datadate,
  intck('month',b.datadate, a.date) as dist
  from crspjune a, ccm2a b
  where a.permno=b.permno and intnx('month',a.date,0,'E')=
  intnx('month',intnx('year',b.datadate,0,'E'),6,'E')
  order by a.date;
quit;

/************************ Part 4: Size and Book to Market Portfolios ***/
/* Forming Portolio by ME and BEME as of each June t                   */
/* Calculate NYSE Breakpoints for Market Equity (ME) and               */
/* Book-to-Market (BEME)                                               */
proc univariate data=ccm2_june noprint;
  where exchcd=1 and shrcd in (10,11) and me>0 and count>=2;
  var NI; * ME is Market Equity at the end of June;
  by date; /*at june;*/
  output out=nyse_breaks pctlpre=NI pctlpts= 10 to 90 by 10; /* HX ADD */
run;

proc export data=nyse_breaks outfile="nyse_breaks.csv"
dbms=csv replace;run;

/* Use Breakpoints to classify stock only at end of all June's */
/* HX MODI */
proc sql;
  create table ccm3_june as
  select a.*,
  b.ni10,b.ni20,b.ni30, b.ni40,b.ni50, b.ni60,b.ni70, b.ni80,b.ni90
  from ccm2_june as a, nyse_breaks as b
  where a.date=b.date;
quit;

/* Create portfolios as of June                       */
/* SIZE Portfolios          : S[mall] or B[ig]        */
/* Book-to-market Portfolios: L[ow], M[edium], H[igh] */
/* HX MODI  10 times 10*/
data june ; set ccm3_june;
 If me>0 and count>=2 then do;
 positivebeme=1;

 if ni <= ni10 then           niport = 'NI0' ;
 else if ni10 < ni <= ni20 then niport = 'NI1' ;
 else if ni20 < ni <= ni30 then niport = 'NI2' ;
 else if ni30 < ni <= ni40 then niport = 'NI3' ;
 else if ni40 < ni <= ni50 then niport = 'NI4' ;
 else if ni50 < ni <= ni60 then niport = 'NI5' ;
 else if ni60 < ni <= ni70 then niport = 'NI6' ;
 else if ni70 < ni <= ni80 then niport = 'NI7' ;
 else if ni80 < ni <= ni90 then niport = 'NI8' ;
 else if ni  > ni90 then          niport = 'NI9' ;
 else niport='';
end;
else positivebeme=0;
if cmiss(niport)=0 then nonmissport=1; else nonmissport=0;
keep permno date niport positivebeme exchcd shrcd nonmissport;
run;

/* HX ADD */
data myjune; set june;
myind25 = catx('_', niport);
run;

data myjune; set myjune;
sortdate = intnx('month',date,0,'E');
nextmon = intnx('month',date,1);
nextmonend = intnx('month',nextmon,0,'E');
run;

/* proc export data=myjune outfile="myjune_25.csv" */
/* dbms=csv replace; */

/* populate annual data to monthly */
%populate(inset=myjune, outset=mymonth, datevar=nextmonend, idvar=permno, forward_max=12);

/* Identifying each month the securities of              */
/* Buy and hold June portfolios from July t to June t+1  */
proc sql; create table ccm4 as
 select a.*, b.niport, b.date as portdate format MMDDYYD10.,
        b.positivebeme , b.nonmissport
 from crspm3 as a, june as b
 where a.permno=b.permno and  1 <= intck('month',b.date,a.date) <= 12
 order by date,niport;
quit;

/*************** Part 5: Calculating Fama-French Factors  **************/
/* Calculate monthly time series of weighted average portfolio returns */
proc means data=ccm4 noprint;
 where weight_port>0 and exchcd in (1,2,3)
      and shrcd in (10,11) and nonmissport=1;
 by date niport;
 var retadj;
 weight weight_port;
 output out=vwret (drop= _type_ _freq_ ) mean=vwret n=n_firms;
run;

/*proc export data=vwret outfile="vwret_6_ni2.csv"*/
/*dbms=csv replace;*/

/* Monthly Factor Returns: SMB and HML */
proc transpose data=vwret(keep=date niport vwret)
 out=vwret2 (drop=_name_ _label_);
 by date ;
 ID niport;
 Var vwret;
run;

proc export data=vwret2 outfile="vwret2_10_ni.csv"
dbms=csv replace;

/* Number of Firms */
proc transpose data=vwret(keep=date niport n_firms)
               out=vwret3 (drop=_name_ _label_) prefix=n_;
by date ;
ID niport;
Var n_firms;
run;

proc export data=vwret3 outfile="vwret3_10_ni.csv"
dbms=csv replace;

proc sort data=mymonth nodupkey out=&table_out;
where "&begdate"d<=mdate<="&enddate"d;
by mdate permno;run;

%mend DIVIDE_PORTFOLIO_10_NI;



%MACRO DIVIDE_PORTFOLIO_10_OP (begdate=, enddate=, table_out=);



/************************ Part 1: Compustat ****************************/
/* Compustat XpressFeed Variables:                                     */
/* AT      = Total Assets                                              */
/* PSTKL   = Preferred Stock Liquidating Value                         */
/* TXDITC  = Deferred Taxes and Investment Tax Credit                  */
/* PSTKRV  = Preferred Stock Redemption Value                          */
/* seq     = Common/Ordinary Equity - Total                            */
/* PSTK    = Preferred/Preference Stock (Capital) - Total              */

/* In calculating Book Equity, incorporate Preferred Stock (PS) values */
/*  use the redemption value of PS, or the liquidation value           */
/*    or the par value (in that order) (FF,JFE, 1993, p. 8)            */
/* USe Balance Sheet Deferred Taxes TXDITC if available                */
/* Flag for number of years in Compustat (<2 likely backfilled data)   */

%let vars = AT LT PSTKL TXDITC PSTKRV seq PSTK REVT COGS XSGA XINT;
data comp;
  set comp.funda
  (keep= gvkey datadate &vars indfmt datafmt popsrc consol);
  by gvkey datadate;
  where indfmt='INDL' and datafmt='STD' and popsrc='D' and consol='C'
  and datadate >='01Jan1900'd;

  PS = coalesce(PSTKRV,PSTKL,PSTK,0);

  BE = seq + TXDITC - PS ;

  if BE<=0 then BE=.;
  cogs0 = coalesce(cogs,0);
	xint0 = coalesce(xint,0);
	xsga0 = coalesce(xsga,0);
	OP = (revt-cogs0-xsga0-xint0)/BE;
	if missing(revt) then OP=.;
	if missing(cogs)=1 and missing(xsga)=1 and missing(xint)=1 then OP=.;
	if missing(BE) then OP=.;


  year = year(datadate);

  label BE='Book Value of Equity FYear t-1' ;
  drop indfmt datafmt popsrc consol ps &vars;
  retain count;
  if first.gvkey then count=1;
  else count = count+1;
run;

/************************ Part 2: CRSP **********************************/
/* Create a CRSP Subsample with Monthly Stock and Event Variables       */
/* This procedure creates a SAS dataset named "CRSP_M"                  */
/* Restrictions will be applied later                                   */
/* Select variables from the CRSP monthly stock and event datasets      */
%let msevars=ticker ncusip shrcd exchcd;
%let msfvars =  prc ret retx shrout cfacpr cfacshr;

%include '/wrds/crsp/samples/crspmerge.sas';

%crspmerge(s=m,start=01jan1900,end=31dec2018,
sfvars=&msfvars,sevars=&msevars,filters=exchcd in (1,2,3));

/* CRSP_M is sorted by date and permno and has historical returns     */
/* as well as historical share codes and exchange codes               */
/* Add CRSP delisting returns */
proc sql; create table crspm2
 as select a.*, b.dlret,
  sum(1,ret)*sum(1,dlret)-1 as retadj "Return adjusted for delisting",
  abs(a.prc)*a.shrout as MEq 'Market Value of Equity'
 from Crsp_m a left join crsp.msedelist(where=(missing(dlret)=0)) b
 on a.permno=b.permno and
    intnx('month',a.date,0,'E')=intnx('month',b.DLSTDT,0,'E')
 order by a.date, a.permco, MEq;
quit;

/* There are cases when the same firm (permco) has two or more         */
/* securities (permno) at same date. For the purpose of ME for         */
/* the firm, we aggregated all ME for a given permco, date. This       */
/* aggregated ME will be assigned to the Permno with the largest ME    */
data crspm2a (drop = Meq); set crspm2;
  by date permco Meq;
  retain ME;
  if first.permco and last.permco then do;
    ME=meq;
  output; /* most common case where a firm has a unique permno*/
  end;
  else do ;
    if  first.permco then ME=meq;
    else ME=sum(meq,ME);
    if last.permco then output;
  end;
run;

/* There should be no duplicates*/
proc sort data=crspm2a nodupkey; by permno date;run;

/* The next step does 2 things:                                        */
/* - Create weights for later calculation of VW returns.               */
/*   Each firm's monthly return RET t willl be weighted by             */
/*   ME(t-1) = ME(t-2) * (1 + RETX (t-1))                              */
/*     where RETX is the without-dividend return.                      */
/* - Create a File with December t-1 Market Equity (ME)                */
data crspm3 (keep=permno date retadj weight_port ME exchcd shrcd cumretx)
decme (keep = permno date ME rename=(me=DEC_ME) )  ;
     set crspm2a;
 by permno date;
 retain weight_port cumretx me_base;
 Lpermno=lag(permno);
 LME=lag(me);
     if first.permno then do;
     LME=me/(1+retx); cumretx=sum(1,retx); me_base=LME;weight_port=.;end;
     else do;
     if month(date)=7 then do;
        weight_port= LME;
        me_base=LME; /* lag ME also at the end of June */
        cumretx=sum(1,retx);
     end;
     else do;
        if LME>0 then weight_port=cumretx*me_base;
        else weight_port=.;
        cumretx=cumretx*sum(1,retx);
     end; end;
output crspm3;
if month(date)=12 and ME>0 then output decme;
run;

/* Create a file with data for each June with ME from previous December */
proc sql;
  create table crspjune as
  select a.*, b.DEC_ME
  from crspm3 (where=(month(date)=6)) as a, decme as b
  where a.permno=b.permno and
  intck('month',b.date,a.date)=6;
quit;

/***************   Part 3: Merging CRSP and Compustat ***********/
/* Add Permno to Compustat sample */
proc sql;
  create table ccm1 as
  select a.*, b.lpermno as permno, b.linkprim
  from comp as a, crsp.ccmxpf_linktable as b
  where a.gvkey=b.gvkey
  and substr(b.linktype,1,1)='L' and linkprim in ('P','C')
  and (intnx('month',intnx('year',a.datadate,0,'E'),6,'E') >= b.linkdt)
  and (b.linkenddt >= intnx('month',intnx('year',a.datadate,0,'E'),6,'E')
  or missing(b.linkenddt))
  order by a.datadate, permno, b.linkprim desc;
quit;

/*  Cleaning Compustat Data for no relevant duplicates                      */
/*  Eliminating overlapping matching : few cases where different gvkeys     */
/*  for same permno-date --- some of them are not 'primary' matches in CCM  */
/*  Use linkprim='P' for selecting just one gvkey-permno-date combination   */
data ccm1a; set ccm1;
  by datadate permno descending linkprim;
  if first.permno;
run;

/* Sanity Check -- No Duplicates */
proc sort data=ccm1a nodupkey; by permno year datadate; run;

/* 2. However, there other type of duplicates within the year                */
/* Some companiess change fiscal year end in the middle of the calendar year */
/* In these cases, there are more than one annual record for accounting data */
/* We will be selecting the last annual record in a given calendar year      */
data ccm2a ; set ccm1a;
  by permno year datadate;
  if last.year;
run;

/* Sanity Check -- No Duplicates */
proc sort data=ccm2a nodupkey; by permno datadate; run;

/* Finalize Compustat Sample                              */
/* Merge CRSP with Compustat data, at June of every year  */
/* Match fiscal year ending calendar year t-1 with June t */
proc sql; create table ccm2_june as
  select a.*, b.BE, (1000*b.BE)/a.DEC_ME as BEME, b.OP, b.count,
  b.datadate,
  intck('month',b.datadate, a.date) as dist
  from crspjune a, ccm2a b
  where a.permno=b.permno and intnx('month',a.date,0,'E')=
  intnx('month',intnx('year',b.datadate,0,'E'),6,'E')
  order by a.date;
quit;

/************************ Part 4: Size and Book to Market Portfolios ***/
/* Forming Portolio by ME and BEME as of each June t                   */
/* Calculate NYSE Breakpoints for Market Equity (ME) and               */
/* Book-to-Market (BEME)                                               */
proc univariate data=ccm2_june noprint;
  where exchcd=1 and beme>0 and shrcd in (10,11) and me>0 and count>=2;
  var OP; * ME is Market Equity at the end of June;
  by date; /*at june;*/
  output out=nyse_breaks pctlpre=OP pctlpts= 10 to 90 by 10; /* HX ADD */
run;


/* Use Breakpoints to classify stock only at end of all June's */
/* HX MODI */
proc sql;
  create table ccm3_june as
  select a.*,
  b.op10,b.op20,b.op30, b.op40,b.op50, b.op60,b.op70, b.op80,b.op90
  from ccm2_june as a, nyse_breaks as b
  where a.date=b.date;
quit;

/* Create portfolios as of June                       */
/* SIZE Portfolios          : S[mall] or B[ig]        */
/* Operating Profitability Portfolios: R[obust], M[edium], W[eak] */
/* HX MODI  10 times 10*/
data june ; set ccm3_june;
 If me>0 and count>=2 then do;
 positivebeme=1;
 if             op <= op10 then opport = 'OP0' ;
 else if op10 < op <= op20 then opport = 'OP1' ;
 else if op20 < op <= op30 then opport = 'OP2' ;
 else if op30 < op <= op40 then opport = 'OP3' ;
 else if op40 < op <= op50 then opport = 'OP4' ;
 else if op50 < op <= op60 then opport = 'OP5' ;
 else if op60 < op <= op70 then opport = 'OP6' ;
 else if op70 < op <= op80 then opport = 'OP7' ;
 else if op80 < op <= op90 then opport = 'OP8' ;
 else if        op  > op90 then opport = 'OP9' ;
 else opport='';
end;
else positivebeme=0;
if cmiss(opport)=0 then nonmissport=1; else nonmissport=0;
keep permno date opport positivebeme exchcd shrcd nonmissport;
run;

/* HX ADD */
data myjune; set june;
myind25 = catx('_', opport);
run;

data myjune; set myjune;
sortdate = intnx('month',date,0,'E');
nextmon = intnx('month',date,1);
nextmonend = intnx('month',nextmon,0,'E');
run;

/* proc export data=myjune outfile="myjune_25_op.csv" */
/* dbms=csv replace; */

/* populate annual data to monthly */
%populate(inset=myjune, outset=mymonth, datevar=nextmonend, idvar=permno, forward_max=12);

/* Identifying each month the securities of              */
/* Buy and hold June portfolios from July t to June t+1  */
proc sql; create table ccm4 as
 select a.*, b.opport, b.date as portdate format MMDDYYD10.,
        b.positivebeme , b.nonmissport
 from crspm3 as a, june as b
 where a.permno=b.permno and  1 <= intck('month',b.date,a.date) <= 12
 order by date, opport;
quit;

/*************** Part 5: Calculating Fama-French Factors  **************/
/* Calculate monthly time series of weighted average portfolio returns */
proc means data=ccm4 noprint;
 where weight_port>0 and positivebeme=1 and exchcd in (1,2,3)
      and shrcd in (10,11) and nonmissport=1;
 by date opport;
 var retadj;
 weight weight_port;
 output out=vwret (drop= _type_ _freq_ ) mean=vwret n=n_firms;
run;

/*proc export data=vwret outfile="vwret_6_op.csv"*/
/*dbms=csv replace;*/

/* Monthly Factor Returns: SMB and RMW */
proc transpose data=vwret(keep=date opport vwret)
 out=vwret2 (drop=_name_ _label_);
 by date ;
 ID opport;
 Var vwret;
run;

proc export data=vwret2 outfile="vwret2_10_op.csv"
dbms=csv replace;

/* Number of Firms */
proc transpose data=vwret(keep=date opport n_firms)
               out=vwret3 (drop=_name_ _label_) prefix=n_;
by date ;
ID opport;
Var n_firms;
run;

proc export data=vwret3 outfile="vwret3_10_op.csv"
dbms=csv replace;


proc sort data=mymonth nodupkey out=&table_out;
where "&begdate"d<=mdate<="&enddate"d;
by mdate permno;run;

%mend DIVIDE_PORTFOLIO_10_OP;


%MACRO DIVIDE_PORTFOLIO_10_RVAR (begdate=, enddate=, table_out=);

/************************ Part 1: Compustat ****************************/
/* Compustat XpressFeed Variables:                                     */
/* AT      = Total Assets                                              */
/* PSTKL   = Preferred Stock Liquidating Value                         */
/* TXDITC  = Deferred Taxes and Investment Tax Credit                  */
/* PSTKRV  = Preferred Stock Redemption Value                          */
/* seq     = Common/Ordinary Equity - Total                            */
/* PSTK    = Preferred/Preference Stock (Capital) - Total              */

/* In calculating Book Equity, incorporate Preferred Stock (PS) values */
/*  use the redemption value of PS, or the liquidation value           */
/*    or the par value (in that order) (FF,JFE, 1993, p. 8)            */
/* USe Balance Sheet Deferred Taxes TXDITC if available                */
/* Flag for number of years in Compustat (<2 likely backfilled data)   */

%let vars = AT PSTKL TXDITC PSTKRV seq PSTK ;
data comp;
  set comp.funda
  (keep= gvkey datadate &vars indfmt datafmt popsrc consol);
  by gvkey datadate;
  where indfmt='INDL' and datafmt='STD' and popsrc='D' and consol='C'
  and datadate >='01Jan1959'd;
 /* Two years of accounting data before 1962 */
  PS = coalesce(PSTKRV,PSTKL,PSTK,0);
  if missing(TXDITC) then TXDITC = 0 ;
  BE = seq + TXDITC - PS ;
  if BE<0 then BE=.;
  year = year(datadate);
  label BE='Book Value of Equity FYear t-1' ;
  drop indfmt datafmt popsrc consol ps &vars;
  retain count;
  if first.gvkey then count=1;
  else count = count+1;
run;

/************************ Part 2: CRSP **********************************/
/* Create a CRSP Subsample with Monthly Stock and Event Variables       */
/* This procedure creates a SAS dataset named "CRSP_M"                  */
/* Restrictions will be applied later                                   */
/* Select variables from the CRSP monthly stock and event datasets      */
%let msevars=ticker ncusip shrcd exchcd;
%let msfvars =  prc ret retx shrout cfacpr cfacshr;

%include '/wrds/crsp/samples/crspmerge.sas';

%crspmerge(s=m,start=01jan1955,end=31dec2018,
sfvars=&msfvars,sevars=&msevars,filters=exchcd in (1,2,3));

/* CRSP_M is sorted by date and permno and has historical returns     */
/* as well as historical share codes and exchange codes               */
/* Add CRSP delisting returns */
proc sql; create table crspm2
 as select a.*, b.dlret,
  sum(1,ret)*sum(1,dlret)-1 as retadj "Return adjusted for delisting",
  abs(a.prc)*a.shrout as MEq 'Market Value of Equity'
 from Crsp_m a left join crsp.msedelist(where=(missing(dlret)=0)) b
 on a.permno=b.permno and
    intnx('month',a.date,0,'E')=intnx('month',b.DLSTDT,0,'E')
 order by a.date, a.permco, MEq;
quit;

/* There are cases when the same firm (permco) has two or more         */
/* securities (permno) at same date. For the purpose of ME for         */
/* the firm, we aggregated all ME for a given permco, date. This       */
/* aggregated ME will be assigned to the Permno with the largest ME    */
data crspm2a (drop = Meq); set crspm2;
  by date permco Meq;
  retain ME;
  if first.permco and last.permco then do;
    ME=meq;
  output; /* most common case where a firm has a unique permno*/
  end;
  else do ;
    if  first.permco then ME=meq;
    else ME=sum(meq,ME);
    if last.permco then output;
  end;
run;

proc sql;
	create table dcrsp
	as select permno,date,ret
	from crsp.dsf
	quit;

proc sql;
    create table ddcrsp as
    select a.*, (a.ret-b.rf) as ERET, b.mktrf, b.smb, b.hml from
    dcrsp a left join ff.factors_daily b
    on
    a.date = b.date;
    quit;

proc sort data=ddcrsp; by permno date; run;


/*proc export data=work.ddcrsp outfile="ddcrsp.csv" */
/*dbms=csv replace;run;*/


%macro RRLOOP(year1=1963, year2=2018, in_ds=ddcrsp, out_ds = work.out_ds);

%local date1 date2 date1f date2f yy mm;

/*Extra step to be sure to start with clean, null datasets for appending*/

proc datasets nolist lib=work;
  delete all_ds oreg_ds1;
run;

%do yy = &year1 %to &year2;
  %do mm = 1 %to 12;

%let date2= %sysfunc(mdy(&mm,1,&yy));
%let date2 = %sysfunc (intnx(month,&date2,0,end));
%let date1 = %sysfunc (intnx(month,&date2,-3,end));

/*An extra step to be sure the loop starts with a clean (empty) dataset for combining results*/
proc datasets nolist lib=work;
  delete oreg_ds1;
run;


/*Regression model estimation -- creates output set with residual*/
proc reg noprint data=&in_ds outest=oreg_ds1 edf sse;
  where date between &date1 and &date2;
  model ERET = mktrf smb hml;
  by permno;
run;

/*Store DATE1 and DATE2 as dataset variables;*/
data oreg_ds1;
  set oreg_ds1;
  date1=&date1;
  date2=&date2;
  date=&date2;
  rename _SSE_=RVAR;
  nobs= _p_ + _edf_;
  if nobs>=20;
  format date1 date2 yymmdd10.;
run;

/*Append loop results to dataset with all date1-date2 observations*/
proc datasets lib=work;
  append base=all_ds data=oreg_ds1;
run;

 %end;   /*MM month loop*/

 %end;  /*YY year loop*/

/*Save results in final dataset*/
data &out_ds;
  set all_ds;
run;

%mend RRLOOP;

%RRLOOP (year1= 1963, year2= 2018,  in_ds=ddcrsp, out_ds=work.out_ds);


/*proc export data=work.out_ds outfile="out_rvar.csv" */
/*dbms=csv replace;run;*/


/*proc export data=work.out_ds outfile="out_mergedate.csv" */
/*dbms=csv replace;run;*/

proc sort data=work.out_ds nodupkey; by permno date; run;

proc sql;
create table crspm2aa as
select a.*, b.* from
crspm2a a left join out_ds b
on
a.permno = b.permno
and year(a.date) = year(b.date)
and month(a.date) = month(b.date)
and day(a.date)>= day(b.date)-3
and day(a.date)<= day(b.date)
order by a.permno, a.date;
quit;


/* There should be no duplicates*/
proc sort data=crspm2aa nodupkey; by permno date;run;

/* The next step does 2 things:                                        */
/* - Create weights for later calculation of VW returns.               */
/*   Each firm's monthly return RET t willl be weighted by             */
/*   ME(t-1) = ME(t-2) * (1 + RETX (t-1))                              */
/*     where RETX is the without-dividend return.                      */
/* - Create a File with December t-1 Market Equity (ME)                */
data crspm3 (keep=permno date retadj weight_port ME ret RVAR exchcd sh rcd cumretx)
decme (keep = permno date ME rename=(me=DEC_ME) )  ;
     set crspm2aa;
 by permno date;
 retain weight_port cumretx me_base;
 Lpermno=lag(permno);
 LME=lag(me);
     if first.permno then do;
     LME=me/(1+retx); cumretx=sum(1,retx); me_base=LME;weight_port=.;end;
     else do;
     if month(date)=7 then do;
        weight_port= LME;
        me_base=LME; /* lag ME also at the end of June */
        cumretx=sum(1,retx);
     end;
     else do;
        if LME>0 then weight_port=cumretx*me_base;
        else weight_port=.;
        cumretx=cumretx*sum(1,retx);
     end; end;
output crspm3;
if month(date)=12 and ME>0 then output decme;
run;

/* Create a file with data for each June with ME from previous December */
proc sql;
  create table crspjune as
  select a.*, b.DEC_ME
  from crspm3 (where=(month(date)=6)) as a, decme as b
  where a.permno=b.permno and
  intck('month',b.date,a.date)=6;
quit;


/***************   Part 3: Merging CRSP and Compustat ***********/
/* Add Permno to Compustat sample */
proc sql;
  create table ccm1 as
  select a.*, b.lpermno as permno, b.linkprim
  from comp as a, crsp.ccmxpf_linktable as b
  where a.gvkey=b.gvkey
  and substr(b.linktype,1,1)='L' and linkprim in ('P','C')
  and (intnx('month',intnx('year',a.datadate,0,'E'),6,'E') >= b.linkdt)
  and (b.linkenddt >= intnx('month',intnx('year',a.datadate,0,'E'),6,'E')
  or missing(b.linkenddt))
  order by a.datadate, permno, b.linkprim desc;
quit;

/*  Cleaning Compustat Data for no relevant duplicates                      */
/*  Eliminating overlapping matching : few cases where different gvkeys     */
/*  for same permno-date --- some of them are not 'primary' matches in CCM  */
/*  Use linkprim='P' for selecting just one gvkey-permno-date combination   */
data ccm1a; set ccm1;
  by datadate permno descending linkprim;
  if first.permno;
run;

/* Sanity Check -- No Duplicates */
proc sort data=ccm1a nodupkey; by permno year datadate; run;

/* 2. However, there other type of duplicates within the year                */
/* Some companiess change fiscal year end in the middle of the calendar year */
/* In these cases, there are more than one annual record for accounting data */
/* We will be selecting the last annual record in a given calendar year      */
data ccm2a ; set ccm1a;
  by permno year datadate;
  if last.year;
run;

/* Sanity Check -- No Duplicates */
proc sort data=ccm2a nodupkey; by permno datadate; run;

/* Finalize Compustat Sample                              */
/* Merge CRSP with Compustat data, at June of every year  */
/* Match fiscal year ending calendar year t-1 with June t */
proc sql; create table ccm2_june as
  select a.*, b.BE, (1000*b.BE)/a.DEC_ME as BEME, b.count,
  b.datadate,
  intck('month',b.datadate, a.date) as dist
  from crspjune a, ccm2a b
  where a.permno=b.permno and intnx('month',a.date,0,'E')=
  intnx('month',intnx('year',b.datadate,0,'E'),6,'E')
  order by a.date;
quit;


/* ********************************************************************************* */
/* ******************* CREATE MONTHLY BIG CCM_CRSP ********************************* */
/*  ccm2_june comes from original script                                             */
/*  merge crspm2a with ccm2_june                                                     */
/*  BE information of June paste to crspm2a for July year t to June year t+1         */
/*                                                                                   */
/*                                                                                   */
/*                                                                                   */
proc sql;
create table ccm_month as
select a.*, b.count, b.beme, b.date as june_date, b.datadate as annual_info_date from
crspm2aa a left join ccm2_june b
on
a.permno = b.permno
and 1<=intck('month', intnx('month',b.date,0,'E'),  intnx('month',a.date,0,'E') )<=12
order by a.permno, a.date;
quit;


/************************ Part 4: Size and Book to Market Portfolios ***/
/* Forming Portolio by ME and BEME as of each June t                   */
/* Calculate NYSE Breakpoints for Market Equity (ME) and               */
/* Book-to-Market (BEME)                                               */


data ccm_month;set ccm_month;
if not missing(RVAR);
run;

proc sort data=ccm_month; by date permno; run;

proc univariate data=ccm_month noprint;
  where exchcd=1 and shrcd in (10,11) and count>=2;
  var RVAR;
  by date;
  output out=nyse_breaks pctlpts = 10 to 90 by 10 pctlpre=RVAR ;
run;

/*proc export data=nyse_breaks outfile="nyse_breaks.csv" */
/*dbms=csv replace;run; */

/* Use Breakpoints to classify stock only at end of all June's */
/* HX MODI */
proc sql;
  create table ccm3 as
  select a.*,
  b.rvar10, b.rvar20,b.rvar30,b.rvar40,b.rvar50,b.rvar60,b.rvar70,b.rvar80,b.rvar90
  from ccm_month as a, nyse_breaks as b
  where a.date=b.date;
quit;

/*roc export data=ccm3 outfile="ccm3.csv"*/
/*dbms=csv replace;run; */

/* Create portfolios as of June                       */
/* SIZE Portfolios          : S[mall] or B[ig]        */
/* Book-to-market Portfolios: L[ow], M[edium], H[igh] */
/* HX MODI  10 times 10*/
data all_month; set ccm3;
 If me>0 and count>=2 then do;
 positiveme = 1;
 if  rvar <= rvar10 then              rvarport = 'RVAR0' ;
 else if rvar10 < rvar <= rvar20 then rvarport = 'RVAR1' ;
 else if rvar20 < rvar <= rvar30 then rvarport = 'RVAR2' ;
 else if rvar30 < rvar <= rvar40 then rvarport = 'RVAR3' ;
 else if rvar40 < rvar <= rvar50 then rvarport = 'RVAR4' ;
 else if rvar50 < rvar <= rvar60 then rvarport = 'RVAR5' ;
 else if rvar60 < rvar <= rvar70 then rvarport = 'RVAR6' ;
 else if rvar70 < rvar <= rvar80 then rvarport = 'RVAR7' ;
 else if rvar80 < rvar <= rvar90 then rvarport = 'RVAR8' ;
 else if rvar  > rvar90 then          rvarport = 'RVAR9' ;
 else rvarport='';
 end;
 else positiveme = 0;
if cmiss(rvarport)=0 then nonmissport=1; else nonmissport=0;
keep permno date retadj rvarport exchcd shrcd nonmissport me;
run;

/* HX ADD */
data all_month; set all_month;
myind = catx('_', rvarport);
run;

/* ********************************************************************************* */
/* lag port labels one month */
proc sort data=all_month; by permno date; run;

data ccm4; set all_month;
by permno date;
mdate = intnx('month', date,0,'E');
format mdate worddatx19.;
mport = lag(rvarport);
weight_port = lag(me);
myind25 = lag(myind);
if first.permno then mport=.;
if first.permno then weight_port=.;
if first.permno then myind25=.;
run;

/* ********************************************************************************* */
/* mymonth */

data mymonth; set ccm4;
keep mdate permno myind25;
run;

/*proc export data=ccm4 outfile="ccm4.csv" */
/*dbms=csv replace;run; */

/*************** Part 5: Calculating Fama-French Factors  **************/
/* Calculate monthly time series of weighted average portfolio returns */
proc sort data=ccm4; by date mport; run;

proc means data=ccm4 noprint;
 where weight_port>0 and exchcd in (1,2,3)
      and shrcd in (10,11) and nonmissport=1;
 by date mport;
 var retadj;
 weight weight_port;
 output out=vwret (drop= _type_ _freq_ ) mean=vwret n=n_firms;
run;


proc transpose data=vwret(keep=date mport vwret)
 out=vwret2 (drop=_name_ _label_);
 by date ;
 ID mport;
 Var vwret;
run;

proc export data=vwret2 outfile="vwret2_10_RVAR.csv"
dbms=csv replace;

/* Number of Firms */
proc transpose data=vwret(keep=date mport n_firms)
               out=vwret3 (drop=_name_ _label_) prefix=n_;
by date ;
ID mport;
Var n_firms;
run;

proc export data=vwret3 outfile="vwret3_10_RVAR.csv"
dbms=csv replace;

proc sort data=mymonth nodupkey out=&table_out;
where "&begdate"d<=mdate<="&enddate"d;
by mdate permno;run;

%mend DIVIDE_PORTFOLIO_10_RVAR;


%MACRO DIVIDE_PORTFOLIO_10_SVAR (begdate=, enddate=, table_out=);

/************************ Part 1: Compustat ****************************/
/* Compustat XpressFeed Variables:                                     */
/* AT      = Total Assets                                              */
/* PSTKL   = Preferred Stock Liquidating Value                         */
/* TXDITC  = Deferred Taxes and Investment Tax Credit                  */
/* PSTKRV  = Preferred Stock Redemption Value                          */
/* seq     = Common/Ordinary Equity - Total                            */
/* PSTK    = Preferred/Preference Stock (Capital) - Total              */

/* In calculating Book Equity, incorporate Preferred Stock (PS) values */
/*  use the redemption value of PS, or the liquidation value           */
/*    or the par value (in that order) (FF,JFE, 1993, p. 8)            */
/* USe Balance Sheet Deferred Taxes TXDITC if available                */
/* Flag for number of years in Compustat (<2 likely backfilled data)   */

%let vars = AT PSTKL TXDITC PSTKRV seq PSTK ;
data comp;
  set comp.funda
  (keep= gvkey datadate &vars indfmt datafmt popsrc consol);
  by gvkey datadate;
  where indfmt='INDL' and datafmt='STD' and popsrc='D' and consol='C'
  and datadate >='01Jan1959'd;
 /* Two years of accounting data before 1962 */
  PS = coalesce(PSTKRV,PSTKL,PSTK,0);
  if missing(TXDITC) then TXDITC = 0 ;
  BE = seq + TXDITC - PS ;
  if BE<0 then BE=.;
  year = year(datadate);
  label BE='Book Value of Equity FYear t-1' ;
  drop indfmt datafmt popsrc consol ps &vars;
  retain count;
  if first.gvkey then count=1;
  else count = count+1;
run;

/************************ Part 2: CRSP **********************************/
/* Create a CRSP Subsample with Monthly Stock and Event Variables       */
/* This procedure creates a SAS dataset named "CRSP_M"                  */
/* Restrictions will be applied later                                   */
/* Select variables from the CRSP monthly stock and event datasets      */
%let msevars=ticker ncusip shrcd exchcd;
%let msfvars =  prc ret retx shrout cfacpr cfacshr;

%include '/wrds/crsp/samples/crspmerge.sas';

%crspmerge(s=m,start=01jan1955,end=31dec2018,
sfvars=&msfvars,sevars=&msevars,filters=exchcd in (1,2,3));

/* CRSP_M is sorted by date and permno and has historical returns     */
/* as well as historical share codes and exchange codes               */
/* Add CRSP delisting returns */
proc sql; create table crspm2
 as select a.*, b.dlret,
  sum(1,ret)*sum(1,dlret)-1 as retadj "Return adjusted for delisting",
  abs(a.prc)*a.shrout as MEq 'Market Value of Equity'
 from Crsp_m a left join crsp.msedelist(where=(missing(dlret)=0)) b
 on a.permno=b.permno and
    intnx('month',a.date,0,'E')=intnx('month',b.DLSTDT,0,'E')
 order by a.date, a.permco, MEq;
quit;

/* There are cases when the same firm (permco) has two or more         */
/* securities (permno) at same date. For the purpose of ME for         */
/* the firm, we aggregated all ME for a given permco, date. This       */
/* aggregated ME will be assigned to the Permno with the largest ME    */
data crspm2a (drop = Meq); set crspm2;
  by date permco Meq;
  retain ME;
  if first.permco and last.permco then do;
    ME=meq;
  output; /* most common case where a firm has a unique permno*/
  end;
  else do ;
    if  first.permco then ME=meq;
    else ME=sum(meq,ME);
    if last.permco then output;
  end;
run;

proc sql;
	create table dcrsp
	as select permno,date,ret
	from crsp.dsf
	quit;

%macro DATELOOP (year1= 1963, year2= 2018, in_ds=dcrsp, out_ds=work.out_ds);

%local date1 date2 date1f date2f yy mm;

/*Extra step to be sure to start with clean, null datasets for appending*/

proc datasets nolist lib=work;
  delete all_ds oreg_ds1;
run;

%do yy = &year1 %to &year2;
  %do mm = 1 %to 12;

%let date2= %sysfunc(mdy(&mm,1,&yy));
%let date2 = %sysfunc (intnx(month,&date2,0,end));
%let date1 = %sysfunc (intnx(month,&date2,-3,end));

/*An extra step to be sure the loop starts with a clean (empty) dataset for combining results*/
proc datasets nolist lib=work;
  delete oreg_ds1;
run;

proc sql;
	create table oreg_ds1
	as select permno,date,ret,
	var(ret) as SVAR
	from &in_ds
  where date between &date1 and &date2
  group by permno;
	quit;

data oreg_ds1;
  set oreg_ds1;
  date1 = &date1;
  date2 = &date2;
  format date1 date2 date9.;
run;

proc datasets lib=work;
  append base=all_ds data=oreg_ds1;
run;

%end;
%end;

/*Save results in final dataset*/
data &out_ds;
  set all_ds;
run;

%mend DATELOOP;

%DATELOOP (year1= 1962, year2= 2018,  in_ds=dcrsp, out_ds=work.out_ds);

proc sort data=work.out_ds nodupkey; by permno date; run;

/*proc export data=work.all_ds outfile="out.csv" */
/*dbms=csv replace;run;*/

proc sql;
create table crspm2aa as
select a.*, b.* from
crspm2a a left join out_ds b
on
a.permno = b.permno
and a.date = b.date
order by a.permno, a.date;
quit;

/* There should be no duplicates*/
proc sort data=crspm2aa nodupkey; by permno date;run;

/* The next step does 2 things:                                        */
/* - Create weights for later calculation of VW returns.               */
/*   Each firm's monthly return RET t willl be weighted by             */
/*   ME(t-1) = ME(t-2) * (1 + RETX (t-1))                              */
/*     where RETX is the without-dividend return.                      */
/* - Create a File with December t-1 Market Equity (ME)                */
data crspm3 (keep=permno date retadj weight_port ME ret SVAR exchcd sh rcd cumretx)
decme (keep = permno date ME rename=(me=DEC_ME) )  ;
     set crspm2aa;
 by permno date;
 retain weight_port cumretx me_base;
 Lpermno=lag(permno);
 LME=lag(me);
     if first.permno then do;
     LME=me/(1+retx); cumretx=sum(1,retx); me_base=LME;weight_port=.;end;
     else do;
     if month(date)=7 then do;
        weight_port= LME;
        me_base=LME; /* lag ME also at the end of June */
        cumretx=sum(1,retx);
     end;
     else do;
        if LME>0 then weight_port=cumretx*me_base;
        else weight_port=.;
        cumretx=cumretx*sum(1,retx);
     end; end;
output crspm3;
if month(date)=12 and ME>0 then output decme;
run;

/* Create a file with data for each June with ME from previous December */
proc sql;
  create table crspjune as
  select a.*, b.DEC_ME
  from crspm3 (where=(month(date)=6)) as a, decme as b
  where a.permno=b.permno and
  intck('month',b.date,a.date)=6;
quit;


/***************   Part 3: Merging CRSP and Compustat ***********/
/* Add Permno to Compustat sample */
proc sql;
  create table ccm1 as
  select a.*, b.lpermno as permno, b.linkprim
  from comp as a, crsp.ccmxpf_linktable as b
  where a.gvkey=b.gvkey
  and substr(b.linktype,1,1)='L' and linkprim in ('P','C')
  and (intnx('month',intnx('year',a.datadate,0,'E'),6,'E') >= b.linkdt)
  and (b.linkenddt >= intnx('month',intnx('year',a.datadate,0,'E'),6,'E')
  or missing(b.linkenddt))
  order by a.datadate, permno, b.linkprim desc;
quit;

/*  Cleaning Compustat Data for no relevant duplicates                      */
/*  Eliminating overlapping matching : few cases where different gvkeys     */
/*  for same permno-date --- some of them are not 'primary' matches in CCM  */
/*  Use linkprim='P' for selecting just one gvkey-permno-date combination   */
data ccm1a; set ccm1;
  by datadate permno descending linkprim;
  if first.permno;
run;

/* Sanity Check -- No Duplicates */
proc sort data=ccm1a nodupkey; by permno year datadate; run;

/* 2. However, there other type of duplicates within the year                */
/* Some companiess change fiscal year end in the middle of the calendar year */
/* In these cases, there are more than one annual record for accounting data */
/* We will be selecting the last annual record in a given calendar year      */
data ccm2a ; set ccm1a;
  by permno year datadate;
  if last.year;
run;

/* Sanity Check -- No Duplicates */
proc sort data=ccm2a nodupkey; by permno datadate; run;

/* Finalize Compustat Sample                              */
/* Merge CRSP with Compustat data, at June of every year  */
/* Match fiscal year ending calendar year t-1 with June t */
proc sql; create table ccm2_june as
  select a.*, b.BE, (1000*b.BE)/a.DEC_ME as BEME, b.count,
  b.datadate,
  intck('month',b.datadate, a.date) as dist
  from crspjune a, ccm2a b
  where a.permno=b.permno and intnx('month',a.date,0,'E')=
  intnx('month',intnx('year',b.datadate,0,'E'),6,'E')
  order by a.date;
quit;



/* ********************************************************************************* */
/* ******************* CREATE MONTHLY BIG CCM_CRSP ********************************* */
/*  ccm2_june comes from original script                                             */
/*  merge crspm2a with ccm2_june                                                     */
/*  BE information of June paste to crspm2a for July year t to June year t+1         */
/*                                                                                   */
/*                                                                                   */
/*                                                                                   */
proc sql;
create table ccm_month as
select a.*, b.count, b.beme, b.date as june_date, b.datadate as annual_info_date from
crspm2aa a left join ccm2_june b
on
a.permno = b.permno
and 1<=intck('month', intnx('month',b.date,0,'E'),  intnx('month',a.date,0,'E') )<=12
order by a.permno, a.date;
quit;


/************************ Part 4: Size and Book to Market Portfolios ***/
/* Forming Portolio by ME and BEME as of each June t                   */
/* Calculate NYSE Breakpoints for Market Equity (ME) and               */
/* Book-to-Market (BEME)                                               */


data ccm_month;set ccm_month;
if not missing(SVAR);
run;

proc sort data=ccm_month; by date permno; run;

proc univariate data=ccm_month noprint;
  where exchcd=1 and shrcd in (10,11) and count>=2;
  var SVAR;
  by date;
  output out=nyse_breaks pctlpts = 10 to 90 by 10 pctlpre=SVAR ;
run;

proc export data=nyse_breaks outfile="nyse_breaks_svar.csv"
dbms=csv replace;run;

/* Use Breakpoints to classify stock only at end of all June's */
/* HX MODI */
proc sql;
  create table ccm3 as
  select a.*,
  b.svar10, b.svar20,b.svar30,b.svar40,b.svar50,b.svar60,b.svar70,b.svar80,b.svar90
  from ccm_month as a, nyse_breaks as b
  where a.date=b.date;
quit;



/* Create portfolios as of June                       */
/* SIZE Portfolios          : S[mall] or B[ig]        */
/* Book-to-market Portfolios: L[ow], M[edium], H[igh] */
/* HX MODI  10 times 10*/
data all_month; set ccm3;
 If me>0 and count>=2 then do;
 positiveme = 1;
 if  0<= svar <= svar10 then              svarport = 'SVAR0' ;
 else if svar10 < svar <= svar20 then svarport = 'SVAR1' ;
 else if svar20 < svar <= svar30 then svarport = 'SVAR2' ;
 else if svar30 < svar <= svar40 then svarport = 'SVAR3' ;
 else if svar40 < svar <= svar50 then svarport = 'SVAR4' ;
 else if svar50 < svar <= svar60 then svarport = 'SVAR5' ;
 else if svar60 < svar <= svar70 then svarport = 'SVAR6' ;
 else if svar70 < svar <= svar80 then svarport = 'SVAR7' ;
 else if svar80 < svar <= svar90 then svarport = 'SVAR8' ;
 else if svar  > svar90 then          svarport = 'SVAR9' ;
 else svarport='';
 end;
 else positiveme = 0;
if cmiss(svarport)=0 then nonmissport=1; else nonmissport=0;
keep permno date retadj svarport exchcd shrcd nonmissport me;
run;

/* HX ADD */
data all_month; set all_month;
myind = catx('_', svarport);
run;

/* ********************************************************************************* */
/* lag port labels one month */
proc sort data=all_month; by permno date; run;

data ccm4; set all_month;
by permno date;
mdate = intnx('month', date,0,'E');
format mdate worddatx19.;
mport = lag(svarport);
weight_port = lag(me);
myind25 = lag(myind);
if first.permno then mport=.;
if first.permno then weight_port=.;
if first.permno then myind25=.;
run;

/* ********************************************************************************* */
/* mymonth */

data mymonth; set ccm4;
keep mdate permno myind25;
run;



proc export data=ccm4 outfile="ccm4_svar.csv"
dbms=csv replace;run;


/*************** Part 5: Calculating Fama-French Factors  **************/
/* Calculate monthly time series of weighted average portfolio returns */
proc sort data=ccm4; by date mport; run;

proc means data=ccm4 noprint;
 where weight_port>0 and exchcd in (1,2,3)
      and shrcd in (10,11) and nonmissport=1;
 by date mport;
 var retadj;
 weight weight_port;
 output out=vwret (drop= _type_ _freq_ ) mean=vwret n=n_firms;
run;

proc transpose data=vwret(keep=date mport vwret)
 out=vwret2 (drop=_name_ _label_);
 by date ;
 ID mport;
 Var vwret;
run;

proc export data=vwret2 outfile="vwret2_10_SVAR.csv"
dbms=csv replace;

/* Number of Firms */
proc transpose data=vwret(keep=date mport n_firms)
               out=vwret3 (drop=_name_ _label_) prefix=n_;
by date ;
ID mport;
Var n_firms;
run;

proc export data=vwret3 outfile="vwret3_10_SVAR.csv"
dbms=csv replace;

proc sort data=mymonth nodupkey out=&table_out;
where "&begdate"d<=mdate<="&enddate"d;
by mdate permno;run;

%mend DIVIDE_PORTFOLIO_10_SVAR;


%MACRO DIVIDE_PORTFOLIO_10_ME (begdate=, enddate=, table_out=);

/************************ Part 1: Compustat ****************************/
/* Compustat XpressFeed Variables:                                     */
/* AT      = Total Assets                                              */
/* PSTKL   = Preferred Stock Liquidating Value                         */
/* TXDITC  = Deferred Taxes and Investment Tax Credit                  */
/* PSTKRV  = Preferred Stock Redemption Value                          */
/* seq     =     hgghggiuhhiuhkjhjhkj                            */
/* PSTK    = Preferred/Preference Stock (Capital) - Total              */

/* In calculating Book Equity, incorporate Preferred Stock (PS) values */
/*  use the redemption value of PS, or the liquidation value           */
/*    or the par value (in that order) (FF,JFE, 1993, p. 8)            */
/* USe Balance Sheet Deferred Taxes TXDITC if available                */
/* Flag for number of years in Compustat (<2 likely backfilled data)   */


%let vars = AT PSTKL TXDITC PSTKRV seq PSTK ;
data comp;
  set comp.funda
  (keep= gvkey datadate &vars indfmt datafmt popsrc consol);
  by gvkey datadate;
  where indfmt='INDL' and datafmt='STD' and popsrc='D' and consol='C'
  and datadate >='01Jan1900'd;
 /* Two years of accounting data before 1962 */
  PS = coalesce(PSTKRV,PSTKL,PSTK,0);
  if missing(TXDITC) then TXDITC = 0 ;
  BE = seq + TXDITC - PS ;
  if BE<0 then BE=.;
  year = year(datadate);
  label BE='Book Value of Equity FYear t-1' ;
  drop indfmt datafmt popsrc consol ps &vars;
  retain count;
  if first.gvkey then count=1;
  else count = count+1;
run;

/************************ Part 2: CRSP **********************************/
/* Create a CRSP Subsample with Monthly Stock and Event Variables       */
/* This procedure creates a SAS dataset named "CRSP_M"                  */
/* Restrictions will be applied later                                   */
/* Select variables from the CRSP monthly stock and event datasets      */
%let msevars=ticker ncusip shrcd exchcd;
%let msfvars =  prc ret retx shrout cfacpr cfacshr;

%include '/wrds/crsp/samples/crspmerge.sas';

%crspmerge(s=m,start=01jan1900,end=31dec2018,
sfvars=&msfvars,sevars=&msevars,filters=exchcd in (1,2,3));

/* CRSP_M is sorted by date and permno and has historical returns     */
/* as well as historical share codes and exchange codes               */
/* Add CRSP delisting returns */
proc sql; create table crspm2
 as select a.*, b.dlret,
  sum(1,ret)*sum(1,dlret)-1 as retadj "Return adjusted for delisting",
  abs(a.prc)*a.shrout as MEq 'Market Value of Equity'
 from Crsp_m a left join crsp.msedelist(where=(missing(dlret)=0)) b
 on a.permno=b.permno and
    intnx('month',a.date,0,'E')=intnx('month',b.DLSTDT,0,'E')
 order by a.date, a.permco, MEq;
quit;

/* There are cases when the same firm (permco) has two or more         */
/* securities (permno) at same date. For the purpose of ME for         */
/* the firm, we aggregated all ME for a given permco, date. This       */
/* aggregated ME will be assigned to the Permno with the largest ME    */
data crspm2a (drop = Meq); set crspm2;
  by date permco Meq;
  retain ME;
  if first.permco and last.permco then do;
    ME=meq;
  output; /* most common case where a firm has a unique permno*/
  end;
  else do ;
    if  first.permco then ME=meq;
    else ME=sum(meq,ME);
    if last.permco then output;
  end;
run;

/* There should be no duplicates*/
proc sort data=crspm2a nodupkey; by permno date;run;

/* The next step does 2 things:                                        */
/* - Create weights for later calculation of VW returns.               */
/*   Each firm's monthly return RET t willl be weighted by             */
/*   ME(t-1) = ME(t-2) * (1 + RETX (t-1))                              */
/*     where RETX is the without-dividend return.                      */
/* - Create a File with December t-1 Market Equity (ME)                */
data crspm3 (keep=permno date retadj weight_port ME exchcd shrcd cumretx)
decme (keep = permno date ME rename=(me=DEC_ME) )  ;
     set crspm2a;
 by permno date;
 retain weight_port cumretx me_base;
 Lpermno=lag(permno);
 LME=lag(me);
     if first.permno then do;
     LME=me/(1+retx); cumretx=sum(1,retx); me_base=LME;weight_port=.;end;
     else do;
     if month(date)=7 then do;
        weight_port= LME;
        me_base=LME; /* lag ME also at the end of June */
        cumretx=sum(1,retx);
     end;
     else do;
        if LME>0 then weight_port=cumretx*me_base;
        else weight_port=.;
        cumretx=cumretx*sum(1,retx);
     end; end;
output crspm3;
if month(date)=12 and ME>0 then output decme;
run;

/* Create a file with data for each June with ME from previous December */
proc sql;
  create table crspjune as
  select a.*, b.DEC_ME
  from crspm3 (where=(month(date)=6)) as a, decme as b
  where a.permno=b.permno and
  intck('month',b.date,a.date)=6;
quit;

/***************   Part 3: Merging CRSP and Compustat ***********/
/* Add Permno to Compustat sample */
proc sql;
  create table ccm1 as
  select a.*, b.lpermno as permno, b.linkprim
  from comp as a, crsp.ccmxpf_linktable as b
  where a.gvkey=b.gvkey
  and substr(b.linktype,1,1)='L' and linkprim in ('P','C')
  and (intnx('month',intnx('year',a.datadate,0,'E'),6,'E') >= b.linkdt)
  and (b.linkenddt >= intnx('month',intnx('year',a.datadate,0,'E'),6,'E')
  or missing(b.linkenddt))
  order by a.datadate, permno, b.linkprim desc;
quit;

/*  Cleaning Compustat Data for no relevant duplicates                      */
/*  Eliminating overlapping matching : few cases where different gvkeys     */
/*  for same permno-date --- some of them are not 'primary' matches in CCM  */
/*  Use linkprim='P' for selecting just one gvkey-permno-date combination   */
data ccm1a; set ccm1;
  by datadate permno descending linkprim;
  if first.permno;
run;

/* Sanity Check -- No Duplicates */
proc sort data=ccm1a nodupkey; by permno year datadate; run;

/* 2. However, there other type of duplicates within the year                */
/* Some companiess change fiscal year end in the middle of the calendar year */
/* In these cases, there are more than one annual record for accounting data */
/* We will be selecting the last annual record in a given calendar year      */
data ccm2a ; set ccm1a;
  by permno year datadate;
  if last.year;
run;

/* Sanity Check -- No Duplicates */
proc sort data=ccm2a nodupkey; by permno datadate; run;

/* Finalize Compustat Sample                              */
/* Merge CRSP with Compustat data, at June of every year  */
/* Match fiscal year ending calendar year t-1 with June t */
proc sql; create table ccm2_june as
  select a.*, b.BE, (1000*b.BE)/a.DEC_ME as BEME, b.count,
  b.datadate,
  intck('month',b.datadate, a.date) as dist
  from crspjune a, ccm2a b
  where a.permno=b.permno and intnx('month',a.date,0,'E')=
  intnx('month',intnx('year',b.datadate,0,'E'),6,'E')
  order by a.date;
quit;

/************************ Part 4: Size and Book to Market Portfolios ***/
/* Forming Portolio by ME and BEME as of each June t                   */
/* Calculate NYSE Breakpoints for Market Equity (ME) and               */
/* Book-to-Market (BEME)                                               */
proc univariate data=ccm2_june noprint;
  where exchcd=1 and beme>0 and shrcd in (10,11) and me>0 and count>=2;
  var ME; * ME is Market Equity at the end of June;
  by date; /*at june;*/
  output out=nyse_breaks pctlpre=ME pctlpts= 10 to 90 by 10; /* HX ADD */
run;

/*proc export data=nyse_breaks outfile="bp.csv" */
/*dbms=csv replace; */


/* Use Breakpoints to classify stock only at end of all June's */
/* HX MODI */
proc sql;
  create table ccm3_june as
  select a.*,
  b.me10,b.me20,b.me30, b.me40,b.me50, b.me60,b.me70, b.me80,b.me90
  from ccm2_june as a, nyse_breaks as b
  where a.date=b.date;
quit;

/* Create portfolios as of June                       */
/* SIZE Portfolios          : S[mall] or B[ig]        */
/* Book-to-market Portfolios: L[ow], M[edium], H[igh] */
/* HX MODI  10 times 10*/
data june ; set ccm3_june;
 If beme>0 and me>0 and count>=2 then do;
 positivebeme=1;

 if 0 < me <= me10 then           btmport = 'ME0' ;
 else if me10 < me <= me20 then btmport = 'ME1' ;
 else if me20 < me <= me30 then btmport = 'ME2' ;
 else if me30 < me <= me40 then btmport = 'ME3' ;
 else if me40 < me <= me50 then btmport = 'ME4' ;
 else if me50 < me <= me60 then btmport = 'ME5' ;
 else if me60 < me <= me70 then btmport = 'ME6' ;
 else if me70 < me <= me80 then btmport = 'ME7' ;
 else if me80 < me <= me90 then btmport = 'ME8' ;
 else if me  > me90 then          btmport = 'ME9' ;
 else btmport='';
end;
else positivebeme=0;
if cmiss(btmport)=0 then nonmissport=1; else nonmissport=0;
keep permno date btmport positivebeme exchcd shrcd nonmissport;
run;

/*proc export data=ccm3_june outfile="ccm3june.csv" */
/*dbms=csv replace; */

/* HX ADD */
data myjune; set june;
myind25 = catx('_', btmport);
run;

data myjune; set myjune;
sortdate = intnx('month',date,0,'E');
nextmon = intnx('month',date,1);
nextmonend = intnx('month',nextmon,0,'E');
run;

/* proc export data=myjune outfile="myjune_25.csv" */
/* dbms=csv replace; */

/* populate annual data to monthly */
%populate(inset=myjune, outset=mymonth, datevar=nextmonend, idvar=permno, forward_max=12);

/* Identifying each month the securities of              */
/* Buy and hold June portfolios from July t to June t+1  */
proc sql; create table ccm4 as
 select a.*, b.btmport, b.date as portdate format MMDDYYD10.,
        b.positivebeme , b.nonmissport
 from crspm3 as a, june as b
 where a.permno=b.permno and  1 <= intck('month',b.date,a.date) <= 12
 order by date, btmport;
quit;

/*************** Part 5: Calculating Fama-French Factors  **************/
/* Calculate monthly time series of weighted average portfolio returns */
proc means data=ccm4 noprint;
 where weight_port>0 and positivebeme=1 and exchcd in (1,2,3)
      and shrcd in (10,11) and nonmissport=1;
 by date btmport;
 var retadj;
 weight weight_port;
 output out=vwret (drop= _type_ _freq_ ) mean=vwret n=n_firms;
run;

/*proc export data=vwret outfile="vwret_10_ME.csv"*/
/*dbms=csv replace;*/

/* Monthly Factor Returns: SMB and HML */
proc transpose data=vwret(keep=date btmport vwret)
 out=vwret2 (drop=_name_ _label_);
 by date ;
 ID btmport;
 Var vwret;
run;

proc export data=vwret2 outfile="vwret2_10_ME.csv"
dbms=csv replace;

/* Number of Firms */
proc transpose data=vwret(keep=date btmport n_firms)
               out=vwret3 (drop=_name_ _label_) prefix=n_;
by date ;
ID btmport;
Var n_firms;
run;

proc export data=vwret3 outfile="vwret3_10_ME.csv"
dbms=csv replace;

proc sort data=mymonth nodupkey out=&table_out;
where "&begdate"d<=mdate<="&enddate"d;
by mdate permno;run;

%mend DIVIDE_PORTFOLIO_10_ME;
