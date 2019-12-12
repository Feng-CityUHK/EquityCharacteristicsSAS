
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
/* MACRO's for me beta svar             */
/* simple bp */

/* ********************************************************************************* */
/* portfolio sorts */

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
  where exchcd in (1,2,3) and beme>0 and shrcd in (10,11) and me>0 and count>=2;
  var ME; * ME is Market Equity at the end of June;
  by date; /*at june;*/
  output out=simple_breaks pctlpre=ME pctlpts= 10 to 90 by 10; /* HX ADD */
run;

/*proc export data=simple_breaks outfile="bp.csv" */
/*dbms=csv replace; */


/* Use Breakpoints to classify stock only at end of all June's */
/* HX MODI */
proc sql;
  create table ccm3_june as
  select a.*,
  b.me10,b.me20,b.me30, b.me40,b.me50, b.me60,b.me70, b.me80,b.me90
  from ccm2_june as a, simple_breaks as b
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
  where exchcd in (1,2,3) and shrcd in (10,11) and count>=2;
  var SVAR;
  by date;
  output out=simple_breaks pctlpts = 10 to 90 by 10 pctlpre=SVAR ;
run;

proc export data=simple_breaks outfile="simple_breaks_svar.csv"
dbms=csv replace;run;

/* Use Breakpoints to classify stock only at end of all June's */
/* HX MODI */
proc sql;
  create table ccm3 as
  select a.*,
  b.svar10, b.svar20,b.svar30,b.svar40,b.svar50,b.svar60,b.svar70,b.svar80,b.svar90
  from ccm_month as a, simple_breaks as b
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
  where exchcd in (1,2,3) and shrcd in (10,11);
  var beta;
  by date;
  output out=simple_breaks pctlpre=beta pctlpts=10 to 90 by 10; /* HX ADD */
run;

/* proc export data=simple_breaks outfile="simple_breaks.csv" */
/* dbms=csv replace;run; */

/* Use Breakpoints to classify stock only at end of all June's */
/* HX MODI */
proc sql;
  create table ccm3 as
  select a.*,
  b.beta10, b.beta20,b.beta30,b.beta40,b.beta50,b.beta60,b.beta70,b.beta80,b.beta90
  from ccm_month as a, simple_breaks as b
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
