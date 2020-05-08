/* fill in missing values by FF49 Industry Average at that time */
/* fill in missing values by All Stock at that time Equal Weight Average */

/* ********************************************* */
/*  Macro                    */
/* ********************************************* */


/* ********************************************************************************* */
/* MACRO 6            */
%MACRO FINRATIO_ind_label2 (begdate=, enddate=, label=, avr=, input=, vars=, output=);

%let allvars=&vars;
%let indclass = &label;

data ratios;
set &INPUT;
/*set time frame*/
where "&begdate"d<=public_date<="&enddate"d;
run;

/* proc export data=ratios outfile="firm_ratios_indclass_&indclass._.csv" */
/* dbms=csv replace; */

/* average zt */
proc sort data = ratios; by public_date &indclass; run;
/*Computing Industry-level average financial ratios in a given month*/
proc means data=ratios noprint;
  where not missing(&indclass);
    by public_date; class &indclass;
     var &allvars;
  /* weight &weight;   */                    /* value-weight */
    output out=indratios &avr=/autoname;
run;
proc sort data=indratios; by public_date &indclass;run;

data indratios; set indratios;
where not missing(&indclass);
drop _type_ _freq_;
format public_date Date9.;
run;

data &output;
set indratios;
run;

/* check */
/* proc export data=indratios outfile="indratios_&indclass..csv" */
/* dbms=csv replace;                                             */

/* clean house */
proc sql; drop table ratios, indratios;
quit;

%mend FINRATIO_ind_label2;

/* ********************************************************************************* */
/* MACRO 4            */

%MACRO FINRATIO_ew_all (begdate=, enddate=, avr=, input=, vars=, output=);

/*List of Ratios to be calculated*/
%let allvars=&vars;

data ratios;
set &INPUT;
/*set time frame*/
where "&begdate"d<=public_date<="&enddate"d;
run;

/* proc export data=ratios outfile="firm_ratios_indclass_&indclass._.csv" */
/* dbms=csv replace; */

proc sort data = ratios; by public_date permno; run;
/*Computing Industry-level average financial ratios in a given month*/
proc means data=ratios noprint;
  where not missing(permno);
    by public_date;
     var &allvars;
    output out=indratios &avr=/autoname;
run;
proc sort data=indratios; by public_date;run;

data &output; set indratios;
drop _type_ _freq_;
format public_date Date9.;
run;

proc sql; drop table ratios, indratios;
quit;

%mend FINRATIO_ew_all;


/* ********************************************* */
/*  Parameters                    */
/* ********************************************* */

%let uni_begdt = 01JAN1976;
%let uni_enddt = 31DEC2018;

libname chars '/scratch/cityuhk/xinhe/eqchars';

%let vars =
  mom12m hxz_abr hxz_sue hxz_re
  bm ep cfp sp
  agr ni acc
  op roe
  seas1a adm rdm
  me svar beta mom1m
;

%let vars_industry =
FFI5_desc   FFI5
FFI10_desc   FFI10
FFI12_desc   FFI12
FFI17_desc   FFI17
FFI30_desc   FFI30
FFI38_desc   FFI38
FFI48_desc   FFI48
FFI49_desc   FFI49
;

/* ********************************************* */
/*  Load data                    */
/* ********************************************* */

data da;
set chars.firmchars_v7_1_final;
keep
  public_date   permno   gvkey   sic  cusip exchcd
  &vars_industry  &vars;
run;

data da; set da;
format public_date Date9.;
run;

proc print data=da(obs=100); run;

/* ********************************************************************************* */
/* FFI49 Industry Average         */

%FINRATIO_ind_label2  (BEGDATE=&uni_begdt, ENDDATE=&uni_enddt, label=FFI49_desc, AVR=median, Input=da, vars=&vars, output=ind_FFI49);

title "FFI49 Industry Weighted Average";
PROC PRINT DATA=ind_FFI49(where=(year(public_date)=1976));RUN;

/* proc export data=ind_FFI49 outfile="ind_FFI49.csv"  */
/* dbms=csv replace;   */

/* ********************************************************************************* */
/* fill in missing eq zt with FFI49 ind zt         */

proc sql;
create table tmp
as
select a.*,
  b.mom12m_Median, b.hxz_abr_Median, b.hxz_sue_Median, b.hxz_re_Median,
  b.bm_Median, b.ep_Median, b.cfp_Median, b.sp_Median,
  b.agr_Median, b.ni_Median, b.acc_Median,
  b.op_Median, b.roe_Median,
  b.seas1a_Median, b.adm_Median, b.rdm_Median,
  b.me_Median, b.svar_Median, b.beta_Median, b.mom1m_Median
 from
da a left join ind_FFI49 b
on
a.public_date = b.public_date and
a.FFI49_desc = b.FFI49_desc
order by cusip, public_date;
quit;

title "Merged Table 1";
PROC PRINT DATA=tmp(obs=10);RUN;

data tmp; set tmp;

if missing(mom12m) then mom12m = mom12m_Median;
if missing(hxz_abr) then hxz_abr = hxz_abr_Median;
if missing(hxz_re) then hxz_re = hxz_re_Median;
if missing(hxz_sue) then hxz_sue = hxz_sue_Median;

if missing(bm) then bm = bm_Median;
if missing(cfp) then cfp = cfp_Median;
if missing(ep) then ep = ep_Median;
if missing(sp) then sp = sp_Median;

if missing(acc) then acc = acc_Median;
if missing(agr) then agr = agr_Median;
if missing(ni) then ni = ni_Median;

if missing(op) then op = op_Median;
if missing(roe) then roe = roe_Median;

if missing(seas1a) then seas1a = seas1a_Median;
if missing(adm) then adm = adm_Median;
if missing(rdm) then rdm = rdm_Median;

if missing(me) then me = me_Median;
if missing(mom1m) then mom1m = mom1m_Median;
if missing(beta) then beta = beta_Median;
if missing(svar) then svar = svar_Median;

run;

data tmp; set tmp;
drop
  mom12m_Median hxz_abr_Median hxz_sue_Median hxz_re_Median
  bm_Median ep_Median cfp_Median sp_Median
  agr_Median ni_Median acc_Median
  op_Median roe_Median
  seas1a_Median adm_Median rdm_Median
  me_Median svar_Median beta_Median mom1m_Median
run;

title "Merged Table 2";
PROC PRINT DATA=tmp(obs=10);RUN;

/* proc export data=tmp(where=(year(public_date)=1976)) outfile="tmp1976.csv"  */
/* dbms=csv replace;  */

/* ********************************************************************************* */
/* All EW Average         */

%FINRATIO_ew_all (begdate=&uni_begdt, enddate=&uni_enddt, avr=median, input=da, vars=&vars, output=ew_all);

title "All Stocks Equal Weighted Average";
PROC PRINT DATA=ew_all(where=(year(public_date)=1976));RUN;

/* proc export data=ew_all outfile="ind_ew.csv"  */
/* dbms=csv replace;  */

proc sql;
create table tmp2
as
select a.*,
  b.mom12m_Median, b.hxz_abr_Median, b.hxz_sue_Median, b.hxz_re_Median,
  b.bm_Median, b.ep_Median, b.cfp_Median, b.sp_Median,
  b.agr_Median, b.ni_Median, b.acc_Median,
  b.op_Median, b.roe_Median,
  b.seas1a_Median, b.adm_Median, b.rdm_Median,
  b.me_Median, b.svar_Median, b.beta_Median, b.mom1m_Median
 from
tmp a left join ew_all b
on
a.public_date = b.public_date
order by cusip, public_date;
quit;

title "Merged Table 3";
PROC PRINT DATA=tmp2(obs=10);RUN;

data tmp2; set tmp2;

if missing(mom12m) then mom12m = mom12m_Median;
if missing(hxz_abr) then hxz_abr = hxz_abr_Median;
if missing(hxz_re) then hxz_re = hxz_re_Median;
if missing(hxz_sue) then hxz_sue = hxz_sue_Median;

if missing(bm) then bm = bm_Median;
if missing(cfp) then cfp = cfp_Median;
if missing(ep) then ep = ep_Median;
if missing(sp) then sp = sp_Median;

if missing(acc) then acc = acc_Median;
if missing(agr) then agr = agr_Median;
if missing(ni) then ni = ni_Median;

if missing(op) then op = op_Median;
if missing(roe) then roe = roe_Median;

if missing(seas1a) then seas1a = seas1a_Median;
if missing(adm) then adm = adm_Median;
if missing(rdm) then rdm = rdm_Median;

if missing(me) then me = me_Median;
if missing(mom1m) then mom1m = mom1m_Median;
if missing(beta) then beta = beta_Median;
if missing(svar) then svar = svar_Median;

run;

data tmp2; set tmp2;
drop
  mom12m_Median hxz_abr_Median hxz_sue_Median hxz_re_Median
  bm_Median ep_Median cfp_Median sp_Median
  agr_Median ni_Median acc_Median
  op_Median roe_Median
  seas1a_Median adm_Median rdm_Median
  me_Median svar_Median beta_Median mom1m_Median
run;

title "Merged Table 4";
PROC PRINT DATA=tmp2(obs=10);RUN;

/* proc export data=tmp2(where=(year(public_date)=1976)) outfile="tmp1976-2.csv" */
/* dbms=csv replace; */

/* ********************************************* */
/*  Save data                    */
/* ********************************************* */

data chars.firmchars_v7_2_final;
set tmp2;
run;
