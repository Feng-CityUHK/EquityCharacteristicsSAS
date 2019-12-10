%include 'sort10_size_XX.sas';
%include 'portfolio_chars.sas';

%let uni_begdt = 01JAN1976;
%let uni_enddt = 31DEC2018;

/* ********************************************************************************* */
/* load lib */
libname chars '/scratch/cityuhk/xinhe_mandy/eqchars/';

%let vars =
mom12m hxz_abr hxz_sue hxz_re
bm ep cfp sp
agr ni acc
op roe
seas1a adm rdm
me svar beta mom1m
;

%let rank_vars =
rank_mom12m rank_hxz_abr rank_hxz_sue rank_hxz_re
rank_bm rank_ep rank_cfp rank_sp
rank_agr rank_ni rank_acc
rank_op rank_roe
rank_seas1a rank_adm rank_rdm
rank_me rank_svar rank_beta rank_mom1m

;

/* 25 characteristcs & industry label */
data eqchars;
set chars.v7_1_rank;
keep
  public_date   permno gvkey cusip sic
  &vars &rank_vars
  FFI5_desc   FFI5   FFI10_desc   FFI10   FFI12_desc   FFI12   FFI17_desc   FFI17
  FFI30_desc   FFI30   FFI38_desc   FFI38   FFI48_desc   FFI48  FFI49_desc   FFI49
;
run;

/* ********************************************************************************* */

data dgtw;
set chars.dgtw;
rename dgtw_port=myind25;
mdate = INTNX('month',date,-1,'end');
run;

proc print data=dgtw(obs=100);run;

%FINRATIO_firm_add_ind_25 (BEGDATE=&uni_begdt, ENDDATE=&uni_enddt, in=eqchars, portfolio_label=dgtw, OUT=firm_output_dgtw);
%FINRATIO_ind  (BEGDATE=&uni_begdt, ENDDATE=&uni_enddt, NIND=25, AVR=mean, Input=firm_output_dgtw, IndRatios=ind_output_ff25, vars=&rank_vars);
proc export data=ind_output_ff25 outfile="/scratch/cityuhk/xinhe_mandy/sortport/ind_output_dgtw.csv" dbms=csv replace; run;
proc sql; drop table dgtw, firm_output_dgtw, ind_output_ff25;quit;
