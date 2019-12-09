
%include 'portfolio_chars.sas';

%let uni_begdt = 01JAN1976;
%let uni_enddt = 31DEC2018;

/* ********************************************************************************* */
/* load lib */
libname chars '/scratch/cityuhk/xinhe_mandy/eqchars/';

%let vars =
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
  public_date   permno gvkey cusip
  &vars
  FFI5_desc   FFI5   FFI10_desc   FFI10   FFI12_desc   FFI12   FFI17_desc   FFI17
  FFI30_desc   FFI30   FFI38_desc   FFI38   FFI48_desc   FFI48  FFI49_desc   FFI49
;
run;

/* ********************************************************************************* */

%FINRATIO_ind  (BEGDATE=&uni_begdt, ENDDATE=&uni_enddt, NIND=49, AVR=mean, Input=eqchars, IndRatios=ind_output_ff49, vars=&vars);
proc export data=ind_output_ff49 outfile="/scratch/cityuhk/xinhe_mandy/sortport/ind_output_ff49.csv"
dbms=csv replace;
