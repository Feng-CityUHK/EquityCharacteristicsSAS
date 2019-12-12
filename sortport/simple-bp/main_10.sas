
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

/* ME 1x10 */
%DIVIDE_PORTFOLIO_10_ME(BEGDATE=&uni_begdt, ENDDATE=&uni_enddt, TABLE_OUT=portfolio_label_25_ME);
%FINRATIO_firm_add_ind_25 (BEGDATE=&uni_begdt, ENDDATE=&uni_enddt, in=eqchars, portfolio_label=portfolio_label_25_ME, OUT=firm_output_ME);
%FINRATIO_ind  (BEGDATE=&uni_begdt, ENDDATE=&uni_enddt, NIND=25, AVR=mean, Input=firm_output_ME, IndRatios=ind_output_ff25, vars=&rank_vars);
proc export data=ind_output_ff25 outfile="/scratch/cityuhk/xinhe_mandy/sortport_simple_bp/ind_output_sort10_ME.csv" dbms=csv replace; run;
proc sql; drop table portfolio_label_25_ME, firm_output_ME, ind_output_ff25;quit;

/* BETA 1x10 */
%DIVIDE_PORTFOLIO_10_BETA(BEGDATE=&uni_begdt, ENDDATE=&uni_enddt, TABLE_OUT=portfolio_label_25_BETA);
%FINRATIO_firm_add_ind_25 (BEGDATE=&uni_begdt, ENDDATE=&uni_enddt, in=eqchars, portfolio_label=portfolio_label_25_BETA, OUT=firm_output_BETA);
%FINRATIO_ind  (BEGDATE=&uni_begdt, ENDDATE=&uni_enddt, NIND=25, AVR=mean, Input=firm_output_BETA, IndRatios=ind_output_ff25, vars=&rank_vars);
proc export data=ind_output_ff25 outfile="/scratch/cityuhk/xinhe_mandy/sortport_simple_bp/ind_output_sort10_BETA.csv" dbms=csv replace; run;
proc sql; drop table portfolio_label_25_BETA, firm_output_BETA, ind_output_ff25;quit;

/* SVAR 1x10 */
%DIVIDE_PORTFOLIO_10_SVAR(BEGDATE=&uni_begdt, ENDDATE=&uni_enddt, TABLE_OUT=portfolio_label_25_SVAR);
%FINRATIO_firm_add_ind_25 (BEGDATE=&uni_begdt, ENDDATE=&uni_enddt, in=eqchars, portfolio_label=portfolio_label_25_SVAR, OUT=firm_output_SVAR);
%FINRATIO_ind  (BEGDATE=&uni_begdt, ENDDATE=&uni_enddt, NIND=25, AVR=mean, Input=firm_output_SVAR, IndRatios=ind_output_ff25, vars=&rank_vars);
proc export data=ind_output_ff25 outfile="/scratch/cityuhk/xinhe_mandy/sortport_simple_bp/ind_output_sort10_SVAR.csv" dbms=csv replace; run;
proc sql; drop table portfolio_label_25_SVAR, firm_output_SVAR, ind_output_ff25;quit;
