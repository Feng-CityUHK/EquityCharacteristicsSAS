%include 'portfolio_chars.sas';

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

%let rank_vars =
rank_mom12m rank_hxz_abr rank_hxz_sue rank_hxz_re
rank_bm rank_ep rank_cfp rank_sp
rank_agr rank_ni rank_acc
rank_op rank_roe
rank_seas1a rank_adm rank_rdm
rank_me rank_svar rank_beta rank_mom1m
;

/* ********************************************* */
/*  Final results                     */
/* ********************************************* */

data da;
set chars.v7_1_rank_label;
keep
    public_date   permno   gvkey   sic  cusip
    &vars
    &rank_vars
    FFI49 FFI49_desc
    DGTW_PORT
    port_me port_beta port_svar;
run;

proc export data=da
outfile="/scratch/cityuhk/xinhe/eqchars/rank_final.csv" dbms=csv replace; run;

proc export data=da(where=(year(public_date)=2018))
outfile="/scratch/cityuhk/xinhe/eqchars/rank_final2018.csv" dbms=csv replace; run;


data da;
set da;
weight_port = me;
run;

data eqchars;
set da;
keep
    public_date   permno   gvkey   sic  cusip
    weight_port
    &vars
    &rank_vars
    FFI49 FFI49_desc
    DGTW_PORT
    port_me port_beta port_svar;
run;

/* ********************************************************************************* */


%FINRATIO_ind_label  (BEGDATE=&uni_begdt, ENDDATE=&uni_enddt, label=port_me, AVR=mean, weight=weight_port, Input=eqchars, vars=&vars);
%FINRATIO_ind_label  (BEGDATE=&uni_begdt, ENDDATE=&uni_enddt, label=port_svar, AVR=mean, weight=weight_port, Input=eqchars, vars=&vars);
%FINRATIO_ind_label  (BEGDATE=&uni_begdt, ENDDATE=&uni_enddt, label=port_beta, AVR=mean, weight=weight_port, Input=eqchars, vars=&vars);
%FINRATIO_ind_label  (BEGDATE=&uni_begdt, ENDDATE=&uni_enddt, label=FFI49_desc, AVR=mean, weight=weight_port, Input=eqchars, vars=&vars);
%FINRATIO_ind_label  (BEGDATE=&uni_begdt, ENDDATE=&uni_enddt, label=DGTW_PORT, AVR=mean, weight=weight_port, Input=eqchars, vars=&vars);
