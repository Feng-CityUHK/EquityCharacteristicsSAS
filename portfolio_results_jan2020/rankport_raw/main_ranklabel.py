
import pandas as pd
import os
import numpy as np

# Customized tools
#from na_tool import na_percentage
from Financial_ratios_industry_sort_fillna import *

######################################################################

d1 = '1976-01-01'
d2 = '2018-12-31'
decade_list = list(range(1960, 2018, 10))
print(decade_list)

######################################################################

# # na ratios

# for ind_class_num in [10,30,49]:
# 	dire = "C:\\Users\\xinhe28\\Desktop\\v3_fill_miss"
# 	#file_name = "ind_output_ff{}.csv".format(ind_class)
# 	file_name = "ind_ratios_ff{}.csv".format(ind_class_num)
# 	ind_class = 'FFI{}_desc'.format(ind_class_num)

# 	ir = INDUSTRY_RATIO(dire, file_name, d1, d2, ind_class)
# 	ir.run()
# 	name_list = ir.name_list
# 	print(name_list)

# 	decade_table = ir.Table_na_rate_decade(name_list, decade_list, ratio_list)
# 	decade_table.to_csv(os.path.join(dire,'decade_na_ratio{}.csv'.format(ind_class)))

######################################################################

# pivot table

#target_var_list = ['bm_Mean', 'ep_Mean', 'agr_Mean', 'roe_Mean', 'dy_Mean', 'invt_act_Mean', 'accrual_Mean', 'GProf_Mean', 'capital_ratio_Mean', 'at_turn_Mean']
#target_var_list = ratio_list

target_var_list = \
        ['mom12m_Mean', 'hxz_abr_Mean', 'hxz_sue_Mean', 'hxz_re_Mean',
        'bm_Mean', 'ep_Mean', 'cfp_Mean', 'sp_Mean',
        'agr_Mean', 'ni_Mean', 'acc_Mean',
        'op_Mean', 'roe_Mean',
        'seas1a_Mean', 'adm_Mean', 'rdm_Mean',
        'me_Mean', 'svar_Mean', 'beta_Mean', 'mom1m_Mean']

#ind_class_num = 10

#for ind_class_num in [10,30,49]:
for ind_class_sorts in ['port_me','port_beta','port_svar','FFI49_desc','DGTW_PORT']:
#for ind_class_sorts in ['BM']:
	print('\n')
	print(ind_class_sorts)
	print('\n')
	dire = "ind_output" #dire = "C:\\Users\\xinhe28\\Desktop\\v3_fill_miss"
	file_name = "indratios_{}.csv".format(ind_class_sorts)
	#file_name = "ind_output_ff{}.csv".format(ind_class)
	ind_class = ind_class_sorts
	save_dire = "pivot_result/ranklabel_{}".format(ind_class_sorts)
	if not os.path.exists(save_dire):
		os.mkdir(save_dire)

	ir = INDUSTRY_RATIO(dire, file_name, d1, d2, ind_class)
	ir.run()
	#ir.Input_ewall('ind_output_all.csv')
	#ir.Table_ewall_time_range()


	for var_name in target_var_list:
		_df = ir.Table_var(var_name, d1, d2, fill=0)
		save_path = os.path.join(save_dire,var_name+'.csv')
		_df.to_csv(save_path)
		print(var_name)
