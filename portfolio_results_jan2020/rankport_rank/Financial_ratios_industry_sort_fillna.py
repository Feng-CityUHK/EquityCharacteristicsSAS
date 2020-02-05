##############################################################################
# python 3.6
# Xin He, Ph.D. student, City University of Hong Kong
#
# Pivot firm level financial characteristics into industry level variables
# Also provide some other tools
##############################################################################
# Jan 25 2019
# original draft

##############################################################################
# Mar26 25 2019
# add ew all market characteristics for impute purposes

##############################################################################
import pandas as pd
import os
import numpy as np

class INDUSTRY_RATIO(object):
	def __init__(self, dire, file_name, d1, d2, ind_class):
		self.dire = dire
		self.file_name = file_name
		self.file_path = os.path.join(dire, file_name)
		self.d1 = d1
		self.d2 = d2               # '2018-12-05'
		self.ind_class = ind_class # str
		print('\n Initialization completed. \n')

	def Input_csv(self):
		print("Reading:\n"+self.file_path)
		self.data = pd.read_csv(self.file_path)

		self.variable_list = self.data.columns
		print('\n Variables: \n')
		print(self.variable_list)
		print('\n total {}'.format(len(self.variable_list)))

		self.name_list = list(set(self.data[self.ind_class]))
		print('\n Industries: \n')
		print(self.name_list)
		print('\n total {}'.format(len(self.name_list)))

		print('\n Input csv completed. \n')

	def Table_time_range(self):
		# define datetime, year, decade
		self.data['date'] = pd.to_datetime(self.data['public_date'])
		self.data['year'] = [i.year for i in self.data['date']]
		self.data['decade'] = [i.year - np.mod(i.year,10) for i in self.data['date']]
		# choose sub-dataframe by time range
		self.df = self.data[(self.data['date']>=self.d1) & (self.data['date']<=self.d2)]
		print('In time range '+self.d1+' to '+self.d2+'.')
		return(self.df)

	def Input_ewall(self,ewall_path):
		self.ewall_path = os.path.join(self.dire, ewall_path)
		self.ewall = pd.read_csv(self.ewall_path)

	def Table_ewall_time_range(self):
		self.ewall['date'] = pd.to_datetime(self.ewall['public_date'])
		self.ewall['year'] = [i.year for i in self.ewall['date']]
		self.ewall['decade'] = [i.year - np.mod(i.year,10) for i in self.ewall['date']]
		self.ewdf = self.ewall[(self.ewall['date']>=self.d1) & (self.ewall['date']<=self.d2)]
		print('In time range '+self.d1+' to '+self.d2+'.')
		# print("create ew all file to impute na's, shape of the table {}".format(self.ewdf.shape))
		return(0)

	def run(self):
		# second step initialized
		self.Input_csv()
		self.Table_time_range()

	def Table_industry(self, ind_name):
		# return a data.frame
		# col varibale names
		# row dates
		# data values of variables on given dates
		_df = self.df[self.df[self.ind_class]==ind_name]
		_df = _df.sort_values(by='date')
		return(_df)

	def Table_industry_year(self, ind_name, year):
		# year int
		_df = self.df[self.df[self.ind_class]==ind_name]
		_df = _df[_df['year']==year]
		_df = _df.sort_values(by='date')
		return(_df)

	def Table_industry_decade(self, ind_name, decade):
		# decade int
		_df = self.df[self.df[self.ind_class]==ind_name]
		_df = _df[_df['decade']==decade]
		_df = _df.sort_values(by='date')
		return(_df)

	def Na_rate_year(self, ind_name, year, ratio_list):
		# given ind_name, year
		# return nan percentage
		_df = self.Table_industry_year(ind_name, year)[ratio_list]
		return(_df.isnull().sum()/_df.shape[0])
		#print(_df.isnull().sum())

	def Na_rate_decade(self, ind_name, decade, ratio_list):
		# given ind_name, decade
		# return nan percentage
		_df = self.Table_industry_decade(ind_name, decade)[ratio_list]
		return(_df.isnull().sum()/_df.shape[0])

	def Table_na_rate_decade(self, name_list, decade_list, ratio_list):
		decade_table = pd.DataFrame()
		for decade in decade_list:
			print(decade)
			for ind_name in name_list:
				na_rate_decade = self.Na_rate_decade(ind_name, decade, ratio_list)
				na_rate_decade['ind'] = ind_name
				na_rate_decade['decade'] = decade
				decade_table = decade_table.append(pd.DataFrame(na_rate_decade).T)
		return(decade_table)

	def Table_var(self, var_name, start_date, end_date, fill):
		df1 = self.df[(self.df['date']>=start_date) & (self.df['date']<=end_date)]
		if fill==1:
			_df = pd.concat([df1,self.ewdf])
			_ind = 'FFI{}_desc'.format(25)
			_df[_ind] = _df[_ind].fillna('ewall')
		else:
			_df = df1

		pivot_table = _df.pivot(index='date', columns=self.ind_class, values=var_name)
		if fill==1:
			#pivot_table['ewall'] = pd.Series(self.ewdf[var_name])
			print(var_name)
			#print(self.ewdf[var_name].head())
			#print(self.ewdf[var_name].shape)
			#print(pivot_table.shape)
			#print(pivot_table.head())
			for i in pivot_table.columns:
				pivot_table[i] = pivot_table[i].fillna(pivot_table['ewall'])
			pivot_table = pivot_table.drop('ewall',1)
			return(pivot_table)
		else:
			# pivot_table = pivot_table.drop('ewall',1)
			return(pivot_table)




if __name__ == "__main__":
	dire = "ind_output"
	file_name = "ind_output_ff10.csv"
	d1 = '1973-01-01'
	d2 = '2017-12-31'
	ind_class = 'FFI10_desc'

	ir = INDUSTRY_RATIO(dire, file_name, d1, d2, ind_class)
	ir.run()
	names = ir.name_list
	print(names)
	ind = ir.Table_industry(names[0])
	print(ind.head(10))
	print(ind.tail(10))

	save_dire = "pivot_result/FF10"
	hxtb = ir.Table_var('dy_Mean', '1990-01-31', '2000-12-31')
	save_path = os.path.join(save_dire,'dy_Mean.csv')
	hxtb.to_csv(save_path)
	print(hxtb)



'''
	year = 1993 # int
	decade = 1990 # int
	ind_name = ir.name_list[0]
	ratio_list = ['pe_exi_Mean', 'ps_Mean',
       'pcf_Mean', 'evm_Mean', 'bm_Mean', 'ep_Mean', 'agr_Mean', 'CAPEI_Mean',
       'npm_Mean', 'opmad_Mean', 'gpm_Mean', 'ptpm_Mean', 'cfm_Mean',
       'roa_Mean', 'roe_Mean', 'roce_Mean', 'aftret_eq_Mean',
       'aftret_invcapx_Mean', 'aftret_equity_Mean', 'pretret_noa_Mean',
       'pretret_earnat_Mean', 'equity_invcap_Mean', 'debt_invcap_Mean',
       'totdebt_invcap_Mean', 'int_debt_Mean', 'int_totdebt_Mean',
       'cash_lt_Mean', 'invt_act_Mean', 'rect_act_Mean', 'debt_at_Mean',
       'short_debt_Mean', 'curr_debt_Mean', 'lt_debt_Mean', 'fcf_ocf_Mean',
       'adv_sale_Mean', 'profit_lct_Mean', 'debt_ebitda_Mean', 'ocf_lct_Mean',
       'lt_ppent_Mean', 'dltt_be_Mean', 'debt_assets_Mean',
       'debt_capital_Mean', 'de_ratio_Mean', 'cash_ratio_Mean',
       'quick_ratio_Mean', 'curr_ratio_Mean', 'capital_ratio_Mean',
       'cash_debt_Mean', 'at_turn_Mean', 'rect_turn_Mean', 'pay_turn_Mean',
       'sale_invcap_Mean', 'sale_equity_Mean', 'sale_nwc_Mean', 'rd_sale_Mean',
       'accrual_Mean', 'GProf_Mean', 'BE_Mean', 'cash_conversion_Mean',
       'intcov_ratio_Mean', 'staff_sale_Mean', 'ptb_Mean', 'dy_Mean']
	decade_list = list(range(1960, 2018, 10))
	print(decade_list)

	decade_table = ir.Table_na_rate_decade(names, decade_list, ratio_list)
	decade_table.to_csv('decade_na_ratio.csv')



	na_year = ir.Na_rate_year(ind_name, year, ratio_list)
	print('\n year test \n')
	print(na_year)

	na_decade = ir.Na_rate_decade(ind_name, decade, ratio_list)
	print('\n decade test\n')
	print(na_decade)

	na_decade['ind'] = ind_name
	na_decade['decade'] = decade
	print(na_decade)
'''
