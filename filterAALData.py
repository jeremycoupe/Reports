import psycopg2
import pandas.io.sql as psql
import numpy as np
import pandas as pd
import os
import time

files = os.listdir('opsSummaryDirectory/tacticalStitched')

# fileName = 'tacticalStitchedKCLT.' + 'flightSummary.20171210.09.00-20171211.08.59.20171211.15.15.05.csv'
# file2 = 'metered.AAL.' + 'flightSummary.20171210.09.00-20171211.08.59.20171211.15.15.05.csv'

#inputFile = 'opsSummary/' + fileName
#outputFile = 'opsSummary/' + file2


for date in range(len(files)):
	if 'tactical' in files[date]:
		print(files[date])
		inputFile = 'opsSummaryDirectory/tacticalStitched/' + files[date]
		outputFile = '~/Desktop/meterFilter/filtered.AAL' 
		
		for i in range(1,len(files[date].split('.'))):
			outputFile = outputFile + '.' +  str(files[date].split('.')[i])
		df0 = pd.read_csv(inputFile, sep=',',index_col=False)
		dfMetered = pd.DataFrame(columns=df0.columns)
		dfMetered = dfMetered.rename(columns={'Runway_Assigned_At_': 'Runway_Assigned_At_Ready'})

		for flight in range(len(df0['gufi'])):
			callsign = str(df0['acid'][flight])[0:3]
			if callsign in ['AAL', 'ASQ', 'AWI', 'ENY', 'JIA', 'PDT', 'RPA']:
				terminal = str(df0['departure_stand_airline'][flight])[0]
				if terminal in ['B','C','D','E']:
					
					if str(df0['Number_Times_In_PUSHBACK_UNCERTAIN'][flight]) == 'nan':
						df0['Number_Times_In_PUSHBACK_UNCERTAIN'][flight] = 1
					else:
						df0['Number_Times_In_PUSHBACK_UNCERTAIN'][flight] +=1


					
					dfMetered = dfMetered.append(df0.loc[flight,:], ignore_index=True)





		dfMetered.to_csv(outputFile,index = False)

