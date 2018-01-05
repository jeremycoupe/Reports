import numpy as np
import pandas as pd

fileName = 'tactical_KCLT.flightSummary.v0.3.20171222.09.00-20171223.08.59.20171223.15.15.04.csv'

inputFile = 'opsSummaryDirectory/tacticalStitched/' + fileName
date = inputFile.split('.')[4]
dateStr = date[0:4] + '-' + date[4:6] + '-' + date[6:8]
outputFile = 'opsSummaryDirectory/summaryStats/test' + 'SummaryMeteringStats.' + fileName.split('.')[2] + '.' + str(dateStr) + '.csv'

### Read the summary data frame 
dfSummary = pd.read_csv(inputFile, sep=',',index_col=False)

### define column names for the dfStats data frame
cols = ['Date', 'Number Aircraft Subject to Surface Metering','Number Aircraft Held For Surface Metering', 'Sum of Total Realized Hold (Minutes)', \
'Average Realized Hold if Held for Surface Metering (Minutes)', 'Average Taxi Time if Held For Surface Metering' , 'Average Excess Taxi Time if Held For Surface Metering' , \
 'Total fuel saved (kg)', 'Total CO saved (g)','Total CO2 saved (kg)' , 'Total HC saved (g)', 'Total NOx saved (g)', 'Mean Advisory When Put on Hold (Minutes)', \
  'Median Advisory When Put on Hold (Minutes)', 'Max Advisory When Put on Hold (Minutes)', 'Mean Pass Back Delay When Put on Hold (Minutes)', \
   'Median Pass Back Delay When Put on Hold (Minutes)', 'Max Pass Back Delay When Put on Hold (Minutes)']

### define DFStats data frame
dfStats = pd.DataFrame(np.empty((1,len(cols)), dtype=object),columns=cols)

### initialize count of aircraft held at 0
countTotalHeldForMetering = 0
countTotalSubjectSurfaceMetering = 0

### define empty list to store data
totalRealizedHold = []
fuelSavedKG = []
coSavedGrams = []
co2SavedKG = []
hcSavedGrams = []
noxSavedGrams = []
passBackHoldVec_Ready = []
gateAdvisory_Ready = []
gateHoldTotalTaxiTime = []
gateHoldExcessTaxiTime = []
subjectExcessTaxiTime = []



### loop through all flights in the summary file looking for ones that meet the criteria
for flight in range(len(dfSummary['gufi'])):
	#### count if flight is subject to surface metering
	if str(dfSummary['Tactical_Controlled_Flight'][flight]) not in ['APREQ_DEPARTURE' , 'EDCT_DEPARTURE']:
		if str(dfSummary['Tactical_Exempt_Flight'][flight]) != 'EXEMPT_DEPARTURE':
			meteredRunways = str(dfSummary['Runway(s)_Being_Metered_At_Ready'][flight]).split(',')
			if dfSummary['Metering_Mode_At_Ready'][flight] == 'TIME_BASED_METERING':
				if dfSummary['Runway_Assigned_At_Ready'][flight] in meteredRunways:
					countTotalSubjectSurfaceMetering +=1
					#### output some things to the terminal for debug if needed
					# print(flight)
					if dfSummary['Track_Hit_Out_Time'][flight] == False:
						subjectExcessTaxiTime.append(dfSummary['Excess_Taxi_Time'][flight])


	### Filter flights to see if they were put on hold while metering was ON
	if str(dfSummary['Held_While_Metering_On_Scheduled_Runway'][flight]) == 'True':
		### Filter flights to see if there was an advisory when put on hold
		if str(dfSummary['Held_With_Non_Zero_Advisory'][flight]) == 'True':
			### Filter flights to exlude APREQ and EDCT and EXEMPT
			if str(dfSummary['Tactical_Controlled_Flight'][flight]) not in ['APREQ_DEPARTURE' , 'EDCT_DEPARTURE']:
				if str(dfSummary['Tactical_Exempt_Flight'][flight]) != 'EXEMPT_DEPARTURE':
						
					### do a check to see if aircraft return to gate after the last time put on hold
					stringPriority = dfSummary['Tactical_Schedule_Priroity_String'][flight].split('--')
					afterReady = False
					for ts in range(len(stringPriority)):
						
						if stringPriority[ts] == 'GATE_DEPARTURE_READY':
							afterReady = True
							countHoldStats = True
						if afterReady:
							if stringPriority[ts] in ['GATE_DEPARTURE_UNCERTAIN' , 'GATE_DEPARTURE_PLANNED']:
								countHoldStats = False
								#### output some things to the terminal for debug if needed
								# print(dfSummary['gufi'][flight])
								# print('IM_HERE')

					### for uncertain that call ready the UOBT is stale which can lead to negative values for TOBT - UOBT
					### if you find a negative value then the TOBT - UOBT should be equal to TOBT - CurrentTime so replace the 
					### passback hold with the total gate hold before computing statistics
					if dfSummary['Passback_Hold_When_Put_On_Hold'][flight] < 0:
						dfSummary['Passback_Hold_When_Put_On_Hold'][flight] = dfSummary['Total_Gate_Hold_When_Put_On_Hold'][flight]

					#countHoldStats = True
					#### count statistics for all aircraft held as long as they did not return to gate after the last time put on hold
					if countHoldStats:						
						#### output some things to the terminal for debug if needed
						print(dfSummary['gufi'][flight])
						# print(dfSummary['Total_Realized_Hold'][flight])
						# print(dfSummary['Passback_Hold_When_Put_On_Hold'][flight])
						
						#### Do the computations
						countTotalHeldForMetering +=1
						totalRealizedHold.append(dfSummary['Total_Realized_Hold'][flight])
						fuelSavedKG.append(dfSummary['Fuel_Flow_Reduced_in_KG'][flight])
						coSavedGrams.append(dfSummary['CO_Emissions_Reduced_in_grams'][flight])
						co2SavedKG.append(dfSummary['CO2_Emissions_Reduced_in_KG'][flight])
						hcSavedGrams.append(dfSummary['HC_Emissions_Reduced_in_grams'][flight])
						noxSavedGrams.append(dfSummary['Nox_Emissions_Reduced_in_grams'][flight])
						gateAdvisory_Ready.append(dfSummary['Total_Gate_Hold_When_Put_On_Hold'][flight])
						passBackHoldVec_Ready.append(dfSummary['Passback_Hold_When_Put_On_Hold'][flight])

						if dfSummary['Track_Hit_Out_Time'][flight] == False:
							if str(dfSummary['Excess_Taxi_Time'][flight]) != 'nan':
								gateHoldExcessTaxiTime.append(dfSummary['Excess_Taxi_Time'][flight])

							if str(dfSummary['Total_Taxi_Time'][flight]) != 'nan':
								gateHoldTotalTaxiTime.append(dfSummary['Total_Taxi_Time'][flight])



### compute sum of all hold in minutes
totalHoldMinutes = sum(totalRealizedHold) / float(60)

### compute average hold in minutes
averageHold = np.mean(totalHoldMinutes) 


### fill data in the dfStats data frame
dfStats['Date'][0] = dateStr
dfStats['Number Aircraft Subject to Surface Metering'][0] = countTotalSubjectSurfaceMetering
dfStats['Number Aircraft Held For Surface Metering'][0] = countTotalHeldForMetering
if countTotalHeldForMetering > 0:
	dfStats['Sum of Total Realized Hold (Minutes)'][0] = totalHoldMinutes
	dfStats['Average Realized Hold if Held for Surface Metering (Minutes)'][0] = totalHoldMinutes / float(countTotalHeldForMetering)
	dfStats['Total fuel saved (kg)'][0] = sum(fuelSavedKG)
	dfStats['Total CO saved (g)'][0] = sum(coSavedGrams)
	dfStats['Total CO2 saved (kg)'][0] = sum(co2SavedKG)
	dfStats['Total HC saved (g)'][0] = sum(hcSavedGrams)
	dfStats['Total NOx saved (g)'][0] = sum(noxSavedGrams)
	dfStats['Mean Advisory When Put on Hold (Minutes)'][0] = np.mean(gateAdvisory_Ready)/float(60)
	dfStats['Median Advisory When Put on Hold (Minutes)'] = np.median(gateAdvisory_Ready)/float(60)
	dfStats['Max Advisory When Put on Hold (Minutes)'][0] = max(gateAdvisory_Ready)/float(60)
	dfStats['Mean Pass Back Delay When Put on Hold (Minutes)'][0] = np.mean(passBackHoldVec_Ready)/float(60)
	dfStats['Median Pass Back Delay When Put on Hold (Minutes)'][0] = np.median(passBackHoldVec_Ready)/float(60)
	dfStats['Max Pass Back Delay When Put on Hold (Minutes)'][0] = max(passBackHoldVec_Ready)/float(60)
	dfStats['Average Taxi Time if Held For Surface Metering'][0] = np.mean(gateHoldTotalTaxiTime) 
	dfStats['Average Excess Taxi Time if Held For Surface Metering'][0] = np.mean(gateHoldExcessTaxiTime) 



dfStats.to_csv(outputFile,index = False)








