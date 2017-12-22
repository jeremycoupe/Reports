import psycopg2
import pandas.io.sql as psql
import numpy as np
import pandas as pd
import mops_emission as em
em.init_emission('fuel_and_emission_table.csv')

def fTactical(fileName):
	advisoryThreshold = 60

	### This is the file name that you want to read in 
	#fileName = 'KCLT.flightSummary.v0.3.20171218.09.00-20171219.09.00.20171221.03.52.12.csv'
	#fileName = 'KCLT.flightSummary.20171207.09.00-20171208.08.59.20171208.15.15.04.csv'

	### this will point you to the correct directory that you want to load the file from
	inputFileWithDirectory = 'opsSummaryDirectoy/originalSummary/' + fileName

	### This is the output file name to save
	outputFileWithDirectory = 'opsSummaryDirectoy/tacticalStitched/' + fileName

	#### Read summary table to get data about flights that you want to 
	#### stitch tactical data too
	dfSummary = pd.read_csv(inputFileWithDirectory, sep=',',index_col=False)


	### define a function to get the actual off EPOCH
	def fGetOffEPOCH(conn,gufi,acid,matm_flight_off):

		q = '''SELECT
		extract( EPOCH from departure_runway_actual_time) as actualoff,
		extract( EPOCH from departure_stand_actual_time) as actualout
		FROM
		matm_flight_summary
		WHERE
		gufi = '%s'
		''' %(gufi)
		df = psql.read_sql(q, conn)

		if df['actualoff'][0] > 0:
			return [df['actualoff'][0],df['actualout'][0]]
		else:
			q2 = '''SELECT
			extract( EPOCH from departure_runway_actual_time) as actualoff,
			extract( EPOCH from departure_stand_actual_time) as actualout
			FROM
			matm_flight_summary
			WHERE
			acid = '%s'
			and departure_runway_actual_time > TIMESTAMP '%s' AT TIME ZONE 'UTC' - INTERVAL '3 MINUTES'
			and departure_runway_actual_time < TIMESTAMP '%s' AT TIME ZONE 'UTC' + INTERVAL '3 MINUTES'
			''' %(acid,matm_flight_off,matm_flight_off)
			df2 = psql.read_sql(q2, conn)
			if (len(df2['actualoff']) > 0) and (df2['actualoff'][0] > 0):
				return [df2['actualoff'][0],df2['actualout'][0]]
			else:
				return [False,False]

	def fGetReadyIndex(df):
		getReadyIndex = True
		trackHitOut = False
		returnToGateFlag = False
		readyIndex = -1
		for idx in range(len(df['model_schedule_state'])):

			if df['model_schedule_state'][idx] == 'PUSHBACK_READY':
				if getReadyIndex == True:
					readyIndex = idx-1
					getReadyIndex = False
					returnToGateFlag = False

			if df['model_schedule_state'][idx] == 'OUT':
				if getReadyIndex == True:
					readyIndex = idx-1
					getReadyIndex = False

			if df['model_schedule_state'][idx] == 'TAXI':
				if getReadyIndex == True:
					readyIndex = idx-1
					getReadyIndex = False
					trackHitOut = True

			if getReadyIndex == False:
				if df['model_schedule_state'][idx] in ['PUSHBACK_UNCERTAIN' , 'PUSHBACK_PLANNED']:
					getReadyIndex = True
					trackHitOut = False
					returnToGateFlag = True
					readyIndex = -1
		return [readyIndex,trackHitOut,returnToGateFlag]

	def fGetHoldData(dfSummary,mss,df,dfGate,readyIndex,ttotTransitionIn,utotTransitionIn):
		
		if readyIndex == -1:
			getReadyIndex = True
			for idx in range(len(df['model_schedule_state'])):
				if df['model_schedule_state'][idx] == 'PUSHBACK_READY':
					if getReadyIndex == True:
						readyIndex = idx-1
						getReadyIndex = False

		
		if df['model_schedule_state'][readyIndex] == 'PUSHBACK_PLANNED':
			ttotTransitionIn[mss] = str(df['scheduled_time'][readyIndex])
			utotTransitionIn[mss] = str(df['estimated_time'][readyIndex])
			readyTime = df['timenow'][readyIndex]
			preReadyUOBT = dfGate['gateuobt'][readyIndex]
			advisory = dfGate['gatetobt'][readyIndex] - dfGate['timenow'][readyIndex]
			gateHold = dfGate['gatetobt'][readyIndex] - preReadyUOBT

		if df['model_schedule_state'][readyIndex] == 'PUSHBACK_UNCERTAIN':
			ttotTransitionIn[mss] = str(df['scheduled_time'][readyIndex+2])
			utotTransitionIn[mss] = str(df['estimated_time'][readyIndex+2])
			readyTime = df['timenow'][readyIndex]
			preReadyUOBT = dfGate['gateuobt'][readyIndex+2]
			advisory = dfGate['gatetobt'][readyIndex+2] - dfGate['timenow'][readyIndex]
			gateHold = dfGate['gatetobt'][readyIndex+2] - preReadyUOBT

		return [ttotTransitionIn,utotTransitionIn,readyTime,preReadyUOBT,advisory,gateHold]


	def fWriteReady(dfSummary,df,dfGate,readyIndex,off_epoch):
		if readyIndex != -1:
			try:
				if dfGate['eta_msg_time'][readyIndex] == df['eta_msg_time'][readyIndex]:
					
					if df['model_schedule_state'][readyIndex] == 'PUSHBACK_PLANNED':
						advisoryReady = dfGate['gatetobt'][readyIndex] - dfGate['timenow'][readyIndex]
						gateHoldReady = dfGate['gatetobt'][readyIndex] - dfGate['gateuobt'][readyIndex]
						dfSummary['TTOT_At_Ready'][flight] = str(df['scheduled_time'][readyIndex])
						dfSummary['UTOT_At_Ready'][flight] = str(df['estimated_time'][readyIndex])
						dfSummary['TTOT_At_Ready_Versus_Actual_Off'][flight] = df['runwayttot'][readyIndex] - off_epoch
					else:
						advisoryReady = dfGate['gatetobt'][readyIndex+2] - dfGate['timenow'][readyIndex]
						gateHoldReady = dfGate['gatetobt'][readyIndex+2] - dfGate['timenow'][readyIndex]
						dfSummary['TTOT_At_Ready'][flight] = str(df['scheduled_time'][readyIndex+2])
						dfSummary['UTOT_At_Ready'][flight] = str(df['estimated_time'][readyIndex+2])
						dfSummary['TTOT_At_Ready_Versus_Actual_Off'][flight] = df['runwayttot'][readyIndex+2] - off_epoch


					dfSummary['Total_Gate_Hold_At_Ready'][flight] = advisoryReady
					dfSummary['Passback_Gate_Hold_At_Ready'][flight] = gateHoldReady	
					dfSummary['Runway_Assigned_At_Ready'][flight] = str(df['runway'][readyIndex])
					dfSummary['Runway(s)_Being_Metered_At_Ready'][flight] = str(df['metering_display'][readyIndex])
					dfSummary['Metering_Mode_At_Ready'][flight] = str(df['metering_mode'][readyIndex])
					dfSummary['Timestamp_At_Ready'][flight] = str(df['eta_msg_time'][readyIndex])

			except:
				pass

		else:
			dfSummary['TTOT_At_Ready'][flight] = 'NA'
			dfSummary['UTOT_At_Ready'][flight] = 'NA'
			dfSummary['Total_Gate_Hold_At_Ready'][flight] = 'NA'
			dfSummary['Passback_Gate_Hold_At_Ready'][flight] = 'NA'
			dfSummary['TTOT_At_Ready_Versus_Actual_Off'][flight] = 'NA'
			dfSummary['Runway_Assigned_At_Ready'][flight] = 'NA'
			dfSummary['Runway(s)_Being_Metered_At_Ready'][flight] = 'NA'
			dfSummary['Metering_Mode_At_Ready'][flight] = 'NA'
			dfSummary['Timestamp_At_Ready'][flight] = 'NA'


		return dfSummary

	#### Connect to the database on the localhost
	#### assumes you have set up tunnel using ssh -fNL 5432:clt-stbo-ops:5432 username@lz35
	print('Attempting to connect to database')

	### use this connection if you are connected to OPS
	#conn = psycopg2.connect("dbname='fuser' user='fuser' password='fuser' host='localhost'  ")

	### use this connection if you are connected to Fuser Warehouse
	conn = psycopg2.connect("dbname='fuserclt' user='fuserclt' password='fuserclt' host='localhost'  ")


	#### Define model schedule states you want to get scheduler data for
	modelVector = [ 'PUSHBACK_UNCERTAIN' , 'PUSHBACK_PLANNED' , 'PUSHBACK_READY' ]


	### Save empty array of objects to use as column head and
	### key when saving data to the data frame (dfSummary)

	### transition IN data
	str_transitions_IN = np.empty(len(modelVector), dtype=object)
	str_ttot_IN = np.empty(len(modelVector), dtype=object)
	str_utot_IN = np.empty(len(modelVector), dtype=object)
	str_advisory_IN = np.empty(len(modelVector), dtype=object)
	str_gatehold_IN = np.empty(len(modelVector), dtype=object)
	str_accuracy_IN = np.empty(len(modelVector), dtype=object)
	str_runway_IN = np.empty(len(modelVector), dtype=object)
	str_meter_IN = np.empty(len(modelVector), dtype=object)
	str_meterMode_IN = np.empty(len(modelVector), dtype=object)
	str_eta_IN = np.empty(len(modelVector), dtype=object)

	### transition OUT data
	str_ttot_OUT = np.empty(len(modelVector), dtype=object)
	str_utot_OUT = np.empty(len(modelVector), dtype=object)
	str_advisory_OUT = np.empty(len(modelVector), dtype=object)
	str_gatehold_OUT = np.empty(len(modelVector), dtype=object)
	str_accuracy_OUT = np.empty(len(modelVector), dtype=object)
	str_runway_OUT = np.empty(len(modelVector), dtype=object)
	str_meter_OUT = np.empty(len(modelVector), dtype=object)
	str_meterMode_OUT = np.empty(len(modelVector), dtype=object)
	str_eta_OUT = np.empty(len(modelVector), dtype=object)
	str_controllerhold_OUT = np.empty(len(modelVector), dtype=object)


	### define new column names to summary df
	dfSummary['Tactical_Aircraft_Type'] = ""
	dfSummary['Tactical_Weight_Class'] = ""
	dfSummary['Tactical_Controlled_Flight'] = ""
	dfSummary['Tactical_Exempt_Flight'] = ""
	dfSummary['Held_While_Metering_On_Scheduled_Runway'] = ""
	dfSummary['Held_With_Non_Zero_Advisory'] = ""
	dfSummary['Held_With_Non_Zero_(TOBT-UOBT)'] = ""
	dfSummary['Return_To_Gate_After_Hold'] = ""
	dfSummary['Total_Realized_Hold'] = ""
	dfSummary['Realized_Hold_BEFORE_UOBT'] = ""
	dfSummary['Realized_Hold_AFTER_UOBT'] = ""
	dfSummary['Tactical_Schedule_Priroity_String'] = ""
	dfSummary['MSS_Transition_String'] = ""


	### for each mss define a string that will be used
	### as dfSummary column name and can be used as a key
	### to access and save the data
	for mss in range(len(modelVector)):
		### make string for transition in to mss
		if modelVector[mss] in ['PUSHBACK_UNCERTAIN','PUSHBACK_PLANNED']:
			str_transitions_IN[mss] = 'Number_Times_In_' + modelVector[mss] 
			str_ttot_IN[mss] = 'TTOT_When_Enter_' + modelVector[mss] 
			str_utot_IN[mss] = 'UTOT_When_Enter_' + modelVector[mss] 
			str_advisory_IN[mss] = 'Total_Gate_Hold_When_Enter_' + modelVector[mss] 
			str_gatehold_IN[mss] = 'Passback_Hold_When_Enter_' + modelVector[mss] 
			str_accuracy_IN[mss] = 'TTOT_When_Enter_' + modelVector[mss] + '_Versus_Actual_OFF'
			str_runway_IN[mss] = 'Runway_Assigned_When_Enter_' + modelVector[mss] 
			str_meter_IN[mss] = 'Runway(s)_Being_Metered_When_Enter_' + modelVector[mss] 
			str_meterMode_IN[mss] = 'Metering_Mode_When_Enter_' + modelVector[mss] 
			str_eta_IN[mss] = 'Timestamp_When_Enter_' + modelVector[mss] 
			### make string for transition out of mss
			str_ttot_OUT[mss] = 'TTOT_When_Exit_' + modelVector[mss] 
			str_utot_OUT[mss] = 'UTOT_When_Exit_' + modelVector[mss] 
			str_advisory_OUT[mss] = 'Total_Gate_Hold_When_Exit_' + modelVector[mss]
			str_gatehold_OUT[mss] = 'Passback_Hold_When_Exit_' + modelVector[mss]
			str_accuracy_OUT[mss] = 'TTOT_When_Exit_' + modelVector[mss] + '_Versus_Actual_OFF'
			str_runway_OUT[mss] = 'Runway_Assigned_When_Exit_' + modelVector[mss]
			str_meter_OUT[mss] = 'Runway(s)_Being_Metered_When_Exit_' + modelVector[mss] 
			str_meterMode_OUT[mss] = 'Metering_Mode_When_Exit_' + modelVector[mss]
			str_eta_OUT[mss] = 'Timestamp_When_Exit_' + modelVector[mss]

		if modelVector[mss] == 'PUSHBACK_READY':
			#### write in new columns when aircraft is put on hold
			dfSummary['Timestamp_At_Ready'] = ""
			dfSummary['TTOT_At_Ready'] = ""
			dfSummary['UTOT_At_Ready'] = ""
			dfSummary['Total_Gate_Hold_At_Ready'] = ""
			dfSummary['Passback_Gate_Hold_At_Ready'] = ""
			dfSummary['TTOT_At_Ready_Versus_Actual_Off'] = ""
			dfSummary['Runway_Assigned_At_Ready'] = ""
			dfSummary['Runway(s)_Being_Metered_At_Ready'] = ""
			dfSummary['Metering_Mode_At_Ready'] = ""


			str_transitions_IN[mss] = 'Number_Times_Held'
			str_ttot_IN[mss] = 'TTOT_When_Put_On_Hold'
			str_utot_IN[mss] = 'UTOT_When_Put_On_Hold'
			str_advisory_IN[mss] = 'Total_Gate_Hold_When_Put_On_Hold'
			str_gatehold_IN[mss] = 'Passback_Hold_When_Put_On_Hold'
			str_accuracy_IN[mss] = 'TTOT_When_Put_On_Hold_Versus_Actual_OFF'
			str_runway_IN[mss] = 'Runway_Assigned_When_Put_On_Hold' 
			str_meter_IN[mss] = 'Runway(s)_Being_Metered_When_Put_On_Hold'
			str_meterMode_IN[mss] = 'Metering_Mode_When_Put_On_Hold' 
			str_eta_IN[mss] = 'Timestamp_When_Put_On_Hold'
		
		### Make new columns in dfSummary	
		dfSummary[str_transitions_IN[mss]] = ""
		dfSummary[str_eta_IN[mss]] = ""
		dfSummary[str_ttot_IN[mss]] = ""
		dfSummary[str_utot_IN[mss]] = ""
		if modelVector[mss] in ['PUSHBACK_PLANNED','PUSHBACK_READY']:
			dfSummary[str_advisory_IN[mss]] = ""
			dfSummary[str_gatehold_IN[mss]] = ""
		dfSummary[str_accuracy_IN[mss]] = ""
		dfSummary[str_runway_IN[mss]] = ""
		dfSummary[str_meter_IN[mss]] = ""
		dfSummary[str_meterMode_IN[mss]] = ""
		
		if modelVector[mss] in ['PUSHBACK_UNCERTAIN','PUSHBACK_PLANNED']:
			dfSummary[str_eta_OUT[mss]] = ""
			dfSummary[str_ttot_OUT[mss]] = ""
			dfSummary[str_utot_OUT[mss]] = ""
			if modelVector[mss] == 'PUSHBACK_PLANNED':
				dfSummary[str_advisory_OUT[mss]] = ""
				dfSummary[str_gatehold_OUT[mss]] = ""
			dfSummary[str_accuracy_OUT[mss]] = ""
			dfSummary[str_runway_OUT[mss]] = ""
			dfSummary[str_meter_OUT[mss]] = ""
			dfSummary[str_meterMode_OUT[mss]] = ""
		
	dfSummary['Fuel_Flow_Reduced_in_KG'] = ""
	dfSummary['CO_Emissions_Reduced_in_grams'] = ""
	dfSummary['CO2_Emissions_Reduced_in_KG'] = ""
	dfSummary['HC_Emissions_Reduced_in_grams'] = ""
	dfSummary['Nox_Emissions_Reduced_in_grams'] = ""
	dfSummary['Track_Hit_Out_Time'] = ""
	dfSummary['Unimpeded_Taxi_Time'] = ""
	dfSummary['Total_Taxi_Time'] = ""
	dfSummary['Excess_Taxi_Time'] = ""


	#### Enter loop: for every flight you will query the database
	#### and get data from the tactical scheduler

	#for flight in range(len(dfSummary['gufi'])):
	for flight in range(243,248):#range(len(dfSummary['gufi'])):
	#for flight in range(796,806):#range(len(dfSummary['gufi'])):

		if str(dfSummary['isDeparture'][flight]) == str('True'):
			actualOffTime = dfSummary['departure_runway_actual_time'][flight]
			
			#### output data to terminal to see the progress
			print('\n')
			print(flight)
			print(dfSummary['gufi'][flight])

			if str(actualOffTime) != 'nan':
				### Query the tactical tables for the relevant RUNWAY data
				q = '''SELECT
				tf.flight_key, tf.general_stream, tfs.schedule_priority, tfs.model_schedule_state, 
				trun.runway, tg.gate, smd.eta_msg_time, rt.scheduled_time, rt.estimated_time,
				extract(EPOCH from rt.scheduled_time) as runwayttot,
				extract(EPOCH from rt.estimated_time) as runwayutot,
				extract(EPOCH from smd.eta_msg_time) as timenow,
				smd.metering_display, smd.metering_mode, tad.ac_type, tad.weight_class
				FROM
				tactical_route_times rt
				INNER JOIN tactical_flight tf ON rt.tactical_flight_id = tf.id
				INNER JOIN tactical_fix tfix ON rt.tactical_fix_schedule_id = tfix.id
				INNER JOIN tactical_route_info ri ON rt.tactical_route_info_id = ri.id
				INNER JOIN tactical_runway trun ON ri.tactical_runway_id = trun.id
				INNER JOIN tactical_schedule_meta_data smd ON rt.tactical_schedule_meta_data_id = smd.id
				INNER JOIN tactical_flight_state tfs on rt.tactical_flight_state_id = tfs.id
				INNER JOIN tactical_gate tg on ri.tactical_gate_id = tg.id
				INNER JOIN tactical_aircraft_data tad on rt.tactical_aircraft_data_id = tad.id
				WHERE
				smd.eta_msg_time > TIMESTAMP '%s' AT TIME ZONE 'UTC' - INTERVAL '180 MINUTES'
				and smd.eta_msg_time < TIMESTAMP '%s' AT TIME ZONE 'UTC' + INTERVAL '15 MINUTES'
				and tf.flight_key = '%s'
				and tfix.fix = trun.runway
				order by smd.eta_msg_time 
				''' % (actualOffTime,actualOffTime,dfSummary['gufi'][flight])

				df = psql.read_sql(q, conn)
				#### save stuff for debug purposes if needed
				# print(df)
				# stSave = 'confirmData/debugRunwayData' + str(flight) + '.csv'
				# df.to_csv(stSave)


				### add hook incase you cant find flight
				if len(df.scheduled_time) > 0:
					### Query the tactical tables for the relevant GATE data
					qGate = '''SELECT
					tf.flight_key, tf.general_stream, tfs.schedule_priority, tfs.model_schedule_state, 
					trun.runway, tg.gate, smd.eta_msg_time, rt.scheduled_time, rt.estimated_time, tfix.fix,
					extract(EPOCH from rt.scheduled_time) as gatetobt,
					extract(EPOCH from rt.estimated_time) as gateuobt,
					extract(EPOCH from smd.eta_msg_time) as timenow,
					smd.metering_display, smd.metering_mode
					FROM
					tactical_route_times rt
					INNER JOIN tactical_flight tf ON rt.tactical_flight_id = tf.id
					INNER JOIN tactical_fix tfix ON rt.tactical_fix_schedule_id = tfix.id
					INNER JOIN tactical_route_info ri ON rt.tactical_route_info_id = ri.id
					INNER JOIN tactical_runway trun ON ri.tactical_runway_id = trun.id
					INNER JOIN tactical_schedule_meta_data smd ON rt.tactical_schedule_meta_data_id = smd.id
					INNER JOIN tactical_flight_state tfs on rt.tactical_flight_state_id = tfs.id
					INNER JOIN tactical_gate tg on ri.tactical_gate_id = tg.id
					WHERE
					smd.eta_msg_time > TIMESTAMP '%s' AT TIME ZONE 'UTC' - INTERVAL '180 MINUTES'
					and smd.eta_msg_time < TIMESTAMP '%s' AT TIME ZONE 'UTC' + INTERVAL '15 MINUTES'
					and tf.flight_key = '%s'
					and tfix.fix = tg.gate
					order by smd.eta_msg_time 
					''' % (actualOffTime,actualOffTime,dfSummary['gufi'][flight])

					dfGate = psql.read_sql(qGate, conn)
					#### save stuff for debug purposes if needed
					# print(dfGate)
					# stSave = 'confirmData/debugGateData' + str(flight) + '.csv'
					# dfGate.to_csv(stSave)

					### get epoch from actual take off time 
					[off_epoch,out_epoch] = fGetOffEPOCH(conn,dfSummary['gufi'][flight],dfSummary['acid'][flight],actualOffTime)
					
					if (off_epoch and out_epoch) != False:
						print(off_epoch)
						print(out_epoch)
						realizedTaxiTime = (off_epoch - out_epoch) / float(60)
						dfSummary['Total_Taxi_Time'][flight] = realizedTaxiTime

					dfSummary['Tactical_Aircraft_Type'][flight] = df['ac_type'][0]
					dfSummary['Tactical_Weight_Class'][flight] = df['weight_class'][0]
					

					# getReadyIndex = True
					# readyIndex = -1
					priorityString = df['schedule_priority'][0]
					lastPriority = df['schedule_priority'][0]
					
					for idx0 in range(len(df['schedule_priority'])):
						if df['schedule_priority'][idx0] != lastPriority:
							priorityString = priorityString + '--' + df['schedule_priority'][idx0]
							lastPriority = df['schedule_priority'][idx0]
							if ('EDCT' in df['schedule_priority'][idx0]) or ('APREQ' in df['schedule_priority'][idx0]):
								dfSummary['Tactical_Controlled_Flight'][flight] = df['schedule_priority'][idx0]
							if ('EXEMPT' in df['schedule_priority'][idx0]):
								dfSummary['Tactical_Exempt_Flight'][flight] = df['schedule_priority'][idx0]


					dfSummary['Tactical_Schedule_Priroity_String'][flight] = priorityString

					### Create string of all mss transitions
					transitionAll = df['model_schedule_state'][0]
					lastState = df['model_schedule_state'][0]
					for idx in range(len(df['model_schedule_state'])):
						if df['model_schedule_state'][idx] != lastState:
							transitionAll = transitionAll + '--' + df['model_schedule_state'][idx]
							lastState = df['model_schedule_state'][idx]

					### Write to summary DF
					dfSummary['MSS_Transition_String'][flight] = transitionAll

					### Get the ready index which is last index before READY, OUT, or TAXI
					[readyIndex,trackHitOut,returnToGateFlag] = fGetReadyIndex(df)

					if trackHitOut:
						dfSummary['Track_Hit_Out_Time'][flight] = 'TRUE'
					else:
						dfSummary['Track_Hit_Out_Time'][flight] = 'FALSE'

					try:
						if readyIndex != -1:
							if df['timenow'][readyIndex-1] == dfGate['timenow'][readyIndex-1]:
								modelTaxiTime = (df['runwayutot'][readyIndex-1] - dfGate['gateuobt'][readyIndex-1]) / float(60)
								dfSummary['Unimpeded_Taxi_Time'][flight] = modelTaxiTime
								if (off_epoch and out_epoch) != False:
									dfSummary['Excess_Taxi_Time'][flight] = realizedTaxiTime - modelTaxiTime
					except:
						pass


					### Create array of zeros to count transitions into 
					### each model schedule state
					countTransitions = np.zeros(len(modelVector))
					
					### Create array of objects to store timestamps when
					### Transition in each model schedule state
					ttotTransitionIn = np.empty(len(modelVector), dtype=object)
					utotTransitionIn = np.empty(len(modelVector), dtype=object)
					accuracyTransitionIn = np.zeros(len(modelVector))
					meterTransitionIn = np.empty(len(modelVector), dtype=object)
					meterModeTransitionIn = np.empty(len(modelVector), dtype=object)
					runwayTransitionIn = np.empty(len(modelVector), dtype=object)
					etaTransitionIn = np.empty(len(modelVector), dtype=object)
					writeInFlag = np.zeros(len(modelVector))
					
					## Transition Out data
					ttotTransitionOut = np.empty(len(modelVector), dtype=object)
					utotTransitionOut = np.empty(len(modelVector), dtype=object)
					accuracyTransitionOut = np.zeros(len(modelVector))
					meterTransitionOut = np.empty(len(modelVector), dtype=object)
					meterModeTransitionOut = np.empty(len(modelVector), dtype=object)
					runwayTransitionOut = np.empty(len(modelVector), dtype=object)
					etaTransitionOut = np.empty(len(modelVector), dtype=object)
					writeOutFlag = np.zeros(len(modelVector))

					### Loop through the different model schedule states
					for mss in range(len(modelVector)):
						### loop through all the time slices for this flight
						for ts in range(1,len(df['model_schedule_state'])):
							### if current time slice is model schedule state you are interested in
							### and the previous time slice is not the model schedule state you are interested in
							### then count this as one transition into the state and store the timestamp for various data elements
							if ( df['model_schedule_state'][ts] == modelVector[mss] ) and ( df['model_schedule_state'][ts-1] != modelVector[mss] ):
								if (df['model_schedule_state'][ts] == 'PUSHBACK_PLANNED') and (df['schedule_priority'][ts] == 'GATE_DEPARTURE_UNCERTAIN'):
									print('UNCERTAIN -- PLANNED ' + str(flight))
									break
								countTransitions[mss] +=1
								### Transition IN Data
								ttotTransitionIn[mss] = str(df['scheduled_time'][ts])
								utotTransitionIn[mss] = str(df['estimated_time'][ts])
								etaTransitionIn[mss] = str(df['eta_msg_time'][ts])
								meterTransitionIn[mss] = str(df['metering_display'][ts-1])
								meterModeTransitionIn[mss] = str(df['metering_mode'][ts-1])
								runwayTransitionIn[mss] = str(df['runway'][ts])
								if (off_epoch and out_epoch) != False:
									accuracyTransitionIn[mss] = df['runwayttot'][ts] - off_epoch
								writeInFlag[mss] = 1

								### if the mss is pushback planned then store data related to 
								### the advisories and the gate hold
								if modelVector[mss] in ['PUSHBACK_PLANNED']:	
									try:
										if dfGate['eta_msg_time'][ts] == df['eta_msg_time'][ts]:
											advisory = dfGate['gatetobt'][ts] - dfGate['timenow'][ts]
											gateHold = dfGate['gatetobt'][ts] - dfGate['gateuobt'][ts]
											dfSummary[str_advisory_IN[mss]][flight] = advisory
											dfSummary[str_gatehold_IN[mss]][flight] = gateHold
									except:
										pass

								if modelVector[mss] in ['PUSHBACK_READY']:	
									try:
										if dfGate['eta_msg_time'][ts] == df['eta_msg_time'][ts]:

											[ttotTransitionIn,utotTransitionIn,readyTime,preReadyUOBT,advisory,gateHold] = fGetHoldData(dfSummary,mss,df,dfGate,readyIndex,ttotTransitionIn,utotTransitionIn)
											
											#### True / False return to gate after put on hold
											if returnToGateFlag:
												dfSummary['Return_To_Gate_After_Hold'][flight] = 'TRUE'
											else:
												dfSummary['Return_To_Gate_After_Hold'][flight] = 'FALSE'
											
											dfSummary[str_advisory_IN[mss]][flight] = advisory
											dfSummary[str_gatehold_IN[mss]][flight] = gateHold
											if advisory > advisoryThreshold:
												dfSummary['Held_With_Non_Zero_Advisory'][flight] = 'TRUE'
											else:
												dfSummary['Held_With_Non_Zero_Advisory'][flight] = 'FALSE'

											if gateHold > 0:
												dfSummary['Held_With_Non_Zero_(TOBT-UOBT)'][flight] = 'TRUE'
											else:
												dfSummary['Held_With_Non_Zero_(TOBT-UOBT)'][flight] = 'FALSE'

											if df['metering_mode'][ts] == 'TIME_BASED_METERING':
												runwayMeterVec = df['metering_display'][ts].split(',')
												if str(df['runway'][ts]) in runwayMeterVec:
													dfSummary['Held_While_Metering_On_Scheduled_Runway'][flight] = 'TRUE'
												else:
													dfSummary['Held_While_Metering_On_Scheduled_Runway'][flight] = 'FALSE'
											else:
												dfSummary['Held_While_Metering_On_Scheduled_Runway'][flight] = 'FALSE'

									except:
										print('YOU FAILED ON THE TRY')


									
												
						if writeInFlag[mss] == 1:
							### Save the count of transitions
							dfSummary[str_transitions_IN[mss]][flight] = countTransitions[mss]
							### Save Transition IN Data
							dfSummary[str_ttot_IN[mss]][flight] = ttotTransitionIn[mss]
							dfSummary[str_utot_IN[mss]][flight] = utotTransitionIn[mss]
							dfSummary[str_accuracy_IN[mss]][flight] = accuracyTransitionIn[mss]
							dfSummary[str_runway_IN[mss]][flight] = runwayTransitionIn[mss]
							dfSummary[str_meter_IN[mss]][flight] = meterTransitionIn[mss]
							dfSummary[str_meterMode_IN[mss]][flight] = meterModeTransitionIn[mss]
							dfSummary[str_eta_IN[mss]][flight] = etaTransitionIn[mss]
						

					### Loop through the different model schedule states
					for mss2 in range(len(modelVector)):
						### loop through all the time slices for this flight
						for ts in range(1,len(df['model_schedule_state'])):
							### if previous time slice is model schedule state you are interested in
							### and the current time slice is not the model schedule state you are interested in
							### then count this as one transition out of the state and store the timestamp for various data elements
							if ( df['model_schedule_state'][ts-1] == modelVector[mss2] ) and ( df['model_schedule_state'][ts] != modelVector[mss2] ):
								if (df['model_schedule_state'][ts-1] == 'PUSHBACK_PLANNED') and (df['schedule_priority'][ts-1] == 'GATE_DEPARTURE_UNCERTAIN'):
									print('UNCERTAIN -- PLANNED ' + str(flight))
									break
								### Transition OUT Data
								ttotTransitionOut[mss2] = str(df['scheduled_time'][ts-1])
								utotTransitionOut[mss2] = str(df['estimated_time'][ts-1])
								etaTransitionOut[mss2] = str(df['eta_msg_time'][ts-1])
								meterTransitionOut[mss2] = str(df['metering_display'][ts-1])
								meterModeTransitionOut[mss2] = str(df['metering_mode'][ts-1])
								runwayTransitionOut[mss2] = str(df['runway'][ts-1])
								if (off_epoch and out_epoch) != False:
									accuracyTransitionOut[mss2] = df['runwayttot'][ts-1] - off_epoch
								
								if modelVector[mss2] in ['PUSHBACK_UNCERTAIN' , 'PUSHBACK_PLANNED']:
									writeOutFlag[mss2] = 1

								### if the mss2 is pushback planned then store data related to 
								### the advisories and the gate hold
								if modelVector[mss2] in ['PUSHBACK_PLANNED']:
									try:
										if dfGate['eta_msg_time'][ts-1] == df['eta_msg_time'][ts-1]:
											advisory2 = dfGate['gatetobt'][ts-1] - dfGate['timenow'][ts-1]
											gateHold2 = dfGate['gatetobt'][ts-1] - dfGate['gateuobt'][ts-1]
											dfSummary[str_advisory_OUT[mss2]][flight] = advisory2
											dfSummary[str_gatehold_OUT[mss2]][flight] = gateHold2
									except:
										pass

								### if the mss2 is pushback ready then store data related to 
								### the advisories and the gate hold and the realized hold
								if modelVector[mss2] in ['PUSHBACK_READY']:
									try:
										if dfGate['eta_msg_time'][ts-1] == df['eta_msg_time'][ts-1]:
											pushTime = dfGate['timenow'][ts]
											# advisory2 = dfGate['gatetobt'][ts-1] - dfGate['timenow'][ts-1]
											# gateHold2 = dfGate['gatetobt'][ts-1] - preReadyUOBT
											totalRealizedHold = pushTime - readyTime
											# holdBeforeUOBT = preReadyUOBT - readyTime
											# holdAfterUOBT = pushTime - preReadyUOBT
											dfSummary['Total_Realized_Hold'][flight] = totalRealizedHold
											
											if pushTime < preReadyUOBT:
												dfSummary['Realized_Hold_BEFORE_UOBT'][flight] = pushTime - readyTime
												dfSummary['Realized_Hold_AFTER_UOBT'][flight] = 0
											else:
												if preReadyUOBT > readyTime:
													dfSummary['Realized_Hold_BEFORE_UOBT'][flight] = preReadyUOBT - readyTime
													dfSummary['Realized_Hold_AFTER_UOBT'][flight] = pushTime - preReadyUOBT
												else:
													dfSummary['Realized_Hold_BEFORE_UOBT'][flight] = 0
													dfSummary['Realized_Hold_AFTER_UOBT'][flight] = pushTime - readyTime

							
											aircraftType = dfSummary['Tactical_Aircraft_Type'][flight]
											weightClass = dfSummary['Tactical_Weight_Class'][flight]
											gateHoldSeconds = dfSummary['Total_Realized_Hold'][flight]
											fuelFlow    = em.aircraft_get_fuel_flow_kg(aircraftType, weightClass, gateHoldSeconds)
											coEmission  = em.aircraft_get_co_emission_gr(aircraftType, weightClass, gateHoldSeconds)
											co2Emission = em.aircraft_get_co2_emission_kg(aircraftType, weightClass, gateHoldSeconds)
											hcEmission  = em.aircraft_get_hc_emission_gr (aircraftType, weightClass, gateHoldSeconds)
											noxEmission = em.aircraft_get_nox_emission_gr (aircraftType, weightClass, gateHoldSeconds)
											dfSummary['Fuel_Flow_Reduced_in_KG'][flight] = fuelFlow
											dfSummary['CO_Emissions_Reduced_in_grams'][flight] = coEmission
											dfSummary['CO2_Emissions_Reduced_in_KG'][flight] = co2Emission
											dfSummary['HC_Emissions_Reduced_in_grams'][flight] = hcEmission
											dfSummary['Nox_Emissions_Reduced_in_grams'][flight] = noxEmission
									except:
										pass
										


						if writeOutFlag[mss2] == 1:
							### Save Transition OUT Data
							dfSummary[str_ttot_OUT[mss2]][flight] = ttotTransitionOut[mss2]
							dfSummary[str_utot_OUT[mss2]][flight] = utotTransitionOut[mss2]
							dfSummary[str_accuracy_OUT[mss2]][flight] = accuracyTransitionOut[mss2]
							dfSummary[str_meter_OUT[mss2]][flight] = meterTransitionOut[mss2]
							dfSummary[str_meterMode_OUT[mss2]][flight] = meterModeTransitionOut[mss2]
							dfSummary[str_runway_OUT[mss2]][flight] = runwayTransitionOut[mss2]
							dfSummary[str_eta_OUT[mss2]][flight] = etaTransitionOut[mss2]

		
					dfSummary = fWriteReady(dfSummary,df,dfGate,readyIndex,off_epoch)



	dfSummary.to_csv(outputFileWithDirectory)
