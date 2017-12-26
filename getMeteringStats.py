import os
from computeMeteringStats import *

files = os.listdir('opsSummaryDirectory/tacticalStitched')

gateHoldAllDays = []

for fileName in range(len(files)):
	if 'tactical' in files[fileName]:
		print(files[fileName])
		gateHoldAllDays = computeMeteringStats(files[fileName], gateHoldAllDays)

		date = files[fileName].split('.')[4]
		dateStr = date[0:4] + '-' + date[4:6] + '-' + date[6:8]
		if fileName == 0:
			firstDay = dateStr

lastDay = dateStr

plt.figure()

plt.hist(gateHoldAllDays,bins=30,alpha=0.7,label='Gate Hold Excess Taxi Time')
plt.title(firstDay + ' to ' + lastDay + ' Excess Taxi Time [Minutes]')
#plt.hist(gateHoldExcessTaxiTime,bins=30,color = 'green',alpha=0.7,label='Subject To Excess Taxi Time')
plt.savefig('opsSummaryDirectory/summaryStats/ExcessTaxiTime' + firstDay + lastDay + '.png')