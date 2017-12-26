import threading
import os
from fTactical import *

files = os.listdir('opsSummaryDirectory/originalSummary')

threads = []

for item in files:
	if 'KCLT' in files[item]:
		t = threading.Thread(target=fTactical, args=(item,))
		#threads.append(t)
		t.start()