import threading
import os
from fTacticalV4 import *

files = os.listdir('opsSummaryDirectory/originalSummary')

threads = []

for item in files:
	if 'KCLT' in item:
		t = threading.Thread(target=fTacticalV4, args=(item,))
		#threads.append(t)
		t.start()