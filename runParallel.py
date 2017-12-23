import threading
import os
from fTactical import *

files = os.listdir('opsSummaryDirectoy/originalSummary')

#print(files)


threads = []

for item in files:
	t = threading.Thread(target=fTactical, args=(item,))
	threads.append(t)
	t.start()