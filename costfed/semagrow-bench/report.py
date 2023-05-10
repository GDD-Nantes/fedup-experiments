#!/usr/bin/env python3

import sys
import os
import csv
import math

for i in range(0, 16, 1):
	os.system("mvn exec:java -Dexec.mainClass=\"org.aksw.simba.start.semagrow.QueryEvaluation\" -Dexec.args=results/sample" + str(i) + ".csv")

reportrows = []
with open("results/sample0.csv") as f:
	reader = csv.reader(f)
	reportrows = list(reader)
		
#combine results
for i in range(1, 16):
	with open("results/sample" + str(i) + ".csv") as f:
		reader = csv.reader(f)
		rowlist = list(reader)
		reportrows[0].append("Sample #" + str(i))
		for rownum in range(1, len(rowlist)):
			reportrows[rownum].append((rowlist[rownum][2]))

#calculate average, standard deviation and standard error
for row in reportrows[1:]:
	n = len(row[2:])
	sum = 0
	for val in row[2:]:
		sum += int(val)
	avr = sum / n
	
	sum = 0
	for val in row[2:]:
		sum += (avr - int(val))**2
	sn = math.sqrt(sum / n)
	se = sn / math.sqrt(n)
	row.append(round(avr))
	row.append(round(sn))
	row.append(round(se))
	
reportrows[0].append("Average")
reportrows[0].append("SD")
reportrows[0].append("SE")


with open("results/report.csv", 'w', newline='') as csvfile:
	reportwriter = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
	reportwriter.writerows(reportrows)
