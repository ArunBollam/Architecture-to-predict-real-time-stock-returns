
import time 
import os

#Write the stock returns from the total_returns file to total_returns_output.csv file with 10 millisecond lag
total_returns_output = open('/home/cloudera/Downloads/total_returns_output.csv','w')
for line in open('total_returns.csv'):
    total_returns_output.write(line)
    time.sleep(0.01)
total_returns_output.close()

