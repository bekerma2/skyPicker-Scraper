# -*- coding: utf-8 -*-
"""
Created on Thu Apr  6 20:05:58 2017

@author: leo
"""

from urllib.request import urlopen as uReq
import json
import time
from datetime import datetime
import csv
from multiprocessing import Pool

origin = 'ORD'

# 52 weeks, starting with July 2017
departure_dates = ('29/06/2017', '06/07/2017', '13/07/2017', 
                   '20/07/2017', '27/07/2017',
    '03/08/2017', '10/08/2017', '17/08/2017', '24/08/2017',
    '31/08/2017', '07/09/2017', '14/09/2017', '21/09/2017',
    '28/09/2017', '05/10/2017', '12/10/2017', '19/10/2017',
    '26/10/2017', '02/11/2017', '09/11/2017', '16/11/2017',
    '23/11/2017', '30/11/2017', '07/12/2017', '14/12/2017',
    '21/12/2017', '28/12/2017', '04/01/2018', '11/01/2018',
    '18/01/2018', '25/01/2018', '01/02/2018', '08/02/2018',
    '15/02/2018', '22/02/2018', '01/03/2018', '08/03/2018',
    '15/03/2018', '22/03/2018', '29/03/2018', '05/04/2018',
    '12/04/2018', '19/04/2018', '26/04/2018', '03/05/2018',
    '10/05/2018', '17/05/2018', '24/05/2018', '31/05/2018',
    '07/06/2018', '14/06/2018', '21/06/2018')

# top 500 desinations, minus US/MEX, islands, Yemen, etc. ~300 destinations
i_destinations = ('ALG', 'AEP', 'EZE', 'ADL', 'BNE', 'CNS', 
                  'DRW', 'MEL', 'PER', 'SYD',
    'BRU', 'CRL', 'KNO', 'LPB', 'BEL', 'BSB', 'CGB')
    
todaydate = time.strftime("%d/%m/%Y")
print("Started run at: ", datetime.now().time())

# create master list of lists
dest_depDate_parameter = []
for dest in i_destinations:
    for date in departure_dates:
        element = [dest,date]
        dest_depDate_parameter.append(element)

# append to csv
filename = 'fares2.csv'
f = open(filename, 'a', newline = '')

# create/write to csv, if needed
# headers = "Unique ID, TodayDate, Origin, Destination, Departure_Date, Airline, Price, Link\n"
# f.write(headers)

def crawlSkyPicker(parameter_list):
    dest = parameter_list[0]
    departure_date = parameter_list[1]
    my_url = ('https://api.skypicker.com/flights?flyFrom='+origin+
            '&to='+dest+'&dateFrom='+departure_date+'&dateTo='+departure_date+'&partner=picky&curr=USD')
    loop_start = time.time()
    try:
        response = uReq(my_url)
    except:
        pass
        print("failed url: ", my_url)
    else:
        # Convert bytes type to string type
        string = response.read().decode('utf-8')
        json_obj = json.loads(string)
    
    results = []
    
    try:
        len(json_obj['data'][0])==0
    except:
        pass
        print("url failed at: ", my_url)
    else:        
        leg_data = json_obj['data'][0]
        price = leg_data['price']
        link = leg_data['deep_link']
        airline = leg_data['route'][0]['airline'] # must remove dict from list
        results = [0, str(todaydate), origin, dest, str(departure_date), 
                   airline, price, link]
        
    print(results)
    now = time.time()
    print("Loop ran in {0:.4f} seconds".format(now - loop_start))
    current_dest_idx = i_destinations.index(dest)
    print("{0:.4f}".format(100*current_dest_idx/len(i_destinations)), "% complete")

    # print("url link: ", my_url)
    # print("destination: ", dest)
    # print("departure date: ", departure_date)
    # print("airline: ", airline)
    # print("price: ", price)
    
    return results
    
if __name__ == "__main__":
   NUM_WORKERS = 8
   FILE_LINES = len(i_destinations) * len(departure_dates)
   chunksize = FILE_LINES // (NUM_WORKERS * 2) #use as needed
   pool = Pool(NUM_WORKERS)
   # results is a list of all the lists returned from each call to crawlSkyPicker 
   results = pool.imap(crawlSkyPicker, dest_depDate_parameter, 1)
   writeFile = csv.writer(f)
   for result in results:
       writeFile.writerow(result)

f.close()
print("Run completed at: ", datetime.now().time())