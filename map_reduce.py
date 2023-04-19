#import libraries
import pandas as pd
import re
import threading
import queue

#read the csv files in pandas
df1 = pd.read_csv('AComp_Passenger_data_no_error.csv', encoding='utf-8')
df2 = pd.read_csv('Top30_airports_LatLong.csv', encoding='utf-8')

#print the first 10 rows
print(df1.head(10))
print(df2.head(10))

#set column names for each column
df1.columns = ['Passenger_id', 'Flight_id', 'From_airport_IATA/FAA_code', 'Destination_airport_IATA/FAA_code', 'Departure_time(GMT)', 'Total_flight_time(mins)']
df2.columns = ['Airport_Name', 'Airport_IATA/FAA_code', 'Latitude', 'Longitude']

#print the first 10 rows to double check the column names
print(df1.head(10))
print(df2.head(10))

#infomation of passenger data
df1.info()

# Check for duplicates of passenger data
duplicates1 = df1.duplicated()
print(duplicates1)

# check for null rows of passenger data
nulls1 = df1.isnull()
print(nulls1)

#remove duplicates for passenger data
df1 = df1.drop_duplicates()

#remove null rows for passenger data
passenger = df1.dropna()

#infomation of passenger data after removing duplicates and null values
passenger.info()

#infomation of airports data
df2.info()

# Check for duplicates of airports data
duplicates2 = df2.duplicated()
print(duplicates2)

# check for null rows of airport data
nulls2 = df2.isnull()
print(nulls2)

#remove duplicates for airport data
df2 = df2.drop_duplicates()

# Drop rows with null values for airport data
airport = df2.dropna()

#infomation of airport data after removing duplicates and null values
airport.info()

#save cleaned data to new csv file
passenger.to_csv('new_passenger.csv', index=False)

#filter the passenger id using Regular Expression
pattern = re.compile(r'^[A-Z]{3}\d{4}[A-Z]{2}\d$')
filtered_df = passenger[passenger['Passenger_id'].str.match(pattern)]
filtered_df['Passenger_id'].to_csv('passenger.txt', index=False, header=False)

#read the passenger text file with buffering
file = open('passenger.txt', "r", buffering=390*1)
text = file.read()

#split function to split lines into two parts
def splitlines(text,a):

  # Splitting the lines into a list
  linessplit = text.splitlines()
  split1 = linessplit[0:a]
  split2 = linessplit[a:]
  return split1, split2


#mapper function
def mapper(text,out_queue):
  key_val = []
  for i in text:
    wordssplit = i.split()
    for j in wordssplit:
      # Appending each word in the line with 1 and storing it in [passenger,1] format
      key_val.append([j, 1])
  out_queue.put(key_val)

#sorted function
def sorted(list1, list2):
  # Appending the two input lists into a single list
  sortedoutput = list1 + list2
  sortedoutput.sort(key=lambda x : x[0])
  return sortedoutput

# shuffle function
def shuffle(sorted_list) :
 sort1out = []
 sort2out = []
 for i in sorted_list:
   # Partitioning the sorted word list into two separate lists
    if i[0][0] < 'n':
      sort1out.append(i)
  #with first list containing words starting with a-m and n-z into second
    else : sort2out.append(i)
 return sort1out, sort2out

#reducer function
def reducer(part_out1,out_queue) :
  sum_reduced = []
  count = 1
  for i in range(0, len(part_out1)):
    if i < len(part_out1)-1:
      if part_out1[i] == part_out1[i+1]:
       count = count+1                              #Counting the number of words
      else :
       sum_reduced.append([part_out1[i][0],count])  #Appending the word along with count to sum_reduced list as ["word",count]
       count = 1
    else: sum_reduced.append(part_out1[i])          #Appending the last word to the output list
  out_queue.put(sum_reduced)

#Multi - Threads function two threads taking two inputs map1_input and map2_input
def multi_threads(func,map1_input,map2_input):
  my_queue1 = queue.Queue()  #Using queue to store the values of mapper output to use them in sort function
  my_queue2 = queue.Queue()
  t1 = threading.Thread(target=func, args=(map1_input, my_queue1))
  t2 = threading.Thread(target=func, args=(map2_input, my_queue2))
  # Starting the execution of thread1
  t1.start()
  # Starting the execution of thread2
  t2.start()
  # Waiting for the thread1 to be completely executed
  t1.join()
  # Waiting for the thread2 to be completely executed
  t2.join()
  list1out = my_queue1.get()
  list2out = my_queue2.get()
  return list1out, list2out

def main_function(text):
  linessplit = splitlines(text, 185)
  mapperout = multi_threads(mapper, linessplit[0], linessplit[1])
  sortedwords = sorted(mapperout[0], mapperout[1])
  slicedwords = shuffle(sortedwords)
  reducerout = multi_threads(reducer, slicedwords[0], slicedwords[1])
  return reducerout[0]+reducerout[1], mapperout

reduceoutput, mapperoutput = main_function(text)

#reducer output in csv file
pd.DataFrame(reduceoutput).reset_index().to_csv("reduce_output.csv", index=False, header=['index', 'passenger', 'count'])
#mapper output in csv file
pd.DataFrame(mapperoutput).reset_index().to_csv("mapper_output.csv", index=False)