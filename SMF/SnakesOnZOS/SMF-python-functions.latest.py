
# coding: utf-8

# <h1>A Hands-On Lab for the System Programmer</h1>
# <ul>
#     <li>You need to catch a run-away job that is generating SMF records and filling our logstream!</li>
#     <li>SYSLOG isnát helpful, we need to do some ad-hoc processing of SMF records to figure this out.</li>
# </ul>

# In[1]:

# Version number
version=6.0
date="June 21, 2018"

print()
print("+----------------------------------+")
print("| z/OS SMF record analysis program |")
print("| Version",version,"-",date,"     |")
print("+----------------------------------+")
print()

# In[2]:

# Import packages
import pandas as pd
import dsdbc

# In[3]:

#Retrieve credentials to access ODL server
def get_credentials():
    with open('/u/userid/python/user_info.txt') as f:
        user = f.readline().rstrip()
        password = f.readline().rstrip()
    return user, password

user, password = get_credentials()


# In[4]:

#Setup ODL
conn = dsdbc.connect(SSID="AZKS", user=user, password=password)
cursor = conn.cursor()


# In[5]:

#User to enter full input dataset name
#n = input("Enter the input dataset name: ")


# In[5]:

#Load full input dataset name file
def get_file():
    with open('/u/userid/python/data_file.txt') as f:
        dataset_name= f.readline().rstrip()
    return dataset_name
dataset_name= get_file()


# In[6]:

# Replace ' . ' in dataset name with ' _ '
dataset_name= dataset_name.replace('.', '_')


# In[7]:

#Load input dataset
def get_input():
    i = pd.read_sql('SELECT SMF_RTY FROM SMF_FILE__'+dataset_name, conn)
    return i

print("Input SMF dataset name =",get_file())
print()

# In[54]:

#get_input().head()


# In[8]:

#Count and sort the input dataset by record type
def sort_input():
    si = get_input().groupby('SMF_RTY').size().reset_index(name='COUNT').sort_values('COUNT',ascending=False)
    si.rename(columns={'SMF_RTY': 'REC_TYPE'}, inplace=True)
    return si


# In[9]:
print("Top 5 SMF record types:")
print(sort_input().head().to_string(index=False))
print()


# In[10]:

#Identify the record type with the highest count
#def get_top_record():
t = sort_input().iloc[0]
t = str(t[0])
print("The largest SMF record type =",t,"with " + str(sort_input().iloc[0]['COUNT']) + " records")
print()
if t == '30':
    m = 'SMF_0' + t + '00_SMF' + t + 'ID__' + dataset_name
    s = pd.read_sql('SELECT SMF' + t + 'JBN, SMF' + t + 'RUD ' 'FROM ' + m, conn)
    ji = s.groupby('SMF' + t + 'JBN').size().reset_index(name='COUNT').sort_values('COUNT',ascending=False)
    print("Sorted by jobname:")
    print(ji.head().to_string(index=False))
    print()
    ui = s.groupby('SMF' + t + 'RUD').size().reset_index(name='COUNT').sort_values('COUNT',ascending=False)
    print("Sorted by user ID:")
    print(ui.head().to_string(index=False))
    print()
    percent_job = round(100*ji.iloc[0]['COUNT']/(len(get_input())-2))
    print("The number of " 'SMF' + t + 'JBN' " entries is "  + str(percent_job) +  " percent of the total.")
    percent_user = round(100*ui.iloc[0]['COUNT']/(len(get_input())-2))
    print("The number of " 'SMF' + t + 'RUD' " entries is "  + str(percent_user) +  " percent of the total.")
elif t == '80':
    m = 'SMF_0' + t + '00__' + dataset_name
    s = pd.read_sql('SELECT SMF' + t + 'JBN, SMF' + t + 'USR ' 'FROM ' + m, conn)
    ji = s.groupby('SMF' + t + 'JBN').size().reset_index(name='COUNT').sort_values('COUNT',ascending=False)
    print("Sorted by jobname:")
    print(ji.head().to_string(index=False))
    print()
    ui = s.groupby('SMF' + t + 'USR').size().reset_index(name='COUNT').sort_values('COUNT',ascending=False)
    print("Sorted by user ID:")
    print(ui.head().to_string(index=False))
    print()
    percent_job = round(100*ji.iloc[0]['COUNT']/(len(get_input())-2))
    print("The number of " 'SMF' + t + 'JBN' " entries is "  + str(percent_job) +  " percent of the total.")
    percent_user = round(100*ui.iloc[0]['COUNT']/(len(get_input())-2))
    print("The number of " 'SMF' + t + 'USR' " entries is "  + str(percent_user) +  " percent of the total.")
elif t == '14':
    m = 'SMF_0' + t + '00__' + dataset_name
    s = pd.read_sql('SELECT SMF' + t + 'JBN, SMF' + t + 'UID ' 'FROM ' + m, conn)
    ji = s.groupby('SMF' + t + 'JBN').size().reset_index(name='COUNT').sort_values('COUNT',ascending=False)
    print("Sorted by jobname:")
    print(ji.head().to_string(index=False))
    print()
    ui = s.groupby('SMF' + t + 'UID').size().reset_index(name='COUNT').sort_values('COUNT',ascending=False)
    print("Sorted by user ID:")
    print(ui.head().to_string(index=False))
    print()
    percent_job = round(100*ji.iloc[0]['COUNT']/(len(get_input())-2))
    print("The number of " 'SMF' + t + 'JBN' " entries is "  + str(percent_job) +  " percent of the total.")
    percent_user = round(100*ui.iloc[0]['COUNT']/(len(get_input())-2))
    print("The number of " 'SMF' + t + 'UID' " entries is "  + str(percent_user) +  " percent of the total.")
elif t == '42':
    print("SMF Record Type "  + t +  " - DFSMS Statistics and Configuration")
elif t == '99':
    print("SMF Record Type "  + t +  " - System Resource Monitor")
elif t == '119':
    m = 'SMF_' + t + '01__' + dataset_name
    s = pd.read_sql('SELECT SMF' + t + 'TI_USERID ' 'FROM ' + m, conn)
    ji = s.groupby('SMF' + t + 'TI_USERID').size().reset_index(name='COUNT').sort_values('COUNT',ascending=False)
    print("Sorted by jobname:")
    print(ji.head().to_string(index=False))
    print()
    percent_job = round(100*ji.iloc[0]['COUNT']/(len(get_input())-2))
    print("The number of " 'SMF' + t + 'TI_USERID' " entries is "  + str(percent_job) +  " percent of the total.")
elif t == '60':
    m = 'SMF_0' + t + '00__' + dataset_name
    s = pd.read_sql('SELECT SMF' + t + 'JNM, SMF' + t + 'UID ' 'FROM ' + m, conn)
    ji = s.groupby('SMF' + t + 'JNM').size().reset_index(name='COUNT').sort_values('COUNT',ascending=False)
    print("Sorted by jobname:")
    print(ji.head().to_string(index=False))
    print()
    ui = s.groupby('SMF' + t + 'UID').size().reset_index(name='COUNT').sort_values('COUNT',ascending=False)
    print("Sorted by user ID:")
    print(ui.head().to_string(index=False))
    print()
    percent_job = round(100*ji.iloc[0]['COUNT']/(len(get_input())-2))
    print("The number of " 'SMF' + t + 'JNM' " entries is "  + str(percent_job) +  " percent of the total.")
    percent_user = round(100*ui.iloc[0]['COUNT']/(len(get_input())-2))
    print("The number of " 'SMF' + t + 'UID' " entries is "  + str(percent_user) +  " percent of the total.")
elif t == '252':
    print("SMF Record Type "  + t +  " - NetView FTP")
elif t == '64':
    m = 'SMF_0' + t + '00__' + dataset_name
    s = pd.read_sql('SELECT SMF' + t + 'JBN, SMF' + t + 'UIf ' 'FROM ' + m, conn)
    ji = s.groupby('SMF' + t + 'JBN').size().reset_index(name='COUNT').sort_values('COUNT',ascending=False)
    print("Sorted by jobname:")
    print(ji.head().to_string(index=False))
    print()
    ui = s.groupby('SMF' + t + 'UIF').size().reset_index(name='COUNT').sort_values('COUNT',ascending=False)
    print("Sorted by user ID:")
    print(ui.head().to_string(index=False))
    print()
    percent_job = round(100*ji.iloc[0]['COUNT']/(len(get_input())-2))
    print("The number of " 'SMF' + t + 'JBN' " entries is "  + str(percent_job) +  " percent of the total.")
    percent_user = round(100*ui.iloc[0]['COUNT']/(len(get_input())-2))
    print("The number of " 'SMF' + t + 'UIF' " entries is "  + str(percent_user) +  " percent of the total.")
elif t == '74':
    print("SMF Record Type "  + t +  " - RMF Activity")
elif t == '50':
    print("SMF Record Type "  + t +  " - VTAM Tuning Statistics")
elif t == '72':
    print("SMF Record Type "  + t +  " - Workload Activity, Storage Data, and Serialization Delay")
#    return t


# END
