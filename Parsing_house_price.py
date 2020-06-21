#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests
import csv
from bs4 import BeautifulSoup

'''
Program purpose:
    Getting house prices near '新竹市東區慈雲路' within 1 year
    
Roadmap:
    1. Collecting  house price data into a csv file by python parser
    2. Create a database in sqlite to store data we collected
    3. Loading csv data into the sqlite database
    4. Adding two more columns in the sqlite database named "latitude" and "longitude" to store coordinate data we'll collect from goolgemap
    5. Getting coordinate from goolgemap by community naming we collected and storing coordinates into database
    6. Using SQL command to calcuate average unitprice of each community we collected
    7. Plotting a figure of googlemap which could demonstrate each communities by coordinates collected from googlemap
    8. Demonstrating each communities house_price and address in the info_box_content
    
'''


# Initial website address
YCURL = 'https://evertrust.yungching.com.tw'
URL = '/region/%E6%96%B0%E7%AB%B9%E5%B8%82/%E6%9D%B1%E5%8D%80?kw=%E6%85%88%E9%9B%B2%E8%B7%AF&dt=2&d=12&t=&a=&c=&x=24.7915659664812&y=121.012094991928#mainContent'


# In[51]:


def get_web_page(url) -> str:
    '''input: url = website which has house price data'''
    '''output: resp.text =  text data collected from website'''
    headers = {
        'User-Agent' : 'Enter your user-agent information'
    }
    try:
        resp = requests.get(url = url, headers = headers)
    except Exception as e:
        print('something wrong')
        print(e)
        return None
    return resp.text


def get_info(dom, total):
    '''input: dom = resp.text which outputs from get_web_page(url), total = A list to contain a dict named info'''
    '''output: total = a list contain a dict named info, containing data like house_price, address..etc'''
    soup = BeautifulSoup(dom, 'html5lib')
    
    # get info
    table = soup.find_all('table', class_ = 'dealTable')[0].tbody
    trs = table.find_all('tr')
    info_list = []
    for t in trs:
        try:
            info = dict()
            info['dealtime'] = t.find('td').span.text
            info['type'] = t.find('td', class_ = 'type').text.strip()
            if t.find('td', class_ = 'add').text.split("格局")[0].strip():
                if "\n\n" in t.find('td', class_ = 'add').text.split("格局")[0].strip():
                    info['address'] = ''.join(t.find('td', class_ = 'add').text.split("格局")[0].strip().replace('\n\n', '').split())
                else:
                    info['address'] = t.find('td', class_ = 'add').text.split("格局")[0].strip()
            else:
                info['address'] = 'N/A'
                
            if t.find('td', class_ = 'add').text.split("：")[-1].strip():
                info['room'] = t.find('td', class_ = 'add').text.split("：")[-1].strip()
            else:
                info['room'] = 'N/A'
                
            if t.find('td', class_ = 'dealPrice').text.split("萬")[0].strip():
                info['dealPrice'] = float(t.find('td', class_ = 'dealPrice').text.split("萬")[0].strip().replace(',', ''))
            else:
                info['dealPrice'] = 'N/A'
                
            if t.find('td', class_ = 'unitPrice').text.split("萬")[0].strip():
                info['unitPrice'] = float(t.find('td', class_ = 'unitPrice').text.split("萬")[0].strip())
            else:
                info['unitPrice'] = 'N/A'
                
            if t.find('td', class_ = 'floorSpace').text.split("坪")[0].strip():
                info['floorSpace'] = float(t.find('td', class_ = 'floorSpace').text.split("坪")[0].strip())
            else:
                info['floorSpace'] = 'N/A'
                
            if t.find('td', class_ = 'floor').text.split("~")[1].strip():
                info['floor'] = t.find('td', class_ = 'floor').text.split("~")[-1].split('/')[0].strip()
                info['total_floor'] = t.find('td', class_ = 'floor').text.split("~")[-1].split('/')[1].strip()
            else:
                info['floor'] = 'N/A'
            total.append(info)
                
        except Exception as e:
            print(e)
            print(info)

    return total


# In[52]:


# main
'''Start to collect data from website and store data into a list named total'''
total = []
current_page = get_web_page(YCURL + URL)
current_page_info = get_info(current_page, total)
for i in range(2, 27):
    next_page_url = YCURL + '/region/%e6%96%b0%e7%ab%b9%e5%b8%82/%e6%9d%b1%e5%8d%80/'    + str(i) + '?kw=慈雲路&dt=2&d=12&t=&a=&c=&x=24.7915659664812&y=121.012094991928#mainContent'
    print(next_page_url)
    current_page = get_web_page(next_page_url)
    current_page_info = get_info(current_page, total)
print(total)
print(len(total))

'''Check how many number of items and save these data as a csv file'''
print('共%d項物件' % (len(total)))
with open('house_price.csv', 'w', encoding = 'utf-8', newline = '') as f:
    writer = csv.writer(f)
    writer.writerow(('dealtime', 'type', 'address', 'room', 'dealprice', 'unitprice', 'space', 'floor', 'total_floor'))
    for i in total:
        writer.writerow((s for s in i.values()))


# In[2]:


import sqlite3
import csv

# Initialize a database (db)
def execute_db(f_name, sql_cmd):
    '''initialize a database in sqlite'''
    '''inuput: f_name = database name, sql_cmd = SQL commands to initialize a database for house price data storage'''
    '''output: A database file named as f_name'''
    conn = sqlite3.connect(f_name)
    c = conn.cursor()
    c.execute(sql_cmd)
    conn.commit()
    conn.close()


# In[3]:


# set up a db
db_name = 'house_price.sqlite'
cmd = 'CREATE TABLE house_price (id INTEGER PRIMARY KEY AUTOINCREMENT, dealtime TEXT, type TEXT, address TEXT, room TEXT, dealprice FLOAT,unitprice FLOAT, floorspace FLOAT,floor TEXT, total_floor TEXT)'
execute_db(db_name, cmd)


# In[4]:


# Loading csv data into sqlite db
with open('house_price.csv', 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for row in reader:
        cmd = 'INSERT INTO house_price (dealtime, type, address, room, dealprice, unitprice, floorspace,floor, total_floor) VALUES ("%s", "%s", "%s", "%s", %f, %f, %f, "%s", "%s")' %        (row['dealtime'], row['type'], row['address'], row['room'], float(row['dealprice']), float(row['unitprice']), float(row['space']), row['floor'], row['total_floor'])
        execute_db(db_name, cmd)


# In[5]:


# Getting info from sqlite db
def select_db(fname, sql_cmd):
    '''input: fname = database name, sql_cmd = command to get data from database'''
    '''output = return data, one row each time'''
    conn = sqlite3.connect(fname)
    c = conn.cursor()
    c.execute(sql_cmd)
    row = c.fetchall()
    conn.close()
    return row

# cmd_select = 'SELECT address FROM house_price WHERE type = "電梯大樓"'
# for r in select_db(db_name, cmd_select):
#     print(r)
    


# In[13]:


# Adding two more columns into sqlite3 for lat/long
'''Adding two columns into database for latitude and longitude, which are coordinates we'll collect from googlemap later'''
cmd_add_latitude = 'ALTER TABLE house_price ADD latitude'
cmd_add_longtitude = 'ALTER TABLE house_price ADD longitude'
execute_db(db_name, cmd_add_latitude)
execute_db(db_name, cmd_add_longtitude)


# In[7]:


# Getting coordinate by community naming and storing coordinates into database
import googlemaps
API_KEY = 'Please enter your googlemap API_KEY'

gmaps = googlemaps.Client(key = API_KEY)
# geocode_result = gmaps.geocode('富宇東方明珠東區慈雲路61~90號') # test
# print(geocode_result[0]['geometry']['location']) # test
cmd_select = 'SELECT * FROM house_price'
# for r in select_db(db_name, cmd_select):
#     print(r[3])
#     geocode_result = gmaps.geocode(r[3])
#     print(geocode_result[0]['geometry']['location'])
for r in select_db(db_name, cmd_select):
    try:
    #     print('r = ', r)
        geocode_result = gmaps.geocode(r[3])
    #     print(geocode_result)
        lat = geocode_result[0]['geometry']['location']['lat']
        lng = geocode_result[0]['geometry']['location']['lng']
        cmd_input_lat = 'UPDATE house_price SET latitude = "%s" WHERE address = "%s"' % (lat, r[3])
        cmd_input_lng = 'UPDATE house_price SET longitude = "%s" WHERE address = "%s"' % (lng, r[3])
    # #     print(r[3])
    # #     print(geocode_result[0]['geometry']['location'])
    # #     print('========')
        execute_db(db_name, cmd_input_lat)
        execute_db(db_name, cmd_input_lng)
    except Exception as e:
        print(e)
        print(r)


# In[9]:


# getting average price of each community

cmd_getting_ave = 'SELECT address, AVG(unitprice) AS Ave_unitprice FROM house_price GROUP BY address HAVING type = "電梯大樓"'
for i in select_db(db_name, cmd_getting_ave):
    print(i)


# In[10]:


import gmaps

''''Storing coordinates from database into a list named marker_locations, which will be used to show dots in googlemap figure'''
gmaps.configure(api_key = API_KEY)
marker_locations = []
cmd_select_building = 'SELECT * FROM house_price GROUP BY address HAVING type = "電梯大樓"'
cmd_getting_ave = 'SELECT address, AVG(unitprice) AS Ave_unitprice FROM house_price GROUP BY address HAVING type = "電梯大樓"'
# marker_locations = [(24.791343, 121.011858)]
for r in select_db(db_name, cmd_select_building):
    lat = float(r[-2])
    lng = float(r[-1])
    marker_locations.append((lat, lng))

# Define template which could demonstrate house_price data on googlemap figure
info_box_template = """
<dl>
<dt>Name</dt><dd>{address}</dd>
<dt>Ave. unitprice</dt><dd>{Ave_unitprice}</dd>
</dl>
"""

'''Storing address and ave unitprice into a list named unit_price_list'''
'''This list will be used to show data in the template info_box_template we've set up before'''
unit_price_list = []
for i in select_db(db_name, cmd_getting_ave):
    unit_price_dict = dict()
    unit_price_dict['address'] = i[0]
    unit_price_dict['Ave_unitprice'] = i[1]
#     print(unit_price_dict)
    unit_price_list.append(unit_price_dict)

# print(unit_price_list)

house_price_info = [info_box_template.format(**i) for i in unit_price_list]


'''Plotting a figure of googlemap which could demonstrate each communities by coordinates collected from googlemap'''
'''Second, demonstrating each communities house_price and address in the info_box_content'''
fig = gmaps.figure()
markers = gmaps.symbol_layer(marker_locations, info_box_content=house_price_info, fill_color='green', stroke_color='green', scale=3)
fig.add_layer(markers)
fig


# In[ ]:




