from flask import Flask, render_template, Response, send_file, request, redirect
import pandas as pd
import sqlalchemy
import json
from datetime import datetime
from dateutil.relativedelta import relativedelta
from bs4 import BeautifulSoup
import requests
import lxml
import os

db_username = process.env.db_username
db_password = process.env.db_password
db_endpoint = process.env.db_endpoint
db_name = process.env.db_name
db_table = process.env.db_table
db_port = process.env.db_port
sqlconnection = f'{db_username}:{db_password}@{db_endpoint}:{db_port}/{db_name}'
eng = f'//{sqlconnection}'
engine = sqlalchemy.create_engine(f'mysql+pymysql://{sqlconnection}')



app = Flask(__name__)

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        caldate = request.form['datefrom']
        df = pd.read_sql(f'SELECT category,filter, date, title, timestamp FROM {db_table} ORDER BY date desc limit 10', engine)
        max_date = pd.read_sql(f'SELECT max(date) as date, max(timestamp) as last FROM {db_table}', engine)
        datenow = (datetime.now() - relativedelta(months=1)).strftime('%Y-%m-%d')
        dfdl = pd.read_sql(f"SELECT * FROM {db_table} WHERE date > '{ caldate }' ORDER BY date desc", engine)
        dfdl.to_excel('test.xlsx', encoding = 'utf-8-sig', index = False)
        return send_file('test.xlsx', attachment_filename='test.xlsx')        
        #return render_template('download.html') 
    else:
        df = pd.read_sql(f'SELECT category,filter, date, title, timestamp FROM {db_table} ORDER BY date desc limit 10', engine)
        max_date = pd.read_sql(f'SELECT max(date) as date, max(timestamp) as last FROM {db_table}', engine)
        datenow = (datetime.now() - relativedelta(months=1)).strftime('%Y-%m-%d')
        caldate = datenow
        return render_template('index.html', db = df,  max_date = max_date, datenow = datenow, caldate = caldate)            

@app.route('/download')
def download():
    df = pd.read_sql(f'SELECT * FROM {db_table}WHERE date < "2022-03-01" ORDER BY date desc ', engine)
    df.to_excel('test.xlsx', encoding = 'utf-8-sig', index = False)
    return send_file('test.xlsx', attachment_filename='test.xlsx')


@app.route('/update')
def update():
    #Start ISPReview Beautiful Soup Scrape
    TCP_final_list = []
    ISPR_final_list = []
    keywords = ['EE', 'Three', 'O2', '3 UK', 'Vodafone', 'VMO2', 'MVNO', 'BT', 'TalkTalk', 'Virgin', 'Sky', 'CityFibre', 'Openreach', 'Ofcom', 'CMA', 'ASA', 'UK gov', 'Nokia', 'Ericsson', 'Huawei', 'NEC', 'Samsung', 'RootMetrics', 'Opensignal', 'Tutela', 'Ookla', 'nPerf', 'OpenRAN', 'TIP', 'ORAN', 'O-RAN', 'LoRA', 'Sigfox'
    ]
    nextpage = True
    code = 200
    pagecount = 1
    tcpurl = "https://telecompaper-api-abduz5l4ya-ez.a.run.app/graphql"
    url = 'https://www.ispreview.co.uk/'
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
    maxPages = 10

    while code == 200 and pagecount < maxPages and nextpage:
        response = requests.get(url ,headers=headers)
        driver = response.text
        code = response.status_code
        soup = BeautifulSoup(driver, 'lxml')
        article_heading = [] #Heading list
        article_headingraw = soup.find_all('h2', class_ = 'h3mobile')
        for item in article_headingraw:
            article_heading.append(item.text)
        article_textraw = soup.find_all('div', class_ = 'text2')
        article_textraw.pop(0)
        article_aref = []
        for item in article_headingraw:
            article_aref.append(item.find('a'))
        article_url = [] #URL List
        for item in article_aref:
            article_url.append(item.get('href'))
        article_text = [] #Summary List
        for article in article_textraw:
            article_text.append(article.text.strip()) 
        article_dateraw = []
        datetemp = soup.find_all('div', class_ = 'isprpara')
        for i in range(0,len(datetemp), 2):
            article_dateraw.append(datetemp[i].find('p').text.split('('))
        article_date = [] #Date List
        for item in article_dateraw:
            article_date.append(item[0]) 
        for j in range(len(article_heading)) :
            temp_list = {'date' : article_date[j], 'title' : article_heading[j],'summary' : article_text[j], 'url' : article_url[j], }
            ISPR_final_list.append(temp_list)
        navigation = soup.find_all('div', class_ = 'navylink')
        nextpage = navigation[1].text != ''
        pagecount += 1
        url2 = f'https://www.ispreview.co.uk/index.php/page/{pagecount}'
        url = url2

    df = pd.DataFrame(ISPR_final_list)

    df['date'] = df['date'].str.strip()
    df['year'] = df['date'].str[-4:]
    df['day'] = df['date'].str[0:2]
    month = df['date'].str.split(' ', expand = True)

    monthlist = {'Jan':'01','Feb':'02','Mar':'03','Apr':'04','May':'05','Jun':'06','Jul':'07','Aug':'08','Sep':'09','Oct':'10','Nov':'11','Dec':'12'}
    tmpfile = list(month[1])
    monthfile = []
    for i in tmpfile:
        monthfile.append((monthlist[i]))
    monthfile = pd.DataFrame(monthfile)
    df['month'] = monthfile[0]
    df['date'] = df['year'].map(str)+"-"+df['month'].map(str)+"-"+df['day'].map(str)
    df.drop(['year', 'day', 'month'], axis=1, inplace=True)
    df['date']=df['date'].replace(regex=['n','r','s','t'], value='')

    #Start TelecomPaper API scrape
    payload = {
        "operationName": None,
        "variables": {
            "first": 100,
            "where": {
                "country": {"id": {"eq": 195}},
                "language": {"id": {"eq": 1}}
            },
            "order": [{"date": "DESC"}]
        },
        "query": "query ($first: Int, $where: ArticleFilterInput, $order: [ArticleSortInput!]) {articles(first: $first, where: $where, order: $order) { nodes { id externalId date title slug abstract imageUrl redirectUrl edition { id name __typename } country { id name __typename } region { id name __typename } contentType { id name colour __typename } __typename } __typename}}"
    }
    headers = {
        "authority": "telecompaper-api-abduz5l4ya-ez.a.run.app",
        "accept": "*/*",
        "x-request-language": "en",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36",
        "content-type": "application/json",
        "sec-gpc": "1",
        "origin": "https://www.telecompaper.com",
        "sec-fetch-site": "cross-site",
        "sec-fetch-mode": "cors",
        "sec-fetch-dest": "empty",
        "referer": "https://www.telecompaper.com/",
        "accept-language": "en-US,en;q=0.9"
    }

    response = requests.request("POST", tcpurl, json=payload, headers=headers)

    jsonraw = json.loads(response.text)

    for ids in jsonraw['data']['articles']['nodes']:
        date = str(datetime.strptime(ids['date'][0:10], '%Y-%m-%d').date())
        title = ids['title']
        url = 'https://www.telecompaper.com/news/'+ids['slug']+'--'+str(ids['externalId'])
        summary = ids['abstract']
        article = {'date' : date, 'title' : title, 'summary' : summary, 'url' : url}
        TCP_final_list.append(article)

    df1 = pd.DataFrame(TCP_final_list)

    union = pd.concat([df, df1], ignore_index=True)

    date1 = list(union['date'])
    datef = []
    for item in date1:
        datef.append((datetime.strptime(item, '%Y-%m-%d').date()))
    datef = pd.DataFrame(datef)
    union['date'] = datef[0]
    union.sort_values(by=['date','title'], ascending = [False, True], inplace=True)

    union['filter'] = union['title'].str.findall('|'.join(keywords)).apply(set).str.join(', ')
    for keyword in keywords:
        union[keyword] = union['title'].str.contains(keyword)
    union['Mobile'] = union[['EE', 'BT', 'Three', 'O2', 'VMO2', 'MVNO', '3 UK', 'Vodafone']].sum(axis=1)
    union['Fixed'] = union[['BT', 'TalkTalk', 'Virgin', 'Sky', 'CityFibre', 'Openreach']].sum(axis=1)
    union['Regulatory'] = union[['Ofcom', 'CMA', 'ASA', 'UK gov']].sum(axis=1)
    union['Vendor'] = union[['Nokia', 'Ericsson', 'Huawei', 'NEC', 'Samsung']].sum(axis=1)
    union['Benchmarking'] = union[['RootMetrics', 'Opensignal', 'Tutela', 'Ookla', 'nPerf']].sum(axis=1)
    union['Special'] = union[['OpenRAN', 'TIP', 'ORAN', 'O-RAN', 'LoRA', 'Sigfox']].sum(axis=1)    
    union['Mobile'] = union.apply(lambda x: 'Mobile, ' if x['Mobile'] > 0 else '', axis=1)
    union['Fixed'] = union.apply(lambda x: 'Fixed, ' if x['Fixed'] > 0 else '', axis=1)
    union['Regulatory'] = union.apply(lambda x: 'Regulatory, ' if x['Regulatory'] > 0 else '', axis=1)
    union['Vendor'] = union.apply(lambda x: 'Vendor, ' if x['Vendor'] > 0 else '', axis=1)
    union['Benchmarking'] = union.apply(lambda x: 'Benchmarking, ' if x['Benchmarking'] > 0 else '', axis=1)
    union['Special'] = union.apply(lambda x: 'Special, ' if x['Special'] > 0 else '', axis=1)
    union = union.drop(columns=keywords)
    union['category'] = union[['Mobile', 'Fixed', 'Regulatory', 'Vendor', 'Benchmarking', 'Special']].agg(''.join, axis=1)
    union = union.drop(columns=['Mobile', 'Fixed', 'Regulatory', 'Vendor', 'Benchmarking', 'Special'])
    union=union[['category', 'filter','date', 'title', 'summary', 'url']]

    engine = sqlalchemy.create_engine(f'mysql+pymysql://{sqlconnection}')
    sqlimport = pd.read_sql(f'{db_table}', engine)

    tempfile = union.merge(sqlimport, on='title', how='left', indicator=True, suffixes = ('', '_y')).query('_merge == "left_only"')
    tempfile = tempfile[['category', 'filter', 'date', 'title', 'summary', 'url']]

    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    tempfile['timestamp'] = timestamp
    tempfile.to_sql(
        name= f'{db_table}',
        con = engine,
        index = False,
        if_exists='append'
    )
    
    df = pd.read_sql(f'SELECT category,filter, date, title, timestamp FROM {db_table} ORDER BY date desc limit 10', engine)
    max_date = pd.read_sql(f'SELECT max(date) as date, max(timestamp) as last FROM {db_table}', engine)
    return render_template('update.html', db = df,  max_date = max_date, timestamp = timestamp)