from flask import Flask, render_template, Response, send_file, request, redirect, url_for
from wordcloud import WordCloud, STOPWORDS, ImageColorGenerator
import PIL.Image
import numpy as np
import pandas as pd
import sqlalchemy
import json
from datetime import datetime
from dateutil.relativedelta import relativedelta
from bs4 import BeautifulSoup
import requests
import lxml
import os
import csv

db_username = os.environ['db_username']
db_password = os.environ['db_password']
db_endpoint = os.environ['db_endpoint']
db_name = os.environ['db_name']
db_table = os.environ['db_table']
wc_table = os.environ['wc_table']
wt_table = os.environ['wt_table']
db_port = os.environ['db_port']
jb_table = os.environ['jb_table']
jwc_table = os.environ['jwc_table']
sqlconnection = f'{db_username}:{db_password}@{db_endpoint}:{db_port}/{db_name}'
eng = f'//{sqlconnection}'
engine = sqlalchemy.create_engine(f'mysql+pymysql://{sqlconnection}')



app = Flask(__name__)

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        form_check = request.form['form-name']
        
        if form_check == 'downloadform':
            opconame = request.form['opco-name']
            caldate = datetime.strptime(request.form['datefrom'], '%Y-%m-%d')
            caldate2 = caldate.strftime('%Y-%m-%d')
            caldate = (caldate -relativedelta(days=1)).strftime('%Y-%m-%d')
            max_date = pd.read_sql(f'SELECT min(date) as date, max(timestamp) as last, count(title) as num FROM {db_table} where country = "{opconame}"', engine)
            datefrom = (datetime.now() - relativedelta(months=1)).strftime('%Y-%m-%d')
            datenow = datetime.now().strftime('%Y-%m-%d')
            dfdl = pd.read_sql(f"SELECT * FROM {db_table} WHERE date > '{ caldate }' AND country = '{opconame}' ORDER BY date desc", engine)
            dfdl.to_excel('./static/CI_download.xlsx', encoding = 'utf-8-sig', index = False)
            return send_file('./static/CI_download.xlsx', attachment_filename=f"CIDownload_{ caldate2 }-{ datenow }-{opconame}.xlsx")        
            #return render_template('download.html') 
        elif form_check == 'wordcloudform':
            opconame = request.form['opco-name']
            dateto = request.form['dateto']
            datefrom = request.form['datefrom']
            select1 = request.form['select1']
            max_date = pd.read_sql(f'SELECT min(date) as date, max(timestamp) as last, count(title) as num FROM {db_table} where country ="{opconame}"', engine)
            wordclouddf = pd.DataFrame({"fromdate":[datefrom], "todate":[dateto], 'catfilter':[select1], 'country':[opconame]})
            wordclouddf.to_sql(
                name= f'{wc_table}',
                con = engine,
                index = False,
                if_exists='replace'
            )
            complist = ['EE', 'Three', 'O2', 'Vodafone', 'VMO2', 'BT', 'TalkTalk', 'Virgin', 'Sky', 'CityFibre']
            dfwc1 = pd.read_sql(f"SELECT * FROM {db_table} WHERE date > '{ datefrom }' AND date < '{ dateto }' and country = '{opconame}'", engine)
            logo = np.array(PIL.Image.open('./static/no_logo.jpg')) 
            if select1 == 'VF':
                dfwc1 = dfwc1[dfwc1['filter'].str.contains('Vodafone', na=False)]
                logo = np.array(PIL.Image.open('./static/Vodaphone logo.jpg')) 
            elif select1 == 'Comp':
                dfwc1 = dfwc1[dfwc1['filter'].str.contains('|'.join(complist), na=False)]
            elif select1 != 'None':
                dfwc1 = dfwc1[dfwc1['category'].str.contains(select1, na=False)]
            textfile = ''
            dfwc = dfwc1['title']
            for row in dfwc:
                textfile = textfile + ' ' + str(row)

            if textfile == '':
                textfile = 'None'  
            textfile = textfile.title()  
            replacelist = {'Virgin Media':'VirginMedia', 'Bt':'BT', 'Ee':'EE', 'Isp':'ISP', '3 Uk':'Three', 'Deutsche Telekom':'DeutscheTelekom', 'Turk Telekom':'TurkTelekom'}
            for key in replacelist:
                textfile = textfile.replace(key, replacelist[key])
            wordfilter = set(list(STOPWORDS)+['UK', 'Full', 'Premises', 'powers', 'Add', 'service', 'new', 'Zen', 'Scotland',
                                    'British', 'dollar', 'network', 'Operator', 'Slashes', 'signs', 'North', 'million',
                                    'Maps', 'Map', 'US', 'GBP', 'England', 'Ireland', 'Germany', 'German', 'Deutsche', 'Greece', 'Eur', 'Irish', 'Hungary',
                                    'Czechia', 'Czech', 'Italian', 'Italia', 'Italy', 'Dutch', 'Netherlands', 'Portugal', 'Portuguese', 'Lisbon', 'Romania',
                                    'Romanian', 'Turkey'])
                
            wc = WordCloud(collocations=False,
                        stopwords = wordfilter,
                        mask = logo,
                        contour_color='red',
                        contour_width=2,
                        background_color='white').generate(textfile)

            wc.to_file('./static/wc.jpg')    
            dfwc2 = pd.read_sql(f"SELECT * FROM {wc_table}", engine)
            # create a dictionary of word frequencies
            text_dictionary = wc.process_text(textfile)
            # sort the dictionary
            word_freq={k: v for k, v in sorted(text_dictionary.items(),reverse=True, key=lambda item: item[1])}
            wordlist = pd.DataFrame.from_dict(word_freq, orient='index')
            wordlist.index.names = ['Words']
            wordlist.rename(columns={0 : "Freq"}, inplace = True)
            wordlist = wordlist.reset_index()
            wordlist.to_sql(
                name= f'{wt_table}',
                con = engine,
                index = False,
                if_exists='replace'
            )
            wordlist2 = pd.read_sql(f"SELECT * FROM {wt_table}", engine)
            return redirect(f'../country/{opconame}')

    else:
        max_date = pd.read_sql(f'SELECT min(date) as date, max(timestamp) as last, count(title) as num FROM {db_table}', engine)
        datenow = (datetime.now() - relativedelta(months=1)).strftime('%Y-%m-%d')
        caldate = datenow
        datefrom = (datetime.now() - relativedelta(months=1)).strftime('%Y-%m-%d')
        datenow = (datetime.now()).strftime('%Y-%m-%d')
        select1 = 'All'
        dfwc2 = pd.read_sql(f"SELECT * FROM {wc_table}", engine)
        wordlist = pd.read_sql(f"SELECT * FROM {wt_table}", engine)
        return render_template('index.html', url ='/static/wc.jpg', max_date = max_date, datenow = datenow, caldate = caldate, name = 'index', datefrom = datefrom, select1 = select1, dfwc2=dfwc2, wordlist=wordlist)            

@app.route('/update', methods=['POST'])
def update():
    opconame = request.form['opco-name']
    #Start ISPReview Beautiful Soup Scrape
    TCP_final_list = []
    ISPR_final_list = []
    keywords = ['EE', 'Three', 'O2', '3 UK', 'Vodafone', 'VMO2', 'MVNO', 'BT', 'TalkTalk', 'Virgin', 'Sky', 'CityFibre', 'Openreach', 'Ofcom', 'CMA', 'ASA', 'UK gov', 'Nokia', 'Ericsson', 'Huawei', 'NEC', 'Samsung', 'RootMetrics', 'Opensignal', 'Tutela', 'Ookla', 'nPerf', 'OpenRAN', 'TIP', 'ORAN', 'O-RAN', 'LoRA', 'Sigfox'
    ]
    countries = [
    {'country': 'UK', 'countryid': 195, 'name': 'United Kingdom'},
    {'country': 'AL', 'countryid': 1, 'name': 'Albania'},
    {'country': 'CZ', 'countryid': 171, 'name': 'Czech Republic'},
    {'country': 'DE', 'countryid': 215, 'name': 'Germany'},
    {'country': 'GR', 'countryid': 201, 'name': 'Greece'},
    {'country': 'HU', 'countryid': 172, 'name': 'Hungary'},
    {'country': 'IE', 'countryid': 188, 'name': 'Ireland'},
    {'country': 'IT', 'countryid': 203, 'name': 'Italy'},
    {'country': 'NL', 'countryid': 219, 'name': 'Netherlands'},
    {'country': 'PT', 'countryid': 207, 'name': 'Portugal'},
    {'country': 'RO', 'countryid': 175, 'name': 'Romania'},
    {'country': 'ES', 'countryid': 211, 'name': 'Spain'},
    {'country': 'TK', 'countryid': 166, 'name': 'Turkey'}]

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
    df['country'] = 'UK'

    #Start TelecomPaper API scrape
    for country in countries:
        payload = {
            "operationName": None,
            "variables": {
                "first": 100,
                "where": {
                    "country": {"id": {"eq": country['countryid']}},
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
            article = {'date' : date, 'title' : title, 'summary' : summary, 'url' : url, 'country':country['country']}
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
    union=union[['category', 'filter','date', 'title', 'summary', 'url', 'country']]

    engine = sqlalchemy.create_engine(f'mysql+pymysql://{sqlconnection}')
    sqlimport = pd.read_sql(f'{db_table}', engine)

    tempfile = union.merge(sqlimport, on='title', how='left', indicator=True, suffixes = ('', '_y')).query('_merge == "left_only"')
    tempfile = tempfile[['category', 'filter', 'date', 'title', 'summary', 'url', 'country']]

    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    tempfile['timestamp'] = timestamp
    tempfile.to_sql(
        name= f'{db_table}',
        con = engine,
        index = False,
        if_exists='append'
    )
    
    df = pd.read_sql(f'SELECT category,filter, date, title, timestamp FROM {db_table} ORDER BY date desc limit 10', engine)
    max_date = pd.read_sql(f'SELECT min(date) as date, max(timestamp) as last, count(title) as num FROM {db_table}', engine)
    dfwc2 = pd.read_sql(f"SELECT * FROM {wc_table}", engine)
    wordlist = pd.read_sql(f"SELECT * FROM {wt_table}", engine)
    datefrom = (datetime.now() - relativedelta(months=1)).strftime('%Y-%m-%d')
    datenow = datetime.now().strftime('%Y-%m-%d')
    caldate = datenow
    select1 = 'All'
    return render_template('index.html', url ='/static/wc.jpg', feedback = 'success', max_date = max_date, datenow = datenow, caldate = caldate, name = 'index', datefrom = datefrom, select1 = select1, dfwc2=dfwc2, wordlist=wordlist)


@app.route('/donwload')
def download():
    dfwc2 = pd.read_sql(f"SELECT * FROM {wc_table}", engine)
    return send_file('./static/wc.jpg', attachment_filename=f"Wordcloud_{ dfwc2['fromdate'][0] }-{ dfwc2['todate'][0] }_filter_{ dfwc2['catfilter'][0] }-{ dfwc2['country'][0] }.jpg", as_attachment=True)  

@app.route('/vacancies', methods=['GET', 'POST'])
def vacancies():
        if request.method == 'POST':
            form_check = request.form['form-name']
            if form_check == 'updateform':
                headers = {
                    'authority': 'uk.indeed.com',
                    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                    'accept-language': 'en-US,en;q=0.9',
                    'referer': 'https://uk.indeed.com/cmp/Vodafone/jobs',
                    'sec-fetch-dest': 'document',
                    'sec-fetch-mode': 'navigate',
                    'sec-fetch-site': 'same-origin',
                    'sec-fetch-user': '?1',
                    'sec-gpc': '1',
                    'upgrade-insecure-requests': '1',
                    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.41 Safari/537.36',
                }

                companylist = ['Vodafone', 'Ee-1', 'O2-8', 'Three', 'Virgin-Media']

                jobids = []
                joblist = []
                jobcount = 0
                jobsnext_page = True
                jobsresponse_code = 200
                for cmp in companylist:
                    while jobsnext_page:
                        url = f'https://uk.indeed.com/cmp/{cmp}/jobs?start={jobcount*100}&clearPrefilter=1#cmp-skip-header-desktop'
                        response = requests.get(url, headers=headers)
                        jobsresponse_code = response.status_code
                        if jobsresponse_code == 200:
                            soup = BeautifulSoup(response.content, 'lxml')
                            for item in soup.find_all('li', attrs={'data-testid' : True}):
                                dictt = {}
                                for l in item.find_all('div', attrs={'data-testid' : True}):
                                    if l['data-testid'] != 'false':
                                        dictt[l['data-testid']] = l.text
                                dictt['company'] = cmp
                                dictt['jobidraw'] = item.get('data-tn-entityid')
                                dictt['country'] = 'UK'
                                joblist.append(dictt)
                            for item in soup.find_all('a', attrs={'title' : True}):
                                if item['title'] != 'Next':
                                    jobsnext_page = False
                                elif item['title'] == 'Next':
                                    jobsnext_page = True
                                    jobcount += 1
                    jobcount = 0
                    jobsnext_page = True
                    
                jobs = pd.DataFrame(joblist)
                jobs.drop(axis=1, columns=['jobListItem-salary', 'jobListItem-tags', 'jobListItem-indeedApply', 'jobListItem-urgentHire'], inplace=True)
                jobs['calcdate'] = jobs.apply(lambda x: 1 if x['jobListItem-date'].find('hour') > 0 else 24, axis=1)
                jobs['jobListItem-date']=jobs['jobListItem-date'].replace(regex=[' hours ago', ' days ago', ' hour ago', ' day ago'], value='')
                jobs['company']=jobs['company'].replace(regex=['-1', '-8'], value='')
                jobs['company']=jobs['company'].replace(regex=['Ee'], value='EE')
                jobs = jobs[jobs["jobListItem-date"].str.contains("30\+")==False]
                jobs['jobListItem-date']= pd.to_numeric(jobs['jobListItem-date'], errors='coerce')
                jobs['hours'] = jobs['jobListItem-date'] * jobs['calcdate']
                jobs['timestamp'] = pd.to_datetime("now", utc=True)
                jobs['datelisted'] = (jobs['timestamp'] - pd.to_timedelta(jobs['hours'], unit='h')).dt.strftime('%Y-%m-%d')
                jobs[['one','jobid', 'three']] = jobs['jobidraw'].str.split(",",expand=True)
                jobs.drop(axis=1, columns=['calcdate', 'hours', 'timestamp', 'jobListItem-date', 'one', 'three', 'jobidraw'], inplace=True)
                jobs.rename(columns={'jobListItem-title':'title', 'jobListItem-location':'location'}, inplace=True)

                jobssqlimport = pd.read_sql(f'{jb_table}', engine)
                jobscheckfile = jobs.merge(jobssqlimport, on='jobid', how='left', indicator=True, suffixes = ('', '_y')).query('_merge == "left_only"')
                jobscheckfile = jobscheckfile[['title', 'location', 'company', 'country', 'datelisted', 'jobid']]
                jobsadded= len(jobscheckfile)
                jobscheckfile.to_sql(
                    name= f'{jb_table}',
                    con = engine,
                    index = False,
                    if_exists='append'
                )
                max_date = pd.read_sql(f'SELECT max(datelisted) as last, count(title) as num FROM {jb_table}', engine)
                datenow = (datetime.now() - relativedelta(months=1)).strftime('%Y-%m-%d')
                caldate = datenow
                datefrom = (datetime.now() - relativedelta(months=1)).strftime('%Y-%m-%d')
                datenow = (datetime.now()).strftime('%Y-%m-%d')
                select1 = 'All'
                dfwc2 = pd.read_sql(f"SELECT * FROM {jwc_table}", engine)
                
                return render_template('vacancies.html', url ='/static/jwc.jpg', feedback = 'success', name = 'vacancies', max_date = max_date, datenow = datenow, caldate = caldate, datefrom = datefrom, select1 = select1, dfwc2=dfwc2, jobsadded=jobsadded)
            
            elif form_check == 'downloadform':
                caldate = datetime.strptime(request.form['datefrom'], '%Y-%m-%d')
                caldate2 = caldate.strftime('%Y-%m-%d')
                caldate = (caldate -relativedelta(days=1)).strftime('%Y-%m-%d')
                max_date = pd.read_sql(f'SELECT max(datelisted) as last, count(title) as num FROM {jb_table}', engine)
                datefrom = (datetime.now() - relativedelta(months=1)).strftime('%Y-%m-%d')
                datenow = datetime.now().strftime('%Y-%m-%d')
                dfdl = pd.read_sql(f"SELECT * FROM {jb_table} WHERE datelisted > '{ caldate }' ORDER BY datelisted desc", engine)
                dfdl.to_excel('./static/JA_download.xlsx', encoding = 'utf-8-sig', index = False)
                return send_file('./static/JA_download.xlsx', attachment_filename=f"JADownload_{ caldate2 }-{ datenow }.xlsx")        

            elif form_check == 'wordcloudform':
                dateto = request.form['dateto']
                datefrom = request.form['datefrom']
                datenow = (datetime.now()).strftime('%Y-%m-%d')
                caldate = datenow
                select1 = request.form['select1']
                max_date = pd.read_sql(f'SELECT max(datelisted) as last, count(title) as num FROM {jb_table}', engine)
                wordclouddf = pd.DataFrame({"fromdate":[datefrom], "todate":[dateto], 'catfilter':[select1], 'country':'UK'})
                wordclouddf.to_sql(
                    name= f'{wc_table}',
                    con = engine,
                    index = False,
                    if_exists='replace'
                )
                complist = ['EE', 'Three', 'O2', 'Vodafone', 'VMO2', 'BT', 'TalkTalk', 'Virgin', 'Sky', 'CityFibre']
                dfwc1 = pd.read_sql(f"SELECT * FROM {jb_table} WHERE datelisted > '{ datefrom }' AND datelisted < '{ dateto }'", engine)
                logo = np.array(PIL.Image.open('./static/no_logo.jpg')) 
                if select1 == 'VF':
                    dfwc1 = dfwc1[dfwc1['company'].str.contains('Vodafone', na=False)]
                    logo = np.array(PIL.Image.open('./static/Vodaphone logo.jpg')) 
                elif select1 == 'Comp':
                    dfwc1 = dfwc1[~dfwc1['company'].str.contains('Vodafone', na=False)]
                elif select1 != 'None':
                    dfwc1 = dfwc1[dfwc1['category'].str.contains(select1, na=False)]
                textfile = ''
                dfwc = dfwc1['title']
                for row in dfwc:
                    textfile = textfile + ' ' + str(row)

                if textfile == '':
                    textfile = 'None'  
                textfile = textfile.title()  
                replacelist = {'Virgin Media':'VirginMedia', 'Bt':'BT', 'Ee':'EE', 'Isp':'ISP', '3 Uk':'Three', 'Deutsche Telekom':'DeutscheTelekom', 'Turk Telekom':'TurkTelekom'}
                for key in replacelist:
                    textfile = textfile.replace(key, replacelist[key])
                wordfilter = set(list(STOPWORDS)+['UK', 'Full', 'Premises', 'powers', 'Add', 'service', 'new', 'Zen', 'Scotland',
                                        'British', 'dollar', 'network', 'Operator', 'Slashes', 'signs', 'North', 'million',
                                        'Maps', 'Map', 'US', 'GBP', 'England', 'Ireland', 'Germany', 'German', 'Deutsche', 'Greece', 'Eur', 'Irish', 'Hungary',
                                        'Czechia', 'Czech', 'Italian', 'Italia', 'Italy', 'Dutch', 'Netherlands', 'Portugal', 'Portuguese', 'Lisbon', 'Romania',
                                        'Romanian', 'Turkey'])
                    
                wc = WordCloud(collocations=False,
                            stopwords = wordfilter,
                            mask = logo,
                            contour_color='red',
                            contour_width=2,
                            background_color='white').generate(textfile)

                wc.to_file('./static/jwc.jpg')    
                dfwc2 = pd.read_sql(f"SELECT * FROM {wc_table}", engine)
                # create a dictionary of word frequencies
                text_dictionary = wc.process_text(textfile)
                # sort the dictionary
                word_freq={k: v for k, v in sorted(text_dictionary.items(),reverse=True, key=lambda item: item[1])}
                wordlist = pd.DataFrame.from_dict(word_freq, orient='index')
                wordlist.index.names = ['Words']
                wordlist.rename(columns={0 : "Freq"}, inplace = True)
                wordlist = wordlist.reset_index()
                wordlist.to_sql(
                    name= f'{wt_table}',
                    con = engine,
                    index = False,
                    if_exists='replace'
                )

                return render_template('vacancies.html', url ='/static/jwc.jpg', name = 'vacancies', max_date = max_date, datenow = datenow, caldate = caldate, datefrom = datefrom, select1 = select1, dfwc2=dfwc2)  
            elif form_check == 'wordclouddownload':
                dfwc2 = pd.read_sql(f"SELECT * FROM {wc_table}", engine)
                return send_file('./static/jwc.jpg', attachment_filename=f"JobsWordcloud_{ dfwc2['fromdate'][0] }-{ dfwc2['todate'][0] }_filter_{ dfwc2['catfilter'][0] }-{ dfwc2['country'][0] }.jpg", as_attachment=True)      

        else :
            max_date = pd.read_sql(f'SELECT max(datelisted) as last, count(title) as num FROM {jb_table}', engine)
            datenow = (datetime.now() - relativedelta(months=1)).strftime('%Y-%m-%d')
            caldate = datenow
            datefrom = (datetime.now() - relativedelta(months=1)).strftime('%Y-%m-%d')
            datenow = (datetime.now()).strftime('%Y-%m-%d')
            select1 = 'All'
            dfwc2 = pd.read_sql(f"SELECT * FROM {jwc_table}", engine)
            return render_template('vacancies.html', url ='/static/jwc.jpg', name = 'vacancies', max_date = max_date, datenow = datenow, caldate = caldate, datefrom = datefrom, select1 = select1, dfwc2=dfwc2)  

@app.route('/country/<opco>')
def country(opco):
        if request.method == 'POST':
            caldate = datetime.strptime(request.form['datefrom'], '%Y-%m-%d')
            caldate2 = caldate.strftime('%Y-%m-%d')
            caldate = (caldate -relativedelta(days=1)).strftime('%Y-%m-%d')
            max_date = pd.read_sql(f"SELECT min(date) as date, max(timestamp) as last, count(title) as num FROM {db_table} WHERE country = '{opco}'", engine)
            datefrom = (datetime.now() - relativedelta(months=1)).strftime('%Y-%m-%d')
            datenow = datetime.now().strftime('%Y-%m-%d')
            dfdl = pd.read_sql(f"SELECT * FROM {db_table} WHERE date > '{ caldate }' AND country like '{opco}' ORDER BY date desc", engine)
            dfdl.to_excel('./static/CI_download.xlsx', encoding = 'utf-8-sig', index = False)
            return send_file('./static/CI_download.xlsx', attachment_filename=f"CIDownload_{ caldate2 }-{ datenow }.xlsx")
        else:
            max_date = pd.read_sql(f"SELECT min(date) as date, max(timestamp) as last, count(title) as num FROM {db_table} WHERE country = '{opco}'", engine)
            datenow = (datetime.now() - relativedelta(months=1)).strftime('%Y-%m-%d')
            caldate = datenow
            datefrom = (datetime.now() - relativedelta(months=1)).strftime('%Y-%m-%d')
            datenow = (datetime.now()).strftime('%Y-%m-%d')
            select1 = 'All'
            dfwc2 = pd.read_sql(f"SELECT * FROM {wc_table}", engine)
            wordlist = pd.read_sql(f"SELECT * FROM {wt_table}", engine)
            return render_template('index.html', url ='/static/wc.jpg', opco = opco, max_date = max_date, datenow = datenow, caldate = caldate, name = 'index', datefrom = datefrom, select1 = select1, dfwc2=dfwc2, wordlist=wordlist) 