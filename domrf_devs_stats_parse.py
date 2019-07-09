import sys
import csv
import json
import re
import os
import sys
import pandas as pd
import smtplib as smtp
import socket
import ssl
from pandas.io.json import json_normalize
from urllib.request import urlopen, Request, URLError
from urllib.parse import quote
from urllib.parse import quote, urlsplit, urlunsplit
from random import randint
from time import sleep

def get_start_index(directory):
    return len(os.listdir(directory))
def iri_to_uri(iri):
    parts = urlsplit(iri)
    uri = urlunsplit((parts.scheme, 
                      parts.netloc.encode('idna').decode('ascii'), 
                      quote(parts.path), 
                      quote(parts.query, '='),
                      quote(parts.fragment),))
    return uri
def get_json(url_page, headers, data, timeout):
    flag = True
    while flag:
        try:
            if data == None:
                request = Request(iri_to_uri(url_page), headers=headers)
            else:
                request = Request(iri_to_uri(url_page), headers=headers, data=data)
            context = ssl._create_unverified_context()
            response = urlopen(request, context=context, timeout=timeout)
            content =  response.read().decode(response.headers.get_content_charset())
            content.replace('\\', '')
            flag = False
        except URLError as e:
            print(e)
            flag = True
        except socket.timeout as e:
            print(e)
            flag = True
    return json.loads(content)
def get_dataframe(directory):
    files = [os.path.join(directory, file) for file in os.listdir(directory)]
    df = pd.DataFrame()
    for file_load in files:
        with open(file_load) as file:
            data_json = json.load(file)
        df = df.append(json_normalize(data_json, sep='_'))
    df = df.reset_index()
    del df['index']
    return df
def translit(text):
    symbols = ('абвгдеёжзийклмнопрстуфхцчшщъыьэюяАБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯ ',
               'abvgdeejzijklmnoprstufhccss_y_euaABVGDEEJZIJKLMNOPRSTUFHCCSS_Y_EUA_')
    tr = {ord(a):ord(b) for a, b in zip(*symbols)}
    return text.translate(tr)
def send_mail(dest_email, email_text):
    error = []
    try:
        email = 'app.notifications@yandex.ru'
        password = 'Notify2019'
        subject = 'Data load notification'
        message = 'From: {}\nTo: {}\nSubject: {}\n\n{}'.format(email, dest_email, subject, email_text)
        server = smtp.SMTP_SSL('smtp.yandex.com')
        server.login(email, password)
        server.auth_plain()
        server.sendmail(email, dest_email, message)
        server.quit()
    except smtp.SMTPException as e:
        error.append(e)
    return error
def main():
    min_time_sleep = 1
    max_time_sleep = min_time_sleep + 4
    timeout = 10
    path = '{}/'.format(sys.argv[1])
    print('got path to save data: ', path)
    table_name = '{}dom_rf_developers_stats_{}.csv'.format(path, str(sys.argv[2]))
    print('got date: ', str(sys.argv[2]), ' | table name: ', table_name)
    directory = '{}/'.format(sys.argv[3])
    print('got directory for cache: ', directory)
    dest_email = sys.argv[4]
    print('got email for notifications: ', dest_email)
    month = str(sys.argv[2])[4:6]
    year = str(sys.argv[2])[0:4]
    print('got: montn {} | year {}'.format(month, year))
    url_page = 'https://наш.дом.рф/аналитика/grapi/v1/developer_group_table?month=' + month + '&year=' + year
    print('url: ', iri_to_uri(url_page))
    headers = {'content-type': 'application/json',
               'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.102 YaBrowser/18.11.1.805 Yowser/2.5 Safari/537.36'}
    data_json = get_json(url_page, headers, None, timeout)
    print('total elements: ', len(data_json['payload']))
    total_pages = len(data_json['payload'])
    count_trial = 0
    flag = True
    while flag:
        try:  
            print('trial: ', count_trial)
            page_start = get_start_index(directory)
            print('start page: ', page_start)
            for page in range(page_start, total_pages):
                id_dev = data_json['payload'][page]['developer_group_id']
                data_dev_json = data_json['payload'][page]
                url_page_dev = 'https://наш.дом.рф/аналитика/grapi/v1/developer_group_region?developerGroupId=' + str(id_dev)
                data_dev_json_id = get_json(url_page_dev, headers, None, timeout)
                temp_dev = []
                for reg_data in data_dev_json_id:
                    reg_data.update({'actual_date': year + '_' + month})
                    reg_data.update({'developer_group_id': data_dev_json['developer_group_id']})
                    reg_data.update({'developer_group_name': data_dev_json['developer_group_name']})
                    temp_dev.append(reg_data)
                filename = directory + 'batches_load_dev_' + str(page) + '_id' + str(id_dev) + '.txt'
                with open(filename, 'w') as file:
                    json.dump(temp_dev, file)
                sleep(randint(min_time_sleep, max_time_sleep))
            flag = False
        except BaseException as e:
            print(e)
            count_trial += 1
            sleep(randint(min_time_sleep, max_time_sleep))
            flag = True
    print('data frame creating...')
    df = get_dataframe(directory)
    df = df.apply(lambda x: x.fillna('') if x.dtype.kind in 'O' else x.fillna(0))
    print('data frame created of shape: ', df.shape)
    df.to_csv(table_name)
    print('saved to file: ', table_name)
    email_text = 'VTB Ya.Cloud: Data collected, table {} created'.format(table_name)
    error_mail = send_mail(dest_email, email_text)
    if error_mail:
        print('email was not sent to: {} | error: {}'.format(dest_email, error_mail))
    else:
        print('email was sent to: ', dest_email)

if __name__ == '__main__':
    main()
