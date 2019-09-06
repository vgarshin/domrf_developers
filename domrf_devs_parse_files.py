# coding: utf-8

import os
import sys
import json
import re
import socket
import ssl
import pandas as pd
import smtplib as smtp
import requests
from pandas.io.json import json_normalize
from urllib.request import Request, urlopen, URLError, HTTPError
from urllib.parse import quote, urlsplit, urlunsplit, urlencode
from random import randint
from time import sleep
from bs4 import BeautifulSoup

USER_AGENT = 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 YaBrowser/19.6.1.153 Yowser/2.5 Safari/537.36'
MIN_TIME_SLEEP = 1
MAX_TIME_SLEEP = 10
MAX_COUNTS = 3
TIMEOUT = 10

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
def get_content(url_page, timeout, file=False):
    counts = 0
    content = None
    while counts < MAX_COUNTS:
        try:
            request = Request(url_page)
            request.add_header('User-Agent', USER_AGENT)
            context = ssl._create_unverified_context()
            response = urlopen(request, context=context, timeout=timeout)
            if file:
                content = response.read()
            else:
                content =  response.read().decode(response.headers.get_content_charset())
            break
        except URLError as e:
            counts += 1
            print('URLError | ', url_page, ' | ', e, ' | counts: ', counts)
            sleep(randint(MIN_TIME_SLEEP, MAX_TIME_SLEEP))
        except HTTPError as e:
            counts += 1
            print('HTTPError | ', url_page, ' | ', e, ' | counts: ', counts)
            sleep(randint(MIN_TIME_SLEEP, MAX_TIME_SLEEP))
        except socket.timeout as e:
            counts += 1
            print('socket timeout | ', url_page, ' | ', e, ' | counts: ', counts)
            sleep(randint(MIN_TIME_SLEEP, MAX_TIME_SLEEP))
    return content
def load_files(links, path, path_files):
    errors = {}
    for file_name, url_file in links.items():
        try:
            content = get_content(url_file, TIMEOUT, file=True)
            file_name = '{}{}{}'.format(path, path_files, file_name)
            with open(file_name, 'wb') as file:
                file.write(content)
        except BaseException as e:
            errors.update({file_name: e})
        sleep(randint(MIN_TIME_SLEEP, MAX_TIME_SLEEP))
    return errors
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
def send_msg_telegram(msg):
    errors = {}
    token = '999334460:AAGTmPoWtNRVZ-dF4Vf7EX19fsOTXWhYf7A'
    url = 'https://api.telegram.org/bot'
    proxies = {'https': 'socks5://10.128.0.32:18080'} #proxy through hetzner
    #proxies = {'https': 'socks5://212.237.34.93:6483', 'https': 'socks5://45.13.30.145:60079'} #free proxies
    method = '{}{}{}'.format(url, token, '/sendMessage')
    r = requests.post(
        method, 
        data={
            'chat_id': '@dataloadnotifications',
            'text': msg
        },
        proxies=proxies
    )
    if r.status_code != 200:
        errors.update({'error code': r.status_code, 'error text': r.text})
    return errors
def main():
    url_page = 'https://наш.дом.рф/Сервис'
    #---url for load list of all developers, set limit to 100K for overkill---
    url_page = '{}/api/erz/main/filter?offset=0&limit=100000&sortField=devShortNm&sortType=asc'.format(iri_to_uri(url_page))
    print('url: ', iri_to_uri(url_page))
    path = '{}/'.format(sys.argv[1])
    print('got path to save data: ', path)
    table_name = '{}dom_rf_developers_{}.csv'.format(path, sys.argv[2])
    print('got date: ', sys.argv[2], ' | table name: ', table_name)
    cache_path = '{}/'.format(sys.argv[3])
    print('got directory for cache: ', cache_path)
    path_files = '{}/'.format(sys.argv[4])
    print('got directory for files: ', path_files)
    dest_email = sys.argv[5]
    print('got email for notifications: ', dest_email)
    #---get list of all developers---
    content = get_content(url_page, TIMEOUT)
    data_json = json.loads(content)
    print('total elements: ', data_json['data']['count'])
    print('total developers in list: ', len(data_json['data']['developers']))
    count_trial = 0
    flag = True
    while flag:
        try:
            print('trial: ', count_trial)
            start_index = get_start_index(cache_path)
            for data_dev in data_json['data']['developers'][start_index:]:
                dev_id = data_dev['devId']
                #---get html data---
                url_page_dev = 'https://наш.дом.рф/Сервис/единый-реестр-застройщиков/застройщик/{}'.format(dev_id)
                html = get_content(iri_to_uri(url_page_dev), TIMEOUT)
                if html:
                    soup = BeautifulSoup(html, 'html.parser')
                    data_json_dev = soup.find('script', {'id': '__NEXT_DATA__', 'type': 'application/json'})
                    data_json_dev = json.loads(data_json_dev.contents[0])
                    data_dev.update(data_json_dev['props']['initialState']['erz']['builder'])
                    #---save developer data to file---
                    file_name = '{}batch_dev_id{}.txt'.format(cache_path, dev_id)
                    with open(file_name, 'w') as file:
                        json.dump(data_dev, file)
                    #---load all files for developer---
                    links = {}
                    for elm in data_dev['documents']['report']:
                        for inelm in elm:
                            links.update({inelm['fileNameDownload']: inelm['link']})
                    for elm in data_dev['documents']['rpd']:
                        file_name = '{}.{}'.format(elm['fileNameDownload'], 'pdf')
                        links.update({file_name: elm['rpdPdfLink']})
                    errors = load_files(links, path, path_files)
                    if errors:
                        print('load files for id {} erros: {}'.format(dev_id, errors))
                else:
                    print('bad url: ', iri_to_uri(url_page_dev))
                sleep(randint(MIN_TIME_SLEEP, MAX_TIME_SLEEP))
            flag = False
        except BaseException as e:
            print('BaseException main cycle | ', e)
            count_trial += 1
            sleep(randint(MIN_TIME_SLEEP, MAX_TIME_SLEEP))
            flag = True
    df = get_dataframe(cache_path)
    df.to_csv(table_name, sep='\t')
    print('saved to file: ', table_name)
    email_text = 'VTB Ya.Cloud: Data collected, table {} created'.format(table_name)
    error_mail = send_mail(dest_email, email_text)
    if error_mail:
        print('email was not sent to: {} | error: {}'.format(dest_email, error_mail))
    else:
        print('email was sent to: ', dest_email)
    error_tlg = send_msg_telegram(email_text)
    if error_tlg:
        print('message was not sent | error: {}'.format(error_tlg))
    else:
        print('message was sent')

if __name__ == '__main__':
    main()