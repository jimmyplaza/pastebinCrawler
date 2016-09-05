# -*- coding: utf-8 -*-
import time
import datetime
#import hashlib
import sys
sys.path.insert(0, "../")

import getopt
import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient
import pymongo
import re
import json

from util.elk import Elastic

elk_host = None
elk_port = None
elk_index = None
elk_type = None
elk_type2 = None

mongo_host = None
mongo_port = None
configPath = None

root_url = None
raw_url = None
db = None

def main(argv):
    loadConfig()
    length              = 0
    time_out            = False
    found_keywords      = []
    paste_list          = set([])
    file_name, keywords, append, run_time, match_total, crawl_total = initialize_options(argv)
    global db
    client = MongoClient(mongo_host, mongo_port)
    db = client['pastebin']

    print ("\nCrawling %s Ctrl+c to save file to %s" % (root_url, file_name))

    try:
        # Continually loop until user stops execution
        while True:
            root_soup = BeautifulSoup(fetch_page(root_url), 'lxml')
            if root_soup is None:
                print ('root_soup is empty!')
                continue

            #    For each paste in the public pastes section of home page
            new_pastes = find_new_pastes(root_soup)
            if new_pastes is None:
                continue
            for paste in new_pastes:
                #    look at length of paste_list prior to new element
                length = len(paste_list)
                paste_list.add(paste)

                #    If the length has increased the paste is unique since a set has no duplicate entries
                if len(paste_list) > length:
                    #    Add the pastes url to found_keywords if it contains keywords
                    raw_paste = raw_url+paste
                    found_keywords = find_keywords(raw_paste, found_keywords, keywords)
                else:
                    #    If keywords are not found enter time_out
                    time_out = True

            # Enter the timeout if no new pastes have been found
            if time_out:
                time.sleep(2)

            sys.stdout.write("\rCrawled total of %d Pastes, Keyword matches %d" % (len(paste_list), len(found_keywords)))
            sys.stdout.flush()

    except KeyboardInterrupt:
        write_out(found_keywords, append, file_name)

    ##    If http request returns an error and
    #except( urllib2.HTTPError, err):
        #if err.code == 404:
            #print ("\n\nError 404: Pastes not found!")
        #elif err.code == 403:
            #print ("\n\nError 403: Pastebin is mad at you!")
        #else:
            #print ("\n\nJesus! HTTP Error code ", err.code)
        #write_out(found_keywords, append, file_name)

    ##    If http request returns an error and
    #except (urllib2.URLError, err):
        #print ("\n\nJesus URLError, Error code ", err)
        #write_out(found_keywords, append, file_name)



def loadConfig():
    with open("config.json", "r") as con:
        config = json.load(con)
    global elk_host
    global elk_port
    global elk_index
    global elk_type
    global mongo_host
    global mongo_port
    global configPath
    global root_url
    global raw_url

    elk_host = config['elk_host']
    elk_port = int(config['elk_port'])
    elk_index = config['elk_index']
    elk_type = config['elk_type']
    mongo_host = config['mongo_host']
    mongo_port = int(config['mongo_port'])
    configPath = config['configPath']
    root_url = config['root_url']
    raw_url = config['raw_url']

def write_out(found_keywords, append, file_name):
    #     if pastes with keywords have been found
    if len(found_keywords):
        #    Write or Append out urls of keyword pastes to file specified
        if append:
            f = open(file_name, 'a')
        else:
            f = open(file_name, 'w')

        for paste in found_keywords:
            f.write(paste)
        print ("\n")
    else:
        print ("\n\nNo relevant pastes found, exiting\n\n")

def find_new_pastes(root_soup):
    new_pastes = []
    try:
        div = root_soup.find('div', {'id': 'menu_2'})
        ul = div.find('ul', {'class': 'right_menu'})
    except:
        print ('soup error\n')
        return None

    for li in ul.findChildren():
        if li.find('a'):
            new_pastes.append(str(li.find('a').get('href')).replace("/", ""))
    return new_pastes


def find_keywords(raw_url, found_keywords, keywords):

    elk = Elastic(elk_host, elk_port)
    flag = 0
    paste = fetch_page(raw_url)
    pattern = "([\d]{1,3}\.){3}[\d]{1,3}|((?!-)[\w-]{1,63}\.)+(com|info|net|org|biz|name|pro|zero|asia|cat|coop|edu|gov|int|jobs|mil|mobi|museum|post|tel|travel|xxx)\b"
    global db


    matchKeyword = []
    for keyword in keywords:
        if paste.lower().find(keyword) != -1:
            matchKeyword.append(keyword)
            found_keywords.append("found " + keyword + " in " + raw_url + "\n")
    if len(matchKeyword) > 0:
        print ("\n============Match===============")
        print (raw_url + ': ')
        print (matchKeyword)
        pat = re.compile(pattern)
        result = pat.search(paste)
        if result != None:
            flag = 1
            print ('******* Match IP & Domain rule: ')
            print (result.group(0))
        body = {
                "text": paste,
                "flag": flag,
                "keywords": matchKeyword,
                "url": raw_url,
                "@timestamp": datetime.datetime.now(),
        }
        print("PID:==========================")
        pid = raw_url.split('=')[1]
        try:
            db.pastebin.update({'pid':pid},{'$setOnInsert':body},upsert=True)
            print("Save to MongoDB")
        except pymongo.errors.PyMongoError as e:
            print("MongoDB error: %s " % e )
        elk.save2elk(elk_index, elk_type, pid, body)
    return found_keywords

def fetch_page(url):
    res = requests.get(url)
    return res.text

def initialize_options(argv):
    keywords         = []
    file_name        = 'log.txt'
    append           = False
    run_time         = 0
    match_total      = None
    crawl_total      = None

    try:
        opts, args = getopt.getopt(argv,"h:o:a")
    except getopt.GetoptError:
        print ('main.py ..... -o <outputfile>')
        sys.exit(2)

    for opt, arg in opts:
        if opt == '-h':
            print ('main.py ..... -o <outputfile>')
            sys.exit()
        elif opt == '-a':
            append = True
        elif opt == "-o":
            file_name = arg

    print(configPath)
    with open(configPath, 'r') as file:
        for line in file:
            line = line.rstrip('\n')
            keywords.append(line)
    print (keywords)
    return file_name, keywords, append, run_time, match_total, crawl_total

if __name__ == "__main__":
	main(sys.argv[1:])
