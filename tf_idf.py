# -*- coding: utf-8 -*-
import sys
import datetime
import json

from pymongo import MongoClient
sys.path.insert(0, "../")
from util.tfidf import TfIdf
from util.elk import Elastic
from textrank import extractKeyphrases

keyWordNum = None
elk_host = None
elk_port = None
elk_index = None
elk_type = None
mongo_host = None
mongo_port = None

def loadConfig():
    with open("config.json", "r") as con:
        config = json.load(con)
    global elk_host
    global elk_port
    global elk_index
    global elk_type
    global mongo_host
    global mongo_port
    global keyWordNum

    elk_host = config['elk_host']
    elk_port = int(config['elk_port'])
    elk_index = config['elk_index']
    elk_type = config['elk_type']
    mongo_host = config['mongo_host']
    mongo_port = int(config['mongo_port'])
    keyWordNum = int(config['keyWordNum'])

def tfidfRun():
    """
    Calcuate the tfidf of today's pastebin.com pastes at MongoDB, update the keywords to elk.
    """
    loadConfig()

    client = MongoClient(mongo_host, mongo_port)
    elk = Elastic(elk_host, elk_port)
    db = client['pastebin']
    pastebin = db.pastebin

    now = datetime.datetime.now()
    date = now.strftime("%Y-%m-%d")
    startdate = datetime.datetime.strptime(date,'%Y-%m-%d')
    print(startdate)

    cursor = pastebin.find({"@timestamp": {"$gt": startdate}});
    print ("article count: %d" % cursor.count())
    if cursor.count() == 0:
        print ('Today %s has no data to analysis.' % (date))
        sys.exit(0)
    article = cursor[:]

    corpus = []
    mapping = {}
    i = 0
    for row in article:
        text = row['text']
        corpus.append(text)
        mapping[i] = row['pid']
        i = i + 1

    tfi = TfIdf(corpus, keyWordNum)
    tfi.tfidf()
    keywordsArr = tfi.keywordsArr

    now = datetime.datetime.now()
    date = now.strftime("%Y-%m-%d")
    for n, json_data in enumerate(keywordsArr):
        jbody= {"doc":{"tfidf": json_data}}
        elk.update2elk(elk_index, elk_type, mapping[n], jbody)
    print("\nComplete!")

def textRank():
    """
    Calcuate the textRank of today's pastebin.com pastes at MongoDB, update the keywords to elk.
    """
    loadConfig()

    client = MongoClient(mongo_host, mongo_port)
    elk = Elastic(elk_host, elk_port)
    db = client['pastebin']
    pastebin = db.pastebin

    now = datetime.datetime.now()
    date = now.strftime("%Y-%m-%d")
    startdate = datetime.datetime.strptime(date,'%Y-%m-%d')
    print(startdate)

    cursor = pastebin.find({"@timestamp": {"$gt": startdate}});
    print ("article count: %d" % cursor.count())
    if cursor.count() == 0:
        print ('Today %s has no data to analysis.' % (date))
        sys.exit(0)
    article = cursor[:]

    for row in article:
        try:
            text = row['text']
            keyphrases = extractKeyphrases(text, keyWordNum)
            jbody= {"doc":{"tfidf": keyphrases}}
            elk.update2elk(elk_index, elk_type, row['pid'], jbody)
        except:
            print("pastebin textRank error.")
            continue
    print("\nComplete!")

if __name__=="__main__":
    #tfidfRun()
    textRank()


