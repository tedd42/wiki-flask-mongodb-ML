#import matplotlib.pyplot as plt
import numpy as np
#import pandas as pd

#from scipy import stats
import pymongo
import re
import requests
from bs4 import BeautifulSoup

from time import sleep
from tqdm import tqdm

import pickle
#import seaborn as sns
from datetime import datetime

# mongo instance
cli = pymongo.MongoClient('34.218.16.158',27016)

#mongo collection for reading and writing
wiki = cli.my_collections['wiki_coll']


# generate responses for individual pages and category pages
def gen_response_title(id_title,input_='pageid'):
    if input_ == 'pageid':
        url = 'https://en.wikipedia.org/w/api.php?format=json&action=query&prop=extracts&pageids={}&redirects=true'.format(id_title)
        response = requests.get(url)
        return response
    elif input_ == 'title':
        url = 'https://en.wikipedia.org/w/api.php?format=json&action=query&prop=extracts&titles={}&redirects=true'.format(id_title)
        response = requests.get(url)
        return response

def gen_response_cat(id_title,input_='pageid'):
    if input_ == 'pageid':
        url = 'https://en.wikipedia.org/w/api.php?action=query&list=categorymembers&cmlimit=500&cmpageid={}&format=json'.format(id_title)
        response = requests.get(url)
        return response
    elif input_ == 'title':
        url = 'https://en.wikipedia.org/w/api.php?action=query&list=categorymembers&cmlimit=500&cmtitle=Category:{}&format=json'.format(id_title)
        response = requests.get(url)
        return response

# function for quickly getting collref and cursor

def cursor_coll_ref(client,table):
    db = client.my_database
    coll_ref_func = db[table]
    cursor_func = coll_ref_func.find()
    return {'coll_ref':coll_ref_func,'cur': cursor_func}

# get data from category title pages

def title_data(url_o_id,input_='url'):
    if input_ == 'url':
        resp = requests.get(url_o_id)
        id_ = list(resp.json()['query']['pages'].keys())[0]
        pag_text = resp.json()['query']['pages'][id_]['extract']
        p_title = resp.json()['query']['pages'][id_]['title']
        soup = BeautifulSoup(pag_text,'lxml')
        soup_txt = soup.text
        soup_txt_clean = [word for word in soup_txt.split() if "'\'" not in word ]
        txt = " ".join(soup_txt_clean)
        node_data = {'pageid': id_,'title':ml_title,'text':txt}
        return node_data
    elif input_ == 'pageid':
        url = 'https://en.wikipedia.org/w/api.php?format=json&action=query&prop=extracts&pageids={}&redirects=true'.format(url_o_id)
        resp = requests.get(url)
        id_ = list(resp.json()['query']['pages'].keys())[0]
        pag_text = resp.json()['query']['pages'][id_]['extract']
        p_title = resp.json()['query']['pages'][id_]['title']
        soup = BeautifulSoup(pag_text,'lxml')
        soup_txt = soup.text
        soup_txt_clean = [word for word in soup_txt.split() if "'\'" not in word ]
        txt = " ".join(soup_txt_clean)
        node_data = {'pageid': id_,'title':ml_title,'text':txt}
        return node_data
    elif input_ == 'title':
        url = 'https://en.wikipedia.org/w/api.php?format=json&action=query&prop=extracts&titles={}&redirects=true'.format(url_o_id)
        resp = requests.get(url)
        id_ = list(resp.json()['query']['pages'].keys())[0]
        pag_text = resp.json()['query']['pages'][id_]['extract']
        p_title = resp.json()['query']['pages'][id_]['title']
        soup = BeautifulSoup(pag_text,'lxml')
        soup_txt = soup.text
        soup_txt_clean = [word for word in soup_txt.split() if "'\'" not in word ]
        txt = " ".join(soup_txt_clean)
        node_data = {'pageid': id_,'title':p_title,'text':txt}
        return node_data
        

# supporting function for insert_ cleanish_pages
def get_titles_ids(response):
    pages = response.json()['query']['categorymembers']

    page_lis = []
    for page in pages:
        page_lis.append((page['pageid'],page['title']))
        
    return page_lis


def cleaner(url,pos,root):
    req = requests.get(url)
    id_ = ''.join(list(req.json()['query']['pages'].keys()))
    p = req.json()['query']['pages'][str(id_)]
    title = p['title']
    presoup = p['extract']
    soup = BeautifulSoup(presoup,'lxml')
    soup_txt = soup.text
    soup_txt_clean = [word for word in soup_txt.split() if "'\'" not in word ]
    txt = " ".join(soup_txt_clean)
    dicty = {'pageid': id_,'title':title,'text':txt,'position': pos,'root': root}
    return dicty
    

def insert(insert_ref,id_,root,input_,pos='child'):
    if input_ == 'pageid':
        coll_ref1 = insert_ref
        url = 'https://en.wikipedia.org/w/api.php?format=json&action=query&prop=extracts&pageids={}'.format(id_)
        clean = cleaner(url,pos,root)
        coll_ref1.insert_one(clean)
        try:
            
            print('inserting title: {} \n {}...'.format(clean['title'],clean['text'][:100]))
        except:
            print(clean)
         
    elif input_ == 'title':
        coll_ref2 = insert_ref
        url = 'https://en.wikipedia.org/w/api.php?format=json&action=query&prop=extracts&titles={}'.format(id_)
        clean = cleaner(url,pos,root)
        coll_ref2.insert_one(clean)
        try:
            print('inserting title: {} \n {}... '.format(clean['title'],clean['text'][:100]))
        except:
            print(clean)
   
# insert clean pages from category list
def insert_pages(title_,coll_ref):
    response = gen_response_cat(title_,input_='title')
    insert(coll_ref,title_,title_,input_='title',pos='root')
    for id_,title in get_titles_ids(response):
        url_page = 'https://en.wikipedia.org/w/api.php?format=json&action=query&prop=extracts&pageids={}&redirects=true'.format(id_)
        insert(coll_ref,id_,title_,'pageid',pos='child')
            
  
# insert clean pages from category list
def insert_recur(title_,root,coll_ref):
    response = gen_response_cat(title_,input_='title')
    for id_,title in get_titles_ids(response):
        url_page = 'https://en.wikipedia.org/w/api.php?format=json&action=query&prop=extracts&pageids={}&redirects=true'.format(id_)
        insert(coll_ref,id_,root,'pageid',pos='recursive_child')


   
def recur_depth(title_,root,coll_ref,depth):
    title_mas = title_
    for i in range(depth):
        resp = gen_response_cat(title_mas,input_='title')
        page_lis = get_titles_ids(resp)

        for id_,title__ in page_lis:
            
           
            if 'Category:' in title__:
                print(title__)
   
                print('inserting subcat {}'.format(title__))
                try:
                    subcollref = coll_ref
                    insert_recur(title__[9:],root,subcollref)
                    title_mas = title__
                         
                except:
                    print('fail')
                    
                    
# insert data into mongo
def get_mongo_and_recur(titles, depth, coll_ref):
    for title in titles:
        insert_pages(title,coll_ref)
        recur_depth(title,title,coll_ref,depth)
        

# get category input from 
def get_cat_input():
    lis = []
    n = input("input category to add('x' to exit): ")
    
    def shell(n):
        try:
            try:
                tit = title_data(n,input_='title')['pageid']
            except:
                print ('not a category title')
                return  False
        
            k =wiki.find({'pageid': tit}, {'pageid': 1}).limit(1)
            next(k)
            print("category already loaded in the DB server")
            return False
        except: 
            print("adding category... {}".format(n))
            return True
           

    while n != 'x':  
        if shell(n):
            lis.append(n)
            n = input("input category to add('x' to exit): ")

        else:
            
            n = input("input category to add('x' to exit): ")

    return lis
       
