import urllib2
import requests
from bs4 import BeautifulSoup
import re
from uuid import uuid4  
import sys, traceback
from cassandra.cluster import Cluster
import datetime 
from dateutil.parser import parse
cluster = Cluster(['127.0.0.1'])
session = cluster.connect('forextrade')

base_url  = 'http://www.trade2win.com/boards/forex/index1.html'

content = urllib2.urlopen(base_url).read()         
soup = BeautifulSoup(content)
while True:

    try:
        urls =  soup.find('div','pagenav') 
        next_url = urls.find('a',{'rel':'next'})['href']
        content = urllib2.urlopen(base_url).read()               
        soup = BeautifulSoup(content)     
        urls = [x.find('td',id=re.compile('td_threadtitle_\d+')).find('a',id=re.compile('thread_title_\d+'))['href'] for x in soup.find('tbody',{'id':'threadbits_forum_54'}).findAll('tr',recursive=False)]         
        for x in urls[4:]:   

            raw = urllib2.urlopen(x).read()    
            soup = BeautifulSoup(raw)                   
            post_tags = soup.find('div',id='posts')
            post_tags_all = post_tags.findAll('div',id=re.compile('edit\d+'))
            for post in post_tags_all:
                page = {}
                try:
                     page['author'] =  post.find('div',id=re.compile('postmenu_\d+')).text.encode("utf-8").strip()
                except:
                    print "error occurred in author"
                    print traceback.print_exc()
                try:
                     page['data'] = post.find('div',id=re.compile('post_message_\d+')).text.encode("utf-8").strip()
                except:
                    print "error occurred data"
                    print traceback.print_exc()
                try:
                    page['posts'] = post.find('td',attrs={'style':'font-weight:normal;'}).text.encode("utf-8").strip()
                except:
                    print "error occurred posted_date"
                    print traceback.print_exc()
                                                            
                        
                        # page['author'] =  post.find('div',id=re.compile('postmenu_\d+')).text.strip()               
                        # page['data'] = post.find('div',id=re.compile('post_message_\d+')).text.strip()
                        # page['posts'] = post.find('td',attrs={'style':'font-weight:normal;'}).text.strip()
                Author  = page['author']                
                Content = page['data']
                try:
                    date = parse(page['posts'])                              
                except:
                    print "date parse error"

                session.execute("""INSERT INTO trade2win (id, author, content, posted_date,url) VALUES (%s, %s, %s, %s, %s)""",(uuid4(),Author,Content,date.__str__(),x))         

        base_url = next_url
        content = urllib2.urlopen(base_url).read()
        soup = BeautifulSoup(content)
        
        
    except:
        print ("No more pagination")
        print traceback.print_exc()
        break



    # content = urllib2.urlopen(base_url).read()           
    # soup = BeautifulSoup(content) 
    # urls = [x.find('td',id=re.compile('td_threadtitle_s\d+')).find('a',id=re.compile('thread_title_\d+'))['href'] for x in soup.find('tbody',{'id':'threadbits_forum_54'}).findAll('tr',recursive=False)] 
    # for x in urls:   

    #     raw = urllib2.urlopen(x).read()    
    #     soup = BeautifulSoup(raw)                   
    #     post_tags = soup.findAll('div',id='posts')
    #     for post in post_tags:
    #         for datum in post.parent.next_siblings:                
    #             page = {}
    #             page['author'] =  post.find('div',id=re.compile('postmenu_\d+')).text.strip()               
    #             page['data'] = post.find('div',id=re.compile('post_message_\d+')).text.strip()
    #             page['posts'] = post.find('td',attrs={'style':'font-weight:normal;'}).text.strip()
    #             Author  = page['author']                
    #             Content = page['data']   
    #             date = parse(page['posts'])                              
    #             session.execute("""INSERT INTO trade2win (id, author, content, posts,url) VALUES (%s, %s, %s, %s, %s)""",(uuid4(),Author,Content,date.__str__(),x))         


            # page['author'] =  post.findAll('div',id=re.compile('postmenu_\d+'))                       
            # page['data'] = post.findAll('div',id=re.compile('post_message_\d+'))
            # page['posts'] = post.findAll('td',attrs={'style':'font-weight:normal;'})            
            # for j in page['author']:
            #     Author = j.text                                                
            # for l in page['data']:
            #     Content = l.text                
            # for k in page['posts']:
            #     Posted_date = k
            #     Posted_date1 = parse(Posted_date)
            #     print Posted_date1
            # session.execute("""INSERT INTO trade2win (id, author, content, posts) VALUES (%s, %s, %s, %s)""",(uuid4(),Author,Content,str(Posted_date1)))                     
                # print Posted_date                
                # print Content            
            # print Author
        # divtags = post.findAll('div',id=re.compile('postmenu_'))
        # Author = divtags[0].text
        # content = soup.findAll('div',id=re.compile('post_message_'))
        # print " "
        # print Author
        # print " "
        # print content
        # print " "


        
# urls = [y.find('a',id=re.compile('thread_title_\d+'))['href'] for y in x.findAll('td',id=re.compile('td_threadtitle_\d+')) for x in soup.find('tbody',{'id':'threadbits_forum_54'}).findAll('tr',recursive=False)]

# links = []
# for i in soup.findAll('tbody', {'id': 'threadbits_forum_4'}):
#   for x in i.findAll('tr'):
#       for y in x.findAll('div'):
#           for z in y.findAll('a'):
#               print z

#   urls = i['href']    
#   print urls

#for i in .findAll('a'):
    #urls =  i['href']
    #print urls
# print '\n'.join(urls)

    # link  = [x.find('a')['href'] for x in i.find('tbody','post_message_')]
    # for x in links:
    #   raw = urllib2.urlopen(links).read()
    #   soup = BeautifulSoup(raw)
    #   content = soup.findAll('table','tborder vbseo_like_postbit')
    #   for i in content:
    #       print i.renderContents()
    #   details = soup.findAll('div','smallfont')
    #   print(" ")
    #   print links
    #   print(" ")
    #   print content
    #   print(" ")
    # list_of_rows = []
    # for row in page.findAll('tr')[0]:
    #     list_of_cells = []
    #     link = []
    #     for cell in row.findAll('td'):
    #         text = cell.text.replace('&nbsp;', '')
    #         list_of_cells.append(text)
    #     print list_of_rows.append(list_of_cells)

    # outfile = open("./diabetesforum.csv", "wb")
    # writer = csv.writer(outfile)
    # writer.writerow(["Title","Smallname","Date","Author","Replied","Views"])
    # writer.writerows(list_of_rows)
