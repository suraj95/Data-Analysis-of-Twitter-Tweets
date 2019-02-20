# -*- coding: utf-8 -*-

import sys
import json
import twitter
import csv

from oauthLogin import oauthLogin

QUERY = '#edm'
MAX_PAGES = 30
RESULTS_PER_PAGE = 100

def search(twitterSearch, max_batches=MAX_PAGES, count=RESULTS_PER_PAGE):

    # See https://dev.twitter.com/docs/api/1.1/get/search/tweets
    searchResults = twitterSearch.search.tweets(q = QUERY, count = RESULTS_PER_PAGE)
    #searchResults = json.dumps(searchResults)
    #searchResults = json.loads(searchResults)
    statuses = searchResults['statuses']
#    print(len(statuses))
    nextID = min([int(i['id']) for i in statuses])
    
    for _ in range(1, MAX_PAGES):
        nextStatuses = twitterSearch.search.tweets(q = QUERY, lang = 'en', count = RESULTS_PER_PAGE, max_id = nextID)['statuses']
        #nextStatuses = json.dumps(nextStatuses)
        #nextStatuses = json.loads(nextStatuses)
#        print(len(nextStatuses))
        nextID = min([int(i['id']) for i in nextStatuses])
        statuses += nextStatuses

#    print(type(searchResults))
#    print(type(searchResults['statuses']))
#    print(type(statuses[0]['text']))
#    print(type(statuses[0]))
#    print(type(statuses))

    #file = open(QUERY + '.csv', 'w', newline = '', encoding = 'utf-8')
    file = open(QUERY + '.csv', 'w', newline = '')
    #fieldnames = ['id', 'text', 'class']
    csvwrite = csv.writer(file)
    #csvwrite = csv.DictWriter(file, fieldnames = fieldnames)
    csvwrite.writerow([0,"Text","User","User ID","Description","Location"])
    for i in range(1,len(statuses)):
        #csvwrite.writerow([i, statuses[i]['text'], QUERY])
        #print(str(statuses[i]['user']['name'].encode('utf-8'))[2:-1])
        csvwrite.writerow([i, str(statuses[i]['text'].encode('utf-8'))[2:-1],str(statuses[i]['user']['name'].encode('utf-8'))[2:-1],str(statuses[i]['user']['id_str'].encode('utf-8'))[2:-1],str(statuses[i]['user']['description'].encode('utf-8'))[2:-1],str(statuses[i]['user']['location'].encode('utf-8'))[2:-1]])
        #csvwrite.writerow({'id': i, 'text':statuses[i]['text'].encode('utf-8'), 'class':QUERY})
    file.close()

    return statuses

if __name__ == '__main__':

    twitterSearch = oauthLogin()
    statuses = search(twitterSearch)
#    print(statuses[0]['text'])
#    print(statuses[10]['text'])
#    print(statuses[50]['text'])
