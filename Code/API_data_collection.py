#!/usr/bin/env python
# coding: utf-8

# In[1]:


#import tweepy as tw
#import webbrowser
import time
import csv
import os
import pandas as pd
import requests
import json
#import searchtweets
from jsonmerge import Merger
import expansions # expansions.py


# In[2]:


# ----------------- attention ------------------------- #
# 
# 1. geo info reduced a lot: https://twittercommunity.com/t/has-geo-search-parameter-outlier-rate-difference-between-v1-1-and-v2-endpoint/152676
#    progress: set geo and go for overall look - most of keywords search don't provide geo info
# 2. rate limit : every 15 mins, each topic only lists 10K tweets


# In[3]:


bearer_token='AAAAAAAAAAAAAAAAAAAAAGx0PQEAAAAAU5tboUI%2FMJeuC54BBmLbFLOJybM%3D8IsaqdJKIhqF9wjyrOkDj89M8Iz1U1Rl7rryitHWZREiYRHIpm'
search_url = "https://api.twitter.com/2/tweets/search/all"


# In[5]:


# topic1: 5G query_params = {'query':'climatechange has:geo lang:en -is:retweet -is:reply',
query_params = {'query':'5G has:geo lang:en -is:retweet -is:reply',
                'start_time':'2020-01-01T00:00:00Z',
                'end_time':'2021-04-30T00:00:00Z',
                'max_results':'500',
                'expansions': 'geo.place_id,author_id',
                'tweet.fields': 'text,author_id,created_at,public_metrics,geo,lang',
                'user.fields': 'id,name,username,created_at,location,public_metrics',
                'place.fields': 'country_code,country,geo,id,place_type,name',
                'next_token': {}}


# In[175]:


FILE = '5G_1.json'
def create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers

def connect_to_endpoint(url, headers, params):
    response = requests.request("GET", search_url, headers=headers, params=params)
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()

def main():
    headers = create_headers(bearer_token)

    json_response = connect_to_endpoint(search_url, headers, query_params)
    print(json.dumps(json_response, indent=4, sort_keys=True))
    #json_response = expansions.flatten(json_response)
    
    with open(FILE, mode='a') as json_file:
        json_file.write(json.dumps(json_response))
        json_file.write("\n")  

    
    while json_response["meta"]["next_token"]:
        time.sleep(2)
        query_params["next_token"] = json_response["meta"]["next_token"]
        json_response = connect_to_endpoint(search_url, headers, query_params)
        #json_response = expansions.flatten(json_response)
        
        with open(FILE, mode='a') as json_file:
            json_file.write(json.dumps(json_response))
            json_file.write("\n")  

        if 'next_token' in json_response["meta"]:
            print(json_response["meta"]["next_token"])
        else:
            break

if __name__ == "__main__":
    main()


# In[176]:


#FILE = '5G_1.json'
schema = {
  "properties": {
    "data": {
      "mergeStrategy": "append"
    },
    "includes": {
      "type": "object",
      "properties": {
        "users": {
          "mergeStrategy": "append"
        },
        "start_times": {
          "mergeStrategy": "append"
        },
        "end_times": {
          "mergeStrategy": "append"
        },
        "tweets": {
          "mergeStrategy": "append"
        },
        "places": {
          "mergeStrategy": "append"
        },
        "expansions": {
          "mergeStrategy": "append"
        }
      }
    }
  }
}

merger = Merger(schema)

dataset = {}

with open(FILE, "r") as f:
    for line in f.readlines():
        dataset = merger.merge(dataset, json.loads(line))

dataset = expansions.flatten(dataset)

with open("5G_expanded1.json", "w") as f:
    f.write(json.dumps(dataset))

df1 = pd.json_normalize(dataset['data'])


# In[177]:


df1.head(3)


# In[178]:


df1['geo.country_code']


# In[180]:


df1.to_csv('df1.csv', encoding='utf-8', index=False)


# In[10]:


# topic2: pizza
query_params = {'query':'pizza has:geo lang:en -is:retweet -is:reply',
                'start_time':'2020-01-01T00:00:00Z',
                'end_time':'2021-04-30T00:00:00Z',
                'max_results':'500',
                'expansions': 'geo.place_id,author_id',
                'tweet.fields': 'text,author_id,created_at,public_metrics,geo,lang',
                'user.fields': 'id,name,username,created_at,location,public_metrics',
                'place.fields': 'country_code,country,geo,id,place_type,name',
                'next_token': {}}
FILE = 'pizza_1.json'
def create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers

def connect_to_endpoint(url, headers, params):
    response = requests.request("GET", search_url, headers=headers, params=params)
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()

def main():
    headers = create_headers(bearer_token)

    json_response = connect_to_endpoint(search_url, headers, query_params)
    print(json.dumps(json_response, indent=4, sort_keys=True))
    #json_response = expansions.flatten(json_response)
    
    with open(FILE, mode='a') as json_file:
        json_file.write(json.dumps(json_response))
        json_file.write("\n")  

    
    while json_response["meta"]["next_token"]:
        time.sleep(2)
        query_params["next_token"] = json_response["meta"]["next_token"]
        json_response = connect_to_endpoint(search_url, headers, query_params)
        #json_response = expansions.flatten(json_response)
        
        with open(FILE, mode='a') as json_file:
            json_file.write(json.dumps(json_response))
            json_file.write("\n")  

        if 'next_token' in json_response["meta"]:
            print(json_response["meta"]["next_token"])
        else:
            break

if __name__ == "__main__":
    main()


# In[198]:


FILE = 'pizza_1.json'
schema = {
  "properties": {
    "data": {
      "mergeStrategy": "append"
    },
    "includes": {
      "type": "object",
      "properties": {
        "users": {
          "mergeStrategy": "append"
        },
        "start_times": {
          "mergeStrategy": "append"
        },
        "end_times": {
          "mergeStrategy": "append"
        },
        "tweets": {
          "mergeStrategy": "append"
        },
        "places": {
          "mergeStrategy": "append"
        },
        "expansions": {
          "mergeStrategy": "append"
        }
      }
    }
  }
}

merger = Merger(schema)

dataset = {}

with open(FILE, "r") as f:
    for line in f.readlines():
        dataset = merger.merge(dataset, json.loads(line))

dataset = expansions.flatten(dataset)

with open("pizza_expanded1.json", "w") as f:
    f.write(json.dumps(dataset))

df2 = pd.json_normalize(dataset['data'])
df2.to_csv('df2.csv', encoding='utf-8', index=False)


# In[199]:


df2.head(2)


# In[200]:


df2['geo.country_code']


# In[14]:


# topic3 : (covid OR corona)


# In[18]:


query_params = {'query':' (covid OR corona) has:geo lang:en -is:retweet -is:reply',
                'start_time':'2020-01-01T00:00:00Z',
                'end_time':'2021-04-30T00:00:00Z',
                'max_results':'500',
                'expansions': 'geo.place_id,author_id',
                'tweet.fields': 'text,author_id,created_at,public_metrics,geo,lang',
                'user.fields': 'id,name,username,created_at,location,public_metrics',
                'place.fields': 'country_code,country,geo,id,place_type,name',
                'next_token': {}}
FILE = 'covid_1.json'
def create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers

def connect_to_endpoint(url, headers, params):
    response = requests.request("GET", search_url, headers=headers, params=params)
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()

def main():
    headers = create_headers(bearer_token)

    json_response = connect_to_endpoint(search_url, headers, query_params)
    print(json.dumps(json_response, indent=4, sort_keys=True))
    #json_response = expansions.flatten(json_response)
    
    with open(FILE, mode='a') as json_file:
        json_file.write(json.dumps(json_response))
        json_file.write("\n")  

    
    while json_response["meta"]["next_token"]:
        time.sleep(2)
        query_params["next_token"] = json_response["meta"]["next_token"]
        json_response = connect_to_endpoint(search_url, headers, query_params)
        #json_response = expansions.flatten(json_response)
        
        with open(FILE, mode='a') as json_file:
            json_file.write(json.dumps(json_response))
            json_file.write("\n")  

        if 'next_token' in json_response["meta"]:
            print(json_response["meta"]["next_token"])
        else:
            break

if __name__ == "__main__":
    main()


# In[201]:


FILE = 'covid_1.json'
schema = {
  "properties": {
    "data": {
      "mergeStrategy": "append"
    },
    "includes": {
      "type": "object",
      "properties": {
        "users": {
          "mergeStrategy": "append"
        },
        "start_times": {
          "mergeStrategy": "append"
        },
        "end_times": {
          "mergeStrategy": "append"
        },
        "tweets": {
          "mergeStrategy": "append"
        },
        "places": {
          "mergeStrategy": "append"
        },
        "expansions": {
          "mergeStrategy": "append"
        }
      }
    }
  }
}

merger = Merger(schema)

dataset = {}

with open(FILE, "r") as f:
    for line in f.readlines():
        dataset = merger.merge(dataset, json.loads(line))

dataset = expansions.flatten(dataset)

with open("covid_expanded1.json", "w") as f:
    f.write(json.dumps(dataset))

df3 = pd.json_normalize(dataset['data'])
df3.to_csv('df3.csv', encoding='utf-8', index=False)


# In[202]:


df3['geo.country_code']


# In[21]:


# topic 4 : bitcoin


# In[34]:


query_params = {'query':'bitcoin has:geo lang:en -is:retweet -is:reply',
                'start_time':'2020-01-01T00:00:00Z',
                'end_time':'2021-04-30T00:00:00Z',
                'max_results':'500',
                'expansions': 'geo.place_id,author_id',
                'tweet.fields': 'text,author_id,created_at,public_metrics,geo,lang',
                'user.fields': 'id,name,username,created_at,location,public_metrics',
                'place.fields': 'country_code,country,geo,id,place_type,name',
                'next_token': {}}
FILE = 'bitcoin_1.json'
def create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers

def connect_to_endpoint(url, headers, params):
    response = requests.request("GET", search_url, headers=headers, params=params)
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()

def main():
    headers = create_headers(bearer_token)

    json_response = connect_to_endpoint(search_url, headers, query_params)
    print(json.dumps(json_response, indent=4, sort_keys=True))
    #json_response = expansions.flatten(json_response)
    
    with open(FILE, mode='a') as json_file:
        json_file.write(json.dumps(json_response))
        json_file.write("\n")  

    
    while json_response["meta"]["next_token"]:
        time.sleep(2)
        query_params["next_token"] = json_response["meta"]["next_token"]
        json_response = connect_to_endpoint(search_url, headers, query_params)
        #json_response = expansions.flatten(json_response)
        
        with open(FILE, mode='a') as json_file:
            json_file.write(json.dumps(json_response))
            json_file.write("\n")  

        if 'next_token' in json_response["meta"]:
            print(json_response["meta"]["next_token"])
        else:
            break

if __name__ == "__main__":
    main()


# In[203]:


FILE = 'bitcoin_1.json'
schema = {
  "properties": {
    "data": {
      "mergeStrategy": "append"
    },
    "includes": {
      "type": "object",
      "properties": {
        "users": {
          "mergeStrategy": "append"
        },
        "start_times": {
          "mergeStrategy": "append"
        },
        "end_times": {
          "mergeStrategy": "append"
        },
        "tweets": {
          "mergeStrategy": "append"
        },
        "places": {
          "mergeStrategy": "append"
        },
        "expansions": {
          "mergeStrategy": "append"
        }
      }
    }
  }
}

merger = Merger(schema)

dataset = {}

with open(FILE, "r") as f:
    for line in f.readlines():
        dataset = merger.merge(dataset, json.loads(line))

dataset = expansions.flatten(dataset)

with open("bitcoin_expanded1.json", "w") as f:
    f.write(json.dumps(dataset))

df4 = pd.json_normalize(dataset['data'])
df4.to_csv('df4.csv', encoding='utf-8', index=False)


# In[204]:


df4['geo.country_code']


# In[37]:


# topic 5 : Kobe


# In[38]:


query_params = {'query':'Kobe has:geo lang:en -is:retweet -is:reply',
                'start_time':'2020-01-01T00:00:00Z',
                'end_time':'2021-04-30T00:00:00Z',
                'max_results':'500',
                'expansions': 'geo.place_id,author_id',
                'tweet.fields': 'text,author_id,created_at,public_metrics,geo,lang',
                'user.fields': 'id,name,username,created_at,location,public_metrics',
                'place.fields': 'country_code,country,geo,id,place_type,name',
                'next_token': {}}
FILE = 'Kobe_1.json'
def create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers

def connect_to_endpoint(url, headers, params):
    response = requests.request("GET", search_url, headers=headers, params=params)
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()

def main():
    headers = create_headers(bearer_token)

    json_response = connect_to_endpoint(search_url, headers, query_params)
    print(json.dumps(json_response, indent=4, sort_keys=True))
    #json_response = expansions.flatten(json_response)
    
    with open(FILE, mode='a') as json_file:
        json_file.write(json.dumps(json_response))
        json_file.write("\n")  

    
    while json_response["meta"]["next_token"]:
        time.sleep(2)
        query_params["next_token"] = json_response["meta"]["next_token"]
        json_response = connect_to_endpoint(search_url, headers, query_params)
        #json_response = expansions.flatten(json_response)
        
        with open(FILE, mode='a') as json_file:
            json_file.write(json.dumps(json_response))
            json_file.write("\n")  

        if 'next_token' in json_response["meta"]:
            print(json_response["meta"]["next_token"])
        else:
            break

if __name__ == "__main__":
    main()


# In[205]:


FILE = 'Kobe_1.json'
schema = {
  "properties": {
    "data": {
      "mergeStrategy": "append"
    },
    "includes": {
      "type": "object",
      "properties": {
        "users": {
          "mergeStrategy": "append"
        },
        "start_times": {
          "mergeStrategy": "append"
        },
        "end_times": {
          "mergeStrategy": "append"
        },
        "tweets": {
          "mergeStrategy": "append"
        },
        "places": {
          "mergeStrategy": "append"
        },
        "expansions": {
          "mergeStrategy": "append"
        }
      }
    }
  }
}

merger = Merger(schema)

dataset = {}

with open(FILE, "r") as f:
    for line in f.readlines():
        dataset = merger.merge(dataset, json.loads(line))

dataset = expansions.flatten(dataset)

with open("Kobe_expanded1.json", "w") as f:
    f.write(json.dumps(dataset))

df5 = pd.json_normalize(dataset['data'])
df5.to_csv('df5.csv', encoding='utf-8', index=False)


# In[206]:


df5['geo.country_code']


# In[41]:


# topic 6 Joe Biden


# In[56]:


query_params = {'query':'Biden has:geo lang:en -is:retweet -is:reply',
                'start_time':'2020-01-01T00:00:00Z',
                'end_time':'2021-04-30T00:00:00Z',
                'max_results':'500',
                'expansions': 'geo.place_id,author_id',
                'tweet.fields': 'text,author_id,created_at,public_metrics,geo,lang',
                'user.fields': 'id,name,username,created_at,location,public_metrics',
                'place.fields': 'country_code,country,geo,id,place_type,name',
                'next_token': {}}
FILE = 'Biden_1.json'
def create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers

def connect_to_endpoint(url, headers, params):
    response = requests.request("GET", search_url, headers=headers, params=params)
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()

def main():
    headers = create_headers(bearer_token)

    json_response = connect_to_endpoint(search_url, headers, query_params)
    print(json.dumps(json_response, indent=4, sort_keys=True))
    #json_response = expansions.flatten(json_response)
    
    with open(FILE, mode='a') as json_file:
        json_file.write(json.dumps(json_response))
        json_file.write("\n")  

    
    while json_response["meta"]["next_token"]:
        time.sleep(2)
        query_params["next_token"] = json_response["meta"]["next_token"]
        json_response = connect_to_endpoint(search_url, headers, query_params)
        #json_response = expansions.flatten(json_response)
        
        with open(FILE, mode='a') as json_file:
            json_file.write(json.dumps(json_response))
            json_file.write("\n")  

        if 'next_token' in json_response["meta"]:
            print(json_response["meta"]["next_token"])
        else:
            break

if __name__ == "__main__":
    main()


# In[207]:


FILE = 'Biden_1.json'
schema = {
  "properties": {
    "data": {
      "mergeStrategy": "append"
    },
    "includes": {
      "type": "object",
      "properties": {
        "users": {
          "mergeStrategy": "append"
        },
        "start_times": {
          "mergeStrategy": "append"
        },
        "end_times": {
          "mergeStrategy": "append"
        },
        "tweets": {
          "mergeStrategy": "append"
        },
        "places": {
          "mergeStrategy": "append"
        },
        "expansions": {
          "mergeStrategy": "append"
        }
      }
    }
  }
}

merger = Merger(schema)

dataset = {}

with open(FILE, "r") as f:
    for line in f.readlines():
        dataset = merger.merge(dataset, json.loads(line))

dataset = expansions.flatten(dataset)

with open("Biden_expanded1.json", "w") as f:
    f.write(json.dumps(dataset))

df6 = pd.json_normalize(dataset['data'])
df6.to_csv('df6.csv', encoding='utf-8', index=False)


# In[208]:


df6['geo.country_code']


# In[59]:


# topic 7: Mars


# In[181]:


query_params = {'query':'Mars has:geo lang:en -is:retweet -is:reply',
                'start_time':'2020-01-01T00:00:00Z',
                'end_time':'2021-04-30T00:00:00Z',
                'max_results':'500',
                'expansions': 'geo.place_id,author_id',
                'tweet.fields': 'text,author_id,created_at,public_metrics,geo,lang',
                'user.fields': 'id,name,username,created_at,location,public_metrics',
                'place.fields': 'country_code,country,geo,id,place_type,name',
                'next_token': {}}
FILE = 'Mars_1.json'
def create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers

def connect_to_endpoint(url, headers, params):
    response = requests.request("GET", search_url, headers=headers, params=params)
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()

def main():
    headers = create_headers(bearer_token)

    json_response = connect_to_endpoint(search_url, headers, query_params)
    print(json.dumps(json_response, indent=4, sort_keys=True))
    #json_response = expansions.flatten(json_response)
    
    with open(FILE, mode='a') as json_file:
        json_file.write(json.dumps(json_response))
        json_file.write("\n")  

    
    while json_response["meta"]["next_token"]:
        time.sleep(2)
        query_params["next_token"] = json_response["meta"]["next_token"]
        json_response = connect_to_endpoint(search_url, headers, query_params)
        #json_response = expansions.flatten(json_response)
        
        with open(FILE, mode='a') as json_file:
            json_file.write(json.dumps(json_response))
            json_file.write("\n")  

        if 'next_token' in json_response["meta"]:
            print(json_response["meta"]["next_token"])
        else:
            break

if __name__ == "__main__":
    main()


# In[182]:


schema = {
  "properties": {
    "data": {
      "mergeStrategy": "append"
    },
    "includes": {
      "type": "object",
      "properties": {
        "users": {
          "mergeStrategy": "append"
        },
        "start_times": {
          "mergeStrategy": "append"
        },
        "end_times": {
          "mergeStrategy": "append"
        },
        "tweets": {
          "mergeStrategy": "append"
        },
        "places": {
          "mergeStrategy": "append"
        },
        "expansions": {
          "mergeStrategy": "append"
        }
      }
    }
  }
}

merger = Merger(schema)

dataset = {}

with open(FILE, "r") as f:
    for line in f.readlines():
        dataset = merger.merge(dataset, json.loads(line))

dataset = expansions.flatten(dataset)

with open("Mars_expended1.json", "w") as f:
    f.write(json.dumps(dataset))

df7 = pd.json_normalize(dataset['data'])
df7.to_csv('df7.csv', encoding='utf-8', index=False)


# In[184]:


df7['geo.country_code']
#df7.to_csv('df7.csv', encoding='utf-8', index=False)


# In[73]:


# topic 8: cat


# In[74]:


query_params = {'query':'cat has:geo lang:en -is:retweet -is:reply',
                'start_time':'2020-01-01T00:00:00Z',
                'end_time':'2021-04-30T00:00:00Z',
                'max_results':'500',
                'expansions': 'geo.place_id,author_id',
                'tweet.fields': 'text,author_id,created_at,public_metrics,geo,lang',
                'user.fields': 'id,name,username,created_at,location,public_metrics',
                'place.fields': 'country_code,country,geo,id,place_type,name',
                'next_token': {}}
FILE = 'cat_1.json'
def create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers

def connect_to_endpoint(url, headers, params):
    response = requests.request("GET", search_url, headers=headers, params=params)
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()

def main():
    headers = create_headers(bearer_token)

    json_response = connect_to_endpoint(search_url, headers, query_params)
    print(json.dumps(json_response, indent=4, sort_keys=True))
    #json_response = expansions.flatten(json_response)
    
    with open(FILE, mode='a') as json_file:
        json_file.write(json.dumps(json_response))
        json_file.write("\n")  

    
    while json_response["meta"]["next_token"]:
        time.sleep(2)
        query_params["next_token"] = json_response["meta"]["next_token"]
        json_response = connect_to_endpoint(search_url, headers, query_params)
        #json_response = expansions.flatten(json_response)
        
        with open(FILE, mode='a') as json_file:
            json_file.write(json.dumps(json_response))
            json_file.write("\n")  

        if 'next_token' in json_response["meta"]:
            print(json_response["meta"]["next_token"])
        else:
            break

if __name__ == "__main__":
    main()


# In[209]:


FILE = 'cat_1.json'
schema = {
  "properties": {
    "data": {
      "mergeStrategy": "append"
    },
    "includes": {
      "type": "object",
      "properties": {
        "users": {
          "mergeStrategy": "append"
        },
        "start_times": {
          "mergeStrategy": "append"
        },
        "end_times": {
          "mergeStrategy": "append"
        },
        "tweets": {
          "mergeStrategy": "append"
        },
        "places": {
          "mergeStrategy": "append"
        },
        "expansions": {
          "mergeStrategy": "append"
        }
      }
    }
  }
}

merger = Merger(schema)

dataset = {}

with open(FILE, "r") as f:
    for line in f.readlines():
        dataset = merger.merge(dataset, json.loads(line))

dataset = expansions.flatten(dataset)

with open("cat_expanded1.json", "w") as f:
    f.write(json.dumps(dataset))

df8 = pd.json_normalize(dataset['data'])
df8.to_csv('df8.csv', encoding='utf-8', index=False)


# In[210]:


df8['geo.country_code']


# In[77]:


# topic 9: PUBG


# In[78]:


query_params = {'query':'PUBG has:geo lang:en -is:retweet -is:reply',
                'start_time':'2020-01-01T00:00:00Z',
                'end_time':'2021-04-30T00:00:00Z',
                'max_results':'500',
                'expansions': 'geo.place_id,author_id',
                'tweet.fields': 'text,author_id,created_at,public_metrics,geo,lang',
                'user.fields': 'id,name,username,created_at,location,public_metrics',
                'place.fields': 'country_code,country,geo,id,place_type,name',
                'next_token': {}}
FILE = 'PUBG_1.json'
def create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers

def connect_to_endpoint(url, headers, params):
    response = requests.request("GET", search_url, headers=headers, params=params)
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()

def main():
    headers = create_headers(bearer_token)

    json_response = connect_to_endpoint(search_url, headers, query_params)
    print(json.dumps(json_response, indent=4, sort_keys=True))
    #json_response = expansions.flatten(json_response)
    
    with open(FILE, mode='a') as json_file:
        json_file.write(json.dumps(json_response))
        json_file.write("\n")  

    
    while json_response["meta"]["next_token"]:
        time.sleep(2)
        query_params["next_token"] = json_response["meta"]["next_token"]
        json_response = connect_to_endpoint(search_url, headers, query_params)
        #json_response = expansions.flatten(json_response)
        
        with open(FILE, mode='a') as json_file:
            json_file.write(json.dumps(json_response))
            json_file.write("\n")  

        if 'next_token' in json_response["meta"]:
            print(json_response["meta"]["next_token"])
        else:
            break

if __name__ == "__main__":
    main()


# In[211]:


FILE = 'PUBG_1.json'
schema = {
  "properties": {
    "data": {
      "mergeStrategy": "append"
    },
    "includes": {
      "type": "object",
      "properties": {
        "users": {
          "mergeStrategy": "append"
        },
        "start_times": {
          "mergeStrategy": "append"
        },
        "end_times": {
          "mergeStrategy": "append"
        },
        "tweets": {
          "mergeStrategy": "append"
        },
        "places": {
          "mergeStrategy": "append"
        },
        "expansions": {
          "mergeStrategy": "append"
        }
      }
    }
  }
}

merger = Merger(schema)

dataset = {}

with open(FILE, "r") as f:
    for line in f.readlines():
        dataset = merger.merge(dataset, json.loads(line))

dataset = expansions.flatten(dataset)

with open("PUBG_expanded1.json", "w") as f:
    f.write(json.dumps(dataset))

df9 = pd.json_normalize(dataset['data'])
df9.to_csv('df9.csv', encoding='utf-8', index=False)


# In[212]:


df9['geo.country_code']


# In[81]:


# topic 10 wallstreet


# In[244]:


# https://developer.twitter.com/en/docs/labs/recent-search/guides/search-queries
query_params = {'query':'wall street has:geo lang:en -is:retweet -is:reply',
                'start_time':'2020-01-01T00:00:00Z',
                'end_time':'2021-04-30T00:00:00Z',
                'max_results':'500',
                'expansions': 'geo.place_id,author_id',
                'tweet.fields': 'text,author_id,created_at,public_metrics,geo,lang',
                'user.fields': 'id,name,username,created_at,location,public_metrics',
                'place.fields': 'country_code,country,geo,id,place_type,name',
                'next_token': {}}
FILE = 'wallstreet_1.json'
def create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers

def connect_to_endpoint(url, headers, params):
    response = requests.request("GET", search_url, headers=headers, params=params)
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()

def main():
    headers = create_headers(bearer_token)

    json_response = connect_to_endpoint(search_url, headers, query_params)
    print(json.dumps(json_response, indent=4, sort_keys=True))
    #json_response = expansions.flatten(json_response)
    
    with open(FILE, mode='a') as json_file:
        json_file.write(json.dumps(json_response))
        json_file.write("\n")  

    
    while json_response["meta"]["next_token"]:
        time.sleep(2)
        query_params["next_token"] = json_response["meta"]["next_token"]
        json_response = connect_to_endpoint(search_url, headers, query_params)
        #json_response = expansions.flatten(json_response)
        
        with open(FILE, mode='a') as json_file:
            json_file.write(json.dumps(json_response))
            json_file.write("\n")  

        if 'next_token' in json_response["meta"]:
            print(json_response["meta"]["next_token"])
        else:
            break

if __name__ == "__main__":
    main()


# In[245]:


FILE = 'wallstreet_1.json'
schema = {
  "properties": {
    "data": {
      "mergeStrategy": "append"
    },
    "includes": {
      "type": "object",
      "properties": {
        "users": {
          "mergeStrategy": "append"
        },
        "start_times": {
          "mergeStrategy": "append"
        },
        "end_times": {
          "mergeStrategy": "append"
        },
        "tweets": {
          "mergeStrategy": "append"
        },
        "places": {
          "mergeStrategy": "append"
        },
        "expansions": {
          "mergeStrategy": "append"
        }
      }
    }
  }
}

merger = Merger(schema)

dataset = {}

with open(FILE, "r") as f:
    for line in f.readlines():
        dataset = merger.merge(dataset, json.loads(line))

dataset = expansions.flatten(dataset)

with open("wallstreet_expanded1.json", "w") as f:
    f.write(json.dumps(dataset))

df10= pd.json_normalize(dataset['data'])
df10.to_csv('df10.csv', encoding='utf-8', index=False)
df10['geo.country_code']


# In[125]:


# topic11: blacklivesmatter
query_params = {'query':'blacklivesmatter has:geo lang:en -is:retweet -is:reply',
                'start_time':'2020-01-01T00:00:00Z',
                'end_time':'2021-04-30T00:00:00Z',
                'max_results':'500',
                'expansions': 'geo.place_id,author_id',
                'tweet.fields': 'text,author_id,created_at,public_metrics,geo,lang',
                'user.fields': 'id,name,username,created_at,location,public_metrics',
                'place.fields': 'country_code,country,geo,id,place_type,name',
                'next_token': {}}
FILE = 'blacklivesmatter_1.json'
def create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers

def connect_to_endpoint(url, headers, params):
    response = requests.request("GET", search_url, headers=headers, params=params)
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()

def main():
    headers = create_headers(bearer_token)

    json_response = connect_to_endpoint(search_url, headers, query_params)
    print(json.dumps(json_response, indent=4, sort_keys=True))
    #json_response = expansions.flatten(json_response)
    
    with open(FILE, mode='a') as json_file:
        json_file.write(json.dumps(json_response))
        json_file.write("\n")  

    
    while json_response["meta"]["next_token"]:
        time.sleep(2)
        query_params["next_token"] = json_response["meta"]["next_token"]
        json_response = connect_to_endpoint(search_url, headers, query_params)
        #json_response = expansions.flatten(json_response)
        
        with open(FILE, mode='a') as json_file:
            json_file.write(json.dumps(json_response))
            json_file.write("\n")  

        if 'next_token' in json_response["meta"]:
            print(json_response["meta"]["next_token"])
        else:
            break

if __name__ == "__main__":
    main()


# In[126]:


schema = {
  "properties": {
    "data": {
      "mergeStrategy": "append"
    },
    "includes": {
      "type": "object",
      "properties": {
        "users": {
          "mergeStrategy": "append"
        },
        "start_times": {
          "mergeStrategy": "append"
        },
        "end_times": {
          "mergeStrategy": "append"
        },
        "tweets": {
          "mergeStrategy": "append"
        },
        "places": {
          "mergeStrategy": "append"
        },
        "expansions": {
          "mergeStrategy": "append"
        }
      }
    }
  }
}

merger = Merger(schema)

dataset = {}

with open(FILE, "r") as f:
    for line in f.readlines():
        dataset = merger.merge(dataset, json.loads(line))

dataset = expansions.flatten(dataset)

with open("blacklivesmatter_expanded1.json", "w") as f:
    f.write(json.dumps(dataset))

df11= pd.json_normalize(dataset['data'])
df11['geo.country_code']


# In[102]:


# topic12: iphone
query_params = {'query':'iphone has:geo lang:en -is:retweet -is:reply',
                'start_time':'2020-01-01T00:00:00Z',
                'end_time':'2021-04-30T00:00:00Z',
                'max_results':'500',
                'expansions': 'geo.place_id,author_id',
                'tweet.fields': 'text,author_id,created_at,public_metrics,geo,lang',
                'user.fields': 'id,name,username,created_at,location,public_metrics',
                'place.fields': 'country_code,country,geo,id,place_type,name',
                'next_token': {}}
FILE = 'iphone_1.json'
def create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers

def connect_to_endpoint(url, headers, params):
    response = requests.request("GET", search_url, headers=headers, params=params)
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()

def main():
    headers = create_headers(bearer_token)

    json_response = connect_to_endpoint(search_url, headers, query_params)
    print(json.dumps(json_response, indent=4, sort_keys=True))
    #json_response = expansions.flatten(json_response)
    
    with open(FILE, mode='a') as json_file:
        json_file.write(json.dumps(json_response))
        json_file.write("\n")  

    
    while json_response["meta"]["next_token"]:
        time.sleep(2)
        query_params["next_token"] = json_response["meta"]["next_token"]
        json_response = connect_to_endpoint(search_url, headers, query_params)
        #json_response = expansions.flatten(json_response)
        
        with open(FILE, mode='a') as json_file:
            json_file.write(json.dumps(json_response))
            json_file.write("\n")  

        if 'next_token' in json_response["meta"]:
            print(json_response["meta"]["next_token"])
        else:
            break

if __name__ == "__main__":
    main()


# In[103]:


schema = {
  "properties": {
    "data": {
      "mergeStrategy": "append"
    },
    "includes": {
      "type": "object",
      "properties": {
        "users": {
          "mergeStrategy": "append"
        },
        "start_times": {
          "mergeStrategy": "append"
        },
        "end_times": {
          "mergeStrategy": "append"
        },
        "tweets": {
          "mergeStrategy": "append"
        },
        "places": {
          "mergeStrategy": "append"
        },
        "expansions": {
          "mergeStrategy": "append"
        }
      }
    }
  }
}

merger = Merger(schema)

dataset = {}

with open(FILE, "r") as f:
    for line in f.readlines():
        dataset = merger.merge(dataset, json.loads(line))

dataset = expansions.flatten(dataset)

with open("iphone_expanded1.json", "w") as f:
    f.write(json.dumps(dataset))

df12= pd.json_normalize(dataset['data'])
df12['geo.country_code']


# In[106]:


# topic13: police
query_params = {'query':'police has:geo lang:en -is:retweet -is:reply',
                'start_time':'2020-01-01T00:00:00Z',
                'end_time':'2021-04-30T00:00:00Z',
                'max_results':'500',
                'expansions': 'geo.place_id,author_id',
                'tweet.fields': 'text,author_id,created_at,public_metrics,geo,lang',
                'user.fields': 'id,name,username,created_at,location,public_metrics',
                'place.fields': 'country_code,country,geo,id,place_type,name',
                'next_token': {}}
FILE = 'police_1.json'
def create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers

def connect_to_endpoint(url, headers, params):
    response = requests.request("GET", search_url, headers=headers, params=params)
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()

def main():
    headers = create_headers(bearer_token)

    json_response = connect_to_endpoint(search_url, headers, query_params)
    print(json.dumps(json_response, indent=4, sort_keys=True))
    #json_response = expansions.flatten(json_response)
    
    with open(FILE, mode='a') as json_file:
        json_file.write(json.dumps(json_response))
        json_file.write("\n")  

    
    while json_response["meta"]["next_token"]:
        time.sleep(2)
        query_params["next_token"] = json_response["meta"]["next_token"]
        json_response = connect_to_endpoint(search_url, headers, query_params)
        #json_response = expansions.flatten(json_response)
        
        with open(FILE, mode='a') as json_file:
            json_file.write(json.dumps(json_response))
            json_file.write("\n")  

        if 'next_token' in json_response["meta"]:
            print(json_response["meta"]["next_token"])
        else:
            break

if __name__ == "__main__":
    main()


# In[107]:


schema = {
  "properties": {
    "data": {
      "mergeStrategy": "append"
    },
    "includes": {
      "type": "object",
      "properties": {
        "users": {
          "mergeStrategy": "append"
        },
        "start_times": {
          "mergeStrategy": "append"
        },
        "end_times": {
          "mergeStrategy": "append"
        },
        "tweets": {
          "mergeStrategy": "append"
        },
        "places": {
          "mergeStrategy": "append"
        },
        "expansions": {
          "mergeStrategy": "append"
        }
      }
    }
  }
}

merger = Merger(schema)

dataset = {}

with open(FILE, "r") as f:
    for line in f.readlines():
        dataset = merger.merge(dataset, json.loads(line))

dataset = expansions.flatten(dataset)

with open("police_expanded1.json", "w") as f:
    f.write(json.dumps(dataset))

df13= pd.json_normalize(dataset['data'])
df13['geo.country_code']


# In[108]:


# topic14: soccer
query_params = {'query':'soccer has:geo lang:en -is:retweet -is:reply',
                'start_time':'2020-01-01T00:00:00Z',
                'end_time':'2021-04-30T00:00:00Z',
                'max_results':'500',
                'expansions': 'geo.place_id,author_id',
                'tweet.fields': 'text,author_id,created_at,public_metrics,geo,lang',
                'user.fields': 'id,name,username,created_at,location,public_metrics',
                'place.fields': 'country_code,country,geo,id,place_type,name',
                'next_token': {}}
FILE = 'soccer_1.json'
def create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers

def connect_to_endpoint(url, headers, params):
    response = requests.request("GET", search_url, headers=headers, params=params)
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()

def main():
    headers = create_headers(bearer_token)

    json_response = connect_to_endpoint(search_url, headers, query_params)
    print(json.dumps(json_response, indent=4, sort_keys=True))
    #json_response = expansions.flatten(json_response)
    
    with open(FILE, mode='a') as json_file:
        json_file.write(json.dumps(json_response))
        json_file.write("\n")  

    
    while json_response["meta"]["next_token"]:
        time.sleep(2)
        query_params["next_token"] = json_response["meta"]["next_token"]
        json_response = connect_to_endpoint(search_url, headers, query_params)
        #json_response = expansions.flatten(json_response)
        
        with open(FILE, mode='a') as json_file:
            json_file.write(json.dumps(json_response))
            json_file.write("\n")  

        if 'next_token' in json_response["meta"]:
            print(json_response["meta"]["next_token"])
        else:
            break

if __name__ == "__main__":
    main()


# In[109]:


schema = {
  "properties": {
    "data": {
      "mergeStrategy": "append"
    },
    "includes": {
      "type": "object",
      "properties": {
        "users": {
          "mergeStrategy": "append"
        },
        "start_times": {
          "mergeStrategy": "append"
        },
        "end_times": {
          "mergeStrategy": "append"
        },
        "tweets": {
          "mergeStrategy": "append"
        },
        "places": {
          "mergeStrategy": "append"
        },
        "expansions": {
          "mergeStrategy": "append"
        }
      }
    }
  }
}

merger = Merger(schema)

dataset = {}

with open(FILE, "r") as f:
    for line in f.readlines():
        dataset = merger.merge(dataset, json.loads(line))

dataset = expansions.flatten(dataset)

with open("soccer_expanded1.json", "w") as f:
    f.write(json.dumps(dataset))

df14= pd.json_normalize(dataset['data'])
df14['geo.country_code']


# In[110]:


# topic15: photography
query_params = {'query':'photography has:geo lang:en -is:retweet -is:reply',
                'start_time':'2020-01-01T00:00:00Z',
                'end_time':'2021-04-30T00:00:00Z',
                'max_results':'500',
                'expansions': 'geo.place_id,author_id',
                'tweet.fields': 'text,author_id,created_at,public_metrics,geo,lang',
                'user.fields': 'id,name,username,created_at,location,public_metrics',
                'place.fields': 'country_code,country,geo,id,place_type,name',
                'next_token': {}}
FILE = 'photography_1.json'
def create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers

def connect_to_endpoint(url, headers, params):
    response = requests.request("GET", search_url, headers=headers, params=params)
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()

def main():
    headers = create_headers(bearer_token)

    json_response = connect_to_endpoint(search_url, headers, query_params)
    print(json.dumps(json_response, indent=4, sort_keys=True))
    #json_response = expansions.flatten(json_response)
    
    with open(FILE, mode='a') as json_file:
        json_file.write(json.dumps(json_response))
        json_file.write("\n")  

    
    while json_response["meta"]["next_token"]:
        time.sleep(2)
        query_params["next_token"] = json_response["meta"]["next_token"]
        json_response = connect_to_endpoint(search_url, headers, query_params)
        #json_response = expansions.flatten(json_response)
        
        with open(FILE, mode='a') as json_file:
            json_file.write(json.dumps(json_response))
            json_file.write("\n")  

        if 'next_token' in json_response["meta"]:
            print(json_response["meta"]["next_token"])
        else:
            break

if __name__ == "__main__":
    main()


# In[111]:


schema = {
  "properties": {
    "data": {
      "mergeStrategy": "append"
    },
    "includes": {
      "type": "object",
      "properties": {
        "users": {
          "mergeStrategy": "append"
        },
        "start_times": {
          "mergeStrategy": "append"
        },
        "end_times": {
          "mergeStrategy": "append"
        },
        "tweets": {
          "mergeStrategy": "append"
        },
        "places": {
          "mergeStrategy": "append"
        },
        "expansions": {
          "mergeStrategy": "append"
        }
      }
    }
  }
}

merger = Merger(schema)

dataset = {}

with open(FILE, "r") as f:
    for line in f.readlines():
        dataset = merger.merge(dataset, json.loads(line))

dataset = expansions.flatten(dataset)

with open("photography_expanded1.json", "w") as f:
    f.write(json.dumps(dataset))

df15= pd.json_normalize(dataset['data'])
df15['geo.country_code']


# In[112]:


# topic16: music
query_params = {'query':'music has:geo lang:en -is:retweet -is:reply',
                'start_time':'2020-01-01T00:00:00Z',
                'end_time':'2021-04-30T00:00:00Z',
                'max_results':'500',
                'expansions': 'geo.place_id,author_id',
                'tweet.fields': 'text,author_id,created_at,public_metrics,geo,lang',
                'user.fields': 'id,name,username,created_at,location,public_metrics',
                'place.fields': 'country_code,country,geo,id,place_type,name',
                'next_token': {}}
FILE = 'music_1.json'
def create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers

def connect_to_endpoint(url, headers, params):
    response = requests.request("GET", search_url, headers=headers, params=params)
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()

def main():
    headers = create_headers(bearer_token)

    json_response = connect_to_endpoint(search_url, headers, query_params)
    print(json.dumps(json_response, indent=4, sort_keys=True))
    #json_response = expansions.flatten(json_response)
    
    with open(FILE, mode='a') as json_file:
        json_file.write(json.dumps(json_response))
        json_file.write("\n")  

    
    while json_response["meta"]["next_token"]:
        time.sleep(2)
        query_params["next_token"] = json_response["meta"]["next_token"]
        json_response = connect_to_endpoint(search_url, headers, query_params)
        #json_response = expansions.flatten(json_response)
        
        with open(FILE, mode='a') as json_file:
            json_file.write(json.dumps(json_response))
            json_file.write("\n")  

        if 'next_token' in json_response["meta"]:
            print(json_response["meta"]["next_token"])
        else:
            break

if __name__ == "__main__":
    main()


# In[113]:


schema = {
  "properties": {
    "data": {
      "mergeStrategy": "append"
    },
    "includes": {
      "type": "object",
      "properties": {
        "users": {
          "mergeStrategy": "append"
        },
        "start_times": {
          "mergeStrategy": "append"
        },
        "end_times": {
          "mergeStrategy": "append"
        },
        "tweets": {
          "mergeStrategy": "append"
        },
        "places": {
          "mergeStrategy": "append"
        },
        "expansions": {
          "mergeStrategy": "append"
        }
      }
    }
  }
}

merger = Merger(schema)

dataset = {}

with open(FILE, "r") as f:
    for line in f.readlines():
        dataset = merger.merge(dataset, json.loads(line))

dataset = expansions.flatten(dataset)

with open("music_expanded1.json", "w") as f:
    f.write(json.dumps(dataset))

df16= pd.json_normalize(dataset['data'])
df16['geo.country_code']


# In[114]:


# topic17: LGBT
query_params = {'query':'LGBT has:geo lang:en -is:retweet -is:reply',
                'start_time':'2020-01-01T00:00:00Z',
                'end_time':'2021-04-30T00:00:00Z',
                'max_results':'500',
                'expansions': 'geo.place_id,author_id',
                'tweet.fields': 'text,author_id,created_at,public_metrics,geo,lang',
                'user.fields': 'id,name,username,created_at,location,public_metrics',
                'place.fields': 'country_code,country,geo,id,place_type,name',
                'next_token': {}}
FILE = 'LGBT_1.json'
def create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers

def connect_to_endpoint(url, headers, params):
    response = requests.request("GET", search_url, headers=headers, params=params)
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()

def main():
    headers = create_headers(bearer_token)

    json_response = connect_to_endpoint(search_url, headers, query_params)
    print(json.dumps(json_response, indent=4, sort_keys=True))
    #json_response = expansions.flatten(json_response)
    
    with open(FILE, mode='a') as json_file:
        json_file.write(json.dumps(json_response))
        json_file.write("\n")  

    
    while json_response["meta"]["next_token"]:
        time.sleep(2)
        query_params["next_token"] = json_response["meta"]["next_token"]
        json_response = connect_to_endpoint(search_url, headers, query_params)
        #json_response = expansions.flatten(json_response)
        
        with open(FILE, mode='a') as json_file:
            json_file.write(json.dumps(json_response))
            json_file.write("\n")  

        if 'next_token' in json_response["meta"]:
            print(json_response["meta"]["next_token"])
        else:
            break

if __name__ == "__main__":
    main()


# In[115]:


schema = {
  "properties": {
    "data": {
      "mergeStrategy": "append"
    },
    "includes": {
      "type": "object",
      "properties": {
        "users": {
          "mergeStrategy": "append"
        },
        "start_times": {
          "mergeStrategy": "append"
        },
        "end_times": {
          "mergeStrategy": "append"
        },
        "tweets": {
          "mergeStrategy": "append"
        },
        "places": {
          "mergeStrategy": "append"
        },
        "expansions": {
          "mergeStrategy": "append"
        }
      }
    }
  }
}

merger = Merger(schema)

dataset = {}

with open(FILE, "r") as f:
    for line in f.readlines():
        dataset = merger.merge(dataset, json.loads(line))

dataset = expansions.flatten(dataset)

with open("LGBT_expanded1.json", "w") as f:
    f.write(json.dumps(dataset))

df17= pd.json_normalize(dataset['data'])
df17['geo.country_code']


# In[118]:


# topic18: Tiktok
query_params = {'query':'tiktok has:geo lang:en -is:retweet -is:reply',
                'start_time':'2020-01-01T00:00:00Z',
                'end_time':'2021-04-30T00:00:00Z',
                'max_results':'500',
                'expansions': 'geo.place_id,author_id',
                'tweet.fields': 'text,author_id,created_at,public_metrics,geo,lang',
                'user.fields': 'id,name,username,created_at,location,public_metrics',
                'place.fields': 'country_code,country,geo,id,place_type,name',
                'next_token': {}}
FILE = 'tiktok_1.json'
def create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers

def connect_to_endpoint(url, headers, params):
    response = requests.request("GET", search_url, headers=headers, params=params)
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()

def main():
    headers = create_headers(bearer_token)

    json_response = connect_to_endpoint(search_url, headers, query_params)
    print(json.dumps(json_response, indent=4, sort_keys=True))
    #json_response = expansions.flatten(json_response)
    
    with open(FILE, mode='a') as json_file:
        json_file.write(json.dumps(json_response))
        json_file.write("\n")  

    
    while json_response["meta"]["next_token"]:
        time.sleep(2)
        query_params["next_token"] = json_response["meta"]["next_token"]
        json_response = connect_to_endpoint(search_url, headers, query_params)
        #json_response = expansions.flatten(json_response)
        
        with open(FILE, mode='a') as json_file:
            json_file.write(json.dumps(json_response))
            json_file.write("\n")  

        if 'next_token' in json_response["meta"]:
            print(json_response["meta"]["next_token"])
        else:
            break

if __name__ == "__main__":
    main()


# In[119]:


schema = {
  "properties": {
    "data": {
      "mergeStrategy": "append"
    },
    "includes": {
      "type": "object",
      "properties": {
        "users": {
          "mergeStrategy": "append"
        },
        "start_times": {
          "mergeStrategy": "append"
        },
        "end_times": {
          "mergeStrategy": "append"
        },
        "tweets": {
          "mergeStrategy": "append"
        },
        "places": {
          "mergeStrategy": "append"
        },
        "expansions": {
          "mergeStrategy": "append"
        }
      }
    }
  }
}

merger = Merger(schema)

dataset = {}

with open(FILE, "r") as f:
    for line in f.readlines():
        dataset = merger.merge(dataset, json.loads(line))

dataset = expansions.flatten(dataset)

with open("tiktok_expanded1.json", "w") as f:
    f.write(json.dumps(dataset))

df18= pd.json_normalize(dataset['data'])
df18['geo.country_code']


# In[120]:


# topic19: animation
query_params = {'query':'animation has:geo lang:en -is:retweet -is:reply',
                'start_time':'2020-01-01T00:00:00Z',
                'end_time':'2021-04-30T00:00:00Z',
                'max_results':'500',
                'expansions': 'geo.place_id,author_id',
                'tweet.fields': 'text,author_id,created_at,public_metrics,geo,lang',
                'user.fields': 'id,name,username,created_at,location,public_metrics',
                'place.fields': 'country_code,country,geo,id,place_type,name',
                'next_token': {}}
FILE = 'animation_1.json'
def create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers

def connect_to_endpoint(url, headers, params):
    response = requests.request("GET", search_url, headers=headers, params=params)
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()

def main():
    headers = create_headers(bearer_token)

    json_response = connect_to_endpoint(search_url, headers, query_params)
    print(json.dumps(json_response, indent=4, sort_keys=True))
    #json_response = expansions.flatten(json_response)
    
    with open(FILE, mode='a') as json_file:
        json_file.write(json.dumps(json_response))
        json_file.write("\n")  

    
    while json_response["meta"]["next_token"]:
        time.sleep(2)
        query_params["next_token"] = json_response["meta"]["next_token"]
        json_response = connect_to_endpoint(search_url, headers, query_params)
        #json_response = expansions.flatten(json_response)
        
        with open(FILE, mode='a') as json_file:
            json_file.write(json.dumps(json_response))
            json_file.write("\n")  

        if 'next_token' in json_response["meta"]:
            print(json_response["meta"]["next_token"])
        else:
            break

if __name__ == "__main__":
    main()


# In[121]:


schema = {
  "properties": {
    "data": {
      "mergeStrategy": "append"
    },
    "includes": {
      "type": "object",
      "properties": {
        "users": {
          "mergeStrategy": "append"
        },
        "start_times": {
          "mergeStrategy": "append"
        },
        "end_times": {
          "mergeStrategy": "append"
        },
        "tweets": {
          "mergeStrategy": "append"
        },
        "places": {
          "mergeStrategy": "append"
        },
        "expansions": {
          "mergeStrategy": "append"
        }
      }
    }
  }
}

merger = Merger(schema)

dataset = {}

with open(FILE, "r") as f:
    for line in f.readlines():
        dataset = merger.merge(dataset, json.loads(line))

dataset = expansions.flatten(dataset)

with open("animation_expanded1.json", "w") as f:
    f.write(json.dumps(dataset))

df19= pd.json_normalize(dataset['data'])
df19['geo.country_code']


# In[122]:


# topic20: weather
query_params = {'query':'weather has:geo lang:en -is:retweet -is:reply',
                'start_time':'2020-01-01T00:00:00Z',
                'end_time':'2021-04-30T00:00:00Z',
                'max_results':'500',
                'expansions': 'geo.place_id,author_id',
                'tweet.fields': 'text,author_id,created_at,public_metrics,geo,lang',
                'user.fields': 'id,name,username,created_at,location,public_metrics',
                'place.fields': 'country_code,country,geo,id,place_type,name',
                'next_token': {}}
FILE = 'weather_1.json'
def create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers

def connect_to_endpoint(url, headers, params):
    response = requests.request("GET", search_url, headers=headers, params=params)
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()

def main():
    headers = create_headers(bearer_token)

    json_response = connect_to_endpoint(search_url, headers, query_params)
    print(json.dumps(json_response, indent=4, sort_keys=True))
    #json_response = expansions.flatten(json_response)
    
    with open(FILE, mode='a') as json_file:
        json_file.write(json.dumps(json_response))
        json_file.write("\n")  

    
    while json_response["meta"]["next_token"]:
        time.sleep(2)
        query_params["next_token"] = json_response["meta"]["next_token"]
        json_response = connect_to_endpoint(search_url, headers, query_params)
        #json_response = expansions.flatten(json_response)
        
        with open(FILE, mode='a') as json_file:
            json_file.write(json.dumps(json_response))
            json_file.write("\n")  

        if 'next_token' in json_response["meta"]:
            print(json_response["meta"]["next_token"])
        else:
            break

if __name__ == "__main__":
    main()


# In[123]:


schema = {
  "properties": {
    "data": {
      "mergeStrategy": "append"
    },
    "includes": {
      "type": "object",
      "properties": {
        "users": {
          "mergeStrategy": "append"
        },
        "start_times": {
          "mergeStrategy": "append"
        },
        "end_times": {
          "mergeStrategy": "append"
        },
        "tweets": {
          "mergeStrategy": "append"
        },
        "places": {
          "mergeStrategy": "append"
        },
        "expansions": {
          "mergeStrategy": "append"
        }
      }
    }
  }
}

merger = Merger(schema)

dataset = {}

with open(FILE, "r") as f:
    for line in f.readlines():
        dataset = merger.merge(dataset, json.loads(line))

dataset = expansions.flatten(dataset)

with open("weather_expanded1.json", "w") as f:
    f.write(json.dumps(dataset))

df20= pd.json_normalize(dataset['data'])
df20['geo.country_code']


# In[85]:


# merge files into one file


# In[214]:


df1.columns


# In[255]:


df2= df2[['created_at', 'lang', 'text', 'id', 'author_id', 'geo.place_id',
       'geo.name', 'geo.country_code', 'geo.country', 'geo.id',
       'geo.place_type', 'geo.geo.type', 'geo.geo.bbox', 'geo.full_name',
       'public_metrics.retweet_count', 'public_metrics.reply_count',
       'public_metrics.like_count', 'public_metrics.quote_count',
       'author.created_at', 'author.username',
       'author.public_metrics.followers_count',
       'author.public_metrics.following_count',
       'author.public_metrics.tweet_count',
       'author.public_metrics.listed_count', 'author.id', 'author.name',
       'author.location', 'geo.coordinates.type',
       'geo.coordinates.coordinates']]
df2.to_csv('df2.csv', encoding='utf-8', index=False)


# In[254]:


df3= df3[['created_at', 'lang', 'text', 'id', 'author_id', 'geo.place_id',
       'geo.name', 'geo.country_code', 'geo.country', 'geo.id',
       'geo.place_type', 'geo.geo.type', 'geo.geo.bbox', 'geo.full_name',
       'public_metrics.retweet_count', 'public_metrics.reply_count',
       'public_metrics.like_count', 'public_metrics.quote_count',
       'author.created_at', 'author.username',
       'author.public_metrics.followers_count',
       'author.public_metrics.following_count',
       'author.public_metrics.tweet_count',
       'author.public_metrics.listed_count', 'author.id', 'author.name',
       'author.location', 'geo.coordinates.type',
       'geo.coordinates.coordinates']]
df3.to_csv('df3.csv', encoding='utf-8', index=False)


# In[253]:


df4= df4[['created_at', 'lang', 'text', 'id', 'author_id', 'geo.place_id',
       'geo.name', 'geo.country_code', 'geo.country', 'geo.id',
       'geo.place_type', 'geo.geo.type', 'geo.geo.bbox', 'geo.full_name',
       'public_metrics.retweet_count', 'public_metrics.reply_count',
       'public_metrics.like_count', 'public_metrics.quote_count',
       'author.created_at', 'author.username',
       'author.public_metrics.followers_count',
       'author.public_metrics.following_count',
       'author.public_metrics.tweet_count',
       'author.public_metrics.listed_count', 'author.id', 'author.name',
       'author.location', 'geo.coordinates.type',
       'geo.coordinates.coordinates']]
df4.to_csv('df4.csv', encoding='utf-8', index=False)


# In[252]:


df5= df5[['created_at', 'lang', 'text', 'id', 'author_id', 'geo.place_id',
       'geo.name', 'geo.country_code', 'geo.country', 'geo.id',
       'geo.place_type', 'geo.geo.type', 'geo.geo.bbox', 'geo.full_name',
       'public_metrics.retweet_count', 'public_metrics.reply_count',
       'public_metrics.like_count', 'public_metrics.quote_count',
       'author.created_at', 'author.username',
       'author.public_metrics.followers_count',
       'author.public_metrics.following_count',
       'author.public_metrics.tweet_count',
       'author.public_metrics.listed_count', 'author.id', 'author.name',
       'author.location', 'geo.coordinates.type',
       'geo.coordinates.coordinates']]
df5.to_csv('df5.csv', encoding='utf-8', index=False)


# In[251]:


df6= df6[['created_at', 'lang', 'text', 'id', 'author_id', 'geo.place_id',
       'geo.name', 'geo.country_code', 'geo.country', 'geo.id',
       'geo.place_type', 'geo.geo.type', 'geo.geo.bbox', 'geo.full_name',
       'public_metrics.retweet_count', 'public_metrics.reply_count',
       'public_metrics.like_count', 'public_metrics.quote_count',
       'author.created_at', 'author.username',
       'author.public_metrics.followers_count',
       'author.public_metrics.following_count',
       'author.public_metrics.tweet_count',
       'author.public_metrics.listed_count', 'author.id', 'author.name',
       'author.location', 'geo.coordinates.type',
       'geo.coordinates.coordinates']]
df6.to_csv('df6.csv', encoding='utf-8', index=False)


# In[250]:


df7= df7[['created_at', 'lang', 'text', 'id', 'author_id', 'geo.place_id',
       'geo.name', 'geo.country_code', 'geo.country', 'geo.id',
       'geo.place_type', 'geo.geo.type', 'geo.geo.bbox', 'geo.full_name',
       'public_metrics.retweet_count', 'public_metrics.reply_count',
       'public_metrics.like_count', 'public_metrics.quote_count',
       'author.created_at', 'author.username',
       'author.public_metrics.followers_count',
       'author.public_metrics.following_count',
       'author.public_metrics.tweet_count',
       'author.public_metrics.listed_count', 'author.id', 'author.name',
       'author.location', 'geo.coordinates.type',
       'geo.coordinates.coordinates']]
df7.to_csv('df7.csv', encoding='utf-8', index=False)


# In[249]:


df8= df8[['created_at', 'lang', 'text', 'id', 'author_id', 'geo.place_id',
       'geo.name', 'geo.country_code', 'geo.country', 'geo.id',
       'geo.place_type', 'geo.geo.type', 'geo.geo.bbox', 'geo.full_name',
       'public_metrics.retweet_count', 'public_metrics.reply_count',
       'public_metrics.like_count', 'public_metrics.quote_count',
       'author.created_at', 'author.username',
       'author.public_metrics.followers_count',
       'author.public_metrics.following_count',
       'author.public_metrics.tweet_count',
       'author.public_metrics.listed_count', 'author.id', 'author.name',
       'author.location', 'geo.coordinates.type',
       'geo.coordinates.coordinates']]
df8.to_csv('df8.csv', encoding='utf-8', index=False)


# In[248]:


df9= df9[['created_at', 'lang', 'text', 'id', 'author_id', 'geo.place_id',
       'geo.name', 'geo.country_code', 'geo.country', 'geo.id',
       'geo.place_type', 'geo.geo.type', 'geo.geo.bbox', 'geo.full_name',
       'public_metrics.retweet_count', 'public_metrics.reply_count',
       'public_metrics.like_count', 'public_metrics.quote_count',
       'author.created_at', 'author.username',
       'author.public_metrics.followers_count',
       'author.public_metrics.following_count',
       'author.public_metrics.tweet_count',
       'author.public_metrics.listed_count', 'author.id', 'author.name',
       'author.location', 'geo.coordinates.type',
       'geo.coordinates.coordinates']]
df9.to_csv('df9.csv', encoding='utf-8', index=False)


# In[247]:


df10= df10[['created_at', 'lang', 'text', 'id', 'author_id', 'geo.place_id',
       'geo.name', 'geo.country_code', 'geo.country', 'geo.id',
       'geo.place_type', 'geo.geo.type', 'geo.geo.bbox', 'geo.full_name',
       'public_metrics.retweet_count', 'public_metrics.reply_count',
       'public_metrics.like_count', 'public_metrics.quote_count',
       'author.created_at', 'author.username',
       'author.public_metrics.followers_count',
       'author.public_metrics.following_count',
       'author.public_metrics.tweet_count',
       'author.public_metrics.listed_count', 'author.id', 'author.name',
       'author.location', 'geo.coordinates.type',
       'geo.coordinates.coordinates']]
df10.to_csv('df10.csv', encoding='utf-8', index=False)


# In[127]:


df11= df11[['created_at', 'lang', 'text', 'id', 'author_id', 'geo.place_id',
       'geo.name', 'geo.country_code', 'geo.country', 'geo.id',
       'geo.place_type', 'geo.geo.type', 'geo.geo.bbox', 'geo.full_name',
       'public_metrics.retweet_count', 'public_metrics.reply_count',
       'public_metrics.like_count', 'public_metrics.quote_count',
       'author.created_at', 'author.username',
       'author.public_metrics.followers_count',
       'author.public_metrics.following_count',
       'author.public_metrics.tweet_count',
       'author.public_metrics.listed_count', 'author.id', 'author.name',
       'author.location', 'geo.coordinates.type',
       'geo.coordinates.coordinates']]
df11.to_csv('df11.csv', encoding='utf-8', index=False)


# In[128]:


df12= df12[['created_at', 'lang', 'text', 'id', 'author_id', 'geo.place_id',
       'geo.name', 'geo.country_code', 'geo.country', 'geo.id',
       'geo.place_type', 'geo.geo.type', 'geo.geo.bbox', 'geo.full_name',
       'public_metrics.retweet_count', 'public_metrics.reply_count',
       'public_metrics.like_count', 'public_metrics.quote_count',
       'author.created_at', 'author.username',
       'author.public_metrics.followers_count',
       'author.public_metrics.following_count',
       'author.public_metrics.tweet_count',
       'author.public_metrics.listed_count', 'author.id', 'author.name',
       'author.location', 'geo.coordinates.type',
       'geo.coordinates.coordinates']]
df12.to_csv('df12.csv', encoding='utf-8', index=False)
df13= df13[['created_at', 'lang', 'text', 'id', 'author_id', 'geo.place_id',
       'geo.name', 'geo.country_code', 'geo.country', 'geo.id',
       'geo.place_type', 'geo.geo.type', 'geo.geo.bbox', 'geo.full_name',
       'public_metrics.retweet_count', 'public_metrics.reply_count',
       'public_metrics.like_count', 'public_metrics.quote_count',
       'author.created_at', 'author.username',
       'author.public_metrics.followers_count',
       'author.public_metrics.following_count',
       'author.public_metrics.tweet_count',
       'author.public_metrics.listed_count', 'author.id', 'author.name',
       'author.location', 'geo.coordinates.type',
       'geo.coordinates.coordinates']]
df13.to_csv('df13.csv', encoding='utf-8', index=False)
df14= df14[['created_at', 'lang', 'text', 'id', 'author_id', 'geo.place_id',
       'geo.name', 'geo.country_code', 'geo.country', 'geo.id',
       'geo.place_type', 'geo.geo.type', 'geo.geo.bbox', 'geo.full_name',
       'public_metrics.retweet_count', 'public_metrics.reply_count',
       'public_metrics.like_count', 'public_metrics.quote_count',
       'author.created_at', 'author.username',
       'author.public_metrics.followers_count',
       'author.public_metrics.following_count',
       'author.public_metrics.tweet_count',
       'author.public_metrics.listed_count', 'author.id', 'author.name',
       'author.location', 'geo.coordinates.type',
       'geo.coordinates.coordinates']]
df14.to_csv('df14.csv', encoding='utf-8', index=False)
df15= df15[['created_at', 'lang', 'text', 'id', 'author_id', 'geo.place_id',
       'geo.name', 'geo.country_code', 'geo.country', 'geo.id',
       'geo.place_type', 'geo.geo.type', 'geo.geo.bbox', 'geo.full_name',
       'public_metrics.retweet_count', 'public_metrics.reply_count',
       'public_metrics.like_count', 'public_metrics.quote_count',
       'author.created_at', 'author.username',
       'author.public_metrics.followers_count',
       'author.public_metrics.following_count',
       'author.public_metrics.tweet_count',
       'author.public_metrics.listed_count', 'author.id', 'author.name',
       'author.location', 'geo.coordinates.type',
       'geo.coordinates.coordinates']]
df15.to_csv('df15.csv', encoding='utf-8', index=False)
df16= df16[['created_at', 'lang', 'text', 'id', 'author_id', 'geo.place_id',
       'geo.name', 'geo.country_code', 'geo.country', 'geo.id',
       'geo.place_type', 'geo.geo.type', 'geo.geo.bbox', 'geo.full_name',
       'public_metrics.retweet_count', 'public_metrics.reply_count',
       'public_metrics.like_count', 'public_metrics.quote_count',
       'author.created_at', 'author.username',
       'author.public_metrics.followers_count',
       'author.public_metrics.following_count',
       'author.public_metrics.tweet_count',
       'author.public_metrics.listed_count', 'author.id', 'author.name',
       'author.location', 'geo.coordinates.type',
       'geo.coordinates.coordinates']]
df16.to_csv('df16.csv', encoding='utf-8', index=False)
df17= df17[['created_at', 'lang', 'text', 'id', 'author_id', 'geo.place_id',
       'geo.name', 'geo.country_code', 'geo.country', 'geo.id',
       'geo.place_type', 'geo.geo.type', 'geo.geo.bbox', 'geo.full_name',
       'public_metrics.retweet_count', 'public_metrics.reply_count',
       'public_metrics.like_count', 'public_metrics.quote_count',
       'author.created_at', 'author.username',
       'author.public_metrics.followers_count',
       'author.public_metrics.following_count',
       'author.public_metrics.tweet_count',
       'author.public_metrics.listed_count', 'author.id', 'author.name',
       'author.location', 'geo.coordinates.type',
       'geo.coordinates.coordinates']]
df17.to_csv('df17.csv', encoding='utf-8', index=False)
df18= df18[['created_at', 'lang', 'text', 'id', 'author_id', 'geo.place_id',
       'geo.name', 'geo.country_code', 'geo.country', 'geo.id',
       'geo.place_type', 'geo.geo.type', 'geo.geo.bbox', 'geo.full_name',
       'public_metrics.retweet_count', 'public_metrics.reply_count',
       'public_metrics.like_count', 'public_metrics.quote_count',
       'author.created_at', 'author.username',
       'author.public_metrics.followers_count',
       'author.public_metrics.following_count',
       'author.public_metrics.tweet_count',
       'author.public_metrics.listed_count', 'author.id', 'author.name',
       'author.location', 'geo.coordinates.type',
       'geo.coordinates.coordinates']]
df18.to_csv('df18.csv', encoding='utf-8', index=False)
df19= df19[['created_at', 'lang', 'text', 'id', 'author_id', 'geo.place_id',
       'geo.name', 'geo.country_code', 'geo.country', 'geo.id',
       'geo.place_type', 'geo.geo.type', 'geo.geo.bbox', 'geo.full_name',
       'public_metrics.retweet_count', 'public_metrics.reply_count',
       'public_metrics.like_count', 'public_metrics.quote_count',
       'author.created_at', 'author.username',
       'author.public_metrics.followers_count',
       'author.public_metrics.following_count',
       'author.public_metrics.tweet_count',
       'author.public_metrics.listed_count', 'author.id', 'author.name',
       'author.location', 'geo.coordinates.type',
       'geo.coordinates.coordinates']]
df19.to_csv('df19.csv', encoding='utf-8', index=False)
df20= df20[['created_at', 'lang', 'text', 'id', 'author_id', 'geo.place_id',
       'geo.name', 'geo.country_code', 'geo.country', 'geo.id',
       'geo.place_type', 'geo.geo.type', 'geo.geo.bbox', 'geo.full_name',
       'public_metrics.retweet_count', 'public_metrics.reply_count',
       'public_metrics.like_count', 'public_metrics.quote_count',
       'author.created_at', 'author.username',
       'author.public_metrics.followers_count',
       'author.public_metrics.following_count',
       'author.public_metrics.tweet_count',
       'author.public_metrics.listed_count', 'author.id', 'author.name',
       'author.location', 'geo.coordinates.type',
       'geo.coordinates.coordinates']]
df20.to_csv('df20.csv', encoding='utf-8', index=False)


# In[187]:


df1 = pd.read_csv('df1.csv',dtype="a", encoding='utf-8')
df2 = pd.read_csv('df2.csv',dtype="a", encoding='utf-8')
df3 = pd.read_csv('df3.csv',dtype="a", encoding='utf-8')
df4 = pd.read_csv('df4.csv',dtype="a", encoding='utf-8')
df5 = pd.read_csv('df5.csv',dtype="a", encoding='utf-8')
df6 = pd.read_csv('df6.csv',dtype="a", encoding='utf-8')
df7 = pd.read_csv('df7.csv',dtype="a", encoding='utf-8')
df8 = pd.read_csv('df8.csv',dtype="a", encoding='utf-8')
df9 = pd.read_csv('df9.csv',dtype="a", encoding='utf-8')
df10 = pd.read_csv('df10.csv',dtype="a", encoding='utf-8')


# In[284]:


frames = [df1,df2,df3,df4,df5,df6,df7,df8,df9,df10,df11,df12,df13,df14,df15,df16,df17,df18,df19,df20]
Total_data = pd.concat(frames)


# In[257]:


Total_data.head(1)


# In[258]:


Total_data.tail(1)


# In[259]:


Total_data.to_csv('Total_data.csv', encoding='utf-8', index=False)


# In[135]:


## combine mydata2 to mydata

#file1 = open("mydata.csv", "a")
#file2 = open("mydata2.csv", "r")

#for line in file2:
#   file1.write(line)

#file1.close()
#file2.close()


# In[289]:


mydata=Total_data


# In[290]:


# length of "mydata"
index = mydata.index
number_of_rows = len(index)
print(number_of_rows)


# In[263]:


mydata['geo.country_code']


# In[ ]:


######## =============== remove geo NAN cells ================ ############


# In[234]:


nan_value = float("NaN")


# In[264]:


print(len(df1.index))
df1.replace("", nan_value, inplace=True)
df1.dropna(subset = ["geo.geo.bbox"], inplace=True)
print(len(df1.index))


# In[265]:


print(len(df2.index))
df2.replace("", nan_value, inplace=True)
df2.dropna(subset = ["geo.geo.bbox"], inplace=True)
print(len(df2.index))

print(len(df3.index))
df3.replace("", nan_value, inplace=True)
df3.dropna(subset = ["geo.geo.bbox"], inplace=True)
print(len(df3.index))

print(len(df4.index))
df4.replace("", nan_value, inplace=True)
df4.dropna(subset = ["geo.geo.bbox"], inplace=True)
print(len(df4.index))

print(len(df5.index))
df5.replace("", nan_value, inplace=True)
df5.dropna(subset = ["geo.geo.bbox"], inplace=True)
print(len(df5.index))

print(len(df6.index))
df6.replace("", nan_value, inplace=True)
df6.dropna(subset = ["geo.geo.bbox"], inplace=True)
print(len(df6.index))

print(len(df7.index))
df7.replace("", nan_value, inplace=True)
df7.dropna(subset = ["geo.geo.bbox"], inplace=True)
print(len(df7.index))

print(len(df8.index))
df8.replace("", nan_value, inplace=True)
df8.dropna(subset = ["geo.geo.bbox"], inplace=True)
print(len(df8.index))

print(len(df9.index))
df9.replace("", nan_value, inplace=True)
df9.dropna(subset = ["geo.geo.bbox"], inplace=True)
print(len(df9.index))

print(len(df10.index))
df10.replace("", nan_value, inplace=True)
df10.dropna(subset = ["geo.geo.bbox"], inplace=True)
print(len(df10.index))


# In[266]:


# remove 
df11.replace("", nan_value, inplace=True)
df11.dropna(subset = ["geo.geo.bbox"], inplace=True)
print(len(df11.index))


# In[267]:


print(len(df12.index))
nan_value = float("NaN")
df12.replace("", nan_value, inplace=True)
df12.dropna(subset = ["geo.geo.bbox"], inplace=True)
print(len(df12.index))


# In[269]:


print(len(df13.index))
df13.replace("", nan_value, inplace=True)
df13.dropna(subset = ["geo.geo.bbox"], inplace=True)
print(len(df13.index))


# In[270]:


print(len(df14.index))
df14.replace("", nan_value, inplace=True)
df14.dropna(subset = ["geo.geo.bbox"], inplace=True)
print(len(df14.index))


# In[271]:


print(len(df15.index))
df15.replace("", nan_value, inplace=True)
df15.dropna(subset = ["geo.geo.bbox"], inplace=True)
print(len(df15.index))


# In[272]:


print(len(df16.index))
df16.replace("", nan_value, inplace=True)
df16.dropna(subset = ["geo.geo.bbox"], inplace=True)
print(len(df16.index))


# In[273]:


print(len(df17.index))
df17.replace("", nan_value, inplace=True)
df17.dropna(subset = ["geo.geo.bbox"], inplace=True)
print(len(df17.index))


# In[274]:


print(len(df18.index))
nan_value = float("NaN")
df18.replace("", nan_value, inplace=True)
df18.dropna(subset = ["geo.geo.bbox"], inplace=True)
print(len(df18.index))


# In[275]:


print(len(df19.index))
nan_value = float("NaN")
df19.replace("", nan_value, inplace=True)
df19.dropna(subset = ["geo.geo.bbox"], inplace=True)
print(len(df19.index))


# In[276]:


print(len(df20.index))
nan_value = float("NaN")
df20.replace("", nan_value, inplace=True)
df20.dropna(subset = ["geo.geo.bbox"], inplace=True)
print(len(df20.index))


# In[277]:


print(len(df1.index), len(df2.index),len(df3.index),len(df4.index), len(df5.index),len(df6.index),
     len(df7.index), len(df8.index),len(df9.index),len(df10.index), len(df11.index),len(df12.index),len(df13.index),
     len(df14.index), len(df15.index),len(df16.index),len(df17.index), len(df18.index),len(df19.index),len(df20.index))


# In[291]:


print(len(mydata.index))
nan_value = float("NaN")
mydata.replace("", nan_value, inplace=True)
mydata.dropna(subset = ["geo.geo.bbox"], inplace=True)
print(len(mydata.index))


# In[278]:


num_list = [23109, 11844, 14183, 10744, 26824, 15270, 16227, 49447, 17813, 9267,
            39544, 12748, 14785, 20371, 14772, 16586, 10283, 57348, 23732, 12358]
res = sum(num_list)
print("sum is: ", res)


# In[280]:


## data collection done! 


# In[292]:


mydata.to_csv('mydata.csv', encoding='utf-8', index=False)


# In[293]:


print(len(mydata.index))


# In[ ]:




