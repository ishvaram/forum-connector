# import urlparse

# def get_id(url):


#   """Extract an integer id from  `url`.

#   Raise ValueError for invalid strings
#   """
#   parts = urlparse.urlsplit(url) 
#   if parts.hostname == 'www.trade2win.com':
#     idstr = parts.path.rpartition('/')[5]
#     if idstr.startswith('index'):
#       try: return int(idstr[5:])
#       except ValueError: pass
#   raise ValueError("Invalid url: %r" % (url,))

# print get_id("http://www.trade2win.com/boards/forex/index256.html")    



import re

def get_id(toParse):
    return re.search('index(\d+)', toParse).groups()[0]

print get_id("http://www.trade2win.com/boards/forex/index256.html")    