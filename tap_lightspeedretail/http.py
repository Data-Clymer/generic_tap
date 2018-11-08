import requests
import pdb
import re
from singer import metrics
import backoff
import googleads
import json 
from datetime import datetime, timedelta
import time
import xmltodict

class RateLimitException(Exception):
    pass

def convert(xml_file, xml_attribs=True):
    xml_file = xml_file.decode("utf-8", "replace")
    d = xmltodict.parse(xml_file)
    return json.dumps(d)
    

TIME_BETWEEN_REQUESTS = timedelta(seconds=1)

def _join(a, b):
    return a.rstrip("/") + b.lstrip("/")

def generate_access_token():
    payload = {
        'refresh_token': '61c11da3041a354016f035be607de4b07324207d',
        "client_id": "1401747925fa658f4138f61cba102eca7f869d3eab5fcc49811b2f1c4f8cc2f3",
        "client_secret": "0a8170d3b1d6bbae3c1cb0aeb56fc60e87c1002199414b21151f72f806e38eaf",
        'grant_type': 'refresh_token',
    }

    r = requests.post('https://cloud.lightspeedapp.com/oauth/access_token.php', data=payload).json()
    return r['access_token']


class Client(object):
    def __init__(self, config):
        #pdb.set_trace()
       	self.user_agent = config.get("user_agent")
        self.base_url = ""
        self.auth = None
        self.session = requests.Session()
        self.next_request_at = datetime.now()
           		
   
    def prepare_and_send(self, request):
        if self.user_agent:
            request.headers["User-Agent"] = self.user_agent
        return self.session.send(request.prepare())

    def url(self, path, base_url):
        return _join(base_url, path)

    def create_get_request(self, path, **kwargs):
        return requests.Request(method="GET", url=self.url(path), **kwargs)

    def _headers(self, headers):
        headers = headers.copy()
        headers["Authorization"] = self.user_agent + str(generate_access_token())
        return headers
	
    def send(self, method, path, headers={}, **kwargs):
        headers = self._headers(headers)
        url = path
        response = requests.get(url=url, headers=headers)
        return response
	 
	
    @backoff.on_exception(backoff.expo,
                          RateLimitException,
                          max_tries=10,
                          factor=2)
    def request_with_handling(self, request, tap_stream_id):
        with metrics.http_request_timer(tap_stream_id) as timer:
            response = self.prepare_and_send(request)
            timer.tags[metrics.Tag.http_status_code] = response.status_code
        if response.status_code in [429, 503]:
            raise RateLimitException()
        response.raise_for_status()
        return response.json()
        
    def request(self, tap_stream_id, *args, **kwargs):
        wait = (self.next_request_at - datetime.now()).total_seconds()
        if wait > 0:
            time.sleep(wait)
        with metrics.http_request_timer(tap_stream_id) as timer:
            response = self.send(*args, **kwargs)
            self.next_request_at = datetime.now() + TIME_BETWEEN_REQUESTS
            timer.tags[metrics.Tag.http_status_code] = response.status_code
        if response.status_code == 429:
            raise RateLimitException()
        elif response.status_code != 200:
            LOGGER.error(response.text)
            raise RuntimeError('Stream returned code {}, exiting!'
                               .format(response.status_code))
        response.raise_for_status()
        
        return response.json()
        
   


