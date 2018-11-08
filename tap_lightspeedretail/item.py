from datetime import datetime, date, timedelta
import pendulum
import singer
from singer import bookmarks as bks_
from http import *
from singer import metrics
import pdb
import strict_rfc3339
import json
from .context import Stream

class Item(Stream):
    def __init__(self, Stream):
        #pdb.set_trace()
        super().__init__(Stream.config, Stream.state)
        super().write_page('Item')
        
    def create_relation(self, start_date):
        relation = "&load_relations=%5B%22ItemShops%22%2C+%22ItemAttributes%22%2C+%22Tags%22%2C+%22TaxClass%22%5D&archived=true&or=timeStamp%3D%3E%2C" +start_date + "%7CItemShops.timeStamp%3D%3E%2C" +start_date
        return relation
        
    def paginate(self, offset, count, ext_time, path, stream_id, transferid):
        if len(self.state) < 14:
            start_date = singer.utils.strptime_with_tz(self.config['start_date'])
        else:
            first_time = False
            start_date = singer.utils.strptime_with_tz(self.state[stream_id])
        start_date = start_date.strftime('%Y-%m-%dT%H:%M:%S')
        ext_time = start_date 
        while (int(count) > int(offset) and (int(count) - int(offset)) >= -100) or (stream_id == ("Inventory/Transfer/" + transferid + "/TransferItems") and len(self.id) > 1):    
            url = "https://api.merchantos.com/API/Account/" + str(self.config['customer_ids']) + "/" + str(stream_id) + ".json?offset="
            relation = self.create_relation(start_date)
            page = self.client.request(stream_id, "GET", (url + str(offset) + relation))
            info = page['@attributes']
            count = info['count']
            if int(count) == 0:
                offset = 0
                continue
            elif int(count) <= 100:
                offset = 300
                data = page[str(stream_id)]  
            else:
                offset = int(info['offset']) + 100
                data = page[str(stream_id)]  
            for key in data:
                if type(key) == str:
                    if data['timeStamp'] >= ext_time:
                        ext_time = data['timeStamp']
                    else:
                        pass
                    singer.write_record(stream_id, data)
                    with metrics.record_counter(stream_id) as counter:
                         counter.increment(len(page))
                    continue
                elif str(stream_id) == "Item": 
                    i = 0
                    for i in range(len(key['ItemShops']['ItemShop'])):
                        #pdb.set_trace()
                        if key['ItemShops']['ItemShop'][i]['timeStamp'] >= ext_time:
                            ext_time =  key['ItemShops']['ItemShop'][i]['timeStamp']
                    if key['timeStamp'] >= ext_time:
                        ext_time = key['timeStamp']
                singer.write_record(stream_id, key)
                with metrics.record_counter(stream_id) as counter:
                     counter.increment(len(page))
            path.append(ext_time)
            self.update_start_date_bookmark(path, str(stream_id))