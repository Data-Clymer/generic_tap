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


class VendorReturn(Stream):
    def __init__(self, Stream):
        #pdb.set_trace()
        super().__init__(Stream.config, Stream.state)
        super().write_page('VendorReturn')
        
    def create_relation(self, stream_id, start_date):
        relation = ""
        if str(stream_id) == "VendorReturn":
            relation = "&timeStamp=%3E," +start_date
            stream_id = "DisplayTemplate/VendorReturn"
        return relation
        
    def paginate(self, offset, count, ext_time, path, stream_id, transferid):
        if len(self.state) < 14:
            start_date = singer.utils.strptime_with_tz(self.config['start_date'])
        else:
            first_time = False
            start_date = singer.utils.strptime_with_tz(self.state[stream_id])
        start_date = start_date.strftime('%m/%d/%YT%H:%M:%S')
        ext_time = start_date 
        if stream_id == "VendorReturn":
            stream_id = "DisplayTemplate/VendorReturn"
        while (int(count) > int(offset) and (int(count) - int(offset)) >= -100) or (stream_id == ("Inventory/Transfer/" + transferid + "/TransferItems") and len(self.id) > 1):    
            url = "https://api.merchantos.com/API/Account/" + str(self.config['customer_ids']) + "/" + str(stream_id) + ".json?offset="
            if stream_id == "DisplayTemplate/VendorReturn":
                stream_id = "VendorReturn"
                relation = self.create_relation(stream_id, start_date)
            else:
                relation = self.create_relation(stream_id, start_date)
            #pdb.set_trace()
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
                else:
                    if key['timeStamp'] >= ext_time:
                        ext_time = key['timeStamp']
                    else:
                        pass
                    singer.write_record(stream_id, key)
                    with metrics.record_counter(stream_id) as counter:
                         counter.increment(len(page))
                    continue
            path.append(ext_time)
            self.update_start_date_bookmark(path, str(stream_id))