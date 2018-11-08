from datetime import datetime, date, timedelta
import pendulum
import singer
from singer import bookmarks as bks_
from .http import Client
from singer import metrics
import pdb
import strict_rfc3339
import json

class Context(object):
    """Represents a collection of global objects necessary for performing
    discovery or for running syncs. Notably, it contains

    - config  - The JSON structure from the config.json argument
    - state   - The mutable state dict that is shared among streams
    - client  - An HTTP client object for interacting with the API
    - catalog - A singer.catalog.Catalog. Note this will be None during
                discovery.
    """
    def __init__(self, config, state):
       	self.config = config
        self.state = state
        self.client = Client(config)
        self._catalog = None
        self.selected_stream_ids = None
        self.now = datetime.utcnow()
        self.first_time = True
        self.id = set()
        self.recurse = False
        
    @property
    def catalog(self):
        return self._catalog

    @catalog.setter
    def catalog(self, catalog):
        self._catalog = catalog
        self.selected_stream_ids = set(
            [s.tap_stream_id for s in catalog.streams
             if s.is_selected()]
        )

    def get_bookmark(self, path):
        return bks_.get_bookmark(self.state, *path)

    def bookmark(self, path, stream_id):
        bookmark = self.state
        for p in path:
            if p not in bookmark:
                if self.first_time:
                    bookmark['type'] = "STATE"
                    bookmark[stream_id] = p 
                else:
                    del bookmark[stream_id]
                    bookmark['type'] = "STATE"
                    bookmark[stream_id] = p  
        return bookmark
        
    def set_bookmark(self, path, val):
        if isinstance(val, date):
            val = val.isoformat()
        bks_.write_bookmark(self.state, path[0], path[1], val)
        

    def get_offset(self, path):
        off = bks_.get_offset(self.state, path[0])
        return (off or {}).get(path[1])

    def set_offset(self, path, val):
        bks_.set_offset(self.state, path[0], path[1], val)

    def clear_offsets(self, tap_stream_id):
        bks_.clear_offset(self.state, tap_stream_id)

    def update_start_date_bookmark(self, path, stream_id):
        val = self.bookmark(path, stream_id)
        if not val:
            val = self.config["start_date"]
            self.set_bookmark(path, val)
        return val
    
    def create_relation(self, stream_id, transferid, start_date):
        relation = ""
        if str(stream_id) == "Item":
            relation = "&load_relations=%5B%22ItemShops%22%2C+%22ItemAttributes%22%2C+%22Tags%22%2C+%22TaxClass%22%5D&archived=true&or=timeStamp%3D%3E%2C" +start_date + "%7CItemShops.timeStamp%3D%3E%2C" +start_date
        elif str(stream_id) == "Shop":
            relation = ""
        elif str(stream_id) == "Transfer":
            relation = "&archived=true&orderby=transferID&timeStamp=%3E," +start_date 
            stream_id = "Inventory/Transfer"
        elif str(stream_id) == "VendorReturn":
            relation = "&timeStamp=%3E," +start_date
            stream_id = "DisplayTemplate/VendorReturn"
        elif str(stream_id) == "TransferItem" or str(stream_id) == "Inventory/Transfer/" + transferid + "/TransferItems":
            relation = "&archived=true&orderby=transferItemID&timeStamp=%3E," +start_date 
            stream_id = "Inventory/Transfer/" + transferid + "/TransferItems"
        elif str(stream_id) == "Customer":
            relation = "&load_relations=all&timeStamp=%3E," +start_date
        elif str(stream_id) == "SaleLine":
            relation = "&load_relations=%5B%22TaxClass%22%5D&timeStamp=%3E," +start_date
        elif str(stream_id) == "ItemMatrix":
            relation = "&load_relations=%5B%22TaxClass%22%5D&timeStamp=%3E," +start_date
        elif str(stream_id) == "Employee":
            relation = "&load_relations=%5B%22EmployeeRole%22%5D&timeStamp=%3E," +start_date
        elif str(stream_id) == "Register":
            relation = ""
        else:
            relation = "&timeStamp=%3E," +start_date
        return relation
    
    
    def paginate(self, offset, count, ext_time, path, stream_id, transferid): 
        if len(self.state) < 14:
            start_date = singer.utils.strptime_with_tz(self.config['start_date'])
        else:
            first_time = False
            start_date = singer.utils.strptime_with_tz(self.state[stream_id])
        end_date = singer.utils.now()
        if str(stream_id) == "Item":
            start_date = start_date.strftime('%Y-%m-%dT%H:%M:%S')
            ext_time = start_date
        else:
            start_date = start_date.strftime('%m/%d/%YT%H:%M:%S')
            ext_time = start_date
        while (int(count) > int(offset) and (int(count) - int(offset)) >= -100) or (stream_id == ("Inventory/Transfer/" + transferid + "/TransferItems") and len(self.id) > 1):
            url = "https://api.merchantos.com/API/Account/" + str(self.config['customer_ids']) + "/" + str(stream_id) + ".json?offset="
            relation = self.create_relation(stream_id, transferid, start_date)
            page = self.client.request(stream_id, "GET", (url + str(offset) + relation))
            if stream_id == "Inventory/Transfer/" + transferid + "/TransferItems":
                stream_id = "TransferItem"
                #pdb.set_trace()
                if len(self.id) < 1: 
                    break
                else:
                    transferid = self.id.pop()
            elif stream_id == "Inventory/Transfer":
                stream_id = "Transfer"
            elif stream_id == "DisplayTemplate/VendorReturn":
                stream_id = "VendorReturn"
            info = page['@attributes']
            count = info['count']
            if int(count) == 0:
                offset = 0
                if len(self.id) < 1: 
                    continue
                else:
                    transferid = self.id.pop()
                stream_id = "Inventory/Transfer/" + transferid + "/TransferItems"
                continue
            elif int(count) <= 100:
                offset = 300
                data = page[str(stream_id)]  
            else:
                offset = int(info['offset']) + 100
                data = page[str(stream_id)]  
            for key in data:
                if str(stream_id) == "Register": 
                	pass
                elif type(key) == str:
                    if data['timeStamp'] >= ext_time:
                        ext_time = data['timeStamp']
                    else:
                        pass
                    singer.write_record(stream_id, data)
                    with metrics.record_counter(stream_id) as counter:
                         counter.increment(len(page))
                    continue
                elif str(stream_id) == "Transfer": 
                    self.id.add(key['transferID'])
                    if key['timeStamp'] >= ext_time:
                        ext_time = key['timeStamp']
                elif str(stream_id) == "Sale": 
                    if key['updateTime'] >= ext_time:
                        ext_time = key['updateTime']
                elif str(stream_id) == "Item": 
                    i = 0
                    for i in range(len(key['ItemShops']['ItemShop'])):
                        #pdb.set_trace()
                        if key['ItemShops']['ItemShop'][i]['timeStamp'] >= ext_time:
                            ext_time =  key['ItemShops']['ItemShop'][i]['timeStamp']
                    if key['timeStamp'] >= ext_time:
                        ext_time = key['timeStamp']
                else: 
                    if key['timeStamp'] >= ext_time:
                        ext_time = key['timeStamp']
                    else:
                        pass
                singer.write_record(stream_id, key)
                with metrics.record_counter(stream_id) as counter:
                     counter.increment(len(page))
            path.append(ext_time)
            self.update_start_date_bookmark(path, str(stream_id))
            if stream_id == "Transfer":
                stream_id = "Inventory/Transfer"
            elif stream_id == "TransferItem":
                offset = 0
                if len(self.id) < 1:
                    pass
                else:
                    transferid = self.id.pop()
                stream_id = "Inventory/Transfer/" + transferid + "/TransferItems"
            elif stream_id == "VendorReturn":
                stream_id = "DisplayTemplate/VendorReturn"
    
    
    def write_page(self, stream_id):
        self.recurse = False
        transferid = "45"
        count = 100
        offset = 0
        path = []
        ext_time = self.config['start_date']
        self.paginate(offset, count, ext_time, path, stream_id, transferid) 
        
            
    def write_state(self):
        singer.write_state(self.state)
        f = open("state.json", 'w')
        message = json.dumps(self.state)
        f.write(str(message))
        f.close()
