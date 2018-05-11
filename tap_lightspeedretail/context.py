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

    def bookmark_order(self, path):
        bookmark = self.state
        for p in path:
            if p not in bookmark:
                del bookmark['order']
                bookmark['type'] = "STATE"
                bookmark['order'] = p  
        return bookmark
        
    def bookmark_item(self, path):
        bookmark = self.state
        for p in path:
            if p not in bookmark:
                del bookmark['item']
                bookmark['type'] = "STATE"
                bookmark['item'] = p  
        return bookmark

    def set_bookmark_item(self, path, val):
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

    def update_start_date_bookmark_item(self, path):
        val = self.bookmark_item(path)
        if not val:
            val = self.config["start_date"]
            self.set_bookmark_item(path, val)
        return val
        
    def update_start_date_bookmark_order(self, path):
        val = self.bookmark_order(path)
        if not val:
            val = self.config["start_date"]
            self.set_bookmark_order(path, val)
        return val

    def write_page_item(self, stream_ids):
        count = 100
        offset = 0
        ext_time = singer.utils.now()
        path = []
        ext_time = ext_time.timestamp()
        ext_time = strict_rfc3339.timestamp_to_rfc3339_utcoffset(ext_time)
        path.append(ext_time)
        #pdb.set_trace()
        start_date =  singer.utils.strptime_with_tz(self.state["item"])
        end_date = (start_date + timedelta(+30))
        start_date = start_date.strftime('%m/%d/%Y')
        end_date = end_date.strftime('%m/%d/%Y')
        while int(count) > int(offset) and (int(count) - int(offset)) > -100:
            page = self.client.request(stream_ids, "GET", "https://api.merchantos.com/API/Account/" + str(self.config['customer_ids']) + "/Item.json?offset=" + str(offset) + "load_relations=%5B%22ItemShops%22%5D&timeStamp=%3E%3C," +str(start_date)+ "," + str(end_date))
            info = page['@attributes']
            data = page["Item"]
            count = info['count']
            offset = int(info['offset']) + 100
            for item in data:
                singer.write_record(stream_ids, item)
                with metrics.record_counter(stream_ids) as counter:
                     counter.increment(len(page))
            self.update_start_date_bookmark_item(path)
                
    def write_page_order(self, stream_ids):
        count = 100
        offset = 0
        ext_time = singer.utils.now()
        path = []
        ext_time = ext_time.timestamp()
        ext_time = strict_rfc3339.timestamp_to_rfc3339_utcoffset(ext_time)
        path.append(ext_time)
        start_date =  singer.utils.strptime_with_tz(self.state["order"])
        end_date = (start_date + timedelta(+30))
        start_date = start_date.strftime('%m/%d/%Y')
        end_date = end_date.strftime('%m/%d/%Y')
        while int(count) > int(offset) and (int(count) - int(offset)) > -100:
            page = self.client.request(stream_ids, "GET", "https://api.merchantos.com/API/Account/160476/Order.json?offset=" + str(offset) + "load_relations=%5B%22OrderLines%22%2C+%22Vendor%22%2C+%22Shop%22%2C+%22CustomFieldValues%22%5D&timeStamp%3E%3C," +str(start_date)+ "," + str(end_date))
            info = page['@attributes']
            data = page["Order"]
            count = info['count']
            offset = int(info['offset']) + 100
            for item in data:
                singer.write_record(stream_ids, item)
                with metrics.record_counter(stream_ids) as counter:
                     counter.increment(len(page))
            self.update_start_date_bookmark_order(path)
    
            
    def write_state(self):
        singer.write_state(self.state)
        f = open("state.json", 'w')
        message = json.dumps(self.state)
        f.write(str(message))
        f.close()