import singer
from .schemas import IDS
import pdb
from .context import *
from .category import Category
from .customer import Customer
from .employee import Employee
from .item import Item
from .itemmatrix import ItemMatrix
from .order import Order
from .orderline import OrderLine
from .register import Register
from .sale import Sale
from .saleline import SaleLine
from .transfer import Transfer
from .shop import Shop
from .vendor import Vendor
from .vendorreturn import VendorReturn

LOGGER = singer.get_logger()


def metrics(tap_stream_id, records):
    with singer.metrics.record_counter(tap_stream_id) as counter:
        counter.increment(len(records))

def write_records(tap_stream_id, records):
    singer.write_records(tap_stream_id, records)
    metrics(tap_stream_id, records)

def sync_lists(ctx):
    #pdb.set_trace()
    Transfer(ctx)
    Shop(ctx)
    VendorReturn(ctx)
    Vendor(ctx)
    Item(ctx)
    Order(ctx)
    OrderLine(ctx)
    Sale(ctx)
    SaleLine(ctx)
    Customer(ctx)
    Register(ctx)
    Category(ctx)
    Employee(ctx)
    ItemMatrix(ctx)        

class Stream(object):
    Order = [IDS.table1, ["orderID"]]
    Item = [IDS.table2, ["itemID"]]
    Customer = [IDS.table3, ["customerID"]]
    Sale = [IDS.table4, ["saleID"]]
    SaleLine = [IDS.table5, ["saleLineID"]]
    Shop = [IDS.table6, ["shopID"]]
    Vendor = [IDS.table7, ["vendorID"]]
    Category = [IDS.table8, ["categoryID"]]
    ItemMatrix = [IDS.table9, ["itemMatrixID"]]
    Employee = [IDS.table10, ["employeeID"]]
    Register = [IDS.table11, ["registerID"]]
    OrderLine = [IDS.table12, ["orderLineID"]]
    Transfer = [IDS.table13, ["transferID"]]
    TransferItem = [IDS.table14, ["transferItemID"]]
    VendorReturn = [IDS.table15, ["vendorReturnID"]]
   
    
    def write_page(self, page):
        singer.write_records(self.tap_stream_id, page)
        with metrics.record_counter(self.tap_stream_id) as counter:
            counter.increment(len(page))

