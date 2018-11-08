#!/usr/bin/env python3
import os
import requests
import pdb
import json
import singer
from singer import utils
from singer import metadata
from singer.catalog import Catalog, CatalogEntry, Schema
from . import streams as streams_
from .context import Stream
from . import schemas

REQUIRED_CONFIG_KEYS = [
    "start_date",
    "oauth_client_id",
    "oauth_client_secret",
    "user_agent",
    "refresh_token",
    "customer_ids",]
LOGGER = singer.get_logger()

def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)
    
def check_credentials_are_authorized():
    pass

def load_schema(tap_stream_id):
    path = "schemas/{}.json".format(tap_stream_id)
    schema = utils.load_json(get_abs_path(path))
    return schema

def discover(ctx):
    check_credentials_are_authorized()
    catalog = Catalog([])
    for tap_stream_id in schemas.stream_ids:
        schema = Schema.from_dict(load_schema(tap_stream_id), inclusion="available")
        catalog.streams.append(CatalogEntry(
            stream=tap_stream_id,
            tap_stream_id=tap_stream_id,
            key_properties=schemas.pk_fields[tap_stream_id],
            schema=schema,
        ))
    return catalog


def sync(ctx):
    for tap_stream_id in ctx.selected_stream_ids:
        schemas.load_and_write_schema(tap_stream_id)
    streams_.sync_lists(ctx)
    ctx.first_time = False
    ctx.write_state()

    
def main():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    ctx = Stream(args.config, args.state)
    if args.discover:
        ctx.catalog = discover(ctx)
        discover(ctx).dump()
        print()
    else:
        ctx.catalog = discover(ctx)
        ctx.selected_stream_ids = set(
            [s.tap_stream_id for s in ctx.catalog.streams]
        )
        #pdb.set_trace()
        sync(ctx)


if __name__ == "__main__":
    main()
