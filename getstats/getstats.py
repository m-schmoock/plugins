#!/usr/bin/env python3

from datetime import datetime
import os
import schedule
import sqlite3
import time
from pyln.client import Plugin, Millisatoshi, RpcError
from threading import Thread

# TODOs:
# - timeseries: trace success fail rate of forwards per peer
# - timeseries: fees collected
# - timeseries: availability per peer
# - optimize output format for API use
# - sample result in different OHLC timeframe candles (1hr 1day 1week 1month)
# - purge db method
# - limit size, remove old entries
# - trace certain event hooks as timeseries (e.g. forward events, db writes, ...)
# - reduce data usage by only counting changes and actual events ?
# - render output method in a nice ASCII chart :D

plugin = Plugin()
plugin.initialized = False
plugin.db = None
plugin.count = 0
plugin.tsi_cache = {}

migrations = [
    "CREATE TABLE timeseries (id INTEGER PRIMARY KEY, name text)",
    "CREATE TABLE data (id INTEGER, ts timestamp, value INTEGER,"
    " FOREIGN KEY(id) REFERENCES timeseries(id)) ",
    "CREATE INDEX idx_data_id ON data (id)",
    "CREATE INDEX idx_data_ts ON data (ts)",
    "CREATE INDEX idx_data_idts ON data (id, ts)",
    "INSERT INTO timeseries (name) VALUES ('utxo_count')",
    "INSERT INTO timeseries (name) VALUES ('utxo_amount')",
    "INSERT INTO timeseries (name) VALUES ('channel_count')",
    "INSERT INTO timeseries (name) VALUES ('peer_count')",
    "INSERT INTO timeseries (name) VALUES ('gossiper_count')",
    "INSERT INTO timeseries (name) VALUES ('liquidity_total_amount')",
    "INSERT INTO timeseries (name) VALUES ('liquidity_out_amount')",
    "INSERT INTO timeseries (name) VALUES ('liquidity_in_amount')",
]


def get_tsi(name: str):
    """ Returns the cached timeseries index """
    if name in plugin.tsi_cache:
        return plugin.tsi_cache[name]
    cursor = plugin.db.execute("SELECT id from timeseries WHERE name = ?", (name,))
    row = cursor.fetchone()
    if row is None:
        raise ValueError("unknown timeseries name")
    plugin.tsi_cache[name] = row[0]
    return row[0]


def set_data(name: str, value: int, ts: datetime = None):
    """ Sets an INTEGER datapoint """
    if type(value) is not int:
        value = int(value)
    if ts is None:
        ts = datetime.now()
    plugin.db.execute("INSERT INTO data (id, ts, value) VALUES (?, ?, ?)",
                      (get_tsi(name), ts, value))


def get_data(name: str, tsfrom: datetime, tsto: datetime):
    """ Returns timeseries data range as query cursor """
    if tsfrom is None:
        tsfrom = datetime.fromtimestamp(0)
    if tsto is None:
        tsto = datetime.now()
    if type(tsfrom) is str:
        tsfrom = datetime.fromisoformat(tsfrom)
    if type(tsto) is str:
        tsto = datetime.fromisoformat(tsto)
    return plugin.db.execute("SELECT * FROM data WHERE id = ? and ts >= ? and ts <= ?",
                             (get_tsi(name), tsfrom, tsto))


def setup_db(plugin: Plugin):
    # open database
    plugin.db = sqlite3.connect('stats.sqlite3', check_same_thread=False)

    # check or create migrations table
    result = plugin.db.execute("""
        SELECT count(name) FROM sqlite_master
        WHERE type='table' AND name='migrations'
    """)
    if not bool(result.fetchone()[0]):
        plugin.db.execute("CREATE TABLE migrations (id INTEGER PRIMARY KEY, ts timestamp)")
        plugin.db.commit()

    old_ver = plugin.db.execute("SELECT max(id) FROM migrations").fetchone()[0]
    old_ver = old_ver if old_ver is not None else 0

    # apply migrations ...
    i = 0
    for migration in migrations[old_ver:]:
        i += 1
        if type(migration) is str:
            migration = (migration, )
        if type(migration) is not tuple or len(migration) < 1 or type(migration[0]) is not str:
            raise ValueError(f'Invalid migration {i}')
        plugin.log(f'applying migration {migration}', 'debug')
        args = migration[1:]
        plugin.db.execute(migration[0], args)
        plugin.db.execute("INSERT INTO migrations (ts) VALUES (?)", (datetime.now(),))

    # read current version
    new_ver = plugin.db.execute("SELECT max(_rowid_) FROM migrations").fetchone()[0]
    plugin.log(f'database version: {new_ver} (migrated from {old_ver})')
    plugin.db.commit()


def job(plugin: Plugin):
    """ The job that collects all data """
    plugin.log('collecting stats ...', 'info')
    plugin.count += 1

    # partly taken from summary.py
    # info = plugin.rpc.getinfo()
    funds = plugin.rpc.listfunds()
    peers = plugin.rpc.listpeers()
    utxos = [int(f['amount_msat']) for f in funds['outputs'] if f['status'] == 'confirmed']
    avail_out = Millisatoshi(0)
    avail_in = Millisatoshi(0)
    num_channels = 0
    num_connected = 0
    num_gossipers = 0
    for p in peers['peers']:
        active_channel = False
        for c in p['channels']:
            if c['state'] != 'CHANNELD_NORMAL':
                continue
            active_channel = True
            if p['connected']:
                num_connected += 1
            if c['our_reserve_msat'] < c['to_us_msat']:
                to_us = c['to_us_msat'] - c['our_reserve_msat']
            else:
                to_us = Millisatoshi(0)
            avail_out += to_us
            to_them = c['total_msat'] - c['to_us_msat']
            if c['their_reserve_msat'] < to_them:
                to_them = to_them - c['their_reserve_msat']
            else:
                to_them = Millisatoshi(0)
            avail_in += to_them
            num_channels += 1
        if not active_channel and p['connected']:
            num_gossipers += 1

    set_data('utxo_count', len(utxos))
    set_data('utxo_amount', Millisatoshi(sum(utxos)))
    set_data('channel_count', num_channels)
    set_data('peer_count', num_connected)
    set_data('gossiper_count', num_gossipers)
    set_data('liquidity_total_amount', avail_out + avail_in)
    set_data('liquidity_out_amount', avail_out)
    set_data('liquidity_in_amount', avail_in)
    plugin.db.commit()


def scheduler(plugin: Plugin):
    """ Simply calls the scheduler again and again """
    schedule.every().hour.at(":37").do(job, plugin)
    # schedule.every().second.do(job, plugin)
    while 1:
        schedule.run_pending()
        time.sleep(10)
        # time.sleep(0.1)


def check_initialized():
    if plugin.initialized is False:
        raise RpcError('getstats', {}, {'message': 'Plugin not yet initialized'})


@plugin.method('getstats')
def get_stats(plugin: Plugin, name: str, tsfrom: str = None, tsto: str = None):
    """ Returns captured getstats timeseries data """
    check_initialized()
    rows = get_data(name, tsfrom, tsto).fetchall()
    return rows


@plugin.method('gettimeseries')
def get_timeseries(plugin: Plugin):
    """ Returns the names of all known getstats timeseries """
    check_initialized()
    return plugin.db.execute("SELECT name from timeseries").fetchall()


@plugin.init()
def init(options, configuration, plugin):
    # setup data base and version table
    setup_db(plugin)

    plugin.thread = Thread(target=scheduler, args=(plugin, ))
    plugin.thread.start()
    plugin.initialized = True
    plugin.log(f"Plugin {os.path.basename(__file__)} initialized")


plugin.run()
