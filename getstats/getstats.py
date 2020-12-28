#!/usr/bin/env python3

from datetime import datetime
import os
import schedule
import sqlite3
import time
from pyln.client import Plugin, Millisatoshi, RpcError
from threading import Thread

# TODOs:
# - thread safety
# - timeseries: trace success fail rate of forwards per peer
# - timeseries: availability per peer
# - optimize output format for API use
# - sample result in different OHLC timeframe candles (1hr 1day 1week 1month)
# - purge db method
# - limit size, remove old entries
# - trace event and hook details
# - reduce data usage by only counting changes and actual events ?
# - render output method in a nice ASCII chart :D

plugin = Plugin()
plugin.initialized = False
plugin.db = None
plugin.count = 0
plugin.tsi_cache = {}

TS_VARIABLE = 1  # can be OHLC sampled
TS_EVENT = 2     # can be added and averaged sampled

migrations = [
    "CREATE TABLE timeseries (id INTEGER PRIMARY KEY, name text UNIQUE, ts_type INTEGER NOT NULL)",
    "CREATE TABLE data (ts timestamp, tsi INTEGER, value INTEGER,"
    " FOREIGN KEY(tsi) REFERENCES timeseries(id)) ",
    "CREATE INDEX idx_data_id ON data (tsi)",
    "CREATE INDEX idx_data_ts ON data (ts)",
    "CREATE INDEX idx_data_idts ON data (ts, tsi)",
]

# Track all variables within a 15min sample
sample = {
    # e.g. 'getinfo_num_peers': 0,
}


def check_initialized():
    if plugin.initialized is False:
        raise RpcError('getstats', {}, {'message': 'Plugin not yet initialized'})


def wait_initialized():
    while plugin.initialized is False:
        time.sleep(1)


def ensure_tsi(name: str, ts_type: int):
    """ ensures a give timeseries and type is known in the database """
    if name in plugin.tsi_cache:
        return plugin.tsi_cache[name]

    cursor = plugin.db.execute("SELECT id from timeseries WHERE name = ?", (name,))
    row = cursor.fetchone()
    if row is not None:
        plugin.tsi_cache[name] = row[0]
        return row[0]

    plugin.db.execute("INSERT INTO timeseries (name, ts_type) VALUES (?, ?)", (name, ts_type))
    plugin.db.commit()
    cursor = plugin.db.execute("SELECT id from timeseries WHERE name = ?", (name,))
    row = cursor.fetchone()
    plugin.tsi_cache[name] = row[0]
    return row[0]


def get_tsi(name: str):
    """ get a (cached) timeseries index """
    if name in plugin.tsi_cache:
        return plugin.tsi_cache[name]
    cursor = plugin.db.execute("SELECT id from timeseries WHERE name = ?", (name,))
    row = cursor.fetchone()
    if row is None:
        raise ValueError("unknown timeseries")
    plugin.tsi_cache[name] = row[0]
    return row[0]


def ts_variable(name: str, value: int):
    """ Sets a variable for the current sample """
    ensure_tsi(name, TS_VARIABLE)
    if type(value) is not int:
        value = int(value)
    sample[name] = value


def ts_event(name: str):
    """ Increments an event counter within the current sample """
    if plugin.initialized is False:
        return
    ensure_tsi(name, TS_EVENT)
    sample[name] = sample.get(name, 0) + 1


def store_sample():
    """ stores the current sample values """
    ts = datetime.now()
    for name in sample:
        value = sample[name]
        plugin.db.execute("INSERT INTO data (ts, tsi, value) VALUES (?, ?, ?)",
                          (ts, get_tsi(name), value))


def reset_sample():
    """ resets the current sampl values to 0 """
    for key in sample:
        sample[key] = 0


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
    return plugin.db.execute("SELECT * FROM data WHERE tsi = ? and ts >= ? and ts <= ?",
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
    if old_ver > len(migrations):
        raise Exception('Database has newer state than expected')

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
    info = plugin.rpc.getinfo()
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
            num_channels += 1
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
        if not active_channel and p['connected']:
            num_gossipers += 1

    ts_variable('getinfo_num_peers', info['num_peers'])
    ts_variable('getinfo_num_pending_channels', info['num_pending_channels'])
    ts_variable('getinfo_num_active_channels', info['num_active_channels'])
    ts_variable('getinfo_num_inactive_channels', info['num_inactive_channels'])
    ts_variable('getinfo_fees_collected_msat', info['fees_collected_msat'])

    ts_variable('summary_num_utxos', len(utxos))
    ts_variable('summary_utxo_amount', Millisatoshi(sum(utxos)))
    ts_variable('summary_num_channels', num_channels)
    ts_variable('summary_num_connected', num_connected)
    ts_variable('summary_num_gossipers', num_gossipers)
    ts_variable('summary_avail_total', avail_out + avail_in)
    ts_variable('summary_avail_out', avail_out)
    ts_variable('summary_avail_in', avail_in)

    store_sample()
    reset_sample()
    plugin.db.commit()


def scheduler(plugin: Plugin):
    """ Simply calls the schedule job at given interval """
    wait_initialized()
    # call once initially to make testing easier
    job(plugin)
    # reschedule at given interval
    schedule.every(15).minutes.do(job, plugin)
    while True:
        schedule.run_pending()
        time.sleep(1)


@plugin.subscribe("forward_event")
def forward_event(plugin: Plugin, forward_event: dict, **kwargs):
    ts_event('forward_event')


@plugin.hook('db_write')
def on_db_write(writes, data_version, plugin, **kwargs):
    ts_event('db_write')
    return {"result": "continue"}


@plugin.hook('htlc_accepted')
def on_htlc_accepted(onion, htlc, plugin, **kwargs):
    ts_event('htlc_accepted')
    return {"result": "continue"}


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
