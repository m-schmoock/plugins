#!/usr/bin/env python3
from pyln.client import Plugin
from collections import OrderedDict

plugin = Plugin()


@plugin.method("forwardstats")
def forwardstats(plugin):
    """Get LN router forwarding statistics for your node. """

    # collect all active CHANNELD_NORMAL scids
    peers = plugin.rpc.listpeers()['peers']
    scids = []
    for peer in peers:
        for chan in peer['channels']:
            if chan['state'] == "CHANNELD_NORMAL":
                scids.append(chan['short_channel_id'])

    forwards = plugin.rpc.listforwards()['forwards']
    stats = {}

    for f in forwards:
        if 'out_channel' not in f:
            continue
        out = f['out_channel']
        if out not in scids:
            continue
        if out not in stats:
            stats[out] = {"success": 0, "failed": 0}

        if f['status'] == 'settled':
            stats[out]['success'] += 1
        if f['status'] == 'failed':
            stats[out]['failed'] += 1

    for scid, stat in stats.items():
        total = stat['success'] + stat['failed']
        stat['rate'] = stat['success'] / total

    ordered = OrderedDict(sorted(stats.items(), key=lambda x: x[1]['rate']))
    return ordered


@plugin.init()
def init(options, configuration, plugin):
    plugin.log(f"Plugin forwardstats initialized")


plugin.run()
