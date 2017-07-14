#!/usr/bin/env python
#
# insert value in cassandra

# pylint: disable=invalid-name,line-too-long,missing-docstring

import socket
import sched
import time
from datetime import datetime
import uuid

import cassandra

from cassandra.cluster import Cluster # pylint: disable=no-name-in-module
from cassandra.auth import PlainTextAuthProvider

from cassandra import ConsistencyLevel
from cassandra.policies import RetryPolicy, ExponentialReconnectionPolicy

from cassandra.query import BatchStatement # pylint: disable=no-name-in-module

print cassandra.__version__

auth_provider = PlainTextAuthProvider(username='dev', password='dev_pwd')
cas_host_cluster = ['172.16.0.210', '172.16.0.211', '172.16.0.215', '172.16.0.216']
cas_port_cluster = 9042
cas_keyspace = ('backend01', 'backend02', 'backend03')

max_batch_insert = 1000
event_interval = 2

text = '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'



def insert_event():

    for ck in cas_keyspace:
        session.set_keyspace(ck)

        print datetime.now(), 'create table autoinsert in keyspace ' + ck
        create_table = session.execute("CREATE TABLE IF NOT EXISTS autoinsert ( \
            id uuid, \
            created_at timestamp, \
            hostname varchar, \
            text varchar, \
            iteration int, \
            PRIMARY KEY (id, created_at, hostname))"
                                      )

        insert = session.prepare("INSERT INTO autoinsert (id, created_at, hostname, text, iteration) VALUES (?, ?, ?, ?, ?)")
        batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)

        print datetime.now(), 'start loop for ' + str(max_batch_insert) + ' iteration "add insert" on keyspace: ' + ck
        for i in range(max_batch_insert):
            batch.add(insert, (uuid.uuid1(), datetime.now(), socket.gethostname(), text, i))
        print datetime.now(), 'execute batch'
        row = session.execute_async(batch)

class PeriodicScheduler(object):
    def __init__(self):
        self.scheduler = sched.scheduler(time.time, time.sleep)

    def setup(self, interval, action, actionargs=()):
        action(*actionargs)
        self.scheduler.enter(interval, 1, self.setup,
                             (interval, action, actionargs))

    def run(self):
        self.scheduler.run()


default_policy = RetryPolicy()
default_policy.on_write_timeout(
    query=None, consistency=4, write_type=0,
    required_responses=1, received_responses=2, retry_num=10)
reco_policy = ExponentialReconnectionPolicy(10, 60, None)


cluster = Cluster(cas_host_cluster, cas_port_cluster, auth_provider=auth_provider, default_retry_policy=default_policy, reconnection_policy=reco_policy)
session = cluster.connect()

periodic_scheduler = PeriodicScheduler()
periodic_scheduler.setup(event_interval, insert_event)
periodic_scheduler.run()
