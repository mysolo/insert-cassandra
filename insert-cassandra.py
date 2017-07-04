#!env python
#
# insert value in cassandra

import socket
from datetime import datetime
from uuid import UUID

import cassandra

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
#from cassandra.cluster import Session

from cassandra import ConsistencyLevel
#from cassandra.query import SimpleStatement
from cassandra.query import BatchStatement

print cassandra.__version__

auth_provider = PlainTextAuthProvider(username='dev', password='dev_pwd')
cas_host_cluster = ['172.16.0.210','172.16.0.211','172.16.0.215','172.16.0.216']
cas_port_cluster = 9042

cas_keyspace = ('backend01','backend02','backend03')

cluster = Cluster(cas_host_cluster, cas_port_cluster, auth_provider=auth_provider)
session = cluster.connect()

for ck in cas_keyspace:
    session.set_keyspace(ck)

    print datetime.now(),'create table autoinsert in keyspace %s',(ck, )
    create_table = session.execute("CREATE TABLE IF NOT EXISTS autoinsert ( \
        id uuid, \
        created_at timestamp, \
        hostname varchar, \
        PRIMARY KEY (created_at, hostname))"
        )

    insert = session.prepare("INSERT INTO autoinsert (id, created_at, hostname) VALUES (?, ?, ?)")
    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)

    for i in (range(100)):
        batch.add(insert, (UUID, datetime.now(), socket.gethostname()))

    session.execute(batch)
    