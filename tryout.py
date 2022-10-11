"""
These are no proper tests, but just a workscript to try

"""

import os
import shutil

from graphrepo.store_in_memgraph import MemgraphStore
from repository import Repository, Node
from store_in_neo4j import Neo4jStore
from store_in_networkx import NetworkXStore
from store_in_whoosh import WhooshStore
from store_in_filesystem import FilesystemStore
if os.path.exists('indexdir'):
    shutil.rmtree('indexdir')

repo = Repository()

nx_store = NetworkXStore('nx')
nx_store.supports_search = 1
nx_store.supports_properties = 1

repo.add_store(nx_store)

ws = WhooshStore('whoosh')
repo.add_store(ws)

neo = Neo4jStore('neo4j',
                 uri="neo4j://localhost:7687",
                 username='neo4j',
                 password='admin')
neo.run('create index _id if not exists for (n:Node) on (n._id)')
neo.run(f'create fulltext index fulltext if not exists for (n:Node) on each [n.{neo.fulltext_property}]')
neo.commit()
neo.run('match (n) detach delete n')
neo.commit()
repo.add_store(neo)
#
fsstore = FilesystemStore('fsstore')
fsstore._clear()
repo.add_store(fsstore)

mg_store = MemgraphStore()
mg_store.run('match (n) detach delete n')
mg_store.commit()
repo.add_store(mg_store)

repo.begin()





node = repo.write(Node(foo='bar', baz='bla'))
node2 = repo.read(node.id)

node2['foo']='bar2'
repo.update(node2)

repo.update(Node(node2.id,foo='bar3'))

node3 = repo.read(node.id)
print(node3)
node4 = repo.write(Node(foo=2, bar=3))
print(node4)



print(repo.search(foo=2, bar=3))

repo.delete(node4.id)


node5 = repo.write(Node(None,
                         '# Test\n\n Dies ist ein Test',
                         content_type='text/markdown'))
print(repo.read(node5.id))

print(repo.fulltext('dies'))

repo.abort()
