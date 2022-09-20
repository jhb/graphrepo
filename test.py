import os
import shutil

from repository import Repository
from store_in_neo4j import Neo4jStore
from store_in_networkx import NetworkXStore
from store_in_whoosh import WhooshStore
from store_in_filesystem import FilesystemStore
if os.path.exists('indexdir'):
    shutil.rmtree('indexdir')

repo = Repository()

nx_store = NetworkXStore('nx')
nx_store.supports_search = 1
nx_store.supports_properties = 0

repo.add_store(nx_store)

ws = WhooshStore('whoosh')
repo.add_store(ws)
neo = Neo4jStore('neo4j',
                 uri="neo4j://localhost:7687",
                 username='neo4j',
                 password='admin')
neo.run('create index _id if not exists for (n:Node) on (n._id)')
neo.run('create fulltext index _fulltext if not exists for (n:Node) on each [n._fulltext]')

neo.commit()
neo.run('match (n) detach delete n')
neo.commit()
repo.add_store(neo)

fsstore = FilesystemStore('fsstore')
fsstore._clear()
repo.add_store(fsstore)


repo.begin()


node5 = repo.create(properties=dict(content_type='text/markdown',
                                    content_data='# Test\n\n Dies ist ein Test'))


node = repo.create(properties=dict(foo='bar'))
node2 = repo.read(node.id)
print(ws.read(node.id))
repo.update(node2.id, properties=dict(foo='bar2'))

node3 = repo.read(node2.id)
print(node3)
node4 = repo.create(properties=dict(foo=2, bar=3))
print(node4)



print(repo.search(foo=2, bar=3))

# repo.delete(node4.id)




print(repo.fulltext('bar2'))

repo.abort()
