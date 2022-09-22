from collections.abc import Iterable

from neo4j import GraphDatabase

from store import Store, Node


class Neo4jStore(Store):
    supports_properties = 1
    supports_graph = 1
    supports_search = 1
    supports_fulltext = 0

    def __init__(self, name='', **connection_details):
        self.tx = None
        super().__init__(name, **connection_details)

    def setup_store(self):

        cd = self.connection_details
        self.driver = GraphDatabase.driver(cd['uri'],
                                           auth=(cd['username'],
                                                 cd['password']))
        self.session = self.driver.session()

    def run(self, statement, **kwargs):
        if not self.tx:
            self.begin()
        return self.tx.run(statement, **kwargs)

    def begin(self):
        if self.tx:
            self.tx.close()
        self.tx = self.session.begin_transaction()

    def commit(self):
        self.tx.commit()
        self.tx.close()
        self.tx = None

    def abort(self):
        self.tx.rollback()
        self.tx.close()
        self.tx = None

    def _neo2node(self,neonode):
        return Node(neonode['_id'], {k: v for k, v in neonode.items() if k not in  [self.fulltext_property, '_id']})

    def write(self, node):
        properties = self._enrich(node.id, node)
        neonode = self.run('create (n:Node) set n=$properties return n', properties=properties).single()['n']
        return self._neo2node(neonode)

    def read(self, nodeid):
        neonode = self.run('match (n:Node) where n._id=$nodeid return n', nodeid=nodeid).single()['n']
        return self._neo2node(neonode)

    def update(self, node, update_only=True):
        if update_only:
            update_node = self.read(node.id)
            update_node.update(node)
            node = update_node

        properties = self._enrich(node.id, node)

        n = self.run('match (n:Node) where n._id=$nodeid set n=$properties return n',
                     nodeid=node.id,
                     properties=properties).single()['n']

    def delete(self, nodeid):
        self.run('match (n:Node) where n._id=$nodeid delete n', nodeid=nodeid)

    def search(self, **searchterms):
        conditions = [f'n.{k}=${k}' for k in searchterms]
        cs = ' and '.join(conditions)
        result = self.run(f'match (n:Node) where  {cs} return n["_id"] as _id',
                          **searchterms)
        return [row['_id'] for row in result]

    def fulltext(self, searchterm):
        result = self.run('call db.index.fulltext.queryNodes("fulltext", $searchterm) yield node, score return '
                          'node._id as _id',
                          searchterm=searchterm)
        return [row['_id'] for row in result]

