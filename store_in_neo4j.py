from collections.abc import Iterable

from neo4j import GraphDatabase

from store import Store


class Neo4jStore(Store):
    supports_properties = 1
    supports_graph = 1
    supports_search = 1
    supports_fulltext = 1

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

    def create(self, nodeid=None, properties=None):
        if properties is None:
            properties = {}
        properties = self._enrich(nodeid, properties)
        return self.run('create (n:Node) set n=$properties return n', properties=properties).single()['n']

    def read(self, nodeid):
        n = self.run('match (n:Node) where n._id=$nodeid return n', nodeid=nodeid).single()['n']
        return dict(n)

    def update(self, nodeid, update=None, properties=None):
        if update is not None:
            ...
        elif properties is not None:
            properties = self._enrich(nodeid, properties)
            n = self.run('match (n:Node) where n._id=$nodeid set n=$properties return n',
                         nodeid=nodeid,
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
        result = self.run('call db.index.fulltext.queryNodes("_fulltext", $searchterm) yield node, score return '
                          'node._id as _id',
                          searchterm=searchterm)
        return [row['_id'] for row in result]

