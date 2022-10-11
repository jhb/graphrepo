from gqlalchemy import utilities as mgu
import mgclient
from store import Store, Node

class MemgraphStore(Store):
    supports_properties = 1
    supports_graph = 1
    supports_search = 1

    def __init__(self, name='', **connection_details):
        self.cursor = None
        super().__init__(name, **connection_details)

    def setup_store(self):
        self.mg = mgclient.connect(host='127.0.0.1', port=7688)

    def run(self, statement, **kwargs):
        if not self.cursor:
            self.begin()
        self.cursor.execute(statement, kwargs)
        return self.cursor.fetchall()  # TODO

    def begin(self):
        self.cursor = self.mg.cursor()

    def commit(self):
        self.mg.commit()
        self.cursor = None

    def abort(self):
        self.mg.rollback()
        self.cursor = None

    def _row2dict(self, row):
        d = {}
        for idx, col in enumerate(self.cursor.description):
            d[col.name] = row[idx]
        return d

    def _mg2node(self, mgnode):
        node = Node(mgnode.properties)
        node.id = mgnode.properties['_id']
        return node

    def write(self, node):
        props = dict(node)
        props['_id']=node.id
        cp = mgu.to_cypher_properties(props)
        result= self.run(f"create (n:Node) set n={cp} return n")
        return self._mg2node(self._row2dict(result[0])['n'])

    def read(self,  nodeid):
        result = self.run(f'match (n:Node) where n._id="{nodeid}" return n')
        return self._mg2node(self._row2dict(result[0])['n'])

    def update(self, node, update_only=True):
        if update_only:
            update_node = self.read(node.id)
            update_node.update(node)
            node = update_node

        props = dict(node)
        props['_id']=node.id
        cp = mgu.to_cypher_properties(props)

        self.run(f'match (n:Node) where n._id="{node.id}" set n={cp} return n')

    def delete(self, nodeid):
        self.run('match (n:Node) where n._id="{nodeid}" delete n')

    def search(self, **searchterms):
        conditions = [f'n.{k}=${k}' for k in searchterms]
        cs = ' and '.join(conditions)
        result = self.run(f'match (n:Node) where  {cs} return n["_id"] as _id',
                          **searchterms)
        return [self._row2dict(row)['_id'] for row in result]