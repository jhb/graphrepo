import networkx as nx

from store import Store, NonExistent

class NetworkXStore(Store):
    supports_properties = 1
    supports_graph = 1
    supports_search = 1
    needs_undo = 1

    def setup_store(self):
        self.G = nx.DiGraph()

    def create(self, nodeid=None, properties=None):
        if properties is None:
            properties = {}
        self.G.add_node(nodeid, **properties)

    def read(self, nodeid):
        return self.G.nodes[nodeid]

    def update(self, nodeid, update=None, properties=None):

        if update is not None:
            self.G.nodes[nodeid].update(update)
        elif properties is not None:
            self.G.add_node(nodeid, **properties)

    def delete(self, nodeid):
        self.G.remove_node(nodeid)

    def search(self, **searchterms):

        def filter_node(nodeid):
            node = self.G.nodes[nodeid]
            return all(node.get(k, NonExistent) == v for k, v in searchterms.items())

        subgraph = nx.subgraph_view(self.G, filter_node=filter_node)
        return subgraph.nodes()
