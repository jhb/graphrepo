import networkx as nx

from store import Store, NonExistent, Node

class NetworkXStore(Store):
    supports_properties = 1
    supports_graph = 1
    supports_search = 1
    needs_undo = 1

    def setup_store(self):
        self.G = nx.DiGraph()

    def write(self, node):
        self.G.add_node(node.id, **node)

    def read(self, nodeid, blob=1):
        return Node(nodeid, **self.G.nodes[nodeid])

    def update(self, node, update_only=True):
        if update_only:
            self.G.nodes[node.id].update(node)
        else:
            self.write(node)

    def delete(self, nodeid):
        self.G.remove_node(nodeid)

    def search(self, **searchterms):

        def filter_node(nodeid):
            node = self.G.nodes[nodeid]
            return all(node.get(k, NonExistent) == v for k, v in searchterms.items())

        subgraph = nx.subgraph_view(self.G, filter_node=filter_node)
        nodes = subgraph.nodes()
        return nodes
