import uuid


class Node(dict):
    ...


class Edge(dict):
    ...


class Repository:

    def __init__(self,
                 search_properties=None,
                 fulltext_properties=None,
                 blob_content_data='content_data',
                 blob_content_type='content_type'):

        self.search_properties = search_properties
        self.fulltext_properties = fulltext_properties
        self.blob_content_data = blob_content_data
        self.blob_content_type = blob_content_type
        self.stores = {}

    def _make_id(self):
        return str(uuid.uuid4())

    def add_store(self, store):
        store.repo = self

        store.search_properties = self.search_properties
        store.fulltext_properties = self.fulltext_properties
        store.blob_content_data = self.blob_content_data
        store.blob_content_type = self.blob_content_type

        self.stores[store.name] = store

    def begin(self):
        for name, store in self.stores.items():
            if store.needs_undo:
                store.undo_log = []
            else:
                store.begin()

    def commit(self):
        for name, store in self.stores.items():
            if store.needs_undo:
                store.undo_log = []
            else:
                store.commit()

    def abort(self):
        for name, store in self.stores.items():
            if store.needs_undo:
                for method, nodeid, params in reversed(store.undo_log):
                    if params:
                        method(nodeid, **params)
                    else:
                        method(nodeid)
                store.undo_log = []
            else:
                store.abort()

    def create(self, nodeid=None, properties=None):
        if properties is None:
            properties = {}
        if nodeid is None:
            nodeid = self._make_id()
        for name, store in self.stores.items():
            if store.needs_undo:
                store.undo_log.append((store.delete, nodeid, {}))
            store.create(nodeid=nodeid, properties=properties)
        node = Node(**properties)
        node.id = nodeid
        return node

    def read(self, nodeid):
        for name, store in self.stores.items():
            if store.supports_properties:
                node = Node(store.read(nodeid))
                node.id = nodeid
                return node
        raise Exception('no property store found')

    def update(self, nodeid, update=None, properties=None):
        for name, store in self.stores.items():
            if store.needs_undo:
                store.undo_log.append((store.update, nodeid, dict(properties=store.read(nodeid))))
            store.update(nodeid, update=update, properties=properties)

    def delete(self, nodeid):
        for name, store in self.stores.items():
            if store.needs_undo:
                store.undo_log.append((self.create, nodeid, dict(properties=self.read(nodeid))))
            store.delete(nodeid)

    def search(self, **searchterms):
        for name, store in self.stores.items():
            if store.supports_search:
                return store.search(**searchterms)
        raise Exception('no search store found')

    def fulltext(self, query):
        for name, store in self.stores.items():
            if store.supports_fulltext:
                return store.fulltext(query)
        raise Exception('no fulltext store found')

    # properties
    # graph

    def create_edge(self, _source, _reltype, _target, id=None, properties=None):
        ...

    def read_edge(self, id):
        ...

    def update_edge(self, id, _all=None, properties=None):
        ...

    def delete_edge(self, id):
        ...

    def incoming(self, node_id, reltypes=None):
        ...

    def outgoing(self, node_id, reltypes=None):
        ...

    # paths?

    def search_edge(self, **searchterms):
        ...

    def fulltext_edge(self):
        ...


if __name__ == "__main__":
    import doctest

    doctest.testmod()
