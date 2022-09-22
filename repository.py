import uuid
from typing import List


class Node(dict):

    def __init__(self,
                 id=None,
                 content=None,
                 /,  # end of positional only arguments
                 **kwargs
                 ):

        super().__init__(self, **kwargs)
        self.id = id
        self.content = content

    def __str__(self):  # sourcery skip: replace-interpolation-with-fstring
        return (f'Node({self.id}, '
                f'{self.content and "..." or None}, '
                f'{", ".join(["%s=%s" % (k, repr(v)) for k, v in self.items()])})')

    def clone(self):
        return Node(self.id,self.content, **self)

class Edge(dict):
    ...


class Repository:

    def __init__(self,
                 type_property = 'content_type',
                 filename_property = 'content_filename',
                 search_properties=None,
                 fulltext_properties=None,):

        self.type_property = type_property
        self.filename_property = filename_property
        self.search_properties = search_properties
        self.fulltext_properties = fulltext_properties
        self.stores = {}

    def _make_id(self):
        return str(uuid.uuid4())

    def add_store(self, store):
        store.repo = self

        store.type_property = self.type_property
        store.filename_property = self.filename_property
        store.search_properties = self.search_properties
        store.fulltext_properties = self.fulltext_properties

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
                for method, *args in reversed(store.undo_log):
                    method(*args)
                store.undo_log = []
            else:
                store.abort()

    def write(self, node: Node):
        if node.id is None:
            node.id = self._make_id()

        for name, store in self.stores.items():
            if store.needs_undo:
                store.undo_log.append((store.delete, node.id))
            store.write(node)
        return node

    def read(self, nodeid, properties=True, blob=True):

        stores = sorted(self.stores.values(), key=lambda x: x.supports_blobs, reverse=True)
        property_stores = [store for store in stores if store.supports_properties]
        blob_stores = [store for store in stores if store.supports_blobs]

        properties = properties and property_stores
        blob = blob and blob_stores

        has_blob = False
        has_properties = False
        node = Node(nodeid)

        for store in stores:
            if properties and not has_properties and store.supports_properties:
                node.update(store.read(nodeid))
                has_properties = True
                # print(f'properties from {store.name}')
            if blob and not has_blob and store.supports_blobs:
                node.content = store.read(nodeid).content
                has_blob = True
                # print(f'blob from {store.name}')

        if (not properties or has_properties) and (not blob or has_blob):
            return node

        raise Exception((f'could not read: properties required: {properties}, {has_properties}, '
                        f'blob required: {blob}, {has_blob}'))

    def update(self, node: Node, update_only=True):

        for name, store in self.stores.items():
            if store.needs_undo:
                store.undo_log.append((store.update, store.read(node.id)))
            store.update(node, update_only)

    def delete(self, nodeid):
        for name, store in self.stores.items():
            if store.needs_undo:
                store.undo_log.append((self.write, store.read(nodeid)))
            store.delete(nodeid)

    def search(self, **searchterms) -> List[str]:
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

    def write_edge(self, _source, _reltype, _target, id=None, properties=None):
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
