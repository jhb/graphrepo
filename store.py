from typing import Union, List

from repository import Node, Edge, Repository

NonExistent = '____ a totally nonexistent value ____'


class Store:
    supports_properties = 0
    supports_graph = 0
    supports_search = 0
    supports_fulltext = 0
    supports_blobs = 0
    needs_undo = 0

    def __init__(self,
                 name='',
                 **connection_details):
        self.name = name
        self.repo: Union[Repository, None] = None
        self.connection_details = connection_details

        self.search_properties = None
        self.fulltext_properties = None
        self.filename_property = 'content_data'
        self.type_property = 'content_type'
        self.fulltext_property = '___fulltext___'
        self.undo_log = []
        self.setup_store()

    def setup_store(self):
        ...

    def _enrich(self, object_id, node):
        properties = dict(node)
        properties['_id'] = object_id
        if self.supports_fulltext and self.fulltext_property not in properties:
            fulltext = []
            if node.content and type(node.content) == str:
                fulltext.append(f'content {node.content}')
            for k, v in properties.items():
                if k == self.fulltext_property:
                    continue
                fulltext.append(k)
                if type(v) in [list, tuple]:
                    fulltext.extend([str(i) for i in v])
                else:
                    fulltext.append(str(v))
            properties[self.fulltext_property] = ' '.join(fulltext)
        return properties

    def _id(self, obj_or_id):
        return obj_or_id.id if type(obj_or_id) in [Node, Edge] else obj_or_id

    def begin(self):
        raise NotImplementedError

    def commit(self):
        raise NotImplementedError

    def abort(self):
        raise NotImplementedError

    def write(self, node: Node):
        raise NotImplementedError

    def read(self, nodeid):
        raise NotImplementedError

    def update(self, node, update_only=True):
        raise NotImplementedError

    def delete(self, nodeid):
        raise NotImplementedError

    def search(self, **searchterms) -> List[str]:
        raise NotImplementedError

    def fulltext(self, query):
        raise NotImplementedError

    # properties
    # graph

    def create_edge(self, _source, _reltype, _target, id=None, properties=None):
        raise NotImplementedError

    def read_edge(self, id):
        raise NotImplementedError

    def update_edge(self, id, _all=None, properties=None):
        raise NotImplementedError

    def delete_edge(self, id):
        raise NotImplementedError

    def incoming(self, node_id, reltypes=None):
        raise NotImplementedError

    def outgoing(self, node_id, reltypes=None):
        raise NotImplementedError

    # paths?

    def search_edge(self, **searchterms):
        raise NotImplementedError

    def fulltext_edge(self):
        raise NotImplementedError
