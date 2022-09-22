import os

import whoosh.fields as wf
from whoosh.index import create_in, open_dir
from whoosh.qparser import QueryParser
from whoosh.query import Term

from store import Store, Node


class WhooshStore(Store):
    supports_search = 1
    supports_fulltext = 1
    needs_undo = 1

    def setup_store(self):
        self.schema = wf.Schema(fulltext=wf.TEXT(stored=True),
                                id=wf.ID(stored=True, unique=True))
        if not os.path.exists('indexdir'):
            os.makedirs('indexdir', exist_ok=True)
            self.ix = create_in("indexdir", self.schema)
        else:
            self.ix = open_dir('indexdir')
        self.parser = QueryParser("fulltext", self.schema)
        self.undo_log = []

    def write(self, node):

        properties = self._enrich(node.id, node)
        with self.ix.writer() as writer:
            writer.add_document(fulltext=properties[self.fulltext_property],
                                id=node.id)

    def read(self, nodeid):
        """This is only for undo"""
        query = Term('id', nodeid)
        with self.ix.searcher() as searcher:
            result = searcher.search(query)
            doc = result[0]
            node = Node(doc['id'])
            node[self.fulltext_property]=doc['fulltext']
            return node

    def update(self, node, update_only=True):
        properties = self._enrich(node.id, node)
        with self.ix.writer() as writer:
            writer.update_document(fulltext=properties[self.fulltext_property],
                                   id=node.id)

    def delete(self, nodeid):
        self.ix.delete_by_term('id', nodeid)

    def fulltext(self, searchterm):
        query = self.parser.parse(searchterm)
        with self.ix.searcher() as searcher:
            result = searcher.search(query)
            return [doc['id'] for doc in result]
