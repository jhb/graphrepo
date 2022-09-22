from store_in_whoosh import WhooshStore
ws = WhooshStore('whoosh')
print(ws.ix.doc_count())
print(ws.fulltext('bar'))