import glob
import os
import shutil

from store import Store, Node
import mimetypes

import yaml

class FilesystemStore(Store):
    supports_properties = 0
    supports_blobs = 1
    needs_undo = 1
    dirname = 'fsstorage'
    content_filename_property = '___content_filename___'

    def _clear(self):
        for root, dirs, files in os.walk(self.dirname):
            for f in files:
                os.unlink(os.path.join(root, f))
            for d in dirs:
                shutil.rmtree(os.path.join(root, d))

    def setup_store(self):
        self.dirname = 'fsstorage'
        os.makedirs(self.dirname, exist_ok=True)
        self.undo_log = []

    def write(self, node):

        if node.content is not None:
            if self.type_property in node:
                blob_extension = mimetypes.guess_extension(node[self.type_property])
            else:
                blob_extension = '.bin'
            content_filename = f'{node.id}{blob_extension}'
            node = node.clone()
            node[self.content_filename_property]= content_filename
            open(os.path.join(self.dirname,content_filename),'w').write(node.content)
        yaml_filename = f'{node.id}.yaml'
        yaml_content = yaml.safe_dump(dict(node))
        open(os.path.join(self.dirname,yaml_filename),'w').write(yaml_content)

    def read(self, nodeid, clean=True):
        yaml_content = open(os.path.join(self.dirname, f'{nodeid}.yaml'))
        node = Node(nodeid, **yaml.safe_load(yaml_content))
        if self.content_filename_property in node:
            location = node[self.content_filename_property]
            node.content = open(os.path.join(self.dirname,location)).read()
        if clean and self.content_filename_property in node:
            del(node[self.content_filename_property])
        return node

    def update(self, node, update_only=True):

        if update_only:
            update_node = self.read(node.id, clean=False)
            update_node.update(node)
            node = update_node
        self.delete(node.id)
        self.write(node)

    def delete(self, nodeid):
        pattern = os.path.join(self.dirname, f'{nodeid}**')
        filelist = glob.glob(pattern)
        for loc in filelist:
            os.remove(loc)