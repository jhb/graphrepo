import glob
import os
import shutil

from store import Store
import mimetypes

import yaml

class FilesystemStore(Store):
    supports_properties = 1
    supports_blobs = 1
    needs_undo = 1
    dirname = 'fsstorage'
    marker = 'in_file::'

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

    def create(self, nodeid=None, properties=None):
        if properties is None:
            properties = {}

        if self.blob_content_type in properties and self.blob_content_data in properties:
            blob_extension = mimetypes.guess_extension(properties[self.blob_content_type])
            blob_filename = f'{nodeid}{blob_extension}'
            data = properties[self.blob_content_data]
            properties[self.blob_content_data]=f'{self.marker}{blob_filename}'
            open(os.path.join(self.dirname,blob_filename),'w').write(data)
        yaml_filename = f'{nodeid}.yaml'
        yaml_content = yaml.safe_dump(properties)
        open(os.path.join(self.dirname,yaml_filename),'w').write(yaml_content)

    def read(self, nodeid):
        yaml_content = open(os.path.join(self.dirname, f'{nodeid}.yaml'))
        properties = yaml.safe_load(yaml_content)
        if self.blob_content_data in properties and self.blob_content_type in properties:
            location = properties[self.blob_content_data]
            filename = location.split(self.marker)[1]
            properties[self.blob_content_data] = open(filename).read()
        return properties

    def update(self, nodeid, update=None, properties=None):
        self.delete(nodeid)
        self.create(nodeid,properties=properties)

    def delete(self, nodeid):
        pattern = os.path.join(self.dirname, f'{nodeid}**')
        filelist = glob.glob(pattern)
        for loc in filelist:
            os.remove(loc)