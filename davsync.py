__author__ = 'ifruchte'
# TODO: check utf-8 filenames with real WebDAV server, it didn't work well with PyWebDAV
# TODO: on windows the watchdog needs a patch to remove update on file access (in winapi_common.py)
#       to reduce copying files each time they are touched in file explorer

import time
import logging
import os.path
import urllib2
import json
import requests

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

config = dict(
    # directory on the machine that you want to sync.
    local_dir = "C:\\chunks\\",

    # address of the webdav server
    webdav_address = "http://127.0.0.1:8008",

    # directories and their target path in the webdav server
    directories = dict(
        CH1='/webdav/streaming/',
        CH2='/webdav/streaming/',
        TEST='/webdav/xstreaming/')
)

def read_config():
    config.update(json.load(open("config.json")))

read_config()

class ChunkHandler(FileSystemEventHandler):
    """Sync files to webdeb on each modification/deletion """

    headers = {'User-agent': config['User-agent'],
               'Cache-control': 'no-cache',
               'Depth': 'infinity',}

    def request_url(self, filename):
        ret = "%s%s" % (config['webdav_address'], self.build_url(filename))
        return  ret

    def build_url(self, filename):
        filename = filename.replace("\\", "/")
        filename = filename[len(config['directories']):]
        filename = filename.encode('utf-8')
        filename = urllib2.quote(filename)

        ret_val = filename
        for key, val in config['directories'].items():
            if filename.startswith(val):
                ret_val = "{0}{1}".format(val, filename)

        return ret_val

    def on_moved(self, event):
        super(ChunkHandler, self).on_moved(event)

        what = 'directory' if event.is_directory else 'file'
        logging.fatal("We don't expect chunk to move")
        logging.info("Moved %s: from %s to %s", what, event.src_path,
            event.dest_path)

    def on_created(self, event):
        super(ChunkHandler, self).on_created(event)

        what = 'directory' if event.is_directory else 'file'
        logging.info("Modified %s: %s", what, event.src_path)

        if event.is_directory:
            self.create_dir(event.src_path)

    def on_deleted(self, event):
        super(ChunkHandler, self).on_deleted(event)

        what = 'directory' if event.is_directory else 'file'
        logging.info("Deleted %s: %s", what, event.src_path)

        url = self.request_url(event.src_path)
        res = requests.delete(url, headers=self.headers)

    def on_modified(self, event):
        super(ChunkHandler, self).on_modified(event)

        what = 'directory' if event.is_directory else 'file'
        logging.info("Modified %s: %s", what, event.src_path)
        # if it's a file upload to webdav
        if not event.is_directory:
            try:
                res = self.put_file(event.src_path)

                # make the directory if needed
                if res.status_code == 424:
                    self.create_dir(os.path.dirname(event.src_path))
                    res = self.put_file(event.src_path)
            except IOError as ex:
                logging.info(ex)

    def put_file(self, filename):
        url = self.request_url(filename)
        with open(filename, "rb") as f:
            res = requests.put(url,
                data=f, headers=self.headers)
        return res

    def create_dir(self, dir_name):
        res = requests.request('mkcol', url= self.request_url(dir_name) )
        return res

class ObserverList(object):
    def __init__(self, root_dir ,dir_list):
        self.event_handler = ChunkHandler()
        self.observer_list = []
        self.dir_list = dir_list
        self.root_dir = root_dir

    def schedule(self):
        """ schedule all the observers """
        for directory in self.dir_list:
            o = Observer()
            o.schedule(self.event_handler,
                path=os.path.join(self.root_dir,directory),
                recursive=True)
            o.start()
            self.observer_list.append(o)

    def stop(self):
        """ stop all the observers """
        for o in self.observer_list:
            o.stop()

    def join(self):
        """ join on all the observers """
        for o in self.observer_list:
            o.join()

def main():
    """ main loop """

    observer_list = ObserverList(config['local_dir'], config['directories'].keys())
    observer_list.schedule()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        observer_list.stop()

    observer_list.join()

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    main()
