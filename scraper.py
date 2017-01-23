import requests
import queue
import threading
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.poolmanager import PoolManager
import ssl

from listcreator import ListCreator


class MyAdapter(HTTPAdapter):
    def init_poolmanager(self, connections, maxsize, block=False):
        self.poolmanager = PoolManager(
            num_pools=connections,
            maxsize=maxsize,
            block=block,
            ssl_version=ssl.PROTOCOL_TLSv1
        )

headers = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Encoding': 'gzip, deflate',
    'Content-Type': 'application/x-www-form-urlencoded',
    'Cache-Control': 'max-age=0',
    'Connection': 'keep-alive',
    'Content-Length': 109,
    'EnableViewState': False
}


class Downloader(object):

    def __init__(self):
        self.viewstate = None
        self.eventvalidation = None
        self.viewstategenerator = None

    def login(self, session, URL, filename, username, password):

        login = 'https://bdpconsole.betfair.com/login/data/login.aspx?ReturnUrl=%2fdatastore%2fdownloadfile.' \
                'aspx%3ffile%3d' + filename + '&file=' + filename
        params = {'file': filename}
        r = session.get(URL, params=params)

        data = r.text
        soup = BeautifulSoup(data, 'html.parser')
        self.viewstate = soup.select("#__VIEWSTATE")[0]['value']
        self.eventvalidation = soup.select("#__EVENTVALIDATION")[0]['value']
        self.viewstategenerator = soup.select('#__VIEWSTATEGENERATOR')[0]['value']

        payload = {
            '__EVENTTARGET': '',
            '__EVENTARGUMENT': '',
            '__VIEWSTATE': self.viewstate,
            '__VIEWSTATEGENERATOR': self.viewstategenerator,
            '__EVENTVALIDATION': self.eventvalidation,
            'txtUser': username,
            'txtPass': password,
            'btnAccept': 'Login'
        }
        params = {
            'ReturnUrl': URL[27:],
            'file': filename
        }
        r = session.post(login, data=payload, params=params, cookies=session.cookies, headers=headers)

        data = r.text
        soup = BeautifulSoup(data, 'html.parser')
        viewstate = soup.findAll("input", {"type": "hidden", "name": "__VIEWSTATE"})
        viewstategenerator = soup.findAll("input", {"type": "hidden", "name": "__VIEWSTATEGENERATOR"})
        self.viewstate = viewstate[0]['value']
        self.viewstategenerator = viewstategenerator[0]['value']
        print('login', self.viewstate, self.viewstategenerator)

    def get_location(self, session, URL):
        payload = {
            '__VIEWSTATE': self.viewstate,
            '__VIEWSTATEGENERATOR': self.viewstategenerator,
            'btnAccept': 'Download'
        }
        req = session.post(URL, data=payload, allow_redirects=False)
        return req.headers['Location']

    def download_file(self, session, loc):
        name = loc.split('/')[-1].split('.')[0]
        filetype = loc.split('.')[1]
        payload = {
            '__VIEWSTATE': self.viewstate,
            '__VIEWSTATEGENERATOR': self.viewstategenerator,
            'btnAccept': 'Download'
        }
        URL = 'http://data.betfair.com' + loc
        req = session.get(URL, data=payload, allow_redirects=False)

        if filetype in ['zip', 'rar']:
            with open('download/' + name + '.' + filetype, 'wb') as f:
                for chunk in req.iter_content(chunk_size=1024):
                    if chunk: # filter out keep-alive new chunks
                        f.write(chunk)
            return True
        else:
            print('error', loc, name, filetype)
            return False


def log(logQu):
    zm = 0
    while True:
        data = logQu.get()
        (URL, loc, filename) = data
        d.log_download(URL, loc, filename)
        zm += 1
        print(str(zm) + '/' + str(total))


def worker(s, fileQu):
    print('Starting worker thread')
    while not fileQu.empty():
        filename = fileQu.get()
        URL = 'http://data.betfair.com/datastore/downloadfile.aspx?file=' + filename

        loc = download.get_location(s, URL)
        ds = download.download_file(s, loc)
        file = loc.split('/')[-1]
        data = (URL, loc, filename)
        log_queue.put(data)


d = ListCreator('')
d.create_URL_list()
filenames = d.load_URLs()
total = len(filenames)
print('toDownload', len(filenames))

log_queue = queue.Queue()
file_queue = queue.Queue()

threading.Thread(target=log, args=(log_queue,), daemon=True).start()
for file in filenames:
    file_queue.put(file)


download = Downloader()

with requests.Session() as s:
    s.cookies.clear()
    s.mount('https://', MyAdapter())

    URL = 'http://data.betfair.com/datastore/downloadfile.aspx?file=' + filenames[0]
    download.login(s, URL, filenames[0], username='', password='')

    for i in range(0, 5):
        threading.Thread(target=worker, args=(s, file_queue,)).start()
