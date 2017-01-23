from bs4 import BeautifulSoup
import requests
import datetime
import csv


class ListCreator(object):

    def __init__(self, directory):
        self.directory = directory
        self.downloaded_list = []
        self.to_download_list = []

    def create_URL_list(self):
        self.downloaded_list = []
        with open(self.directory + 'log.txt', 'r') as file:
            reader = csv.reader(file)
            next(reader, None)
            for row in reader:
                self.downloaded_list.append(row[2])
        print('AlreadyDownloaded', len(self.downloaded_list))
        return self.downloaded_list

    def load_URLs(self):
        self.to_download_list = []
        date_home_page = 'http://data.betfair.com/#null'
        r = requests.get(date_home_page)
        data = r.text
        soup = BeautifulSoup(data, 'html.parser')
        for a in soup.findAll('a'):
            if '#null' in a['href']:
                a = str(a)
                url = a[a.index("(") + 1:a.rindex(")")].split(',')[0].replace("'", '')
                filename = url[33::]
                data = ['http://data.betfair.com/' + url, filename]
                if filename not in self.downloaded_list and len(filename) == 32:
                    self.to_download_list.append(filename)
        return self.to_download_list

    def log_download(self, URL, file, filename):
        with open(self.directory + 'log.txt', 'a', newline="\n") as f:
            writer = csv.writer(f)
            writer.writerow((URL, file, filename, datetime.datetime.now()))
