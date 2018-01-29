
import scrapy
from scrapy.linkextractors import LinkExtractor
import urlparse
import json
import sys
import logging

class QuotesSpider1(scrapy.Spider):

    name = "i2p_spider_1"
    extractor_links= LinkExtractor(tags=('a','area','link'),attrs=('src','href'), deny_extensions=())
    extractor_resources= LinkExtractor(tags=('img','script','audio'),attrs=('src','href'), deny_extensions=())

    visited_links=[]

    def __init__(self, url_to_crawl=None):
        self.extracted_urls=[]
        #Obtenemos el dominio de la url
        self.start_urls  = [url_to_crawl] #A;adir mas semillas
        self.source_domain= "{0.scheme}://{0.netloc}/".format(urlparse.urlsplit(url_to_crawl))
        self.white_list_types=["text/plain", "text/html","text/xml"]

    def closed(self, reason):
        self.logger.error("Links extraidos=[%s]", json.dumps(self.extracted_urls))
        fh = open("spider1.txt","w")
        fh.write(json.dumps(self.extracted_urls))
        fh.close()


    #Las unicas urls que entran aqui son las que queremos ver si contienen mas urls
    def extract_links(self,response):
        
        self.logger.error("Headers %s", response.headers)
        url_type=response.headers['Content-Type']
        self.logger.error("Headers %s", url_type)
        safe_to_scrape=False
        for safe_type in self.white_list_types:
            if safe_type in url_type:
                safe_to_scrape=True
        if safe_to_scrape:
            links_to_visit=self.extractor_links.extract_links(response)
            request_list=[]
            D={}
            D[response.request.url]=[]
            for link in links_to_visit:
                destination_domain= "{0.scheme}://{0.netloc}/".format(urlparse.urlsplit(link.url)) 
                link_cleaned="{0.scheme}://{0.netloc}{0.path}".format(urlparse.urlsplit(link.url))
                if self.source_domain == destination_domain and link_cleaned not in self.visited_links:  
                    self.visited_links.append(link_cleaned)
                    request=scrapy.Request(link_cleaned, callback=self.parse,errback = self.err, dont_filter=True)
                    request_list.append(request)
                elif self.source_domain != destination_domain:
                    D[response.request.url].append(link_cleaned)

            links_not_visit=self.extractor_resources.extract_links(response)
            for link_resources in links_not_visit:
                destination_domain= "{0.scheme}://{0.netloc}/".format(urlparse.urlsplit(link_resources.url)) 
                link_cleaned="{0.scheme}://{0.netloc}{0.path}".format(urlparse.urlsplit(link_resources.url))
                if self.source_domain != destination_domain:
                    D[response.request.url].append(link_cleaned)

            self.extracted_urls.append(D)
            return request_list

    #Reporta los errores que se producen en caso de que una peticion falle
    def err(self,failure):
        self.logger.error(repr(failure))

    def parse(self, response):
        request_list=self.extract_links(response)
        if request_list:
            for request in request_list:
                yield request

