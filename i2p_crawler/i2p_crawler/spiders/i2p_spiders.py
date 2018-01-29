
import scrapy
from scrapy.linkextractors import LinkExtractor
import urlparse
import json
import sys
import logging

class QuotesSpider1(scrapy.Spider):

    name = "i2p_spider_1"
    extractor= LinkExtractor(tags=('a','area','link','img','script'),attrs=('src','href'), deny_extensions=())
    visited_links=[]

    def __init__(self, url_to_crawl=None):
        extracted_urls=[]
        #Obtenemos el dominio de la url
        url=url_to_crawl
        source_domain= "{0.scheme}://{0.netloc}/".format(urlparse.urlsplit(url))

    def extract_links(self,response):
        links=self.extractor.extract_links(response)
        for link in links:
            D={}
            destination_domain= "{0.scheme}://{0.netloc}/".format(urlparse.urlsplit(link.url)) 
            link_cleaned="{0.scheme}://{0.netloc}{0.path}".format(urlparse.urlsplit(link.url))

            if source_domain == destination_domain and link_cleaned not in self.visited_links:  
                visited_links.append(link_cleaned)
                request=scrapy.Request(link_cleaned, callback=self.parse,errback = self.err, dont_filter=True)
                request_list.append(request)
            elif source_domain != destination_domain:
                D[response.request.url].append(link_cleaned)
        extracted_urls.append(D)
        return request_list

    #Reporta los errores que se producen en caso de que una peticion falle
    def err(self,failure):
        self.logger.error(repr(failure))

    def parse(self, response):
        request_list=extract_links(response)
        if len(request_list) == 0:
            #TODO: Pasarle todos los links al padre
            self.logger.warning('Links extraidos %s', json.dumps(extracted_urls))
            
        else:
            for request in request_list:
                    yield request

