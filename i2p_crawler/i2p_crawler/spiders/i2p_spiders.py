
import scrapy
from scrapy.linkextractors import LinkExtractor
import urlparse
import json
import sys
import logging
import socket
import time

class LinksExtractor():

    extractor_area= LinkExtractor(tags=('area'),attrs=('src','href'), deny_extensions=())
    extractor_a= LinkExtractor(tags=('a'),attrs=('src','href'), deny_extensions=())
    extractor_link= LinkExtractor(tags=('link'),attrs=('src','href'), deny_extensions=())
    extractor_img= LinkExtractor(tags=('img'),attrs=('src','href'), deny_extensions=())
    extractor_script= LinkExtractor(tags=('script'),attrs=('src','href'), deny_extensions=())
    extractor_audio= LinkExtractor(tags=('audio'),attrs=('src','href'), deny_extensions=())

    def get_links(self,response):
        links={}
        links["a"]=self.extractor_a.extract_links(response)
        links["area"]=self.extractor_area.extract_links(response)
        links["link"]=self.extractor_link.extract_links(response)
        links["img"]=self.extractor_img.extract_links(response)
        links["script"]=self.extractor_script.extract_links(response)
        links["audio"]=self.extractor_audio.extract_links(response)
        return links

class QuotesSpider1(scrapy.Spider):

    name = "i2p_spider_1"
    links_extractor=LinksExtractor()
    data={}
    visited_links=[]

    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
    
    def __init__(self, url_to_crawl=None, *args, **kwargs):
        super(QuotesSpider1, self).__init__(*args, **kwargs)
        self.logger.info("URL to crawl %s", url_to_crawl)
        #Obtenemos el dominio de la url
        self.start_urls  = [url_to_crawl] #A;adir mas semillas
        self.source_domain= "{0.scheme}://{0.netloc}/".format(urlparse.urlsplit(url_to_crawl))
        self.white_list_types=["text/plain", "text/html","text/xml"]

        self.data["url_to_crawl"]=url_to_crawl
        self.data["urls"]={}

    #Metodo llamado cuando se va a finalizar la ejecucion de la arania
    def closed(self, reason):
        #Si no existe status implica que nunca se ha entrado a procesar una respuesta lo cual implica que ha fallado la peticion a la url inicial
        if "status" not in self.data:
            self.data["status"] = "error"
        conn=False
        sock = socket.socket( socket.AF_INET, socket.SOCK_STREAM )
        self.logger.info("Informacion extraida [%s]",json.dumps(self.data))
        while(not conn):
            self.logger.error("Socket %s", sock)
            self.logger.error("Conn %s", conn)
            try:
                sock.connect( ('127.0.0.1', 55616) )
                conn=True
            except:
                 self.logger.debug("Waiting for the( server )")
                 print 'Waiting for the( server'
                 self.logger.error("Waiting for the( server")
                 time.sleep(10)

        result=sock.sendall(json.dumps(self.data))

        sock.flush()
       # sock.send("$#END")
        data=recv(20)
        while data != "$#OK":
            sock.sendall(json.dumps(self.extracted_urls))
            data=recv(20)
        sock.close()

    #Las unicas urls que entran aqui son las que queremos ver si contienen mas urls

    def process_links_to_visit(self,links,response_url ,source_type):
        request_list=[]
        for link in links:
            destination_domain= "{0.scheme}://{0.netloc}/".format(urlparse.urlsplit(link.url)) 
            link_cleaned="{0.scheme}://{0.netloc}{0.path}".format(urlparse.urlsplit(link.url))
            if self.source_domain == destination_domain and link_cleaned not in self.visited_links:  
                self.visited_links.append(link_cleaned)
                request=scrapy.Request(link_cleaned, callback=self.parse,errback = self.err, dont_filter=True)
                request_list.append(request)
            elif self.source_domain != destination_domain:
                self.logger.debug("Link tipo [%s] estraido: [%s]", source_type,link_cleaned)
                self.data["urls"][response_url]["urls"][link_cleaned]=source_type

        return request_list

    def process_links_not_visit(self,links,response_url ,source_type):
        for link in links:
            destination_domain= "{0.scheme}://{0.netloc}/".format(urlparse.urlsplit(link.url)) 
            link_cleaned="{0.scheme}://{0.netloc}{0.path}".format(urlparse.urlsplit(link.url))
            if self.source_domain != destination_domain:
                self.data["urls"][response_url]["urls"][link_cleaned]=source_type
                self.logger.debug("Link tipo [%s] estraido: [%s]", source_type,link_cleaned)

    def process_links(self,response):
        if "status" not in self.data:
            self.data["status"] = "ok"
        self.data["urls"][response.url]={} 
        self.data["urls"][response.url]["status"]="ok"
        self.data["urls"][response.url]["urls"]={}
        request_list=[]
        self.logger.debug("Headers %s", response.headers)
        url_type=response.headers['Content-Type']
        safe_to_scrape=False
        for safe_type in self.white_list_types:
            if safe_type in url_type:
                safe_to_scrape=True
        if safe_to_scrape:
            links=self.links_extractor.get_links(response)
        request_list=request_list + self.process_links_to_visit(links["area"],response.url,"area") 
        request_list=request_list + self.process_links_to_visit(links["a"],response.url,"a") 

        self.process_links_not_visit(links["link"],response.url,"link")
        self.process_links_not_visit(links["img"],response.url,"img")
        self.process_links_not_visit(links["script"],response.url,"script")
        self.process_links_not_visit(links["audio"],response.url,"audio")

        return request_list

    #Reporta los errores que se producen en caso de que una peticion falle
    def err(self,failure):

        self.data["urls"][failure.request.url]={"status" : "error"}
        self.logger.error("Error en request [%s]", repr(failure))   

    def parse(self, response):
        request_list=self.process_links(response)
        if request_list:
            for request in request_list:
                yield request

