
import scrapy
from scrapy.linkextractors import LinkExtractor
import urlparse
import json
import sys
import logging
import socket
import time
import os
import hashlib

class I2PLinksExtractor():
    """
    Class responsible of extracting links from a scrappy response
    """
    extractor_area= LinkExtractor(tags=('area'),attrs=('src','href'), deny_extensions=())
    extractor_a= LinkExtractor(tags=('a'),attrs=('src','href'), deny_extensions=())
    extractor_link= LinkExtractor(tags=('link'),attrs=('src','href'), deny_extensions=())
    extractor_img= LinkExtractor(tags=('img'),attrs=('src','href'), deny_extensions=())
    extractor_script= LinkExtractor(tags=('script'),attrs=('src','href'), deny_extensions=())
    extractor_audio= LinkExtractor(tags=('audio'),attrs=('src','href'), deny_extensions=())

    def get_links(self,response):
        """
        Extract links from a scrappy response 
        :param response: scrappy response from where the links are going to be extracted
        :return: list containing the "a", "area", "link","img","script","audio" links from the response
        """
        list_extracted_links={}
        list_extracted_links["a"]=self.extractor_a.extract_links(response)
        list_extracted_links["area"]=self.extractor_area.extract_links(response)
        list_extracted_links["link"]=self.extractor_link.extract_links(response)
        list_extracted_links["img"]=self.extractor_img.extract_links(response)
        list_extracted_links["script"]=self.extractor_script.extract_links(response)
        list_extracted_links["audio"]=self.extractor_audio.extract_links(response)
        return list_extracted_links

class I2PSpider(scrapy.Spider):#CAMBIAR

    """
    Spider that extract all the links contained in a website
    """
    name = "i2p_spider_1"
    links_extractor=I2PLinksExtractor()
    message_to_master={}#Cambiar nombre
    visited_links=[]
    custom_settings = {
                'HTTPPROXY_ENABLED': True
            }
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
    
    def __init__(self, url_to_crawl=None, master_url=None , master_port=None, *args, **kwargs):

        """
        Create a new spider that extract all the links of a website 
        :param url_to_crawl: link where the crawling process start
        :param master_url: adres where the master is listening
        :param master_port:  port where the master is listening
        """
        super(I2PSpider, self).__init__(*args, **kwargs)
        self.logger.info("URL to crawl %s", url_to_crawl)
        #Obtenemos el dominio de la url
        self.starting_i2p_link  = [url_to_crawl] #A;adir mas semillas
        
        self.master_url = master_url #Cabiar a IP
        self.master_port  = int(master_port) #A;adir mas semillas
        self.source_i2p_website= "{0.scheme}://{0.netloc}".format(urlparse.urlsplit(url_to_crawl))
        self.white_list_types=["text/plain", "text/html","text/xml"]

        self.message_to_master["url_to_crawl"]=url_to_crawl
        self.message_to_master["urls"]={}

    def start_requests(self):
        """
        Start the crawling process
        """
        proxy = 'http://127.0.0.1:4444/'
        #yield scrapy.Request(url=self.starting_i2p_link, callback=self.parse, meta={'proxy': proxy})
        yield scrapy.Request(url='https://www.example.com', callback=self.parse, meta={'proxy': proxy})

    #Metodo llamado cuando se va a finalizar la ejecucion de la arania

    def closed(self, reason):
        """
        Called when the spider have ended the crawling process. It sends all the data extracted to the master.
        :param reason: reason why the process have endend 
        """
        #Si no existe status implica que nunca se ha entrado a procesar una respuesta lo cual implica que ha fallado la peticion a la url inicial
        if "status" not in self.message_to_master:
            self.message_to_master["status"] = "error"

        conn=False
        sock = socket.socket( socket.AF_INET, socket.SOCK_STREAM )
        self.logger.info("Arania [%s]Informacion extraida [%s]",self.message_to_master["url_to_crawl"],self.message_to_master)
        time_wait=0

        while(not conn):
            self.logger.debug("HOST [%s]: PORT %s )",self.master_url, self.master_port)
            try:
                sock.connect( (self.master_url ,self.master_port) )
                self.logger.info("Conexion con el maestro establecida")
                conn=True
            except:
                 self.logger.debug("Arania [%s]: Waiting for the( server )",self.message_to_master["url_to_crawl"])
                 time.sleep(10)
                 time_wait+=1
                 if time_wait == 6: break

        self.logger.debug("Contactado por el servidor")
        self.send_information_collected_to_master(sock)

    def send_information_collected_to_master(self,sock):

        message_to_master_to_json=json.dumps(self.message_to_master)
        size=len(self.message_to_master)

        m=hashlib.sha256()
        m.update(message_to_master_to_json)
        hash_message=m.hexdigest()
        introductory_information={"url_to_crawl":self.starting_i2p_link[0], "data_size":size, "hash": hash_message}
        introductory_information_to_json=json.dumps(introductory_information)
        sock.sendall(introductory_information_to_json)
        self.logger.debug("Enviado [%s]", introductory_information_to_json)

        received_message_from_master=sock.recv(1024)

        self.logger.debug("Recibido [%s]", received_message_from_master)
        if introductory_information_to_json == received_message_from_master:
            sock.sendall("OK")
            self.logger.debug("Enviado OK")
            sock.sendall(message_to_master_to_json)
        else:
            self.logger.debug("Enviado ERROR")
            sock.sendall("ERROR")
        sock.close()

    #Las unicas urls que entran aqui son las que queremos ver si contienen mas urls

    def process_links_to_visit(self,links,response_url ,source_type):
        """
        Process the links that is going to visit
        :param links: list of links
        :param response_url: the url where the links come from
        :param source_type:  from which element response_url have been extracted
        :return: list of links from source_i2p_website not visited
        """

        i2p_links_to_visit=[]
        for link in links:
            destination_website= "{0.scheme}://{0.netloc}".format(urlparse.urlsplit(link.url)) 
            link_cleaned="{0.scheme}://{0.netloc}{0.path}".format(urlparse.urlsplit(link.url))
            if self.source_i2p_website == destination_website and link_cleaned not in self.visited_links:  
                self.visited_links.append(link_cleaned)
                request=scrapy.Request(link_cleaned, callback=self.parse,errback = self.err, dont_filter=True)
                request.meta['proxy']='http://127.0.0.1:4444/'
                i2p_links_to_visit.append(request)
            elif self.source_i2p_website != destination_website:
                self.message_to_master["urls"][response_url]["urls"][link_cleaned]=source_type
                self.logger.debug("Arania [%s]: link tipo [%s] estraido: [%s]",self.message_to_master["url_to_crawl"], source_type,link_cleaned)

        return i2p_links_to_visit

    def process_links_not_visit(self,links,response_url ,source_type):
        """
        Process the links that is not going to visit
        :param links: list of links
        :param response_url: the url where the links come from
        :param source_type:  from which element response_url have been extracted
        """
        for link in links:
            destination_website= "{0.scheme}://{0.netloc}/".format(urlparse.urlsplit(link.url)) 
            link_cleaned="{0.scheme}://{0.netloc}{0.path}".format(urlparse.urlsplit(link.url))
            if self.source_i2p_website != destination_website:
                self.message_to_master["urls"][response_url]["urls"][link_cleaned]=source_type
                self.logger.debug("Arania [%s]: link tipo [%s] estraido: [%s]",self.message_to_master["url_to_crawl"], source_type,link_cleaned)

    def check_if_valid_webpage(self, response):
        """
        Check if the visited webpage is an html, xml, txt, plain text webpage 
        :param response: response from a website.
        :return True if the visited webpage is of html, xml, txt type, false otherwise
        """
        webpage_type=response.headers['Content-Type']
        safe_to_scrape=False
        for safe_type in self.white_list_types:
            if safe_type in webpage_type:
                safe_to_scrape=True
        return safe_to_scrape


    def add_to_message_master_visited_i2p_link(self, i2p_link):
        self.message_to_master["urls"][i2p_link]={} 
        self.message_to_master["urls"][i2p_link]["status"]="ok"
        self.message_to_master["urls"][i2p_link]["urls"]={}

    def process_response(self,response):
        """
        Process the links from a response of scrapy
        :param response: response from a website.
        :return return a list of links from response to visit
        """

        if "status" not in self.message_to_master: #We add to the message to master that we were able to contact the start url
            self.message_to_master["status"] = "ok"
        self.add_to_message_master_visited_i2p_link(response.url)

        i2p_links_visit_from_response=[]

        self.logger.debug("Arania [%s]: headers %s",self.message_to_master["url_to_crawl"] ,response.headers)

        safe_to_scrape=self.check_if_valid_webpage(response)
        if safe_to_scrape:
            links=self.links_extractor.get_links(response)
            i2p_links_visit_from_response=i2p_links_visit_from_response + self.process_links_to_visit(links["area"],response.url,"area") + self.process_links_to_visit(links["a"],response.url,"a")
            self.process_links_not_visit(links["link"],response.url,"link")
            self.process_links_not_visit(links["img"],response.url,"img")
            self.process_links_not_visit(links["script"],response.url,"script")
            self.process_links_not_visit(links["audio"],response.url,"audio")

        return i2p_links_visit_from_response

    #Reporta los errores que se producen en caso de que una peticion falle
    def err(self,failure):

        self.message_to_master["urls"][failure.request.url]={"status" : "error"}
        self.logger.error("Arania [%s] :error en request [%s]", self.message_to_master["url_to_crawl"],repr(failure))   

    def parse(self, response):
        request_list=self.process_response(response)
        if request_list:
            for request in request_list:
                yield request

