
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
import re
from urllib2 import HTTPError
from scrapy.spidermiddlewares.httperror  import HttpError
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError


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

class I2PSpider(scrapy.Spider):

    """
    Spider that extract all the links contained in a website
    """
    name = "i2p_spider_1"
    links_extractor=I2PLinksExtractor()
    message_to_master={}#Cambiar nombre
    visited_links=[] #Links that we have DOWNLOADED the i2page
    verified_text_i2pages=[]  #Links we have send HEAD to test type
    not_text_i2pages=[]
    custom_settings = {
                'HTTPPROXY_ENABLED': True
            }


    def __init__(self, url_to_crawl=None, master_url=None , master_port=None, *args, **kwargs):

        """
        Create a new spider that extract all the links of a website
        :param url_to_crawl: link where the crawling process start
        :param master_url: adres where the master is listening
        :param master_port:  port where the master is listening
        """
        super(I2PSpider, self).__init__(*args, **kwargs)
        self.logger.info("Spider starting crawling of  %s", url_to_crawl)
        self.starting_i2p_link  = url_to_crawl

        self.master_url = master_url
        self.master_port  = int(master_port) #A;adir mas semillas
        self.source_i2p_website= "{0.scheme}://{0.netloc}".format(urlparse.urlsplit(url_to_crawl))

        self.white_list_types=["text/plain", "text/html","text/xml"]

        self.message_to_master["url_to_crawl"]=url_to_crawl
        self.message_to_master["urls"]={}

        dlevel=getattr(logging, "DEBUG")
        logging.basicConfig(level=dlevel)

        self.link_homogenizer=re.compile('^https?://www\..*$')#Match all that contains the standard http(s)://www.

        self.add_https_www_link=re.compile('^https://')#Match if a link start with https:// 
        self.add_http_www_link=re.compile('^http://')#Match if a link start with http://

        self.visited_webpages=0
        self.limit_visited_webpages=100#Limit of page to visit in a website

    def start_requests(self):
        """
        Start the crawling process
        """
        proxy = 'http://127.0.0.1:4444/'
        yield scrapy.Request(url=self.starting_i2p_link, callback=self.parse,errback = self.err, headers={'Accept':"text/plain, text/html, text/xml"}, method='HEAD',meta={'proxy': proxy} )


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
        self.logger.info("Arania [%s] Informacion extraida [%s]",self.message_to_master["url_to_crawl"],self.message_to_master)
        time_wait=0

        while(not conn):
            try:
                sock.connect( (self.master_url ,self.master_port) )
                self.logger.debug("Conexion con el maestro establecida")
                conn=True
            except:
                 self.logger.info("Arania [%s]: Waiting for the( server )",self.message_to_master["url_to_crawl"])
                 time.sleep(10)
                 time_wait+=1
                 if time_wait == 6: break

        self.logger.debug("Contactado por el servidor")
        self.send_information_collected_to_master(sock)

    def send_information_collected_to_master(self,sock):
        """
        Comunicate with the master with the porpuse of sending all the information that has extracted
        :param sock: established connection with the master.
        """

        message_to_master_to_json=json.dumps(self.message_to_master)

        self.logger.debug("Mensaje a maestro: %s", self.message_to_master)
        m=hashlib.sha256()
        m.update(message_to_master_to_json)
        hash_message=m.hexdigest()

        #First it send the hash and the start url
        introductory_information={"url_to_crawl":self.starting_i2p_link,  "hash": hash_message}
        introductory_information_to_json=json.dumps(introductory_information)
        sock.sendall(introductory_information_to_json)
        self.logger.debug("Enviado [%s]", introductory_information_to_json)

        received_message_from_master=sock.recv(1024)

        self.logger.debug("Recibido [%s]", received_message_from_master)
        if introductory_information_to_json == received_message_from_master:

            #If the introductory information is correctly recibed it sends all the information that has extracted
            sock.sendall(message_to_master_to_json)
        else:
            self.logger.debug("Enviado ERROR")
            sock.sendall("ERROR")
        sock.close()



    def i2p_links_request_type(self,links,response_url,source_type):
        """
        Return a list of request to the links that are contained in links, if they points to webpages that need to be checked its type
        :param links: list of links.
        :param response_url: the source url where the links have been extracted.
        :param source_type: the HTML tag where the links were contained.
        :return list of request
        """
        i2p_links_to_request_type=[]

        self.logger.debug("Procesando links extraidos de [%s]  tipo [%s]", response_url, source_type)
        self.logger.debug("Links extraidos son [%s]", links)
        for link in links:
            link_url=link.url
            self.logger.debug("Link extraido [%s]", link_url)
            destination_website= "{0.scheme}://{0.netloc}".format(urlparse.urlsplit(link_url))
            destination_website=self.homogenize_link(destination_website)
            link_cleaned="{0.scheme}://{0.netloc}{0.path}".format(urlparse.urlsplit(link_url))

            query="{0.query}".format(urlparse.urlsplit(link_url))
            if query:
                link_cleaned=link_cleaned+"?"+query
            unknown_type_and_not_visited = link_cleaned not in (self.verified_text_i2pages + self.not_text_i2pages) and link_cleaned not in self.visited_links
            if self.source_i2p_website == destination_website and unknown_type_and_not_visited :  
                #Si el link es del sitio y no se sabe si es texto o una imagen o que se visita utilizando HEAD para ver las cabeceras
                request=scrapy.Request(link_cleaned, callback=self.parse,errback = self.err,headers={'Accept':"text/plain, text/html, text/xml"}, method='HEAD')
                has_https=self.add_https_www_link.search(link_cleaned)
                #Si es https hay que utilizar el puerto 4445, si es http el 4444
                if has_https:
                    request.meta['proxy']='http://127.0.0.1:4445/'
                else:
                    request.meta['proxy']='http://127.0.0.1:4444/'
                i2p_links_to_request_type.append(request)
                self.logger.debug("Link %s added to verify_type",link_cleaned)
            elif self.source_i2p_website != destination_website:
                response_url_homogenized=self.homogenize_link(response_url)
                link_cleaned_homogenized=self.homogenize_link(link_cleaned)
                self.message_to_master["urls"][response_url_homogenized]["urls"][link_cleaned_homogenized]=source_type
                self.logger.debug("Link sitio externo de tipo [%s] extraido:de [%s] a [%s]", source_type,link_cleaned_homogenized, response_url_homogenized)
        return i2p_links_to_request_type

    def been_verified_response_is_text(self,response):
        """
        Check if the webpage that has responded  has been verified that is text
        :param response: response from a webpage.
        """
        link_cleaned="{0.scheme}://{0.netloc}{0.path}".format(urlparse.urlsplit(response.url))
        query="{0.query}".format(urlparse.urlsplit(response.url))
        if query:
            link_cleaned=link_cleaned+"?"+query
        safe_to_visit=False

        if link_cleaned not in self.verified_text_i2pages and link_cleaned  not in self.not_text_i2pages: 
            #Si no ha sido verificada entonces no esta contenida en las listas de paginas verificadas

            self.logger.info("Nuevo enlace del sitio [%s] aniadiado al mensaje", link_cleaned)
            self.add_to_message_master_visited_i2p_link(link_cleaned)
            try:
                webpage_type=response.headers['Content-Type']
                self.logger.info("Tipo de %s es %s",link_cleaned, webpage_type)
                for safe_type in self.white_list_types:
                    if safe_type in webpage_type:
                        self.verified_text_i2pages.append(link_cleaned)
                        safe_to_visit=True
                        self.logger.info("Tipo de %s es seguro",link_cleaned)
                if safe_to_visit == False:
                    self.not_text_i2pages.append(link_cleaned)
                    self.logger.info("Tipo de %s NO es seguro",link_cleaned)
            except:
                #If the reponse server dont want to tell us the type then we dont visit it
                self.not_text_i2pages.append(link_cleaned)
                self.logger.info("Tipo de %s NO nos lo dice",link_cleaned)


    def process_links_not_visit(self,links,response_url ,source_type):
        """
        Process the links that are not going to  be visited
        :param links: list of links
        :param response_url: the url of the webpage where the links come from
        :param source_type:  from which HTML tag the links have been extracted 
        """

        self.logger.debug("Procesando links no visitar extraidos de [%s] tipo [%s]", response_url, source_type)
        for link in links:
            link_url=link.url
            destination_website= "{0.scheme}://{0.netloc}".format(urlparse.urlsplit(link_url))
            destination_website=self.homogenize_link(destination_website)
            link_cleaned="{0.scheme}://{0.netloc}{0.path}".format(urlparse.urlsplit(link_url))
            query="{0.query}".format(urlparse.urlsplit(link_url))
            if query:
                link_cleaned=link_cleaned+"?"+query
            if self.source_i2p_website != destination_website:
                response_url_homogenized=self.homogenize_link(response_url)
                link_cleaned_homogenized=self.homogenize_link(link_cleaned)

                self.logger.debug("Sitio externo no ha visitar: [%s]" ,destination_website)
                self.message_to_master["urls"][response_url_homogenized]["urls"][link_cleaned_homogenized]=source_type
                self.logger.debug("Links no ha visitar, aniadido a la lista de enlaces de [%s] link [%s] tipo [%s]", response_url, link_cleaned,source_type)

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


    def homogenize_link(self, link): 
        """
        Format a link to the a standard format: http://www.
        :param link: the link that is going to be formated.
        :return the links formated
        """
        has_www=self.link_homogenizer.search(link)
        if not has_www:
            self.logger.debug("[%s] dont have www]",link)

            if self.add_http_www_link.search(link):
                link=self.add_http_www_link.sub('http://www.',link,1)
                self.logger.debug("[%s] Added www to http link]",link)

            else:
                link=self.add_https_www_link.sub('https://www.',link,1)
                self.logger.debug("[%s] Added www to https link]",link)

        has_https=self.add_https_www_link.search(link)##No hay problema porque los links tambien estan homogenizados
        if has_https:
                self.logger.debug("[%s] contains https",link)
                link=self.add_https_www_link.sub('http://',link,1)
                self.logger.debug("[%s] Replaced https for http link]",link)

        

        return link

    def add_to_message_master_visited_i2p_link(self, i2p_link):
        """
        Add an i2p link to the list of visited links in the message that is going to be sent
        to the master
        :param i2p_link: the links that is going to be added to the message.
        """
        i2p_link_homogenized=self.homogenize_link(i2p_link)
        self.logger.debug("[%s] aniadida al mensaje del maestro", i2p_link_homogenized)
        self.message_to_master["urls"][i2p_link_homogenized]={}
        self.message_to_master["urls"][i2p_link_homogenized]["status"]="ok"
        self.message_to_master["urls"][i2p_link_homogenized]["urls"]={}


    def get_list_requests(self, check_type_area, check_type_a):
        """
        Join two list in one list
        :param check_type_area: list of requests of links that were extracted from the area tag
        :param check_type_a: list of requests of links that were extracted from the area tag
        :return  the union of the two list of requests
        """
        i2p_links_visit_from_response=[]
        if check_type_area:
            i2p_links_visit_from_response=check_type_area
        if check_type_a:
            i2p_links_visit_from_response=i2p_links_visit_from_response + check_type_a

        return i2p_links_visit_from_response

    def process_response(self,response):
        """
        Process a response from a webpage to extract the necesary info to continue with the crawling process
        :param response: response from a webpage.
        :return a list of request to be sent 
        """

        if "status" not in self.message_to_master: #If we receive at least one response, we add to the message to master that we were able to contact the start url
            self.message_to_master["status"] = "ok"
        #The visit to an i2page consist in two steps: Verfiy his type using HEAD then get it using GET

        self.logger.debug("Entrando a procesar respuesta")

        self.been_verified_response_is_text(response)

        #Now the webpage that has responed is for sure contained in verified_text_i2pages or in not_text_i2pages
        i2p_links_visit_from_response=[]

        if response.request.method == 'GET':
            self.visited_webpages=self.visited_webpages+1#Only page we download are counted, because they are html,txt and not multimedia,zip,torrent type

            self.logger.debug("Entrando a procesar respuesta a %s solicitada utilizando GET", response.url)
            self.logger.debug("Ya se ha verificado que la respuesta es texto")
            self.logger.debug("Respuesta [%s] headers  [%s]",response.url ,response.headers)

            links=self.links_extractor.get_links(response)

            self.logger.debug("Se han extraido los links de la respuesta")


            i2p_links_request_type_area =[]
            i2p_links_request_type_a=[]
            if self.visited_webpages <self.limit_visited_webpages:
                i2p_links_request_type_area=self.i2p_links_request_type(links["area"],response.url,"area")
                i2p_links_request_type_a=self.i2p_links_request_type(links["a"],response.url,"a")

            #En este punto ya tenemos almacenados en listas peticiones a los enlaces que no se ha verificado si son texto

            self.logger.debug("Se ha acabado la parte de solicitar el request type")
            self.logger.debug("Verificar tipo a [%s] \n area [%s]", i2p_links_request_type_a, i2p_links_request_type_area)

            i2p_links_visit_from_response=self.get_list_requests(i2p_links_request_type_area,i2p_links_request_type_a)


            self.logger.debug("Empieza la parte de procesar links no ha visitar")
            self.process_links_not_visit(links["link"],response.url,"link")
            self.process_links_not_visit(links["img"],response.url,"img")
            self.process_links_not_visit(links["script"],response.url,"script")
            self.process_links_not_visit(links["audio"],response.url,"audio")

            self.logger.debug("Finaliza la parte de links no ha visitar")
            
        elif response.request.method == 'HEAD':
            self.logger.debug("Entrando a procesar respuesta a %s solicitada utilizando HEAD",response.url)
            link_cleaned="{0.scheme}://{0.netloc}{0.path}".format(urlparse.urlsplit(response.url))
            query="{0.query}".format(urlparse.urlsplit(response.url))
            if query:
                link_cleaned=link_cleaned+"?"+query
            if link_cleaned not in self.visited_links and link_cleaned in self.verified_text_i2pages:
                #Cuando hemos verificado que el link es texto se descarga la pagina web asociada
                self.visited_links.append(link_cleaned)

                request=scrapy.Request(link_cleaned, callback=self.parse,errback = self.err, headers={'Accept':"text/plain, text/html, text/xml"})
                has_https=self.add_https_www_link.search(link_cleaned)
                if has_https:
                    request.meta['proxy']='http://127.0.0.1:4445/'
                else:
                    request.meta['proxy']='http://127.0.0.1:4444/'
                i2p_links_visit_from_response.append(request)

        return i2p_links_visit_from_response

    #Reporta los errores que se producen en caso de que una peticion falle
    def err(self,failure):
        """
        Method that is called when a sent request have failed
        :param failure: object that contains information of the request that have failed.
        """
            # you can get the response

	if failure.check(HttpError):
            response = failure.value.response
        elif failure.check(DNSLookupError):
            response = failure.request
        if response:
            response_website= "{0.scheme}://{0.netloc}".format(urlparse.urlsplit(response.url))
            response_website=self.homogenize_link(response_website)
            if self.source_i2p_website == response_website:

                response_cleaned="{0.scheme}://{0.netloc}{0.path}".format(urlparse.urlsplit(response.url))
                query="{0.query}".format(urlparse.urlsplit(response.url))
                if query:
                    response_cleaned=response_cleaned+"?"+query

                response_url_homogenized=self.homogenize_link(response_cleaned)
                self.logger.error('HttpError  %s   value %s  when visiting %s',failure, response_url_homogenized, response.url)
                self.message_to_master["urls"][response_url_homogenized]={"status" : "error"}
                self.logger.error("Arania [%s] :error en request [%s]", self.message_to_master["url_to_crawl"],repr(failure))

    def parse(self, response):
        """
        Method that is called when a webpage respons to a request that we we have sent
        It deploys all the new request to send from  links contained in the webpage
        """
        response_website= "{0.scheme}://{0.netloc}".format(urlparse.urlsplit(response.url))
        response_website=self.homogenize_link(response_website)
        if self.source_i2p_website == response_website:
            request_list=self.process_response(response)
            if request_list:
                for request in request_list:
                    self.logger.info("Enviando request a %s utilizando %s",request, request.method)

                    yield request
        else:
            #Esto suele ocurrir cuando se envia la peticion a un enlace del sitio origen pero este nos redirige a otro
            #Ahora mismo no se soporta analisis de redirects
            self.logger.info("La respuesta recibida por la arania [%s] no proviene del sitio a crawlear", response)
