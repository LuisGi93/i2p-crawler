#Basic module do all the job of jumping from one link to another
import scrapy
#Helper from scrapy, module specialized in extracing links from an scrapy response
from scrapy.linkextractors import LinkExtractor
#Only needed if we want to monitor the activity of our spider manager
import logging
import os
#If we want to dissasamble an url into parts (protocol, domain, extension ...)
import urlparse#Used for the communication between the spider manager and the spiders
import socket

import requests
#To build and decipher strings in json format 
import json
#To verify the integrity of the data sent by the spider
import hashlib
#In case of error, to sleep while the spider die
import time

from i2p_webpage_manager import I2PWebpageManager 

dlevel=getattr(logging, "DEBUG")
open('../master.log', 'w').close()
logging.basicConfig(filename='../master.log',level=dlevel)




class SpiderManager():
    
    def __init__(self): 
        self.sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        self.sock.bind((os.environ["URL_MASTER"], int(os.environ["MASTER_PORT"]) ))
        self.sock.listen(5)
        logging.debug('Escuando en [%s] puerto [%d]  ', os.environ["URL_MASTER"], int(os.environ["MASTER_PORT"])) 
        self.i2p_webpage_manager=I2PWebpageManager()
        self.spiders={"num_spiders" : 0}

    def verify_hash(self,expected_hash, message):
        message_sha256_calculator=hashlib.sha256()
        message_sha256_calculator.update(message)
        hash_message=message_sha256_calculator.hexdigest()
        if expected_hash== hash_message:
            return True
        else:
            return False

    def process_conection_with_spider(self,conection_with_spider):
        introductory_information_from_spider=self.get_introductory_information_from_spider(conection_with_spider)
        if introductory_information_from_spider:
            spider_id=introductory_information_from_spider["url_to_crawl"]
            expected_hash_extraction_information=introductory_information_from_spider["hash"]
            size_message_going_be_recibed=int(introductory_information_from_spider["data_size"])

            logging.info('Contactado por [%s] va a transmitir [%d]  ', spider_id,size_message_going_be_recibed) 
            extraction_information=self.get_extraction_information_from_spider(conection_with_spider, size_message_going_be_recibed)
            logging.info('Datos recibidos de  [%s]:  [%s]  ', spider_id, extraction_information) 

            same_hash=self.verify_hash(expected_hash_extraction_information,extraction_information)
            if same_hash:
                extraction_information_to_dic=self.message_from_spider_to_dic(extraction_information)
                if extraction_information_to_dic and extraction_information_to_dic["status"] == "ok" :
                    self.i2p_webpage_manager.process_crawled_i2p_webpages(extraction_information_to_dic["urls"])
                else:
                    self.error_when_visiting_i2p_link(spider_id)
            else:
                self.error_when_visiting_i2p_link(spider_id)
        else:
            self.error_when_visiting_i2p_link(spider_id)

    def error_when_visiting_i2p_link(self, spider_id):
        self.stop_spider(spider_id)
        logging.error('Error arania :[%s] ', spider_id) 

    def message_from_spider_to_dic(self, raw_message_from_spider):
        """
        Translate the message from the spider from raw to dictionary
        :param message_from_spider: raw message that the spider sent
        :return: the message from spider translated in python dictionary format 
        """
        try:
            message_to_dic= json.loads(raw_message_from_spider)
        except:
            message_to_dic= {}
            logging.error('Datos recibidos [%s] error al pasarlos a formato', raw_message_from_spider)
        return message_to_dic



    def get_introductory_information_from_spider(self, conection_with_spider):
        """
        If succes it return a dictionary containing data needed to exchange extraction information from the spider 
        .
        Else it returns an empty dic
        """
        message_to_dic={}
        try:
            message_from_spider=conection_with_spider.recv(1024)
            logging.info('Recibido datos de contacto de arania [%s]. ', message_from_spider) 
            if message_from_spider: 
                #Spider open connection and send nothing. We interpret it as error so we close the connection and kills the spider. Only valid JSON messages are accepted.
                #The same happens if we are not able to deparse the json message recived.
                conection_with_spider.sendall(message_from_spider)
                confirmation_same_message_from_spider=conection_with_spider.recv(1024)
                logging.info('Respueta arania [%s]', confirmation_same_message_from_spider) 
                if confirmation_same_message_from_spider=="ERROR":
                    raise
            else:
                conection_with_spider.close()
        except:
            logging.error('Error en la fase de contacto con la  arania, abortando conexion') 
            conection_with_spider.close()

        message_from_spider_to_dic=self.message_from_spider_to_dic(message_from_spider)
        return message_from_spider_to_dic


    def  get_extraction_information_from_spider(self, conection_with_spider,size_message_going_be_recibed):
        """
        .
        """

        total_transmitted_data_from_spider=0
        message_from_spider=""
        while total_transmitted_data_from_spider != size_message_going_be_recibed:
            data_transmitted_from_spider=conection_with_spider.recv(1024)
            logging.info('Datos[%s]  ', data_transmitted_from_spider) 
            if data_transmitted_from_spider:
                total_transmitted_data_from_spider+=len(data_transmitted_from_spider)
                logging.info('Datos totales recibidos [%d], datos que faltan [%d]  ', total_transmitted_data_from_spider, total_transmitted_data_from_spider-size_message_going_be_recibed) 
                message_from_spider+=data_transmitted_from_spider
            else:
                #Conexion cerrada por la arania antes de completar el traspaso de datos
                conection_with_spider.close()
                break
        conection_with_spider.close()
        logging.debug('Conexion con arania cerrada') 
        return message_from_spider

    def listen(self):
        while 1:
            logging.info('Esperando conexiones de las aranias') 
            (conection_with_spider, address) = self.sock.accept()

            logging.info('Conexion establecida') 
            self.process_conection_with_spider(conection_with_spider)
            url_to_crawl=self.i2p_webpage_manager.get_link_to_visit()
            if url_to_crawl:
                logging.info('Proxima url a analizar [%s]\n ', url_to_crawl) 
                response=self.send_url_spider(url_to_crawl)
                logging.info('Lanzando arania con url [%s]\n. Info [%s] ', response,url_to_crawl) 
                self.spiders["num_spiders"]=self.spiders["num_spiders"]+1
                self.spiders[url_to_crawl]=response["jobid"]
            else:
                logging.info('No more links to visit') 


    def wait_spider_to_stop(self,id_spider):

        time_wait=0
        stopped_spider=False
        while(stopped_spider == False):
            response=requests.get('http://localhost:6800/listjobs.json?project=i2p_crawler').json()
            if response["finished"]:
                for job in response["finished"]:
                    if job["id"] == self.spiders[id_spider]:
                        stopped_spider=True
                        break
            if stopped_spider==False and time_wait < 7:
                time_wait+=1
                time.sleep(10)

    def stop_spider(self, id_spider):
        params={'project': 'i2p_crawler', 'jobid':id_spider}
        response=requests.post("http://localhost:6800/cancel.json",data=params).json()
        self.wait_spider_to_stop(id_spider)
        self.spiders.pop(id_spider)
        self.spiders["num_spiders"]=self.spiders["num_spiders"]-1
        self.i2p_webpage_manager.i2p_link_error(id_spider)
        return response
        

    def send_url_spider(self,url):

        logging.info('Lanzando arania sobre [%s]\n ', url) 
        params={'project': 'i2p_crawler', 'spider':'i2p_spider_1', 'url_to_crawl':url, 'master_url':os.environ["URL_MASTER"] , 'master_port' :int(os.environ["MASTER_PORT"])}
        response=requests.post("http://localhost:6800/schedule.json",data=params).json()
        logging.debug('Arania lanzada [%s]\n Url a analizar [%s] ', response,url) 

        logging.debug('Scrapyd: trabajo lanzado [%s]  ', response) 
        return response

    def get_seed_website(self):
        seed_websites=["http://www.zzz.i2p/", "http://www.inr.i2p/", "http://www.stats.i2p/", "http://www.forums.i2p/"]
        for website in seed_websites:
            visited=self.i2p_webpage_manager.check_website_existence(website)
            if not visited:
                return website
                

    def start(self):
        logging.info('Status [%s]  ', requests.get("http://localhost:6800/daemonstatus.json")) 
        response=requests.get("http://localhost:6800/daemonstatus.json").json()
        logging.info('Status [%s]  ', response) 
        if response['status']:
            response=requests.get("http://localhost:6800/listjobs.json?project=i2p_crawler").json()
            logging.debug('Scrapyd: trabajos activos [%s]  ', response) 
            if len(response["pending"]) == 0  and len(response["running"]) == 0:
                for x in range(0,3):
                    url_to_crawl=self.i2p_webpage_manager.get_link_to_visit()
                    response=self.send_url_spider(url_to_crawl) 
                    if response["status"] == "ok":
                        self.spiders[url_to_crawl]=response["jobid"]
                    self.listen()
 
                
spider_manager=SpiderManager() 
spider_manager.start()
