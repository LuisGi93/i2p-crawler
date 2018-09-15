
#Needed if we want to monitor the activity of our spider manager
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

import sys
reload(sys)
sys.setdefaultencoding('utf8')

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
        self.max_spiders=6

    def verify_hash(self,expected_hash, message):
        """
        Verify if the hash of a message is the same as the expected_hash
        :param message: raw message that the spider sent
        :param expected_hash: the hash that is expected to be the hash of the message 
        :return: True if tthe hash of the message is the same as expected_hash, False otherwise
        """
        message_sha256_calculator=hashlib.sha256()
        message_sha256_calculator.update(message)
        hash_message=message_sha256_calculator.hexdigest()
        if expected_hash== hash_message:
            return True
        else:
            return False


    def process_conection_with_spider(self,conection_with_spider):

        """
        Work with an established connection with a spider to get all the data that the spider have extracted
        :param co: raw message that the spider sent
        :param expected_hash: the hash that is expected to be the hash of the message 
        :return: True if tthe hash of the message is the same as expected_hash, False otherwise
        """
        introductory_information_from_spider=self.get_introductory_information_from_spider(conection_with_spider)
        if introductory_information_from_spider:
            spider_id=introductory_information_from_spider["url_to_crawl"]
            expected_hash_extraction_information=introductory_information_from_spider["hash"]

            logging.info('Contactado por [%s]', spider_id)
            logging.info('Introductory information from spider %s  ',introductory_information_from_spider)
            extraction_information=self.get_extraction_information_from_spider(conection_with_spider, introductory_information_from_spider["extraction_information_first_message"] )
            logging.info('Datos recibidos de  [%s]:  [%s]  ', spider_id, extraction_information)

            same_hash=self.verify_hash(expected_hash_extraction_information,extraction_information)
            if same_hash:
                extraction_information_to_dic=self.message_from_spider_to_dic(extraction_information)
                if extraction_information_to_dic and extraction_information_to_dic["status"] == "ok" :
                    self.i2p_webpage_manager.process_crawled_i2p_webpages(extraction_information_to_dic["urls"])
                    self.spiders.pop(spider_id)
                    self.spiders["num_spiders"]=self.spiders["num_spiders"]-1
                    logging.info('Arania  [%s] ha finalizado, numero de aranias actuales %d  ', spider_id,self.spiders["num_spiders"] )
                else:
                    self.error_when_visiting_i2p_link(spider_id)
            else:
                self.error_when_visiting_i2p_link(spider_id)

        else:
            #El maestro no sabe cual de las aranias activas ha dado error porque no ha podido recibir el identificador
            #La arania que ha dado error en esta fase acabara muriendose ella misma en menos de un minuto
            #El maestro no espera a que se muera sino que lanza otra arania con la siguiente url y cada vez que le contacte una arania
            #comprobara cual entre las que tiene activas ha acabo muriendose sin mandarle informacion del proceso de crawling
            conection_with_spider.close()
    def error_when_visiting_i2p_link(self, spider_id):
        """
        Called when an error occur in the comunication with a spider 
        :param spider_id: the starting link sent to the spider 
        """
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
        Use the connection with spider to extract the first message of the comunication protocol between the spider and the master. 
        :param conection_with_spider: the established conection with the spider
        :return: the message from spider translated in python dictionary format
        """
        message_to_dic={}
        message_from_spider_to_dic=None
        try:
            message_from_spider=conection_with_spider.recv(1024)
            logging.info('Recibido datos de contacto de arania [%s]. ', message_from_spider)
            if message_from_spider:
                #Spider open connection and send nothing. We interpret it as error so we close the connection and kills the spider. Only valid JSON messages are accepted.
                #The same happens if we are not able to deparse the json message received.
                conection_with_spider.sendall(message_from_spider)
                confirmation_same_message=conection_with_spider.recv(1024)
                logging.info('Respueta arania [%s]', confirmation_same_message)
                if confirmation_same_message=="ERROR":
                    message_to_dic=None
                    raise

            else:
                conection_with_spider.close()

            message_from_spider_to_dic=self.message_from_spider_to_dic(message_from_spider)
            message_from_spider_to_dic["extraction_information_first_message"]=confirmation_same_message
        except:
            logging.error('Error en la fase de contacto con la  arania, abortando conexion')
            conection_with_spider.close()

        return message_from_spider_to_dic


    def  get_extraction_information_from_spider(self, conection_with_spider, first_message_extraction_phase):
        """
        Use the connection with spider to get the secondn of the comunication protocol, the one who contains all the information that the spider has extracted. 
        :param conection_with_spider: the established conection with the spider
        :return: the message from spider translated in python dictionary format
        """

        total_transmitted_data=0
        message_from_spider=first_message_extraction_phase

        logging.info('Datos iniciales [%s]  ', first_message_extraction_phase)

        data_transmitted_from_spider=conection_with_spider.recv(1024)
        logging.info('Datos[%s]  ', data_transmitted_from_spider)
        while data_transmitted_from_spider:
            total_transmitted_data+=len(data_transmitted_from_spider)
            logging.info('Datos totales recibidos [%d]  ', total_transmitted_data )
            message_from_spider+=data_transmitted_from_spider
            data_transmitted_from_spider=conection_with_spider.recv(1024)




        conection_with_spider.close()
        logging.debug('Conexion con arania cerrada')
        return message_from_spider

    def get_next_link(self):
        """
        It provides the next link to a webpage of the next i2p website to be visited
        :return: If there are available link the URL of the next webpage to visit. None otherwise.
        """
        invalid_links=[]

        url_to_crawl=self.i2p_webpage_manager.get_link_to_visit()
        url_been_visited=url_to_crawl in self.spiders and url_to_crawl not in invalid_links
        while url_to_crawl and url_been_visited:
            #Mientras no se de un enlace que no este siendo visitado
            logging.debug('%s ya esta siendo visitada', url_to_crawl)
            invalid_links.append(url_to_crawl)
            url_to_crawl=self.i2p_webpage_manager.get_link_to_visit()
            url_been_visited=url_to_crawl in self.spiders and url_to_crawl not in invalid_links
        
        if url_to_crawl in invalid_links:
            logging.debug('Urls que nos da el webpage manager ya estan siendo visitadas: %s', url_to_crawl)
            #Todas las urls que nos ofrece el webpage manager ya estan siendo visitadas
            url_to_crawl=None
        return url_to_crawl

    def listen(self):
        """
        Process called to wait for connections from the spiders that are running.
        """

        while self.spiders["num_spiders"] != 0:
            logging.info('Esperando conexiones de las aranias')
            (conection_with_spider, address) = self.sock.accept()

            logging.info('Conexion establecida')
            self.process_conection_with_spider(conection_with_spider)
            #En este punto la conexion con la arnia ya se ha cerrado y procesado la informacion que ha extraido
            url_to_crawl=self.get_next_link()
            if url_to_crawl:
                logging.info('Proxima url a analizar [%s]\n ', url_to_crawl)
                response=self.send_url_spider(url_to_crawl)
                logging.info('Lanzando arania con url [%s]\n. Info [%s] ', response,url_to_crawl)
                self.spiders["num_spiders"]=self.spiders["num_spiders"]+1
                logging.info('Aumentando el numero de aranias a  %d  ', self.spiders["num_spiders"] )
                self.spiders[url_to_crawl]=response["jobid"]

            else:
                logging.info('No more links to visit')
            self.clean_unexpected_dead_spiders()
            if self.spiders["num_spiders"] < self.max_spiders:
                self.wake_up_more_spiders()

    def wake_up_more_spiders(self):
        """
        It launch new spiders until the max number allowed of running spiders is reached
        """
        alive_spiders=self.spiders["num_spiders"]
        logging.info('Aranias actualemente vivas %d, intentado levantar mas aranias ', alive_spiders)
        for x in range(alive_spiders, self.max_spiders+1):
            url_to_crawl=self.get_next_link()
            if url_to_crawl:
                logging.info('Reviviendo arania con [%s]\n ', url_to_crawl)
                response=self.send_url_spider(url_to_crawl)
                logging.info('Lanzando arania con url [%s]\n. Info [%s] ', response,url_to_crawl)
                self.spiders["num_spiders"]=self.spiders["num_spiders"]+1
                logging.info('Aumentando el numero de aranias a  %d  ', self.spiders["num_spiders"] )
                self.spiders[url_to_crawl]=response["jobid"]
            else:
                logging.info('No se pueden revivir aranias ya que no hay mas links')
                break
            

    def wait_spider_to_stop(self,id_spider):
        """
        Wait until a spider die
        """
        logging.info('Esperando a que pare %s' , id_spider)
        time_wait=0
        stopped_spider=False
        while(stopped_spider == False):
            #Enlace utilizado para obtener informacion de que aranias estan ejecutandose
            response=requests.get('http://localhost:6800/listjobs.json?project=i2p_crawler').json()
            if response["finished"]:
                for job in response["finished"]:
                    if job["id"] == self.spiders[id_spider]:
                        stopped_spider=True
                        break
            if stopped_spider==False and time_wait <14:
                logging.info('Esperando a que pare %s, durmiendo 5 segundos' , id_spider)
                time_wait+=1
                time.sleep(5)

        logging.info('Arania [%s] parada', id_spider)


    def clean_unexpected_dead_spiders(self):
        """
        To remove spiders that died unexpectedly from the running spider.
        """
        response=requests.get("http://localhost:6800/listjobs.json?project=i2p_crawler").json()
        running_spiders=[]
        for job in response["running"]:
            running_spiders.append(job["id"])
            running_spiders.append(job["id"].encode('utf-8'))
        for job in response["pending"]:
            running_spiders.append(job["id"])
            running_spiders.append(job["id"].encode('utf-8'))

        [x.encode('UTF8') for x in running_spiders]
        logging.debug('Trabajos activos [%s]  ', running_spiders)
        dead_spiders=[]
        for id_spider, spider_job in self.spiders.iteritems():
            if id_spider != "num_spiders" and spider_job not in running_spiders:
                logging.debug('Encontrada arania muerta de manera inesperada con id [%s] y jobid [%s]  ', id_spider, spider_job)
                dead_spiders.append(id_spider)
        for id_dead_spider in dead_spiders:
            self.spiders.pop(id_dead_spider)
            self.spiders["num_spiders"]=self.spiders["num_spiders"]-1
            self.i2p_webpage_manager.i2p_link_error(id_dead_spider)




            #Cada vez que se coja una url tiene que rotarse la cola sino todas las aranias reciben la misma url
            #


    def stop_spider(self, id_spider):
        """
        End the execution of a spider.
        :param id_spider: the starting URL that was sent to the spider 
        """
        params={'project': 'i2p_crawler', 'jobid':id_spider}
        response=requests.post("http://localhost:6800/cancel.json",data=params).json()
        response=requests.post("http://localhost:6800/cancel.json",data=params).json()
        #Hay que enviarlo dos veces para abortar, tarda un tiempo en cancelarse
        self.wait_spider_to_stop(id_spider)
        self.spiders.pop(id_spider)
        self.spiders["num_spiders"]=self.spiders["num_spiders"]-1
        self.i2p_webpage_manager.i2p_link_error(id_spider)
        return response


    def send_url_spider(self,url):
        """
        Launchs a spider with the given URL
        :param url: the starting URL to give to the spider 
        :return: The scrapyd response to the order of launching the spider
        """

        logging.info('Lanzando arania sobre [%s]\n ', url)
        params={'project': 'i2p_crawler', 'spider':'i2p_spider_1', 'url_to_crawl':url, 'master_url':os.environ["URL_MASTER"] , 'master_port' :int(os.environ["MASTER_PORT"])}
        response=requests.post("http://localhost:6800/schedule.json",data=params).json()
        logging.debug('Arania lanzada [%s]\n Url a analizar [%s] ', response,url)

        logging.debug('Scrapyd: trabajo lanzado [%s]  ', response)
        return response



    def start(self):
        """
        Starts the crawling process launching some spiders
        """
        
        logging.info('Status [%s]  ', requests.get("http://localhost:6800/daemonstatus.json"))
        response=requests.get("http://localhost:6800/daemonstatus.json").json()
        logging.info('Status [%s]  ', response)
        if response['status']:
            response=requests.get("http://localhost:6800/listjobs.json?project=i2p_crawler").json()
            logging.debug('Scrapyd: trabajos activos [%s]  ', response)
            if len(response["pending"]) == 0  and len(response["running"]) == 0:
                for x in range(0,self.max_spiders):
                    url_to_crawl=self.get_next_link()
                    if url_to_crawl:
                        response=self.send_url_spider(url_to_crawl)
                        if response["status"] == "ok":
                            self.spiders[url_to_crawl]=response["jobid"]

                            self.spiders["num_spiders"]=self.spiders["num_spiders"]+1
                self.listen()


spider_manager=SpiderManager()
spider_manager.start()
