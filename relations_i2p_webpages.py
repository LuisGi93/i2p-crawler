from i2p_webpages_common import I2PWebpagesCommon

import logging
import urlparse# Mirar si scrapy tiene funciones para parsear la url
class RelationsI2PWebpages(I2PWebpagesCommon):
    """
    Reponsible of knowing the relations between links
    """

    def get_url_webpage(self,id_webpage):
        self.cursor.execute("SELECT i2psite_webpages.id_i2psite,i2psite_webpages.path_i2pwebpage FROM i2psite_webpages WHERE i2psite_webpages.id = %s",
        (id_webpage,))
        query_result=cursor.fetchone()
        webpage_path=query_result[1]

        self.cursor.execute("SELECT name FROM i2psites WHERE id = %s",
        (query_result[0],))
        name_website=cursor.fetchone()[0]
        return name_website+webpage_path

    def get_id_webpage(self,webpage):

        website= "{0.scheme}://{0.netloc}/".format(urlparse.urlsplit(webpage))
        path_i2pwebpage= "{0.path}".format(urlparse.urlsplit(webpage))

        self.cursor.execute("SELECT id FROM i2psites where name = %s",
        (website,))
        id_website=cursor.fetchone()
        self.cursor.execute("SELECT i2psite_webpages.id_i2p_webpage FROM i2psite_webpages WHERE i2psite_webpages.path_i2pwebpage = %s AND i2psite_webpages.id_i2psite=%s",
        (path_i2pwebpage, id_website,))
        query_result=cursor.fetchone()
        id_webpage=query_result[0]
        return id_webpage 

    def __init__(self,cur):
        
        self.links_list={}
        """
        Constructs a new 'LinksMap' object.
        :param cur: A cursor for working with the database
        """
        self.cursor=cur
        self.cursor.execute("SELECT id_source_i2pwebpage, id_destination_webpage,type FROM connection_between_webpages")
        connections=self.cursor.fetchall()
        logging.debug('[BD] Extraidos relacion de links [%s].\n ', connections) 
        logging.debug('MAPA [%s]', self.links_list)
        for connection in connections:
            source_url=self.get_url_webpage(connection[0])
            destination_url=self.get_url_webpage(connection[0])
            access_type=connection[2]

            website= "{0.scheme}://{0.netloc}/".format(urlparse.urlsplit(destination_url))
            if website not in self.links_list:
                #Sitio web ha sido visitado
                self.links_list[website]=[]
            if source_url not in self.links_list[website]:
                #Sitio web ha sido visitado
                self.links_list[website][source_url]={}
            self.links_list[website][source_url][destination_url]=[]
            self.links_list[website][source_url][destination_url].append(access_type)

    def create_relation_between_sites(self, url_source_i2psite, url_destination_website):
            self.cursor.execute("SELECT id FROM i2psites where name = %s",
            (url_source_i2psite,))
            id_source_i2psite=cursor.fetchone()

            self.cursor.execute("SELECT id FROM i2psites where name = %s",
            (url_destination_website,))
            id_destination_i2psite=cursor.fetchone()

            query="INSERT INTO connection_between_sites ( id_source_i2psite, id_destionation_website) VALUES (%s,%s)"
            self.cursor.execute(query, (id_source_i2psite, id_destination_website,)) 
            logging.debug('New connection between sites, from [%s] to [%s]', url_source_i2psite, url_destination_website) 

    def  check_relation(self, source_url, destination_url, access_type):
        """
        Check if the relation between source_url and destination_url of access_type is known
        creating it if it is unknown
        :param source_url: the origin url who contains destionation_url
        :param destination_url: the url contained by source_url
        :param access_type: how source access destination (a, img, js, css...)
        """
        source_i2psite= "{0.scheme}://{0.netloc}/".format(urlparse.urlsplit(source_url))
        if source_i2psite not in self.links_list:
            self.links_list[source_i2psite]={}
            destination_website= "{0.scheme}://{0.netloc}/".format(urlparse.urlsplit(destination_url))
            self.create_relation_between_sites(source_i2psite,destination_website)
        if source_url not  in self.links_list[source_i2psite]:
            #La url fuente no es conocidaccess_type))
            self.new_relation(source_url,destination_url, access_type)
        else:
            source_url_links=self.links_list[source_i2psite][source_url]
            #Extraemos las relaciones conocidas de la url fuente
            if destination_url not in source_url_links: 
                self.new_relation(source_url,destination_url, access_type)
            elif access_type not in self.links_list[source_i2psite][source_url][destination_url] :
                links_list[source_i2psite][source_url][destination_url].append(access_type)
                self.insert_new_relation(source_url,destination_url,acess_type)
                logging.debug('New connection from [%s] to [%s] of type [%s]', source_url,destination_url,access_type,) 



    def insert_new_relation(self,source_url, destination_url, access_type):
        source_i2p_webpage_id=self.get_id_webpage(source_url)
        destination_webpage_id=self.get_id_webpage(destination_url)
        query="INSERT INTO connection_between_webpages (id_source_i2pwebpage,id_destination_webpage,type) VALUES (%s,%s,%s)"
        self.cursor.execute(query, (source_i2p_webpage_id, destination_webpage_id,access_type,)) 

    def new_relation(self,source_url, destination_url, access_type):
        """
        Creates a new relation between source_url and destionation url of the type access_type
        :param source_url: the origin url who contains destionation_url
        :param destination_url: the url contained by source_url
        :param access_type: how source access destination (a, img, js, css...)
        """
        
        website= "{0.scheme}://{0.netloc}/".format(urlparse.urlsplit(source_url))
        dic={}
        dic[destination_url]=[access_type]
        self.links_list[website][source_url]=dic
        self.insert_new_relation(source_url, destination_url,acess_type)
        logging.debug('New connection from [%s] to [%s] of type [%s]', source_url,destination_url,access_type,) 

