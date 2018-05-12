
import logging
import psycopg2
import urlparse
import os
from relations_i2p_webpages import RelationsI2PWebpages
from visited_i2p_webpages import VisitedI2PWebpages
from not_visited_i2p_webpages import NotVisitedI2PWebpages

class I2PWebpageManager():
    """
    It is the responsible of working with links, the one who know how to operate with them
    Orchestrate all the operations on links
    """
    def __init__(self):

        """
        Constructs a new 'LinkManager' object.
        """

        try:
            conn = psycopg2.connect("dbname='"+os.environ["DATABASE"]+"' user='"+os.environ["DATABASE_USER"]+"' host='"+os.environ["URL_DATABASE"]+"' password='"+os.environ["DATABASE_PASSWORD"]+"'")

#            conn.set_session( autocommit=True)
            logging.debug('Conectado a la base de datos [%s]', os.environ["URL_DATABASE"]) 
        except:
            logging.error('Imposible conectar base datos') 
        self.cursor=conn.cursor()
        self.visited_i2p_webpages=VisitedI2PWebpages(self.cursor)
        self.not_visited_i2p_webpages=NotVisitedI2PWebpages(self.cursor)
        self.relation_I2P_webpages=RelationsI2PWebpages(self.cursor)
        self.start_urls=['http://thethinhat.i2p','http://ugha.i2p', 'http://lawiki.i2p', 'http://planet.i2p']

    def get_link_to_visit(self):
        """
        Return the next link in queue pending to be visited
        :return: An  i2p link
        """
        link=self.not_visited_i2p_webpages.next_link()
        if link is None:
            for start_link in self.start_urls:
                been_visited=self.visited_i2p_webpages.contains(start_link) 
                in_queue=self.not_visited_i2p_webpages.contains(start_link)
                if not in_queue and not been_visited:
                    self.not_visited_i2p_webpages.add(start_link)
                    return start_link
        else:
            return link



    def check_website_existence(self,website):
        """
        Insert a website into database if it  is previously unknown
        :param website: website to check
        """
        ##Comprobamos si es la primera vez que aparece este sitio web
        visited_website_before=self.visited_i2p_webpages.website_known(website)
        website_in_queue=self.not_visited_i2p_webpages.website_known(website)
        if visited_website_before == False and website_in_queue==False:

            query="INSERT INTO i2psites (website_url) VALUES (%s)"
            self.cursor.execute(query,(website,)) 

            logging.debug('{DATABASE} Adding new website:[%s]', website) 

    def extracted_links_from_i2p_webpage(self,source_url,links):
        """
        Process the new links extracted from and url
        :param source_url: url from where the links were extracted
        :param links: list of links extracted from source_url
        """
        ##Llamado cuando se ha visitado una nueva pagina  y se han extraido los enlaces que almacena
        for link, access_type in links.iteritems():
            website= "{0.scheme}://{0.netloc}/".format(urlparse.urlsplit(link))
            path_webpage= "{0.path}".format(urlparse.urlsplit(link))
            self.check_website_existence(website)
            #Cualquier link que pase por aqui su sitio web ya ha sido creado
            logging.debug('Url: [%s] acces_type [%s]\n ', link, access_type) 

            been_visited=self.visited_i2p_webpages.contains(link) 
            in_queue=self.not_visited_i2p_webpages.contains(link)
            logging.debug('Url: [%s] Visited [%s]\n ', link, been_visited) 
            logging.debug('Url [%s] In queue: [%s]', link, in_queue) 

            if not in_queue and not been_visited:
                self.cursor.execute("SELECT id FROM i2psites where name = %s",
                (website,))
                id_i2psite=cursor.fetchone()

                query="INSERT INTO i2psite_webpages (id_i2psite,path_i2pwebpage) VALUES (%s,%s)"
                self.cursor.execute(query,(id_i2psite,path_webpage,)) 
                logging.debug('{DATABASE} Added new webpage:[%s] to website [%s]', link,website) 
                self.not_visited_i2p_webpages.add(website, id_i2psite, path_webpage)

            logging.debug('Comprobando relacion entre source_url: [%s] y dest_url [%s] con acces_type [%s]\n ', source_url, link ,access_type) 
            self.relation_I2P_webpages.check_relation(source_url,link,access_type)


 
    def process_crawled_i2p_webpages(self,links):
        links_visit=[]
        logging.debug('Links extraidos [%s]', links) 
        for i2p_visited_link,extracted_information in links.iteritems():
            logging.debug('URL= [%s] \n DATA=[%s]', i2p_visited_link,extracted_information) 
            if extracted_information["status"] == "ok":
                self.visited_i2p_webpages.add(i2p_visited_link)
                self.not_visited_i2p_webpages.visited(i2p_visited_link)

                self.extracted_links_from_i2p_webpage(i2p_visited_link,extracted_information["urls"])

            logging.debug('\n\n***Fin procesamiento links de webpage [%s] ***\n\n', i2p_visited_link ) 
    
    
    def link_error(self,link):
        website= "{0.scheme}://{0.netloc}/".format(urlparse.urlsplit(link))
        self.not_visited_i2p_webpages.pull_back(website)



