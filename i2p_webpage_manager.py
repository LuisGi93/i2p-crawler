
import logging
import psycopg2
import urlparse
import os
#test tld of link is i2p
import re
from relations_i2p_webpages import RelationsI2PWebpages
from visited_i2p_webpages import VisitedI2PWebpages
from not_visited_i2p_webpages import NotVisitedI2PWebpages

class I2PWebpageManager():
    """
    It is the responsible of working with the information that has been extracted from spiders
    """
    def __init__(self):

        """
        Constructs a new 'LinkManager' object.
        """

        try:
            conn = psycopg2.connect("dbname='"+os.environ["DATABASE"]+"' user='"+os.environ["DATABASE_USER"]+"' host='"+os.environ["URL_DATABASE"]+"' password='"+os.environ["DATABASE_PASSWORD"]+"'")


            logging.debug('Conectado a la base de datos [%s]', os.environ["URL_DATABASE"])
        except:
            logging.error('Imposible conectar base datos')
        
        conn.autocommit = True #True it write changes to the database, False the database will not be modified, usefull for debugging
        self.cursor=conn.cursor()
        self.visited_i2p_webpages=VisitedI2PWebpages(self.cursor)
        self.not_visited_i2p_webpages=NotVisitedI2PWebpages(self.cursor)
        self.relation_I2P_webpages=RelationsI2PWebpages(self.cursor)
        self.check_if_i2p_link=re.compile('[^\.]2p$')#Match all that ends in ._2p, _ can be anything

    def get_link_to_visit(self):
        """
        Return the next link in queue pending to be visited
        :return: An  i2p link
        """
        link=self.not_visited_i2p_webpages.next_link()
        if link:
            i2p_site= "{0.scheme}://{0.netloc}".format(urlparse.urlsplit(link))
            self.not_visited_i2p_webpages.pull_back(i2p_site)
        return link



    def check_website_existence(self,website):
        """
        Insert a website into database if it  was previously unknown
        :param website: website to check
        """
        ##Comprobamos si es la primera vez que aparece este sitio web
        ##Aniade a la base de datos los nuevos sitios webs diferente al que se ha crawleado


        is_i2p_link=self.check_if_i2p_link.search(website)
        logging.debug('(Website existence) %s check  if i2p site result:[%s]', website, is_i2p_link)
        if is_i2p_link:

            visited_website_before=self.visited_i2p_webpages.website_known(website)
            website_in_queue=self.not_visited_i2p_webpages.website_known(website)
            logging.debug('Website visited before:[%s] and website in queue:[%s]', visited_website_before, website_in_queue)
            if visited_website_before == False and website_in_queue==False:

                query="INSERT INTO websites (name) VALUES (%s)"
                self.cursor.execute(query,(website,))

                logging.debug('{DATABASE} Added new i2psite:[%s]', website)
        else:
            self.cursor.execute("SELECT id FROM websites where name = %s",
            (website,))
            website_exist=self.cursor.fetchone()
            if not website_exist:
                query="INSERT INTO websites (name) VALUES (%s)"
                self.cursor.execute(query,(website,))
                logging.debug('{DATABASE} Added new normal website:[%s]', website)



    def extracted_links_from_i2p_webpage(self,source_url,links):
        """
        Process the new links extracted from and url
        :param source_url: url from where the links were extracted
        :param links: list of links extracted from source_url
        """
        ##Llamado cuando se ha visitado una nueva pagina  y se han extraido los enlaces que almacena
        logging.debug('Extracted links from webpage [%s] ', source_url)

        for link, access_type in links.iteritems():

            logging.debug('Extracted link: [%s] acces_type [%s]\n ', link, access_type)

            website= "{0.scheme}://{0.netloc}".format(urlparse.urlsplit(link))
            self.check_website_existence(website)
            #Cualquier link que pase por aqui su sitio web ya ha sido creado
            is_i2p_website=self.check_if_i2p_link.search(website)
            if is_i2p_website:
                logging.debug('Extracted link: [%s] is and i2p link\n ', link)
                been_visited=self.visited_i2p_webpages.contains(link)
                in_queue=self.not_visited_i2p_webpages.contains(link)
                logging.debug('Extracted link: [%s] Visited [%s]\n ', link, been_visited)
                logging.debug('Extracted link: [%s] In queue: [%s]', link, in_queue)
                if not in_queue and not been_visited:
                    self.add_webpage_database(link)

                    self.not_visited_i2p_webpages.add(website, link)
            else:
                exist_webpage=self.check_webpage_exist_database(link)
                if not exist_webpage:
                    self.add_webpage_database(link)

                logging.debug('Url: [%s] is NOT i2p link\n ', link)

            logging.debug('Comprobando relacion entre source_url: [%s] y dest_url [%s] con acces_type [%s]\n ', source_url, link ,access_type)
            self.relation_I2P_webpages.check_relation(source_url,link,access_type)



    def add_webpage_database(self, webpage):
        """
        Add a new webpage to the database
        :param webpage: new webpage to be added to the datase 
        """
        website= "{0.scheme}://{0.netloc}".format(urlparse.urlsplit(webpage))
        path_webpage= "{0.path}".format(urlparse.urlsplit(webpage))
        query="{0.query}".format(urlparse.urlsplit(webpage))
        if query:
            path_webpage=path_webpage+"?"+query

        self.cursor.execute("SELECT id FROM websites where name = %s",(website,))
        id_i2psite=self.cursor.fetchone()[0]

        query="INSERT INTO webpages (id_site,path_webpage) VALUES (%s,%s)"
        self.cursor.execute(query,(id_i2psite,path_webpage,))
        logging.debug('{DATABASE} Added new webpage:[%s] to website [%s]', path_webpage,website)


    def check_webpage_exist_database(self, webpage):
        """
        Check if a webpage exist in the database
        :param webpage: webpage to check if it exist in the database 
        :return: True if it exist, False otherwise
        """
        website= "{0.scheme}://{0.netloc}".format(urlparse.urlsplit(webpage))
        path_webpage= "{0.path}".format(urlparse.urlsplit(webpage))
        query="{0.query}".format(urlparse.urlsplit(webpage))
        if query:
            path_webpage=path_webpage+"?"+query
        exist=False
        self.cursor.execute("SELECT id FROM websites where name = %s",
        (website,))
        id_i2psite=self.cursor.fetchone()[0]

        self.cursor.execute("SELECT path_webpage FROM webpages where path_webpage = %s AND id_site = %s",
        (path_webpage,id_i2psite,))
        exist_webpage=self.cursor.fetchall()

        logging.debug('Webpage [%s] Website [%s]  Path_webpage [%s]  id_website [%s] exist: ',webpage, website, path_webpage, id_i2psite)
        if exist_webpage:
            logging.debug('Si existe')
            exist=True
        else:
            logging.debug('No existe')
        return exist


    def process_crawled_i2p_webpages(self,links):
        """
        Add the crawled information obtained from an spider to the system
        :param links: links of webpages that has been visited 
        """
        link=next(iter(links))
        i2psite= "{0.scheme}://{0.netloc}".format(urlparse.urlsplit(link))

        for i2p_visited_link,extracted_information in links.iteritems():
            logging.debug('Visited url= [%s]  Extracted data=[%s]', i2p_visited_link,extracted_information)

            been_visited=self.visited_i2p_webpages.contains(i2p_visited_link)
            in_queue=self.not_visited_i2p_webpages.contains(i2p_visited_link)
            logging.debug('Visited url=: [%s] Visited [%s]\n ', i2p_visited_link, been_visited)
            logging.debug('Visited url=: [%s] In queue: [%s]', i2p_visited_link, in_queue)
            if not in_queue and not been_visited:
                    self.add_webpage_database(i2p_visited_link)

            if extracted_information["status"] == "ok":

                #If the visit to the webpage has been succesfull
                if in_queue:
                    self.not_visited_i2p_webpages.visited(i2p_visited_link)
                if not been_visited:
                    #Puede darse el caso de que desde la url inicial que se manda a la arania  se acceda a una pagina web que ya se ha visitado, y de la que por tanto, ya se han extraido los datos que contiene 
                    self.visited_i2p_webpages.add(i2p_visited_link)
                    self.extracted_links_from_i2p_webpage(i2p_visited_link,extracted_information["urls"])
            else:
                logging.debug('Error al visitar url: [%s]\n ', i2p_visited_link)
                if not in_queue and not been_visited:
                    self.not_visited_i2p_webpages.add(i2psite, i2p_visited_link)
            logging.debug('\n\n***Fin procesamiento links de webpage [%s] \n\n', i2p_visited_link )
        self.not_visited_i2p_webpages.check_if_no_pending_links(i2psite)
        logging.debug('\n\n***Fin procesamiento de todas las paginas extraidos por la arania')



    def i2p_link_error(self,link):
        """
        It append a website to the end list of pending website to visit 
        :param link: starting link sent to a spider that has failed  
        """
        logging.debug('Error al visitar [%s]',link)
        i2p_site= "{0.scheme}://{0.netloc}".format(urlparse.urlsplit(link))
        self.not_visited_i2p_webpages.pull_back(i2p_site)
