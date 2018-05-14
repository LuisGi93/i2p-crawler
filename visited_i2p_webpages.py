
from i2p_webpages_common import I2PWebpagesCommon

import logging
import urlparse# Mirar si scrapy tiene funciones para parsear la url


class VisitedI2PWebpages(I2PWebpagesCommon):
    """
    Represent the previously visited links
    """
    def __init__(self,cur):
        """
        Constructs a new 'VisitedLinks' object.
        :param cur: A cursor for working with the database
        """

        self.links_list={}
        self.cursor=cur

        cur.execute("SELECT i2psites.name,visited_i2pwebpages.path_i2pwebpage FROM i2psites,visited_i2pwebpages WHERE i2psites.id = visited_i2pwebpages.id_i2psite")
        visited_i2p_webpages=self.cursor.fetchall()
        logging.debug('Extracting links to visit\n [%s]\n ', visited_i2p_webpages) 
        for i2p_link in visited_i2p_webpages:
            i2p_website=link[0]
            i2p_webpage=link[1]
            have_link=False
            if i2p_website not in self.links_list:
                #Sitio web ha sido visitado
                self.links_list[i2p_website]=[]

            logging.debug('Links [%s] aniadido a la cola de visitar.\n ', i2p_webpage) 
            self.links_list[i2p_website].append(i2p_webpage)

    def add(self,i2p_link):
        """
        Add a new links to the visited links
        :param link: new links that is gonna be added
        """
        i2psite= "{0.scheme}://{0.netloc}/".format(urlparse.urlsplit(i2p_link))
        path_i2pwebpage= "{0.path}".format(urlparse.urlsplit(link))
        logging.debug('STARTTT %s   ', self.start_urls) 
        if i2psite not in self.links_list: 
            self.links_list[i2psite]=[]

            if i2psite not in self.start_urls:
                logging.debug('{DATABASE}Adding new i2psite [%s] to {i2psite}   ', i2p_link) 
                query="INSERT INTO i2psites (i2psite) VALUES (%s)"
                self.cursor.execute(query, (i2psite,)) 
                logging.debug('{DATABASE} Added new website [%s] to {website}', i2p_link) 


        if i2p_link not in self.links_list[i2psite]:
            if i2p_link not in self.start_urls:


                self.cursor.execute("SELECT id FROM i2psites where name = %s",
                (i2psite,))
                id_i2psite=cursor.fetchone()

                query="INSERT INTO i2psite_webpages (id_i2psite,path_i2pwebpage) VALUES (%s,%s)"
                self.cursor.execute(query,(id_i2psite,path_i2pwebpage,)) 
                logging.debug('{DATABASE} Added new webpage:[%s] to website [%s]', link,website) 


            logging.debug('Adding new url to visited links:\nUrl: [%s]\n Web [%s]  ', i2p_link, i2psite) 
            self.links_list[i2psite].append(i2p_link)
            query="INSERT INTO visited_i2pwebpages (id_i2psite,path_i2pwebpage) VALUES (%s,%s)"
            self.cursor.execute(query, (id_i2psite,path_i2pwebpage)) 
            logging.debug('[%s] inserted into not_visited_webpage [%s]', path,i2psite, )

