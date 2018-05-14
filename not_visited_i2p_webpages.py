
from i2p_webpages_common import I2PWebpagesCommon

import logging
import urlparse

#Queda por implementar el link error pull back
class NotVisitedI2PWebpages(I2PWebpagesCommon):

    """
    Contains all the information related to links pending to visit
    """
    def __init__(self,cur):
        self.queue_links=[]
        self.links_list={}
        """
        Constructs a new 'LinksToVisit' object.
        :param cur: A cursor for working with the database
        """
        self.cursor=cur
        cur.execute("SELECT i2psites.name,not_visited_i2pwebpages.path_i2pwebpage FROM i2psites,not_visited_i2pwebpages WHERE i2psites.id = not_visited_i2pwebpages.id_i2psite")
        not_visited_i2p_webpages=self.cursor.fetchall()
        logging.debug('Extracting links to visit\n [%s]\n ', not_visited_i2p_webpages) 
        for i2p_link in not_visited_i2p_webpages:
            i2p_website=i2p_link[0]
            i2p_webpage=i2p_link[1]
            if i2p_website not in self.links_list:
                #Sitio web ha sido visitado
                self.links_list[i2p_website]=[]

            logging.debug('Links [%s] aniadido a la cola de visitar.\n ', i2p_webpage) 
            self.links_list[i2p_website].append(i2p_webpage)

        logging.debug('Links to visit queue \n [%s]\n ', self.links_list) 

        self.cursor.execute("select i2psites.name from i2psites,queue_i2psites_with_not_visited_i2pwebpages where i2psites.id = queue_i2psites_with_not_visited_i2pwebpages.id_i2psite ORDER BY queue_i2psites_with_not_visited_i2pwebpages.pos_queue ASC")
        self.queue_links=self.cursor.fetchall()
        logging.debug('Cola sitios i2p sin visitar %s.\n ', self.queue_links) 


    def next_link(self):
        """
        Returns the next link in queue to visit
        :return: an url.  
        """
        if len(self.queue_links) > 0: 
            website= self.queue_links[0]
            self.queue_links.append(website)
            self.queue_links.remove(website)
            link=website+self.links_list[website][0]
        else:
            website= None
            link= None
        return link

    def visited(self,i2p_link):
        """
        Remove a link from queue
        :param link: link that is going to be removed  
        """
        i2psite= "{0.scheme}://{0.netloc}/".format(urlparse.urlsplit(i2p_link))
        path_i2pwebpage= "{0.path}".format(urlparse.urlsplit(link))
        self.links_list[i2psite].remove(path_i2pwebpage)


        self.cursor.execute("SELECT id FROM i2psites where name = %s",
        (i2psite,))
        id_i2psite=cursor.fetchone()

        query="DELETE FROM not_visited_i2pwebpages WHERE not_visited_i2pwebpages.path_i2pwebpage = (%s) AND not_visited_i2pwebpages.id_i2psite = (%s)"
        self.cursor.execute(query, (path_i2pwebpage,id_i2psite,)) 

        logging.debug('Deleted [%s] from  not_visited_webpage', i2p_link) 
        if len(self.links_list[website]) == 0:
            self.links_list.pop(website)
            self.queue_links.remove(website)
            query="DELETE FROM queue_i2psites_with_not_visited_i2pwebpages WHERE id_i2psite=(%s)"
            self.cursor.execute(query, (id_i2psite,)) 




    def add(self,i2psite,id_i2psite,path_i2pwebpage):
        """
        Add a new link to the pending links to visit
        Returns the next link in queue to visit
        :return: an url.  
        """
        logging.debug('Adding [%s] to the list of pending links to visit\n ', i2psite) 
        if i2psite not in self.links_list:
            self.links_list[i2psite]=[]
            self.queue_links.append(i2psite)

            logging.debug('Adding to table [website_pending_urls]  website [%s] pos_queue [%d] ', i2psite,len(self.queue_links)) 

            query="INSERT INTO queue_i2psites_with_not_visited_i2pwebpages (id_i2psite,pos_queue) VALUES (%s,%s)"
            self.cursor.execute(query,(i2psite, len(self.queue_links),))
            logging.debug('[%s] inserted into website_pending_urls with pos=[%d]', i2psite, len(self.queue_links))

        self.links_list[website].append(path_i2pwebpage)

        query="INSERT INTO not_visited_i2pwebpages (id_i2psite,path_i2pwebpage) VALUES (%s,%s)"
        self.cursor.execute(query, (id_i2psite,path_i2pwebpage)) 
        logging.debug('[%s] inserted into not_visited_webpage [%s]', path,i2psite, )









