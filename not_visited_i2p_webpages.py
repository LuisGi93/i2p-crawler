

import logging
import urlparse

class NotVisitedI2PWebpages():

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
        cur.execute("SELECT websites.name,not_visited_i2pwebpages.path_i2pwebpage FROM websites,not_visited_i2pwebpages WHERE websites.id = not_visited_i2pwebpages.id_i2psite")
        not_visited_i2p_webpages=self.cursor.fetchall()
        for i2p_link in not_visited_i2p_webpages:
            i2p_website=i2p_link[0]
            i2p_webpage=i2p_link[1]

            if i2p_website not in self.links_list:
                self.links_list[i2p_website]=[]

            self.links_list[i2p_website].append(i2p_webpage)


        self.cursor.execute("select websites.name from websites,queue_i2psites_with_not_visited_i2pwebpages where websites.id = queue_i2psites_with_not_visited_i2pwebpages.id_i2psite ORDER BY queue_i2psites_with_not_visited_i2pwebpages.pos_queue ASC")
        i2p_site_name_query=self.cursor.fetchall()
        for query_result in i2p_site_name_query:
            logging.debug('Nombre %s.\n ', query_result[0])
            self.queue_links.append(query_result[0])



    def next_link(self):
        """
        Returns the next link in queue to visit
        :return: an url.
        """
        link=None
        if len(self.queue_links) > 0:
            website= self.queue_links[0]
            self.queue_links.remove(website)
            self.queue_links.append(website)
            if len(self.links_list[website]) > 0:
                link=website+self.links_list[website][0] ## ESto implica que la lista solo puede haber sitios webs que tengan al menos una url pendiente, sino aqui daria fallo
        logging.debug('Proximo link  %s.\n ', self.queue_links)
        return link

    def visited(self,i2p_link):
        """
        Remove a link from queue of links to visit
        :param link: link that is going to be removed
        """
        i2psite= "{0.scheme}://{0.netloc}".format(urlparse.urlsplit(i2p_link))
        query="{0.query}".format(urlparse.urlsplit(i2p_link))
        path_i2pwebpage= "{0.path}".format(urlparse.urlsplit(i2p_link))
        if query:
            path_i2pwebpage=path_i2pwebpage+"?"+query

        logging.debug('Deleting [%s] from not_visited_webpage', i2p_link)


        if path_i2pwebpage in self.links_list[i2psite]:
            self.links_list[i2psite].remove(path_i2pwebpage)


            self.cursor.execute("SELECT id FROM websites where name = %s",
            (i2psite,))
            id_i2psite=self.cursor.fetchone()[0]

            query="DELETE FROM not_visited_i2pwebpages WHERE not_visited_i2pwebpages.path_i2pwebpage = (%s) AND not_visited_i2pwebpages.id_i2psite = (%s)"
            self.cursor.execute(query, (path_i2pwebpage,id_i2psite,))

            logging.debug('Deleted [%s%s] from  not_visited_webpage', i2psite,path_i2pwebpage)



    def check_if_no_pending_links(self, i2psite):
        """
        Remove a site from the queue of sites to visit if it doesn't more links to visit 
        :param i2psite: i2p site to remove from the queue 
        """
        if len(self.links_list[i2psite]) == 0:
            logging.debug('Sitio [%s] va a ser borrdo', i2psite)

            self.cursor.execute("SELECT id FROM websites where name = %s",
            (i2psite,))
            id_i2psite=self.cursor.fetchone()[0]

            self.links_list.pop(i2psite)
            self.queue_links.remove(i2psite)
            query="DELETE FROM queue_i2psites_with_not_visited_i2pwebpages WHERE id_i2psite=(%s)"
            self.cursor.execute(query, (id_i2psite,))

    def pull_back(self,i2psite):
        """
        Append an i2p site to the end of the queue of the i2p sites to visit
        :param i2psite: i2p site to be moved to the end of the queue
        """
        self.queue_links.remove(i2psite)
        self.queue_links.append(i2psite)
        logging.debug('Moving [%s] to the end of the list', i2psite)


    def contains(self, i2page):
        """
        Check if a link exists in the queue of links
        :param link: links to check
        :return: True if the links exists or False if not
        """
        path_i2pwebpage= "{0.path}".format(urlparse.urlsplit(i2page))
        query="{0.query}".format(urlparse.urlsplit(i2page))
        if query:
            path_i2pwebpage=path_i2pwebpage+"?"+query
        website= "{0.scheme}://{0.netloc}".format(urlparse.urlsplit(i2page))
        have_link=False
        if website in self.links_list:
            #Sitio web ha sido visitado
            website_links=self.links_list[website]
            if path_i2pwebpage in  website_links:
                #El enlace ha sido visitado
                have_link=True

        #print(self.links_list)
        return have_link


    def get_last_queue_position(self):
        """
        Get the last position in the queue of sites to visit
        :return:  The last position in the queue of sites to visit
        """
        self.cursor.execute("SELECT pos_queue FROM queue_i2psites_with_not_visited_i2pwebpages ORDER BY pos_queue DESC LIMIT 1")
        last_position_queue=self.cursor.fetchone()[0]
        return last_position_queue

    def add(self,i2psite, i2page):
        """
        Add a new link to the pending links to visit
        Returns the next link in queue to visit
        :return: an url.
        """
        path_i2pwebpage= "{0.path}".format(urlparse.urlsplit(i2page))
        query="{0.query}".format(urlparse.urlsplit(i2page))
        if query:
            path_i2pwebpage=path_i2pwebpage+"?"+query
        logging.debug('Adding [%s] to the list of pending links to visit\n ', i2page)

        self.cursor.execute("SELECT id FROM websites where name = %s",(i2psite,))
        id_i2psite=self.cursor.fetchone()[0]

        if i2psite not in self.links_list:
            self.links_list[i2psite]=[]
            self.queue_links.append(i2psite)

            pos_queue=self.get_last_queue_position()+1
            logging.debug('Adding to table [website_pending_urls]  website [%s] pos_queue [%d] ', i2psite,pos_queue)

            query="INSERT INTO queue_i2psites_with_not_visited_i2pwebpages (id_i2psite,pos_queue) VALUES (%s,%s)"
            self.cursor.execute(query,(id_i2psite, pos_queue,))
            logging.debug('[%s] inserted into website_pending_urls with pos=[%d]', i2psite, pos_queue)

        self.links_list[i2psite].append(path_i2pwebpage)

        query="INSERT INTO not_visited_i2pwebpages (id_i2psite,path_i2pwebpage) VALUES (%s,%s)"
        self.cursor.execute(query, (id_i2psite,path_i2pwebpage))
        logging.debug('[%s] inserted into not_visited_webpage [%s]', path_i2pwebpage,i2psite, )

    def website_known(self,website):
        in_queue=False
        if website in self.links_list:
           in_queue=True
        return in_queue
