


import logging
import urlparse# Mirar si scrapy tiene funciones para parsear la url


class VisitedI2PWebpages():
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

        cur.execute("SELECT websites.name,visited_i2pwebpages.path_i2pwebpage FROM websites,visited_i2pwebpages WHERE websites.id = visited_i2pwebpages.id_i2psite")
        visited_i2p_webpages=self.cursor.fetchall()
        for i2p_visited_link in visited_i2p_webpages:
            i2p_website=i2p_visited_link[0]
            i2p_webpage=i2p_visited_link[1]
            if i2p_website not in self.links_list:
                #Sitio web ha sido visitado
                self.links_list[i2p_website]=[]

            self.links_list[i2p_website].append(i2p_webpage)




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

        return have_link

    def add(self,i2page):
        """
        Add a new link to the pending links to visit
        Returns the next link in queue to visit
        :return: an url.
        """

        i2psite= "{0.scheme}://{0.netloc}".format(urlparse.urlsplit(i2page))
        path_i2pwebpage= "{0.path}".format(urlparse.urlsplit(i2page))
        query="{0.query}".format(urlparse.urlsplit(i2page))
        if query:
            path_i2pwebpage=path_i2pwebpage+"?"+query

        logging.debug('Adding %s to visited   ', i2page)

        if i2psite not in self.links_list:
            self.links_list[i2psite]=[]


        self.cursor.execute("SELECT id FROM websites where name = %s",(i2psite,))
        id_i2psite=self.cursor.fetchone()[0]

        query="INSERT INTO visited_i2pwebpages (id_i2psite,path_i2pwebpage) VALUES (%s,%s)"
        self.cursor.execute(query, (id_i2psite,path_i2pwebpage))
        logging.debug('[%s] inserted into visited_webpage [%s]', path_i2pwebpage,i2psite, )

        self.links_list[i2psite].append(path_i2pwebpage)


    def website_known(self,website):
        in_queue=False
        if website in self.links_list:
           in_queue=True
        return in_queue
