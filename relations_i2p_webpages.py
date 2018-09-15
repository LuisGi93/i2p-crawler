

import logging
import urlparse# Mirar si scrapy tiene funciones para parsear la url
class RelationsI2PWebpages():
    """
    Reponsible of knowing the relations between links
    """

    def get_id_site(self,website):
        self.cursor.execute("SELECT id FROM websites where name = %s",
        (website,))
        id_website=self.cursor.fetchone()[0]
        return id_website

    def get_url_webpage(self,id_webpage):
        self.cursor.execute("SELECT webpages.id_site,webpages.path_webpage FROM webpages WHERE webpages.id_webpage = %s",
        (id_webpage,))
        query_result=self.cursor.fetchone()

        #logging.debug('id_webpage %s', id_webpage)
        #logging.debug('query_result %s', query_result)
        webpage_path=query_result[1]

        self.cursor.execute("SELECT name FROM websites WHERE id = %s",
        (query_result[0],))
        name_website=self.cursor.fetchone()[0]
        return name_website+webpage_path

    def get_id_webpage(self,webpage):
        """
        Get the id that was asigned to a webpage from the database
        :param webpage: webpage to search the id 
        :return: the id of the webpage 
        """
        website= "{0.scheme}://{0.netloc}".format(urlparse.urlsplit(webpage))
        query="{0.query}".format(urlparse.urlsplit(webpage))
        path_webpage= "{0.path}".format(urlparse.urlsplit(webpage))
        if query:
            path_webpage=path_webpage+"?"+query
        id_website=self.get_id_site(website)
        self.cursor.execute("SELECT webpages.id_webpage FROM webpages WHERE webpages.path_webpage = %s AND webpages.id_site=%s",
        (path_webpage, id_website,))
        query_result=self.cursor.fetchone()
        id_webpage=query_result[0]
        return id_webpage




    def extract_connections_bw_webpages(self):
        """
        Loads the connections between webpages from the database
        """
        self.cursor.execute("SELECT id_source_i2pwebpage, id_destination_webpage,type FROM connection_between_webpages")
        connections=self.cursor.fetchall()
        for connection in connections:
            source_url=self.get_url_webpage(connection[0])
            destination_url=self.get_url_webpage(connection[1])
            access_type=connection[2]

            source_website= "{0.scheme}://{0.netloc}".format(urlparse.urlsplit(source_url))
            if source_website not in self.connections_bw_webpages:
                self.connections_bw_webpages[source_website]={}
            if source_url not in self.connections_bw_webpages[source_website]:
                self.connections_bw_webpages[source_website][source_url]={}
            if destination_url not in self.connections_bw_webpages[source_website][source_url]:
                self.connections_bw_webpages[source_website][source_url][destination_url]=[]
            self.connections_bw_webpages[source_website][source_url][destination_url].append(access_type)

    def get_name_website(self, id):

        """
        Get the name of a website using his id
        :param id: the id to use to search 
        :return: the name of the website with the id provided
        """
        self.cursor.execute("SELECT name FROM websites where id = %s",(id,))
        name_i2psite=self.cursor.fetchone()[0]
        return name_i2psite

    def extract_connections_bw_sites(self):
        """
        Loads the connections between websites from the database
        """
        self.cursor.execute("SELECT id_source_i2psite, id_destination_website FROM connection_between_sites")
        connections=self.cursor.fetchall()

        for connection in connections:
            name_source_i2psite=self.get_name_website(connection[0])
            name_destination_site=self.get_name_website(connection[1])

            if name_source_i2psite not in self.connections_bw_sites:
                #Sitio web ha sido visitado
                self.connections_bw_sites[name_source_i2psite]=[]
            if name_destination_site not in self.connections_bw_sites[name_source_i2psite]:
                #Sitio web ha sido visitado
                self.connections_bw_sites[name_source_i2psite].append(name_destination_site)
            #logging.debug('MAPA relacion entre sitios [%s]', self.connections_bw_sites)

    def __init__(self,cur):

        self.connections_bw_webpages={}
        self.connections_bw_sites={}
        """
        Constructs a new 'LinksMap' object.
        :param cur: A cursor for working with the database
        """
        self.cursor=cur
        self.extract_connections_bw_webpages()
        self.extract_connections_bw_sites()


    def create_relation_between_sites(self, url_source_i2psite, url_destination_website):
        """
        Create a new relation between two websites
        :param url_source_i2psite: the URL of the website that contains a link to other website
        :param url_destination_website: the URL of the website whose URL is contained 
        """
        self.cursor.execute("SELECT id FROM websites where name = %s",
        (url_source_i2psite,))
        id_source_i2psite=self.cursor.fetchone()[0]

        self.cursor.execute("SELECT id FROM websites where name = %s",
        (url_destination_website,))
        id_destination_website=self.cursor.fetchone()[0]

        query="INSERT INTO connection_between_sites ( id_source_i2psite, id_destination_website) VALUES (%s,%s)"
        self.cursor.execute(query, (id_source_i2psite, id_destination_website,))
        logging.debug('New connection between sites, from [%s] to [%s]', url_source_i2psite, url_destination_website)


    def increment_num_relations(self, source_i2psite, destination_site):

        """
        Increment the number of relations that exists between two websites
        :param url_source_i2psite: the URL of the website that contains a link to other website
        :param url_destination_website: the URL of the website whose URL is contained 
        """
        id_source_i2psite=self.get_id_site(source_i2psite)
        id_destination_site=self.get_id_site(destination_site)
        self.cursor.execute("UPDATE connection_between_sites SET number_connections = number_connections + 1 WHERE id_source_i2psite = %s AND id_destination_website = %s ",
                    (id_source_i2psite, id_destination_site,))
        logging.debug('Aumentado el numero de conexiones entre  [%s] y [%s]', source_i2psite, destination_site)


    def  check_relation(self, source_url, destination_url, access_type):
        """
        Check if the relation between source_url and destination_url of access_type is known creating it if it is unknown
        :param source_url: the origin url who contains destionation_url
        :param destination_url: the url contained by source_url
        :param access_type: how source access destination (a, img, js, css...)
        """
        source_i2psite= "{0.scheme}://{0.netloc}".format(urlparse.urlsplit(source_url))
        destination_website= "{0.scheme}://{0.netloc}".format(urlparse.urlsplit(destination_url))

        if source_i2psite != destination_website:
            if source_i2psite not in self.connections_bw_webpages:
                logging.debug('Nuevo sitio i2p sin relaciones [%s]', source_i2psite )

                self.connections_bw_webpages[source_i2psite]={}
                self.connections_bw_sites[source_i2psite]=[destination_website]
                self.create_relation_between_sites(source_i2psite,destination_website)

            elif destination_website not in self.connections_bw_sites[source_i2psite]:
                self.connections_bw_sites[source_i2psite].append(destination_website)
                self.create_relation_between_sites(source_i2psite,destination_website)
            if source_url not  in self.connections_bw_webpages[source_i2psite]:
                #La url fuente no es conocidaccess_type))
                self.new_relation(source_url,destination_url, access_type)
                self.increment_num_relations(source_i2psite, destination_website)

            else:
                source_url_links=self.connections_bw_webpages[source_i2psite][source_url]
                #Extraemos las relaciones conocidas de la url fuente
                logging.debug('source url links [%s]', source_url_links ,)
                logging.debug('destination [%s]', destination_url ,)
                if destination_url not in source_url_links:
                    self.connections_bw_webpages[source_i2psite][source_url][destination_url]=[access_type]

                    self.insert_new_relation(source_url, destination_url,access_type)
                    self.increment_num_relations(source_i2psite, destination_website)
                    logging.debug('New connection from [%s] to [%s] of type [%s]', source_url,destination_url,access_type,)
                    #self.new_relation(source_url,destination_url, access_type)
                elif access_type not in self.connections_bw_webpages[source_i2psite][source_url][destination_url] :
                    logging.debug('Tipos entre [%s] to [%s] of type [%s]', source_url,destination_url,self.connections_bw_webpages[source_i2psite][source_url][destination_url])
                    links_list[source_i2psite][source_url][destination_url].append(access_type)
                    self.insert_new_relation(source_url,destination_url,acess_type)
                    self.increment_num_relations(source_i2psite, destination_website)
                    logging.debug('New connection from [%s] to [%s] of type [%s]', source_url,destination_url,access_type,)

    def insert_new_relation(self,source_url, destination_url, access_type):

        """
        Inserts a new relation between two webpages into the database
        :param source_url: the origin url who contains destination_url
        :param destination_url: the url contained by source_url
        :param access_type: how source access destination (a, img, js, css...)
        """
        source_i2p_webpage_id=self.get_id_webpage(source_url)
        destination_webpage_id=self.get_id_webpage(destination_url)
        query="INSERT INTO connection_between_webpages (id_source_i2pwebpage,id_destination_webpage,type) VALUES (%s,%s,%s)"

        logging.debug('Insertando nueva relacion entre  [%s]  y [%s]     type [%s]', source_url, destination_url,access_type)
        self.cursor.execute(query, (source_i2p_webpage_id, destination_webpage_id,access_type,))



    def new_relation(self,source_url, destination_url, access_type):
        """
        Creates a new relation between source_url and destination url of the type access_type
        :param source_url: the origin url who contains destionation_url
        :param destination_url: the url contained by source_url
        :param access_type: how source access destination (a, img, js, css...)
        """

        website= "{0.scheme}://{0.netloc}".format(urlparse.urlsplit(source_url))
        dic={}
        dic[destination_url]=[access_type]
        self.connections_bw_webpages[website][source_url]=dic
        self.insert_new_relation(source_url, destination_url,access_type)
        logging.debug('New connection from webpage [%s] to [%s] of type [%s]', source_url,destination_url,access_type,)
