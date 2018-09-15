
import urlparse
class I2PWebpagesCommon():
    """
    Represent a collection of links
    """

    #start_urls=['http://www.inr.i2p','http://www.stats.i2p', 'http://www.zzz.i2p','http://www.forums.i2p', 'http://www.planet.i2p']
    #start_urls=['http://www.echelon.i2p','http://www.secure.thetinhat.i2p','http://www.forums.i2p', 'http://www.planet.i2p']

    #start_urls=['http://www.anoncoin.i2p','http://www.planet.i2p']
    #start_urls=['http://www.i2pwiki.i2p']
    #start_urls=['http://www.secure.thetinhat.i2p']
    #start_urls=['http://www.i2pforum.i2p']
    start_urls=[]
    def contains(self, link):
        """
        Check if a link exists in the queue of links
        :param link: links to check
        :return: True if the links exists or False if not
        """
        website= "{0.scheme}://{0.netloc}".format(urlparse.urlsplit(link))
        have_link=False
        if website in self.links_list:
            #Sitio web ha sido visitado
            website_links=self.links_list[website]
            if link in  website_links:
                #El enlace ha sido visitado
                have_link=True

        return have_link

    def website_known(self,website):
        in_queue=False
        if website in self.links_list:
           in_queue=True
        return in_queue
