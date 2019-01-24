__author__ = 'roycehaynes'

import scrapy_rabbitmq.connection as connection

from scrapy.spiders import Spider
from scrapy import signals
from scrapy.exceptions import DontCloseSpider


class RabbitMQMixin(object):
    """ A RabbitMQ Mixin used to read URLs from a RabbitMQ queue.
    """

    rabbitmq_key = None
    server = None

#    def __init__(self):
#        self.server = None
        
    def start_requests(self):
        """Returns a batch of start requests from redis."""
        return self.next_request()

    def setup_rabbitmq(self):
        """ Setup RabbitMQ connection.

            Call this method after spider has set its crawler object.
        :return: None
        """

        if self.crawler.settings.get('RABBITMQ_QUEUE_NAME', None):
            self.rabbitmq_key = self.crawler.settings.get('RABBITMQ_QUEUE_NAME', None)
            self.rabbitmq_key = self.rabbitmq_key % {'name':self.name}
            self.crawler.settings.frozen = False
            self.crawler.settings.set('RABBITMQ_QUEUE_NAME', self.rabbitmq_key)
            self.crawler.settings.frozen = True
            
        if not self.rabbitmq_key:
            self.rabbitmq_key = '{}:start_urls'.format(self.name)

        self.server = connection.from_settings(self.crawler.settings)
        self.crawler.signals.connect(self.spider_idle, signal=signals.spider_idle)
        self.crawler.signals.connect(self.item_scraped, signal=signals.item_scraped)

    def next_request(self):
        """ Provides a request to be scheduled.
        :return: Request object or None
        """
        
        if not self.server.is_open:
            #重新打开连接
            self.server = connection.from_settings(self.crawler.settings)

        method_frame, header_frame, url = self.server.basic_get(queue=self.rabbitmq_key)

        if url:
            req = self.make_requests_from_url(bytes_to_str(url))
            yield req
            #return req

    #与scrapy-redis一致
    def schedule_next_requests(self):
        return schedule_next_request()
    
    def schedule_next_request(self):
        """ Schedules a request, if exists.

        :return:
        """
#        req = self.next_request()

#        if req:
        for req in self.next_request():
            self.crawler.engine.crawl(req, spider=self)

    def spider_idle(self):
        """ Waits for request to be scheduled.

        :return: None
        """
        self.schedule_next_request()
        raise DontCloseSpider

    def item_scraped(self, *args, **kwargs):
        """ Avoid waiting for spider.
        :param args:
        :param kwargs:
        :return: None
        """
        self.schedule_next_request()


class RabbitMQSpider(RabbitMQMixin, Spider):
    """ Spider that reads urls from RabbitMQ queue when idle.
    """
    
    @classmethod
    def from_crawler(self, crawler, *args, **kwargs):
        obj = super(RabbitMQSpider, self).from_crawler(crawler, *args, **kwargs)
        obj.setup_rabbitmq()
        return obj

    def set_crawler(self, crawler):
        super(RabbitMQSpider, self).set_crawler(crawler)
        self.setup_rabbitmq()


import six
def bytes_to_str(s, encoding='utf-8'):
    """Returns a str if a bytes object is given."""
    if six.PY3 and isinstance(s, bytes):
        return s.decode(encoding)
    return s