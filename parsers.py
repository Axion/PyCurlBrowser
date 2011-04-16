# -*- coding: utf-8 -*-
"""PyCurlBrowser module is used to simultaneously fetch thousands of pages
in real world, where urllib fails because of hanging sockets"""

# pylint: disable=R0902,W0142,R0903,R0201,R0914,R0912,R0915,R0913

import cStringIO
import hashlib
import json
import logging
import os
import pycurl
import re
import time
import zlib

from warnings import warn

from lxml.html.soupparser import fromstring
from lxml import etree


class ParserNotConfigured(Exception):
    """Raised when one of the required functions is not overrided"""
    pass

class SimpleParser(object):
    """Parser thet extracts data from single page

    Example: You want to extrac every image or link from a single page
    """

    # Extractor instance to get data from listing page or link in
    # case of useing page_data_Extractor
    data_extractor = None

    def __init__(self, browser):
        self.browser = browser
        self.logger = logging.getLogger("SimpleParser")

    def fetch(self, url):
        data = self.browser.fetch(url).data
        return self.browser.extract(data, self.data_extractor)
    

class ListParser(object):
    """Use this parser if you want to get data from any kind of multi-page
    listing where first page contains info about total page count and every
    page contains some elements you wish to extract

    Examle: you want to get links to webpages from google search
    """
# Extractor instance to get page count
    count_extractor = None

    # Extractor instance to get data from listing page or link in
    # case of useing page_data_Extractor
    list_data_extractor = None

    # pylint: disable=W0613
    def _construct_url(self, num):
        """You need to override this function and provide your own
        which can construct url base on it's 'base' part and page number passed
        as 'num'
        """
        raise ParserNotConfigured("You need to override _construct_url")
    # pylint: enable=W0613

    def __extract_page_data(self, data):
        """Transform string data of web page into python object"""
        return self.browser.extract(data, self.list_data_extractor)

    def __get_page_count(self, data):
        """Get count of pages which can be fetched"""
        return self.browser.extract(data, self.count_extractor).count

    def __get_page(self, num):
        """Get data of one page by it's number in search result"""
        data = self.browser.fetch(self._construct_url(num))
        return data.data

    def fetch(self):
        """Get and return data as struct"""
        results = list()

        data = self.__get_page(1)
        results.extend(self.__extract_page_data(data).items)


        if self.count_extractor or self.max_pages:
            """If we can get maximum number of pages or we have a known last page"""
            if self.count_extractor:
                ct = self.__get_page_count(data)

                self.logger.info("Last page num is %s" % ct)

                if not ct:
                    pages = []
                else:
                    count = int(ct)

                    if self.max_pages:
                        pages = xrange(2, max(self.max_pages, count) - 4)
                    else:
                        pages = xrange(2, count - 1)

            else:
                pages = xrange(2, self.max_pages + 1)

            for page_num in pages:
                self.logger.debug("Working on page %s" % page_num)
                data = self.__get_page(page_num)

                results.extend(self.__extract_page_data(data).items)

                if self.stop_word and self.stop_word in data:
                    break

        else:
            """Last page can'be guessed untill it's reached, iterate untill we found it by using stop_function"""

            if not self.stop_function(data):
                """First page may be the last. no need to fetch page 2 in this case"""
                page_num = 2
                while True:
                    self.logger.debug("Working on page %s" % page_num)
                    data = self.__get_page(page_num)

                    results.extend(self.__extract_page_data(data).items)

                    if self.stop_function(data):
                        break

                    page_num += 1


        return results

    def __init__(self, browser, max_pages=None, stop_word=None):

        self.browser = browser
        self.max_pages = max_pages
        self.stop_word = stop_word
        self.logger = logging.getLogger("ListParser")


class SearchParser(ListParser):
    """SearchParses is an extended version of ListParses, that would use links
    in listing to descend deeper to linked pages and extract data from there

    Example: you want to get page contents from google search
    """

    # Extractor instance to get data from every info page
    page_data_extractor = None

    def fetch(self):
        results = super(SearchParser,self).fetch()

        lresults = list()
        links = [{"url": link} for link in results]
        entries = self.browser.multi_fetch(links, num_conn=5)

        for url, entry in entries.items():
            try:
                data = self.browser.extract(entry.data,
                                        self.page_data_extractor)
                data.link = url
                lresults.append(data)
            except (KeyboardInterrupt, SystemExit):
                raise
            except etree.XPathEvalError:
                self.logger.exception("Couldn't exract page data")

        return lresults

class Extractor(object):
    """This class may be extended to provide custom parsing functionality
    You should init it with a dict, where every key is a name of resulting
    field and it's value is a dict with one of the parsing options. like this

    Extractor({
        "property_name": {
            "type_key": "type_value",
            ...options...
        }
    })

    type_key can be one of the following:

    regexp, type_value = a string for creation of regexp or a regexp instance

    xpath, type_value = a string representing xpath

    options can be:
        - mode
          - single - first found element would be assigned to property
          - multi - every element found would be put into list
          - loop - every element would be put into list as new dict entry
          , and it's 'items' property would be used to populate dict's values

        - parser after getting string representation of node it will be passed
        to function defined in parser

    xpath_multi, type_value = a string representing xpath,


    """
    def __init__(self, fields):
        self.fields = fields
