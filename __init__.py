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

import lxml
from lxml.html.soupparser import fromstring
from lxml import etree


class ConnectionsNumberWarning(UserWarning):
    """Number of connections is too big"""
    pass


class UrlTooLongWarning(UserWarning):
    """Url up to 1024 characters expected"""
    pass


class ConnectionsNumberException(Exception):
    """Minimum 1 connection required"""
    pass


class CacheConfigurationException(Exception):
    """Caching can only work when cache_root provided"""
    pass


class Struct:
    """
    http://stackoverflow.com/questions/1305532/convert-python-dict-to-object
    """
    def __init__(self, **entries):
        self.__dict__.update(entries)


class Browser(object):
    """
    CurlBrowser performs single or simultaneously requests to remote URLs
    trying to mimic actual browser as much as possible, can be configured
    to cache files, use different user-agent strings, headers, and delay time
    between requests.

    Uses PyCurl but tries to hide it's complexity and do things more
    high level without sacrifiying too much perfomance or adding memory
    consuption.

    You will need to recompile libcurl with c-ares support if you want
    to get maximum perfomance when doing mass fetch.


    Any gzip data will be returned uncompressed
    """

    def __init__(self, **kwargs):
        """Browser settings may be passed as **kwargs or Browser will
        use default sane values.
        You may want to change cache_timeout and cache_method if you want
        to use Browser for mass fetching.

        Because browser has many config parameters, it's recommended to use
        it following approach to configuration:


        config = {
            "cache_method": ...
            "cookies_file": ..,
            "cache_root": ...
        }

        browser = Browser(**config)

        """

        # Show debug messages when data is loaded from cache file
        self.show_cache_hits = kwargs.get("show_cache_hits", False)

        #  Possible values are:
        #    'never' - always perform actual request
        #
        #    'forever' - perform reuest only on first time
        #    and use cached file every next one
        #
        #    'expire' - check cache file 'mtime' and perform request
        #    if it's outdated
        #
        # setting 'expire' or 'forever' is useful when you need to run some
        # code many times but don't want to stress server too much, switching
        # mode at anytime will use new caching rules disregard cache files on
        # disk, but you will need to delete them manually if you don't
        # need them
        #
        # Data is cached to file in cache_root direcopry, md5 of full
        # url with all parameters will be used as filename
        self.cache_method = kwargs.get("cache_method", 'never')

        # Path to directory where cache file would be stored
        # It's required if you set 'cache_method' to 'forever' of 'expire'
        self.cache_root = kwargs.get("cache_root", None)

        # Time in seconds for cache to expire, will be used only if
        # cache_method = 'expire', default = 10 minutes
        self.cache_expiration = kwargs.get("cache_expiration", 600)

        if self.cache_method in ['expire', 'forever'] and not self.cache_root:
            raise CacheConfigurationException("""You need to set 'cache_root'
            if you want to use caching""")

        # By default browser will desguise as Firefox 3.6 on Ubuntu
        # you can user_agent string for any browser
        # from http://www.user-agents.org/
        user_agent = "User-Agent: Mozilla/5.0 (Macintosh; U; Intel" \
        "Mac OS X 10.6; ru; rv:1.9.2.13) Gecko/20101203 Firefox/3.6.13"

        self.user_agent = kwargs.get("user_agent", user_agent)

        # Browser will wait 'connection_timeout' seconds for server to respond
        # before dropping connection
        self.connection_timeout = kwargs.get("connection_timeout", 10)

        # Browser will wait 'transfer_timeout' seconds for server to finish
        # sending data before dropping connection, you may need to raise
        # 'connection_timeout' and 'timeout' if you perform mass fetching and
        # you don't have anough CPU or network capacity or lower connections
        # count, which is a better solution
        self.transfer_timeout = kwargs.get("transfer_timeout", 10)

        # Path to a file where cookies will be stored, by default
        # cookies are disabled
        self.cookies_file = kwargs.get("cookies_file", None)

        # Should Browser follow server redirects, defaults to True,
        # as any real-life browser, but may be useful to disable it
        # if you need to get data from redirect page (some websites do this)
        self.follow_redirects = kwargs.get("follow_redirects", True)

        # Drop connection if server tries to return more than 'max_size'
        # bytes, default to 1mb which is more than enough for most pages
        self.max_size = kwargs.get("max_size", 1024 * 1024)

        self.logger = logging.getLogger("Browser")
        self.logger.debug("PycURL %s (compiled against 0x%x)" %
                          (pycurl.version, pycurl.COMPILE_LIBCURL_VERSION_NUM))

    def __curl_init(self, curl):
        """Inialize curl object and set settings"""
        #self.curl = pycurl.Curl()

        if self.follow_redirects:
            curl.setopt(pycurl.FOLLOWLOCATION, 1)
            curl.setopt(pycurl.MAXREDIRS, 5)

        curl.setopt(pycurl.CONNECTTIMEOUT, self.connection_timeout)
        curl.setopt(pycurl.TIMEOUT, self.transfer_timeout)

        # Required for mass fetch
        curl.setopt(pycurl.NOSIGNAL, 1)

        # USe IPv4 for now, it's faster and safer for time being
        curl.setopt(pycurl.IPRESOLVE, pycurl.IPRESOLVE_V4)

        curl.setopt(pycurl.USERAGENT, self.user_agent)

        if self.cookies_file:
            curl.setopt(pycurl.COOKIEFILE, self.cookies_file)
            curl.setopt(pycurl.COOKIEJAR, self.cookies_file)

        accept = "Accept: text/html,application/xhtml+xml"\
        ",application/xml;q=0.9,*/*;q=0.8"

        headers = list()
        headers.append("Accept: %s" % accept)
        headers.append("Accept-Language: ru-ru,ru;q=0.8,en-us;q=0.5,en;q=0.3")
        headers.append("Accept-Encoding: gzip,deflate")
        headers.append("Accept-Charset: utf-8, windows-1251;q=0.7,*;q=0.7")
        headers.append("Keep-Alive: 115")
        headers.append("Connection: keep-alive")

        curl.setopt(pycurl.HTTPHEADER, headers)

        curl.setopt(pycurl.MAXFILESIZE, self.max_size)

    def __normalize_data(self, data, cheaders):
        """Check data for gzip compression and decompress if possible"""
        headers = {}
        for entry in cheaders.split("\n"):
            if ":" in entry:
                (key, value) = entry.split(":", 1)
                headers[key] = value.strip()

        if headers.get("Content-Encoding", "") == "gzip":
            try:
                self.logger.debug("decompressimg gzip stream")
                # Sometimes server can report gzip but send plain text
                data = zlib.decompress(data, 15 + 32)
            except zlib.error:
                self.logger.exception("gzip decompression error")

        return data

    def __get_filename(self, url, method):
        """Construct filename for cache"""
        url_hash = hashlib.md5(url).hexdigest()
        return os.path.join(self.cache_root, url_hash) + "." + method

    def __load_cached_response(self, url, method, uid=None):
        """Save data if caching is enabled"""

        if self.cache_method == 'never':
            return None

        filename = self.__get_filename(url, method)

        cached = False
        if self.cache_method == "forever" and os.path.exists(filename):
            cached = True

        elif self.cache_method == "expire" and os.path.exists(filename) and \
            time.time() - os.path.getmtime(filename) < self.cache_expiration:
            cached = True

        if cached:
            self.logger.debug("Getting page %s from cache: %s" %
                              (url, filename))

            data = open(filename).read()
            metadata = json.load(open(filename + ".meta"))

            result = {'file': filename,
                       'data': data,
                       'source': "cache",
                       'result': "ok",
                       'url': metadata["url"],
                       'code': metadata["code"],
                       'content_type': metadata["content_type"],
                       'id': uid}

            return Struct(**result)

        else:
            return None

    def __set_request_params(self, params, url, method, curl):
        """Set params for GET or POST request"""
        url = url.strip()

        params = ["%s=%s" % (key, value) for key, value in params.items()]
        params_str = "&".join(params)

        if method == "POST":
            curl.setopt(pycurl.POST, 1)
            curl.setopt(pycurl.POSTFIELDS, params_str)

        if method == "GET":
            url = url + "?" + params_str
        # PyCurl can accept only strings, but url can be unicode object
        return str(url)

    def fetch(self, url, method="GET", ref=None, **kwargs):
        """Get data of one page by performing GET or POST request, result value
        is a dict"""

        curl = pycurl.Curl()
        self.__curl_init(curl)

        params = kwargs.get("params", None)
        if params:
            url = self.__set_request_params(params, url, method, curl)

        self.logger.debug("Fetching single url [%s]" % url)

        result = self.__load_cached_response(url, method)
        if result:
            return result

        self.logger.debug("Fetching from remote server")

        # Check if data can be returned from cache
        if ref is not None:
            curl.setopt(pycurl.REFERER, ref)

        curl.setopt(pycurl.URL, url)

        strbuff = cStringIO.StringIO()
        curl.setopt(pycurl.WRITEFUNCTION, strbuff.write)

        headers = cStringIO.StringIO()
        curl.setopt(pycurl.HEADERFUNCTION, headers.write)

        try:
            curl.perform()
            data = self.__normalize_data(strbuff.getvalue(), headers.getvalue())

            result = Struct(**{
                'result': 'ok',
                'source': 'web',
                'data'  : data,
                'code'  : curl.getinfo(pycurl.HTTP_CODE),
                'content_type': curl.getinfo(pycurl.CONTENT_TYPE),
                'url'   : url,
                'method': method,
            })

            result.file = self.__cache_response(result)

            return result
        except pycurl.error:
            self.logger.exception("Error downloading page")
            return Struct(**{'result': 'error',
                    'source': 'web',
                    'data': None,
                    'code': curl.getinfo(pycurl.HTTP_CODE),
                    'url': url,
                    'method': method
            })

            
    def __cache_response(self, data):
        """Save response data and request metadata if caching is enabled"""
        filename = self.__get_filename(data.url, data.method)

        if self.cache_method in ["expire", "forever"]:
            data_file = open(filename, 'w')
            data_file.write(data.data)
            data_file.close()

            json.dump({'code': data.code,
                'content_type': data.content_type,
                'url': data.url
            }, open(filename + ".meta", 'w'))


            return filename

    def multi_fetch(self, url_requests, num_conn=100, percentile=100):
        """Get no more than 'percentile' % of requested urls,
        limiting simultaneously connections to 'num_conn'

        Set percentfile = 100(default) if you need to get every page
        (getting urls can still fail based on transfer and connection timeouts)

        Set it to lower value (like 90-95) if you care about speed
        and don't care about getting all the urls.

        You need to pass a list of dicts following this structure


        urls = [{
                    "url": "http://google.com/",
                    "ref": "http://google.com/",
                    "id": 1
                },
                {
                    "url": "http://python.org/",
                    "ref": "http://python.org/",
                    "id": 2
                }]

        entries = browser.multi_fetch(urls)

        Only url is required, you can pass None for 'referer' and 'id'
        'id' will be set for every url returned so you can map them back to
        your data easely(for example, I pass primary key of my db entry
        so I don't need to maintain url-id associative array)

        Based on http://habrahabr.ru/blogs/personal/61960/"""

        queue = []
        results = dict()

        for entry in url_requests:
            url = entry["url"]

            if not url or url[0] == "#":
                continue

            if len(url) > 1024:
                warn("URLs longer than 1024 characters are ignored",
                     UrlTooLongWarning)

                results[url] = Struct({'result': 'error'})
                continue

            result = self.__load_cached_response(url, "GET",
                                                 entry.get("id", None))
            if result:
                results[url] = result
                continue

            queue.append(entry)

        # Queue empty
        if not len(queue):
            return results

        num_urls = len(queue)
        num_conn = min(num_conn, num_urls)

        if num_conn < 1:
            raise ConnectionsNumberException("""Number of concurent connections
            can't be less than 1""")

        # Using 200-300 connections maximum is a good idea, but choose
        # depending on CPU load and network perfomance
        # If you fetch pages from one server, you should limit maximum
        # connections to 5-10
        if num_conn > 1024:
            warn("You should lower number of concurent connections",
                 ConnectionsNumberWarning)

        self.logger.debug("Getting %s URLs using %s connections" %
                          (num_urls, num_conn))

        mcurl = pycurl.CurlMulti()
        mcurl.handles = []
        for _ in range(num_conn):
            curl = pycurl.Curl()
            #curl.fp = None

            self.__curl_init(curl)
            mcurl.handles.append(curl)

        freelist = mcurl.handles[:]
        num_processed = 0
        bailout = 0

        while num_processed < num_urls:

            # Got enough results
            if bailout:
                break

            while queue and freelist:
                url_data = queue.pop(0)

                url = str(url_data["url"])

                curl = freelist.pop()

                curl.setopt(pycurl.URL, url)
                curl.res = cStringIO.StringIO()
                curl.setopt(pycurl.WRITEFUNCTION, curl.res.write)
                curl.headers = cStringIO.StringIO()
                curl.setopt(pycurl.HEADERFUNCTION, curl.headers.write)

                if url_data.get("ref", None):
                    curl.setopt(pycurl.REFERER, url_data["ref"])

                mcurl.add_handle(curl)

                curl.url = url

                if id in url_data:
                    curl.id = url_data["id"]
                else:
                    curl.id = None

            while 1:
                ret, _ = mcurl.perform()
                if ret != pycurl.E_CALL_MULTI_PERFORM:
                    break

            while 1:
                num_q, ok_list, err_list = mcurl.info_read()
                for curl in ok_list:
                    self.logger.debug("Succesfull fetched %s" % curl.url)
                    mcurl.remove_handle(curl)

                    data = self.__normalize_data(curl.res.getvalue(),
                                               curl.headers.getvalue())

                    result = Struct(**{
                       'result': 'ok',
                       'source': 'web',
                       'content_type': curl.getinfo(pycurl.CONTENT_TYPE),
                       'code': curl.getinfo(pycurl.HTTP_CODE),
                       'data': data,
                       'id': curl.id,
                       'url': curl.url,
                       'method': "GET"
                    })

                    
                    result.file = self.__cache_response(result)
                    
                    results[curl.url] = result

                    if self.cache_method in ["expire", "forever"]:

                        data_file = \
                            open(self.__get_filename(curl.url, "GET"), 'w')

                        data_file.write(data)
                        data_file.close()

                    freelist.append(curl)
                    time.sleep(1.0)

                for curl, errno, errmsg in err_list:
                    self.logger.debug("Error fetching %s" % curl.url)
                    curl.fp = None
                    mcurl.remove_handle(curl)
                    results[curl.url] = Struct(**{'result': 'error',
                                            'error': "%s %s" % (errno, errmsg),
                                            'id': curl.id,
                                            'url': curl.url})
                    freelist.append(curl)

                num_processed = num_processed + len(ok_list) + len(err_list)

                if num_urls:
                    if float(num_processed) / num_urls * 100 > percentile:
                        bailout = 1
                        break

                if not num_q:
                    break

            mcurl.select(1.0)

        mcurl.close()

        return results


    def __get_str(self, element, info):
        """
        res = element.xpath("string()").\
        replace("\\n", "\n").\
        replace("\\t", "\t").\
        replace("\\r", "\r").strip()
        """
        if "parser" in info and element:
            element = info["parser"](element)

        return element

    def __extract_data(self, element, extractor):
        """Actual parsing and extraction of data from lxml element or string"""

        # if we got string as input and we need to perform xpath query in
        # any off the extractor's child
        if isinstance(element, basestring):
            data_str = element

            for field, info in extractor.fields.items():
                if "xpath" in info:
                    try:
                        data_xml = fromstring(element)
                    except:
                        data_xml = None
                        self.logger.exception("Couldn't parse element")

                    break

        # if we got tree element and we need to perform regexp
        else:

            data_xml = element

            for field, info in extractor.fields.items():
                if "regexp" in info:
                    data_str = etree.tostring(element)
                    break

        result = dict()

        for field, info in extractor.fields.items():

            if "xpath" in info:

                # Get one element, "mode" can be ommited in this case
                if not "mode" in info or info["mode"] == "single":
                    try:
                        data_list = list()

                        for entry in data_xml.xpath(info["xpath"]):
                            if isinstance(entry, etree._ElementStringResult) \
                            or isinstance(entry, etree._ElementUnicodeResult) \
                            or isinstance(entry, unicode)\
                            or isinstance(entry, str):
                                data_list.append(entry)
                            else:
                                data_list.append(lxml.html.tostring(entry))
 
                        felement = "".join(data_list)

                        result[field] = unicode(self.__get_str(felement, info))

                    except IndexError:
                        self.logger.exception("Couldn't execute xpath search [%s]" % info["xpath"])
                        result[field] = None
                        
                elif info["mode"] == "multi":

                    self.logger.debug("xpath_multi [%s]" % info["xpath"])
                    results = list()

                    if data_xml is not None:
                        elements = data_xml.xpath(info["xpath"])

                        for felement in elements:
                            results.append(
                                self.__get_str(felement, info)
                            )

                    result[field] = results
                    
                elif info["mode"] == "loop":
                    
                    self.logger.debug("xpath_multi [%s]" % info["xpath_multi"])
                    elements = data_xml.xpath(info["xpath_multi"])
                    results = list()
                    for felement in elements:

                        results.append(
                            Struct(**self.__extract_data(felement, info["items"]))
                        )

                    result[field] = results

            elif "regexp" in info:
                groups = re.search(info["regexp"], data_str)
                try:
                    res = groups.group("content")

                    # Use parser if provided
                    if "parser" in info:
                        res = info["parser"](res)

                    result[field] = unicode(res)

                except AttributeError:
                    self.logger.exception("Couldn't execute regexp")
                    result[field] = None

        # add lxml reference for additional document parsing by user
        result["lxml_handle"] = data_xml

        return result

    def extract(self, data, extractor):
        """Get parts of page and return as Struct"""
        #return Struct(**self.__extract_data(data.encode('string_escape'),
        return Struct(**self.__extract_data(data, extractor))




def init_simple_logger():

    browser_logger = logging.getLogger("Browser")
    browser_logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler()
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    browser_logger.addHandler(handler)

    search_logger = logging.getLogger("SearchParser")
    search_logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler()
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    search_logger.addHandler(handler)

    search_logger = logging.getLogger("ListParser")
    search_logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler()
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    search_logger.addHandler(handler)

    return browser_logger, search_logger

"""
logger = logging.getLogger("Browser")
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

config = {
    "cache_method": "never"
}

browser = Browser(**config)

urls = [{
            "url": "http://google.com/",
            "ref": "http://google.com/",
            "id": 1
        },
        {
            "url": "http://python.org/",
            "ref": "http://python.org/",
            "id": 2
        },
        {
            "url": "http://stackoverflow.com/",
            "ref": "http://stackoverflow.com/",
            "id": 3
        }]


entries = browser.multi_fetch(urls)

for url, entry in entries.items():

    #print len(entry.data)
    print url, entry.status, len(entry.data), entry.id


"""    