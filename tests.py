import unittest

from curlbrowser import Browser, CacheConfigurationException

class CacheConfigured(unittest.TestCase):
    def runTest(self):

        config = {
            "cache_method": "forever"
        }

        self.assertRaises(CacheConfigurationException,
                          Browser, **config)


class FetchOne(unittest.TestCase):
    def runTest(self):        
        config = {
            "cache_method": "never"
        }

        browser = Browser(**config)
        data = browser.fetch("http://google.com/")

        self.assertEqual(data.result, "ok")
        self.assertEqual(data.code, 200)
        #self.assertIn("google", data.data)

class FetchMany(unittest.TestCase):
    def runTest(self):
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

        from pprint import pprint
        pprint(entries)

        self.assertEqual(len(entries), 3)
        
if __name__ == '__main__':
    unittest.main()        