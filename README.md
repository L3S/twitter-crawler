A Twitter crawler module to resolve and store URL content

Specifications:

- FetcherJobs should be run in a serial way to ensure that we adhere to
the rate limits (politeness)
- we have three modules: twitter-crawler, icrawl-url-expander,
icraw-injector
- the twitter crawler hands all URLs to the url expander
- the url expander has distributor, which has a chain of URL shortener
plugins
- the distributor checks, if the URL has already been crawled
- if not, it is handed to the plugins
- each plugin checks, if it can handle the URL
- if not, the URL is handed over to the next plugin
- if yes, the plugin checks, if the URL is in the queue
- if not, it is added to the queue
- regularly, the top elements of the queue are queried via a
service-specific API
- the expanded URLs are given to the distributor, which gives them to
the injector and to the redirect archiver
- the injector puts them into the crawl db for crawling
- the redirect archiver stores the association between short URL and
long URL, including metadata
- the plugin regularly checks, if there haven't been any additions to
the queue and if so, resolves the URLs in the queue
