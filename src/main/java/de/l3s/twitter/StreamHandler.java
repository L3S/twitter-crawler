package de.l3s.twitter;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.util.NutchConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.URLEntity;
import twitter4j.auth.AccessToken;
import twitter4j.auth.RequestToken;

import com.google.common.collect.Lists;
import com.google.common.collect.Queues;

import de.l3s.crawl.Distributor;
import de.l3s.nutch.listener.QueueWaiter;
import de.l3s.nutch.listener.QueueWaiterListenerImpl;
import de.l3s.nutch.listener.StatusWaiter;
import de.l3s.nutch.utils.NSecurityManager;


/* This class in handling tweet stream from the twitter API.
 * URLs are extracted from the tweet stream and put into a buffer
 * with fix size. When the buffer is full, it will flush to the Distributor
 * where URLs are resolved. The URLs are then put into the CrawlDB. After this 
 * step, the crawler is called.
 */
public class StreamHandler {
	public static final Logger LOG = LoggerFactory.getLogger(StreamHandler.class);

	private final static String CONSUMER_KEY = "8E7MraQc163102j0G3vIKIjnR";
	private final static String CONSUMER_KEY_SECRET = "4rnEAEk7qP8cJ8mOnuvOIymo9JIXGRVIjAQcrVcclDRWv7jyB2";

	private final static String TOKEN = "27426413-txA0jPBPMwiiZRNTjbZeksywONnDeQ6mTPeFORIZo";
	private final static String TOKEN_SECRET = "GqOGRX3BCBC9TGBr7bV8LluAFr5XvW74hC5ZTMaienk18";

	final static int MAX_BUFF_SIZE = 3000;

	public static final int MAX_DRAIN_SIZE = 1000;

	protected static final int MAX_NO_CRAWLER = 1;

	protected static LinkedBlockingQueue<String> tweets_buff;

	/**
	 * Authentication
	 * @throws TwitterException
	 * @throws IOException
	 */
	public static void auth() throws TwitterException, IOException {
		// The factory instance is re-useable and thread safe.
		Twitter twitter = TwitterFactory.getSingleton();
		twitter.setOAuthConsumer(CONSUMER_KEY, CONSUMER_KEY_SECRET);
		RequestToken requestToken = twitter.getOAuthRequestToken();
		AccessToken accessToken = null;
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		while (null == accessToken) {
			LOG.info("Open the following URL and grant access to your account:");
			LOG.info(requestToken.getAuthorizationURL());
			LOG.info("Enter the PIN(if aviailable) or just hit enter.[PIN]:");
			String pin = br.readLine();
			try {
				if (pin.length() > 0) {
					accessToken = twitter.getOAuthAccessToken(requestToken, pin);
				} else {
					accessToken = twitter.getOAuthAccessToken();
				}
				LOG.info("Access Token: " + accessToken.getToken());
				//27426413-txA0jPBPMwiiZRNTjbZeksywONnDeQ6mTPeFORIZo
				LOG.info("Access Token Secret: " + accessToken.getTokenSecret());
				//GqOGRX3BCBC9TGBr7bV8LluAFr5XvW74hC5ZTMaienk18
			} catch (TwitterException te) {
				if (401 == te.getStatusCode()) {
					LOG.error("Unable to get the access token.");
				} else { 
					te.printStackTrace();
				}
			}
		}
	}

	public static void accessAuth(TwitterStream twitter) {
		AccessToken accessToken = new AccessToken(TOKEN, TOKEN_SECRET);
		twitter.setOAuthConsumer(CONSUMER_KEY, CONSUMER_KEY_SECRET);
		twitter.setOAuthAccessToken(accessToken);
	}


	/**
	 * Parallel crawlers
	 */
	public void parallelStart() {
		System.setSecurityManager(new NSecurityManager());

		TwitterStream twitterStream = new TwitterStreamFactory().getInstance();
		accessAuth(twitterStream);

		tweets_buff = Queues.newLinkedBlockingQueue(MAX_BUFF_SIZE);

		StatusListener listener = new StatusListener() {

			List<URLEntity> urls = Lists.newArrayList();
			List<String> _urls = Lists.newArrayList();
			Configuration conf = NutchConfiguration.create();
			/* The Twitter crawler that handles the URLs */
			TwitterCrawler c = new TwitterCrawler();
			// initialize Distributor
			Distributor distributor = new Distributor();

			AtomicInteger noCrawler = new AtomicInteger(0);
			AtomicInteger crawlerID = new AtomicInteger(0);
			@Override
			public void onStatus(Status status) {

				//extract URL from tweet and put in queue
				//when queue is full flush to injectorjob
				urls = new LinkedList<URLEntity>(Arrays.asList(status.getURLEntities()));

				if (tweets_buff.size() < MAX_BUFF_SIZE) {
					if (!urls.isEmpty()) {
						for (URLEntity url : urls) {
							//get URL text
							String tco;
							//handle t.co by API

							if ((tco = url.getExpandedURL()) != null) {
								//LOG.info("t.co URL: " + url.getText() + " >> " + tco);
								tweets_buff.add(tco);
							}
							else tweets_buff.add(url.getURL());
						}	
						urls.clear();
					}
				}
				if (tweets_buff.size() % 100 == 0) LOG.info("number of URLs in buff: " + tweets_buff.size() );
				if (tweets_buff.size() > MAX_DRAIN_SIZE && noCrawler.get() < MAX_NO_CRAWLER) {

					LOG.info("number of active thread: " + noCrawler.get());

					LOG.info("Start crawling from buffer: " + tweets_buff.size());
					tweets_buff.drainTo(_urls, MAX_DRAIN_SIZE);
					LOG.info("buffer size: " + tweets_buff.size());


					// inject phase
					distributor.setInjector(conf, crawlerID.incrementAndGet());
					distributor.run(_urls);

					ExecutorService e = Executors.newFixedThreadPool(MAX_NO_CRAWLER);

					CrawlerThread crawlerT = new CrawlerThread(noCrawler, conf, c , null);

					e.submit(crawlerT);

				}

			}

			@Override
			public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
				//System.out.println("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());
			}

			@Override
			public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
				LOG.info("Got track limitation notice:" + numberOfLimitedStatuses);
			}

			@Override
			public void onScrubGeo(long userId, long upToStatusId) {
				LOG.info("Got scrub_geo event userId:" + userId + " upToStatusId:" + upToStatusId);
			}

			@Override
			public void onStallWarning(StallWarning warning) {
				LOG.warn("Got stall warning:" + warning);
			}

			@Override
			public void onException(Exception ex) {
				ex.printStackTrace();
			}
		};
		twitterStream.addListener(listener);
		twitterStream.sample();

	}

	/**
	 * parallel nutch start
	 * with queue listener: crawler runs 
	 * when queue is full. Parallel is allowed:
	 * multiple crawlers can run at the same time
	 */
	public void parallelStartWithQueueListener () {

		TwitterStream twitterStream = new TwitterStreamFactory().getInstance();
		accessAuth(twitterStream);
         /* buffer */
		tweets_buff = Queues.newLinkedBlockingQueue(MAX_BUFF_SIZE);
		
		/* a queue listener that controls when to start a crawler */
		final QueueWaiter<String> queue = new QueueWaiter<String>(tweets_buff, new QueueWaiterListenerImpl());


		StatusListener listener = new StatusListener() {
			List<URLEntity> urls = Lists.newArrayList();
			List<String> _urls = Lists.newArrayList();

			Configuration conf = NutchConfiguration.create();


			// initialize Distributor
			Distributor distributor = new Distributor();

			int i = 0;

			@Override
			public void onStatus(Status status) {

				//extract URL from tweet and put in queue
				//when queue is full flush to injectorjob
				urls = new LinkedList<URLEntity>(Arrays.asList(status.getURLEntities()));

				if (queue.queue.size() < MAX_BUFF_SIZE) {
					if (!urls.isEmpty()) {
						for (URLEntity url : urls) {
							//get URL text
							String tco;
							//handle t.co by API

							if ((tco = url.getExpandedURL()) != null) {
								//LOG.info("t.co URL: " + url.getText() + " >> " + tco);
								queue.queue.add(tco);
							}
							else queue.queue.add(url.getURL());
						}	
						urls.clear();
					}
				}
				if (queue.queue.size() % 100 == 0) LOG.info("number of URLs in buff: " + queue.queue.size() );
				if (queue.queue.size() > MAX_DRAIN_SIZE) {

					LOG.info("Start crawling from buffer: " + queue.queue.size());
					queue.queue.drainTo(_urls, MAX_DRAIN_SIZE);
					LOG.info("buffer size: " + queue.queue.size());

					// inject phase
					// inject urls into CrawlDB
					distributor.setInjector(conf, i++);
					distributor.run(_urls);

					queue.setConf(conf);
				}

			}

			@Override
			public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
				//System.out.println("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());
			}

			@Override
			public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
				LOG.info("Got track limitation notice:" + numberOfLimitedStatuses);
			}

			@Override
			public void onScrubGeo(long userId, long upToStatusId) {
				LOG.info("Got scrub_geo event userId:" + userId + " upToStatusId:" + upToStatusId);
			}

			@Override
			public void onStallWarning(StallWarning warning) {
				LOG.warn("Got stall warning:" + warning);
			}

			@Override
			public void onException(Exception ex) {
				ex.printStackTrace();
			}
		};


		twitterStream.addListener(listener);
		twitterStream.sample();

		queue.start();

	}

    /**
     * Single start 
     * One single crawler at a time
     */
	public void start () {

		TwitterStream twitterStream = new TwitterStreamFactory().getInstance();
		accessAuth(twitterStream);
        /* buffer */
		tweets_buff = Queues.newLinkedBlockingQueue(MAX_BUFF_SIZE);

        /* status waiter that controls when to start a crawler */
		final StatusWaiter<String> statusWaiter = new StatusWaiter<String>(new TwitterCrawler(), tweets_buff, new QueueWaiterListenerImpl());


		StatusListener listener = new StatusListener() {
			List<URLEntity> urls = Lists.newArrayList();
			List<String> _urls = Lists.newArrayList();

			Configuration conf = NutchConfiguration.create();

			// initialize Distributor
			Distributor distributor = new Distributor();

			//crawler id
			int i = 0;

			@Override
			public void onStatus(Status status) {

				//extract URL from tweet and put in queue
				//when queue is full flush to injectorjob
				urls = new LinkedList<URLEntity>(Arrays.asList(status.getURLEntities()));

				if (statusWaiter.queue.size() < MAX_BUFF_SIZE) {
					if (!urls.isEmpty()) {
						for (URLEntity url : urls) {
							//get URL text
							String tco;
							//handle t.co by API

							if ((tco = url.getExpandedURL()) != null) {
								//LOG.info("t.co URL: " + url.getText() + " >> " + tco);
								statusWaiter.queue.add(tco);
							}
							else statusWaiter.queue.add(url.getURL());
						}	
						urls.clear();
					}
				}
				if (statusWaiter.queue.size() % 100 == 0) LOG.info("number of URLs in buff: " + statusWaiter.queue.size() );
				if (statusWaiter.queue.size() > MAX_DRAIN_SIZE) {

					LOG.info("Start crawling from buffer: " + statusWaiter.queue.size());
					tweets_buff.drainTo(_urls, MAX_DRAIN_SIZE);
					LOG.info("buffer size: " + statusWaiter.queue.size());

					// inject phase
					// inject urls into CrawlDB
					distributor.setInjector(conf, i++);
					distributor.run(_urls);
					
					statusWaiter.setConf(conf);

					if (i == 1 ) statusWaiter.crawler.run(conf);
					LOG.info("crawler ID (incremental) :" + i);
				}

			}

			@Override
			public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
				//System.out.println("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());
			}

			@Override
			public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
				LOG.info("Got track limitation notice:" + numberOfLimitedStatuses);
			}

			@Override
			public void onScrubGeo(long userId, long upToStatusId) {
				LOG.info("Got scrub_geo event userId:" + userId + " upToStatusId:" + upToStatusId);
			}

			@Override
			public void onStallWarning(StallWarning warning) {
				LOG.warn("Got stall warning:" + warning);
			}

			@Override
			public void onException(Exception ex) {
				ex.printStackTrace();
			}
		};



		twitterStream.addListener(listener);
		twitterStream.sample();
		
		statusWaiter.start();
	}

	/**
	 * Main method
	 */

	public static void main (String[] args) {

		StreamHandler main = new StreamHandler();
		main.start();

	}

}
