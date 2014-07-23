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

import de.l3s.nutch.utils.NSecurityManager;

public class StreamHandler {
	public static final Logger LOG = LoggerFactory.getLogger(StreamHandler.class);

	private final static String CONSUMER_KEY = "8E7MraQc163102j0G3vIKIjnR";
	private final static String CONSUMER_KEY_SECRET = "4rnEAEk7qP8cJ8mOnuvOIymo9JIXGRVIjAQcrVcclDRWv7jyB2";

	private final static String TOKEN = "27426413-txA0jPBPMwiiZRNTjbZeksywONnDeQ6mTPeFORIZo";
	private final static String TOKEN_SECRET = "GqOGRX3BCBC9TGBr7bV8LluAFr5XvW74hC5ZTMaienk18";

	final static int MAX_BUFF_SIZE = 10000;

	protected static final int MAX_DRAIN_SIZE = 1000;

	protected static final int MAX_NO_CRAWLER = 3;

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
	 * Main entry of this application.
	 * Parallel crawlers
	 * @param args arguments doesn't take effect with this example
	 * @throws Exception 
	 */
	public static void parallelStart(final String[] args) throws Exception {
		System.setSecurityManager(new NSecurityManager());

		TwitterStream twitterStream = new TwitterStreamFactory().getInstance();
		accessAuth(twitterStream);

		//Buffer
		//TODO: redesign the buffering mechanism so that no urls lost 
		//keep track of url from which tweet id
		//the tmp url file dont work
		final LinkedBlockingQueue<String> tweets_buff = Queues.newLinkedBlockingQueue(MAX_BUFF_SIZE * 2);

		StatusListener listener = new StatusListener() {

			List<URLEntity> urls = Lists.newArrayList();
			Configuration conf = NutchConfiguration.create();
			TwitterCrawler c = new TwitterCrawler();
			AtomicInteger noCrawler = new AtomicInteger(0);
			@Override
			public void onStatus(Status status) {

				//extract URL from tweet and put in queue
				//when queue is full flush to injectorjob
				urls = new LinkedList<URLEntity>(Arrays.asList(status.getURLEntities()));

				if (tweets_buff.size() <= MAX_BUFF_SIZE) {
					if (!urls.isEmpty()) {
						
						for (URLEntity url : urls) {
							//get URL text
							tweets_buff.add(url.getURL());
						}	
						urls.clear();
					}
				}
				if (tweets_buff.size() % 100 == 0) LOG.info("TwitterMain: number of URLs in buff: " + tweets_buff.size() );
				if (tweets_buff.size() > MAX_DRAIN_SIZE && noCrawler.get() < MAX_NO_CRAWLER) {

					LOG.info("number of active thread: " + noCrawler.get());

					LOG.info("Start crawling from buffer: " + tweets_buff.size());
					tweets_buff.drainTo(TwitterCrawler.urls, MAX_DRAIN_SIZE);
					LOG.info("buffer size: " + tweets_buff.size());

					ExecutorService e = Executors.newFixedThreadPool(MAX_NO_CRAWLER);

					CrawlerThread crawlerT = new CrawlerThread(noCrawler, conf, c , args);

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
	 * Single start
	 * @param args
	 * @throws Exception
	 */
	public static void start(final String[] args) throws Exception {
		System.setSecurityManager(new NSecurityManager());

		TwitterStream twitterStream = new TwitterStreamFactory().getInstance();
		accessAuth(twitterStream);

		//Buffer
		//TODO: redesign the buffering mechanism so that no urls lost 
		//keep track of url from which tweet id
		//the tmp url file dont work
		final LinkedBlockingQueue<String> tweets_buff = Queues.newLinkedBlockingQueue(MAX_BUFF_SIZE * 2);

		StatusListener listener = new StatusListener() {

			List<URLEntity> urls = Lists.newArrayList();
			Configuration conf = NutchConfiguration.create();
			TwitterCrawler c = new TwitterCrawler();
			AtomicInteger noCrawler = new AtomicInteger(0);
			@Override
			public void onStatus(Status status) {

				//extract URL from tweet and put in queue
				//when queue is full flush to injectorjob
				urls = new LinkedList<URLEntity>(Arrays.asList(status.getURLEntities()));

				if (tweets_buff.size() <= MAX_BUFF_SIZE) {
					if (!urls.isEmpty()) {
						for (URLEntity url : urls) {
							//get URL text
							tweets_buff.add(url.getURL());
						}	
						urls.clear();
					}
				}
				if (tweets_buff.size() % 100 == 0) LOG.info("TwitterMain: number of URLs in buff: " + tweets_buff.size() );
				if (tweets_buff.size() > MAX_DRAIN_SIZE && noCrawler.get() < MAX_NO_CRAWLER) {

					LOG.info("number of active thread: " + noCrawler.get());

					LOG.info("Start crawling from buffer: " + tweets_buff.size());
					tweets_buff.drainTo(TwitterCrawler.urls, MAX_DRAIN_SIZE);
					LOG.info("buffer size: " + tweets_buff.size());


					//TODO: start crawler

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
}
