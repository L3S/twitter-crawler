package de.l3s.nutch.listener;

import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.conf.Configuration;

import de.l3s.crawl.Crawler;
import de.l3s.twitter.StreamHandler;
import de.l3s.twitter.TwitterCrawler;

/*
 * This class controls when to start the new crawler.
 * This allows a sequential execution (one-by-one) of the crawler.
 * When to start depends on the status of the queue (size) and the running 
 * crawler (finished)
 */
public class StatusWaiter<T> extends Thread {
	
	public final TwitterCrawler c;
	
	protected final QueueWaiterListenerImpl listener;
	
	public final LinkedBlockingQueue<T> queue;
	
	public final Crawler crawler = new Crawler();
	protected Configuration conf;
	
	public StatusWaiter (TwitterCrawler c, LinkedBlockingQueue<T> queue, QueueWaiterListenerImpl listener) {
		this.listener = listener;
		this.c = c;
		this.queue = queue;
	}
	
	public void setConf (Configuration conf) {
		this.conf = conf;
	}
	
	@Override
	public void run() {
		while (!isInterrupted()) {
			if (TwitterCrawler.finished && queue.size() >= StreamHandler.MAX_DRAIN_SIZE) {
				listener.runCrawler(conf, crawler);
			}
 		}
	}

}
