package de.l3s.nutch.listener;

import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.conf.Configuration;

import de.l3s.crawl.Crawler;
import de.l3s.twitter.StreamHandler;

/*
 * This class controls when to start the new crawler.
 * This allows a parallelization execution (multi-thread) of the crawler.
 * When to start depends solely on the status of the queue (size)
 */
public class QueueWaiter<T> extends Thread {

	public final LinkedBlockingQueue<T> queue;
	protected final QueueWaiterListenerImpl listener;
	protected final Crawler crawler = new Crawler();
	protected Configuration conf;

	public QueueWaiter(LinkedBlockingQueue<T> queue, QueueWaiterListenerImpl listener) {
		this.queue = queue;
		this.listener = listener;
	}
	
	public void setConf (Configuration conf) {
		this.conf = conf;
	}

	@Override
	public void run() {
		while (!isInterrupted()) {
			if (queue.size() == StreamHandler.MAX_DRAIN_SIZE) 
			{
				System.out.println("Queue size: " + queue.size());
				listener.runCrawler(conf, crawler);
			}
		}
	}
}
