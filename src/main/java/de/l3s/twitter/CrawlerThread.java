package de.l3s.twitter;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 *  a crawler thread for parallelization 
 */
public class CrawlerThread implements Runnable {
	
	public static final Logger LOG = LoggerFactory.getLogger(StreamHandler.class);
	
	private AtomicInteger counter;
	
	public Configuration conf;

	public TwitterCrawler c;
	
	public String[] args;
	
	public CrawlerThread(AtomicInteger counter, Configuration conf, TwitterCrawler c, String[] args) {
		this.counter = counter;
		this.conf = conf;
		this.c = c;
		this.args = args;
	}

	@Override
	public void run() {
		try {
			counter.incrementAndGet();
			LOG.info("Crawler starts " + counter.get());
			int res = ToolRunner.run(conf, c, args);
			counter.decrementAndGet();
			LOG.info("Crawler finished " + counter.get());
			System.exit(res);
		} catch (Exception e) {
			LOG.error("Error: " + e.getMessage());
		}
		
	}

}
