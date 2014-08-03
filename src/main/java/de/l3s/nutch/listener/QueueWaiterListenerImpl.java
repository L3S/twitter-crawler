package de.l3s.nutch.listener;

import org.apache.hadoop.conf.Configuration;

import de.l3s.crawl.Crawler;

public class QueueWaiterListenerImpl implements QueueWaiterListener<String> {

	@Override
	public void runCrawler(Configuration conf, Crawler crawler) {
		crawler.run(conf);		
	}

}
