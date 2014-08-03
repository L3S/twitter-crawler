package de.l3s.crawl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.l3s.twitter.TwitterCrawler;

public class Crawler {
	private static final Logger LOG = LoggerFactory.getLogger(Distributor.class);
	/* The Twitter crawler that handles the URLs */
	static final TwitterCrawler c = new TwitterCrawler();
	
	public void run(Configuration conf) {
		try {
			ToolRunner.run(conf, c, null);
		} catch (Exception e) {
			LOG.error(e.getMessage());
		}

	}

}
