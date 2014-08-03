package de.l3s.nutch.listener;

import org.apache.hadoop.conf.Configuration;

import de.l3s.crawl.Crawler;

interface QueueWaiterListener<T> {
    public void runCrawler(Configuration conf, Crawler crawler);
}
