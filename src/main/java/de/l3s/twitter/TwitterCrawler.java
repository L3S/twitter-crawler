package de.l3s.twitter;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.crawl.DbUpdaterJob;
import org.apache.nutch.crawl.GeneratorJob;
import org.apache.nutch.fetcher.FetcherJob;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.parse.ParserJob;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchTool;
import org.apache.nutch.util.ToolUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import de.l3s.crawl.Distributor;


public class TwitterCrawler extends NutchTool implements Tool {
	private static final Logger LOG = LoggerFactory.getLogger(TwitterCrawler.class);

	private static final String ARG_RESOLVE = null;

	public static List<String> urls = Lists.newLinkedList();

	private HashMap<String,Object> results = new HashMap<String,Object>();
	private Map<String,Object> status =
		Collections.synchronizedMap(new HashMap<String,Object>());
	private NutchTool currentTool = null;
	private boolean shouldStop = false;

	@Override
	public Map<String,Object> getStatus() {
		return status;
	}

	private Map<String,Object> runTool(Class<? extends NutchTool> toolClass,
			Map<String,Object> args) throws Exception {
		currentTool = (NutchTool)ReflectionUtils.newInstance(toolClass, getConf());
		return currentTool.run(args);
	}

	@Override
	public boolean stopJob() throws Exception {
		shouldStop = true;
		if (currentTool != null) {
			return currentTool.stopJob();
		}
		return false;
	}

	@Override
	public boolean killJob() throws Exception {
		shouldStop = true;
		if (currentTool != null) {
			return currentTool.killJob();
		}
		return false;
	}

	@Override
	public Map<String,Object> run(Map<String, Object> args) throws Exception {
		results.clear();
		status.clear();
		String crawlId = (String)args.get(Nutch.ARG_CRAWL);
		if (crawlId != null) {
			getConf().set(Nutch.CRAWL_ID_KEY, crawlId);
		}

        // initialize Distributor
		Distributor distributor = new Distributor(getConf());
		distributor.init();
		LOG.info("TweetCrawler: Number of URLs in buffer: " + urls.size());
		

		Integer depth = (Integer)args.get(Nutch.ARG_DEPTH);
		if (depth == null) depth = 1;
		boolean parse = getConf().getBoolean(FetcherJob.PARSE_KEY, false);
		int onePhase = 3;
		if (!parse) onePhase++;
		float totalPhases = depth * onePhase;

		float phase = 0;
		Map<String,Object> jobRes = null;
		LinkedHashMap<String,Object> subTools = new LinkedHashMap<String,Object>();
		status.put(Nutch.STAT_JOBS, subTools);
		results.put(Nutch.STAT_JOBS, subTools);
		// inject phase
		
		distributor.run(urls);
				
		//empty queue;
		urls.clear();
		
		if (shouldStop) {
			return results;
		}
		// run "depth" cycles
		for (int i = 0; i < depth; i++) {
			LOG.info("depth: " + i);
			status.put(Nutch.STAT_PHASE, "generate " + i);
			for (Entry<String, Object> arg : args.entrySet()) {
				LOG.info("arg: " + arg.getKey() + " " + arg.getValue().toString());
			}
			jobRes = runTool(GeneratorJob.class, args);
			if (jobRes != null) {
				subTools.put("generate " + i, jobRes);
			}
			status.put(Nutch.STAT_PROGRESS, ++phase / totalPhases);
			if (shouldStop) {
				return results;
			}
			status.put(Nutch.STAT_PHASE, "fetch " + i);
			jobRes = runTool(FetcherJob.class, args);
			if (jobRes != null) {
				subTools.put("fetch " + i, jobRes);
			}
			status.put(Nutch.STAT_PROGRESS, ++phase / totalPhases);
			if (shouldStop) {
				return results;
			}
			if (!parse) {
				status.put(Nutch.STAT_PHASE, "parse " + i);
				jobRes = runTool(ParserJob.class, args);
				if (jobRes != null) {
					subTools.put("parse " + i, jobRes);
				}
				status.put(Nutch.STAT_PROGRESS, ++phase / totalPhases);
				if (shouldStop) {
					return results;
				}
			}
			status.put(Nutch.STAT_PHASE, "updatedb " + i);
			jobRes = runTool(DbUpdaterJob.class, args);
			if (jobRes != null) {
				subTools.put("updatedb " + i, jobRes);
			}
			status.put(Nutch.STAT_PROGRESS, ++phase / totalPhases);
			if (shouldStop) {
				return results;
			}
		}

		return results;
	}

	@Override
	public float getProgress() {
		Float p = (Float)status.get(Nutch.STAT_PROGRESS);
		if (p == null) return 0;
		return p;
	}

	@Override
	public int run(String[] args) throws Exception {
		// parse most common arguments here
		String seedDir = null;
		int threads = getConf().getInt("fetcher.threads.fetch", 10);    
		int depth = 1;
		long topN = 1;
		//long topN = Long.MAX_VALUE;
		Integer numTasks = null;
		//leave resolve job to Nutch
		boolean resolve = false;

		Map<String,Object> argMap = ToolUtil.toArgMap(
				Nutch.ARG_THREADS, threads,
				Nutch.ARG_DEPTH, depth,
				Nutch.ARG_TOPN, topN,
				ARG_RESOLVE, resolve,
				Nutch.ARG_SEEDDIR, seedDir,
				Nutch.ARG_NUMTASKS, numTasks);
		run(argMap);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		TwitterCrawler c = new TwitterCrawler();
		Configuration conf = NutchConfiguration.create();
		int res = ToolRunner.run(conf, c, args);
		System.exit(res);
	}
}

