package de.l3s.crawl.plugin.impl;

import com.rosaloves.bitlyj.*;

import static com.rosaloves.bitlyj.Bitly.*;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import de.l3s.crawl.Distributor;
import de.l3s.crawl.TinyURL;
import de.l3s.crawl.plugin.Plugin;

public class BitLyPlugin implements Plugin {
	/* Batch size per request via API */
	private static final int MAX_SIZE = 15;
	public Set<String> queue = Sets.newLinkedHashSetWithExpectedSize(MAX_SIZE);


	// both bit.ly and j.mp are handled with the provider
	// hence calling j.mp plugin is redundant
	Provider bitly = as("o_46dea4b2g3", "R_983de0b2f71045f0bd8135685e8850b0");

	protected int cur_pos;


	@Override
	public boolean isSupported(String url) {
		if (url.toLowerCase().contains("bit.ly")) return true;
		else return false;
	}

	@Override
	public void setIdx(int idx) {
		cur_pos = idx;

	}

	@Override
	public void next(List<String> urls) {
		Plugin plugin = null;
		try {
			plugin = Distributor.plugins.get(cur_pos++);
		} catch (IndexOutOfBoundsException e) {}
		if (plugin != null)  plugin.handle(urls);
		else Distributor.non_expanded.addAll(urls);
	}

	/**
	 * Handle MAX_SIZE number of URLs per request
	 * over set of URLs
	 */
	@Override
	public void handle(List<String> urls) {
		List<String> supported = Lists.newArrayList();
		for (Iterator<String> iter = urls.iterator(); iter.hasNext();) {
			String url = iter.next();
			if (isSupported(url)) {
				supported.add(url);

				iter.remove();
			}
		}

		if (urls.size() > 0) next(urls);


		int cnt = 0;
		for (String url : supported) {
			queue.add(url);
			if (queue.size() == MAX_SIZE || cnt++ == supported.size() -1) {
				// send batch processing request
				BitlyMethod<Set<Url>> expandMethod = expand(queue.toArray(new String[queue.size()]));

				//empty queue
				queue.removeAll(queue);

				Set<Url> bitlyUrls = bitly.call(expandMethod);
				Set<TinyURL> url_set = Sets.newLinkedHashSet();

				for (Url u : bitlyUrls) {
					//there are cases when getLongUrl() returns null
					//this will be handled at Distributor
					url_set.add(new TinyURL(u.getShortUrl(), u.getLongUrl()));
				}

				Distributor.expanded.addAll(url_set);
			}
		}

	}

}
