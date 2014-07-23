package de.l3s.crawl.plugin.impl;

import com.rosaloves.bitlyj.*;

import static com.rosaloves.bitlyj.Bitly.*;

import java.util.List;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import de.l3s.crawl.Distributor;
import de.l3s.crawl.plugin.Plugin;

public class JmpPlugin implements Plugin{

	private static final int MAX_SIZE = 15;
	public Set<String> queue = Sets.newLinkedHashSet();

	Provider jmp = Jmp.as("o_46dea4b2g3", "R_983de0b2f71045f0bd8135685e8850b0");

	protected int cur_pos;
	

	@Override
	public boolean isSupported(String url) {
		if (url.toLowerCase().contains("j.mp")) return true;
		else return false;
	}

	@Override
	public void setIdx(int idx) {
		cur_pos = idx;
		
	}

	
	@Override
	public void next(List<String> urls) {
		Plugin plugin = Distributor.plugins.get(cur_pos++);
		if (plugin != null) plugin.handle(urls);
	}

	@Override
	public void handle(List<String> urls) {
		List<String> supported = Lists.newArrayList();
		for (String url : urls) {
			if (isSupported(url)) supported.add(url);
			urls.remove(url);
			
			if (urls.size() > 0) next(urls);
		}
		
		queue.addAll(supported);
		if (queue.size() == MAX_SIZE) {
			// send batch processing request
			BitlyMethod<Set<Url>> expandMethod = expand(queue.toArray(new String[MAX_SIZE]));

			//empty queue
			queue.removeAll(queue);

			Set<Url> bitlyUrls = jmp.call(expandMethod);


			Set<String> url_set = Sets.newLinkedHashSet();

			for (Url u : bitlyUrls) {
				url_set.add(u.getLongUrl());
			}

			Distributor.expanded.addAll(url_set);
		}

	}

}
