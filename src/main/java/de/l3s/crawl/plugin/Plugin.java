package de.l3s.crawl.plugin;

import java.util.List;

public interface Plugin {
	
	public void setIdx(int idx);

	public boolean isSupported(String url);
	
	public void next(List<String> urls);
	
	public void handle(List<String> urls);
	

}
