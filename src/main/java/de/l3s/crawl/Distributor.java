package de.l3s.crawl;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import de.l3s.icrawl.nutch.Injector;
import de.l3s.icrawl.nutch.InjectorInjectionException;
import de.l3s.icrawl.nutch.InjectorSetupException;
import de.l3s.crawl.plugin.Plugin;
import de.l3s.crawl.plugin.impl.BitLyPlugin;
import de.l3s.crawl.plugin.impl.JmpPlugin;

public class Distributor {
    private static final Logger logger = LoggerFactory.getLogger(Distributor.class);
	public Configuration conf;
    /*Custom Injector that handles normal and redirect URLs */
	public Injector injector;
    
	/*Chain of responsibility: list of plugins*/
	public static List<Plugin> plugins = Lists.newArrayList();

	/*expanded URLs*/
	public static List<TinyURL> expanded = Lists.newArrayList();
    
	/*not yet expanded URLs*/
	public static List<TinyURL> non_expanded = Lists.newArrayList();


	public Distributor(Configuration conf) {
		this.conf = conf;
		try {
			this.injector = new Injector(conf);
		} catch (InjectorSetupException e) {
			logger.error(e.getMessage());
		}
	}

	public void init() {
		BitLyPlugin bitly = new BitLyPlugin();
		bitly.setIdx(0);
		plugins.add(bitly);

		JmpPlugin jmp = new JmpPlugin();
		jmp.setIdx(1);
		plugins.add(jmp);
	}
	
	/**
	 * Send URLs from stream to chain of 
	 * responsibility plugins
	 * @param urls
	 */
	public void expand (List<String> urls) {
		plugins.get(0).handle(urls);
	}
   
	/**
	 * Flush the expanded/non-expanded URLs
	 * into the Injector -> store in crawlDB
	 */
	public void store () {
		if (expanded != null) {
			for (TinyURL url : expanded) {
				//inject into crawl DB
				try {
					injector.inject(url._long);
					//store redirect
					injector.addRedirect(url._short, url._long);
				} catch (InjectorInjectionException e) {
					logger.error(e.getMessage());
				}
			}
		}
		if (non_expanded != null) {
			//TODO: will be handled by Injector?
		}
	}
	
	public void run(List<String> urls) {
		// expand
		expand(urls);
		//store
		store();
	}

}
