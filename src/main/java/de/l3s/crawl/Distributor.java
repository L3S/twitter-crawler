package de.l3s.crawl;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.net.URLFilters;
import org.apache.nutch.net.URLNormalizers;
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
	/* Custom Injector that handles normal and redirect URLs */
	private Injector injector;

	/* Nutch URL filter */
	private URLFilters filters;

	/* Nutch URL normalizer */ 
	private URLNormalizers urlNormalizers;

	/* Chain of responsibility: list of plugins */
	public static List<Plugin> plugins = Lists.newArrayList();

	/* expanded URLs */
	public static List<TinyURL> expanded = Lists.newArrayList();

	/* not yet expanded URLs */
	public static List<String> non_expanded = Lists.newArrayList();


	public Distributor(Configuration conf) {
		try {
			this.injector = new Injector(conf);

		} catch (InjectorSetupException e) {
			logger.error(e.getMessage());
		}

		urlNormalizers = new URLNormalizers(conf,
				URLNormalizers.SCOPE_INJECT);

		filters = new URLFilters(conf);
		init();
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
					String long_normalized;
					logger.info("import to crawlDB");
					injector.inject((long_normalized = useNutchURLNormalizer(url._long)));
					//store redirect
					logger.info("import redirect " + url._short + " >>  " + url._long);
					injector.addRedirect(useNutchURLNormalizer(url._short), long_normalized);
				} catch (InjectorInjectionException e) {
					logger.error(e.getMessage());
				}
			}
		}
		if (non_expanded != null) {

			for (String url : non_expanded) {
				try {
					//give redirect handing job to Nutch
					logger.info("flush to Nutch Injector");
					injector.inject(useNutchURLNormalizer(url));
				} catch (InjectorInjectionException e) {
					logger.error(e.getMessage());
				}
			}
		}
	}

	public void run(List<String> urls) {
		// expand
		expand(urls);
		//store
		store();
	}


	/**
	 * normalization function of Nutch
	 * @param url
	 * @return
	 */
	public String useNutchURLNormalizer(String url) {
		try {
			url = urlNormalizers.normalize(url, URLNormalizers.SCOPE_INJECT);
			url = filters.filter(url); // filter the url
		} catch (Exception e) {
			logger.warn("Skipping " + url + ":" + e);
			url = null;
		}
		return url;
	}

}
