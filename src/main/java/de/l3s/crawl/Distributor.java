package de.l3s.crawl;

import java.util.List;

import org.apache.gora.store.DataStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.net.URLFilters;
import org.apache.nutch.net.URLNormalizers;
import org.apache.nutch.storage.WebPage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import de.l3s.icrawl.nutch.Injector;
import de.l3s.icrawl.nutch.InjectorInjectionException;
import de.l3s.icrawl.nutch.InjectorSetupException;
import de.l3s.crawl.plugin.Plugin;
import de.l3s.crawl.plugin.impl.BitLyPlugin;
import de.l3s.crawl.plugin.impl.JmpPlugin;

/* This class is a distributor of URL service plugins.
 * The class contains a chain of responsibility, list of plugins.
 * URLs from the stream are handled in a batch request to the service API,
 * which is more efficient and in a polite manner. Unresolved URI are flushed 
 * to the resolving mechanism of Nutch.
 */
public class Distributor {
	private static final Logger logger = LoggerFactory.getLogger(Distributor.class);
	/* Custom Injector that handles normal and redirect URLs */
	private Injector injector;

	/* Nutch URL filter */
	private static URLFilters filters;

	/* Nutch URL normalizer */ 
	private static URLNormalizers urlNormalizers;

	/* Chain of responsibility: list of plugins */
	public static List<Plugin> plugins = Lists.newArrayList();

	/* expanded URLs */
	public static List<TinyURL> expanded = Lists.newArrayList();

	/* not yet expanded URLs */
	public static List<String> non_expanded = Lists.newArrayList();

	private DataStore<String, WebPage> store;
	public Distributor() {
		BitLyPlugin bitly = new BitLyPlugin();
		bitly.setIdx(0);
		plugins.add(bitly);

		JmpPlugin jmp = new JmpPlugin();
		jmp.setIdx(1);
		plugins.add(jmp);
	}

	public void setInjector(Configuration conf, int id) {
		try {
			//conf.set("storage.data.store.class", MemStore.class.getName());
			store = Injector.createStore(conf, "" + id);
			injector = new Injector(conf, store);

		} catch (InjectorSetupException e) {
			logger.error(e.getMessage());
		}
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
					//logger.info("import to crawlDB");
					injector.inject((url._long));
					//store redirect
					//logger.info("import redirect " + url._short + " >>  " + url._long);
					injector.addRedirect(url._short, url._long);
				} catch (InjectorInjectionException e) {
					logger.error(e.getMessage());
				} catch (IllegalArgumentException iae) {
					//catch malformed URLs
					logger.error(iae.getMessage());
				}
			}
		}
		if (non_expanded != null) {

			for (String url : non_expanded) {
				try {
					//give redirect handing job to Nutch
					//logger.info("flush to Nutch Injector");
					injector.inject(url);
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
	public static String useNutchURLNormalizer(String url) {
		if (url != null && url.trim().startsWith("#")) {
			/* Ignore line that start with # */
			return null;
		}
		if (url.indexOf("\t") != -1) {
			String[] splits = url.split("\t");
			url = splits[0];
		}
		try {
			url = urlNormalizers.normalize(url, URLNormalizers.SCOPE_INJECT);
			url = filters.filter(url); // filter the url
		} catch (Exception e) {
			logger.warn("Skipping " + url + ":" + e);
			e.printStackTrace();
			url = null;
		}
		return url;
	}

}
