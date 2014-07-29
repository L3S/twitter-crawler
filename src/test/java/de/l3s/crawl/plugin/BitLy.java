package de.l3s.crawl.plugin;

import static com.rosaloves.bitlyj.Bitly.as;
import static com.rosaloves.bitlyj.Bitly.expand;

import java.util.Arrays;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import junit.framework.TestCase;

import com.rosaloves.bitlyj.BitlyMethod;
import com.rosaloves.bitlyj.Url;
import com.rosaloves.bitlyj.Bitly.Provider;


public class BitLy extends TestCase {
	private static final Logger logger = LoggerFactory.getLogger(BitLy.class);
	Provider bitly = as("o_46dea4b2g3", "R_983de0b2f71045f0bd8135685e8850b0");

	public void testAPI() {
		String[] a = {"http://bit.ly/JGVkUk", "http://j.mp/xonmK"};
		BitlyMethod<Set<Url>> expanded = expand(a);


		Set<Url> urls = bitly.call(expanded);

		String[] _urls = new String[urls.size()];
		int idx = 0;
		for (Url url : urls) {
			_urls[idx] = url.getLongUrl();
			idx++;
		}

		assertTrue(Arrays.equals(_urls, 
				new String[]{"http://mashable.com/2009/09/04/iphone-auto-tune/","http://mergerecords.com/news"}));
	}

	public void testAPIBlockingTime () {
		//random urls
		String[] bitlies = {"http://bit.ly/JGVkUk", "http://j.mp/xonmK", "http://bit.ly/Wn2Xdz", "bit.ly/6wgJO", "bit.ly/GOAEY", 
				"bit.ly/1amjxAx", "bit.ly/ICCSmb", "bit.ly/ga5OXd", "http://bit.ly/qQoknw", "http://bit.ly/1st0Roh"};

		long millis = 1000; // 1 sec
		
        int time = 0;
       
		while (time++ < 1000) {
			BitlyMethod<Set<Url>> expanded = expand(bitlies);

			int cnt = 0; String[] expanded_urls = new String[bitlies.length];


			if (cnt == 0)  {
				//first URL expansion, expected to work
				Set<Url> _expanded_urls = bitly.call(expanded);
				int idx = 0;
				for (Url url : _expanded_urls) {
					expanded_urls[idx] = url.getLongUrl();
					idx++;
				}
			}

			else {
				Set<Url> _expanded_urls = bitly.call(expanded);
				logger.info("Assert not null");
				assertNotNull("Assert not null passed", _expanded_urls);
				String[] _urls = new String[_expanded_urls.size()];
				int idx = 0;
				for (Url url : _expanded_urls) {
					_urls[idx] = url.getLongUrl();
					idx++;
				}
				logger.info("Assert equality");
				assertTrue(Arrays.equals(expanded_urls, _urls));
			}
			try {
				Thread.sleep(millis);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
	}
}
