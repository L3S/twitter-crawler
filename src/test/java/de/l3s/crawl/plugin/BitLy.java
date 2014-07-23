package de.l3s.crawl.plugin;

import static com.rosaloves.bitlyj.Bitly.as;
import static com.rosaloves.bitlyj.Bitly.expand;

import java.util.Arrays;
import java.util.Set;

import junit.framework.TestCase;

import com.rosaloves.bitlyj.BitlyMethod;
import com.rosaloves.bitlyj.Url;
import com.rosaloves.bitlyj.Bitly.Provider;

public class BitLy extends TestCase {

	public void testAPI() {
		String[] a = {"http://bit.ly/JGVkUk", "http://j.mp/xonmK"};
		BitlyMethod<Set<Url>> expanded = expand(a);

		Provider bitly = as("o_46dea4b2g3", "R_983de0b2f71045f0bd8135685e8850b0");

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
}
