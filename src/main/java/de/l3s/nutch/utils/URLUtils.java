package de.l3s.nutch.utils;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.Proxy;
import java.net.URL;

public class URLUtils {
    
	/**
	 * Trick for faster getting the redirect URL from HTTP header
	 * @param address
	 * @return
	 * @throws IOException
	 */
	public static String expandShortURL(String address) throws IOException {
		URL url = new URL(address);

		HttpURLConnection connection = (HttpURLConnection) url.openConnection(Proxy.NO_PROXY); //using proxy may increase latency
		connection.setInstanceFollowRedirects(false);
		connection.connect();
		//Hack
		String expandedURL = connection.getHeaderField("Location");
		connection.getInputStream().close();
		return expandedURL;
	}
	
	public static void main (String[] args) {
		try {
			System.out.println(URLUtils.expandShortURL("http://t.co/LlvaszT7t"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
