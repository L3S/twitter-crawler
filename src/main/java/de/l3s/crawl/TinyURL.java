package de.l3s.crawl;

import java.util.Map;

public class TinyURL {
	/*From URL*/
	public String _short;
	/*To URL*/
	public String _long;
	/*Metadata if available*/
	Map<String, String> metadata;
	
	public TinyURL(String _short, String _long) {
		this._long = _long;
		this._short = _short;
	}
	
	public TinyURL(String _short, String _long, Map<String, String> metadata) {
		this._long = _long;
		this._short = _short;
		this.metadata = metadata;
	}
	

}
