import java.util.ArrayList;
import org.json.simple.JSONObject;
import org.json.simple.JSONArray;
//import org.json.simple.parser.*;
import org.json.simple.parser.ParseException;
import org.json.simple.parser.JSONParser;
import org.apache.log4j.Logger;

public class RequestHandle{
	static Logger logger = Logger.getLogger(RequestHandle.class.getName());  

	static JSONObject create(String br){
		JSONParser parser = new JSONParser();
		JSONObject job = new JSONObject();
		try{
			if(br.contains("\\x"))
				job = (JSONObject)parser.parse(br.substring(1, br.length()-2).replace("\\x", "\\u00"));
			job = (JSONObject)parser.parse(br);
		}catch(org.json.simple.parser.ParseException e){
			logger.info("in parse");
			logger.info(br);
		}catch(Exception e){
			logger.info(br);
			logger.info(e.getMessage());
		}
		return job;
	}
	static String getArrStr(JSONObject job, String key){
		StringBuffer sbf = new StringBuffer();
		String val = "";
		try{
			JSONArray value = (JSONArray) job.get(key);
			int len = value.size();
			if(len > 0){
                sbf.append(String.valueOf(value.get(0)));
                for(int i=1; i < len; i++){
                        sbf.append(":").append(String.valueOf(value.get(i)));
                }
			}
			val = sbf.toString().toLowerCase(); 
		}catch(Exception e){
			logger.info(job.toString()+" "+key);
			logger.info(e.getMessage());
		}
		return val;
	}
	static JSONObject getJson(JSONObject job, String key){
		JSONObject val = new JSONObject();
		try{
			val = (JSONObject)job.get(key);
		}catch(Exception e){
			logger.info(job.toString()+" "+key);
			logger.info(e.getMessage());
		}
		return val;
	}
	static JSONArray getJsonArr(JSONObject job, String key){
		JSONArray val = new JSONArray();
		try{
			val = (JSONArray)job.get(key);
		}catch(Exception e){
			logger.info(job.toString()+" "+key);
			logger.info(e.getMessage());
		}
		return val;
	}
	static float getDouble(JSONObject job, String key){
		float val = -1;
		try {
			val = (float)(double)job.get(key);
		}catch(Exception e){
			logger.info(job.toString()+" "+key);
			logger.info(e.getMessage());
		}
		return val;
	}
	static String getString(JSONObject job, String key){
		String val = new String("");
		try {
			val = (String)job.get(key);
		}catch(Exception e){
			logger.info(job.toString()+" "+key);
			logger.info(e.getMessage());
		}
		return (val!=null)?val.toLowerCase():val;
	}
	static int getInt(JSONObject job, String key){
		int val=-1;
		try {
			val = (int)(long)job.get(key);
		}catch(Exception e){
			logger.info(job.toString()+" "+key);
			logger.info(e.getMessage());
		}
		return val;
	}
	static BidAttributes getCommonRow(String req){
		
		String id, badv, bcat, displaymanager, banner_api, banner_battr, displaymanagerver, app_ver, 
		  app_cat, ua, ip, city, region, carrier, app_domain, device_lang;
		id = badv = bcat = displaymanager = banner_api = banner_battr = displaymanagerver = app_ver = 
				  app_cat = ua = ip = city = region = carrier = app_domain = device_lang = "";
		float bidfloor, lat, lon ;
		bidfloor = lat = lon = -1;
		int banner_pos, banner_w, banner_h, connectiontype, devicetype, tmax;
		banner_pos = banner_w = banner_h = connectiontype = devicetype = tmax  = -1;
		char instl, dnt, device_lmt, imp_secure, banner_topframe;
		instl = dnt = device_lmt = imp_secure = banner_topframe = '-';
		
		JSONObject br = new JSONObject();
			br = create(req);
			  JSONObject user = getJson(br,  "user");
			  if(user != null){
				  id = getString(user, "id");
				  badv = getArrStr(br, "badv");
				  bcat = getArrStr(br, "bcat");
				  //at = getInt(br, "at");
				  tmax = getInt(br, "tmax");
				  JSONObject imp = (JSONObject)getJsonArr(br, "imp").get(0);
				  //JSONObject imp = (JSONObject)getJson(br, "imp").get(0);
				  if(imp != null){
					  bidfloor = getDouble(imp, "bidfloor");
					  displaymanager = getString(imp, "displaymanager");
					  instl = (char)(getInt(imp, "instl")+'0');
					  imp_secure = (char)(getInt(imp, "secure")+'0');
					  JSONObject banner = getJson(imp, "banner");
					  if(banner != null){
						  banner_pos = getInt(banner, "pos");
						  banner_w = getInt(banner, "w");
						  //banner_btype = getArrStr(banner, "btype");
						  banner_h = getInt(banner, "h");
						  banner_api = getArrStr(banner, "api");
						  banner_battr = getArrStr(banner, "battr");
						  banner_topframe = (char)(getInt(banner, "topframe")+'0');
					  }
					  displaymanagerver = getString(imp, "displaymanagerver");
				  }
				  JSONObject app = getJson(br, "app");
				  if(app != null){
					  app_ver = getString(app, "ver");
					  app_cat = getArrStr(app, "cat");
					  app_domain = getString(app, "domain");
				  }
				  JSONObject device = getJson(br, "device");
				  if(device != null){
					  ua = getString(device, "ua");
					  ip = getString(device, "ip");
					  device_lang = getString(device, "language");
					  device_lmt = (char)(getInt(device, "lmt")+'0');
					  JSONObject geo = getJson(device, "geo");
					  if(geo != null){
						  //zip = getString(geo, "zip");
						  lat = getDouble(geo, "lat");
						  lon = getDouble(geo, "lon");
						  city = getString(geo, "city");
						  region = getString(geo, "region");
						  //geo_type = getInt(geo, "type");
					  }
					  carrier = getString(device, "carrier");
					  connectiontype = getInt(device, "connectiontype");
					  //js = getInt(device, "js");
					  devicetype = getInt(device, "devicetype");
					  dnt = (char)(getInt(device, "dnt")+'0');
				  }	
			  }
	      
		 BidAttributes bidOb = new BidAttributes();
		 bidOb.setApp_cat(app_cat);
		 bidOb.setApp_domain(app_domain);
		 bidOb.setApp_ver(app_ver);
		 bidOb.setBadv(badv);
		 bidOb.setBanner_api(banner_api);
		 bidOb.setBanner_battr(banner_battr);
		 bidOb.setBanner_h(banner_h);
		 bidOb.setBanner_pos(banner_pos);
		 bidOb.setBanner_topframe(banner_topframe);
		 bidOb.setBanner_w(banner_w);
		 bidOb.setBcat(bcat);
		 bidOb.setBidfloor(bidfloor);
		 bidOb.setCarrier(carrier);
		 bidOb.setCity(city);
		 bidOb.setConnectiontype(connectiontype);
		 bidOb.setDevice_lang(device_lang);
		 bidOb.setDevice_lmt(device_lmt);
		 bidOb.setDevicetype(devicetype);
		 bidOb.setDisplaymanager(displaymanager);
		 bidOb.setDisplaymanagerver(displaymanagerver);
		 bidOb.setUa(ua);
		 bidOb.setTmax(tmax);
		 bidOb.setRegion(region);
		 bidOb.setLon(lon);
		 bidOb.setLat(lat);
		 bidOb.setIp(ip);
		 bidOb.setInstl(instl);
		 bidOb.setImp_secure(imp_secure);
		 bidOb.setId(id);
		 bidOb.setDnt(dnt);
	      return bidOb;
	}
	static ArrayList<String> buildKey(CommonRow bidOb){
		ArrayList<String> key = new ArrayList<String>();
		key.add(bidOb.getTs());
		
		String ip = bidOb.getIp();
		String ip1="", ip2="", ip3 = "";
		if(!ip.isEmpty()){
			String[] ips = ip.split("\\.");
			if(ips.length==4){
				ip1 = ips[0];
				ip2 = ips[1];
				ip3 = ips[2];
			}else{
				System.out.println(ip);
				System.out.println(ips.length);
			}
			
		}
		key.add(ip1);
		key.add(ip2);
		key.add(ip3);
//		String city = bidOb.getCity();
//		if(city == null || city.isEmpty())
//			city = "";
//		key.add(city);
//		String region = bidOb.getRegion();
//		if(region.isEmpty())
//			region="";
//		key.add(region);
		return key;
	}
}
