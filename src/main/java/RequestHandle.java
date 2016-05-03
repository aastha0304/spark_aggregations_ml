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
	static ArrayList<String> getArrString(JSONObject job, String key){
		ArrayList<String> val = new ArrayList<>();
		try{
			JSONArray value = (JSONArray) job.get(key);
			int len = value.size();
			if(len > 0){
				for(int i=0; i < len; i++){
                    val.add(String.valueOf(value.get(i)));
				}
			}
		}catch(Exception e){
			logger.info(job.toString()+" "+key);
			logger.info(e.getMessage());
		}
		return val;
	}
	static ArrayList<Integer> getArrInt(JSONObject job, String key){
		ArrayList<Integer> val = new ArrayList<>();
		try{
			JSONArray value = (JSONArray) job.get(key);
			
			int len = value.size();
			if(len > 0){
				 for(int i=0; i < len; i++){
                     val.add(Integer.parseInt(value.get(i).toString()));
             }
			}
		}catch(Exception e){
			logger.info(job.toString()+" "+key);
			logger.info(e.getMessage());
		}
		return val;
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
	static String getGenderString(JSONObject job, String key){
		String val = new String("z");
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
		ArrayList<String> app_cat = new ArrayList<>();
		String app_domain = "";
		String app_ver = "";

		ArrayList<String> badv = new ArrayList<>();
		ArrayList<Integer> banner_api = new ArrayList<>();
		ArrayList<Integer> banner_battr = new ArrayList<>();
		int banner_h = -1;
		int banner_pos = -1;
		char banner_topframe = '-'; //
		int banner_w = -1;
		ArrayList<String> bcat = new ArrayList<>();
		float bidfloor = -1;
		
		String carrier = "";
		String city= "";
		int connectiontype = -1;
		
		String device_lang = "";
		char device_lmt = '-'; //
		int devicetype = -1;
		String displaymanager = "";
		String displaymanagerver = "";
		char dnt = '-';
		
		char extra_atts = '0';
		
		String id = "";
		char imp_secure = '-'; //
		int imp_size = -1;
		char instl= '-';
		String ip = "";
		
		float lat = -1;
		float lon = -1;
		
		String region = "";
		String ua = "";
		
		String zip = "";
		
		int banner_hmax = -1;
		int banner_hmin = -1;
		int banner_wmax = -1;
		int banner_wmin = -1;
		int device_h = -1;
		int device_w = -1;
		int user_ext_cokkie_age = -1;
		String user_gender = "z";
		String user_keywords = "";
		 int user_yob = -1;
		
		
		JSONObject br = new JSONObject();
			br = create(req);
			  JSONObject user = getJson(br,  "user");
			  if(user != null){
				  id = getString(user, "id");
				 
				  badv = getArrString(br, "badv");
				  bcat = getArrString(br, "bcat");
				  imp_size = getJsonArr(br, "imp").size();
				  JSONObject imp = (JSONObject)getJsonArr(br, "imp").get(0);
				  if(imp != null){
					  bidfloor = getDouble(imp, "bidfloor");
					  displaymanager = getString(imp, "displaymanager");
					  instl = (char)(getInt(imp, "instl")+'0');
					  imp_secure = (char)(getInt(imp, "secure")+'0');
					  JSONObject banner = getJson(imp, "banner");
					  if(banner != null){
						  banner_pos = getInt(banner, "pos");
						  banner_w = getInt(banner, "w");
						  banner_h = getInt(banner, "h");
						  banner_api = getArrInt(banner, "api");
						  banner_battr = getArrInt(banner, "battr");
						  banner_topframe = (char)(getInt(banner, "topframe")+'0');
						  //for extra_atts
						  banner_hmax = getInt(banner, "hmax");
						  if(banner_hmax != -1)
							  extra_atts = '1';
						  banner_hmin = getInt(banner, "hmin");
						  if(banner_hmin != -1)
							  extra_atts = '1';
						  banner_wmax = getInt(banner, "wmax");
						  if(banner_wmax != -1)
							  extra_atts = '1';
						  banner_wmin = getInt(banner, "wmin");
						  if(banner_wmin != -1)
							  extra_atts = '1';
					  }
					  displaymanagerver = getString(imp, "displaymanagerver");
				  }
				  JSONObject app = getJson(br, "app");
				  if(app != null){
					  app_ver = getString(app, "ver");
					  app_cat = getArrString(app, "cat");
					  app_domain = getString(app, "domain");
				  }
				  JSONObject device = getJson(br, "device");
				  if(device != null){
					  ua = getString(device, "ua");
					  ip = getString(device, "ip");
					  device_lang = getString(device, "language");
					  device_lmt = (char)(getInt(device, "lmt")+'0');
					  //for extra_atts
					  device_h = getInt(device, "h");
					  if(device_h!=-1){
						  extra_atts = '1';
					  }
					  device_w = getInt(device, "w");
					  if(device_w!=-1){
						  extra_atts = '1';
					  }
					  JSONObject geo = getJson(device, "geo");
					  if(geo != null){
						  zip = getString(geo, "zip");
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
				  //for extra_atts
				  JSONObject ext = getJson(user, "ext");
				  if(ext != null){
					  Object cookie_age = ext.get("cookie_age");
					  if(cookie_age != null){
						  user_ext_cokkie_age = (int)(long)cookie_age;
						  extra_atts = '1';
					  }
				  }
				  user_gender = getGenderString(user, "gender");
				  if(user_gender != "z")
					  extra_atts = '1';
				  user_yob = getInt(user, "yob");
				  if(user_yob != -1)
					  extra_atts = '1';
				  user_keywords = getString(user, "keywords");
				  if(user_keywords != "")
					  extra_atts = '1';
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
		 bidOb.setRegion(region);
		 bidOb.setLon(lon);
		 bidOb.setLat(lat);
		 bidOb.setIp(ip);
		 bidOb.setInstl(instl);
		 bidOb.setImp_secure(imp_secure);
		 bidOb.setId(id);
		 bidOb.setDnt(dnt);
		 if(extra_atts == '1'){
			 ExtraAttributes extra = new ExtraAttributes();
			 extra.setBanner_hmax(banner_hmax);
			 extra.setBanner_hmin(banner_hmin);
			 extra.setBanner_wmax(banner_wmax);
			 extra.setBanner_wmin(banner_wmin);
//			 extra.setDevice_h(device_h);
//			 extra.setDevice_w(device_w);
//			 extra.setUser_ext_cokkie_age(user_ext_cokkie_age);
//			 extra.setUser_gender(user_gender);
//			 extra.setUser_keywords(user_keywords);
//			 extra.setUser_yob(user_yob);
			 bidOb.setExtra(extra);
		 }
	      return bidOb;
	}
	static ArrayList<String> buildKey(CommonRow bidOb){
		ArrayList<String> key = new ArrayList<String>();
		key.add(bidOb.getTs());
		
//		String ip = bidOb.getIp();
//		String ip1="", ip2="", ip3 = "";
//		if(!ip.isEmpty()){
//			String[] ips = ip.split("\\.");
//			if(ips.length==4){
//				ip1 = ips[0];
//				ip2 = ips[1];
//				ip3 = ips[2];
//			}else{
//				System.out.println(ip);
//				System.out.println(ips.length);
//			}
//			
//		}
//		key.add(ip1);
//		key.add(ip2);
//		key.add(ip3);
//		String city = bidOb.getCity();
//		if(city == null || city.isEmpty())
//			city = "";
//		key.add(city);
//		String region = bidOb.getRegion();
//		if(region.isEmpty())
//			region="";
//		key.add(region);
		char extra_atts = bidOb.getExtra_atts();
		key.add(extra_atts+"");
		return key;
	}
}
