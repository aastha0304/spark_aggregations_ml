import java.io.Serializable;
import java.util.ArrayList;
public class BidAttributes implements Serializable{
	private ArrayList<String> app_cat;
	//private String app_domain; //
	//private String app_ver;

	private ArrayList<String> badv;
	private ArrayList<Integer> banner_api;
	private ArrayList<Integer> banner_battr;
	private int banner_h;
	private int banner_pos;
	private char banner_topframe; //
	private int banner_w;
	private ArrayList<String> bcat;
	private float bidfloor;
	
	private String carrier;
	//private String city;
	private int connectiontype;
	
	private String device_lang;
	//private char device_lmt; //
	private int devicetype;
	//private String displaymanager;
	//private String displaymanagerver;
	private char dnt;
	
	private char extra_atts;

	private String id;
	private char imp_secure; //
	private int imp_size;
	private char instl;
	//private String ip;
	
	private float lat;
	private float lon;
	
	private String region;
	//private String ua;
		
	//private String zip;
	private ExtraAttributes extra;
	

	//long at; //
	//long geo_type; //
	//private int tmax; //
	BidAttributes(){
		app_cat = new ArrayList<>();
		//app_domain = "";
		//app_ver = "";
		
		badv = new ArrayList<>();
		banner_api = new ArrayList<>();
		banner_battr = new ArrayList<>();
		banner_h = -1;
		banner_pos = -1;
		banner_topframe = '-';
		banner_w = -1;
		bcat = new ArrayList<>();
		bidfloor = -1;
		
		carrier = "";
		//city = "";
		connectiontype = -1;
		
		device_lang = "";
		//device_lmt = '-';
		devicetype = -1;
		//displaymanager = "";
		//displaymanagerver = "";
		dnt = '-';
		
		extra_atts = '0';
		
		id = "";
		imp_secure = '-';
		imp_size = -1;
		instl = '-';
		//ip = "";
		
//		lat = -1;
//		lon = -1;
		
		region = "";
		
//		ua = "";
//		
//		zip = "";
		
		extra = new ExtraAttributes();
		//at = -1;
		//geo_type = -1;
		//tmax = -1;
	}
	
	public ArrayList<String> getApp_cat() {
		return app_cat;
	}

//	public String getApp_domain() {
//		return app_domain;
//	}
//
//	public String getApp_ver() {
//		return app_ver;
//	}

	public ArrayList<String> getBadv() {
		return badv;
	}
	
	public ArrayList<Integer> getBanner_api() {
		return banner_api;
	}
	public ArrayList<Integer> getBanner_battr() {
		return banner_battr;
	}
	public int getBanner_h() {
		return banner_h;
	}
	public int getBanner_pos() {
		return banner_pos;
	}
	public char getBanner_topframe() {
		return banner_topframe;
	}
	public int getBanner_w() {
		return banner_w;
	}
	public ArrayList<String> getBcat() {
		return bcat;
	}
	public float getBidfloor() {
		return bidfloor;
	}
	public String getCarrier() {
		return carrier;
	}
//	public String getCity() {
//		return city;
//	}
	public int getConnectiontype() {
		return connectiontype;
	}
	public String getDevice_lang() {
		return device_lang;
	}
//	public char getDevice_lmt() {
//		return device_lmt;
//	}
	public int getDevicetype() {
		return devicetype;
	}
//	public String getDisplaymanager() {
//		return displaymanager;
//	}
//	public String getDisplaymanagerver() {
//		return displaymanagerver;
//	}
	public char getDnt() {
		return dnt;
	}
	public ExtraAttributes getExtra() {
		return extra;
	}

	
	public char getExtra_atts() {
		return extra_atts;
	}
	public String getId() {
		return id;
	}
	public char getImp_secure() {
		return imp_secure;
	}
	
	public int getImp_size() {
		return imp_size;
	}
	public char getInstl() {
		return instl;
	}
//	public String getIp() {
//		return ip;
//	}
	public float getLat() {
		return lat;
	}
	public float getLon() {
		return lon;
	}
	public String getRegion() {
		return region;
	}
//	public String getUa() {
//		return ua;
//	}
//	public String getZip() {
//		return zip;
//	}
	public void setApp_cat(ArrayList<String> app_cat) {
		this.app_cat = app_cat;
	}
//	public void setApp_domain(String app_domain) {
//		this.app_domain = app_domain;
//	}
//	public void setApp_ver(String app_ver) {
//		this.app_ver = app_ver;
//	}
	public void setBadv(ArrayList<String> badv) {
		this.badv = badv;
	}
	public void setBanner_api( ArrayList<Integer> banner_api) {
		this.banner_api = banner_api;
	}
	public void setBanner_battr( ArrayList<Integer> banner_attr) {
		this.banner_battr = banner_battr;
	}
	public void setBanner_h(int banner_h) {
		this.banner_h = banner_h;
	}
	public void setBanner_pos(int banner_pos) {
		this.banner_pos = banner_pos;
	}
	public void setBanner_topframe(char banner_topframe) {
		this.banner_topframe = banner_topframe;
	}
	public void setBanner_w(int banner_w) {
		this.banner_w = banner_w;
	}
	public void setBcat(ArrayList<String> bcat) {
		this.bcat = bcat;
	}
	public void setBidfloor(float bidfloor) {
		this.bidfloor = bidfloor;
	}
	public void setCarrier(String carrier) {
		this.carrier = carrier;
	}
//	public void setCity(String city) {
//		this.city = city;
//	}
	public void setConnectiontype(int connectiontype) {
		this.connectiontype = connectiontype;
	}
	public void setDevice_lang(String device_lang) {
		this.device_lang = device_lang;
	}
//	public void setDevice_lmt(char device_lmt) {
//		this.device_lmt = device_lmt;
//	}
	public void setDevicetype(int devicetype) {
		this.devicetype = devicetype;
	}
//	public void setDisplaymanager(String displaymanager) {
//		this.displaymanager = displaymanager;
//	}
//	public void setDisplaymanagerver(String displaymanagerver) {
//		this.displaymanagerver = displaymanagerver;
//	}
	public void setDnt(char dnt) {
		this.dnt = dnt;
	}
	public void setExtra_atts(char extra_atts) {
		this.extra_atts = extra_atts;
	}
	public void setExtra(ExtraAttributes extra) {
		this.extra = extra;
	}
	public void setId(String id) {
		this.id = id;
	}
	public void setImp_secure(char imp_secure) {
		this.imp_secure = imp_secure;
	}
	public void setImp_size(int imp_size) {
		this.imp_size = imp_size;
	}
	public void setInstl(char instl) {
		this.instl = instl;
	}
//	public void setIp(String ip) {
//		this.ip = ip;
//	}
	public void setLat(float lat) {
		this.lat = lat;
	}
	public void setLon(float lon) {
		this.lon = lon;
	}
	public void setRegion(String region) {
		this.region = region;
	}
//	public void setUa(String ua) {
//		this.ua = ua;
//	}
//	
//	public void setZip(String zip) {
//		this.zip = zip;
//	}
}
class ExtraAttributes implements Serializable{
	private int banner_hmax;
	private int banner_hmin;
	private int banner_wmax;
	private int banner_wmin;
	private int device_h;
	private int device_w;
	private int user_ext_cokkie_age;
	private String user_gender;
	private String user_keywords;
	private int user_yob;
	
	ExtraAttributes(){
		banner_hmax = -1;
		banner_hmin = -1;
		banner_wmax = -1;
		banner_wmin = -1;
		device_h = -1;
		device_w = -1;
		user_ext_cokkie_age = -1;
		user_gender = "z";
		user_keywords = "";
		user_yob = -1;
	}
	public int getBanner_hmax() {
		return banner_hmax;
	}
	public int getBanner_hmin() {
		return banner_hmin;
	}
	public int getBanner_wmax() {
		return banner_wmax;
	}
	public int getBanner_wmin() {
		return banner_wmin;
	}
	public int getDevice_h() {
		return device_h;
	}
	public int getDevice_w() {
		return device_w;
	}
	public int getUser_ext_cokkie_age() {
		return user_ext_cokkie_age;
	}
	public String getUser_gender() {
		return user_gender;
	}
	public String getUser_keywords() {
		return user_keywords;
	}
	public int getUser_yob() {
		return user_yob;
	}
	public void setBanner_hmax(int banner_hmax) {
		this.banner_hmax = banner_hmax;
	}
	public void setBanner_hmin(int banner_hmin) {
		this.banner_hmin = banner_hmin;
	}
	public void setBanner_wmax(int banner_wmax) {
		this.banner_wmax = banner_wmax;
	}
	public void setBanner_wmin(int banner_wmin) {
		this.banner_wmin = banner_wmin;
	}
	public void setDevice_h(int device_h) {
		this.device_h = device_h;
	}
	public void setDevice_w(int device_w) {
		this.device_w = device_w;
	}
	
	public void setUser_ext_cokkie_age(int user_ext_cokkie_age) {
		this.user_ext_cokkie_age = user_ext_cokkie_age;
	}
	public void setUser_gender(String user_gender) {
		this.user_gender = user_gender;
	}
	public void setUser_keywords(String user_keywords) {
		this.user_keywords = user_keywords;
	}
	public void setUser_yob(int user_yob) {
		this.user_yob = user_yob;
	}
	
}

//ssh -i ~/Downloads/zeotap_specs/us_zeoa.key root@10.51.85.231
