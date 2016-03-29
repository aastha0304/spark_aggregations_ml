import java.io.Serializable;

public class BidAttributes implements Serializable{
	private String id;
	private String badv;
	private String bcat;
	private String displaymanager;
	//String banner_btype;
	private String banner_api;
	private String banner_battr;
	private String displaymanagerver;
	private String app_ver;
	private String app_cat;
	private String ua;
	private String ip;
	//String zip;
	private String city;
	private String region;
	private String app_domain; //
	private String device_lang;
	private String carrier;
	
	private float bidfloor;
	private float lat;
	private float lon;
	
	private char instl;
	private int banner_pos;
	private int banner_w;
	private int banner_h;
	private int connectiontype;
	private int devicetype;
	private char dnt;
	//long at; //
	//long geo_type; //
	private char device_lmt; //
	private char imp_secure; //
	private int tmax; //
	private char banner_topframe; //
	BidAttributes(){
		id = "";
		badv = "";
		bcat = "";
		displaymanager = "";
		//banner_btype = "";
		banner_api = "";
		banner_battr = "";
		displaymanagerver = "";
		app_ver = "";
		app_cat = "";
		ua = "";
		ip = "";
		//zip = "";
		city = "";
		region = "";
		carrier = "";
		app_domain = "";
		device_lang = "";
		bidfloor = -1;
		lat = -1;
		lon = -1;
		instl = '-';
		banner_pos = -1;
		banner_w = -1;
		banner_h = -1;
		connectiontype = -1;
		devicetype = -1;
		dnt = '-';
		//at = -1;
		//geo_type = -1;
		device_lmt = '-';
		imp_secure = '-';
		tmax = -1;
		banner_topframe = '-';
	}
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getBadv() {
		return badv;
	}
	public void setBadv(String badv) {
		this.badv = badv;
	}
	public String getBcat() {
		return bcat;
	}
	public void setBcat(String bcat) {
		this.bcat = bcat;
	}
	public String getDisplaymanager() {
		return displaymanager;
	}
	public void setDisplaymanager(String displaymanager) {
		this.displaymanager = displaymanager;
	}
	public String getBanner_api() {
		return banner_api;
	}
	public void setBanner_api(String banner_api) {
		this.banner_api = banner_api;
	}
	public String getBanner_battr() {
		return banner_battr;
	}
	public void setBanner_battr(String banner_battr) {
		this.banner_battr = banner_battr;
	}
	public String getDisplaymanagerver() {
		return displaymanagerver;
	}
	public void setDisplaymanagerver(String displaymanagerver) {
		this.displaymanagerver = displaymanagerver;
	}
	public String getApp_ver() {
		return app_ver;
	}
	public void setApp_ver(String app_ver) {
		this.app_ver = app_ver;
	}
	public String getApp_cat() {
		return app_cat;
	}
	public void setApp_cat(String app_cat) {
		this.app_cat = app_cat;
	}
	public String getUa() {
		return ua;
	}
	public void setUa(String ua) {
		this.ua = ua;
	}
	public String getIp() {
		return ip;
	}
	public void setIp(String ip) {
		this.ip = ip;
	}
	public String getCity() {
		return city;
	}
	public void setCity(String city) {
		this.city = city;
	}
	public String getRegion() {
		return region;
	}
	public void setRegion(String region) {
		this.region = region;
	}
	public String getApp_domain() {
		return app_domain;
	}
	public void setApp_domain(String app_domain) {
		this.app_domain = app_domain;
	}
	public String getDevice_lang() {
		return device_lang;
	}
	public void setDevice_lang(String device_lang) {
		this.device_lang = device_lang;
	}
	public String getCarrier() {
		return carrier;
	}
	public void setCarrier(String carrier) {
		this.carrier = carrier;
	}
	public float getBidfloor() {
		return bidfloor;
	}
	public void setBidfloor(float bidfloor) {
		this.bidfloor = bidfloor;
	}
	public float getLat() {
		return lat;
	}
	public void setLat(float lat) {
		this.lat = lat;
	}
	public float getLon() {
		return lon;
	}
	public void setLon(float lon) {
		this.lon = lon;
	}
	public char getInstl() {
		return instl;
	}
	public void setInstl(char instl) {
		this.instl = instl;
	}
	public int getBanner_pos() {
		return banner_pos;
	}
	public void setBanner_pos(int banner_pos) {
		this.banner_pos = banner_pos;
	}
	public int getBanner_w() {
		return banner_w;
	}
	public void setBanner_w(int banner_w) {
		this.banner_w = banner_w;
	}
	public int getBanner_h() {
		return banner_h;
	}
	public void setBanner_h(int banner_h) {
		this.banner_h = banner_h;
	}
	public int getConnectiontype() {
		return connectiontype;
	}
	public void setConnectiontype(int connectiontype) {
		this.connectiontype = connectiontype;
	}
	public int getDevicetype() {
		return devicetype;
	}
	public void setDevicetype(int devicetype) {
		this.devicetype = devicetype;
	}
	public char getDnt() {
		return dnt;
	}
	public void setDnt(char dnt) {
		this.dnt = dnt;
	}
	public char getDevice_lmt() {
		return device_lmt;
	}
	public void setDevice_lmt(char device_lmt) {
		this.device_lmt = device_lmt;
	}
	public char getImp_secure() {
		return imp_secure;
	}
	public void setImp_secure(char imp_secure) {
		this.imp_secure = imp_secure;
	}
	public int getTmax() {
		return tmax;
	}
	public void setTmax(int tmax) {
		this.tmax = tmax;
	}
	public char getBanner_topframe() {
		return banner_topframe;
	}
	public void setBanner_topframe(char banner_topframe) {
		this.banner_topframe = banner_topframe;
	}
}
