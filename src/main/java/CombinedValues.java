import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.List;

import org.apache.spark.api.java.function.*;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;

import scala.Tuple2;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.collections.CollectionUtils;
class AddInHash implements Function2<Map<String, Float>, Tuple2<String, Float>, Map<String, Float>>{
	@Override
	public Map<String, Float> call(Map<String, Float> scoreMap, Tuple2<String, Float> cr ){
		float currentSim = cr._2;
		String currentId = cr._1;
		if(scoreMap.get(currentId) == null || (float)scoreMap.get(currentId)<currentSim)
			scoreMap.put(currentId, currentSim);
		return scoreMap;	
	}
}

class AddPartHash implements Function2<Map<String, Float>, Map<String, Float>, Map<String, Float>>{
	@Override
	public Map<String, Float> call(Map<String, Float> crList1, Map<String, Float> crList2){
			Map<String, Float> result = new HashMap<>();
			result.putAll(crList1);
			Iterator it = crList2.entrySet().iterator();
			while (it.hasNext()) {
		        Map.Entry pair = (Map.Entry)it.next();
		        if( (result.get(pair.getKey())==null) || (float) result.get(pair.getKey()) < (float) pair.getValue() ){
		        		result.put((String) pair.getKey(), (float) pair.getValue());
		        }	
		        //System.out.println(pair.getKey() + " = " + pair.getValue());
		        it.remove(); // avoids a ConcurrentModificationException
		    }
		return result;
	}
}

public class CombinedValues {
	private float app_cat_sim;
	private float app_domain_sim; //
	private float app_ver_sim;
	
	private float badv_sim;
	private float banner_api_sim;
	private float banner_battr_sim;
	private float banner_h_sim;
	private float banner_pos_sim;
	private float banner_topframe_sim; //
	private float banner_w_sim;
	private float bcat_sim;
	private float bidfloor_sim;
	
	private float carrier_sim;
	private float city_sim;
	private float connectiontype_sim;
	
	CommonRow cr;
	
	private float device_lang_sim;
	private float device_lmt_sim; //
	private float devicetype_sim;
	private float displaymanager_sim;
	private float displaymanagerver_sim;
	private float dnt_sim;
	
	private float extra_atts_sim;
	private float extra_sim;
	
	private float geo_sim;

	private float imp_secure_sim; //
	private float imp_size_sim;
	private float instl_sim;
	private float ip_sim;
	
	ModifiedRow mr;
	
	private float region_sim;
	
	private float small_ts_sim;
	
	private float time_sim;
	
	float totalSim;
		
	private float ua_sim;
	private float zip_sim;
	float appVerSim(String mver, String over){
		//default
		   if(StringUtils.isEmpty(mver) && StringUtils.isEmpty(over))
			   return 0;
		   //non default
		   if(over != null && mver !=null && !over.isEmpty() && !mver.isEmpty() && over.startsWith(mver))
			   return 1;
		   return -1;
	   }
	float arrSim(ArrayList outer, ArrayList inner){
		//default
		   if(CollectionUtils.isEmpty(outer) && CollectionUtils.isEmpty(inner))
			   return 0;
		 //non default
		   else if (outer !=null && inner != null && !outer.isEmpty() && !inner.isEmpty() && outer.containsAll(inner))
			   return 1;
		  return -1;
	   }

	float bidSim(float bf1, float bf2) {
		//default
		if(bf1 == -1 && bf2 == -1)
			return 0;
		//non default
		if(bf2 != -1 && bf1 > bf2 )
			return 1;
		return -1;
	}

	float boolSim(char c1, char c2) {
		//default
		if(c1 != '1'  && c1 != '0' && c2 != '1' && c2 != '0' && c1==c2)
			return 0;
		//non default
		if (c1 == c2 && (c1 == '1' || c1 == '0'))
			return 1;
		return -1;
	}

	void calSim(ModifiedRow mr, CommonRow cr) {
		this.app_cat_sim = arrSim(mr.getApp_cat(), cr.getApp_cat());
		this.totalSim += this.app_cat_sim;
		this.app_domain_sim = strSim(mr.getApp_domain(), cr.getApp_domain());
		//this.totalSim += this.app_domain_sim;
		this.app_ver_sim = appVerSim(mr.getApp_ver(), cr.getApp_ver());
		//this.totalSim += this.app_ver_sim;
		
		this.badv_sim = arrSim(mr.getBadv(), cr.getBadv());
		this.totalSim += this.badv_sim;
		this.banner_api_sim =arrSim(mr.getBanner_api(), cr.getBanner_api());
		this.totalSim += this.banner_api_sim;
		this.banner_battr_sim = arrSim(mr.getBanner_battr(), cr.getBanner_battr());
		this.totalSim += this.banner_battr_sim;
		this.banner_h_sim = typeSim(mr.getBanner_h(), cr.getBanner_h());
		//this.totalSim += this.banner_h_sim;
		this.banner_pos_sim = typeSim(mr.getBanner_pos(), cr.getBanner_pos());
		//this.totalSim += this.banner_pos_sim;
		this.banner_topframe_sim = boolSim(mr.getBanner_topframe(), cr.getBanner_topframe());
		//this.totalSim += this.banner_topframe_sim;
		this.banner_w_sim = typeSim(mr.getBanner_w(), cr.getBanner_w());
		//this.totalSim += this.banner_w_sim;
		this.bcat_sim = arrSim(mr.getBcat(), cr.getBcat());
		this.totalSim += this.bcat_sim;
		//this.bidfloor_sim = bidSim(mr.getBidfloor(), cr.getBidfloor());
		//this.totalSim += this.bidfloorSim;
		
		//this.city_sim = typeStrSim(mr.getCity(), cr.getCity());
		this.carrier_sim = strSim(mr.getCarrier(), cr.getCarrier());
		this.totalSim += this.carrier_sim;
		this.connectiontype_sim = typeSim(mr.getConnectiontype(), cr.getConnectiontype());
		this.totalSim += this.connectiontype_sim;
		
		this.device_lang_sim = typeStrSim(mr.getDevice_lang(), cr.getDevice_lang());
		this.totalSim += this.device_lang_sim;
		this.device_lmt_sim = boolSim(mr.getDevice_lmt(), cr.getDevice_lmt());
		//this.totalSim += this.device_lmt_sim;
		this.devicetype_sim = typeSim(mr.getDevicetype(), cr.getDevicetype());
	    this.totalSim += this.devicetype_sim;
		//this.displaymanager_sim = strSim(mr.getDisplaymanager(), cr.getDisplaymanager());
		//this.totalSim += this.displaymanager_sim;
		//this.displaymanagerver_sim = strSim(mr.getDisplaymanagerver(), cr.getDisplaymanagerver());
		//this.totalSim += this.displaymgrverSim;
		this.dnt_sim = boolSim(mr.getDnt(), cr.getDnt());
		this.totalSim += this.dnt_sim;
		
		
		
		//this.geo_sim = geoSim(mr.getLat(), mr.getLon(), cr.getLat(), cr.getLon());
		//this.totalSim += this.latSim;
		
		this.imp_secure_sim = boolSim(mr.getImp_secure(), cr.getImp_secure());
		this.totalSim += this.imp_secure_sim;
		this.imp_size_sim = typeSim(mr.getImp_size(), cr.getImp_size());
		this.totalSim += this.imp_size_sim;
		this.instl_sim = boolSim(mr.getInstl(), cr.getInstl());
		this.totalSim += this.instl_sim;
		//this.ip_sim = ipTypeSim(mr.getIp(), cr.getIp());

		//this.region_sim = typeStrSim(mr.getRegion(), cr.getRegion());
		
		//this.small_ts_sim = smalltimeTypeSim(mr.getSmallTs(), cr.getSmallTs());
		//this.totalSim += this.small_ts_sim;
		
		//this.time_sim = timeTypeSim(mr.getTs(), cr.getTs());
		//this.totalSim += this.time_sim;
		 
		//this.ua_sim = strSim(mr.getUa(), cr.getUa());
		//this.totalSim += this.uaSim;
		
		//this.zip_sim = typeStrSim(mr.getZip(), cr.getZip());
		
		this.extra_atts_sim = boolSim(mr.getExtra_atts(), cr.getExtra_atts());
		if(this.extra_atts_sim == 1){
			this.extra_sim = typeSim(mr.getExtra().getBanner_hmax(), cr.getExtra().getBanner_hmax()) +
					typeSim(mr.getExtra().getBanner_hmin(), cr.getExtra().getBanner_hmin()) +
					typeSim(mr.getExtra().getBanner_wmax(), cr.getExtra().getBanner_wmax()) +
					typeSim(mr.getExtra().getBanner_wmin(), cr.getExtra().getBanner_wmin()) +
					typeSim(mr.getExtra().getDevice_h(), cr.getExtra().getDevice_h()) +
					typeSim(mr.getExtra().getDevice_w(), cr.getExtra().getDevice_w()) +
					typeSim(mr.getExtra().getUser_ext_cokkie_age(), cr.getExtra().getUser_ext_cokkie_age()) +
					typeSim(mr.getExtra().getUser_yob(), cr.getExtra().getUser_yob()) + 
					strSim(mr.getExtra().getUser_keywords(), cr.getExtra().getUser_keywords()) +
					genderSim(mr.getExtra().getUser_gender(), cr.getExtra().getUser_gender());
			this.totalSim += this.extra_sim;
			this.totalSim = this.totalSim/23;
		}else if(this.extra_atts_sim  == 0)
			this.totalSim = this.totalSim / 13;
		else{
			this.totalSim += this.extra_atts_sim;
			this.totalSim = this.totalSim/24;
		}
	}

	float genderSim(String mr_gen, String or_gen){
		//both default
		if(StringUtils.isEmpty(mr_gen) && StringUtils.isEmpty(or_gen))
			return 0;
		if(StringUtils.isEmpty(mr_gen) && !StringUtils.isEmpty(or_gen))
			return -1;
		if(!StringUtils.isEmpty(mr_gen) && StringUtils.isEmpty(or_gen))
			return -1;
		if(mr_gen.equals("z") && or_gen.equals("z"))
			return 0;
		//both non default
		if(mr_gen.equals(or_gen))
 			return 1;
 		return -1;
	}

//	float intSim(int t1, int t2) {
//		if (t1 == t2)
//			return 1;
//		return 0;
//	}

	float geoSim(float lat1, float lon1, float lat2, float lon2) {
		//both non default, haversine
		if (lat1 != -1 && lat2 != -1 && lon1 != -1 && lon2 != -1) {
			float distance = GeoHelper.haversine(lat1, lon1, lat2, lon2);
			
			return (1.0f )/ (1 + distance);
		}
		//both default
		else if(lat1 == -1 && lon1 == -1 && lat2 ==-1 && lon2 ==-1)
			return 0;
		return -1;
	}

	float ipTypeSim(String ip1, String ip2) {
		if (!StringUtils.isEmpty(ip1) && !StringUtils.isEmpty(ip2)) {
			String[] ips1 = ip1.split("\\.");
			String[] ips2 = ip1.split("\\.");
			if (ips1.length == 4 && ips2.length == 4) {
				if (ips1[0].equals(ips2[0])) {
					if (ips1[1].equals(ips2[1])) {
						if (ips1[2].equals(ips2[2])) {
							if (ips1[3].equals(ips1[3])) {
								return 1;
							}
							return 1;
						}
						return 1 >> 8;
					}
					return 1 >> 16;
				}
				return 1 >> 24;
			}
		}
		else if((ip1 == null && ip2 == null) || (ip1.isEmpty() && ip2.isEmpty()))
			return 0;
		return -1;
	}

	float strSim(String s1, String s2) {
		//both default
		if(StringUtils.isEmpty(s1) && StringUtils.isEmpty(s2))
			return 0;
		//both non default,  levenshtein
		if (s1 != null && s2 != null && !s1.isEmpty() && !s2.isEmpty()) {
			int blen = s2.length();
			int alen = s1.length();
			int[] costs = new int[blen + 1];
			for (int j = 0; j < blen + 1; j++)
				costs[j] = j;
			for (int i = 1; i <= alen; i++) {
				// j == 0; nw = lev(i - 1, j)
				costs[0] = i;
				int nw = i - 1;
				for (int j = 1; j <= blen; j++) {
					int cj = Math.min(1 + Math.min(costs[j], costs[j - 1]),
							s1.charAt(i - 1) == s2.charAt(j - 1) ? nw : nw + 1);
					nw = costs[j];
					costs[j] = cj;
				}
			}
			return 1.0f / (1 + costs[blen]);
		}
		return -1;
	}
	float smalltimeTypeSim(double ts1, double ts2){
		try{
			Date d1 = new Date((long) (ts1*1000));
			Date d2 = new Date((long) (ts2*1000));
			if(Math.abs( d1.getTime()- d2.getTime()) <=0.3){
				return 1;
			}
		}catch(Exception e){
			e.printStackTrace();
		}
		return 0;
	}
   float timeTypeSim(String ts1, String ts2) {
	SimpleDateFormat format = new SimpleDateFormat("yyyy-mm-dd HH:mm:ss");
	Date d1 = null;
	Date d2 = null;
	try {
		d1 = format.parse(ts1);
		d2 = format.parse(ts2);
		float diff = d2.getTime() - d1.getTime();
		float diffSeconds = diff / 1000 % 60;
		return 1 / (1 + diffSeconds);
	} catch (Exception e) {
		e.printStackTrace();
	}
	return 0;
}
   float typeSim(int t1, int t2) {
	//both non default
	if ((t1 == t2) && (t1 != -1))
		return 1;
	//both default
	else if(t1 == t2)
		return 0;
	return -1;
}
	float typeStrSim(String s1, String s2) {
		//both non default
		if (s1 != null && s2 != null && !s1.isEmpty() && !s2.isEmpty() && s1.equals(s2))
			return 1;
		//default
		else if(StringUtils.isEmpty(s1) && StringUtils.isEmpty(s2))
			return 0;
		return -1;
	}
}

class GetCombinedValues implements
		PairFlatMapFunction<Tuple2<ArrayList<ModifiedRow>, ArrayList<CommonRow>>, String, Tuple2<String, Float>> {
	@Override
	public Iterable<Tuple2<String, Tuple2<String, Float>>> call(
			Tuple2<ArrayList<ModifiedRow>, ArrayList<CommonRow>> joined) {
		ArrayList<ModifiedRow> m = joined._1;
		ArrayList<CommonRow> o = joined._2;

		List<Tuple2<String, Tuple2<String, Float>>> results = new ArrayList<>();

		for (ModifiedRow mOb : m) {
			for (CommonRow cOb : o) {
				CombinedValues combiOb = new CombinedValues();
				if(!StringUtils.isEmpty(cOb.getId())){
					SimpleDateFormat format = new SimpleDateFormat("yyyy-mm-dd HH:mm:ss");
					Date d1 = null;
					Date d2 = null;
					try {
						d1 = format.parse(mOb.getTs());
						d2 = format.parse(cOb.getTs());
						float diff = d2.getTime() - d1.getTime();
						float diffSeconds = diff / 1000 % 60;
						if(diffSeconds <= 180){
							combiOb.calSim(mOb, cOb);
							results.add(new Tuple2(mOb.getO_id(), new Tuple2(cOb.getId(), combiOb.totalSim)));
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
					
	//				try{
	//					Date d1 = new Date((long) (mOb.getSmallTs()*1000));
	//					Date d2 = new Date((long) (cOb.getSmallTs()*1000));
	//					if(Math.abs( d1.getTime()- d2.getTime()) <=3000){
	//						combiOb.calSim(mOb, cOb);
	//						results.add(new Tuple2(mOb.getO_id(), new Tuple2(cOb.getId(), combiOb.totalSim)));
	//					}
	//				}catch(Exception e){
	//					e.printStackTrace();
	//				}
				}	
				
			}
		}
		return results;
	}
}

//class GetVectorScores implements FlatMapFunction<Tuple2<ArrayList<ModifiedRow>, ArrayList<CommonRow>>, LabeledPoint> {
//	@Override
//	public Iterable<LabeledPoint> call(Tuple2<ArrayList<ModifiedRow>, ArrayList<CommonRow>> joined) {
//		ArrayList<ModifiedRow> m = joined._1;
//		ArrayList<CommonRow> o = joined._2;
//		List<LabeledPoint> results = new ArrayList<>();
//
//		CommonRow cOb;
//		for (ModifiedRow mOb : m) {
//			int olen = o.size();
//			if (olen == 1) {
//				cOb = o.get(0);
//				CombinedValues combiOb = new CombinedValues();
//				combiOb.calSim(mOb, cOb);
//				if (combiOb.totalSim > 0.65) {
//					results.add(new LabeledPoint(1.0,
//							Vectors.dense(combiOb.badvSim, combiOb.bcatSim, combiOb.displaymgrSim, combiOb.bannerapiSim,
//									combiOb.bannerattrSim, combiOb.displaymgrverSim, combiOb.appcatSim,
//									combiOb.appverSim, combiOb.uaSim, combiOb.appdomainSim, combiOb.dlangSim,
//									combiOb.carrierSim, combiOb.bidfloorSim, combiOb.latSim, combiOb.instlSim,
//									combiOb.dntSim, combiOb.lmtSim, combiOb.secureSim, combiOb.bannertfSim,
//									combiOb.dtypeSim, combiOb.bannerwSim, combiOb.bannerhSim, combiOb.ctypeSim,
//									combiOb.bannerposSim, combiOb.tmaxSim, combiOb.citySim, combiOb.regionSim,
//									combiOb.timeSim, combiOb.ipSim)));
//				} else {
//					results.add(new LabeledPoint(0.0,
//							Vectors.dense(combiOb.badvSim, combiOb.bcatSim, combiOb.displaymgrSim, combiOb.bannerapiSim,
//									combiOb.bannerattrSim, combiOb.displaymgrverSim, combiOb.appcatSim,
//									combiOb.appverSim, combiOb.uaSim, combiOb.appdomainSim, combiOb.dlangSim,
//									combiOb.carrierSim, combiOb.bidfloorSim, combiOb.latSim, combiOb.instlSim,
//									combiOb.dntSim, combiOb.lmtSim, combiOb.secureSim, combiOb.bannertfSim,
//									combiOb.dtypeSim, combiOb.bannerwSim, combiOb.bannerhSim, combiOb.ctypeSim,
//									combiOb.bannerposSim, combiOb.tmaxSim, combiOb.citySim, combiOb.regionSim,
//									combiOb.timeSim, combiOb.ipSim)));
//				}
//			} else {
//				float first = 0, second = 0;
//				int firstIdx = -1, secondIdx = -1;
//				CommonRow firstOb = null, secondOb = null;
//				List<LabeledPoint> tres = new ArrayList<>();
//				for (int idx = 0; idx < o.size(); idx++) {
//					cOb = o.get(idx);
//					CombinedValues combiOb = new CombinedValues();
//					combiOb.calSim(mOb, cOb);
//
//					tres.add(new LabeledPoint(0.0,
//							Vectors.dense(combiOb.badvSim, combiOb.bcatSim, combiOb.displaymgrSim, combiOb.bannerapiSim,
//									combiOb.bannerattrSim, combiOb.displaymgrverSim, combiOb.appcatSim,
//									combiOb.appverSim, combiOb.uaSim, combiOb.appdomainSim, combiOb.dlangSim,
//									combiOb.carrierSim, combiOb.bidfloorSim, combiOb.latSim, combiOb.instlSim,
//									combiOb.dntSim, combiOb.lmtSim, combiOb.secureSim, combiOb.bannertfSim,
//									combiOb.dtypeSim, combiOb.bannerwSim, combiOb.bannerhSim, combiOb.ctypeSim,
//									combiOb.bannerposSim, combiOb.tmaxSim, combiOb.citySim, combiOb.regionSim,
//									combiOb.timeSim, combiOb.ipSim)));
//					if (combiOb.totalSim > first) {
//						second = first;
//						if (firstOb != null) {
//							secondOb = firstOb;
//							secondIdx = firstIdx;
//						}
//						first = combiOb.totalSim;
//						firstOb = cOb;
//						firstIdx = idx;
//					} else if (combiOb.totalSim > second && combiOb.totalSim != first) {
//						second = combiOb.totalSim;
//						secondOb = cOb;
//						secondIdx = idx;
//					}
//				}
//				if (first > 0.65 && ((first - second) >= 0.25)) {
//					CombinedValues combiOb = new CombinedValues();
//					cOb = firstOb;
//					combiOb.calSim(mOb, cOb);
//					tres.set(firstIdx,
//							new LabeledPoint(1.0,
//									Vectors.dense(combiOb.badvSim, combiOb.bcatSim, combiOb.displaymgrSim,
//											combiOb.bannerapiSim, combiOb.bannerattrSim, combiOb.displaymgrverSim,
//											combiOb.appcatSim, combiOb.appverSim, combiOb.uaSim, combiOb.appdomainSim,
//											combiOb.dlangSim, combiOb.carrierSim, combiOb.bidfloorSim, combiOb.latSim,
//											combiOb.instlSim, combiOb.dntSim, combiOb.lmtSim, combiOb.secureSim,
//											combiOb.bannertfSim, combiOb.dtypeSim, combiOb.bannerwSim,
//											combiOb.bannerhSim, combiOb.ctypeSim, combiOb.bannerposSim, combiOb.tmaxSim,
//											combiOb.citySim, combiOb.regionSim, combiOb.timeSim, combiOb.ipSim)));
//					results.addAll(tres);
//				}
//			}
//		}
//		return results;
//	}
//}
// combiOb.badvSim, combiOb.bcatSim, combiOb.displaymgrSim,
// combiOb.bannerapiSim, combiOb.bannerattrSim,combiOb.displaymgrverSim,
// combiOb.appcatSim, combiOb.appverSim, combiOb.uaSim, combiOb.appdomainSim,
// combiOb.dlangSim, combiOb.carrierSim,combiOb.bidfloorSim, combiOb.latSim,
// combiOb.instlSim, combiOb.dntSim, combiOb.lmtSim, combiOb.secureSim,
// combiOb.bannertfSim, combiOb.dtypeSim,combiOb.bannerwSim, combiOb.bannerhSim,
// combiOb.ctypeSim, combiOb.bannerposSim, combiOb.tmaxSim, combiOb.citySim,
// combiOb.regionSim, combiOb.timeSim, combiOb.ipSim
// [0.06866224393246213,6.3027006360595745,-2.0557831534214346,0.4917859451131982,1.4044699102529405,5.725201627564257,2.3208554917549002,1.2598136360350272,8.449201273002522,2.579633539267534,-0.634530677368856,-6.692824933754689,-8.70028355981595,0.004558137488103601,-7.559674709393713,1.9722693322234175,2.9998219273585427,5.8555678756669565,-2.9266254545706714,0.13148615808039046,-4.131846704019089,5.695392692746006,-3.165940968551012,0.973480752839042,2.292575082146467,0.15659317338439668,-0.7545207747248179,0.0,0.0]


class InitHash implements Function<Tuple2<String, Float>, Map<String, Float>>{
	@Override
	public Map<String, Float> call(Tuple2<String, Float> cr){
		Map<String, Float> crList = new HashMap<>();
		crList.put(cr._1,  cr._2);
		return crList;
	}
}
