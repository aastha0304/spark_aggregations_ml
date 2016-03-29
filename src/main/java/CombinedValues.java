import java.util.ArrayList;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import scala.Tuple2;
import org.apache.spark.api.java.function.*;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
public class CombinedValues {
	float totalSim;
	ModifiedRow mr;
	CommonRow cr;
	float badvSim;
	float bcatSim;
	float displaymgrSim;
	float bannerapiSim;
	float bannerattrSim;
	float displaymgrverSim;
	float appcatSim;
	float appverSim;
	float uaSim;
	float appdomainSim;
	float dlangSim;
	float carrierSim;
	float bidfloorSim;
	float latSim;
	float instlSim;
	float dntSim;
	float lmtSim;
	float secureSim;
	float bannertfSim;
	float dtypeSim;
	float bannerwSim;
	float bannerhSim;
	float ctypeSim;
	float bannerposSim;
	float tmaxSim;
	float citySim;
	float regionSim;
	float timeSim;
	float ipSim;
	float strSim(String s1, String s2){
		if(s1!=null && s2 != null && !s1.isEmpty() && !s2.isEmpty()){
			int blen = s2.length();
			int alen = s1.length();
			int [] costs = new int [blen + 1];
	        for (int j = 0; j < blen+1; j++)
	            costs[j] = j;
	        for (int i = 1; i <= alen; i++) {
	            // j == 0; nw = lev(i - 1, j)
	            costs[0] = i;
	            int nw = i - 1;
	            for (int j = 1; j <= blen; j++) {
	                int cj = Math.min(1 + Math.min(costs[j], costs[j - 1]), s1.charAt(i - 1) == s2.charAt(j - 1) ? nw : nw + 1);
	                nw = costs[j];
	                costs[j] = cj;
	            }
	        }
	        return 1.0f/(1+costs[blen]);
		}
		return 0;
	}
	float bidSim(float bf1, float bf2){
		if(bf2 != -1 && bf1>bf2)
			return 1;
		return 0;
	}
	float geoSim(float lat1, float lon1, float lat2, float lon2){
		if(lat1 != -1 && lat2 != -1 && lon1 != -1 && lon2 != -1){
			double dLat = Math.toRadians(lat2 - lat1);
	        double dLon = Math.toRadians(lon2 - lon1);
	        double lat1d = Math.toRadians(lat1);
	        double lat2d = Math.toRadians(lat2);
	 
	        double a = Math.pow(Math.sin(dLat / 2),2) + Math.pow(Math.sin(dLon / 2),2) * Math.cos(lat1d) * Math.cos(lat2d);
	        double c = 2 * Math.asin(Math.sqrt(a));
	        float distance = (float) ((float) 6372.8*c);
	        return 1.0f/(1+distance);
		}
		return 0;
	}
	float boolSim(char c1, char c2){
		if(c1==c2 && (c1=='1' || c1=='0'))
			return 1;
		return 0;
	}
	float typeSim(int t1, int t2){
		if(t1==t2 && (t1!=-1))
			return 1;
		return 0;
	}
	float intSim(int t1, int t2){
		if(t1==t2)
			return 1;
		return 0;
	}
	float typeStrSim(String s1, String s2){
		if(s1!=null && s2!=null && !s1.isEmpty() && s1.equals(s2))
			return 1;
		return 0;
	}
	float timeTypeSim(String ts1, String ts2){
		SimpleDateFormat format = new SimpleDateFormat("yyyy-mm-dd HH:mm:ss");
		Date d1 = null;
		Date d2 = null;
		try {
			d1 = format.parse(ts1);
			d2 = format.parse(ts2);
			float diff = d2.getTime() - d1.getTime();
			float diffSeconds = diff / 1000 % 60;
			return 1/(1+diffSeconds);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 0;
	}
	float ipTypeSim(String ip1, String ip2){
		if(!ip1.isEmpty() && !ip2.isEmpty()){
			String[] ips1 = ip1.split("\\.");
			String[] ips2 = ip1.split("\\.");
			if(ips1.length==4 && ips2.length==4){
				if(ips1[0].equals(ips2[0]))
				{
					if(ips1[1].equals(ips2[1]))
					{
						if(ips1[2].equals(ips2[2]))
						{	
							if(ips1[3].equals(ips1[3]))
							{
								return 1;
							}
							return 1>>8;
						}
						return 1>>16;
					}
					return 1>>24;
				}
				return 1>>32;
			}	
		}
		return 0;
	}
	void calSim(ModifiedRow mr, CommonRow cr){
		this.citySim = typeStrSim(mr.getCity(), cr.getCity());
		this.regionSim = typeStrSim(mr.getRegion(), cr.getRegion());
		this.timeSim = timeTypeSim(mr.getTs(), cr.getTs());
		this.ipSim = ipTypeSim(mr.getIp(), cr.getIp());
		this.badvSim = strSim(mr.getBadv(), cr.getBadv());
		this.totalSim += this.badvSim;
		this.bcatSim = strSim(mr.getBcat(), cr.getBcat());
		this.totalSim += this.bcatSim;
		this.displaymgrSim = strSim(mr.getDisplaymanager(), cr.getDisplaymanager());
		this.totalSim += this.displaymgrSim;
		this.bannerapiSim = strSim(mr.getBanner_api(), cr.getBanner_api());
		this.totalSim += this.bannerapiSim;
		this.bannerattrSim = strSim(mr.getBanner_battr(), cr.getBanner_battr());
		this.totalSim += this.bannerattrSim;
		this.displaymgrverSim = strSim(mr.getDisplaymanagerver(), cr.getDisplaymanagerver());
		this.totalSim += this.displaymgrverSim;
		this.appcatSim = strSim(mr.getApp_cat(), cr.getApp_cat());
		this.totalSim += this.appcatSim;
		this.appverSim = strSim(mr.getApp_ver(), cr.getApp_ver());
		this.totalSim += this.appverSim;
		this.uaSim = strSim(mr.getUa(), cr.getUa());
		this.totalSim += this.uaSim;
		this.appdomainSim = strSim(mr.getApp_domain(), cr.getApp_domain());
		this.totalSim += this.appdomainSim;
		this.dlangSim = typeStrSim(mr.getDevice_lang(), cr.getDevice_lang());
		this.totalSim += this.dlangSim;
		this.carrierSim = strSim(mr.getCarrier(), cr.getCarrier());
		this.totalSim += this.carrierSim;
		
		this.bidfloorSim = bidSim(mr.getBidfloor(), cr.getBidfloor());
		this.totalSim += this.bidfloorSim;
		
		this.latSim = geoSim(mr.getLat(), mr.getLon(), cr.getLat(), cr.getLon());
		this.totalSim += this.latSim;
		
		this.instlSim = boolSim(mr.getInstl(), cr.getInstl());
		this.totalSim += this.instlSim;
		this.dntSim = boolSim(mr.getDnt(), cr.getDnt());
		this.totalSim += this.dntSim;
		this.lmtSim = boolSim(mr.getDevice_lmt(), cr.getDevice_lmt());
		this.totalSim +=this.lmtSim;
		this.secureSim = boolSim(mr.getImp_secure(), cr.getImp_secure());
		this.totalSim += this.secureSim;
		this.bannertfSim = boolSim(mr.getBanner_topframe(), cr.getBanner_topframe());
		this.totalSim += this.bannertfSim;
		
		this.dtypeSim = typeSim(mr.getDevicetype(), cr.getDevicetype());
		this.totalSim += this.dtypeSim;
		this.bannerwSim = typeSim(mr.getBanner_w(), cr.getBanner_w());
		this.totalSim += this.bannerwSim;
		this.bannerhSim = typeSim(mr.getBanner_h(), cr.getBanner_h());
		this.totalSim += this.bannerhSim;
		this.ctypeSim = typeSim(mr.getConnectiontype(), cr.getConnectiontype());
		this.totalSim += this.ctypeSim;
		this.bannerposSim = typeSim(mr.getBanner_pos(), cr.getBanner_pos());
		this.totalSim += this.bannerposSim;
		
		this.tmaxSim = intSim(mr.getTmax(), cr.getTmax());
		this.totalSim += this.tmaxSim;
		
		totalSim = totalSim/25;
	}
}

class GetCombinedValues implements 
   PairFlatMapFunction<Tuple2<ArrayList<ModifiedRow>, ArrayList<CommonRow>>, ModifiedRow, ArrayList<Tuple2<CommonRow, Float>>>{
	@Override
	public Iterable<Tuple2<ModifiedRow, ArrayList<Tuple2<CommonRow, Float>>>> call(Tuple2<ArrayList<ModifiedRow> , ArrayList<CommonRow>> joined){
		ArrayList<ModifiedRow> m = joined._1;
		ArrayList<CommonRow> o = joined._2;
		
		List<Tuple2<ModifiedRow, ArrayList<Tuple2<CommonRow, Float>>>> results = new ArrayList<>();
		
		for(ModifiedRow mOb: m){
			ArrayList<Tuple2<CommonRow, Float>> mList = new ArrayList<>();
			for(CommonRow cOb: o){
				CombinedValues combiOb = new CombinedValues();
				combiOb.calSim(mOb, cOb);
				mList.add(new Tuple2(cOb, combiOb.totalSim));
			}
			results.add(new Tuple2(mOb, mList));
		}
		return results;
	}
}
class GetCombinedScores implements 
  PairFlatMapFunction<Tuple2<ArrayList<ModifiedRow>, ArrayList<CommonRow>>, ModifiedRow, Tuple2<CommonRow, Integer>>{
	@Override
	public Iterable<Tuple2<ModifiedRow, Tuple2<CommonRow, Integer>>> call(Tuple2<ArrayList<ModifiedRow> , ArrayList<CommonRow>> joined){
		ArrayList<ModifiedRow> m = joined._1;
		ArrayList<CommonRow> o = joined._2;
		
		List<Tuple2<ModifiedRow, Tuple2<CommonRow, Integer>>> results = new ArrayList<>();

		float sim;
		CommonRow cOb;
		for(ModifiedRow mOb: m){
			int olen = o.size();
			if(olen==1){
				 cOb = o.get(0);
				 CombinedValues combiOb = new CombinedValues();
				 combiOb.calSim(mOb, cOb);
				 if(combiOb.totalSim>0.65){
					 results.add(new Tuple2(mOb, new Tuple2(cOb, 1)));
				 }	 
				 else{
					 results.add(new Tuple2(mOb, new Tuple2(cOb, 0)));
				 }
			}else{
				float first = 0, second = 0;
				int firstIdx = -1, secondIdx = -1;
				CommonRow firstOb = null, secondOb = null;
				List<Tuple2<ModifiedRow, Tuple2<CommonRow, Integer>>> tres = new ArrayList<>();
				for(int idx=0;idx<o.size();idx++){
					cOb = o.get(idx);
					tres.add(new Tuple2(mOb, new Tuple2(cOb, 0)));
					CombinedValues combiOb = new CombinedValues();
					combiOb.calSim(mOb, cOb);
					if(combiOb.totalSim>first){
						second = first;
						
						if(firstOb!=null){
							secondOb = firstOb;
							secondIdx = firstIdx;
						}
		                first = combiOb.totalSim;
		                firstOb = cOb;
		                firstIdx = idx;
					}else if(combiOb.totalSim>second && combiOb.totalSim!=first){
						second = combiOb.totalSim;
						secondOb = cOb;
						secondIdx = idx;
					}
				}
				if(first>0.65 && second<0.5){
					tres.set(firstIdx, new Tuple2(mOb, new Tuple2(firstOb, 1)));
					results.addAll(tres);
				}
			}
		}
		return results;
	}
}

class GetVectorScores implements 
	FlatMapFunction<Tuple2<ArrayList<ModifiedRow>, ArrayList<CommonRow>>, LabeledPoint>{
	@Override
	public Iterable<LabeledPoint> call(Tuple2<ArrayList<ModifiedRow>, ArrayList<CommonRow>> joined){
		ArrayList<ModifiedRow> m = joined._1;
		ArrayList<CommonRow> o = joined._2;
		List<LabeledPoint> results = new ArrayList<>();

		
		CommonRow cOb;
		for(ModifiedRow mOb: m){
			int olen = o.size();
			if(olen==1){
				 cOb = o.get(0);
				 CombinedValues combiOb = new CombinedValues();
				 combiOb.calSim(mOb, cOb);
				 if(combiOb.totalSim>0.65){
					 results.add(new LabeledPoint(1.0, Vectors.dense(combiOb.badvSim, combiOb.bcatSim, combiOb.displaymgrSim, combiOb.bannerapiSim, combiOb.bannerattrSim,
							 combiOb.displaymgrverSim, combiOb.appcatSim, combiOb.appverSim, combiOb.uaSim, combiOb.appdomainSim, combiOb.dlangSim, combiOb.carrierSim,
							 combiOb.bidfloorSim, combiOb.latSim, combiOb.instlSim, combiOb.dntSim, combiOb.lmtSim, combiOb.secureSim, combiOb.bannertfSim, combiOb.dtypeSim,
							 combiOb.bannerwSim, combiOb.bannerhSim, combiOb.ctypeSim, combiOb.bannerposSim, combiOb.tmaxSim, combiOb.citySim, combiOb.regionSim, 
							 combiOb.timeSim, combiOb.ipSim)));
				 }	 
				 else{
					 results.add(new LabeledPoint(0.0, Vectors.dense(combiOb.badvSim, combiOb.bcatSim, combiOb.displaymgrSim, combiOb.bannerapiSim, combiOb.bannerattrSim,
							 combiOb.displaymgrverSim, combiOb.appcatSim, combiOb.appverSim, combiOb.uaSim, combiOb.appdomainSim, combiOb.dlangSim, combiOb.carrierSim,
							 combiOb.bidfloorSim, combiOb.latSim, combiOb.instlSim, combiOb.dntSim, combiOb.lmtSim, combiOb.secureSim, combiOb.bannertfSim, combiOb.dtypeSim,
							 combiOb.bannerwSim, combiOb.bannerhSim, combiOb.ctypeSim, combiOb.bannerposSim, combiOb.tmaxSim, combiOb.citySim, combiOb.regionSim, 
							 combiOb.timeSim, combiOb.ipSim)));
				 }
			}else{
				float first = 0, second = 0;
				int firstIdx = -1, secondIdx = -1;
				CommonRow firstOb = null, secondOb = null;
				List<LabeledPoint> tres = new ArrayList<>();
				for(int idx=0;idx<o.size();idx++){
					cOb = o.get(idx);
					CombinedValues combiOb = new CombinedValues();
					combiOb.calSim(mOb, cOb);
					
					tres.add(new LabeledPoint(0.0, Vectors.dense(combiOb.badvSim, combiOb.bcatSim, combiOb.displaymgrSim, combiOb.bannerapiSim, combiOb.bannerattrSim,
							 combiOb.displaymgrverSim, combiOb.appcatSim, combiOb.appverSim, combiOb.uaSim, combiOb.appdomainSim, combiOb.dlangSim, combiOb.carrierSim,
							 combiOb.bidfloorSim, combiOb.latSim, combiOb.instlSim, combiOb.dntSim, combiOb.lmtSim, combiOb.secureSim, combiOb.bannertfSim, combiOb.dtypeSim,
							 combiOb.bannerwSim, combiOb.bannerhSim, combiOb.ctypeSim, combiOb.bannerposSim, combiOb.tmaxSim, combiOb.citySim, combiOb.regionSim, 
							 combiOb.timeSim, combiOb.ipSim)));
					if(combiOb.totalSim>first){
						second = first;
						if(firstOb!=null){
							secondOb = firstOb;
							secondIdx = firstIdx;
						}
		                first = combiOb.totalSim;
		                firstOb = cOb;
		                firstIdx = idx;
					}else if(combiOb.totalSim>second && combiOb.totalSim!=first){
						second = combiOb.totalSim;
						secondOb = cOb;
						secondIdx = idx;
					}
				}
				if(first>0.65 && ((first-second)<0.25)){
					CombinedValues combiOb = new CombinedValues();
					cOb = firstOb;
					combiOb.calSim(mOb, cOb);
					tres.set(firstIdx, new LabeledPoint(1.0, Vectors.dense(combiOb.badvSim, combiOb.bcatSim, combiOb.displaymgrSim, combiOb.bannerapiSim, combiOb.bannerattrSim,
							 combiOb.displaymgrverSim, combiOb.appcatSim, combiOb.appverSim, combiOb.uaSim, combiOb.appdomainSim, combiOb.dlangSim, combiOb.carrierSim,
							 combiOb.bidfloorSim, combiOb.latSim, combiOb.instlSim, combiOb.dntSim, combiOb.lmtSim, combiOb.secureSim, combiOb.bannertfSim, combiOb.dtypeSim,
							 combiOb.bannerwSim, combiOb.bannerhSim, combiOb.ctypeSim, combiOb.bannerposSim, combiOb.tmaxSim, combiOb.citySim, combiOb.regionSim, 
							 combiOb.timeSim, combiOb.ipSim)));
					results.addAll(tres);
				}
			}
		}
		return results;
	}
}
//combiOb.badvSim, combiOb.bcatSim, combiOb.displaymgrSim, combiOb.bannerapiSim, combiOb.bannerattrSim,combiOb.displaymgrverSim, combiOb.appcatSim, combiOb.appverSim, combiOb.uaSim, combiOb.appdomainSim, combiOb.dlangSim, combiOb.carrierSim,combiOb.bidfloorSim, combiOb.latSim, combiOb.instlSim, combiOb.dntSim, combiOb.lmtSim, combiOb.secureSim, combiOb.bannertfSim, combiOb.dtypeSim,combiOb.bannerwSim, combiOb.bannerhSim, combiOb.ctypeSim, combiOb.bannerposSim, combiOb.tmaxSim, combiOb.citySim, combiOb.regionSim, combiOb.timeSim, combiOb.ipSim
//[0.06866224393246213,6.3027006360595745,-2.0557831534214346,0.4917859451131982,1.4044699102529405,5.725201627564257,2.3208554917549002,1.2598136360350272,8.449201273002522,2.579633539267534,-0.634530677368856,-6.692824933754689,-8.70028355981595,0.004558137488103601,-7.559674709393713,1.9722693322234175,2.9998219273585427,5.8555678756669565,-2.9266254545706714,0.13148615808039046,-4.131846704019089,5.695392692746006,-3.165940968551012,0.973480752839042,2.292575082146467,0.15659317338439668,-0.7545207747248179,0.0,0.0]
