import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import scala.Tuple2;
import org.apache.spark.api.java.function.*;

public class CommonRow extends BidAttributes implements Serializable{
	private String ts;
	public String getTs() {
		return ts;
	}
	public void setTs(String ts) {
		this.ts = ts;
	}
	CommonRow(){
		super();
		ts = "";
	}
	public String toString(){
		StringBuffer sbf = new StringBuffer();
		sbf.append(this.getId()).append('\t').append(this.getBadv()).append('\t').append(this.getBcat()).append('\t')
		.append(this.getDisplaymanager()).append('\t').append(this.getBanner_api()).append('\t')
		.append(this.getBanner_battr())
		.append('\t').append(this.getDisplaymanagerver()).append('\t').append(this.getApp_ver()).append('\t')
		.append(this.getApp_cat())
		.append('\t').append(this.getUa()).append('\t').append(this.getIp())
		.append('\t').append(this.getCity()).append('\t').append(this.getRegion()).append('\t').append(this.getCarrier())
		.append('\t').append(this.getApp_domain())
		.append('\t').append(this.getDevice_lang()).append('\t').append(this.getBidfloor()).append('\t').append(this.getLat())
		.append('\t').append(this.getLon()).append('\t').append(this.getInstl())
		.append('\t').append(this.getBanner_pos()).append('\t').append(this.getBanner_w()).append('\t')
		.append(this.getBanner_h())
		.append('\t').append(this.getConnectiontype())
		.append('\t').append(this.getDevicetype()).append('\t').append(this.getDnt()).append('\t')
		.append(this.getDevice_lmt())
		.append('\t').append(this.getImp_secure()).append('\t').append(this.getTmax()).append('\t')
		.append(this.getBanner_topframe()).append('\t').append(this.getTs());
		return sbf.toString();
	}
	@Override
	public boolean equals(Object obj) {
	    if (obj == null) {
	        return false;
	    }
	    if (!CommonRow.class.isAssignableFrom(obj.getClass())) {
	        return false;
	    }
	    final CommonRow other = (CommonRow) obj;
	    if(!this.toString().equals(other.toString()))
	    	return false;
	    return true;
	}
	@Override
	public int hashCode(){
	    return this.toString().hashCode();
	}
	
}

class GetCommonRow implements PairFunction<String, KeyClass, CommonRow>{
	public Tuple2<KeyClass, CommonRow> call(String s){
		String[] line_arr = s.split("\t");
		if(line_arr.length==2){
			String ts = line_arr[0].trim();
			String bidrequest = line_arr[1].trim();
			BidAttributes crob = RequestHandle.getCommonRow(bidrequest);
			CommonRow bidOb = new CommonRow();
				 bidOb.setApp_cat(crob.getApp_cat());
				 bidOb.setApp_domain(crob.getApp_domain());
				 bidOb.setApp_ver(crob.getApp_ver());
				 bidOb.setBadv(crob.getBadv());
				 bidOb.setBanner_api(crob.getBanner_api());
				 bidOb.setBanner_battr(crob.getBanner_battr());
				 bidOb.setBanner_h(crob.getBanner_h());
				 bidOb.setBanner_pos(crob.getBanner_pos());
				 bidOb.setBanner_topframe(crob.getBanner_topframe());
				 bidOb.setBanner_w(crob.getBanner_w());
				 bidOb.setBcat(crob.getBcat());
				 bidOb.setBidfloor(crob.getBidfloor());
				 bidOb.setCarrier(crob.getCarrier());
				 bidOb.setCity(crob.getCity());
				 bidOb.setConnectiontype(crob.getConnectiontype());
				 bidOb.setDevice_lang(crob.getDevice_lang());
				 bidOb.setDevice_lmt(crob.getDevice_lmt());
				 bidOb.setDevicetype(crob.getDevicetype());
				 bidOb.setDisplaymanager(crob.getDisplaymanager());
				 bidOb.setDisplaymanagerver(crob.getDisplaymanagerver());
				 bidOb.setUa(crob.getUa());
				 bidOb.setTmax(crob.getTmax());
				 bidOb.setRegion(crob.getRegion());
				 bidOb.setLon(crob.getLon());
				 bidOb.setLat(crob.getLat());
				 bidOb.setIp(crob.getIp());
				 bidOb.setInstl(crob.getInstl());
				 bidOb.setImp_secure(crob.getImp_secure());
				 bidOb.setId(crob.getId());
				 bidOb.setDnt(crob.getDnt());
				 bidOb.setTs(ts);
			KeyClass kOb = new KeyClass();
			kOb.setS(RequestHandle.buildKey(bidOb));
			return new Tuple2(kOb, bidOb)	;	
			
		}else{
			return new Tuple2(new KeyClass(), new CommonRow())	;	
		}
	}
}

class InitList implements Function<CommonRow, ArrayList<CommonRow>>{
	@Override
	public ArrayList<CommonRow> call(CommonRow cr){
		ArrayList<CommonRow> crList = new ArrayList<CommonRow>();
		crList.add(cr);
		return crList;
	}
}
class AddInList implements Function2<ArrayList<CommonRow>, CommonRow, ArrayList<CommonRow>>{
	@Override
	public ArrayList<CommonRow> call(ArrayList<CommonRow> crList, CommonRow cr ){
		crList.add(cr);
		return crList;
	}
}
class AddInListPart implements Function2<ArrayList<CommonRow>, ArrayList<CommonRow>, ArrayList<CommonRow>>{
	@Override
	public ArrayList<CommonRow> call(ArrayList<CommonRow> crList1, ArrayList<CommonRow> crList2){
		crList1.addAll(crList2);
		return crList1;
	}
}