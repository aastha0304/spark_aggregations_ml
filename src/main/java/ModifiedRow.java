import java.io.Serializable;
import java.util.ArrayList;

import scala.Tuple2;
import org.apache.spark.api.java.function.*;
public class ModifiedRow extends CommonRow implements Serializable{
	private String o_id;
	ModifiedRow(){
		super();
		o_id = "";
	}
	public String getO_id() {
		return o_id;
	}
    public void setO_id(String o_id) {
		this.o_id = o_id;
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
		.append('\t').append(this.getDevice_lang()).append('\t').append(this.getBidfloor()).append('\t')
		.append(this.getLat())
		.append('\t').append(this.getLon()).append('\t').append(this.getInstl())
		.append('\t').append(this.getBanner_pos()).append('\t').append(this.getBanner_w()).append('\t')
		.append(this.getBanner_h())
		.append('\t').append(this.getConnectiontype())
		.append('\t').append(this.getDevicetype()).append('\t').append(this.getDnt()).append('\t')
		.append(this.getDevice_lmt())
		.append('\t').append(this.getImp_secure()).append('\t')
		.append(this.getBanner_topframe()).append('\t').append(this.getTs()).append('\t').append(this.getO_id());
		return sbf.toString();
	}
	@Override
	public boolean equals(Object obj) {
	    if (obj == null) {
	        return false;
	    }
	    if (!ModifiedRow.class.isAssignableFrom(obj.getClass())) {
	        return false;
	    }
	    final ModifiedRow other = (ModifiedRow) obj;
	    if(!this.toString().equals(other.toString()))
	    	return false;
	    return true;
	}
	@Override
	public int hashCode(){
	    return this.toString().hashCode();
	}
}
class GetModifiedRow implements PairFunction<String, KeyClass, ModifiedRow> {
	  public Tuple2<KeyClass, ModifiedRow> call(String s) { 
		  String[] line_arr = s.split("\t");
		  
		  if(line_arr.length==4){
			  String ts = line_arr[0].trim();
			  String bidrequest = line_arr[1].trim();
			  String orig_id = line_arr[2].trim();
			  BidAttributes crob = RequestHandle.getCommonRow(bidrequest);
		  	  ModifiedRow bidOb = new ModifiedRow();
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
				 bidOb.setRegion(crob.getRegion());
				 bidOb.setLon(crob.getLon());
				 bidOb.setLat(crob.getLat());
				 bidOb.setIp(crob.getIp());
				 bidOb.setInstl(crob.getInstl());
				 bidOb.setImp_secure(crob.getImp_secure());
				 bidOb.setId(crob.getId());
				 bidOb.setDnt(crob.getDnt());
				 bidOb.setTs(ts);
				 bidOb.setO_id(orig_id.toLowerCase());
				 bidOb.setExtra(crob.getExtra());
				 bidOb.setExtra_atts(crob.getExtra_atts());
			
			KeyClass kOb = new KeyClass();
			kOb.setS(RequestHandle.buildKey(bidOb));
			return new Tuple2(kOb, bidOb)	;	 
			  
		  }else{
			  return new Tuple2(new KeyClass(), new ModifiedRow())	;	 
		  }
	  }
}
class InitMList implements Function<ModifiedRow, ArrayList<ModifiedRow>>{
	@Override
	public ArrayList<ModifiedRow> call(ModifiedRow cr){
		ArrayList<ModifiedRow> crList = new ArrayList<ModifiedRow>();
		crList.add(cr);
		return crList;
	}
}
class AddInMList implements Function2<ArrayList<ModifiedRow>, ModifiedRow, ArrayList<ModifiedRow>>{
	@Override
	public ArrayList<ModifiedRow> call(ArrayList<ModifiedRow> crList, ModifiedRow cr ){
		crList.add(cr);
		return crList;
	}
}
class AddInMListPart implements Function2<ArrayList<ModifiedRow>, ArrayList<ModifiedRow>, ArrayList<ModifiedRow>>{
	@Override
	public ArrayList<ModifiedRow> call(ArrayList<ModifiedRow> crList1, ArrayList<ModifiedRow> crList2){
		crList1.addAll(crList2);
		return crList1;
	}
}