import java.io.Serializable;
import java.util.ArrayList;
import scala.Tuple2;

public class KeyClass implements Serializable{
	ArrayList<String> s;

	public ArrayList<String> getS() {
		return s;
	}

	public void setS(ArrayList<String> s) {
		this.s = s;
	}
	public KeyClass(){
		s = new ArrayList<String>();
	}
	public String toString(){
		StringBuffer sbf = new StringBuffer();
		for(String str: s){
			sbf.append(str);
		}
		return sbf.toString();
	}
	@Override
	public boolean equals(Object obj) {
	    if (obj == null) {
	        return false;
	    }
	    if (!KeyClass.class.isAssignableFrom(obj.getClass())) {
	        return false;
	    }
	    final KeyClass other = (KeyClass) obj;
	    int this_size = this.s.size();
	    int other_size = other.s.size();
	    if(other_size!=this_size)
	    	return false;
	    for(int idx=0;idx<this_size;idx++){
	    	if(!this.s.get(idx).equals(other.s.get(idx)))
	    		return false;
	    }
	    return true;
	}
	@Override
	public int hashCode(){
	    return this.s.hashCode();
	}
}