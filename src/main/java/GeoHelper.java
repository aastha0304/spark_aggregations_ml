

import java.util.HashMap;
import java.util.Map;

public class GeoHelper {
static float haversine(float lat1, float lon1, float lat2, float lon2){
	double dLat = Math.toRadians(lat2 - lat1);
	double dLon = Math.toRadians(lon2 - lon1);
	double lat1d = Math.toRadians(lat1);
	double lat2d = Math.toRadians(lat2);

	double a = Math.pow(Math.sin(dLat / 2), 2)
			+ Math.pow(Math.sin(dLon / 2), 2) * Math.cos(lat1d) * Math.cos(lat2d);
	double c = 2 * Math.asin(Math.sqrt(a));
	float distance = (float) ((float) 6372.8 * c);
	return distance;
}
static String coordState(float lat, float lon){
	double[][] stateCoords = {{61.3850,-152.2683},{32.7990,-86.8073},{34.9513,-92.3809},{14.2417,-170.7197},{33.7712,-111.3877},{36.1700,-119.7462},{39.0646,-105.3272},{41.5834,-72.7622},{38.8964,-77.0262},{39.3498,-75.5148},{27.8333,-81.7170},{32.9866,-83.6487},{21.1098,-157.5311},{42.0046,-93.2140},{44.2394,-114.5103},{40.3363,-89.0022},{39.8647,-86.2604},{38.5111,-96.8005},{37.6690,-84.6514},{31.1801,-91.8749},{42.2373,-71.5314},{39.0724,-76.7902},{44.6074,-69.3977},{43.3504,-84.5603},{45.7326,-93.9196},{38.4623,-92.3020},{14.8058,145.5505},{32.7673,-89.6812},{46.9048,-110.3261},{35.6411,-79.8431},{47.5362,-99.7930},{41.1289,-98.2883},{43.4108,-71.5653},{40.3140,-74.5089},{34.8375,-106.2371},{38.4199,-117.1219},{42.1497,-74.9384},{40.3736,-82.7755},{35.5376,-96.9247},{44.5672,-122.1269},{40.5773,-77.2640},{18.2766,-66.3350},{41.6772,-71.5101},{33.8191,-80.9066},{44.2853,-99.4632},{35.7449,-86.7489},{31.1060,-97.6475},{40.1135,-111.8535},{37.7680,-78.2057},{18.0001,-64.8199},{44.0407,-72.7093},{47.3917,-121.5708},{44.2563,-89.6385},{38.4680,-80.9696},{42.7475,-107.2085}};
	String[] states = {"AK","AL","AR","AS","AZ","CA","CO","CT","DC","DE","FL","GA","HI","IA","ID","IL","IN","KS","KY","LA","MA","MD","ME","MI","MN","MO","MP","MS","MT","NC","ND","NE","NH","NJ","NM","NV","NY","OH","OK","OR","PA","PR","RI","SC","SD","TN","TX","UT","VA","VI","VT","WA","WI","WV","WY",};
	float min = 10000;
	String state = new String();
	for(int i = 0;i<stateCoords.length; i++){
		float dist = haversine(lat, lon, (float)stateCoords[i][0], (float)stateCoords[i][1]);
		if(dist<min){
			min = dist;
			state = states[i].toLowerCase();
		}
	}
	return state;
}
}
