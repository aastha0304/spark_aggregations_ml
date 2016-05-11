import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS;
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;
import org.apache.spark.mllib.regression.LabeledPoint;

import scala.Tuple2;
import org.apache.spark.api.java.function.FlatMapFunction;


/***
 * 
 * @author aastha TO DO get uniques in joined data 'data', get them by
 *         combinebykey() and not distinct() change prints to loggers remove
 *         clines and scores organize imports
 */
public class ZhlFlatten {
	static Logger logger = Logger.getLogger(ZhlFlatten.class.getName());

	public static void main(String[] args) {
		if (args.length < 3) {
			System.out.println("1 - modified path, 2 - original path, 3 labels");
			System.exit(-1);
		}
		String logMFile = args[0]; // Should be some file on your system
		String logOFile = args[1]; // Should be some file on your system
		SparkConf conf = new SparkConf().setAppName("ZHL").set("spark.rdd.compress", "true");
		conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer");
		conf.set("spark.kryo.registrator","MyKryoRegistrator");
		
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaPairRDD<KeyClass, ArrayList<ModifiedRow>> mLines = sc.textFile(logMFile, 300)
				.mapToPair(new GetModifiedRow()).combineByKey(new InitMList(), new AddInMList(), new AddInMListPart());
		JavaPairRDD<KeyClass, ArrayList<CommonRow>> oLines = sc.textFile(logOFile, 600)
//				.filter(new Function<String, Boolean>() {
//					@Override
//					public Boolean call(String s) throws Exception {
//						if (s.contains("\"carrier\":\"T-Mobile\"") || s.contains("\"carrier\":\"T-Mobile USA\"")
//										|| s.contains("\"carrier\":\"310-260\""))
//							return true;
//						return false;
//					}
//				})
				.mapToPair(new GetCommonRow()).combineByKey(new InitList(), new AddInList(), new AddInListPart());

		// JavaPairRDD<ModifiedRow, ArrayList<Tuple2<CommonRow, Float>>> cLines
		// = mLines
		// .join(oLines).values().distinct().flatMapToPair(new
		// GetCombinedValues()).cache();
		//
		// JavaPairRDD<ModifiedRow, Tuple2<CommonRow, Integer>> scored = mLines
		// .join(oLines).values().distinct().flatMapToPair(new
		// GetCombinedScores()).cache();

		//JavaRDD<Tuple2<ArrayList<ModifiedRow>, ArrayList<CommonRow>>> tupleData = mLines.join(oLines).values().cache();
		//tupleData.saveAsTextFile(args[2]);
		
		//JavaPairRDD<String, Map<String, Float>> scoredSets = mLines.join(oLines).values().flatMapToPair(new GetCombinedValues());
		//scoredSets.saveAsTextFile(args[3]);
		
//		mLines.saveAsTextFile(args[2]);
//		oLines.saveAsTextFile(args[3]);
		mLines.join(oLines).saveAsTextFile(args[4]);
		mLines.join(oLines).values()
				.flatMapToPair(new GetCombinedValues())
				.combineByKey(new InitHash(), new AddInHash(), new AddPartHash())
			    .flatMap(new FlatMapFunction<Tuple2<String, Map<String, Float>>, String>() {
					@Override
					public Iterable<String> call(Tuple2<String, Map<String, Float>> scoreMap){
						List<String>  result = new ArrayList<>();
						String tid = scoreMap._1;
						Set<Map.Entry<String, Float>>   es = scoreMap._2.entrySet();
						Iterator it = es.iterator();
						
						
						
						float max = 0.5f;
						int idx = -1;
						String maxKey = "";
						int c = -1;
						//finding max
						while (it.hasNext()) { 
					        Map.Entry pair = (Map.Entry)it.next();
					        float currentSim = (float)pair.getValue();
					        if(currentSim>max){
					        		max = currentSim;
					        }
					    }
						Iterator add = scoreMap._2.entrySet().iterator();
						int count_of_max = 0;
						String oid = new String();
						//finding how many have a max
						//System.out.println("tid "+tid);
						while (add.hasNext()) { 
							
					        Map.Entry pair = (Map.Entry)add.next();
					        //result.add(tid+','+pair.getKey()+','+pair.getValue()+','+'0'); 
					        float currentSim = (float)pair.getValue();
					        if(currentSim==max){
					        		count_of_max++;
					        		if(count_of_max>1  && !((String)pair.getKey()).equals(oid))
					        		{	
					        			break;
					        		}
					        		else{
					        			oid = (String)pair.getKey();
					        		}
					        }
					        add.remove(); // avoids a ConcurrentModificationException
					    }
						//update only if unique
						if(count_of_max == 1){
							result.add(tid+','+oid+','+max);
						}
						
					    return result;
					}
				}).saveAsTextFile(args[5]);
		 
		
		
		//JavaPairRDD<String, Map<String, Float>> sims = flatSets.combineByKey(new InitSet(), new AddInSet(), new AddAccSet()).flatMapToPair(new LabelPairs());
		
//		JavaRDD<LabeledPoint> data = tupleData.flatMap(new GetVectorScoresWithoutBadvBcatCheck1_1Maps()).cache();
//		JavaRDD<String> actualMatchStrings = tupleData.flatMap(new GetVectorScoresWithStringsWithoutBadvBcatCheck1_1Maps());
//		JavaRDD<String> actualMatchStringsFor1_1Maps = tupleData
//				.flatMap(new GetVectorScoresWithStringsWithoutBadvBcatFor1_1MapsCheck1_1Maps());
//
//		System.out.println("###### LENGTH : " + tupleData.count());
//
//		actualMatchStringsFor1_1Maps.saveAsTextFile(args[7]);
//		actualMatchStrings.saveAsTextFile(args[5]);
//		// //mLines.saveAsTextFile(args[2]);
//		// //oLines.saveAsTextFile(args[3]);
//		// cLines.saveAsTextFile(args[2]);
//		// scored.saveAsTextFile(args[3]);
//		data.saveAsTextFile(args[4]);
//		// System.out.println(mLines.count());
//		// System.out.println(oLines.count());
//		// System.out.println(cLines.count());
//		// System.out.println(scored.count());
//		System.out.println("########### DATA COUNT : " + data.count());
//		JavaRDD<LabeledPoint>[] splits = data.randomSplit(new double[] { 0.6, 0.4 }, 11L);
//		JavaRDD<LabeledPoint> training = splits[0].cache();
//		List<LabeledPoint> results = training.take(10);
//		for (LabeledPoint lp : results) {
//			System.out.println(lp.toString());
//		}
//		JavaRDD<LabeledPoint> test = splits[1];
//		results = test.take(10);
//		for (LabeledPoint lp : results) {
//			System.out.println(lp.toString());
//		}
//		final LogisticRegressionModel model = new LogisticRegressionWithLBFGS().setNumClasses(2).run(training.rdd());
//		// model.clearThreshold();
//		// Compute raw scores on the test set.
//		JavaRDD<Tuple2<Object, Object>> scoreAndLabels = test.map(new Function<LabeledPoint, Tuple2<Object, Object>>() {
//			public Tuple2<Object, Object> call(LabeledPoint p) {
//				Double score = model.predict(p.features());
//				return new Tuple2<Object, Object>(score, p.label());
//			}
//		});
//
//		// Get evaluation metrics.
//		BinaryClassificationMetrics metrics = new BinaryClassificationMetrics(JavaRDD.toRDD(scoreAndLabels));
//		double auROC = metrics.areaUnderROC();
//		System.out.println(model.weights());
//		System.out.println("Area under ROC = " + auROC);
		sc.stop();
	}
}
// to run
// mvn package
// zip -d target/zhlFlatten.jar META-INF/*.RSA META-INF/*.DSA META-INF/*.SF
// remove the files given in command line arguments for saving
// ../spark-1.5.1-bin-hadoop2.6/bin/spark-submit --class "ZhlFlatten" --master
// local[8]
// /Users/aastha/zHLexperiments/codes/spark_codes/zhl/target/zhlFlatten.jar
// ~/zHLexperiments/data/2016-03-16-08-15-01.UTC.modified_bid_request.staging-aws-va-1_451aa.log
// ~/zHLexperiments/data/2016-03-16-08-15-01.UTC.original_bid_request.staging-aws-va-1_187c0.log
// ~/combi ~/score ~/data

// /usr/hdp/2.4.0.0-169/spark/bin/spark-submit --class "ZhlFlatten" --master
// yarn --deploy-mode client target/zhlFlatten.jar ~/zhl_2016-04-01/modified.log
// ~/zhl_2016-04-01/original.log ~/zhl_2016-04-01_results/modified/
// ~/zhl_2016-04-01_results/original/ ~/zhl_2016-04-01_results/res///
// /usr/hdp/2.4.0.0-169/spark/bin/spark-submit --class "ZhlFlatten" --master
// yarn --deploy-mode client target/zhlFlatten.jar ~/zhl_2016-04-01/modified.log
// ~/zhl_2016-04-01/original.log "" "" ~/zhl_2016-04-01/results

// 2016-04-01 :
// [-438.359569763476,-43.084703677545626,17.85888167755565,4.82966339387984,7.293106804782623,0.0,4.316630366649318,5.778443227201751,21.920246911272702,7.070290879676206,-5.112153400304959,-12.332793804208317,-18.079878250662894,5.5324510333385115,-34.844625001862575,5.122666493001688,5.7162704391021775,15.989427457814452,7.219355844242403,-18.56429210681011,-16.537201446263904,25.4487752172613,3.6879819732023984,8.208612065962372,-2.5783124315748873,0.0,-0.07548857958657286,0.0,0.0]
// 2016-04-01 : Area under ROC = 0.9955735085228961
// 10kqps : total labeled records : 33594, 0 : 31427, 1 : 2167, %age -> 6.45%

// 2016-04-01 15th hr :
// [63.349054492747946,-50.5971491769977,13.167195462245251,3.733499422059894,4.732894575034823,0.0,1.8777478430455898,4.855727326104178,8.262254457947648,5.313254227145057,-2.7113798117089476,-8.473371960551379,-11.358280024096278,4.950465262920697,-8.06722093045548,-6.231034376470128,3.5221724296309023,8.950328744105942,0.23076594976405274,-5.3426876519173385,-12.24615664560761,10.73202534750319,1.7612632530746783,3.882895630789293,2.4840947612936395,0.0,-1.037607783263863,0.0,0.0]16/04/04
// 14:10:32 INFO YarnHistoryService: Discarding event
// Some(org.apache.hadoop.yarn.api.records.timeline.TimelineEvent@58094d6e)
// 2016-04-01 15th hr : Area under ROC = 0.996450017667657
// 10kqps : total labeled records : 33633, 0 : 31353, 1 : 2280, %age -> 6.78%

// 2016-03-28 :
// [4.55474269613318,5.871356479408493,-6.192429712208679,1.893829553014548,1.969765353180702,5.387445098131729,3.2928605284059573,3.0194085408489757,50.753839434951836,-0.7039027847484552,-0.5489039686789112,-2.0003371596240154,-32.126301563833486,4.462956413559165,-67.00772916311975,-2.4122508337255346,3.572276839161545,1.3237337300783425,-0.352619633436145,3.1707367830060766,-35.826944109329204,49.418581684642916,5.948657598152153,9.406762469486795,11.063830900284731,-0.3378225059317445,-0.006019310723188285,0.0,0.0]
// 2016-03-28 : Area under ROC = 0.9924835341294697
// !10kqps : total labeled records : 17595, 0 : 3783, 1 : 13812, %age -> 78.50%

// 2016-03-16:
// [2.9286537367967505,2.3968730188908323,-6.020540182002129,0.32576480676535635,3.623227798131008,4.244890967619318,2.545224136355098,3.1320827127020254,10.931174188137065,-1.7519227462786637,-0.2718667574005288,-9.934446215635537,-8.816101527793041,-0.9159572057377651,-6.71634551015315,1.870627313586418,5.1205963472646765,8.83606278806458,-2.8243418287801867,-14.040641273826678,-3.4038097984787092,10.369804284239805,-0.008532475793554739,-1.9135897370317814,13.537991866392913,0.3643845895345746,-0.05414448821551903,0.0,0.0]
// 2016-03-16: Area under ROC = 0.9947468661672662
// 10kqps : # modified.log : 620523,
// 10kqps : output : 1766471

// 16th march : 3rd (03am) :
// [3.1368622557753794,3.0690114489486273,-7.434240524422826,0.23249268409652424,4.728619491758473,4.696256788867953,1.480571708620736,3.363834920289862,10.716059077628609,-3.2271135896708136,0.26765091609058916,-14.912661733617044,-10.361323680879105,-0.9398092130419071,-8.08190725104087,1.1620939241824195,6.184380536115188,10.727909255306576,-5.239626258728045,-10.095780174779561,-6.091587687541411,14.731937288583941,-0.3283266223400648,-0.13462851781141277,14.975418223711157,1.019810096321167,-0.45865839695329497,0.0,0.0]
// Area under ROC = 0.9951456227426744
//

// cat result_2016-03-16_2ndhr_truthfile.txt | awk -F":" '{print $2}' | awk
// -F"->" '{ print ($1 == $2) ? "true" : "false" }'

// cat original.log | awk -F"\t" '{print $2}' | jq '.user | .id' | awk '{print
// tolower($1)}' > uid.log
// cat original.log | awk -F"\t" '{print $2}' | jq '.device | .ifa' | awk
// '{print tolower($1)}' > ifa.log

// expt 2 : remove city, region from buildkey phase
// expt 3 : remove all attributes (ts and ip 3 octets) from key formation

// /usr/hdp/2.4.0.0-169/spark/bin/spark-submit --class "ZhlFlatten" --master
// yarn --deploy-mode client target/zhlFlatten.jar
// /home/spark/zhl_2016-04-01_15thHr/modified.log
// /home/spark/zhl_2016-04-01_15thHr/original.log "" ""
// /home/spark/zhl_2016-04-01_15thHr/