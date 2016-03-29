import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import scala.Tuple2;
import org.apache.spark.api.java.function.*;

import java.util.ArrayList;
import java.util.List;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import scala.Tuple2;

import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS;
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.spark.SparkContext;
/***
 * 
 * @author aastha
 * TO DO
 * get uniques in joined data 'data', get them by combinebykey() and not distinct()
 * change prints to loggers
 * remove clines and scores
 * organize imports
 */
public class ZhlFlatten {
	static Logger logger = Logger.getLogger(ZhlFlatten.class.getName());  

  public static void main(String[] args) {
	if (args.length < 5) {
	      System.out.println("1 - modified path, 2 - original path, 3 o/p path modifed, 4 o/p path original");
	      System.exit(-1);
	}
    String logMFile = args[0]; // Should be some file on your system
    String logOFile = args[1]; // Should be some file on your system
    SparkConf conf = new SparkConf().setAppName("ZHL").set("spark.rdd.compress", "true");
    JavaSparkContext sc = new JavaSparkContext(conf);

    JavaPairRDD<KeyClass, ArrayList<ModifiedRow>> mLines = sc.textFile(logMFile, 300).mapToPair(new GetModifiedRow()).combineByKey(new InitMList(), new AddInMPart(), new AddAccMPart());
    JavaPairRDD<KeyClass, ArrayList<CommonRow>> oLines = sc.textFile(logOFile, 600).filter(new Function<String, Boolean>() {
    	  @Override
    	  public Boolean call(String s) throws Exception{
    		    if((!(s.contains("displaymanager")) || s.contains("\"displaymanager\":\"mopub\"")) 
    		    		&& (s.contains("\"carrier\":\"T-Mobile\"") || s.contains("\"carrier\":\"T-Mobile USA\"") ||
    		    				s.contains("\"carrier\":\"310-260\"")))
    		    	return true;
    		    return false;
    		  }
    		}).mapToPair(new GetCommonRow()).combineByKey(new InitList(), new AddInPart(), new AddAccPart());
    

//    JavaPairRDD<ModifiedRow, ArrayList<Tuple2<CommonRow, Float>>> cLines = mLines
//    		.join(oLines).values().distinct().flatMapToPair(new GetCombinedValues()).cache();
//    
//    JavaPairRDD<ModifiedRow, Tuple2<CommonRow, Integer>> scored = mLines
//    		.join(oLines).values().distinct().flatMapToPair(new GetCombinedScores()).cache();
    
    JavaRDD<LabeledPoint> data = mLines
    		.join(oLines).values().distinct().flatMap(new GetVectorScores()).cache();
    
//    //mLines.saveAsTextFile(args[2]);
//    //oLines.saveAsTextFile(args[3]);
//    cLines.saveAsTextFile(args[2]);
//    scored.saveAsTextFile(args[3]);
    data.saveAsTextFile(args[4]);
//    System.out.println(mLines.count());
//    System.out.println(oLines.count());
//    System.out.println(cLines.count());
//    System.out.println(scored.count());
    System.out.println(data.count());
    JavaRDD<LabeledPoint>[] splits = data.randomSplit(new double[] {0.6, 0.4}, 11L);
    JavaRDD<LabeledPoint> training = splits[0].cache();
    List<LabeledPoint> results = training.take(10);
	for(LabeledPoint lp :  results){
		System.out.println(lp.toString());
	}
    JavaRDD<LabeledPoint> test = splits[1];
    results = test.take(10);
    for(LabeledPoint lp :  results){
		System.out.println(lp.toString());
	}
    final LogisticRegressionModel model = new LogisticRegressionWithLBFGS()
    	      .setNumClasses(2)
    	      .run(training.rdd());
//    model.clearThreshold();
 // Compute raw scores on the test set.
    JavaRDD<Tuple2<Object, Object>> scoreAndLabels = test.map(
      new Function<LabeledPoint, Tuple2<Object, Object>>() {
        public Tuple2<Object, Object> call(LabeledPoint p) {
          Double score = model.predict(p.features());
          return new Tuple2<Object, Object>(score, p.label());
        }
      }
    );

    // Get evaluation metrics.
    BinaryClassificationMetrics metrics =
      new BinaryClassificationMetrics(JavaRDD.toRDD(scoreAndLabels));
    double auROC = metrics.areaUnderROC();
    System.out.println(model.weights());
    System.out.println("Area under ROC = " + auROC);
    sc.stop();
  }
}
//to run
//mvn package
//zip -d target/zhlFlatten.jar  META-INF/*.RSA META-INF/*.DSA META-INF/*.SF
//remove the files given in command line arguments for saving
// ../spark-1.5.1-bin-hadoop2.6/bin/spark-submit --class "ZhlFlatten" --master local[8] /Users/aastha/zHLexperiments/codes/spark_codes/zhl/target/zhlFlatten.jar  ~/zHLexperiments/data/2016-03-16-08-15-01.UTC.modified_bid_request.staging-aws-va-1_451aa.log ~/zHLexperiments/data/2016-03-16-08-15-01.UTC.original_bid_request.staging-aws-va-1_187c0.log  ~/combi ~/score ~/data