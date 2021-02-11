package org.anasis;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class LinkPrediction {

    private static final Pattern SPACE = Pattern.compile("[ \\t\\x0B\\f\\r]+");
    
    public static JavaPairRDD<Tuple2<String, String>, Double> computeCommon (JavaRDD<String> lines) {
    	
    	JavaPairRDD<String, String> edges = lines.flatMapToPair(t -> {
        	List<Tuple2<String,String>> result = new ArrayList<>();
        	if(!t.contains("#")) {
        		String [] nodes = SPACE.split(t);
        		result.add(new Tuple2<>(nodes[0], nodes[1]));
        		result.add(new Tuple2<>(nodes[1], nodes[0]));
        	}
        	return result.iterator();
        }).distinct();
    	
    	JavaPairRDD<Tuple2<String, String>, Double> commonNeighborsSorted = edges.join(edges).flatMapToPair(t -> {
    		double neighbor = Double.parseDouble(t._1());
    		double source = Double.parseDouble(t._2()._1());
    		double target = Double.parseDouble(t._2()._2());
    		List<Tuple2<Tuple2<String,String>,Double>> result = new ArrayList<>();
    		if(source == target) {
    			if(neighbor<source) {
    				result.add(new Tuple2<>(new Tuple2<>(t._1(),t._2()._1()), Double.parseDouble("0")));
    			}
    		} else if (source<target) {
    			result.add(new Tuple2<>(new Tuple2<>(t._2()._1(),t._2()._2()), Double.parseDouble("1")));
    		}
    		return result.iterator();
    	}).groupByKey().mapValues(v -> {
    		int count=0;
    		for (Double item: v) {
    			if(item == 0) {
    				return Double.parseDouble("0");
    			}
    			count++;
    		}
    		return Double.parseDouble(Integer.toString(count));
    	}).filter(t -> {
    		return t._2!=0.0?true:false;
    	}).mapToPair(t -> {        	
        	return new Tuple2<>(t._2(),t._1());
        }).sortByKey(false).mapToPair(t -> {
        	return new Tuple2<>(t._2(),Double.parseDouble(t._1().toString()));
        });
    	
    	return commonNeighborsSorted;
    }
    
    public static JavaPairRDD<Tuple2<String, String>, Double> computeJaccard (JavaRDD<String> lines) {
    	
    	JavaPairRDD<String, String> edges = lines.flatMapToPair(t -> {
        	List<Tuple2<String,String>> result = new ArrayList<>();
        	if(!t.contains("#")) {
        		String [] nodes = SPACE.split(t);
        		result.add(new Tuple2<>(nodes[0], nodes[1]));
        		result.add(new Tuple2<>(nodes[1], nodes[0]));
        	}
        	return result.iterator();
        }).distinct();
    	
    	JavaPairRDD<Tuple2<String, String>, Double> intersection = edges.join(edges).flatMapToPair(t -> {
    		double neighbor = Double.parseDouble(t._1());
    		double source = Double.parseDouble(t._2()._1());
    		double target = Double.parseDouble(t._2()._2());
    		List<Tuple2<Tuple2<String,String>,Double>> result = new ArrayList<>();
    		if(source == target) {
    			if(neighbor<source) {
    				result.add(new Tuple2<>(new Tuple2<>(t._1(),t._2()._1()), Double.parseDouble("0")));
    			}
    		} else if (source<target) {
    			result.add(new Tuple2<>(new Tuple2<>(t._2()._1(),t._2()._2()), Double.parseDouble("1")));
    		}
    		return result.iterator();
    	}).groupByKey().mapValues(v -> {
    		int count=0;
    		for (Double item: v) {
    			if(item == 0) {
    				return Double.parseDouble("0");
    			}
    			count++;
    		}
    		return Double.parseDouble(Integer.toString(count));
    	}).filter(t -> {
    		return t._2!=0.0?true:false;
    	});
    	
    	JavaPairRDD<String, Double> adjacencyListCount =  edges.mapValues(v -> Double.parseDouble("1")).reduceByKey((a,b)->a+b);
    	
    	JavaPairRDD<Tuple2<String, String>, Double> jaccardSorted = intersection.mapToPair(t -> {
    		return new Tuple2<>(t._1()._1(), new Tuple2<>(t._1()._2(),t._2()));
    	}).join(adjacencyListCount).mapToPair(t -> {
    		return new Tuple2<>(t._2()._1()._1(), new Tuple2<>(new Tuple2<>(t._1(),t._2()._1()._2()),t._2()._2()));
    	}).join(adjacencyListCount).mapToPair(t -> {
    		String source = t._2()._1()._1()._1();
    		String target = t._1();
    		double inter = t._2()._1()._1()._2();
    		double sourceNeighbors = t._2()._1()._2();
    		double targetNeighbors = t._2()._2();
    		double sumNeighbors = sourceNeighbors + targetNeighbors;
    		return new Tuple2<>(new Tuple2<>(source,target),inter/(sumNeighbors-inter));
    	}).mapToPair(t -> {        	
        	return new Tuple2<>(t._2(),t._1());
        }).sortByKey(false).mapToPair(t -> {
        	return new Tuple2<>(t._2(),t._1());
        });
    	
    	
		return jaccardSorted;
    }
    
    public static JavaPairRDD<Tuple2<String, String>, Double> computeAdamicAdar (JavaRDD<String> lines) {
    	JavaPairRDD<String, String> edges = lines.flatMapToPair(t -> {
        	List<Tuple2<String,String>> result = new ArrayList<>();
        	if(!t.contains("#")) {
        		String [] nodes = SPACE.split(t);
        		result.add(new Tuple2<>(nodes[0], nodes[1]));
        		result.add(new Tuple2<>(nodes[1], nodes[0]));
        	}
        	return result.iterator();
        }).distinct();
    	
    	JavaPairRDD<String, Double> logarithms = edges.mapValues(v -> Long.parseLong("1") ).reduceByKey((a,b) -> a+b)
    			.mapValues(v -> {
    				if (v == 1) {
    					return Double.parseDouble("-1");
    				}
    				return 1/Math.log10(v.doubleValue());
    			});
    	
    	JavaPairRDD<Tuple2<String, String>, Double> adamicadarSorted = edges.join(edges).join(logarithms).flatMapToPair(t -> {
    		long neighbor = Long.parseLong(t._1());
    		long source = Long.parseLong(t._2()._1()._1());
    		long target = Long.parseLong(t._2()._1()._2());
    		List<Tuple2<Tuple2<String,String>, Double>> result = new ArrayList<>();
    		if(source == target) {
    			if(neighbor<source) {
    				result.add(new Tuple2<>(new Tuple2<>(t._1(),t._2()._1()._1()), Double.parseDouble("-1")));
    			}
    		} else if (source<target) {
    			result.add(new Tuple2<>(new Tuple2<>(t._2()._1()._1(),t._2()._1()._2()), t._2()._2()));
    		}
    		return result.iterator();
    	}).groupByKey().mapValues(v -> {
    		double sum=0.0;
    		for (Double item: v) {
    			if(item == -1.0) {
    				return Double.parseDouble("-1");
    			}
    			sum+=item;
    		}
    		return Double.parseDouble(Double.toString(sum));
    	}).filter(t -> {
    		return t._2>=0.0?true:false;
    	}).mapToPair(t -> {
    		return new Tuple2<>(t._2(),t._1());
        }).sortByKey(false).mapToPair(t -> {
        	return new Tuple2<>(t._2(),t._1());
        });
    	
    	return adamicadarSorted;
    }
    
    public static void main(String[] args) throws Exception {

        if (args.length != 4) {
        	System.err.println("WRONG: NUMBER OF ARGUMENTS");
            System.err.println("Usage: LinkPrediction <inputpath> <outputpath> <top-k> <method>");
            System.err.println("method:\tString Argument");
            System.err.println("common neighbors:\t\"common\"");
            System.err.println("Jaccard coeﬃcient:\t\"jaccard\"");
            System.err.println("Adamic Adar:\t\t\"adamicadar\"");
            System.exit(1);
        }
        if (!args[3].equals("common")&&!args[3].equals("jaccard")&&!args[3].equals("adamicadar")) {
        	System.err.println("WRONG: NAME OF METHOD");
            System.err.println("Usage: LinkPrediction <inputpath> <outputpath> <top-k> <method>");
            System.err.println("method:\tString Argument");
            System.err.println("common neighbors:\t\"common\"");
            System.err.println("Jaccard coeﬃcient:\t\"jaccard\"");
            System.err.println("Adamic Adar:\t\t\"adamicadar\"");
            System.exit(1);
        }
        try {
        	if(Integer.parseInt(args[2])<0) {
        		System.err.println("WRONG: NEGATIVE NUMBER OF K");
        		System.err.println("Usage: LinkPrediction <inputpath> <outputpath> <top-k> <method>");
                System.err.println("top-k inappropriate value. Give an appropriate k: k>0 or k=0 to get all the records");
                System.exit(1);
        	}
        } catch (NumberFormatException e){
        	System.err.println("WRONG: K IS NOT A NUMBER");
        	System.err.println("Usage: LinkPrediction <inputpath> <outputpath> <top-k> <method>");
            System.err.println("top-k inappropriate value. Give an appropriate k: k>0 or k=0 to get all the records");
            System.exit(1);
        }

        SparkConf sparkConf = new SparkConf().setAppName("LinkPrediction");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        
        JavaRDD<String> lines = sc.textFile(args[0]);
        if (Integer.parseInt(args[2])>0) {
        	if (args[3].equals("common")) {
           	 //sc.parallelizePairs(computeScore(lines,"common").take(Integer.parseInt(args[2]))).saveAsTextFile(args[1]);
           	computeCommon(lines).zipWithUniqueId().filter(t->{
           		return t._2()<Integer.parseInt(args[2])?true:false;
           	}).saveAsTextFile(args[1]);
           } else if (args[3].equals("jaccard")) {
           	computeJaccard(lines).zipWithUniqueId().filter(t->{
           		return t._2()<Integer.parseInt(args[2])?true:false;
           	}).saveAsTextFile(args[1]);
           } else if (args[3].equals("adamicadar")) {
           	computeAdamicAdar(lines).zipWithUniqueId().filter(t->{
           		return t._2()<Integer.parseInt(args[2])?true:false;
           	}).saveAsTextFile(args[1]);
           }
        } else {
        	if (args[3].equals("common")) {
            	computeCommon(lines).saveAsTextFile(args[1]);
            } else if (args[3].equals("jaccard")) {
            	computeJaccard(lines).saveAsTextFile(args[1]);
            } else if (args[3].equals("adamicadar")) {
            	computeAdamicAdar(lines).saveAsTextFile(args[1]);
            }
        }
        

        sc.stop();
    }
}
