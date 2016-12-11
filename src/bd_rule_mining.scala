import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

//Total number of clusters
val max_clusters = 6
//Current cluster
var num_clusters = 0

for( num_clusters <- 0 to max_clusters-1){
	//Load the current cluster file from HDFS
	val data = sc.textFile("cluster_"+num_clusters+".csv")
	//Create an RDD[Array[String]]
	val transactions: RDD[Array[String]] = data.map(s => s.trim.split("  "))
	//Create a FPGrowth RDD
	val fpg = new FPGrowth().setMinSupport(0.3).setNumPartitions(10)
	//Need to get unique records others FPG reports duplicate errors
	//Run the Frequent Pattern Growth Mining Algorithm with minimum support of 0.3
	val model = fpg.run(transactions)

	//Print all frequent item sets
	model.freqItemsets.collect().foreach { itemset =>
	  println(itemset.items.mkString("[", ",", "]") + ", " + itemset.freq)
	}

	//Default Association rule minimum confidence = 80%
	var minConfidence = 0.8

	//For these clusters  we need to set lower confidence to get more rules
	if(num_clusters == 2 || num_clusters == 5) 
		minConfidence = 0.7

	//Run the Association Rule Mining Algorithm to detect all Association rules with the minimum Confidence
	model.generateAssociationRules(minConfidence).collect().foreach { rule =>
	  println(
	    rule.antecedent.mkString("[", ",", "]")
	      + " => " + rule.consequent .mkString("[", ",", "]")
	      + ", " + rule.confidence)
	}        
}
