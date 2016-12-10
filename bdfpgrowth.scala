import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.rdd.RDD
val data = sc.textFile("cluster_0.csv")
val transactions: RDD[Array[String]] = data.map(s => s.trim.split("  "))
val fpg = new FPGrowth().setMinSupport(0.1).setNumPartitions(10)
//Need to get unique records others FPG reports duplicate errors
val model = fpg.run(transactions)

model.freqItemsets.collect().foreach { itemset =>
  println(itemset.items.mkString("[", ",", "]") + ", " + itemset.freq)
}

val minConfidence = 0.8
model.generateAssociationRules(minConfidence).collect().foreach { rule =>
  println(
    rule.antecedent.mkString("[", ",", "]")
      + " => " + rule.consequent .mkString("[", ",", "]")
      + ", " + rule.confidence)
}
