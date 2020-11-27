import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;


public class caller_Q2 {

    public static void main(String args[])
    {

        System.setProperty("hadoop.home.dir", "C:/winutils");
        SparkConf sparkConf = new SparkConf()
                .setAppName("LSDA_Assignment3_Q2") //setting appname to uniquely recognise job in a cluster(isn't much relevant to our task here)
                .setMaster("local[4]").set("spark.executor.memory", "1g"); //4 core processor to work individually with 1 gigabyte of heap memory

        //creating JavaSparkContext object to start the spark session
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        Linear_SVM_Q2 s1=new Linear_SVM_Q2();
        JavaPairRDD<Object, Object>  scoreAndLabels=s1.build_SVM_model(sparkContext);

        // Get evaluation metrics.
        BinaryClassificationMetrics metrics = new BinaryClassificationMetrics(scoreAndLabels.rdd());

        // AUPRC - Area under precision-recall curve
        System.out.println("\nArea under precision-recall curve = " + metrics.areaUnderPR() + " (" + metrics.areaUnderPR()*100 + " %)\n");

        // AUROC - Area under ROC
        System.out.println("\nArea under ROC = " + metrics.areaUnderROC() + " (" + metrics.areaUnderROC()*100 + " %)\n");

        //The take(n) function fetches the first n elements of the dataset
        scoreAndLabels.take(10).forEach(x -> {
            System.out.println("\nScore:"+x._1()+" Label:"+x._2());
        });

        //ending the spark session
        sparkContext.stop();
        sparkContext.close();
    }
}



