import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.classification.SVMWithSGD;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.regression.LabeledPoint;
import scala.Tuple2;

import java.io.File;
import java.util.Arrays;


public class Linear_SVM_Q2 {

    public  JavaPairRDD<Object, Object> build_SVM_model(JavaSparkContext sparkContext)
    {
        /*
        References : https://spark.apache.org/docs/latest/mllib-linear-methods.html
                     https://spark.apache.org/docs/latest/mllib-evaluation-metrics.html
                     https://www.javadoc.io/doc/ml.combust.mleap/mleap-spark_2.11/0.3.0/org/apache/spark/ml/mleap/classification/SVMWithSGD.html
        */

        //relative path
        File textfile = new File("imdb_labelled.txt");

        // absolute path to our data file
        String path = textfile.getAbsolutePath();

        //reading the text file into a rdd
        JavaRDD<String> file = sparkContext.textFile(path);

        // Creating a HashingTF instance to map email text to vectors of 10000 features
        final HashingTF tf = new HashingTF(1000);

        /*
         each line in the file is split into words, and each word is mapped to one feature.
         Then creating LabeledPoint instances by parsing the label in token[1] of each tokenized line
        */
        JavaRDD<LabeledPoint> labelled_data = file.map(line -> {
            String[] tokens = line.split("\t");
            return new LabeledPoint(Integer.parseInt(tokens[1]), tf.transform(Arrays.asList(tokens[0].split(" "))));
        });

        // Splitting the initial RDD into two... [60% training data, 40% testing data].
        JavaRDD<LabeledPoint> training_set = labelled_data.sample(false, 0.6, 11L);
        // Caches data in memory since SVM is an iterative algorithm and needs the training data needs to be revisited many times
        training_set.cache();

        JavaRDD<LabeledPoint> test_set = labelled_data.subtract(training_set);
        //test_set.cache();

        /*
         Creating a Linear SVM with Stochastic Gradient descent
        The parameters are tuned after observing the model accuracy on different values
        */
        SVMModel svm_model = SVMWithSGD.train(training_set.rdd(), 1000, 1.5,  0.001);

        // Clearing the default threshold.
        svm_model.clearThreshold();

        // Computing raw scores on the test set.
        JavaPairRDD<Object, Object> scoreAndLabels = test_set.mapToPair(p ->
                new Tuple2<>(svm_model.predict(p.features()), p.label()));

        return scoreAndLabels;

    }
}
