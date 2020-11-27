import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Serializable;

public class caller_Q1 implements Serializable {

    private static final long serialVersionUID = 1L;
    public static void main(String args[])
    {

        System.setProperty("hadoop.home.dir", "C:/winutils");
        SparkConf sparkConf = new SparkConf()
                .setAppName("LSDA_Assignment3_Q1") //setting appname to uniquely recognise job in a cluster(isn't much relevant to our task here)
                .setMaster("local[4]").set("spark.executor.memory", "1g"); //4 core processor to work individually with 1 gigabyte of heap memory

        ///creating JavaSparkContext object to start the spark session
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        //creating Measurement class objects
        Measurement_Q1 a = new Measurement_Q1(1, 22.0);
        Measurement_Q1 b = new Measurement_Q1(13, 8.1);
        Measurement_Q1 c = new Measurement_Q1(24, 12.5);
        Measurement_Q1 d = new Measurement_Q1(3, 23.6);
        Measurement_Q1 e = new Measurement_Q1(11, 13.8);
        Measurement_Q1 f = new Measurement_Q1(23, 19.5);
        Measurement_Q1 g = new Measurement_Q1(16,18);

        //creating list of measurements to pass to WeatherStation objects
        List<Measurement_Q1> measurements1 = Arrays.asList(a,b,c,d);
        List<Measurement_Q1> measurements2 = Arrays.asList(e,f,g);

        //creating WeatherStation class objects
        WeatherStation_Q1 s1 = new WeatherStation_Q1("Galway", measurements1);
        WeatherStation_Q1 s2 = new WeatherStation_Q1("Cork", measurements2);

        //Adding weather stations to the static list of stations
        s1.addStation(s1);
        s2.addStation(s2);

        double temp=19.0;

        //Collecting the returned result into a map (also notice that the sparkContext is passed to the countTemperature() method from here as a parameter)
        Map<Double, Integer> result = WeatherStation_Q1.countTemperature(temp,sparkContext);
        for(Entry<Double, Integer> entry : result.entrySet()) {
            System.out.println("\nThe temperature " + entry.getKey() + " was measured " + entry.getValue() + " times so far across all the weather stations");
        }

        //ending the spark session
        sparkContext.stop();
        sparkContext.close();
    }
}



