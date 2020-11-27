import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Serializable;
import scala.Tuple2;
import java.util.*;



public class WeatherStation_Q1 implements Serializable {
    String city;
    List<Measurement_Q1> measurements;
    public static List<WeatherStation_Q1> stations = new ArrayList<>();
    //Initializing serialVersionUID with default value
    private static final long serialVersionUID = 1L;

    //Constructor of WeatherStation class for initializing variables for the objects of class
    public WeatherStation_Q1(String city, List<Measurement_Q1> measurements) {
        this.city = city;
        this.measurements = measurements;
    }

    //Method to add a new station to the static list of stations
    public void addStation(WeatherStation_Q1 w) {
        stations.add(w);
    }

    //Standard Getters
    public List<Measurement_Q1> getMeasurements() {
        return measurements;
    }

    public String getCity() {
        return city;
    }

    public static Map<Double, Integer> countTemperature(double temp, JavaSparkContext sparkContext) {

        //Parallelizing the stations and storing all the weather stations in rdd
        JavaRDD<WeatherStation_Q1> stations_rdd = sparkContext.parallelize(stations);
        //getting all the measurements of all the stations into the rdd
        JavaRDD<Measurement_Q1> measurements_rdd = stations_rdd.flatMap(s -> s.getMeasurements().iterator());
        //filtering out temperature to be in the required interval i.e [temp-1,....,temp+1]
        JavaRDD<Measurement_Q1> filtered_rdd = measurements_rdd.filter(x -> ((temp - 1) <= x.getTemperature() && x.getTemperature() <= (temp + 1)));
        //map each filtered temperature to key-value pair
        JavaPairRDD<Double, Integer> mapped_rdd = filtered_rdd.mapToPair((Measurement_Q1 s) -> new Tuple2<Double, Integer>(temp, 1));
        //reduce the pair by key by adding the values
        JavaPairRDD<Double, Integer> reduced_rdd = mapped_rdd.reduceByKey((Integer a, Integer b) -> a + b);
        //collect as map
        Map<Double, Integer> result = reduced_rdd.collectAsMap();

        return result;
    }
}