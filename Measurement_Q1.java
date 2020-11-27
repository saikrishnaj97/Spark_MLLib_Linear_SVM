import scala.Serializable;

public class Measurement_Q1 implements Serializable {

    int time;
    double temperature;
    private static final long serialVersionUID = 1L;
    //Constructor of Measurement class for initializing variables for the objects of class
    public Measurement_Q1(int time, double temperature)
    {
        this.time=time;
        this.temperature=temperature;
    }

    //Standard getters
    public int getTime(){
        return time;
    }

    public double getTemperature(){
        return temperature;
    }

}
