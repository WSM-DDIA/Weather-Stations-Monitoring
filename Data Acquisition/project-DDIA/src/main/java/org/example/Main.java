package org.example;

public class Main {
    public static void main(String[] args) {
        // arg[0] ==> 1 -> 10
        // arg[1] ==> -90 -> 90
        // arg[2] ==> -180 -> 180

        WeatherStationProducer produce = new WeatherStationProducer("1","50.6","63.7");
        produce.produce();
    }
}