package org.example;

public class Main {
    /**
     * Main method to run the WeatherStationProducer.
     * Arguments: station number, latitude and longitude.
     * station number: 1 -> 10.
     * latitude: -90 -> 90.
     * longitude: -180 -> 180.
     *
     * @param args command line arguments
     */
    public static void main(String[] args) {
        System.out.println(args[0] + " " + args[1] + " " + args[2]);
        WeatherStationProducer produce = new WeatherStationProducer(args[0], args[1], args[2]);
        produce.produce();
    }
}