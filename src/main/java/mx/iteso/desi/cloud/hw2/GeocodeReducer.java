package mx.iteso.desi.cloud.hw2;

import org.apache.hadoop.mapreduce.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.io.*;
import mx.iteso.desi.cloud.Geocode;
import mx.iteso.desi.cloud.GeocodeWritable;

public class GeocodeReducer extends Reducer<Text, GeocodeWritable, Text, Text> {

  /* TODO: Your reducer code here */
  @Override
  protected void reduce(Text key, Iterable<GeocodeWritable> values,
      Reducer<Text, GeocodeWritable, Text, Text>.Context context)
      throws IOException, InterruptedException {
    Geocode trueGeocode;
    List<Geocode> fakeGeocodes = new ArrayList<Geocode>();
    List<GeocodeWritable> allGeocodes = new ArrayList<GeocodeWritable>();
    values.forEach(allGeocodes::add);
    trueGeocode = allGeocodes.stream().filter(v -> v.getName().toString().equals("LatLong"))
        .findAny().get().getGeocode();
    fakeGeocodes = allGeocodes.stream().filter(v -> !v.getName().toString().equals("LatLong"))
        .map(GeocodeWritable::getGeocode).collect(Collectors.toList());

    if(trueGeocode != null){
      Text newGeoKey = new Text(trueGeocode.formatParenthesis());
      String article = key.toString();
      List<Text> articlesAndImagesValues = fakeGeocodes.stream().map( fake -> {
        return new Text(article + "\t" + fake.getName());
      }).collect(Collectors.toList());
      
      for(Text articleAndImageValue : articlesAndImagesValues){
        context.write(newGeoKey, articleAndImageValue);
      }
    }
  }
}
