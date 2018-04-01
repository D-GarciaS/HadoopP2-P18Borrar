package mx.iteso.desi.cloud.hw2;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import mx.iteso.desi.cloud.Geocode;
import mx.iteso.desi.cloud.GeocodeWritable;

public class GeocodeReducer extends Reducer<Text, GeocodeWritable, Text, Text> {

  /* TODO: Your reducer code here */
  public void reduce(Text key, Iterable<GeocodeWritable> values, Context context)
      throws java.io.IOException, InterruptedException {

    Geocode trueGeocode = null;

    trueGeocode = StreamSupport.stream(values.spliterator(), false)
        .filter(v -> v.getName().toString().equals("LatLong")).map(v -> v.getGeocode()).findAny()
        .orElse(null);

    if (trueGeocode != null) {
      Text newGeoKey = new Text(trueGeocode.formatParenthesis());
      String article = key.toString();
      List<Text> articlesAndImagesValues = StreamSupport.stream(values.spliterator(), false)
          .filter(v -> !v.getName().toString().equals("LatLong"))
          .map(fake -> new Text(article + "\t" + fake.getName())).collect(Collectors.toList());

      for (Text articleAndImageValue : articlesAndImagesValues) {
        context.write(newGeoKey, articleAndImageValue);
      }
    }
  }
}
