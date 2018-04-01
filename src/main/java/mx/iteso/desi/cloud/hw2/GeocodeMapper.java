package mx.iteso.desi.cloud.hw2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import mx.iteso.desi.cloud.Geocode;
import mx.iteso.desi.cloud.GeocodeWritable;
import mx.iteso.desi.cloud.ParseTriple;
import mx.iteso.desi.cloud.ParserCoordinates;
import mx.iteso.desi.cloud.Triple;

public class GeocodeMapper extends Mapper<LongWritable, Text, Text, GeocodeWritable> {

  List<Geocode> cities;

  @Override
  protected void setup(Mapper<LongWritable, Text, Text, GeocodeWritable>.Context context)
      throws IOException, InterruptedException {
    cities = new ArrayList<>();
    cities.add(new Geocode("Philadelphia", 39.88, -75.25));
    cities.add(new Geocode("Houston", 29.97, -95.35));
    cities.add(new Geocode("Seattle", 47.45, -122.30));
    cities.add(new Geocode("Guadalajara", 20.6597, -103.3496));
    cities.add(new Geocode("Monterrey", 25.6866, 100.3161));
  }

  /* TODO: Your mapper code here */
  @Override
  protected void map(LongWritable key, Text value,
      Mapper<LongWritable, Text, Text, GeocodeWritable>.Context context)
      throws IOException, InterruptedException {
    String rawString = value.toString();
    Triple parsedTriple = ParseTriple.parseTriple(rawString);
    if (parsedTriple != null) {
      String relationType = parsedTriple.get(1);
      processTriple(context, parsedTriple, relationType);
    }
  }

  private void processTriple(Mapper<LongWritable, Text, Text, GeocodeWritable>.Context context,
      Triple parsedTriple, String relationType) throws IOException, InterruptedException {
    if (relationType.equals("http://xmlns.com/foaf/0.1/depiction")) {
      processImage(parsedTriple, context);
    } else if (relationType.equals("http://www.georss.org/georss/point")) {
      processGeocode(parsedTriple, context);
    }
  }

  private boolean checkDistanceToCities(Double latLong[]) {
    return cities.stream()
        .anyMatch(city -> city.getHaversineDistance(latLong[0], latLong[1]) < 5000);
  }

  private void processGeocode(Triple parsedTriple,
      Mapper<LongWritable, Text, Text, GeocodeWritable>.Context context)
      throws IOException, InterruptedException {
    Text article = new Text(parsedTriple.get(0));
    String rawCoordinates = parsedTriple.get(2);
    Double latLong[] = ParserCoordinates.parseCoordinates(rawCoordinates);

    if (checkDistanceToCities(latLong)) {
      Geocode geo = new Geocode("LatLong", latLong[0], latLong[1]);
      GeocodeWritable geocodeWritable = new GeocodeWritable(geo);
      context.write(article, geocodeWritable);
    }
  }

  private void processImage(Triple parsedTriple,
      Mapper<LongWritable, Text, Text, GeocodeWritable>.Context context)
      throws IOException, InterruptedException {
    Text article = new Text(parsedTriple.get(0));
    String imageURL = parsedTriple.get(2);
    Geocode fakeGeo = new Geocode(imageURL, 0, 0);
    GeocodeWritable fakeGeocodeWritable = new GeocodeWritable(fakeGeo);

    context.write(article, fakeGeocodeWritable);
  }
}
