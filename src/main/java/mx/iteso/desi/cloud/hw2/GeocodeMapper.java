package mx.iteso.desi.cloud.hw2;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
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
    URI[] cacheFiles = context.getCacheFiles();
    if (cacheFiles != null) {
      cities = Files.readAllLines(Paths.get(".", cacheFiles[0].toString())).stream()
          .map(ParseTriple::parseTriple).map(t -> t.getObject())
          .map(ParserCoordinates::parseCoordinates).map(t -> new Geocode("", t[0], t[1]))
          .collect(Collectors.toList());
    }
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
