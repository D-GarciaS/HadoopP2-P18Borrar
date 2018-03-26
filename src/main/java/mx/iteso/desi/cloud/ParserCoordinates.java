package mx.iteso.desi.cloud;

public class ParserCoordinates {
  public static Double[] parseCoordinates(String raw) {
    int space = raw.indexOf(' ');
    String latStr = raw.substring(0, space);
    String lonStr = raw.substring(space + 1);
    Double lat = Double.parseDouble(latStr);
    Double lon = Double.parseDouble(lonStr);
    if (lat == null || lon == null)
      return null;
 
    return new Double[] {lat, lon};
  }    
}
