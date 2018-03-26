package mx.iteso.desi.cloud;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * URLGeocodeWritable -- wrapper for serializing Geocode class
 *
 */
public class GeocodeWritable implements Writable {
	
  private Text name;
  private DoubleWritable lat;
  private DoubleWritable lon;

  public GeocodeWritable() {
    set(new Text(), new DoubleWritable(0), new DoubleWritable(0));
  }

  public GeocodeWritable(String name, double lat, double lon) {
    set (new Text(name), new DoubleWritable(lat), new DoubleWritable(lon));
  }

  public GeocodeWritable(Geocode g) {
    set (new Text(g.getName()), new DoubleWritable(g.getLatitude()), new DoubleWritable(g.getLongitude()));
  }

  public GeocodeWritable(Text name, DoubleWritable lat, DoubleWritable lon) {
    this.name = name;
    this.lat = lat;
    this.lon = lon;
  }

  public Geocode getGeocode() {
    return new Geocode(getName().toString(), getLatitude(), getLongitude());
  }

  public Text getName() {
    return name;
  }

  public double getLatitude() {
    return lat.get();
  }

  public double getLongitude() {
    return lon.get();
  }

  public void set(Text n, DoubleWritable la, DoubleWritable lo) {
    name = n;
    lat = la;
    lon = lo;
  }

  public void readFields(DataInput arg0) throws IOException {
    name.readFields(arg0);
    lat.readFields(arg0);
    lon.readFields(arg0);
  }

  public void write(DataOutput arg0) throws IOException {
    name.write(arg0);
    lat.write(arg0);
    lon.write(arg0);
  }

  @Override
  public int hashCode() {
    return name.hashCode() * 163 + (lat.hashCode() ^ lon.hashCode());
  }

  @Override
  public String toString() {
    return name + "\t(" + lat + "," + lon + ")";
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof GeocodeWritable))
      return false;
    
    GeocodeWritable u = (GeocodeWritable)o;
    return name.equals(u.name) && u.lat == lat && u.lon == lon;
  }
}
