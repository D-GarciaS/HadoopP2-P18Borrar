package mx.iteso.desi.cloud;


public class ParseTriple {

    public static Triple parseTriple(String str) {
    try {
      int subjLAngle = 0;
      int subjRAngle = str.indexOf('>');
      int predLAngle = str.indexOf('<', subjRAngle + 1);
      int predRAngle = str.indexOf('>', predLAngle + 1);
      int objLAngle = str.indexOf('<', predRAngle + 1);
      int objRAngle = str.indexOf('>', objLAngle + 1);

      if (objLAngle == -1) {
	objLAngle = str.indexOf('\"', predRAngle + 1);
	objRAngle = str.indexOf('\"', objLAngle + 1); 
      }

      String subject = str.substring(subjLAngle + 1, subjRAngle);
      String predicate = str.substring(predLAngle + 1, predRAngle);
      String object = str.substring(objLAngle + 1, objRAngle);

      return new Triple(subject, predicate, object);
    } catch(Exception e) {
      return null;
    }
  }

}

