import org.apache.hadoop.io.Text;

public class MSDRecordParser {
private static final int MISSING_YEAR = 0;
private int year;
private String artistName;
private float hotness;
public void parse(String record) {

String str[] = record.split("\t");
year = Integer.parseInt(str[53]);
artistName = str[12];

try{
hotness = Float.parseFloat(str[43]);
}catch(Exception e){
hotness = 0.0f;
}

}

public void parse(Text record) {
	parse(record.toString());
}

public int getYear() {
	return year;
}

public String getArtistName(){
       return artistName;
}

public float getHotness(){
      return hotness;
}

public boolean isValidYear() {
	return year!= MISSING_YEAR;
}

}
