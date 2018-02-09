import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MSDReducer extends Reducer<IntWritable,Text, IntWritable, Text> {

  public MSDReducer(){
  }

  @Override
  public void reduce(IntWritable key, Iterable<Text> values,Context context) throws IOException,InterruptedException {

    String outputText = "None found for this year";
    float val = 0.0f;
    System.out.println(outputText);

    for (Text value : values) {
      String strOutput = value.toString();
      String[]  strArr = strOutput.split("\t"); 
      int comp = Float.compare(Float.parseFloat(strArr[0]),val);

      if(comp > 0){
         val = Float.parseFloat(strArr[0]);
         outputText = strOutput;   
      }
    }
    context.write(key, new Text(outputText));
  }
}
