import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MSDMapper
extends Mapper<LongWritable, Text, IntWritable, Text> {
private MSDRecordParser parser = new MSDRecordParser();

@Override
public void map(LongWritable key, Text value, Context context)
throws IOException, InterruptedException {
parser.parse(value);
if (parser.isValidYear()) {
System.out.println("commit2");
context.write(new IntWritable(parser.getYear()),
new Text(parser.getHotness()+""+"\t"+parser.getArtistName()));
}
}
}
