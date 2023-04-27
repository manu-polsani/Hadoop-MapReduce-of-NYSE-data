import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;



class NYSEDataMapper extends Mapper<LongWritable, Text, Text, NYSEDataWritable> {
    private Text outputKey = new Text();
    private NYSEDataWritable outputValue = new NYSEDataWritable();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Skip header row
        if (key.get() == 0) {
            return;
        }

        String[] fields = value.toString().split(",");
        String date = fields[0];
        double volume = Double.parseDouble(fields[6]);
        double price = Double.parseDouble(fields[5]);

        // Update fields in NYSEDataWritable object
        outputValue.updateFields(date,date, volume, volume, price);

        // Emit date as key and NYSEDataWritable object as value
        outputKey.set("NYSE");
        context.write(outputKey, outputValue);
    }
}
class NYSEDataReducer extends Reducer<Text, NYSEDataWritable, Text, Text> {
    private NYSEDataWritable result = new NYSEDataWritable();

    public void reduce(Text key, Iterable<NYSEDataWritable> values, Context context) throws IOException, InterruptedException {
        double maxStockPriceAdjClose = Double.MIN_VALUE;
        String maxStockVolumeDate = "";
        String minStockVolumeDate = "";
        
        for (NYSEDataWritable value : values) {
            maxStockPriceAdjClose = Math.max(maxStockPriceAdjClose, value.getMaxStockPriceAdjClose().get());
            result.updateFields(value.getMaxStockVolumeDate().toString().split(",")[0], value.getMinStockVolumeDate().toString().split(",")[0], Double.parseDouble(value.getMaxStockVolumeDate().toString().split(",")[1]), Double.parseDouble(value.getMinStockVolumeDate().toString().split(",")[1]), maxStockPriceAdjClose);
        }

        context.write(key, new Text("Max Stock Volume Date: " + result.getMaxStockVolumeDate().toString() + ", Min Stock Volume Date: " + result.getMinStockVolumeDate().toString() + ", Max Stock Price Adj Close: " + Double.toString(result.getMaxStockPriceAdjClose().get())));
    }
}



class NYSEDataWritable implements Writable {
    private Text maxStockVolumeDate;
    private Text minStockVolumeDate;
    private DoubleWritable maxStockPriceAdjClose;
    
    // Constructor
    public NYSEDataWritable() {
        this.maxStockVolumeDate = new Text();
        this.minStockVolumeDate = new Text();
        this.maxStockPriceAdjClose = new DoubleWritable();
    }
    
    // Getter and Setter methods for each field
    public void setMaxStockVolumeDate(String date) {
        this.maxStockVolumeDate.set(date);
    }
    
    public Text getMaxStockVolumeDate() {
        return this.maxStockVolumeDate;
    }
    
    public void setMinStockVolumeDate(String date) {
        this.minStockVolumeDate.set(date);
    }
    
    public Text getMinStockVolumeDate() {
        return this.minStockVolumeDate;
    }
    
    public void setMaxStockPriceAdjClose(double price) {
        this.maxStockPriceAdjClose.set(price);
    }
    
    public DoubleWritable getMaxStockPriceAdjClose() {
        return this.maxStockPriceAdjClose;
    }
    
    // Override methods from the Writable interface
    @Override
    public void write(DataOutput out) throws IOException {
        this.maxStockVolumeDate.write(out);
        this.minStockVolumeDate.write(out);
        this.maxStockPriceAdjClose.write(out);
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
        this.maxStockVolumeDate.readFields(in);
        this.minStockVolumeDate.readFields(in);
        this.maxStockPriceAdjClose.readFields(in);
    }
    
    // Helper method to update fields based on a record from the dataset
    public void updateFields(String max_date, String min_date, double max_volume, double min_volume, double price) {
        // Update max stock volume date if volume is greater than current value
        if (this.maxStockVolumeDate.getLength() == 0 || max_volume > Double.parseDouble(this.maxStockVolumeDate.toString().split(",")[1])) {
            this.setMaxStockVolumeDate(max_date + "," + Double.toString(max_volume));
        }
        
        // Update min stock volume date if volume is less than current value
        if (this.minStockVolumeDate.getLength() == 0 || min_volume < Double.parseDouble(this.minStockVolumeDate.toString().split(",")[1])) {
            this.setMinStockVolumeDate(min_date + "," + Double.toString(min_volume));
        }
        
        // Update max stock price adj close if price is greater than current value
        if (this.maxStockPriceAdjClose == null || price > this.maxStockPriceAdjClose.get()) {

            this.setMaxStockPriceAdjClose(price);
        }
    }
}
public class NYSEmapreducer {

    public static void main(String[] args) throws Exception {
      Configuration conf = new Configuration();
      String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
  
      if (otherArgs.length != 2) {
        System.err.println("Usage: NYSEDataDriver <input> <output>");
        System.exit(2);
      }
  
      Job job = Job.getInstance(conf, "NYSE Data Analysis");
      job.setJarByClass(NYSEmapreducer.class);
      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);
      job.setMapperClass(NYSEDataMapper.class);
      job.setReducerClass(NYSEDataReducer.class);
      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(NYSEDataWritable.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);
      TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
      TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
  
      System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
  }
