import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Iterator;
import java.io.DataInput;
import java.io.DataOutput;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;





public class Statistics {


	public static class Data implements Writable{

		private IntWritable count;
		private FloatWritable sum;
		private FloatWritable sumSquare;
		private FloatWritable min;
		private FloatWritable max;
		private FloatWritable stdDev;

		public Data(){
			this.count = new IntWritable(0);
			this.sum = new FloatWritable(0.0f);
			this.sumSquare = new FloatWritable(0.0f);
			this.min = new FloatWritable(0.0f);
			this.max = new FloatWritable(0.0f);
			this.stdDev = new FloatWritable(0.0f);
		}

		public Data(int count, float sum, float sumSquare, float min, float max, float stdDev){
			this.count.set(count);
			this.sum.set(sum);
			this.sumSquare.set(sumSquare);
			this.min.set(min);
			this.max.set(max);
			this.stdDev.set(stdDev);
		}

		public void write(DataOutput dataOutput) throws IOException{
			count.write(dataOutput);
			sum.write(dataOutput);
			sumSquare.write(dataOutput);
			min.write(dataOutput);
			max.write(dataOutput);
			stdDev.write(dataOutput);
		}

		public void readFields(DataInput dataInput) throws IOException{
			count.readFields(dataInput);
			sum.readFields(dataInput);
			sumSquare.readFields(dataInput);
			min.readFields(dataInput);
			max.readFields(dataInput);
			stdDev.readFields(dataInput);
		}

		public int getCount(){
			return count.get();
		}

		public float getSum(){
			return sum.get();
		}

		public float getSumSquare(){
			return sumSquare.get();
		}

		public float getMin(){
			return min.get();
		}

		public float getMax(){
			return max.get();
		}

		public float getStdDev(){
			return stdDev.get();
		}

		public void setCount(int count){
			this.count = new IntWritable(count);
		}

		public void setMin(float min){
			this.min = new FloatWritable(min);
		}

		public void setStdDev(float stdDev){
			this.stdDev = new FloatWritable(stdDev);
		}

		public void setMax(float max){
			this.max = new FloatWritable(max);
		}

		public void setSum(float sum){
			this.sum = new FloatWritable(sum);
		}

		public void setSumSquare(float sumSquare){
			this.sumSquare = new FloatWritable(sumSquare);
		}

		public int hashCode(){
			return count.get();
		}

	}


  public static class Map 
            extends Mapper<LongWritable, Text, Text, Data>{

    private Text word = new Text();   // type of output key
      
    public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {
      System.out.println("Value: "+value);
	  System.out.println("Key: "+key);

	  StringTokenizer itr = new StringTokenizer(value.toString()); // line to string token
      int count = 0;
	  float sum = 0.0f;
	  float min = 0.0f;
	  float max = 0.0f;

      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());    // set word as each input keyword
		String numStr = word.toString();

		if (numStr.equals("-----")){
			return;
		}

		float num = Float.valueOf(numStr);

		//Now that we are done parsing let's start the calculations
		count++;
		sum += num;
		if (min == 0.0 || min > num){
			min = num;
		}
		if (max == 0.0 || max < num){
			max = num;
		}
	  }

	  float sumSquare = sum * sum;

	  //Debugging
	  System.out.println("Mapper count is: " + count);
	  System.out.println("Mapper sum is: " + sum);
	  System.out.println("Mapper min is: " + min);
	  System.out.println("Mapper max is: " + max);
	  System.out.println("Mapper sum squared is: " + sumSquare);

	  //Condense data into a single object for the key value store
	  Data curData = new Data();
	  curData.setCount(count);
	  curData.setMin(min);
	  curData.setMax(max);
	  curData.setSum(sum);
	  curData.setSumSquare(sumSquare);

	  System.out.println("Converting to float writable");
	  Text sumAdj = new Text(Float.toString(sum));

	  context.write(sumAdj, curData);
    }
  }
  
  public static class Reduce
       extends Reducer<FloatWritable,Data,String,FloatWritable> {

    private FloatWritable result = new FloatWritable();
	private float num = 0.0f;

    public void reduce(FloatWritable key, Iterable<Data> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      Float sum = 0.0f; // initialize the sum for each keyword
	  
	  for (Data val : values) {
        sum += val.getSum();  
      }
      /*result.set(sum);

	  //Parse text to float
	  String keyStr = key.toString();
	  num = Float.valueOf(keyStr);

	  //Add to the totals
	  totalSum += (num * sum);
	  total += sum;

	  //Compare current min and max and update
	  if (min == 0.0f || min > num){
		min = num;
	  }

	  if (max == 0.0f || max < num){
		max = num;
	  }*/

      context.write("Sum", result); // create a pair <keyword, number of occurences>
    }
  }

  /*public static class Combiner extends Reducer<Text,IntWritable,Text,IntWritable> {

  	//Override the recude function and combine values
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
		int sum = 0;

		//Iterator through all the values and add up instances of word
		Iterator<IntWritable> valuesIt = values.iterator();

		while (valuesIt.hasNext()){
			sum += valuesIt.next().get();
		}

		context.write(key, new IntWritable(sum));

	}

  }*/


  // Driver program
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration(); 
    conf.set("textinputformat.record.delimiter","-----");
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs(); // get all args
    if (otherArgs.length != 2) {
      System.err.println("Usage: Stats <in> <out>");
      System.exit(2);
    }

    // create a job with name "wordcount"
    Job job = new Job(conf, "stats");
    job.setInputFormatClass(TextInputFormat.class);
    job.setJarByClass(Statistics.class);
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);

    // Add a combiner here, not required to successfully run the wordcount program  
    //System.err.println("Including Combiner Class");
	//job.setCombinerClass(Combiner.class);
	// set output key type   
    job.setOutputKeyClass(Text.class);
    // set output value type
    job.setOutputValueClass(Data.class);
    //set the HDFS path of the input data
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    // set the HDFS path for the output
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));


    //Wait till job completion
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
