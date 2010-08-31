package com.tdunning.plume.local.lazy;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.util.GenericOptionsParser;

import com.google.common.collect.Lists;
import com.tdunning.plume.DoFn;
import com.tdunning.plume.EmitFn;
import com.tdunning.plume.Pair;
import com.tdunning.plume.types.IntegerType;
import com.tdunning.plume.types.PTableType;
import com.tdunning.plume.types.StringType;

/**
 * 
 * @author pere
 *
 */
public class TestMSCRToMapRed {

  /**
   * Wrapper class for multi-type shuffling
   * 
   * @author pere
   *
   */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  public static class PlumeObject implements WritableComparable<PlumeObject> {

    private WritableComparable obj;
    private long sourceId; // to identify its output channel

    public PlumeObject() {

    }

    public PlumeObject(WritableComparable obj, long sourceId) {
      this.obj = obj;
      this.sourceId = sourceId;
    }

    @Override
    public void readFields(DataInput arg0) throws IOException {
      String className = arg0.readUTF();
      try {
        obj = (WritableComparable) Class.forName(className).newInstance();
      } catch (InstantiationException e) {
        throw new IOException(e);
      } catch (IllegalAccessException e) {
        throw new IOException(e);
      } catch (ClassNotFoundException e) {
        throw new IOException(e);
      }
      obj.readFields(arg0);
      sourceId = arg0.readLong();
    }

    @Override
    public void write(DataOutput arg0) throws IOException {
      arg0.writeUTF(obj.getClass().getName());
      obj.write(arg0);
      arg0.writeLong(sourceId);
    }

    @Override
    public int compareTo(PlumeObject arg0) {
      if(arg0.obj.getClass().equals(obj.getClass())) {
        return obj.compareTo(arg0.obj);
      } else {
        return -1;
      }
    }
  }

  /**
   * The wordcount example to test with local hadoop
   * 
   * TODO
   * 
   * 1) Classes have to be WritableComparable
   * 2) There has to be a convenient way to point to a file
   * 3) Refactor MSCR in a maybe more convenient way
   */
  @SuppressWarnings("unchecked")
  public void test() {

    MSCR mscr = new MSCR();
    LazyPlume plume = new LazyPlume();

    DoFn wordCountMap = new DoFn<Pair<String, String>, Pair<String, Integer>>() {
      @Override
      public void process(Pair<String, String> v,
          EmitFn<Pair<String, Integer>> emitter) {
        emitter.emit(Pair.create(v.getValue(), 1));
      }
    };
    
    DoFn wordCountReduce = new DoFn<Pair<String, Iterable<Integer>>, Pair<String, Integer>>() {
      @Override
      public void process(Pair<String, Iterable<Integer>> v,
          EmitFn<Pair<String, Integer>> emitter) {
        int c = 0;
        Iterable<Integer> values = v.getValue();
        for(Integer i : values) {
          c += i;
        }
        emitter.emit(Pair.create(v.getKey(), c));
      }
    };
      
    LazyTable t = (LazyTable) plume.fromJava(Lists.newArrayList(Pair.create("", "foo"), Pair.create("", "bar")), 
        new PTableType(new StringType(), new StringType()));
    
    LazyTable result = (LazyTable) t.map(wordCountMap,
        new PTableType(new StringType(), new IntegerType()))
        .groupByKey()
        .map(wordCountReduce, new PTableType(new StringType(), new IntegerType()));
    
    // TODO
  }

  /**
   * This hadoop wordcount works with MultipleInputs and MultipleOutputs
   * It is a first example of how general multi-input/multi-processing/multi-output mapreds can be built
   * 
   * I use deprecated API because in 0.20 there's no ported MultipleInputs
   */
  @SuppressWarnings("deprecation")
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    JobConf job = new JobConf(conf, TestMSCRToMapRed.class);
    job.setJobName("Test");

    job.setMapOutputKeyClass(PlumeObject.class);
    job.setMapOutputValueClass(PlumeObject.class);

    job.setJarByClass(TestMSCRToMapRed.class);

    /**
     * One input
     */
    MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class,
        new Mapper<Object, Text, PlumeObject, PlumeObject> () {

      private final IntWritable one = new IntWritable(1);
      private Text word = new Text();

      @Override
      public void map(Object arg0, Text value,
          OutputCollector<PlumeObject, PlumeObject> arg2, Reporter arg3)
      throws IOException {
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
          word.set(itr.nextToken());
          arg2.collect(new PlumeObject(word, 1), new PlumeObject(one, 1));
        }
      }

      @Override
      public void configure(JobConf arg0) {

      }
      @Override
      public void close() throws IOException {

      }
    }.getClass());

    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    MultipleOutputs.addNamedOutput(job, "1", TextOutputFormat.class, Text.class, IntWritable.class);

    job.setReducerClass(new Reducer<PlumeObject, PlumeObject, NullWritable, NullWritable> () {

      MultipleOutputs mos;

      @Override
      public void configure(JobConf arg0) {
        mos = new MultipleOutputs(arg0);
      }

      @Override
      public void close() throws IOException {

      }

      @SuppressWarnings("unchecked")
      @Override
      public void reduce(PlumeObject arg0, Iterator<PlumeObject> values,
          OutputCollector<NullWritable, NullWritable> arg2, Reporter arg3)
      throws IOException {

        if(arg0.sourceId == 1) {
          int sum = 0;
          while(values.hasNext()) {
            PlumeObject value = values.next();
            IntWritable iW = (IntWritable)value.obj;
            sum += iW.get();
          }
          mos.getCollector("1", arg3).collect(arg0.obj, new IntWritable(sum));
        } else {
          System.err.println("Unknown source id: " + arg0.sourceId);
        }
      }
    }.getClass());

    JobClient.runJob(job);
  }
}
