import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import java.util.ArrayList;
import java.io.BufferedReader;
import java.io.InputStreamReader;

public class GenomeAnalysisMapper extends MapReduceBase
    implements Mapper<LongWritable, Text, Text, IntWritable> {

  private String gene_file;
  private ArrayList<Integer> id = new ArrayList<Integer>();
  private ArrayList<Integer> chromosome = new ArrayList<Integer>();
  private ArrayList<String> strand = new ArrayList<String>();
  private ArrayList<Integer> start = new ArrayList<Integer>();
  private ArrayList<Integer> end = new ArrayList<Integer>();
  private int gene_num = 0;  

  // set up task, so gene file only read once
  public void configure(JobConf job) {

    super.configure(job);
    gene_file = job.get("genePath");
    try {
      Path pt = new Path(gene_file);
      FileSystem fs = FileSystem.get(new Configuration());
      BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
      String line;
      String[] parts;
      line = br.readLine();
      while (line != null) {
        gene_num = gene_num + 1;
        line = line.trim();
        parts = line.split("\t");
        id.add(new Integer(parts[0]));
        chromosome.add(new Integer(parts[1]));
        strand.add(parts[2]);
        start.add(new Integer(parts[3]));
        end.add(new Integer(parts[4]));
        line = br.readLine();
      }
    } catch (Exception e) {
      System.err.println("Can't read gene file.");
    }
  }

  private int indexOf(int frag_chr) {

    for (int i = 0; i < chromosome.size(); i++) {
      int chr = chromosome.get(i).intValue();
      if (frag_chr == chr) {
        return i;
      }
    }
    return -1;
  }

  private int lastIndexOf(int frag_chr, int first_index) {

    if (-1 == first_index) {
      return -1;
    } else {
      for (int i = first_index; i < chromosome.size(); i++) {
        int chr = chromosome.get(i).intValue();
        if (frag_chr != chr) {
          return i - 1;
        }
      }
      return chromosome.size() - 1;
    }
  }

  private int frag2gene(int frag_chr, int frag_str, int frag_end) {

    int first_index = indexOf(frag_chr);
    int last_index = lastIndexOf(frag_chr, first_index);
    int gene = -1;
    int distance = Integer.MAX_VALUE;

    if (-1 == first_index) {
      return gene;
    } else {
      for (int i = first_index; i <= last_index; i++) {
        int gene_start = start.get(i).intValue();
        int gene_end = end.get(i).intValue();
        String gene_strand = strand.get(i);
        if (!(frag_str >= gene_end || frag_end <= gene_start)) {
          int frag_middle = (frag_str + frag_end) / 2;
          int tss;
          if (gene_strand.equals("+")) {
            tss = gene_start;
          } else {
            tss = gene_end;
          }
          
          int frag2tss = Math.abs(frag_middle - tss);
          if (frag2tss < distance) {
            gene = id.get(i).intValue();
            distance = frag2tss;
          }
        }
      }
      return gene;
    }
  }

  public void map(LongWritable key, Text value,
      OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {

    String line = value.toString();
    line = line.trim();
    String[] parts = line.split("\t");
    int frag1_chr = Integer.parseInt(parts[0]);
    int frag1_str = Integer.parseInt(parts[1]);
    int frag1_end = Integer.parseInt(parts[2]);
    int frag2_chr = Integer.parseInt(parts[3]);
    int frag2_str = Integer.parseInt(parts[4]);
    int frag2_end = Integer.parseInt(parts[5]);
    int gene1 = frag2gene(frag1_chr, frag1_str, frag1_end);
    int gene2 = frag2gene(frag2_chr, frag2_str, frag2_end);
    if (-1 != gene1 && -1 != gene2) {
      String genes = Integer.toString(gene1) + ":" + Integer.toString(gene2);
      Text gene_pair = new Text();
      gene_pair.set(genes);
      IntWritable count = new IntWritable(Integer.parseInt(parts[6]));
      output.collect(gene_pair, count);
    }
  }
}
