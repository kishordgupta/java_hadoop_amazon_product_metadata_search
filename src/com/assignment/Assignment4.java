package com.assignment;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class Assignment4 {
public static class product{
	public	String id;
		public	int ind;
		public	int count;
		public	int reviews;
		public int ratings;
		public String categories="";
		
		
	}

	  public static class TokenizerMapper
	       extends Mapper<Object, Text, Text, Text>{

	    static String[] querycat;
	    static String[] catid;
	    public static ArrayList<product> quer = new ArrayList<Assignment4.product>();
	   
@Override
protected void setup(Mapper<Object, Text, Text, Text>.Context context)
		throws IOException, InterruptedException {
	// TODO Auto-generated method stub
	super.setup(context);
	querycat=context.getConfiguration().getStrings("Query");
	int k=0;
	for(String s :querycat)
	{
		String[] att = s.split(" ");
		product pr = new product();
		pr.categories=att[0];
		pr.ind=k;
		k++;
		pr.count=Integer.parseInt(att[1]);
		pr.reviews=Integer.parseInt(att[2]);
		pr.ratings=Integer.parseInt(att[3]);
		quer.add(pr);
	}
	
}
	    public void map(Object key, Text value, Context context
	                    ) throws IOException, InterruptedException {
	      StringTokenizer itr = new StringTokenizer(value.toString());
	      String data= itr.nextToken();
	      while (itr.hasMoreTokens()) {
	    	  String cat =itr.nextToken().toString();
	    	  for(product p: quer)
	    	  {
	    		  if(p.categories.equals(cat))
	    		  {
	    			  context.write(new Text(p.ind+""), new Text(data+","+p.count));
	    			 
	    		  }
	    	  }
	     
	      }
	  
	    }
	  }

	  public static class IntSumReducer
	       extends Reducer<Text,Text,Text,Text> {
		  static String[] querycat;
		    static String[] catid;
		    public static ArrayList<product> quer = new ArrayList<Assignment4.product>();
		   
		    @Override
		    protected void setup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
		    	// TODO Auto-generated method stub
		    	super.setup(context);
		    	querycat=context.getConfiguration().getStrings("Query");
		    	int k=0;
		    	for(String s :querycat)
		    	{
		    		String[] att = s.split(" ");
		    		
		    		product pr = new product();
		    		
		    		pr.id=att[0];
		    		pr.ind=k;
		    		k++;
		    		pr.count=Integer.parseInt(att[1]);
		    		pr.reviews=Integer.parseInt(att[2]);
		    		pr.ratings=Integer.parseInt(att[3]);
		    		quer.add(pr);
		    	}
		    }
		    
	    public void reduce(Text key, Iterable<Text> values,
	                       Context context
	                       ) throws IOException, InterruptedException {
	    	ArrayList<String> result = new ArrayList<String>();
	    	String ss="";;
			   if(!key.toString().contains("Recomend")) {
	    	product matcp = null ;
	    	for(product a : quer)
	    	{
	    		if(a.ind==Integer.parseInt(key.toString()))
	    			{matcp=a;
	    			break;
	    			}
	    	}
	    	for (Text text : values) {
	    		String[] line = text.toString().split("\n");
	    		for(String l : line) {
	    		String[] att = l.split(",");
	    		if(att.length>2)
	    		{
	    		product pr = new product();
	    		pr.id=att[0];
	    		//pr.count=Integer.parseInt(att[3]);
	    		pr.reviews=Integer.parseInt(att[1]);
	    		pr.ratings=Integer.parseInt(att[2]);
	    	//	if(pr.reviews>=matcp.reviews && pr.ratings>=matcp.reviews)
	    		{
	    			result.add(pr.id);
	    		}
	    		}
	    		}
			}
	    	int count=0;
	    	 ss="";
	    	for (String string : result) {
				ss=ss+ "\n"+string;
				if(count>matcp.count) {
					 context.write(new Text("Recomend query id"+key.toString()), new Text(ss));
					 
					break;
				}
				count++;
			}   if(count<matcp.count)context.write(new Text(key.toString()), new Text(ss));
			 
			   }
			   else
			   {for (Text text : values) {
				   ss=ss+text.toString()+" ";
			   }
				   context.write(key,new Text(ss));
			   }
	   }

	  }

	  public static void main(String[] args) throws Exception {
		  Configuration conf = new Configuration();
		  ArrayList<String> quer = new ArrayList<String>(); 
		
		    ArrayList<String> result = new ArrayList<String>();
		    Path Dtest = new Path("/mapreduce/assignment4/query/query");
		    Path output = new Path("\\output\\assignment4\\");
		    Path imput = new Path("/mapreduce/assignment4/data/");
		    FileSystem hdfs = Dtest.getFileSystem(conf);
			FSDataInputStream inputpathstreamsecmatrix = hdfs.open(Dtest);
			InputStreamReader isra = new InputStreamReader(inputpathstreamsecmatrix, "ISO-8859-1");	
			BufferedReader dbr = new BufferedReader(isra);
			Scanner scanner1 = new Scanner(dbr);
			while(scanner1.hasNext())
			{
				quer.add(scanner1.nextLine());
			}
			
			  if (hdfs.exists(output))
					hdfs.delete(output, true);
		
			scanner1.close();
			dbr.close();
			isra.close();
			inputpathstreamsecmatrix.close();
			System.gc();
			
			String[] query = new String[quer.size()];
			int conte=0;
			for(String a: quer)
			{
				query[conte]=a;conte++;
			}
			conf.setStrings("Query", query);
		    
		    /////////
		    
		    
		    
		    
		    
		    
		    
		    
		    
		    
		    
		    
		    
		    
		 
	    Job job = Job.getInstance(conf, "Amazon Product search");
	    job.setJarByClass(Assignment4.class);
	    job.setMapperClass(TokenizerMapper.class);
	    job.setCombinerClass(IntSumReducer.class);
	    job.setReducerClass(IntSumReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, imput);
	    FileOutputFormat.setOutputPath(job, output);
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
	}
