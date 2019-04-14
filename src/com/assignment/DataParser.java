package com.assignment;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Scanner;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.wordcount.WordCount.IntSumReducer;
import com.wordcount.WordCount.TokenizerMapper;

public class DataParser {
	public static class product{
		
		public	String id;
		public	String reviews;
		public String ratings;
		public String categories="";
		public String stringdat() {
			// TODO Auto-generated method stub
			
			String s=id+","+reviews+","+ratings+","+categories+"\n";
			return s;
		}
		
	}

	public static void main(String[] args) throws Exception {
		ArrayList<product> Productz = new ArrayList<product>();System.out.print(" hhh");
		File datafile = new File("C:\\Users\\kishor\\Desktop\\WordCount\\input\\amazon-meta.txt");
		Scanner in = new Scanner(datafile);
		System.out.print(" hhh");
		 File file = new File("C:\\Users\\kishor\\Desktop\\WordCount\\input\\dataparsed.txt");
	     FileWriter filew = new FileWriter(file);
		
	     product p=null;;
	     String s =in.next();
		while(in.hasNext())
		{// String datas = in.nextLine();
		
		  if(s.contains("Id:"))
		  {
			  if(p!=null)
			  { Productz.add(p);
			  filew.write(p.stringdat());
			  p=null;}
			 // else
			  {
			  p = new product();
			  p.id=in.next();
			  }
		  }
		  else if(s.contains("categories:"))
		  {
			//  p = new product();
			  int k=in.nextInt();
			  for(int i=0;i<k;i++)
			  {
				  String cs = in.nextLine();
				  cs = cs.replaceAll("[^-?0-9]+", " "); 
				   p.categories=p.categories+" "+cs;
			  }
			  
		  }
		  else if(s.contains("total:"))
		  {
			  p.reviews=in.next();
		  }
		  else if(s.contains("rating:"))
		  {
			  p.ratings=in.next();
		  }
		// else
		  {   s = in.next();
		  }
		}
		
		in.close();
	}		
		

}

