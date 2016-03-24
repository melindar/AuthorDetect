package authorDetect;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;

public class Parser 
{
	private Scanner scan;
	private String author;
	public ArrayList<String> tokens = new ArrayList<String>();

	public Parser(String value) 
	{
		this.scan = new Scanner(value.toString());
	}
	
	public Parser(Text value) 
	{
		this.scan = new Scanner(value.toString());
	}

	
	/*public static void main(String[] args)
	{
		String file = "Sample";
		Parser parse = new Parser(file);
		parse.openFile(file);
		parse.tokenizeUnigrams();
		parse.writeTokens();
	}*/

	public void openFile(String filename)
	{
		try {
			File file = new File(filename);
			scan = new Scanner(new FileInputStream(file));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public void printTokens()
	{
		for(String s : tokens)
		{
			System.out.println(s);
		}
	} 
	
	public void writeTokens()
	{
		try {
			PrintWriter p = new PrintWriter(new File("output.txt"));
			for(String s : tokens)
			{
				p.println(s);
			}
			
			p.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/******************************** Methods to parse Unigrams ********************************/

	public void tokenizeUnigrams()
	{
		while (scan.hasNextLine()) 
		{
			String line = scan.nextLine();
			String[] halfLine = line.split("<===>");
			author = halfLine[0];
			tokenizeUnigramLine(halfLine[1]);
		}
		scan.close();
	}
	
	private void tokenizeUnigramLine(String line)
	{
		StringTokenizer itr = new StringTokenizer(line);
        while (itr.hasMoreTokens()) 
        {
        	String token = itr.nextToken().trim();
        	String word = removePunctuation(token);
        	if(word.length() > 0) //Make sure the string is not just whitespace
        	{
        		tokens.add(word);
        	}
        }
	}
	
	private String removePunctuation(String s)
	{
		s = s.replaceAll("\\p{Punct}+", "");
		s = s.replaceAll("[^a-zA-Z0-9]", "");
		return s.toLowerCase();
	}

	public String getAuthor() {
		// TODO Auto-generated method stub
		return author;
	}

}
