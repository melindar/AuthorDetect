package ngrams;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
/*import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException; */
import java.util.ArrayList;
import java.util.Scanner;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;

public class Parser 
{
	private String date;
	private String author;
	private String title;
	private String ngramType;
	private Scanner scan;
	public ArrayList<String> tokens = new ArrayList<String>();

	public Parser(String ngramType, Text value) 
	{
		this.ngramType = ngramType;
		this.scan = new Scanner(value.toString());
	}

	
	/*public static void main(String[] args)
	{
		String file = "11.txt";
		//Text value = new Text();
		Parser parse = new Parser("unigramDate", file);
		parse.openFile(file);
		parse.setHeaderInfo();
		parse.tokenizeNgrams();
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
	
	/******************************** Getters and Setters ********************************/
	
	public String getDate() 
	{
		return date;
	}

	public String getAuthor() 
	{
		return author;
	}

	public String getTitle() 
	{
		return title;
	}
	
	public String getValue()
	{
		if(ngramType.equals("unigramDate") || ngramType.equals("bigramDate"))
			return getDate();
		else // unigramAuthor or bigramAuthor for all other cases
			return getAuthor();
	}

	/******************************** Methods to retrieve header data ********************************/
	
	public void setHeaderInfo() 
	{
		String stopString = "***";
		
		if(scan.hasNextLine())
		{
			String line = scan.nextLine();
			
			while(!line.startsWith(stopString))
			{
				if(line.startsWith("Title:"))
				{
					this.title = parseTitle(line);
				}
				else if(line.startsWith("Author:"))
				{
					this.author = parseAuthor(line);
				}
				else if(line.startsWith("Release Date:"))
				{
					this.date = parseDate(line);
				}
				
				line = scan.nextLine();
			}
			
		}
		
	}
	
	
	/******************************** Methods to parse header ********************************/
	
	private String parseDate(String containsDate)
	{
		@SuppressWarnings("resource")
		Scanner scan = new Scanner(containsDate);
		String pattern = "\\p{Digit}\\p{Digit}\\p{Digit}\\p{Digit}";
		String date = scan.findInLine(pattern);
		return date;
	}
	
	private String parseAuthor(String containsAuthor)
	{
		String[] tokens = containsAuthor.split("\\p{Blank}");  
		String lastToken = tokens[tokens.length -1];
		// Remove the ')' from last name in parenthesis (e.g. Mark Twain (Samuel Clemens))
		if(lastToken.charAt(lastToken.length() - 1) == ')')
			lastToken = lastToken.substring(0,lastToken.length() - 1);
		return lastToken; 
	}
	
	private String parseTitle(String containsTitle)
	{
		String title = containsTitle.replaceAll("Title:", "").trim();
		return title;
	}
	
	/******************************** Methods to parse Ngrams ********************************/
	
	public void tokenizeNgrams()
	{
		if(ngramType.startsWith("unigram"))
			tokenizeUnigrams();
		else // Type is bigram if not unigram
			tokenizeBigrams();	
	}
	
	private void tokenizeBigrams() 
	{
		String[] fileString = putFileIntoString();
		tokenizeBigramSentences(fileString);
	}
	
	private String[] putFileIntoString()
	{
		String fileString = "";
		while (scan.hasNextLine()) 
		{
			String line = scan.nextLine();
			if(line.startsWith("***"))
				break;
			fileString += " " + line;
		}
		scan.close();
		fileString = fileString.replaceAll("[\\\t|\\\n|\\\r]"," ");
		String[] sentences = fileString.split("\\.");
		return sentences;
	}
	
	private void tokenizeBigramSentences(String[] sentences)
	{
		for(int i = 0; i < sentences.length; i++)
		{
			String sentence = removePunctuation(sentences[i]);
			setBigramTokens(sentence);
		}
	}
	
	private void setBigramTokens(String sentence)
	{
		sentence = "_START_ " + sentence + " _END_";
		int first = 0;
		int second = 1;
		String[] sentenceTokens = sentence.split("\\p{Space}+");
		while(second < sentenceTokens.length)
		{
			String token = sentenceTokens[first] + " " + sentenceTokens[second];
			tokens.add(token);
			first ++;
			second ++;
		}
	}
	
	private void tokenizeUnigrams()
	{
		while (scan.hasNextLine()) 
		{
			String line = scan.nextLine();
			if(line.startsWith("***"))
				break;
			tokenizeUnigramLine(line);
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

}
