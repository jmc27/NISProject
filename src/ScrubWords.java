import java.util.ArrayList;

public class ScrubWords
{
	/*
	// takes a word until the first punctuation
	public String scrubWord(String word)
	{
		word = word.toLowerCase();
		int i = 0;
		boolean done = false;
		String end = "";
		char letter;
		
		while(!done && i < word.length())
		{
			letter = word.charAt(i);
			
			if(letter >= 97 && letter <= 122)
			{
				end = end + letter;
			}
			else
			{
				done = true;
			}
			i++;
		}
		
		
		return end;
		
	}*/
	
	// splits the word and returns an ArrayList<String> of what punctuation splits up
	// i assumed most contractions would result in one letter things we could filter out as stop words?
	// ex. mother-in-law should result in an arraylist {mother, in, law}
	// won't results in arraylist {won, t} issues arise when deciding what to do with these because both aren't really viable words to index
	// lions' results in arraylist {lions} and here is the issue of pluralization is there a difference between lion and lions or should it be scrubbed
	public ArrayList<String> scrubWords(String word)
	{
		//word = word.toLowerCase();
		int i = 0;
		ArrayList<String> end = new ArrayList<String>();
		char letter;
		String wordEnd = "";
		
		while(i < word.length())
		{
			letter = word.charAt(i);
			
			if(Character.isLetterOrDigit(letter) || letter == 39)
			{
				wordEnd = wordEnd + letter;
			}
			else
			{
				if(wordEnd.length() > 0)
				{
					cleanup(wordEnd);
					end.add(wordEnd);
				}
				wordEnd = "";
			}
			i++;
		}
		
		
		return end;
	}
	
	
	public boolean isLetter(char letter)
	{
		if(letter >= 97 && letter <= 122 || letter >= 65 && letter <= 90)
		{
			return true;
		}
		return false;
	}
	
	public String cleanup(String word)
	{
		if(word.charAt(0)==39)
			word = word.substring(1);
		
		if(word.charAt(word.length()-1)==39)
			word = word.substring(0, word.length()-1);
		
		if(word.substring(word.length()-2).equals("'s"))
			word = word.substring(0, word.length()-2);
		
		return word;
	}
}
