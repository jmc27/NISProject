import java.util.ArrayList;

public class ScrubWords
{
	
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
		
	}
	
	// splits the word and returns an ArrayList<String> of what punctuation splits up
	// i assumed most contractions would result in one letter things we could filter out as stop words?
	// ex. mother-in-law should result in an arraylist {mother, in, law}
	// won't results in arraylist {won, t} issues arise when deciding what to do with these because both aren't really viable words to index
	// lions' results in arraylist {lions} and here is the issue of pluralization is there a difference between lion and lions or should it be scrubbed
	public ArrayList<String> scrubWords(String word)
	{
		word = word.toLowerCase();
		int i = 0;
		ArrayList<String> end = new ArrayList<String>();
		char letter;
		String word2 = "";
		
		while(i < word.length())
		{
			letter = word.charAt(i);
			
			if(letter >= 97 && letter <= 122)
			{
				word2 = word2 + letter;
			}
			else
			{
				if(word2.length() > 0)
					end.add(word2);
				word2 = "";
			}
			i++;
		}
		
		
		return end;
	}
	
	
	// i haven't coded this in because i just thought of it, but maybe i could differentiate between different kinds of non-letter characters
	// some would count as word enders like periods and commas and apostrophes and be treated like the first method
	// others would not and would be treated like the second method, things like dashes, slashes, @, &, *, etc.
	// if we think that's a good idea i could fix that soon
	
}
