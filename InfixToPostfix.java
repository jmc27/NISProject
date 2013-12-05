/*import java.util.Stack;
import java.util.StringTokenizer;


public class InfixToPostfix {
	public static String inToPost(String query)
	{
		Stack<String> stack = new Stack<String>();
		String result = "";
		StringTokenizer tokenizer = new StringTokenizer(query);
		while (tokenizer.hasMoreTokens())
		{
			String current = tokenizer.nextToken();
			if (current.equals("not"))
			{
				stack.push(current);
				result = " ("+result;
			}
			else if (current.equals("and"))
			{
				stack.push(current);
				result = " ("+result;
 			}
			else if (current.equals("or"))
 			{
				stack.push(current);
				result = " ("+ result;
			}
			else if (current.equals("("))
			{
				stack.push("(");
			}
			else if (current.equals(")"))
			{
				String pop = "";
				while (!pop.equals("("))
				{
					pop = stack.pop();
					if (!pop.equals("("))
					{
						
							result+= " "+pop+ " )";
					}
				}
			}
			else
			{
				result += " "+current;
			}
		}
		while (!stack.isEmpty())
		{
			String pop = stack.pop();
			if (!pop.equals("("))
			{
				result += " " + pop + " )";
			}
		}
		
		// what's up with this line?
		//result = "("+result+ " or )";
		return result;
	}*/

/*import java.util.Arrays;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Stack;
import java.util.StringTokenizer;

public class InfixToPostfix
{
	
	public final static String[] OPERATOR_LIST = { "and", "not", "or", "(", ")"};
	public final static HashSet<String> OPERATORS = new HashSet<String>(Arrays.asList(OPERATOR_LIST));
	
	public static String inToPost(String query) {
		String result = "";
		
		Stack<String> stack = new Stack<String>();
		String stackTop = "";
		
		StringTokenizer tokenizer = new StringTokenizer(query);
		while (tokenizer.hasMoreTokens())
		{
			String current = tokenizer.nextToken();
			
			if(isOperator(current))
			{
				if(stack.isEmpty())
				{
					stack.push(current);
				}
				else
				{
					stackTop = stack.peek();
					while(!stack.isEmpty())
					{
						result = result + " " + stack.pop();
						if (!stack.isEmpty())
							stackTop = stack.peek();
					}
					stack.push(current);
				}
			}
			else
			{
				result = result + " " + current;
			}
		}
		
		while (!(stack.isEmpty()))
			result = result + " " + stack.pop();
		return result;
	}

	private static boolean isOperator(String check) {
		if (OPERATORS.contains(check))
			return true;
		else
			return false;
	}*/
	
	public static void main(String args[]){
		System.out.println(inToPost("( ( apple and orange ) not banana ) or banana"));
		System.out.println(inToPost("( not apple ) and orange"));
		System.out.println(inToPost("( orange or green ) or ( apple and banana )"));
		System.out.println(inToPost("( ( orange ) ( not green ) ) and ( apple or banana )"));
	}

}
