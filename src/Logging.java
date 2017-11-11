import java.text.SimpleDateFormat;
import java.util.Date;

public class Logging
{
	private static final SimpleDateFormat sdf = new SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss");
	
	public static void print(String message) 
	{
		Date d = new Date();
		System.out.println(sdf.format(d) + ": " + message);
	}
}
