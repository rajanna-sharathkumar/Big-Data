import java.io.PrintStream;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;

public class FORMAT_GENRE
  extends EvalFunc<String>
{
  public String exec(Tuple input)
  {
    try
    {
      if ((input == null) || (input.size() == 0)) {
        return null;
      }
      String genres = "";
      String strng = (String)input.get(0);
      String[] str = strng.split("\\|");
      for (int i = 0; i < str.length; i++) {
        if (i == str.length - 1) {
          genres = genres + (i + 1) + ") " + str[i] + " ";
        } else if (i == str.length - 2) {
          genres = genres + (i + 1) + ") " + str[i] + "& ";
        } else if (i < str.length - 2) {
          genres = genres + (i + 1) + ") " + str[i] + ", ";
        }
      }
      return genres + "sns140230";
    }
    catch (ExecException ex)
    {
      System.out.println("Error: " + ex.toString());
    }
    return null;
  }
}
