import InvertedIndex.InvertedIndexDriver;
import Sorter.SorterDriver;
import TFIDF.TFIDFDriver;

import java.util.ArrayList;
import java.util.List;

public class Main
{
    public static void main(String[] args) throws Exception
    {
        boolean sort = false;
        boolean tfidf = false;
        List<String> paths = new ArrayList<>();

        for (String arg : args)
        {
            if (arg.equals("-s") || arg.equals("--sort"))
            {
                sort = true;
            }
            else if (arg.equals("-t") || arg.equals("--tfidf"))
            {
                tfidf = true;
            }
            else if (arg.equals("-h") || arg.equals("--help"))
            {
                printHelp();
                return;
            }
            else if (!arg.startsWith("-"))
            {
                paths.add(arg);
            }
            else
            {
                System.out.println("Unknown option: " + arg);
                printHelp();
                System.exit(1);
            }
        }

        if (paths.size() != 2)
        {
            System.out.println("Invalid number of arguments");
            printHelp();
            System.exit(1);
        }

        String input = paths.get(0);
        String output = paths.get(1);

        if (tfidf)
        {
            System.out.println("Running in TFIDF mode...");
            TFIDFDriver.run(new String[]{input, output});
        }
        else
        {
            System.out.println("Running in Inverted Index mode...");
            int exitCode = InvertedIndexDriver.run(new String[]{input, output});
            if (sort && exitCode == 0)
            {
                String sortOutputPath = output + "_sorted";
                SorterDriver.run(new String[]{output, sortOutputPath});
            }
        }
    }

    private static void printHelp()
    {
        System.out.println("Usage: hadoop jar <jar_file> Main [options] <input> <output>");
        System.out.println("Options:");
        System.out.println("  -s, --sort\tSort the output by average frequency");
        System.out.println("  -t, --tfidf\tCalculate TF-IDF for the input documents");
        System.out.println("  -h, --help\tPrint this help message");
    }
}
