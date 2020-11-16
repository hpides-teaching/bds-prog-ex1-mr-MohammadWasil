import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static java.util.Objects.requireNonNull;
import static org.apache.commons.io.FileUtils.deleteDirectory;
import static org.junit.Assert.assertEquals;

import java.io.*;
import java.util.HashMap;
import java.util.TreeMap;

public class MapReduceTaskTest {

    private MapReduceTask task;
    private String fileInputPath;

    public static void cleanOutputs() throws IOException {
        File[] directories = new File(".").listFiles(File::isDirectory);
        for (File directory : requireNonNull(directories)){
            if (directory.getName().startsWith("out-")) {
                deleteDirectory(directory);
            }
        }
    }

    HashMap<Text, IntWritable> getResults(String filePath) throws IOException {            
		HashMap<Text, IntWritable> results = new HashMap<>();
        BufferedReader reader = new BufferedReader(new FileReader(filePath));
        String pair = reader.readLine();
        while(pair!=null){
            String[] kv = pair.split("\t");
            results.put(new Text(kv[0]), new IntWritable(Integer.parseInt(kv[1])));
            pair = reader.readLine();
        }
        reader.close();
        return results;
    }

	public TreeMap<Text, IntWritable> aggregateFiles(String folderPath) throws IOException {
    	File folder = new File(folderPath);
    	HashMap<Text, IntWritable> output = new HashMap<>();
    	for(File f: requireNonNull(folder.listFiles((file, s) -> s.startsWith("part")))){
    		HashMap<Text, IntWritable> content = getResults(f.getPath());
    		output.putAll(content);
    	}
    	return new TreeMap<Text, IntWritable>(output);
    }

    @BeforeClass
    public static void cleanup() throws IOException {
        cleanOutputs();
    }
    
    @Before
    public void setUp() {
        this.task = new MapReduceTask();
        this.fileInputPath = "input/tweets-hillary-trump.csv";
    }

    @After
    public void tearDown() {
    }
    
    @Test
    public void wordCount() throws InterruptedException, IOException, ClassNotFoundException {
    	String outputFolder = "out-1";
        this.task.wordCount(this.fileInputPath, outputFolder);
        TreeMap<Text, IntWritable> jobResults = this.aggregateFiles(outputFolder);
        TreeMap<Text, IntWritable> solution = this.aggregateFiles("outputs/out-task1");
        
        assertEquals(jobResults, solution);
    }

    @Test
    public void topWord() throws InterruptedException, IOException, ClassNotFoundException {
    	String outputFolder = "out-2";
        this.task.topWord(this.fileInputPath, outputFolder);
        TreeMap<Text, IntWritable> jobResults = this.aggregateFiles(outputFolder);
        TreeMap<Text, IntWritable> solution = this.aggregateFiles("outputs/out-task2");
        
        assertEquals(jobResults, solution);
    }

    @Test
    public void top10TrumpWords() throws InterruptedException, IOException, ClassNotFoundException {
    	String outputFolder = "out-3";
        this.task.top10TrumpWords(this.fileInputPath, outputFolder);
        TreeMap<Text, IntWritable> jobResults = this.aggregateFiles(outputFolder);
        TreeMap<Text, IntWritable> solution = this.aggregateFiles("outputs/out-task3");

        assertEquals(jobResults, solution);
    }

    @Test
    public void top10TrumpHashtags() throws InterruptedException, IOException, ClassNotFoundException {
    	String outputFolder = "out-4";
        this.task.top10TrumpHashtags(this.fileInputPath, outputFolder);
        TreeMap<Text, IntWritable> jobResults = this.aggregateFiles(outputFolder);
        TreeMap<Text, IntWritable> solution = this.aggregateFiles("outputs/out-task4");

        assertEquals(jobResults, solution);
    }


    @Test
    public void main() throws IOException {

    }
}
