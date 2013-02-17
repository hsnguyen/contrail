package contrail.io;

import java.io.File;
import org.junit.*;
import static org.junit.Assert.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.LineReader;

import contrail.util.ByteUtil;
import contrail.util.FileHelper;
import contrail.util.MockFSDataInputStream;

import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.nio.ByteBuffer;
import java.util.List;

public class TestFastQInputFormat
{

  public static final String FastQ_1 =
      "@1/1\n" +
          "GGCGCGGGCCAGTGCGGCAAAGAATTTCGCCGAGATCCCACGCAAGGTGCGCATACCATCACCTACCACCGAGATAATGGCCAGCCGTTCCGTCACTGCC\n" +
          "+\n" +
          "+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n" +
          "@2/1\n" +
          "GGCTCCGAAGTGTAGCCCAGTTCTTTTAACTCACGCATTGTCTGTTGCGTGGTTTCATCATCCACGGCTGCATAACCCAGCTCTTTCAGTTGCCAGATTT\n" +
          "+\n" +
          "????????????????????????????????????????????????????????????????????????????????????????????????????\n";

  public static final String FastQ_2 =
          "@@@@@@@@@@@@++++++++\n" +
          "@2/1\n" +
          "GGCTCCGAAGTGTAGCCCAG\n" +
          "+\n" +
          "?????????????????????\n";

  public static final String FastQ_3 =
          "@2/1\n" +
          "GGCTCCGAAGTGTAGCCCAG\n" +
          "+\n" +
          "?????????????????????\n"+
          "@3/1\n" +
          "AAAAACCAGAAAAAAAAAAA\n" +
          "+\n" +
          "?????????????????????\n"+
          "@4/1\n" +
          "AATTACCAGAAAACGTAAAA\n" +
          "+\n" +
          "?????????????????????\n"+
          "@5/1\n" +
          "CATTACCAGAAAACGTAAAA\n" +
          "+\n" +
          "?????????????????????\n"+
          "@8/1\n" +
          "CCTTACCAGAAAACGTAAAA\n" +
          "+\n" +
          "?????????????????????\n";

  private JobConf conf;
  private NumberedFileSplit split;
  private File tempfile;
  private Text key;

  @Before
  public void setup() throws IOException
  {
    tempfile = File.createTempFile("fqinputformat_test", "fastq");
    conf = new JobConf();
    key = new Text();
  }

  @After
  public void tearDown()
  {
    tempfile.delete();
    split = null;
  }

  private void fileWrite(String s) throws IOException
  {
    PrintWriter pw = new PrintWriter( new BufferedWriter( new FileWriter(tempfile) ) );
    pw.write(s);
    pw.close();
  }

  @Test
  public void test_takeToNextStart_fromStart() throws IOException {
    long offset;
    fileWrite(FastQ_1);
    FastQInputFormat fqif = new FastQInputFormat();
    ByteBuffer byteBuffer = ByteBuffer.wrap(ByteUtil.stringToBytes(FastQ_1));
    MockFSDataInputStream mock_stream = new MockFSDataInputStream(byteBuffer);
    FSDataInputStream fstream = new FSDataInputStream(mock_stream);
    offset = fqif.takeToNextStart(fstream, 0);
    assertEquals(0, offset);
    assertEquals(0, fstream.getPos());
  }

  @Test
  public void test_takeToNextStart_fromMiddle() throws IOException
  {
    Text buffer = new Text();
    FastQInputFormat fqif = new FastQInputFormat();
    ByteBuffer byteBuffer = ByteBuffer.wrap(ByteUtil.stringToBytes(FastQ_1));

    File tempDir = FileHelper.createLocalTempDir();
    File tempFile = new File(tempDir, "input.fastq");
    PrintStream outStream = new PrintStream(tempFile);
    outStream.append(FastQ_1);
    outStream.close();

    Configuration conf = new Configuration();
    FSDataInputStream fstream = FileSystem.get(conf).open(
        new Path(tempFile.getPath()));

    long offset = fqif.takeToNextStart(fstream, 10);
    assertEquals(210, offset);
    assertEquals(210, fstream.getPos());

    // Double check that when we read the next line we get the start of
    // a FastQ Record.
    LineReader lineReader = new LineReader(fstream);
    lineReader.readLine(buffer);
    assertEquals(buffer.toString(), "@2/1");
  }

  @Test
  public void test_takeToNextStart_withAmpersand() throws IOException
  {
    Text buffer = new Text();
    FastQInputFormat fqif = new FastQInputFormat();
    ByteBuffer byteBuffer = ByteBuffer.wrap(ByteUtil.stringToBytes(FastQ_2));

    File tempDir = FileHelper.createLocalTempDir();
    File tempFile = new File(tempDir, "input.fastq");
    PrintStream outStream = new PrintStream(tempFile);
    outStream.append(FastQ_1);
    outStream.close();

    Configuration conf = new Configuration();
    FSDataInputStream fstream = FileSystem.get(conf).open(
        new Path(tempFile.getPath()));

    long offset = fqif.takeToNextStart(fstream, 1);
    LineReader lineReader = new LineReader(fstream);
    lineReader.readLine(buffer);
    assertEquals(buffer.toString(), "@2/1");
  }


  @Test
  public void test_retrieveSplits() throws IOException
  {
    List<NumberedFileSplit> numberedSplitList;

    long streamLength = FastQ_3.length();

    Text buffer = new Text();
    FastQInputFormat fqif = new FastQInputFormat();
    ByteBuffer byteBuffer = ByteBuffer.wrap(ByteUtil.stringToBytes(FastQ_3));
    MockFSDataInputStream mock_stream = new MockFSDataInputStream(byteBuffer);
    FSDataInputStream fstream = new FSDataInputStream(mock_stream);

    JobConf conf = new JobConf();
    conf.setInt("splitSize",60);
    FastQInputFormat fastQInputFormat = new FastQInputFormat();
    fastQInputFormat.configure(conf);

    long bytes = 0;
    for (NumberedFileSplit split: fastQInputFormat.retrieveSplits(new Path("dummy"), fstream, streamLength))
      bytes += split.getLength();

    // Check that the total bytes in all splits put together is equal to the
    // number of bytes in file - that is, no bytes are getting 'missed' in the splitting.
    assertEquals(bytes, streamLength);

    //TODO:(deepak) - More Test Cases to cover the RecordReader.
  }

  public static void main(String args[]) {
    org.junit.runner.JUnitCore.main(TestFastQInputFormat.class.getName());
  }
}