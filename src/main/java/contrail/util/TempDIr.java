package contrail.util;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class TempDIr implements Tool {
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new TempDIr(), args);
    System.exit(res);
  }

  @Override
  public void setConf(Configuration conf) {
    // TODO Auto-generated method stub

  }

  @Override
  public Configuration getConf() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int run(String[] args) throws Exception {
    System.out.println("tmpdir" + System.getProperty("java.io.tmpdir"));

    File someFile = FileHelper.createLocalTempDir();
    // TODO Auto-generated method stub
    return 0;
  }
}
