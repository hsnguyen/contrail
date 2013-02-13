/**
 * Copyright 2012 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// Author: Jeremy Lewi (jeremy@lewi.us)
package contrail.stages;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.util.DefaultPrettyPrinter;

/**
 * Writer for information about which stages executed.
 *
 */
public class StageInfoWriter {
  private static final Logger sLogger = Logger.getLogger(StageInfoWriter.class);
  private static class Node {
    public StageInfo stageInfo;
    public Node parent;
    public ArrayList<Node> children;

    public Node() {
      children = new ArrayList<Node>();
    }
  }

  private Node root;
  private Node current;

  private String outputPath;
  private Configuration conf;

  public StageInfoWriter(Configuration conf, String outputPath) {
    root = new Node();
    current = root;

    this.conf = conf;
    this.outputPath = outputPath;
  }

  /**
   * Sets the current stage info.
   */
  public void setInfo(StageInfo other) {
    current.stageInfo = SpecificData.get().deepCopy(other.getSchema(), other);
  }

  /**
   * Adds a child to the current stage info and points the instance at that
   * child.
   *
   * This function should be called before passing the writer to
   * a substage.
   */
  public void addChild() {
    Node child = new Node();
    current.children.add(child);
    child.parent = current;
    current = current.children.get(current.children.size() - 1);
  }

  /**
   * Moves up the tree to the parent. Should be called when a substage
   * returns control to the parent.
   */
  public void moveToParent() {
    if (current.parent == null) {
      sLogger.fatal(
          "Tried to move to the parent but current node has no parent.",
          new RuntimeException("No Parent."));
      System.exit(-1);
    }
    current = current.parent;
  }

  /**
   * Construct a StageInfo from the tree of stage info.
   */
  private StageInfo buildStageInfo() {
    StageInfo rootInfo = root.stageInfo;

    // Descend through the tree and fill in the substages.
    ArrayList<Node> nodeStack = new ArrayList<Node>();
    nodeStack.add(root);

    while (nodeStack.size() > 0) {
      Node node = nodeStack.remove(nodeStack.size() - 1);
      node.stageInfo.setSubStages(new ArrayList<StageInfo>());
      for (Node child : node.children) {
        node.stageInfo.getSubStages().add(child.stageInfo);
        nodeStack.add(child);
      }
    }

    // Make a copy of the stageInfo before returning it.
    rootInfo = SpecificData.get().deepCopy(rootInfo.getSchema(), rootInfo);
    return rootInfo;
  }

  /**
   * Write the stageInfo
   */
  public void writeStage(StageInfo lastInfo) {
    setInfo(lastInfo);
    write();
  }

  /**
   * Write the stageInfo
   */
  public void write() {
    StageInfo info = buildStageInfo();
    // TODO(jlewi): We should cleanup old stage files after writing
    // the new one. Or we could try appending json records to the same file.
    // When I tried appending, the method fs.append threw an exception.
    SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd-HHmmss");
    Date date = new Date();
    String timestamp = formatter.format(date);

    String stageDir = FilenameUtils.concat(
        outputPath, "stage_info");

    String outputFile = FilenameUtils.concat(
        stageDir, "stage_info." + timestamp + ".json");
    try {
      FileSystem fs = FileSystem.get(conf);
      if (!fs.exists(new Path(stageDir))) {
        fs.mkdirs(new Path(stageDir));
      }
      FSDataOutputStream outStream = fs.create(new Path(outputFile));

      JsonFactory factory = new JsonFactory();
      JsonGenerator generator = factory.createJsonGenerator(outStream);
      generator.setPrettyPrinter(new DefaultPrettyPrinter());
      JsonEncoder encoder = EncoderFactory.get().jsonEncoder(
          info.getSchema(), generator);
      SpecificDatumWriter<StageInfo> writer =
          new SpecificDatumWriter<StageInfo>(StageInfo.class);
      writer.write(info, encoder);
      // We need to flush it.
      encoder.flush();
      outStream.close();
    } catch (IOException e) {
      sLogger.fatal("Couldn't create the output stream.", e);
      System.exit(-1);
    }
  }
}
