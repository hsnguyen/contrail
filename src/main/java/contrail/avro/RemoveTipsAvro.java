package contrail.avro;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroMapper;
import org.apache.avro.mapred.AvroReducer;
import org.apache.avro.mapred.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import contrail.ContrailConfig;
import contrail.RemoveTipMessage;
import contrail.Stats;
import contrail.graph.EdgeDirection;
import contrail.graph.EdgeTerminal;
import contrail.graph.GraphNode;
import contrail.graph.GraphNodeData;
import contrail.sequences.DNAStrand;
import contrail.sequences.StrandsForEdge;
import contrail.sequences.StrandsUtil;

/*
removeTips Phase  identifies the 'tips' in the graphdata;
These tips are identified by 
1. Sum of inDegree and outDegree is at most 1
2. their sequence length being greater than a particular limit (TIPLENGTH)

We can have lots of tips along one strand; and sometimes all the edges in a particular Strand direction are tips, 
In that case we only keep the longest one and remove all other shorter tips.

Mapper:
-- Identify the tips
-- Tell the corresponding neighbor that I am the tip by sending Removetip Message
-- collect nodeID of terminal and Removetip Message (contains complement of Strand to neighbor, nodeID of tip)

Reducer:
-- we identify the best-tip (longest tip) in both kind of DNAStrands
-- delete rest of the tips for both kind of DNAStrands
 */

public class RemoveTipsAvro extends Stage {	
  private static final Logger sLogger = Logger.getLogger(RemoveTipsAvro.class);

  public static final Schema MAP_OUT_SCHEMA = Pair.getPairSchema(Schema.create(Schema.Type.STRING), (new RemoveTipMessage()).getSchema());
  private static Pair<CharSequence, RemoveTipMessage> out_pair = new Pair<CharSequence, RemoveTipMessage>(MAP_OUT_SCHEMA);

  protected Map<String, ParameterDefinition> createParameterDefinitions() {
    HashMap<String, ParameterDefinition> defs = new HashMap<String, ParameterDefinition>();
    defs.putAll(super.createParameterDefinitions());
    
    ParameterDefinition TIPLENGTH = new ParameterDefinition("TIPLENGTH", "minimum allowed value of Tip", Integer.class, new Integer(0));

    for (ParameterDefinition def: new ParameterDefinition[] {TIPLENGTH}) {
      defs.put(def.getName(), def);
    }
    for (ParameterDefinition def:
      ContrailParameters.getInputOutputPathOptions()) {
      defs.put(def.getName(), def);
    }

    return Collections.unmodifiableMap(defs);
  }
  // RemoveTipsMapper
  ///////////////////////////////////////////////////////////////////////////

  public static class RemoveTipsAvroMapper extends 
  AvroMapper<GraphNodeData, Pair<CharSequence, RemoveTipMessage>>  {

    public int TIPLENGTH = 0;
    public  GraphNode node= null;
    public static boolean VERBOSE = false;
    public static RemoveTipMessage msg= null;

    public void configure(JobConf job) {	
      RemoveTipsAvro stage = new RemoveTipsAvro();
      Map<String, ParameterDefinition> definitions = stage.getParameterDefinitions();
      TIPLENGTH = (Integer)(definitions.get("TIPLENGTH").parseJobConf(job));
      msg= new RemoveTipMessage();
      out_pair = new Pair<CharSequence,  RemoveTipMessage>("", msg);
    }

    @Override
    public void map(GraphNodeData graph_data,
        AvroCollector<Pair<CharSequence, RemoveTipMessage>> output, 
        Reporter reporter) throws IOException  {

      node = new GraphNode(graph_data);
      int fdegree = node.degree(DNAStrand.FORWARD);
      int rdegree = node.degree(DNAStrand.REVERSE);
      int len     = graph_data.getSequence().getLength();

      if ((fdegree == 0) && (rdegree == 0))   {
        reporter.incrCounter("Contrail", "tips_island", 1);
        return;
      }
      if ((len <= TIPLENGTH) && (fdegree + rdegree <= 1))  {
        reporter.incrCounter("Contrail", "tips_found", 1);

        if (VERBOSE)	{
          sLogger.info("Removing tip " + node.getNodeId() + " len=" + len);
        }

        // Tell the one neighbor that I'm a tip	
        DNAStrand strand;
        if (fdegree == 1) {
          strand = DNAStrand.FORWARD; 
        } 
        else {
          strand = DNAStrand.REVERSE;
        }

        List<EdgeTerminal> terminals = node.getEdgeTerminals(strand, EdgeDirection.OUTGOING);
        StrandsForEdge key = StrandsUtil.form(strand, terminals.get(0).strand);

        msg.setNode(graph_data);
        msg.setEdgeStrands(key);			
        out_pair.set( terminals.get(0).nodeId, msg);
        output.collect(out_pair);
      }
      else	{
        msg.setNode(graph_data);
        msg.setEdgeStrands(null); /*setEdgeStrands is set null to indicate 
							  that this node is normal, not a tip*/
        out_pair.set(node.getNodeId(), msg);
        output.collect(out_pair);
        reporter.incrCounter("Contrail", "nodes", 1);   	
      }
    }
  }

  // RemoveTipsReducer
  ///////////////////////////////////////////////////////////////////////

  public static class RemoveTipsAvroReducer 
  extends AvroReducer<CharSequence, RemoveTipMessage, GraphNodeData>   {
    GraphNode temp_node = null;
    GraphNode actual_node= null;
    GraphNode tip_node = null;

    public void configure(JobConf job) {
      temp_node = new GraphNode();
      actual_node= new GraphNode();
      tip_node= new GraphNode();
    }

    // identifies the best-tip (longest tip) for a particular kind of DNAStrands
    GraphNodeData LongestTip(List<RemoveTipMessage> msg_list)	{
      int bestlen = 0;
      RemoveTipMessage besttip_msg = null;

      for (RemoveTipMessage message : msg_list)	{
        tip_node.setData(message.getNode());
        int len = tip_node.getData().getSequence().getLength();
        if (len > bestlen)	{
          bestlen = len;
          besttip_msg = message;
        }
      }
      return besttip_msg.getNode();
    }

    @Override
    public void reduce(CharSequence nodeid, Iterable<RemoveTipMessage> iterable,
        AvroCollector<GraphNodeData> output, Reporter reporter)
            throws IOException   {

      Iterator<RemoveTipMessage> iter = iterable.iterator();

      //-- set-up 2 lists in a HashMap keyed by DNAStrand and its corresponding message from mapper
      //-- Thus HashMap has 2 entries as lists; one list corresponds to Forward EdgeStrands generated from mapper; and other corresponds to Reverse Edgestrands
      //-- populate the 2 lists using the output from the mapper sent to reducer for a particular terminal (whose nodeID is sent as key); 

      Map<DNAStrand, List<RemoveTipMessage>> tips = new HashMap<DNAStrand, List<RemoveTipMessage>>();

      List<RemoveTipMessage> f_msglist = new ArrayList<RemoveTipMessage>();
      tips.put(DNAStrand.FORWARD, f_msglist);
      List<RemoveTipMessage> r_msglist = new ArrayList<RemoveTipMessage>();
      tips.put(DNAStrand.REVERSE, r_msglist);

      int sawnode = 0;
      String besttip_Id= "";
      GraphNodeData besttip_data= null;

      while(iter.hasNext())	{
        RemoveTipMessage msg = iter.next();	
        if (msg.getEdgeStrands() == null)    {  // non tip , normal node
          actual_node.setData(msg.getNode());
          actual_node = actual_node.clone();

          sawnode++;
        }
        else 	{
          RemoveTipMessage copy = new RemoveTipMessage();
          copy.setEdgeStrands(msg.getEdgeStrands());
          temp_node.setData(msg.getNode());
          temp_node = temp_node.clone();
          copy.setNode(temp_node.getData());

          DNAStrand dnastrand= StrandsUtil.src(StrandsUtil.complement(copy.getEdgeStrands()) );
          tips.get(dnastrand).add(copy);
        }
      } 

      if (sawnode != 1)	{
        throw new IOException("ERROR: Didn't see exactly 1 NON-tip node (" + sawnode + ") for " + nodeid.toString());
      }

      for(DNAStrand strand: DNAStrand.values())	{
        int deg = 0;
        int numtrim = 0;
        boolean result= false;

        List<RemoveTipMessage> msg_list = tips.get(strand);

        numtrim += msg_list.size(); 
        if (numtrim == 0) { continue; }
        deg = actual_node.degree(strand);   

        if (numtrim == deg)	{
          // All edges in this direction are tips, only keep the longest one				
          besttip_data= LongestTip(msg_list);       // getNodeID of Longest Tip	
          besttip_Id= besttip_data.getNodeId().toString();
          output.collect(besttip_data);             // we output the one node that is a tip
          reporter.incrCounter("Contrail", "tips_kept", 1);
        }	
        /* if the number of tips is > 0 but not equal to the degree
	of the non tip node;then we'll remove all the tips and
	leave non-tips intact
	the tips with same length are not removed
         */

        for (RemoveTipMessage message : msg_list)   {
          tip_node= new GraphNode(message.getNode());
          if(numtrim == deg)	{			
            // keep the longest ones
            if ( !tip_node.getNodeId().equals(besttip_Id) )     {	
              // not the best tip
              if( tip_node.getData().getSequence().getLength() < besttip_data.getSequence().getLength() )    { // check if its len < len of longest tip
                result = actual_node.removeNeighbor(tip_node.getNodeId());
              }
            }
          }
          else	{
            // remove all
            result = actual_node.removeNeighbor(tip_node.getNodeId()); 
          }

          if(result)    {
            reporter.incrCounter("Contrail", "tips_clipped", 1);
          }

        }	
      }
      output.collect(actual_node.getData());
    }
  }

  // Run
  //////////////////////////////////////////////////////////////////////////

  protected int run() throws Exception
  {

    String[] required_args = {"inputpath", "outputpath", "TIPLENGTH"};
    checkHasParametersOrDie(required_args);

    String inputPath = (String) stage_options.get("inputpath");
    String outputPath = (String) stage_options.get("outputpath");

    int TIPLENGTH=  (Integer) stage_options.get("TIPLENGTH");

    sLogger.info(" - input: "  + inputPath);
    sLogger.info(" - output: " + outputPath);

    Configuration base_conf = getConf();
    JobConf conf = null;
    if (base_conf != null) {
      conf = new JobConf(getConf(), this.getClass());
    } 
    else {
      conf = new JobConf(this.getClass());
    }
    conf.setJobName("RemoveTips " + inputPath + " " + TIPLENGTH);
    initializeJobConfiguration(conf);

    FileInputFormat.addInputPath(conf, new Path(inputPath));
    FileOutputFormat.setOutputPath(conf, new Path(outputPath));

    GraphNodeData graph_data = new GraphNodeData();
    AvroJob.setInputSchema(conf, graph_data.getSchema());
    AvroJob.setMapOutputSchema(conf, RemoveTipsAvro.MAP_OUT_SCHEMA);

    AvroJob.setMapperClass(conf, RemoveTipsAvroMapper.class);
    AvroJob.setReducerClass(conf, RemoveTipsAvroReducer.class);

    AvroJob.setOutputSchema(conf, graph_data.getSchema());

    //delete the output directory if it exists already
    FileSystem.get(conf).delete(new Path(outputPath), true);

    JobClient.runJob(conf);
    return 0;
  }

  public int run(String[] args) throws Exception {
    sLogger.info("Tool name: RemoveTips");
    parseCommandLine(args);   
    return run();
  }	

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new RemoveTipsAvro(), args);
    System.exit(res);
  }
}
