package contrail.gephi;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import contrail.stages.ContrailParameters;
import contrail.stages.ParameterDefinition;
import contrail.stages.Stage;
import contrail.graph.EdgeDirection;
import contrail.graph.EdgeTerminal;
import contrail.graph.GraphNode;
import contrail.graph.GraphNodeData;
import contrail.sequences.DNAStrand;
import contrail.sequences.StrandsUtil;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

/**
 * Write a gephi XML file to represent a graph.
 *
 * Doc about gexf format:
 * http://gexf.net/1.2draft/gexf-12draft-primer.pdf
 *
 * WARNING: Gephi appears to have problems reading files in "/tmp" so
 * write the file somewhere else.
 *
 * TODO(jlewi): It would be good if we could label each node so that
 * we could see
 */
public class WriteGephiFile extends Stage {
  private static final Logger sLogger =
      Logger.getLogger(WriteGephiFile.class);

  // A mapping from node id's to integers used by gephi.
  private HashMap<EdgeTerminal, Integer> node_id_map =
      new HashMap<EdgeTerminal, Integer>();

  // The next value to assign to a node;
  private int next_id = 0;

  private Document doc;

  // Hashmap mapping node attributes to their id values.
  private HashMap<String, String> nodeAttrIdMap;

  protected Map<String, ParameterDefinition>
      createParameterDefinitions() {
    HashMap<String, ParameterDefinition> defs =
        new HashMap<String, ParameterDefinition>();
    defs.putAll(super.createParameterDefinitions());
    for (ParameterDefinition def:
      ContrailParameters.getInputOutputPathOptions()) {
      defs.put(def.getName(), def);
    }

    ParameterDefinition start_node = new ParameterDefinition(
        "start_node", "(Optional) if supplied num_hops must also be given.",
        String.class, null);
    ParameterDefinition num_hops = new ParameterDefinition(
        "num_hops", "(Optional) Number of hops to take starting at start_node.",
        Integer.class, null);

    defs.put(start_node.getName(), start_node);
    defs.put(num_hops.getName(), num_hops);
    return Collections.unmodifiableMap(defs);
  }

  /**
   * Create an XML element to represent the edge.
   * @param node_id_map
   * @param node
   * @param terminal
   * @return
   */
  private Element createElementForEdge(
      Document doc, int edge_id, EdgeTerminal src, EdgeTerminal dest) {
    Element xml_edge = doc.createElement("edge");
    Integer this_node_id = IdForTerminal(src);

    xml_edge.setAttribute("id", Integer.toString(edge_id));
    edge_id++;

    xml_edge.setAttribute("source", this_node_id.toString());
    Integer target_id = IdForTerminal(dest);
    xml_edge.setAttribute("target", target_id.toString());

    xml_edge.setAttribute("type", "directed");
    xml_edge.setAttribute(
        "label",
        StrandsUtil.form(src.strand,  dest.strand).toString());
    return xml_edge;
  }

  private void AddTerminalToIndex(EdgeTerminal terminal) {
    node_id_map.put(terminal, next_id);
    ++next_id;
  }

  private void AddNodesToIndex(Collection<GraphNode> nodes) {
    // A node for both the forward and reverse strands.
    EdgeTerminal terminal;
    for (GraphNode node: nodes) {
      terminal = new EdgeTerminal(node.getNodeId(), DNAStrand.FORWARD);
      node_id_map.put(terminal, next_id);
      ++next_id;
      node_id_map.put(terminal.flip(), next_id);
      ++next_id;
    }
  }

  private Integer IdForTerminal(EdgeTerminal terminal) {
    return node_id_map.get(terminal);
  }

  private Element CreateTerminal(EdgeTerminal terminal, GraphNode node) {
    Element xml_node = doc.createElement("node");
    Integer this_node_id = IdForTerminal(terminal);

    xml_node.setAttribute("id", this_node_id.toString());
    xml_node.setAttribute("label", terminal.toString());

    // Set all the attributes.
    Element attributeRoot = doc.createElement("attvalues");
    xml_node.appendChild(attributeRoot);
    for (Entry<String, String> entry : nodeAttrIdMap.entrySet()) {
      Element attribute = doc.createElement("attvalue");
      attribute.setAttribute("for", entry.getValue());
      String value;
      if (entry.getKey().equals("out-degree")) {
        value = Integer.toString(
            node.degree(terminal.strand, EdgeDirection.OUTGOING));
      } else if (entry.getKey().equals("in-degree")) {
        value = Integer.toString(
            node.degree(terminal.strand, EdgeDirection.INCOMING));
      } else {
        throw new RuntimeException(
            "No handler for attribute:" + entry.getKey());
      }
      attribute.setAttribute("value", value);
      attributeRoot.appendChild(attribute);
    }
    return xml_node;
  }

  public void writeGraph(Map<String, GraphNode> nodes, String xml_file) {
    if (xml_file.startsWith("/tmp")) {
      throw new RuntimeException(
          "Don't write the file to '/tmp' gephi has problems with that.");
    }
    DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
    DocumentBuilder dBuilder = null;
    try {
       dBuilder = dbFactory.newDocumentBuilder();
    } catch (Exception exception) {
        sLogger.error("Exception:" + exception.toString());
    }
    doc = dBuilder.newDocument();

    Element gexf_root = doc.createElement("gexf");
    doc.appendChild(gexf_root);
    Element root = doc.createElement("graph");
    gexf_root.appendChild(root);

    nodeAttrIdMap = new HashMap<String, String>();
    {
      // Declare some attributes for nodes
      Element attributes = doc.createElement("attributes");
      root.appendChild(attributes);
      attributes.setAttribute("class", "node");

      nodeAttrIdMap.put("out-degree", "0");
      nodeAttrIdMap.put("in-degree", "1");

      for (Entry<String, String> entry : nodeAttrIdMap.entrySet()) {
        Element attribute = doc.createElement("attribute");
        attribute.setAttribute("id", entry.getValue());
        attribute.setAttribute("title", entry.getKey());
        attribute.setAttribute("type", "string");
        attributes.appendChild(attribute);
      }

    }
    root.setAttribute("mode", "static");
    root.setAttribute("defaultedgetype", "directed");

    Element xml_nodes = doc.createElement("nodes");
    Element xml_edges = doc.createElement("edges");

    root.appendChild(xml_nodes);
    root.appendChild(xml_edges);

    // We assign each edge a unique id.
    int edge_id = 0;

    // I think the id's in the gephi xml file need to be string representations
    // of integers so we assign each node an integer.
    AddNodesToIndex(nodes.values());

    for (GraphNode node: nodes.values()) {
      for (DNAStrand strand : DNAStrand.values()) {
        EdgeTerminal terminal = new EdgeTerminal(node.getNodeId(), strand);

        Element xml_node = CreateTerminal(terminal, node);
        xml_nodes.appendChild(xml_node);

        List<EdgeTerminal> edges =
            node.getEdgeTerminals(strand, EdgeDirection.OUTGOING);
        for (EdgeTerminal other_terminal: edges){
          // If the node for the edge isn't provided skip it.
          // TODO(jlewi): It would be nice to visually indicate those
          // terminals which are actually terminal's in the node (i.e
          // we don't have GraphNode's for them.)
          if (IdForTerminal(other_terminal) == null) {
            AddTerminalToIndex(other_terminal);
            Element new_node = CreateTerminal(other_terminal, node);
            xml_nodes.appendChild(new_node);
          }
          Element xml_edge = createElementForEdge(
              doc, ++edge_id, terminal, other_terminal);
          xml_edges.appendChild(xml_edge);
        }

        // TODO(jlewi): Should we also plot any incoming edges if the
        // edge terminates on a node which isn't in nodes?
      }
    }
      // write the content into xml file
      try {
        TransformerFactory transformerFactory = TransformerFactory.newInstance();
        Transformer transformer = transformerFactory.newTransformer();
        DOMSource source = new DOMSource(doc);
        StreamResult result = new StreamResult(xml_file);
        transformer.transform(source, result);
      } catch (Exception exception) {
        sLogger.error("Exception:" + exception.toString());
      }

  }

  private HashMap<String, GraphNode> readNodes() {
    HashMap<String, GraphNode> nodes = new HashMap<String, GraphNode>();
    String input_path = (String) stage_options.get("inputpath");
    sLogger.info(" - input: "  + input_path);
    Schema schema = (new GraphNodeData()).getSchema();

    try {
      FileInputStream in_stream = new FileInputStream(input_path);
      SpecificDatumReader<GraphNodeData> reader =
          new SpecificDatumReader<GraphNodeData>(GraphNodeData.class);

      DataFileStream<GraphNodeData> avro_stream =
          new DataFileStream<GraphNodeData>(in_stream, reader);

      while(avro_stream.hasNext()) {
        GraphNodeData data = avro_stream.next();
        GraphNode node = new GraphNode(data);

        nodes.put(node.getNodeId(), node);
      }
    } catch (IOException exception) {
      throw new RuntimeException(
          "There was a problem reading the nodes the graph to an avro file." +
          " Exception:" + exception.getMessage());
    }
    return nodes;
  }

  /**
   * Find the subgraph by starting at the indicated node and walking the
   * specified number of hops.
   */
  private HashMap<String, GraphNode> getSubGraph(
      HashMap<String, GraphNode> nodes) {
    HashMap<String, GraphNode> subGraph = new HashMap<String, GraphNode>();

    // Use two lists so we can keep track of the hops.
    HashSet<String> thisHop = new HashSet<String>();
    HashSet<String> nextHop = new HashSet<String>();

    String start_node = (String) stage_options.get("start_node");
    int num_hops = (Integer) stage_options.get("num_hops");
    int hop = 0;
    thisHop.add(start_node);
    while (hop <= num_hops && thisHop.size() > 0) {
      // Fetch each node in thisHop.
      for (String nodeId : thisHop) {
        if (subGraph.containsKey(nodeId)) {
          continue;
        }
        subGraph.put(nodeId, nodes.get(nodeId));
        nextHop.addAll(nodes.get(nodeId).getNeighborIds());
      }
      thisHop.clear();
      thisHop.addAll(nextHop);
      nextHop.clear();
      ++hop;
    }
    return subGraph;
  }

  @Override
  public RunningJob runJob() throws Exception {
    // Check for missing arguments.
    String[] required_args = {"inputpath", "outputpath"};
    checkHasParametersOrDie(required_args);

    String outputPath = (String) stage_options.get("outputpath");
    sLogger.info(" - output: " + outputPath);

    HashMap<String, GraphNode> nodes = readNodes();

    //TODO(jlewi): Filter the nodes to get the subgraph of interest.
    if (stage_options.containsKey("num_hops") !=
        stage_options.containsKey("start_node")) {
      throw new RuntimeException(
          "You must supply num_hops and start_node if you want to draw only " +
          "part of the graph.");
    }

   if (stage_options.containsKey("start_node")) {
     nodes = getSubGraph(nodes);
   }
    writeGraph(nodes, outputPath);
    sLogger.info("Wrote: " + outputPath);

    return null;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new WriteGephiFile(), args);
    System.exit(res);
  }
}
