package contrail.spark;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectStreamException;
import java.io.Serializable;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import contrail.graph.GraphNodeData;

/**
 * For now, Spark does not support Avro. This class is just a quick
 * workaround that (de)serializes GraphNodeData objects using Avro.
 *
 */
public class SerializableGraphNodeData extends GraphNodeData implements Serializable {
    public SerializableGraphNodeData(GraphNodeData data) {
      // Copy the data into this node.
      // This does a deep copy but could be expensive. The other way
      // To do this would be to use the get/put methods to set the individual
      // fields.
      try {
        // TODO(jeremy@lewi.us): We could probably make this code
        // more efficient by reusing objects.
        SpecificDatumWriter<GraphNodeData> datumWriter =
            new SpecificDatumWriter<GraphNodeData>(data.getSchema());

        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        BinaryEncoder encoder =
            EncoderFactory.get().binaryEncoder(outStream, null);

        datumWriter.write(data, encoder);
        // We need to flush the encoder to write the data to the byte
        // buffer.
        encoder.flush();
        outStream.flush();

        // Now read it back in as a specific datum reader.
        ByteArrayInputStream inStream = new ByteArrayInputStream(
            outStream.toByteArray());

        BinaryDecoder decoder =
            DecoderFactory.get().binaryDecoder(inStream, null);
        SpecificDatumReader<GraphNodeData> specificReader = new
            SpecificDatumReader<GraphNodeData>(data.getSchema());

        specificReader.read(this, decoder);
      } catch (IOException e) {
        throw new RuntimeException(
            "There was a problem converting the GraphNodeData to " +
            "SerializableGraphNodeData", e);
      }
    }
    private void writeObject(java.io.ObjectOutputStream out)
            throws IOException {
        DatumWriter<GraphNodeData> writer = new SpecificDatumWriter<GraphNodeData>(GraphNodeData.class);
        Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        writer.write(this, encoder);
        encoder.flush();
    }

    private void readObject(java.io.ObjectInputStream in)
            throws IOException, ClassNotFoundException {
        DatumReader<GraphNodeData> reader =
                new SpecificDatumReader<GraphNodeData>(GraphNodeData.class);
        Decoder decoder = DecoderFactory.get().binaryDecoder(in, null);
        reader.read(this, decoder);
    }

    private void readObjectNoData()
            throws ObjectStreamException {
    }
}