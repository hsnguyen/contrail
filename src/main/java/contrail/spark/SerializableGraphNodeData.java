package contrail.spark;

import java.io.IOException;
import java.io.ObjectStreamException;
import java.io.Serializable;

import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
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
        setValues(reader.read(null, decoder));
    }

    private void readObjectNoData()
            throws ObjectStreamException {
    }
}