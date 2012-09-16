package storm.test;

import backtype.storm.generated.*;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Tuple;
import org.apache.thrift7.TException;
import org.apache.thrift7.protocol.TBinaryProtocol;
import org.apache.thrift7.protocol.TProtocol;
import org.apache.thrift7.transport.TFramedTransport;
import org.apache.thrift7.transport.TSocket;
import org.apache.thrift7.transport.TTransport;
import org.apache.thrift7.transport.TTransportException;

import java.util.Map;

public class NateTopology {

    public static StormTopology createTopology() {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("TestSpout", new NateSpout());
        builder.setBolt("TestBolt", new NateBolt(), 5);
        return builder.createTopology();
    }

    public static class NateSpout implements IRichSpout {

        SpoutOutputCollector outputCollector;

        @Override
        public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
            //To change body of implemented methods use File | Settings | File Templates.
            this.outputCollector = spoutOutputCollector;
        }

        @Override
        public void close() {
            //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public void activate() {
            //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public void deactivate() {
            //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public void nextTuple() {
            //outputCollector.emit(new Values("TEST"));
        }

        @Override
        public void ack(Object o) {
            //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public void fail(Object o) {
            //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public Map<String, Object> getComponentConfiguration() {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }
    }

    public static class NateBolt implements IRichBolt {

        @Override
        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
            //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public void execute(Tuple tuple) {
            //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public void cleanup() {
            //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public Map<String, Object> getComponentConfiguration() {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }
    }

    public static StormTopology getRemoteTopology(String host, String topologyId) {
        TTransport transport = null;
        try {
            TSocket socket = new TSocket(host, 6627);
            transport = new TFramedTransport(socket);
            TProtocol protocol = new TBinaryProtocol(transport);

            Nimbus.Client client = new Nimbus.Client(protocol);
            transport.open();


            return client.getTopology(topologyId);

        } catch (TTransportException e) {
            e.printStackTrace();
        } catch (TException e) {
            e.printStackTrace();
        } catch (NotAliveException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } finally {
            if (transport != null)
                transport.close();
        }

    }
}
