package fdb.mqtt;

import com.foundationdb.Database;
import com.foundationdb.FDB;
import com.foundationdb.MutationType;
import com.foundationdb.directory.DirectoryLayer;
import com.foundationdb.directory.DirectorySubspace;
import com.google.common.primitives.Longs;
import com.sampullara.cli.Argument;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Arrays.asList;

/**
 * Start with the simplest implementation possible that passes the benchmark.
 */
public class MQTTServer {

  private static final byte[] ONE_BYTES = Longs.toByteArray(1);
  @Argument(alias = "p", description = "Port to listen on")
  private static Integer port = 1883;

  @Argument(alias = "c", description = "FDB cluster file")
  private static String cluster;

  @Argument(alias = "n", description = "MQTT namespace")
  private static String namespace = "default";

  enum QoS {
    AT_MOST_ONCE_DELIVERY,
    AT_LEAST_ONCE_DELIVERY,
    EXACTLY_ONCE_DELIVERY
  }

  enum PacketType {
    Reserved,
    CONNECT,
    CONNACK,
    PUBLISH,
    PUBACK,
    PUBREC,
    PUBREL,
    PUBCOMP,
    SUBSCRIBE,
    SUBACK,
    UNSUBSCRIBE,
    UNSUBACK,
    PINGREQ,
    PINGRESP,
    DISCONNECT,
    Reserved2
  }

  enum ConnectionReturnCode {
    ACCEPTED,
    REFUSED_VERSION,
    REFUSED_IDENTIFIER,
    REFUSED_SERVER_UNAVAILABLE,
    REFUSED_AUTHENTICATION,
    REFUSED_AUTHORIZATION
  }

  private static byte[] PROTOCOL_NAME = "MQTT".getBytes();

  public static void main(String[] args) throws IOException, MqttException, InterruptedException {
    FDB fdb = FDB.selectAPIVersion(200);
    Database db = fdb.open(cluster);
    DirectoryLayer dl = DirectoryLayer.getDefault();
    DirectorySubspace subspace = dl.createOrOpen(db, asList("mqtt", namespace)).get();
    byte[] nextIdKey = subspace.get("nextId").pack();
    byte[] numTopicsKey = subspace.get("numTopics").pack();

    // client id / topic -> last message id read
    DirectorySubspace sessions = subspace.createOrOpen(db, asList("sessions")).get();
    // topic -> message
    DirectorySubspace retained = subspace.createOrOpen(db, asList("retained")).get();
    // topic / message id -> message
    DirectorySubspace messages = subspace.createOrOpen(db, asList("messages")).get();
    // topic -> number of messages
    DirectorySubspace topics = subspace.createOrOpen(db, asList("topics")).get();

    ExecutorService executorService = Executors.newCachedThreadPool();
    executorService.submit(() -> {
      ServerSocket serverSocket = new ServerSocket(port);
      while (true) {
        Socket socket = serverSocket.accept();
        socket.setSoTimeout(60000);
        executorService.submit(() -> {
          try {

            String willTopic = null;
            byte[] willMessage = null;

            DataInputStream dis = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
            DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
            while (true) {
              // Read the fixed header
              int headerbyte1 = dis.read();
              int packetType = headerbyte1 >> 4;
              int flags = headerbyte1 & 0x0F;
              AtomicInteger remainingLength = new AtomicInteger(readEncodedLength(dis));
              switch (PacketType.values()[packetType]) {
                case CONNECT: {
                  assertFlags(flags, 0);
                  if (remainingLength.intValue() == 0) {
                    throw new ProtocolException("Packet payload required");
                  }
                  String protocolName = readUTF8(dis, remainingLength);
                  int level = dis.read();
                  int connectFlags = dis.read();
                  int keepAlive = readInt16(dis);
                  boolean cleanSession = readBit(connectFlags, 1);
                  boolean willFlag = readBit(connectFlags, 2);
                  boolean willRetain = readBit(connectFlags, 5);
                  boolean passwordFlag = readBit(connectFlags, 6);
                  boolean usernameFlag = readBit(connectFlags, 7);
                  int willQoS = ((connectFlags & 24) >> 3);
                  String clientId = readUTF8(dis, remainingLength);
                  if (clientId.length() == 0) {
                    if (!cleanSession) {
                      writeConnAck(dos, false, ConnectionReturnCode.REFUSED_IDENTIFIER);
                      socket.close();
                      return;
                    }
                    clientId = UUID.randomUUID().toString();
                  }
                  if (willFlag) {
                    willTopic = readUTF8(dis, remainingLength);
                    willMessage = readBytes(dis);
                  }
                  String username = null;
                  if (usernameFlag) {
                    username = readUTF8(dis, remainingLength);
                    byte[] password = null;
                    if (passwordFlag) {
                      password = readBytes(dis);
                    }
                  }
                  writeConnAck(dos, cleanSession, ConnectionReturnCode.ACCEPTED);
                  socket.setSoTimeout((int) (keepAlive * 1.5) * 1000);
                  break;
                }
                case CONNACK: {
                  assertFlags(flags, 0);
                  break;
                }
                case PUBLISH: {
                  boolean duplicate = readBit(flags, 3);
                  boolean retain = readBit(flags, 1);
                  QoS qos = QoS.values()[((flags & 6) >> 1)];
                  String topicName = readUTF8(dis, remainingLength);
                  int packetId = -1;
                  if (qos.ordinal() > 0) {
                    remainingLength.addAndGet(-2);
                    packetId = readInt16(dis);
                  } else if (duplicate) {
                    throw new ProtocolException("Can't have qos = 0 and duplicates");
                  }
                  byte[] message = new byte[remainingLength.intValue()];
                  dis.readFully(message);
                  db.run(tx -> {
                    byte[] nextIdBytes = tx.get(nextIdKey).get();
                    long id;
                    if (nextIdBytes == null) {
                      id = 0;
                      tx.set(nextIdKey, Longs.toByteArray(1));
                    } else {
                      id = Longs.fromByteArray(nextIdBytes);
                      tx.mutate(MutationType.ADD, nextIdKey, ONE_BYTES);
                    }
                    tx.mutate(MutationType.ADD, topics.get(topicName).pack(), ONE_BYTES);
                    tx.set(messages.get(topicName).get(id).pack(), message);
                    if (retain) {
                      // TODO: handle special retain rules
                      tx.set(retained.get(topicName).pack(), message);
                    }
                    return null;
                  });
                  break;
                }
                case PUBACK: {
                  assertFlags(flags, 0);
                  int packetId = readInt16(dis);
                  break;
                }
                case PUBREC: {
                  assertFlags(flags, 0);
                  int packetId = readInt16(dis);
                  break;
                }
                case PUBREL: {
                  assertFlags(flags, 2);
                  int packetId = readInt16(dis);
                  break;
                }
                case PUBCOMP: {
                  assertFlags(flags, 0);
                  int packetId = readInt16(dis);
                  break;
                }
                case SUBSCRIBE: {
                  assertFlags(flags, 2);
                  int packetId = readInt16(dis);
                  remainingLength.addAndGet(-2);
                  if (remainingLength.intValue() == 0) {
                    throw new ProtocolException("Packet payload required");
                  }
                  Map<String, QoS> topicFilters = new LinkedHashMap<>();
                  do {
                    String topicFilter = readUTF8(dis, remainingLength);
                    QoS qos = QoS.values()[dis.read()];
                    remainingLength.decrementAndGet();
                    topicFilters.put(topicFilter, qos);
                  } while (remainingLength.longValue() > 0);
                  writeSubAck(packetId, topicFilters);
                  break;
                }
                case SUBACK: {
                  assertFlags(flags, 0);
                  int packetId = readInt16(dis);
                  if (remainingLength.intValue() == 0) {
                    throw new ProtocolException("Packet payload required");
                  }
                  break;
                }
                case UNSUBSCRIBE: {
                  assertFlags(flags, 2);
                  int packetId = readInt16(dis);
                  if (remainingLength.intValue() == 0) {
                    throw new ProtocolException("Packet payload required");
                  }
                  break;
                }
                case UNSUBACK: {
                  assertFlags(flags, 0);
                  int packetId = readInt16(dis);
                  break;
                }
                case PINGREQ: {
                  assertFlags(flags, 0);
                  break;
                }
                case PINGRESP: {
                  assertFlags(flags, 0);
                  break;
                }
                case DISCONNECT: {
                  assertFlags(flags, 0);
                  break;
                }
                case Reserved:
                case Reserved2:
                default:
                  socket.close();
              }
            }
          } catch (IOException e) {
            e.printStackTrace();
            try {
              socket.close();
            } catch (IOException e1) {
              // Ignore
            }
          }
        });
      }
    });
    Thread.sleep(1000);
    MqttClient test = new MqttClient("tcp://localhost:1883", "test");
    test.connect();
    System.out.println("Connected");
    test.publish("mytopic", "message".getBytes(), 0, false);

  }

  private static void writeSubAck(int packetId, Map<String, QoS> topicFilters) {
    
  }

  private static byte[] readBytes(DataInputStream dis) throws IOException {
    int length = readInt16(dis);
    byte[] bytes = new byte[length];
    dis.readFully(bytes);
    return bytes;
  }

  private static void writeConnAck(DataOutputStream dos, boolean session, ConnectionReturnCode returnCode) throws IOException {
    dos.write(2 << 4);
    dos.write(2);
    dos.write(session ? 1 : 0);
    dos.write(returnCode.ordinal());
    dos.flush();
  }

  private static boolean readBit(int connectFlags, int i) {
    return (connectFlags & 1 << i) != 0;
  }

  private static String readUTF8(DataInputStream dis, AtomicInteger remainingLength) throws IOException {
    int length = readInt16(dis);
    byte[] bytes = new byte[length];
    dis.readFully(bytes);
    remainingLength.addAndGet(-2 - length);
    return new String(bytes, StandardCharsets.UTF_8);
  }

  private static int readInt16(DataInputStream dis) throws IOException {
    return dis.read() * 256 + dis.read();
  }

  private static int readEncodedLength(DataInputStream dis) throws IOException {
    int multiplier = 1;
    int value = 0;
    int b;
    do {
      b = dis.read();
      value += (b & 127) * multiplier;
      multiplier *= 128;
      if (multiplier > 128*128*128) {
        throw new ProtocolException("Malformed remainging length");
      }
    } while ((b & 128) != 0);
    return value;
  }

  private static void assertFlags(int flags, int i) throws ProtocolException {
    if (flags != i) {
      throw new ProtocolException("Invalid flags");
    }
  }
}
