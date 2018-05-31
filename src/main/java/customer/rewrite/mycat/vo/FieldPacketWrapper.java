package customer.rewrite.mycat.vo;

import io.mycat.backend.mysql.BufferUtil;
import io.mycat.net.mysql.FieldPacket;
import java.nio.ByteBuffer;

/***
 * @author：westsince2001
 * 将对FieldPacket的修改，抽离
 */
public class FieldPacketWrapper extends FieldPacket {

  public void object2Bytes(ByteBuffer buffer) {
    int size = calcPacketSize();
    BufferUtil.writeUB3(buffer, size);
    buffer.put(packetId);
    writeBody(buffer);

  }

  private void writeBody(ByteBuffer buffer) {
    byte nullVal = 0;
    BufferUtil.writeWithLength(buffer, catalog, nullVal);
    BufferUtil.writeWithLength(buffer, db, nullVal);
    BufferUtil.writeWithLength(buffer, table, nullVal);
    BufferUtil.writeWithLength(buffer, orgTable, nullVal);
    BufferUtil.writeWithLength(buffer, name, nullVal);
    BufferUtil.writeWithLength(buffer, orgName, nullVal);
    buffer.put((byte) 0x0C);
    BufferUtil.writeUB2(buffer, charsetIndex);
    BufferUtil.writeUB4(buffer, length);
    buffer.put((byte) (type & 0xff));
    BufferUtil.writeUB2(buffer, flags);
    buffer.put(decimals);
    buffer.put((byte) 0x00);
    buffer.put((byte) 0x00);
    //buffer.position(buffer.position() + FILLER.length);
    if (definition != null) {
      BufferUtil.writeWithLength(buffer, definition);
    }
  }

  public String toString() {
    return new StringBuilder().append("catalog:[" + new String(catalog) + "]").
        append(",db:[" + new String(db) + "]").
        append(",table:[" + new String(table) + "]").
        append(",orgTable:[" + new String(orgTable) + "]").
        append(",name::[" + new String(name) + "]").
        append(",orgName:[" + new String(orgName) + "]").toString();
  }
}
