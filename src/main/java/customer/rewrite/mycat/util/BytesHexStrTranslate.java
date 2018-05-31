package customer.rewrite.mycat.util;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * byte[]与16进制字符串相互转换
 *
 * @date：2017年4月10日 下午11:04:27
 */
public class BytesHexStrTranslate {

  private static final char[] HEX_CHAR = {'0', '1', '2', '3', '4', '5',
      '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};

  /**
   * 方法一：
   * byte[] to hex string
   */
  public static String bytesToHexFun1(byte[] bytes) {
    // 一个byte为8位，可用两个十六进制位标识
    char[] buf = new char[bytes.length * 2];
    int a = 0;
    int index = 0;
    for (byte b : bytes) { // 使用除与取余进行转换
      if (b < 0) {
        a = 256 + b;
      } else {
        a = b;
      }

      buf[index++] = HEX_CHAR[a / 16];
      buf[index++] = HEX_CHAR[a % 16];
    }

    return new String(buf);
  }

  /**
   * 方法二：
   * byte[] to hex string
   */
  public static String bytesToHexFun2(byte[] bytes) {
    char[] buf = new char[bytes.length * 2];
    int index = 0;
    for (byte b : bytes) { // 利用位运算进行转换，可以看作方法一的变种
      buf[index++] = HEX_CHAR[b >>> 4 & 0xf];//0xf==0x0f
      buf[index++] = HEX_CHAR[b & 0xf];
    }

    return new String(buf);
  }

  public static String bytesToHexFun22(byte[] bytes) {
    char[] buf = new char[bytes.length * 2];
    int index = 0;
    for (byte b : bytes) { // 利用位运算进行转换，可以看作方法一的变种
      buf[index++] = HEX_CHAR[b >> 4 & 0x0f];
      buf[index++] = HEX_CHAR[b & 0x0f];
    }

    return new String(buf);
  }

  /**
   * 方法三：
   * byte[] to hex string
   */
  public static String bytesToHexFunWithSpace(byte[] bytes) {
    StringBuilder buf = new StringBuilder(bytes.length * 2);
    for (byte b : bytes) { // 使用String的format方法进行转换
      buf.append(String.format("%02x", new Integer(b & 0xff)));
      buf.append(" ");
    }
    return buf.toString().substring(0, buf.length() - 1);
  }

  public static String byteBufferToHexFunWithSpace(ByteBuffer byteBuffer) {
    byte[] bs = new byte[byteBuffer.remaining()];
    for (int i = 0; i < bs.length; i++) {
      bs[i] = byteBuffer.get(i);
    }
    return bytesToHexFunWithSpace(bs);
  }

  public static String byteBufferToChar(ByteBuffer byteBuffer) {
    byte[] bs = new byte[byteBuffer.remaining()];
    for (int i = 0; i < bs.length; i++) {
      bs[i] = byteBuffer.get(i);
    }
    return bytesToChar((bs));
  }

  public static String bytesToChar(byte[] bytes) {
    StringBuilder buf = new StringBuilder(bytes.length);
    for (byte b : bytes) {
      buf.append((char) b);
    }

    return buf.toString();
  }

  /**
   * 将16进制字符串转换为byte[]
   */
  public static byte[] toBytes(String str) {
    if (str == null || str.trim().equals("")) {
      return new byte[0];
    }

    byte[] bytes = new byte[str.length() / 2];
    for (int i = 0; i < str.length() / 2; i++) {
      String subStr = str.substring(i * 2, i * 2 + 2);
      bytes[i] = (byte) Integer.parseInt(subStr, 16);
    }

    return bytes;
  }

  public static void main(String[] args) throws Exception {
    byte[] bytes = "测试".getBytes("utf-8");
    System.out.println("字节数组为：" + Arrays.toString(bytes));
    System.out.println("方法一：" + bytesToHexFun1(bytes));
    System.out.println("方法二：" + bytesToHexFun2(bytes));
    System.out.println("方法三：" + bytesToHexFunWithSpace(bytes));

    System.out.println("==================================");

    String str = "e6b58be8af95";
    System.out.println("转换后的字节数组：" + Arrays.toString(toBytes(str)));
    System.out.println(new String(toBytes(str), "utf-8"));

    System.out.println("=========");
    byte[] bs = new byte[2];
    bs[0] = 10;
    bs[1] = 100;
    System.out.println(bytesToHexFun2(bs));
    System.out.println(bytesToHexFun22(bs));
    System.out.println(0xf == 0x0f);
    System.out.println(0xf == 0xf0);
    System.out.println(Integer.parseInt("0f", 16));
    System.out.println(Integer.parseInt("f0", 16));
  }

}
