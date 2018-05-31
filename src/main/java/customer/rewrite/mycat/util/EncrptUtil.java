package customer.rewrite.mycat.util;

import com.alibaba.druid.util.Base64;

/***
 * @author：westsince2001
 * 加解密模拟类
 */
public class EncrptUtil {

  public static String doEncrpt(String source) {
    return new String(Base64.byteArrayToBase64(source.getBytes()));
  }

  public static String doDecipher(String source) {
    try {
      return new String(Base64.base64ToByteArray(source));
    } catch (Exception e) {
      return "invalid sourceStr...[" + source + "]";
    }
  }

  public static void main(String[] args) {
    String test = doEncrpt("socailNo2");
    System.out.println(test);
    System.out.println(doDecipher("c2VjdXJpdHkgbmFtZTE="));
  }
}
