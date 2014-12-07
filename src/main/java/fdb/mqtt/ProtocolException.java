package fdb.mqtt;

import java.io.IOException;

/**
 * Created by sam on 12/6/14.
 */
public class ProtocolException extends IOException {
  public ProtocolException(String s) {
    super(s);
  }
}
