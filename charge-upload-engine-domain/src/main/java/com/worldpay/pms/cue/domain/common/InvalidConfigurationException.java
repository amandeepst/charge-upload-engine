package com.worldpay.pms.cue.domain.common;

/** Exception indicating an error in input configuration. */
public class InvalidConfigurationException extends ChargingUploadException {

  public InvalidConfigurationException(String format, Object... args) {
    super(format, args);
  }

  public InvalidConfigurationException(Throwable e, String format, Object... args) {
    super(e, format, args);
  }
}
