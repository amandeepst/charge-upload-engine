package com.worldpay.pms.cue.domain.common;

/**
 * Base exception for this component, highlights that something went wrong when finding
 * billableCharge of a particular event. And that the charge should be marked as error.
 */
public class ChargingUploadException extends RuntimeException {

  public ChargingUploadException(String format, Object... args) {
    super(String.format(format, args));
  }

  public ChargingUploadException(Throwable e, String format, Object... args) {
    super(args.length > 0 ? String.format(format, args) : format, e);
  }
}
