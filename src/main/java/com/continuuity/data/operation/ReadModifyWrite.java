package com.continuuity.data.operation;

import com.continuuity.api.data.ConditionalWriteOperation;

public class ReadModifyWrite implements ConditionalWriteOperation {

  private final byte [] key;
  private final Modifier<byte[]> modifier;

  public ReadModifyWrite(final byte [] key, Modifier<byte[]> modifier) {
    this.key = key;
    this.modifier = modifier;
  }

  @Override
  public byte [] getKey() {
    return key;
  }

  public Modifier<byte[]> getModifier() {
    return modifier;
  }

  @Override
  public int getPriority() {
    return 1;
  }
}
