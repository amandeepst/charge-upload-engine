package com.worldpay.pms.cue.engine.kryo.serializer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.io.Files;
import com.worldpay.pms.cue.engine.ChargingKryoRegistrator;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class TestKryoSerializer<T> {

  private final Kryo kryo;
  private final Class<T> classTag;

  TestKryoSerializer(Class<T> classTag) {
    this.classTag = classTag;
    this.kryo = new Kryo();
    new ChargingKryoRegistrator().registerClasses(kryo);
  }

  T canSerializeAndDeserializeBack(T subject) throws IOException {
    return deserialize(serialize(subject));
  }

  //
  // helper methods
  //

  private File serialize(T subject) throws IOException {
    File file = File.createTempFile(subject.getClass().getSimpleName(), "test.bin");
    try (Output out = new Output(new FileOutputStream(file))) {
      kryo.writeObject(out, subject);
    }

    log.info(
        "Wrote temp file `{}` of size {} kb.",
        Files.simplifyPath(file.getAbsolutePath()),
        file.getTotalSpace() / 1024);
    return file;
  }

  private T deserialize(File file) throws FileNotFoundException {
    try (Input in = new Input(new FileInputStream(file))) {
      return kryo.readObject(in, classTag);
    }
  }
}
