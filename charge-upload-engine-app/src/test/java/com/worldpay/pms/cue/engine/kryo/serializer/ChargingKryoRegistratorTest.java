package com.worldpay.pms.cue.engine.kryo.serializer;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.sql.Date;
import java.time.LocalDate;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ChargingKryoRegistratorTest {

  private TestKryoSerializer<TestKryo> serializerTest;

  @BeforeEach
  void init() {
    this.serializerTest = new TestKryoSerializer<>(TestKryo.class);
  }

  @Test
  void testSerializationAndDeserializationUsingKryo() {
    Stream.of(
            new TestKryo(0, 0L, null, true, 't', null),
            new TestKryo(10, 100L, "test", true, 't', Date.valueOf(LocalDate.now())))
        .forEach(
            kryoObj -> {
              try {
                assertMatch(kryoObj, serializerTest.canSerializeAndDeserializeBack(kryoObj));
              } catch (IOException e) {
                throw new UncheckedIOException(e);
              }
            });
  }

  void assertMatch(TestKryo subject, TestKryo candidate) {
    assertThat("subject!=candidate", subject, equalTo(candidate));
  }

  @Data
  @AllArgsConstructor
  @NoArgsConstructor
  static class TestKryo {
    int i;
    long l;
    String s;
    boolean b;
    char c;
    Date d;
  }
}
