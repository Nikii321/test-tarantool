package com.tarantooltest.tarantool;

import com.google.protobuf.ByteString;
import com.tarantool.grpc.Scheme.CountResponse;
import com.tarantool.grpc.Scheme.KeyValueModel;
import com.tarantool.grpc.Scheme.IsDeleted;
import com.tarantooltest.tarantool.service.TarantoolServiceImpl;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.stream.Stream;

@ExtendWith(SpringExtension.class)
@SpringBootTest
public class TarantoolTest {

    @Autowired
    private TarantoolServiceImpl tarantoolService;

    private KeyValueModel getKeyValue(String key, byte[] value){
        return KeyValueModel.newBuilder().setKey(key).setValue(ByteString.copyFrom(value)).build();
    }

    @Test
    void testPutAndGet() throws Exception {
        String key = "key1";
        String value = "value1";
        KeyValueModel model = getKeyValue(key, value.getBytes());
        tarantoolService.put(model).get();
        KeyValueModel retrievedModel = tarantoolService.get(key).get();
        assertEquals(key, retrievedModel.getKey());
        assertEquals(value, new String(retrievedModel.getValue().toByteArray()));

        value = "value2";
        model = getKeyValue(key, value.getBytes());
        tarantoolService.put(model).get();
        retrievedModel = tarantoolService.get(key).get();
        assertEquals(key, retrievedModel.getKey());
        assertEquals(value, new String(retrievedModel.getValue().toByteArray()));
    }

    @Test
    void testDelete() throws Exception {
        String key = "keyToDelete";
        ByteString value = ByteString.copyFrom("valueToDelete".getBytes());
        KeyValueModel model = KeyValueModel.newBuilder().setKey(key).setValue(value).build();
        tarantoolService.put(model).get();
        IsDeleted isDeleted = tarantoolService.delete(key).get();
        assertTrue(isDeleted.getValue());
        assertThrows(Exception.class, () -> tarantoolService.get(key).get());
    }

    @Test
    void testCount() throws Exception {
        String key1 = "keyCount1";
        String key2 = "keyCount2";
        String value = "valueCount";
        tarantoolService.put(getKeyValue(key1, value.getBytes())).get();
        tarantoolService.put(getKeyValue(key2, value.getBytes())).get();
        CountResponse countResponse = tarantoolService.count().get();
        assertTrue(countResponse.getValue() >= 2);
    }

    @Test
    void testRange() throws Exception {
        tarantoolService.put(KeyValueModel.newBuilder().setKey("keyRange1").setValue(ByteString.copyFrom("value1".getBytes())).build()).get();
        tarantoolService.put(KeyValueModel.newBuilder().setKey("keyRange2").setValue(ByteString.copyFrom("value2".getBytes())).build()).get();
        tarantoolService.put(KeyValueModel.newBuilder().setKey("keyRange3").setValue(ByteString.copyFrom("value3".getBytes())).build()).get();
        tarantoolService.put(KeyValueModel.newBuilder().setKey("keyRange4").setValue(ByteString.copyFrom("value3".getBytes())).build()).get();
        tarantoolService.put(KeyValueModel.newBuilder().setKey("keyRange5").setValue(ByteString.copyFrom("value3".getBytes())).build()).get();

        Stream<KeyValueModel> rangeResult = tarantoolService.range("keyRange2", "keyRange4").get();

        // Assert
        assertEquals(3, rangeResult.count());
    }

//    @AfterEach
//    void cleanUp() throws Exception {
//        tarantoolService.clean();
//    }

}
