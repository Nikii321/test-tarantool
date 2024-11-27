package com.tarantooltest.tarantool.grpc;

import com.google.protobuf.Empty;
import com.tarantool.grpc.KeyValueServiceGrpc;
import com.tarantool.grpc.Scheme.*;
import com.tarantooltest.tarantool.service.TarantoolService;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.server.service.GrpcService;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.stream.Stream;

@GrpcService
@Slf4j
public class GrpcServiceImpl extends KeyValueServiceGrpc.KeyValueServiceImplBase {

    private final TarantoolService tarantoolService;

    @Autowired
    public GrpcServiceImpl(TarantoolService tarantoolService) {
        this.tarantoolService = tarantoolService;
    }

    @Override
    public void put(KeyValueModel request,
                    StreamObserver<KeyValueModel> responseObserver) {
        tarantoolService.put(request)
                .whenComplete((res, ex) -> whenComplete(res, ex, responseObserver));
    }

    @Override
    public void get(Key request,
                    StreamObserver<KeyValueModel> responseObserver) {
        tarantoolService.get(request.getKey())
                .whenComplete((res, ex) -> whenComplete(res, ex, responseObserver));
    }

    @Override
    public void delete(Key request,
                       StreamObserver<IsDeleted> responseObserver) {
        tarantoolService.delete(request.getKey())
                .whenComplete((res, ex) -> whenComplete(res, ex, responseObserver));
    }

    @Override
    public void range(RangeRequest request,
                      StreamObserver<KeyValueModel> responseObserver) {
        tarantoolService.range(request.getKeySince(), request.getKeyTo())
                .whenComplete((res, ex) -> whenComplete(res, ex, responseObserver));
    }

    @Override
    public void count(Empty request,
                      StreamObserver<CountResponse> responseObserver) {
        tarantoolService.count()
                .whenComplete((res, ex) -> whenComplete(res, ex, responseObserver));
    }

    private <T> void whenComplete(
            T res,
            Throwable ex,
            StreamObserver<T> streamObserver
    ) {
        if (ex != null) {
            log.error(ex.getMessage());
            streamObserver.onError(Status.INTERNAL.withCause(ex.getCause()).asRuntimeException());
        }
        streamObserver.onNext(res);
        streamObserver.onCompleted();

    }

    private <T> void whenComplete(
            Stream<T> res,
            Throwable ex,
            StreamObserver<T> streamObserver
    ) {
        if (ex != null) {
            log.error(ex.getMessage());
            streamObserver.onError(Status.INTERNAL.withCause(ex.getCause()).asRuntimeException());
        }
        res.forEach(streamObserver::onNext);
        streamObserver.onCompleted();

    }

}
