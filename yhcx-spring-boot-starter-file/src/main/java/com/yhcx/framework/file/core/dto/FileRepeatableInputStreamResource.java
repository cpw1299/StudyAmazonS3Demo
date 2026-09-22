package com.yhcx.framework.file.core.dto;

import org.springframework.core.io.AbstractResource;

import java.io.InputStream;
import java.util.function.Supplier;

public class FileRepeatableInputStreamResource extends AbstractResource {

    private final Supplier<InputStream> streamSupplier;

    public FileRepeatableInputStreamResource(Supplier<InputStream> streamSupplier) {
        this.streamSupplier = streamSupplier;
    }

    @Override
    public InputStream getInputStream() {
        return streamSupplier.get();
    }

    @Override
    public String getDescription() {
        return "Repeatable S3 Object Stream";
    }
}
