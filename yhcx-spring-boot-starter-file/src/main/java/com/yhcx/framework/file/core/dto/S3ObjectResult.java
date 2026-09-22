package com.yhcx.framework.file.core.dto;

import com.amazonaws.services.s3.model.S3ObjectSummary;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class S3ObjectResult {

    private List<S3ObjectSummary> objectSummaries = new ArrayList<>();
    private List<String> commonPrefixes = new ArrayList<>();

    public S3ObjectResult() {
    }

    public S3ObjectResult(List<S3ObjectSummary> objectSummaries, List<String> commonPrefixes) {
        this.objectSummaries = objectSummaries;
        this.commonPrefixes = commonPrefixes;
    }
}
