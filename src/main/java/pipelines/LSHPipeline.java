package pipelines;


import common.GenericLogger;
import models.LSHPair;
import models.MinHashSignature;
import models.coders.LSHPairCustomCoder;
import models.coders.MinHashSignatureCustomCoder;
import operations.DocumentSource;
import operations.LSHTransform;
import operations.ShingleConstructor;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pipelines.config.LSHPipelineOptions;

import java.util.List;
import java.util.regex.Pattern;

/**
 * Created by Laurens on 8/10/19.
 *
 * How to run?
 * -----------
 *  mvn compile -e exec:java -Dexec.mainClass=be.uhasselt.similar-items.pipelines.LSHPipeline
 *
 */
public class LSHPipeline {
    private static final Logger LOG = LoggerFactory.getLogger(LSHPipeline.class);

    public static class ReportResults extends DoFn<KV<MinHashSignature, Iterable<Long>>, Long> {
        @DoFn.ProcessElement
        public void processElement(@Element KV<MinHashSignature, Iterable<Long>> element, ProcessContext c) {
            List list = Lists.newArrayList(element.getValue());

            if(list.size() > 1) {
                LOG.info("{}", list.toString());
            }
        }
    }

    private static void assemblePipeline(Pipeline pipeline, LSHPipelineOptions options) {

        pipeline
                .apply("Ingest", new DocumentSource(options.getInputDirectory()))
                .apply("LogDocuments", ParDo.of(new GenericLogger<>()))
                .apply("Construct Shingles", ParDo.of(new ShingleConstructor(options.getShingleSize())))
                .apply("LogShingles", ParDo.of(new GenericLogger<>()))
                .apply("PerformLSH", new LSHTransform(options.getLSHNumBands(), options.getLSHNumRows()))
                .apply("LogResults", ParDo.of(new ReportResults()));
    }

    public static void main(String[] args) {

        // Parse arguments into options
        LSHPipelineOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .create()
                .as(LSHPipelineOptions.class);

        // Create pipeline object and assemble pipeline
        Pipeline p = Pipeline.create(options);

        // Register codes
        CoderRegistry cr = p.getCoderRegistry();
        cr.registerCoderForClass(LSHPair.class, new LSHPairCustomCoder());
        cr.registerCoderForClass(MinHashSignature.class, new MinHashSignatureCustomCoder());

        // Assemble pipeline
        assemblePipeline(p, options);

        // Set options
        options.setRunner(DataflowRunner.class);
        options.setJobName("lsh-pipeline-v7");
        options.setRegion("europe-west1");
        options.setZone("europe-west1-b");
        options.setProject("uhasselt-bda");
        options.setWorkerMachineType("n1-standard-1");

        // Run the pipeline
        p.run();
    }

    static String resolveFileName(String fileName) {
        if(fileName.contains(".")) {
            return fileName.split(Pattern.quote("."))[0];
        }
        return fileName;
    }
}