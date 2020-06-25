package operations;

import accumulators.MinHashSignatureAccumulator;
import com.google.common.hash.Hashing;
import common.GenericLogger;
import models.LSHPair;
import models.MinHashSignature;
import java.nio.charset.StandardCharsets;

import models.coders.LSHPairCustomCoder;
import models.coders.MinHashSignatureCustomCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

/**
 * Created by Laurens on 12/10/19.
 */
public class LSHTransform extends PTransform<PCollection<KV<Long, String>>, PCollection<KV<MinHashSignature, Iterable<Long>>>> {

    private final Integer LSHNumBands;
    private final Integer LSHNumRows;

    public LSHTransform(Integer LSHNumBands, Integer LSHNumRows) {
        this.LSHNumBands = LSHNumBands;
        this.LSHNumRows = LSHNumRows;
    }

    @Override
    public PCollection<KV<MinHashSignature, Iterable<Long>>> expand(PCollection<KV<Long, String>> input) {
         return input
                 .apply("HashFanOut", ParDo.of(new HashFanOut(LSHNumBands, LSHNumRows)))
                 .apply("LogHashes", ParDo.of(new GenericLogger<>()))
                 .apply("ReduceHashes", Combine.perKey(new MinHashSignatureAccumulator.ConsumerProfileCombiner(LSHNumRows))).setCoder(KvCoder.of(new LSHPairCustomCoder(), new MinHashSignatureCustomCoder()))
                 .apply("ExtractSignature", MapElements.via(new SignatureExtractor()))
                 .apply("LSH", GroupByKey.<MinHashSignature, Long>create());
    }

    private class SignatureExtractor extends SimpleFunction<KV<LSHPair, MinHashSignature>, KV<MinHashSignature, Long>> {
        public KV<MinHashSignature, Long> apply(KV<LSHPair, MinHashSignature> input) {
            MinHashSignature sig = input.getValue();
            sig.setBandId(input.getKey().getBandId());
            return KV.of(input.getValue(), input.getKey().getDocIdentifier());
        }
    }

    private class HashFanOut extends DoFn<KV<Long, String>, KV<LSHPair, MinHashSignature>> {
        private final Integer numHashesPerBand;
        private final Integer numBands;

        public HashFanOut(Integer numBands, Integer numHashesPerBand) {
            this.numBands = numBands;
            this.numHashesPerBand = numHashesPerBand;
        }

        @DoFn.ProcessElement
        public void processElement(@Element KV<Long, String> element, ProcessContext c) {
            for(int bandId = 1; bandId <= numBands; ++bandId) {
                MinHashSignature signature = new MinHashSignature(bandId, numHashesPerBand);

                for(int hashId = 1; hashId <= numHashesPerBand; ++hashId) {
                    signature.setHashValue(hashId - 1, computeHashValue(bandId, hashId, element.getValue()));
                }

                c.output(KV.of(new LSHPair(element.getKey(), bandId), signature));
            }
        }

        private Long computeHashValue(Integer bandId, Integer hashId, String input) {
            String newValue = input + (bandId * hashId);
            return Hashing.md5().hashString(newValue, StandardCharsets.UTF_8).asLong();
        }
    }
}
