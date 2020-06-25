package accumulators;

import models.MinHashSignature;
import models.coders.MinHashSignatureCustomCoder;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.transforms.Combine;

/**
 * Created by Laurens on 12/10/19.
 */
public class MinHashSignatureAccumulator {
    @DefaultCoder(AvroCoder.class)
    public static class ConsumerProfileCombiner extends Combine.CombineFn<MinHashSignature, MinHashSignature, MinHashSignature> {

        private final Integer numHashes;

        public ConsumerProfileCombiner(Integer numHashes) {
            this.numHashes = numHashes;
        }

        @Override
        public MinHashSignature createAccumulator() {
            return new MinHashSignature(numHashes);
        }

        @Override
        public MinHashSignature addInput(MinHashSignature accum, MinHashSignature signature) {
            return accum.resolveSignature(signature);
        }

        @Override
        public MinHashSignature mergeAccumulators(Iterable<MinHashSignature> accums) {
            MinHashSignature result = createAccumulator();
            for(MinHashSignature signature: accums) {
                result.resolveSignature(signature);
            }

            return result;
        }

        @Override
        public MinHashSignature extractOutput(MinHashSignature accum) {
            return accum;
        }

        @Override
        public Coder<MinHashSignature> getAccumulatorCoder(CoderRegistry registry, Coder<MinHashSignature> inputCoder) throws CannotProvideCoderException {
            return new MinHashSignatureCustomCoder();
        }
    }
}
