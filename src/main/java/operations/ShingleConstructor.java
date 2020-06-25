package operations;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

/**
 * Created by Laurens on 8/10/19.
 */
public class ShingleConstructor extends DoFn<KV<Long,String>, KV<Long,String>> {

    private final Integer singleSize;

    public ShingleConstructor(Integer shingleSize) {
        this.singleSize = shingleSize;
    }

    @ProcessElement
    public void processElement(@Element KV<Long, String> document, ProcessContext c) {
        for(int i = 0; i < document.getValue().length() - singleSize; ++i) {
            c.output(KV.of(document.getKey(), document.getValue().substring(i, i + singleSize)));
        }
    }
}
