package common;

import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generic logger transform that simply logs each input element. Transform is
 * used for debugging purposes.
 *
 * Created by Laurens Vijnck on 23/07/19.
 */
public class GenericLogger<T> extends DoFn<T, T> {

    private static final Logger LOG = LoggerFactory.getLogger(GenericLogger.class);

    @ProcessElement
    public void processElement(@Element T element, ProcessContext c) {
        LOG.info("{}", element.toString());
        c.output(element);
    }
}