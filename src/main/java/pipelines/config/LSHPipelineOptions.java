package pipelines.config;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;

/**
 * Created by Laurens on 8/10/19.
 */
public interface LSHPipelineOptions extends DataflowPipelineOptions {
    @Description("K - Length of the Singles to construct")
    @Validation.Required
    @Default.Integer(3)
    Integer getShingleSize();
    void setShingleSize(Integer value);

    @Description("Input directory")
    @Validation.Required
    @Default.String("gs://uhasselt-bda/documents/*")
    String getInputDirectory();
    void setInputDirectory(String value);

    @Description("Number of bands for LSH")
    @Validation.Required
    @Default.Integer(100)
    Integer getLSHNumBands();
    void setLSHNumBands(Integer value);

    @Description("Number of rows per LSH band")
    @Validation.Required
    @Default.Integer(10)
    Integer getLSHNumRows();
    void setLSHNumRows(Integer value);
}

