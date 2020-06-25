package models.coders;

import models.MinHashSignature;
import org.apache.beam.sdk.coders.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Created by Laurens on 12/10/19.
 */
public class MinHashSignatureCustomCoder extends CustomCoder<MinHashSignature> {
    @Override
    public void encode(MinHashSignature minHashSignature, OutputStream outputStream) throws CoderException, IOException {
        VarIntCoder varIntCoder = VarIntCoder.of();
        VarLongCoder varLongCoder = VarLongCoder.of();
        ListCoder<Long> listCoder = ListCoder.of(varLongCoder);
        NullableCoder<Integer> nullableIntCoder =  NullableCoder.of(varIntCoder);

        // Encode band
        nullableIntCoder.encode(minHashSignature.getBandId(), outputStream);

        // Encode hash values
        listCoder.encode(minHashSignature.getHashValues(), outputStream);
    }

    @Override
    public MinHashSignature decode(InputStream inputStream) throws CoderException, IOException {
        VarIntCoder varIntCoder = VarIntCoder.of();
        VarLongCoder varLongCoder = VarLongCoder.of();
        ListCoder<Long> listCoder = ListCoder.of(varLongCoder);
        NullableCoder<Integer> nullableIntCoder =  NullableCoder.of(varIntCoder);

        return new MinHashSignature(nullableIntCoder.decode(inputStream), listCoder.decode(inputStream));
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {

    }
}
