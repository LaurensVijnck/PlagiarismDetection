package models.coders;

import models.LSHPair;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VarLongCoder;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Created by Laurens on 12/10/19.
 */
public class LSHPairCustomCoder extends CustomCoder<LSHPair> {
    @Override
    public void encode(LSHPair LSHPair, OutputStream outputStream) throws CoderException, IOException {

        VarLongCoder varLongCoder = VarLongCoder.of();
        VarIntCoder varIntCoder = VarIntCoder.of();

        // Encode doc identifier
        varLongCoder.encode(LSHPair.getDocIdentifier(), outputStream);

        // Encode band identifier
        varIntCoder.encode(LSHPair.getBandId(), outputStream);
    }

    @Override
    public LSHPair decode(InputStream inputStream) throws CoderException, IOException {

        VarLongCoder varLongCoder = VarLongCoder.of();
        VarIntCoder varIntCoder = VarIntCoder.of();

        return new LSHPair(varLongCoder.decode(inputStream), varIntCoder.decode(inputStream));
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {

    }
}
