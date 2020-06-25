package models;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/*
 * Class that represents the 'banded' MinHash signature of a document.
 * The order of the elements in the hashValues is important.
 */
public class MinHashSignature {
    private Integer bandId;
    private List<Long> hashValues;

    public MinHashSignature(Integer numHashes) {
        this.hashValues = new ArrayList<>(Collections.nCopies(numHashes, Long.MAX_VALUE));
    }

    public MinHashSignature(Integer bandId, Integer numHashes) {
        // Initialized to max value, i.e, infty.
        this.hashValues = new ArrayList<>(Collections.nCopies(numHashes, Long.MAX_VALUE));
        this.bandId = bandId;
    }

    public MinHashSignature(Integer bandId, List<Long> hashValues) {
        this.bandId = bandId;
        this.hashValues = hashValues;
    }

    public List<Long> getHashValues() {
        return this.hashValues;
    }

    public void setBandId(Integer bandId) {
        this.bandId = bandId;
    }

    public Integer getBandId() {
        return this.bandId;
    }

    public void setHashValue(Integer hashId, Long value) {
        this.hashValues.set(hashId, value);
    }

    public MinHashSignature resolveSignature(MinHashSignature signature) {
        for(int i = 0; i < signature.getHashValues().size(); ++i) {
            if(hashValues.get(i) > signature.hashValues.get(i)) {
                hashValues.set(i, signature.getHashValues().get(i));
            }
        }

        return this;
    }

    public String toString() {
        return this.bandId + " " + this.hashValues.toString();
    }
}
