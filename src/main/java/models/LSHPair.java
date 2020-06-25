package models;

/**
 * Created by Laurens on 12/10/19.
 */
public class LSHPair {
    private Long docIdentifier;
    private Integer bandId;

    public LSHPair(Long docIdentifier, Integer bandId) {
        this.docIdentifier = docIdentifier;
        this.bandId = bandId;
    }

    public Long getDocIdentifier() {
        return docIdentifier;
    }

    public Integer getBandId() {
        return bandId;
    }

    @Override
    public String toString() {
        return this.docIdentifier.toString();
    }
}
