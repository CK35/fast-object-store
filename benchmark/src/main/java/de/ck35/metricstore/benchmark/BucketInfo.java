package de.ck35.metricstore.benchmark;

public class BucketInfo {

	private final String bucketName;
	private final String bucketType;
	
	public BucketInfo(String bucketName, String bucketType) {
		this.bucketName = bucketName;
		this.bucketType = bucketType;
	}
	public String getBucketName() {
		return bucketName;
	}
	public String getBucketType() {
		return bucketType;
	}
    @Override
    public String toString() {
        return "BucketInfo [bucketName=" + bucketName + ", bucketType=" + bucketType + "]";
    }
}