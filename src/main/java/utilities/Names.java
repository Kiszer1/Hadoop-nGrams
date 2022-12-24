package utilities;

import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.regions.Regions;


public class Names {

    public static final String BUCKET = "s3://yaad-nitzan-hadoop-bucket/";
    public static final String NAME = "yaad-nitzan-hadoop-bucket";

    public static final String INPUT = "s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-all/3gram/data";

    public static final String KEY = "Yaad-Nitzan-Key";
    public static final String INSTANCE_TYPE = InstanceType.M4Large.toString();
    public static final Regions REGION = Regions.US_EAST_1;
}
