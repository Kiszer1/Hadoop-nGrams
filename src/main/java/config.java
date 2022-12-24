import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;

public class config {

    public static StepConfig stepConfig (String name, HadoopJarStepConfig jar, String action) {
        return new StepConfig()
                .withName(name)
                .withHadoopJarStep(jar)
                .withActionOnFailure(action);
    }

    public static HadoopJarStepConfig hadoopStepConfig (String jar, String[] args) {
        return new HadoopJarStepConfig()
                .withJar(jar)
                .withArgs(args);
    }
}
