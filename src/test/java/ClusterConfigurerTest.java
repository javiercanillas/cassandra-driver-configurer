import com.datastax.driver.core.Cluster;
import com.github.javiercanillas.cassandra.driver.ClusterConfigurer;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import junit.framework.TestCase;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

import java.net.URL;

/**
 * Created by Kani on 03/08/14.
 */

@RunWith(BlockJUnit4ClassRunner.class)
public class ClusterConfigurerTest {

    @Test
    public void loadConfiguration01() {
        Config configuration = ConfigFactory.load(this.getClass().getClassLoader(), "configurations/configuration01.conf");
        TestCase.assertNotNull(configuration);
        Cluster cluster = ClusterConfigurer.buildFromConfig(configuration);
        TestCase.assertNotNull(cluster);
    }
}
