package spring;

import org.apache.logging.log4j.core.config.ConfigurationSource;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.runners.model.InitializationError;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.util.ResourceUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

public class JUnit4ClassRunner extends SpringJUnit4ClassRunner {
    static {  
            initLog4j2();  
    }  
      
    public JUnit4ClassRunner(Class<?> clazz) throws InitializationError{  
        super(clazz);  
    }  
    public static void initLog4j2(){
        try {
            File cfgFile = ResourceUtils.getFile("classpath:log4j2.xml");
            ConfigurationSource source = new ConfigurationSource(new FileInputStream(cfgFile));
            Configurator.initialize(null, source);  
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
