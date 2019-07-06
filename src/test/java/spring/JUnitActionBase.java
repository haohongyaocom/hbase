package spring;

import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;

/**
 * JUnit测试action时使用的基类
 */
@RunWith(JUnit4ClassRunner.class)  
@ContextConfiguration({"classpath*:/spring/applicationContext-all.xml"}) 
public abstract class JUnitActionBase {

}