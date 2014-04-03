package pageTests;

import static org.junit.Assert.*;
import static org.junit.Assume.assumeTrue;

import java.util.Scanner;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import pages.ProcessPage;
import drivers.Global;
import static drivers.Global.*;

/**  ProcessPageTests provides sanity checks for the process page.
 * Checking existence of elements and texts
 * 
 * 
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ProcessPageTests extends GenericTest {
  static ProcessPage page;
  @BeforeClass
  public static void setUp(){
    page = new ProcessPage();
    Global.getDriver();
    globalDriver.get(page.getBaseUrl());

  }
  @Test
  public void test_02_H1() {
    Global.driverWait(5);
    assertTrue("Can't H1 title", page.isH1Present());
  }
  @Test
  public void test_03_H1Text() {
    assumeTrue(page.isH1Present());
    String strToMatch = Global.properties.getProperty("processH1");
    assertEquals(strToMatch, page.getH1().getText());
  }
  
  @Test 
  public void test_04_NoFlow() {
    assertTrue("Can't find 'No Flows' div", page.isNoContentDivPresent());
  }
  @Test
  public void test_05_TitleNoFlows() {
    Scanner scanner = new Scanner(page.getNoContentDiv().getText());
    String strToMatch = Global.properties.getProperty("noFlows");
    assertEquals(strToMatch, scanner.nextLine());
    strToMatch = Global.properties.getProperty("addFlows");
    assertEquals(strToMatch, scanner.nextLine());
  }
  @AfterClass
  public static void tearDown() {
    closeDriver();
  }
  

}
