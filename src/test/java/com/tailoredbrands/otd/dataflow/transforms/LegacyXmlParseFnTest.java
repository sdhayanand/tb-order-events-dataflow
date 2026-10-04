package com.tailoredbrands.otd.dataflow.transforms;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.tailoredbrands.otd.dataflow.TestResources;
import com.tailoredbrands.otd.dataflow.model.LegacyOrder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class LegacyXmlParseFnTest {

  @Test
  public void parsesNamespacedExtractWithAllOrdersAndLines() throws Exception {
    List<LegacyOrder> orders =
        LegacyXmlParseFn.parse(
            TestResources.bytes("reconciliation/legacy/oms-extract-2026-10-03.xml"), "extract.xml");

    assertEquals(4, orders.size());
    LegacyOrder first = orders.get(0);
    assertEquals("ORD-2026-000001", first.orderNbr());
    assertEquals("TAILORED", first.orderType());
    assertEquals("0412", first.storeNbr());
    assertEquals("C-77812", first.custNbr());
    assertEquals("2026-10-03T14:02:11", first.orderDate());
    assertEquals(2, first.lines().size());
    assertEquals("ALT-HEM-TROUSER", first.lines().get(1).sku());
    assertEquals("ALTERATION", first.lines().get(1).fulfillType());
    assertNull(first.totalAmount());
    assertEquals(649.99, first.effectiveTotal(), 0.0001);
    assertEquals("extract.xml", first.sourceFile());

    LegacyOrder fourth = orders.get(3);
    assertEquals(3, fourth.lines().size());
    assertEquals(150.0, fourth.effectiveTotal(), 0.0001);
  }

  @Test
  public void explicitTotalAmountWinsOverLineSum() throws Exception {
    String xml =
        "<Orders><Order><OrderNbr>X-1</OrderNbr><StoreNbr>0001</StoreNbr><TotalAmount>99.5</TotalAmount>"
            + "<Lines><Line><LineNbr>1</LineNbr><SKU>A</SKU><Qty>2</Qty><Price>10</Price>"
            + "<FulfillType>STORE_PICKUP</FulfillType></Line></Lines></Order></Orders>";
    List<LegacyOrder> orders = LegacyXmlParseFn.parse(xml.getBytes(StandardCharsets.UTF_8), "x.xml");
    assertEquals(1, orders.size());
    assertEquals(99.5, orders.get(0).effectiveTotal(), 0.0001);
    assertEquals(20.0, orders.get(0).lines().get(0).lineAmount(), 0.0001);
  }

  @Test
  public void emptyExtractYieldsNoOrders() throws Exception {
    List<LegacyOrder> orders =
        LegacyXmlParseFn.parse("<Orders/>".getBytes(StandardCharsets.UTF_8), "empty.xml");
    assertEquals(0, orders.size());
  }
}
