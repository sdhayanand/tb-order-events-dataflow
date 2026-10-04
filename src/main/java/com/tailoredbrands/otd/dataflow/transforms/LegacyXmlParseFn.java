package com.tailoredbrands.otd.dataflow.transforms;

import com.tailoredbrands.otd.dataflow.model.LegacyLine;
import com.tailoredbrands.otd.dataflow.model.LegacyOrder;
import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;
import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * Parses one legacy OMS extract file ({@code <Orders><Order>...</Order></Orders>}) with the JDK DOM
 * parser into {@link LegacyOrder}s. Namespace-agnostic (matches local names), DTD/external entities
 * disabled. Extract files are small daily dumps, so reading each file fully is fine; a very large
 * extract would call for a StAX streaming parse with {@code SplittableDoFn}-style restrictions.
 *
 * <p>We intentionally do not use {@code XmlIO}: it requires JAXB-annotated classes generated from
 * the XSD, which would pull jaxb2-maven-plugin into a project that otherwise has no code generation.
 */
public class LegacyXmlParseFn extends DoFn<FileIO.ReadableFile, LegacyOrder> {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(LegacyXmlParseFn.class);

  private final Counter filesParsed = Metrics.counter("otd.reconciliation", "legacy_files_parsed");
  private final Counter ordersParsed = Metrics.counter("otd.reconciliation", "legacy_orders_parsed");
  private final Counter filesFailed = Metrics.counter("otd.reconciliation", "legacy_files_failed");

  @ProcessElement
  public void processElement(ProcessContext c) {
    FileIO.ReadableFile file = c.element();
    String fileName = file.getMetadata().resourceId().toString();
    byte[] bytes;
    try {
      bytes = file.readFullyAsBytes();
    } catch (Exception e) {
      filesFailed.inc();
      LOG.error("Cannot read legacy extract {}: {}", fileName, e.getMessage());
      return;
    }
    List<LegacyOrder> orders;
    try {
      orders = parse(bytes, fileName);
    } catch (Exception e) {
      filesFailed.inc();
      LOG.error("Cannot parse legacy extract {}: {}", fileName, e.getMessage());
      return;
    }
    filesParsed.inc();
    for (LegacyOrder o : orders) {
      ordersParsed.inc();
      c.output(o);
    }
  }

  /** Pure parsing function, package-visible for unit tests. */
  static List<LegacyOrder> parse(byte[] xml, String sourceFile) throws Exception {
    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    factory.setNamespaceAware(true);
    factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
    factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
    factory.setFeature("http://xml.org/sax/features/external-general-entities", false);
    factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
    factory.setXIncludeAware(false);
    factory.setExpandEntityReferences(false);
    DocumentBuilder builder = factory.newDocumentBuilder();
    Document doc = builder.parse(new ByteArrayInputStream(xml));

    List<LegacyOrder> orders = new ArrayList<>();
    NodeList orderNodes = doc.getElementsByTagNameNS("*", "Order");
    for (int i = 0; i < orderNodes.getLength(); i++) {
      org.w3c.dom.Element order = (org.w3c.dom.Element) orderNodes.item(i);
      List<LegacyLine> lines = new ArrayList<>();
      NodeList lineNodes = order.getElementsByTagNameNS("*", "Line");
      for (int j = 0; j < lineNodes.getLength(); j++) {
        org.w3c.dom.Element line = (org.w3c.dom.Element) lineNodes.item(j);
        lines.add(
            new LegacyLine(
                parseInt(childText(line, "LineNbr")),
                childText(line, "SKU"),
                parseInt(childText(line, "Qty")),
                parseDouble(childText(line, "Price")),
                childText(line, "FulfillType")));
      }
      String total = childText(order, "TotalAmount");
      if (total == null) {
        total = childText(order, "TotalAmt");
      }
      orders.add(
          new LegacyOrder(
              childText(order, "OrderNbr"),
              childText(order, "OrderType"),
              childText(order, "StoreNbr"),
              childText(order, "CustNbr"),
              childText(order, "OrderDate"),
              parseDouble(total),
              lines,
              sourceFile));
    }
    return orders;
  }

  /** Text of the first direct child element with the given local name, or null. */
  private static String childText(org.w3c.dom.Element parent, String localName) {
    NodeList children = parent.getChildNodes();
    for (int i = 0; i < children.getLength(); i++) {
      Node n = children.item(i);
      if (n.getNodeType() == Node.ELEMENT_NODE && localName.equals(n.getLocalName())) {
        String text = n.getTextContent();
        return text == null ? null : text.trim();
      }
    }
    return null;
  }

  private static Integer parseInt(String s) {
    if (s == null || s.isBlank()) {
      return null;
    }
    return Integer.valueOf(s.trim());
  }

  private static Double parseDouble(String s) {
    if (s == null || s.isBlank()) {
      return null;
    }
    return Double.valueOf(s.trim());
  }
}
