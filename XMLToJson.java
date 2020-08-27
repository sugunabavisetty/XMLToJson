package com.mercury.hadoop.converter;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ResourceBundle;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathFactory;

import org.apache.derby.tools.sysinfo;
import org.datanucleus.store.types.simple.LinkedList;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.mercury.hadoop.utils.MercuryUtils;

/**
 * @author Systech Solutions Inc.,
 *
 */

public class XMLToJson
{
    public String nodesToExclude = MercuryUtils.nodesToExclude;
    ArrayList ar = new ArrayList();
    ArrayList ar2 = new ArrayList();
    String nonCustomRCs = new String();
    String delimiter = ",";
    public String excludedFields=MercuryUtils.excludedFields;
    PrintWriter pw;
    //Global Incremental Variables:
    public static int generated_id1 = 1; //grandparent incremental generator
    public static int generated_id2 = 1; //parent incremental generator
    public static int generated_id3 = 1; //child incremental generator
    //Residential Report ID counter... Increments count each time a new RESIDENTIAL_REPORT tag is called.
    //public static int res_report_id = 1;
   
    public static HashMap<String, String> h_gen_id = new HashMap<String,String>();  //Child Hashmap to store <gen_<node_name>_id, value>
    HashMap<String, String> h_gen_id2 = new HashMap<String,String>(); //Parent Hashmap to store <gen_parent_<node_name>_id, value from h_gen_id>
    HashMap<String, String> h_gen_id3= new HashMap<String,String>();  //Grand Parent Hashmap to store <gen_grand_parent_<node_name>_id, value from h_gen_id>
   
    public static void main(String[] args) throws Exception
    {
        String sourceSystem="gw";
        String vendor="pc";
        String lob ="ho";
        XMLToJson a = new XMLToJson();
//        FileInputStream fi = new FileInputStream("C:\\Users\\SBavisetty\\Documents\\XMLDirect\\jar_files\\XMLFiles\\vendorxml1.xml");
//        //Looping through xml files
//       
//        byte[] b = new byte[fi.available()];
//        fi.read(b);
//        String s = new String(b);
//        System.out.println(a.getJSON(s, sourceSystem, vendor, lob));
       
        //Looping through a directory of XMLFiles
        //Test whether generated_id3 increments for next XML File
        File path = new File("C:\\Users\\SBavisetty\\Documents\\XMLDirect\\jar_files\\XMLFiles");

        File [] files = path.listFiles();
        for (int i = 0; i < files.length; i++){
            if (files[i].isFile()){ //this line weeds out other directories/folders
                System.out.println(files[i]);
                FileInputStream fis = new FileInputStream(files[i]);
                byte[]bytes = new byte[fis.available()];
                fis.read(bytes);
                String str = new String(bytes);
               
                System.out.println(a.getJSON(str, sourceSystem, vendor, lob));
                String res_id = h_gen_id.get("residential_report");
//                if (((Object) h_gen_id).getKey()=="residential_report"){
//                    generated_id3 = i+2;
//                   
//                }
//                else
                    //
                    generated_id3=2;
                //incrementing next xml file residential_id
                //generated_id3=i+2;
            }
           
        }
       
       

    }
    /**
     *This method is used to get node information  from the given xpath string
     *
     */

    public ArrayList getJSON(String xmlString,String source_system, String vendor, String lob) throws Exception
    {

        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        dbf.setValidating(false);
        DocumentBuilder db = dbf.newDocumentBuilder();
        ByteArrayInputStream bi = new ByteArrayInputStream(xmlString.getBytes());
        Document doc = db.parse(bi);
        String expression = MercuryUtils.expression;
        XPath xPath = XPathFactory.newInstance().newXPath();
        NodeList nl1 = (NodeList)xPath.compile(expression).evaluate(doc, XPathConstants.NODESET);
        for (int i = 0; i < nl1.getLength(); i++)
        {
            Node n1 = nl1.item(i);

            if(source_system.equalsIgnoreCase("pmis") && vendor.equalsIgnoreCase("dc"))
            {
                getJsonNodes(n1,source_system,vendor,lob);
            }
            else if(source_system.equalsIgnoreCase("gw") && vendor.equalsIgnoreCase("pc"))
            {
                getJSONNodes(n1,source_system,vendor,lob);
            }
        }
        return this.ar;
    }
    /**
     * This method is to get recursive tag information and also it skips some tags which has mentioned as nodesToExclude
     *
     */
    public void getJsonNodes(Node n,String source_system, String vendor, String lob) throws Exception
    {
        String nodeName = n.getNodeName().toLowerCase();
        if(nodeName != null && nodeName.equalsIgnoreCase("coverage") && source_system.equalsIgnoreCase("pmis") && vendor.equalsIgnoreCase("dc") && lob.equalsIgnoreCase("ppa"))
        {
            Node parentNode = n.getParentNode();
            String parentNodeName = null;
            String parentNodeID=null;
            if(parentNode  != null )
            {
                parentNodeName=parentNode.getNodeName();
                parentNodeID=parentNode.getAttributes().getNamedItem("id").getTextContent();
                if(parentNodeID==null) parentNodeID="noid";
            }
            if(!parentNodeID.equalsIgnoreCase("noid") && nonCustomRCs.indexOf("~"+parentNodeID+"~") != -1)
                return;
            if(parentNodeName != null && parentNodeName.equalsIgnoreCase("riskcoverages"))
            {

                NodeList typeNodes = parentNode.getChildNodes();
                for(int i=0;i<typeNodes.getLength();i++)
                {

                    Node oneNode = typeNodes.item(i);
                    if(oneNode != null && oneNode.getNodeName() != null && oneNode.getNodeName().equalsIgnoreCase("type"))
                    {
                        String typeValue = oneNode.getTextContent();
                        if(typeValue != null && ( !typeValue.equalsIgnoreCase("custom") && !typeValue.equalsIgnoreCase("custommp")))
                        {
                            nonCustomRCs= nonCustomRCs + "~"+parentNodeID+"~";
                            return;
                        }
                        else
                            break;
                    }
                }
            }
        }

        if(nodeName != null && nodeName.equalsIgnoreCase("riskcoverages") && source_system.equalsIgnoreCase("pmis") && vendor.equalsIgnoreCase("dc") && lob.equalsIgnoreCase("ppa"))
        {
            String rcNodeID=null;
            rcNodeID=n.getAttributes().getNamedItem("id").getTextContent();
            if(rcNodeID==null) rcNodeID="noid";

            if(!rcNodeID.equalsIgnoreCase("noid") && nonCustomRCs.indexOf("~"+rcNodeID+"~") != -1)
                return;
            NodeList typeNodes = n.getChildNodes();
            for(int i=0;i<typeNodes.getLength();i++)
            {
                Node oneNode = typeNodes.item(i);
                if(oneNode != null && oneNode.getNodeName() != null && oneNode.getNodeName().equalsIgnoreCase("type"))
                {
                    String typeValue = oneNode.getTextContent();
                    if(typeValue != null && ( !typeValue.equalsIgnoreCase("custom") && !typeValue.equalsIgnoreCase("custommp") ))
                    {
                        nonCustomRCs= nonCustomRCs + "~"+rcNodeID+"~";
                        return;
                    }
                    else
                        break;
                }
            }

        }
        if(nodeName == null || nodeName.equalsIgnoreCase("id"))
            return;
        if (((nodeName.endsWith("rq")) || (nodeName.endsWith("request")) /*|| (nodeName.endsWith("rs"))*/ || (nodeName.endsWith("response")) || (nodeName.endsWith("repsonses")) || (nodeName.endsWith("responses")) || (nodeName.endsWith("responsedriver")) || (nodeName.endsWith("responsevehicle"))) && (!nodeName.equals("indicators"))) {
            return;
        }
        if(nodesToExclude.indexOf(","+replaceDot(nodeName)+",") != -1)
            return;
        HashMap<String, String> h = new HashMap();
        String subNodes = "";
        NodeList nl1 = n.getChildNodes();
        h.put(replaceDot(n.getNodeName()), "");
        if(n.getAttributes()!= null)
        {
            for(int i=0;i<n.getAttributes().getLength();i++)
            {
                String attributeName = n.getAttributes().item(i).getNodeName();
                String attributeValue = n.getAttributes().item(i).getTextContent().replaceAll("\"", "").replace("{", "").replace("}","").replaceAll("[^\\p{Print}]","").replaceAll("~", "");
                if(!attributeName.equalsIgnoreCase("id"))
                    subNodes = subNodes+ delimiter + "\""+ columnRename(attributeName) +"\":\""+attributeValue + "\"";
            }
        }
        if ((n.getAttributes() != null) && (n.getAttributes().getNamedItem("id") != null) && (n.getAttributes().getNamedItem("id").getTextContent() != null))
        {
            String id = n.getAttributes().getNamedItem("id").getTextContent();
            if (id.trim().length() > 0) {
                subNodes = subNodes + delimiter+"\"" + columnRename(n.getNodeName()+"_id") + "\":\"" + id + "\"";
            }
        }

        Node n1 = n.getParentNode();
        if ((n1 != null) && (n1.getAttributes() != null) && (n1.getAttributes().getNamedItem("id") != null) && (n1.getAttributes().getNamedItem("id").getTextContent() != null))
        {
            String id = n1.getAttributes().getNamedItem("id").getTextContent();
            if (id.trim().length() > 0) {
                subNodes = subNodes + delimiter+"\"" + columnRename("parent_"+n1.getNodeName()+"_id") + "\":\"" + id + "\"";
            }
        }

        if(n.getNodeName() != null)
        {
            Node parentNode = n.getParentNode();
            String parentNodeName = null;
            String parentNodeID=null;
            Node grandParentNode = n.getParentNode().getParentNode();
            String grandParentNodeName = null;
            String grandParentNodeID=null;
            if ((parentNode != null) && (parentNode.getAttributes() != null) && (parentNode.getAttributes().getNamedItem("id") != null) && (parentNode.getAttributes().getNamedItem("id").getTextContent() != null))       
            {
                parentNodeName=parentNode.getNodeName();
                parentNodeID=parentNode.getAttributes().getNamedItem("id").getTextContent();
                if(parentNodeID==null) parentNodeID="noid";
                grandParentNode = n.getParentNode().getParentNode();
                if ((grandParentNode != null) && (grandParentNode.getAttributes() != null) && (grandParentNode.getAttributes().getNamedItem("id") != null) && (grandParentNode.getAttributes().getNamedItem("id").getTextContent() != null))
                {
                    grandParentNodeName=grandParentNode.getNodeName();
                    grandParentNodeID=grandParentNode.getAttributes().getNamedItem("id").getTextContent();
                    if(grandParentNodeID==null) grandParentNodeID="noid";
                    if(grandParentNodeID.trim().length() > 0) {
                        subNodes = subNodes + delimiter+"\"" + columnRename("grand_parent_"+grandParentNodeName+"_id") + "\":\"" + grandParentNodeID + "\"";
                    }
                }

            }
        }

        if(nodeName != null && nodeName.equalsIgnoreCase("messages") && source_system.equalsIgnoreCase("pmis") && vendor.equalsIgnoreCase("dc") && lob.equalsIgnoreCase("ho"))
        {
            NodeList a = n.getChildNodes();
            for(int i=0;i<a.getLength();i++)
            {
                Node b = a.item(i);
                String nodeName2 = replaceDot(b.getNodeName());
                String txtContent = b.getTextContent();
                txtContent= txtContent.replaceAll("\"","").replace("{","").replace("}","").replaceAll("[^\\p{Print}]","").replaceAll("~", "");
                String output ="";
                output = subNodes + delimiter + "\"" + columnRename(nodeName2)+"\":\""+txtContent+"\"";
                if((b.getAttributes() != null) && (b.getAttributes().getNamedItem("id") != null) && (b.getAttributes().getNamedItem("id").getTextContent() != null))
                {
                    String id = b.getAttributes().getNamedItem("id").getTextContent();
                    if (id.trim().length() > 0) {
                        output = output + delimiter+"\"" + columnRename(b.getNodeName()+"_id") + "\":\"" + id + "\"";
                    }
                }
                HashMap<String, String> m = new HashMap();
                if(output.length()>1)
                {
                    m.put(replaceDot(n.getNodeName()),output.substring(1, output.length())+"}");
                    this.ar.add(m);
                }
            }
            return;
        }

        for (int i = 0; i < nl1.getLength(); i++)
        {
            n1 = nl1.item(i);
            Node firstChild = n1.getFirstChild();
            boolean flag=true;
            if (((n1.getFirstChild() == null) || (!n1.getFirstChild().hasChildNodes())) )
                flag=false;
            if(n1.getFirstChild() != null && n1.getFirstChild().getNodeName() != null && n1.getFirstChild().getNodeType() == Node.ELEMENT_NODE)
                flag=true;
            if(!flag) 
            {
                String txtContent=n1.getTextContent();
                txtContent = txtContent.replaceAll("\"", "").replace("{", "").replace("}","").replaceAll("[^\\p{Print}]","").replaceAll("~", "");
                String nodeName2=replaceDot(n1.getNodeName());
                if ((txtContent != null) && (txtContent.length() > 0) && nodeName2 != null) {
                    if(excludedFields.indexOf("~"+nodeName2+"~") == -1 && !nodeName2.equalsIgnoreCase("id") && nodesToExclude.indexOf(","+nodeName2+",") == -1)
                        subNodes = subNodes + delimiter + "\"" + columnRename(nodeName2)+ "\":\"" + txtContent + "\"";
                }
            }
            else {
                getJsonNodes(n1,source_system,vendor,lob);
            }
        }

        if (subNodes.length() > 1) {
            h.put(replaceDot(n.getNodeName()), subNodes.substring(1, subNodes.length()) + "}");
        }
        this.ar.add(h);
    }

    public String replaceDot(String name) throws Exception
    {
        if(name == null)
            return null;
        else
        {
            name=name.replaceAll("\\.", "_").toLowerCase();
            name=name.replaceAll("#", "hash_");
            if(name.startsWith("_"))
                name=name.substring(1,name.length());
            return name;
        }
    }

    public String columnRename(String column) throws Exception
    {
        if(column == null)
            return null;
        else
        {
            column=column.replaceAll("\\.", "_").toLowerCase();
            column=column.replaceAll("#", "hash_");
            if(column.startsWith("_"))
                column=column.substring(1,column.length());
            return new MercuryUtils().getSchema(column);
        }
    }

    //Modify below function only
    //Concept of Java Enhancement: Add 3 new columns to the existing hashmap, gen_<node_name>_id, gen_parent_<node_name>_id and gen_grand_parent_<node_name>_id
    //There will be scenarios where not all parent and grandparent nodes will be output to the final string
    //The base node, Context, should not be in the string as an id name, in any case
    //Highest parent/grandparent/node is Residential Report
   
    public void getJSONNodes(Node n, String source_system, String vendor, String lob) throws Exception
    {
        String nodeName = n.getNodeName().toLowerCase();
        if(nodeName == null || nodeName.equalsIgnoreCase("id"))
            return;
        if(nodesToExclude.indexOf(","+replaceDot(nodeName)+",") != -1)
            return;
        HashMap<String, String> h = new HashMap<String,String>();
        //h_gen_id is created to store <gen_nodename_id, generated_id3> as the <key, value> pair.

        String subNodes = "";
        NodeList nl1 = n.getChildNodes();
        //Gets all attributes of the tag.
        if(n.getAttributes()!= null)
        {
            for(int i=0;i<n.getAttributes().getLength();i++)
            {
                String attributeName = n.getAttributes().item(i).getNodeName();
                String attributeValue = n.getAttributes().item(i).getTextContent().replaceAll("\"", "").replace("{", "").replace("}","").replaceAll("[^\\p{Print}]","").replaceAll("~", "");
                if(!attributeName.equalsIgnoreCase("id")) {
                    subNodes = subNodes+ delimiter + "\""+ columnRename(attributeName) +"\":\""+attributeValue + "\"";
                //+ delimiter + "\""+ columnRename("gen_parent_"+n.getParentNode().getNodeName()+"_id") +"\":\""+generated_id2 + "\"";
                    //Add gen_<nodename>_id to this string for tagnames without id element.
                    //System.out.println("Child Node Generator: " + generated_id3);
                    //generated_id3 = generated_id3+1;
                }
            }
        }
        //You don't want to return a grandparent that equals context... adding that statement to this if-clause to handle this situation
        if ((n.getAttributes() != null) && (n.getAttributes().getNamedItem("id") != null) && (n.getAttributes().getNamedItem("id").getTextContent() != null) && (!n.getParentNode().getParentNode().getNodeName().toLowerCase().matches("context")) && (!n.getParentNode().getNodeName().toLowerCase().matches("context")))
        {
            //System.out.println(n.getParentNode().getParentNode().getNodeName().toLowerCase());
            String id = n.getAttributes().getNamedItem("id").getTextContent().replaceAll("\"", "").replace("{", "").replace("}","").replaceAll("[^\\p{Print}]","");
            if (id.trim().length() > 0) {
                //Load all hashmaps (child, parent and grandparent hashmaps based on value from child map(h_gen_id))
                h_gen_id.put("gen_"+replaceDot(n.getNodeName())+"_id", ""+generated_id3);
                h_gen_id2.put("gen_"+replaceDot(n.getParentNode().getNodeName())+"_id", h_gen_id.get("gen_"+replaceDot(n.getParentNode().getNodeName())+"_id"));           
                h_gen_id3.put("gen_"+replaceDot(n.getParentNode().getParentNode().getNodeName())+"_id", h_gen_id.get("gen_"+replaceDot(n.getParentNode().getParentNode().getNodeName())+"_id"));                               
                subNodes = subNodes + delimiter+"\"" + columnRename(n.getNodeName()+"_id") + "\":\"" + id + "\""+delimiter+"\"" + columnRename("gen_"+n.getNodeName()+"_id") + "\":\"" + generated_id3 + "\""+delimiter+"\"" + columnRename("gen_parent_"+n.getParentNode().getNodeName()+"_id") + "\":\"" + h_gen_id2.get("gen_"+replaceDot(n.getParentNode().getNodeName())+"_id") + "\""+delimiter+"\"" + columnRename("gen_grand_parent_"+n.getParentNode().getParentNode().getNodeName()+"_id") + "\":\"" + h_gen_id2.get("gen_"+replaceDot(n.getParentNode().getParentNode().getNodeName())+"_id") + "\"";
               
                //subNodes = subNodes + delimiter+"\"" + columnRename(n.getNodeName()+"_id") + "\":\"" + id + "\""+delimiter+"\"" + columnRename("gen_"+n.getNodeName()+"_id") + "\":\"" + generated_id3 + "\""+delimiter+"\"" + columnRename("gen_parent_"+n.getParentNode().getNodeName()+"_id") + "\":\"" + generated_id2 + "\""+delimiter+"\"" + columnRename("gen_grand_parent_"+n.getParentNode().getParentNode().getNodeName()+"_id") + "\":\"" + generated_id1 + "\"";
            }
           
            generated_id3 = generated_id3+1;

        }
        //handling cases where grandparent id = context
        else if ((n.getAttributes() != null) && (n.getAttributes().getNamedItem("id") != null) && (n.getAttributes().getNamedItem("id").getTextContent() != null)&& (n.getParentNode().getParentNode().getNodeName().toLowerCase().matches("context"))) {
            //System.out.println(n.getParentNode().getParentNode().getNodeName());
            String id = n.getAttributes().getNamedItem("id").getTextContent().replaceAll("\"", "").replace("{", "").replace("}","").replaceAll("[^\\p{Print}]","");
            if (id.trim().length() > 0) {
                h_gen_id.put("gen_"+replaceDot(n.getNodeName())+"_id", ""+generated_id3);
                h_gen_id2.put("gen_"+replaceDot(n.getParentNode().getNodeName())+"_id", h_gen_id.get("gen_"+replaceDot(n.getParentNode().getNodeName())+"_id"));               
                subNodes = subNodes + delimiter+"\"" + columnRename(n.getNodeName()+"_id") + "\":\"" + id + "\""+delimiter+"\"" + columnRename("gen_"+n.getNodeName()+"_id") + "\":\"" + generated_id3 + "\""+delimiter+"\"" + columnRename("gen_parent_"+n.getParentNode().getNodeName()+"_id") + "\":\"" + h_gen_id2.get("gen_"+replaceDot(n.getParentNode().getNodeName())+"_id") + "\"";
               
            }
            //h_gen_id.put("gen_"+replaceDot(n.getNodeName())+"_id", ""+generated_id3);
            generated_id3 = generated_id3+1;
        }
        //If Parent Node equals context, concatenate only until node name
        else if ((n.getAttributes() != null) && (n.getAttributes().getNamedItem("id") != null) && (n.getAttributes().getNamedItem("id").getTextContent() != null)&& (n.getParentNode().getNodeName().toLowerCase().matches("context")))  {
            //System.out.println(n.getParentNode().getParentNode().getNodeName());
            String id = n.getAttributes().getNamedItem("id").getTextContent().replaceAll("\"", "").replace("{", "").replace("}","").replaceAll("[^\\p{Print}]","");
            if (id.trim().length() > 0) {
                h_gen_id.put("gen_"+replaceDot(n.getNodeName())+"_id", ""+generated_id3);
                h_gen_id2.put("gen_"+replaceDot(n.getParentNode().getNodeName())+"_id", h_gen_id.get("gen_"+replaceDot(n.getParentNode().getNodeName())+"_id"));               
                subNodes = subNodes + delimiter+"\"" + columnRename(n.getNodeName()+"_id") + "\":\"" + id + "\""+delimiter+"\"" + columnRename("gen_"+n.getNodeName()+"_id") + "\":\"" + generated_id3 + "\"";
            }
           
            generated_id3 = generated_id3+1;
        }
        //This handles Residential Report tag since there is no id here and the parent tag is context
        else if ((n.getParentNode().getNodeName().toLowerCase().matches("context"))&&(n.getNodeName().toLowerCase().matches("residential_report")))  {
            //System.out.println(n.getParentNode().getParentNode().getNodeName());
            h_gen_id.put("gen_"+replaceDot(n.getNodeName())+"_id", ""+generated_id3);
            h_gen_id2.put("gen_"+replaceDot(n.getParentNode().getNodeName())+"_id", h_gen_id.get("gen_"+replaceDot(n.getParentNode().getNodeName())+"_id"));           
            subNodes = subNodes + delimiter+"\"" + columnRename("gen_"+n.getNodeName()+"_id") + "\":\"" + generated_id3 + "\"";
           
            generated_id3 = generated_id3+1;
        }
        else {
            //For cases where id element does not exist: ex: Owner Information
            h_gen_id.put("gen_"+replaceDot(n.getNodeName())+"_id", ""+generated_id3);
            h_gen_id2.put("gen_"+replaceDot(n.getParentNode().getNodeName())+"_id", h_gen_id.get("gen_"+replaceDot(n.getParentNode().getNodeName())+"_id"));
            subNodes = subNodes+ delimiter + "\""+ columnRename("gen_"+n.getNodeName()+"_id") +"\":\""+generated_id3 + "\""+delimiter+"\"" + columnRename("gen_parent_"+n.getParentNode().getNodeName()+"_id") + "\":\"" + h_gen_id2.get("gen_"+replaceDot(n.getParentNode().getNodeName())+"_id") + "\"";;
            //subNodes = subNodes+ delimiter + "\""+ columnRename("gen_"+n.getNodeName()+"_id") +"\":\""+generated_id3 + "\""+ delimiter + "\""+ columnRename("gen_parent_"+n.getParentNode().getNodeName()+"_id") +"\":\""+h_gen_id.get("gen_"+replaceDot(n.getParentNode().getNodeName())+"_id") +"\"";
           
            generated_id3 = generated_id3+1;
           
        }
       
        //System.out.println(h_gen_id);
        //h_gen_id2.put("gen_"+replaceDot(n.getParentNode().getNodeName())+"_id", h_gen_id.get("gen_"+replaceDot(n.getParentNode().getNodeName())+"_id"));
       

        //Some Test Cases
       
//        Iterator<Map.Entry<String, String>> iterator = h_gen_id.entrySet().iterator();
//        Iterator<Map.Entry<String, String>> iterator2 = h_gen_id2.entrySet().iterator();
//        while(iterator.hasNext()){
//           Map.Entry<String, String> entry = iterator.next();
//           Map.Entry<String, String> entry2 = iterator2.next();
//          
//           //Check the generated id from h_gen_id2 (The Parent Hashmap)
//           if (h_gen_id.containsKey(entry.getKey())){
//               //String gen_key_id = h_gen_id2.put("gen_"+replaceDot(n.getNodeName())+"_id",h_gen_id.get("gen_"+replaceDot(n.getNodeName())+"_id"));
//               //System.out.println("Value for h_gen_id: "+ h_gen_id.get("gen_"+replaceDot(n.getNodeName())+"_id"));
//          
//           }
//           System.out.printf("Key : %s and Value: %s %n",entry2.getKey(),entry2.getValue());
//           //subNodes = subNodes + delimiter +"\"" + columnRename("gen_parent_"+n.getParentNode().getNodeName()+"_id") + "\":\"" + entry.getValue()+"\"";
//           //System.out.println(subNodes);
//           //iterator.remove(); // right way to remove entries from Map,
//           // avoids ConcurrentModificationException
//        }
//       
       
        //Test a particular key
       
        //subNodes = subNodes + delimiter +"\"" + columnRename("gen_parent_"+n.getParentNode().getNodeName()+"_id") + "\":\"" + h_gen_id.get("gen_"+replaceDot(n.getParentNode().getNodeName())+"_id")+"\"";
       
        //subNodes = subNodes + delimiter +"\"" + columnRename("gen_parent_"+n.getParentNode().getNodeName()+"_id") + "\":\"" + h_gen_id.get("gen_"+replaceDot(n.getParentNode().getNodeName())+"_id")+"\"";   
        //System.out.println(h_gen_id.get("gen_answer_id"));
        //System.out.println("gen_"+replaceDot(n.getParentNode().getNodeName())+"_id");
        //End of Parent Node Loop
       
       
/*        Iterator<Map.Entry<String, String>> iterator = h_gen_id.entrySet().iterator();
        while(iterator.hasNext()){
           Map.Entry<String, String> entry = iterator.next();
           System.out.printf("Key : %s and Value: %s %n", entry.getKey(), entry.getValue());
           subNodes = subNodes + delimiter +"\"" + columnRename("gen_parent_"+n.getParentNode().getNodeName()+"_id") + "\":\"" + entry.getValue()+"\"";
           System.out.println(subNodes);
           //iterator.remove(); // right way to remove entries from Map,
                              // avoids ConcurrentModificationException
        }*/
       
/*----------------------------------------------------------------------------------------*/
        //Looping through Parent Nodes
        Node n1 = n.getParentNode();
        if ((n1 != null) && (n1.getAttributes() != null) && (n1.getAttributes().getNamedItem("id") != null) && (n1.getAttributes().getNamedItem("id").getTextContent() != null) && (!n.getParentNode().getParentNode().getNodeName().toLowerCase().matches("context")) && (!n.getParentNode().getNodeName().toLowerCase().matches("context")))
        {
            String id = n1.getAttributes().getNamedItem("id").getTextContent().replaceAll("\"", "").replace("{", "").replace("}","").replaceAll("[^\\p{Print}]","");
            if (id.trim().length() > 0) {
                generated_id2 = generated_id2+1;
                subNodes = subNodes + delimiter+"\"" + columnRename("parent_"+n1.getNodeName()+"_id") + "\":\"" + id + "\"";//+ delimiter+"\"" + columnRename("gen_parent_"+n.getParentNode().getNodeName()+"_id") + "\":\"" + generated_id2 + "\"";

                //subNodes = subNodes + delimiter+"\"" + columnRename("parent_"+n1.getNodeName()+"_id") + "\":\"" + id + "\""+ delimiter+"\"" + columnRename("gen_parent_"+n.getParentNode().getNodeName()+"_id") + "\":\"" + generated_id2 + "\""+ delimiter+"\"" + columnRename("gen_grand_parent_"+n.getParentNode().getParentNode().getNodeName()) + "\":\"" + generated_id1 + "\"";
               
            }
           
           
        }
        else if ((n1 != null) && (n1.getAttributes() != null) && (n1.getAttributes().getNamedItem("id") != null) && (n1.getAttributes().getNamedItem("id").getTextContent() != null) && (n.getParentNode().getParentNode().getNodeName().toLowerCase().matches("context"))) {
            String id = n1.getAttributes().getNamedItem("id").getTextContent().replaceAll("\"", "").replace("{", "").replace("}","").replaceAll("[^\\p{Print}]","");
            if (id.trim().length() > 0) {
                generated_id2 = generated_id2+1;
                subNodes = subNodes + delimiter+"\"" + columnRename("parent_"+n1.getNodeName()+"_id") + "\":\"" + id + "\"";//+ delimiter+"\"" + columnRename("gen_parent_"+n.getParentNode().getNodeName()+"_id") + "\":\"" + generated_id2 + "\"";   
            }
           
        }
        else if ((n1 != null) && (n1.getAttributes() != null) && (n1.getAttributes().getNamedItem("id") != null) && (n1.getAttributes().getNamedItem("id").getTextContent() != null) && (n.getParentNode().getNodeName().toLowerCase().matches("context"))) {
            String id = n1.getAttributes().getNamedItem("id").getTextContent().replaceAll("\"", "").replace("{", "").replace("}","").replaceAll("[^\\p{Print}]","");
            if (id.trim().length() > 0) {
                generated_id2 = generated_id2+1;
                subNodes = subNodes + delimiter+"\"" + columnRename("parent_"+n1.getNodeName()+"_id") + "\":\"" + id + "\"";   
            }
            //generated_id2 = generated_id2+1;
        }
        if(n.getNodeName() != null)
        {
            Node parentNode = n.getParentNode();
            String parentNodeName = null;
            String parentNodeID=null;
            Node grandParentNode = n.getParentNode().getParentNode();
            String grandParentNodeName = null;
            String grandParentNodeID=null;
            if ((parentNode != null) && (parentNode.getAttributes() != null) && (parentNode.getAttributes().getNamedItem("id") != null) &&  (!n.getParentNode().getParentNode().getNodeName().toLowerCase().matches("context")) && (!n.getParentNode().getNodeName().toLowerCase().matches("context")))       
            {
                parentNodeName=parentNode.getNodeName();
                parentNodeID=parentNode.getAttributes().getNamedItem("id").getTextContent().replaceAll("\"", "").replace("{", "").replace("}","").replaceAll("[^\\p{Print}]","");
                if(parentNodeID==null) parentNodeID="noid";
                grandParentNode = n.getParentNode().getParentNode();
                //Looping through grand parent node names
                if ((grandParentNode != null) && (grandParentNode.getAttributes() != null) && (grandParentNode.getAttributes().getNamedItem("id") != null) && (grandParentNode.getAttributes().getNamedItem("id").getTextContent() != null) &&  (!n.getParentNode().getParentNode().getNodeName().toLowerCase().matches("context")) &&  (!n.getParentNode().getNodeName().toLowerCase().matches("context")))
                {
                    grandParentNodeName=grandParentNode.getNodeName();
                    grandParentNodeID=grandParentNode.getAttributes().getNamedItem("id").getTextContent().replaceAll("\"", "").replace("{", "").replace("}","").replaceAll("[^\\p{Print}]","");
                    if(grandParentNodeID==null) grandParentNodeID="noid";
                    if(grandParentNodeID.trim().length() > 0) {
                        //Hashmap for grandparent nodes.
                        //h_gen_id3.put("gen_"+replaceDot(n.getParentNode().getParentNode().getNodeName())+"_id", h_gen_id.get("gen_"+replaceDot(n.getParentNode().getParentNode().getNodeName())+"_id"));
                        subNodes = subNodes + delimiter+"\"" + columnRename("grand_parent_"+grandParentNodeName+"_id") + "\":\"" + grandParentNodeID + "\"";
                        //+ delimiter+"\"" + columnRename("gen_grand_parent_"+n.getParentNode().getParentNode().getNodeName()) + "\":\"" + h_gen_id3.get("gen_"+replaceDot(n.getParentNode().getParentNode().getNodeName())+"_id") + "\"";
                        generated_id1 = generated_id1+1;
                    }
                }
                else if (n.getParentNode().getParentNode().getNodeName().toLowerCase().matches("context")) {
                    grandParentNodeName=grandParentNode.getNodeName();
                    grandParentNodeID=grandParentNode.getAttributes().getNamedItem("id").getTextContent().replaceAll("\"", "").replace("{", "").replace("}","").replaceAll("[^\\p{Print}]","");
                    if(grandParentNodeID==null) grandParentNodeID="noid";
                    if(grandParentNodeID.trim().length() > 0) {
                        //h_gen_id3.put("gen_"+replaceDot(n.getParentNode().getNodeName())+"_id", h_gen_id.get("gen_"+replaceDot(n.getParentNode().getNodeName())+"_id"));
                        subNodes = subNodes + delimiter+"\"" + columnRename("grand_parent_"+grandParentNodeName+"_id") + "\":\"" + grandParentNodeID + "\"";
                        generated_id1 = generated_id1+1;
                    }
                }
                else if (n.getParentNode().getNodeName().toLowerCase().matches("context")) {
                    grandParentNodeName=grandParentNode.getNodeName();
                    grandParentNodeID=grandParentNode.getAttributes().getNamedItem("id").getTextContent().replaceAll("\"", "").replace("{", "").replace("}","").replaceAll("[^\\p{Print}]","");
                    if(grandParentNodeID==null) grandParentNodeID="noid";
                    if(grandParentNodeID.trim().length() > 0) {
                        subNodes = subNodes + delimiter+"\"" + columnRename("grand_parent_"+grandParentNodeName+"_id") + "\":\"" + grandParentNodeID + "\"";
                        generated_id1 = generated_id1+1;
                    }
                }               
            }
            //generated_id1 = generated_id1+1;
        }
        for (int i = 0; i < nl1.getLength(); i++)
        {
            n1 = nl1.item(i);

            boolean flag=true;
            if(n1.hasChildNodes())
            {
                //generated_id3 = generated_id3+1;
                if(n1.getChildNodes().getLength() == 1 && n1.getFirstChild() != null )
                {
                    String txtContent=n1.getFirstChild().getTextContent();
                    txtContent = txtContent.replaceAll("\"", "").replace("{", "").replace("}","").replaceAll("[^\\p{Print}]","").replaceAll("~", "");
                    String nodeName2=replaceDot(n1.getNodeName());
                    if ((txtContent != null) && (txtContent.length() > 0) && nodeName2 != null) {
                        if(excludedFields.indexOf("~"+nodeName2+"~") == -1 && !nodeName2.equalsIgnoreCase("id") && nodesToExclude.indexOf(","+nodeName2+",") == -1)
                            subNodes = subNodes + delimiter + "\"" + columnRename(nodeName2) + "\":\"" + txtContent + "\"";
                    }
                }
                else
                {
                    getJSONNodes(n1,source_system,vendor,lob);
                }
            }
            else if(n1.hasAttributes())
            {
                getJSONNodes(n1,source_system,vendor,lob);
            }

        }

        if (subNodes.length() > 1) {
            h.put(replaceDot(n.getNodeName()), subNodes.substring(1, subNodes.length()));
            //h_gen_id.put(replaceDot(n.getNodeName()), generated_id3);
            //h_gen_id.put(replaceDot(n.getNodeName()), ""+generated_id3);
            //h_gen_id.put(replaceDot(n.getNodeName()), subNodes.substring(1, subNodes.length()));
        }
        //this.ar.add(h_gen_id);
        this.ar.add(h);
        //generated_id3=1;
        //This line is added to increment all gen_<node_name>_ids for the sequential xml files.
        //generated_id3 = generated_id3+1;
        //h_gen_id.clear();
       
       
    }
   
}
