/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.cbde.neo4j;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.Month;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import static org.neo4j.driver.v1.Values.parameters;

public class Neo4j {
    
    static void delete_graph(Session session) {
        session.run("MATCH (a) DETACH DELETE a");
    }
    
    
    //create a line item node l_returnflag l_linestatus l_quantity l_extendedprice l_discount L_tax l_shipdate l_orderkey
    static void create_lineitem(Session session, String identifier, String l_returnflag, String l_linestatus, int l_quantity, int l_extendedprice, int l_discount, int l_tax, int l_shipdate, int l_orderkeyint, int l_suppkey) {
        String s =  "CREATE (" + identifier + ":LineItem {orderkey: " + l_orderkeyint +
        		", suppkey: " + l_suppkey + ", returnflag: '" + l_returnflag + "', quantity: " + l_quantity +
  	            ", extendedPrice: " + l_extendedprice + ", discount: " + l_discount + 
                    ", tax: " + l_tax + ", shipdate: " + l_shipdate + ", linestatus: '" + l_linestatus + "'})";
        System.out.println(s);
        session.run(s);
    }

    //create supplier S_accbal S_name S_address S_phone S_comment s_suppkey s_nationkey
    static void create_supplier(Session session, String identifier, int s_suppkey, int s_nationkey, int s_accbal, String s_name, String s_address, int s_phone, String s_comment) {
        String s =  "CREATE (" + identifier + ":Supplier {suppkey:" + s_suppkey + 
                ", nationkey: " + s_nationkey +  ", name: '" + s_name + "', accbal: " + 
                s_accbal + ", address: '" + s_address + "', phone: " + s_phone + 
                ", comment: '" + s_comment + "'})";
        System.out.println(s);
        session.run(s);
    }
  	
    //create part table P_partkey P_mfgr P_size P_type 
    static void create_part(Session session, String identifier, int p_partkey, String p_mfgr, int p_size, String p_type) {
  	String s =  "CREATE (" + identifier + ":Part {partkey:" + p_partkey +
                    ", mfgr:'" + p_mfgr + "', size: " + p_size + ", type: '" + p_type + "'})";
        System.out.println(s);
        session.run(s);
    }
  	
    //create orders o_orderdate o_shippriority
    static void create_orders(Session session, String identifier, int o_orderkey, int o_orderdate, String o_shippriority, int o_custkey) {
  	String s =  "CREATE (" + identifier + ":Order {orderkey: " + o_orderkey + 
  			", custkey: " + o_custkey +  ", orderdate: " + o_orderdate + ", shippriority: '" + o_shippriority + "'})";
        System.out.println(s);
        session.run(s);
    }
  	
    //create region r_name
    static void create_region(Session session, String identifier, int r_regionkey, String r_name) {
        String s =  "CREATE (" + identifier + ":Region {regionkey: " + r_regionkey + ", name: '" + r_name + "'})";
        System.out.println(s);
        session.run(s);
    }
  	
    //create nation n_name n_nationkey n_regionkey
    static void create_nation(Session session, String identifier, long n_regionkey, long n_nationkey, String n_name) {
        String s =  "CREATE (" + identifier + ":Nation {regionkey: $regionkey" +
                    ", nationkey: $nationkey, name: '" + n_name + "'})";
        System.out.println(s);
        session.run(s,parameters("regionkey",n_regionkey,"nationkey",n_nationkey));
    }
  	
    // create partsupp ps_supplycost ps_partkey ps_suppkey
    static void create_partsupp(Session session, String identifier, long ps_partkey, long ps_suppkey, long ps_supplycost) {
        String s =  "CREATE (" + identifier + ":Partsupp {partkey: $partkey" + 
                ", suppkey: $suppkey, supplycost: $supplycost})";
        System.out.println(s);
        session.run(s,parameters("partkey",ps_partkey,"suppkey",ps_suppkey,"supplycost",ps_supplycost));
    }
  	
    //create customer C_mktsegment c_custkey
    static void create_customer(Session session, String identifier, int c_custkey, int c_nationkey, String c_mktsegment) {
        String s =  "CREATE (" + identifier + ":Customer {custkey: " + 
                    c_custkey + ", c_nationkey:" + c_nationkey +", mktsegment: '" + c_mktsegment + "'})";
        System.out.println(s);
        session.run(s,parameters("custkey",c_custkey,"nationkey",c_nationkey));
    }
    
    static void create_join_nation_region(Session session, long n_regionkey, long r_regionkey) {
    	String s =  "MATCH (r:Region {regionkey: $n_regionkey}), (n:Nation {regionkey: $n_regionkey})" +
                "CREATE (n)-[:has]->(r)";
        System.out.println(s);
        session.run(s,parameters("n_regionkey",n_regionkey,"r_regionkey",r_regionkey));
    }
    
    static void create_join_supplier_nation(Session session,long s_nationkey, long n_nationkey) {
    	String s =  "MATCH (s:Supplier {nationkey: $s_nationkey}), (n:Nation {nationkey: $n_nationkey})" + 
                    "CREATE (s)-[:has]->(n)";
        System.out.println(s);
        session.run(s,parameters("s_nationkey",s_nationkey,"n_nationkey",n_nationkey));
    }
    
    static void create_join_nation_customer(Session session, long n_nationkey, long c_nationkey) {
    	String s =  "MATCH (c:Customer {nationkey: $c_nationkey}), (n:Nation {nationkey: $n_nationkey})" +
                    "CREATE (c)-[:has]->(n)";
        System.out.println(s);
        session.run(s,parameters("c_nationkey",c_nationkey,"n_nationkey",n_nationkey));
    }
    
    static void create_join_supplier_partsupplier(Session session, long s_suppkey, long ps_suppkey) {
    	String s =  "MATCH (ps:Partsupp {suppkey: $ps_suppkey}), (s:Supplier {suppkey: $s_suppkey})" + 
                    "CREATE (ps)-[:has]->(s)";
        System.out.println(s);
        session.run(s,parameters("s_suppkey",s_suppkey,"ps_suppkey",ps_suppkey));
    }
    
    static void create_join_partsupp_part(Session session, long ps_partkey, long p_partkey) {
    	String s =  "MATCH (ps:Partsupp {partkey: $ps_partkey}), (p:Part {partkey: $p_partkey})" +
                    "CREATE (p)-[:has]->(ps)";
        System.out.println(s);
        session.run(s,parameters("ps_partkey",ps_partkey,"p_partkey",p_partkey));
    }
    
    static void create_join_supplier_lineitem(Session session, long l_suppkey, long s_suppkey) {
    	String s =  "MATCH (li:Lineitem {suppkey: $l_suppkey}), (s:Supplier {suppkey: $s_suppkey})" +
                    "CREATE (li)-[:has]->(s)";
        System.out.println(s);
        session.run(s,parameters("l_suppkey",l_suppkey,"s_suppkey",s_suppkey));
    }
    
    static void create_join_lineitem_order(Session session, long l_orderkey, long o_orderkey) {
    	String s =  "MATCH (li:LineItem {orderkey: $l_orderkey}), (o:Order {orderkey: $o_orderkey})" +
                    "CREATE (li)-[:has]->(o)";
        System.out.println(s);
        session.run(s,parameters("l_orderkey",l_orderkey,"o_orderkey",o_orderkey));
    }
    
    static void create_join_order_customer(Session session, long o_custkey, long c_custkey) {
        String s =  "MATCH (o:Order {custkey: $o_custkey}), (c:Customer {custkey: $c_custkey})" +
                    "CREATE (o)-[:has]->(c)";
        System.out.println(s);
        session.run(s,parameters("o_custkey",o_custkey,"c_custkey",c_custkey));
    }
    
    static void create_graph(Session session) {
        
        create_lineitem(session, "l1", "T", "R", 10, 105, 30,  5, 20180525, 1,1);
        create_lineitem(session, "l2", "T", "S", 40,  50, 15, 10, 20180525, 1,1);
        create_lineitem(session, "l3", "F", "R", 20,  75, 10, 10, 20180525, 2,1);
        create_lineitem(session, "l4", "T", "S", 30,  80, 15,  5, 20180510, 2,2);
        create_lineitem(session, "l5", "F", "R", 10,  65, 20, 10, 20180510, 3,2);
        create_lineitem(session, "l6", "T", "R", 20,  90, 30,  5, 20180510, 3,2);
        
        create_supplier(session,"s1", 1, 1, 10, "supply_name_1", "B", 123, "comment1");
        create_supplier(session,"s2", 2, 2, 10, "supply_name_2", "B", 123, "comment2");
        create_supplier(session,"s3", 3, 2, 10, "supply_name_3", "B", 123, "comment3");
        
        create_part(session, "p1", 1, "ab", 10, "type");
        create_part(session, "p2", 2, "ab", 10, "type");
        create_part(session, "p3", 3, "ab", 10, "type");
        
        create_orders(session, "o1", 1, 20180525, "ab0", 1);
        create_orders(session, "o2", 2, 20180525, "ab0", 1);
        create_orders(session, "o3", 3, 20180510, "ab0", 2);
        
        create_region(session, "r1", 1, "region1");
        create_region(session, "r2", 2, "region2");
        create_region(session, "r3", 3, "region3");
        create_region(session, "r4", 4, "region4");
        
        create_nation(session, "n1", 1, 1, "nation_name_1");
        create_nation(session, "n2", 1, 2, "nation_name_2");
        create_nation(session, "n3", 3, 3, "nation_name_3");
        create_nation(session, "n4", 4, 4, "nation_name_4");
        
        create_partsupp(session, "ps1", 1, 1, 20);
        create_partsupp(session, "ps2", 2, 2, 30);
        create_partsupp(session, "ps3", 3, 3, 10);
        
        create_customer(session, "c1", 1, 2, "aa");
        create_customer(session, "c2", 2, 1, "aa");
        create_customer(session, "c3", 3, 3, "aa");
        create_customer(session, "c4", 4, 4, "aa");


	create_join_nation_region(session, 1, 1);
        create_join_nation_region(session, 1, 1);
        create_join_nation_region(session, 3, 3);
        create_join_nation_region(session, 4, 4);
        
        create_join_supplier_nation(session, 1, 1);
        create_join_supplier_nation(session, 2, 2);
        create_join_supplier_nation(session, 2, 2);
        
        create_join_nation_customer(session, 1, 1);
        create_join_nation_customer(session, 2, 2);
        create_join_nation_customer(session, 3, 3);
        create_join_nation_customer(session, 4, 4);
        
       create_join_supplier_partsupplier(session, 1, 1);
       create_join_supplier_partsupplier(session, 2, 2);
       create_join_supplier_partsupplier(session, 3, 3);
       
       create_join_partsupp_part(session, 1, 1);
       create_join_partsupp_part(session, 2, 2);
       create_join_partsupp_part(session, 3, 3);
       
       create_join_supplier_lineitem(session, 1, 1);
       create_join_supplier_lineitem(session, 1, 1);
       create_join_supplier_lineitem(session, 2, 2);
       create_join_supplier_lineitem(session, 2, 2);
       create_join_supplier_lineitem(session, 3, 3);
       create_join_supplier_lineitem(session, 3, 3);
       
       create_join_lineitem_order(session, 1, 1);
       create_join_lineitem_order(session, 1, 1);
       create_join_lineitem_order(session, 2, 2);
       create_join_lineitem_order(session, 2, 2);
       create_join_lineitem_order(session, 3, 3);
       create_join_lineitem_order(session, 3, 3);
       
       create_join_order_customer(session, 1, 1);
       create_join_order_customer(session, 1, 1);
       create_join_order_customer(session, 2, 2);

      
    }
    
    static public void query1(Session session, long date) {
        String query =
                "MATCH (li:LineItem) " +
                    " WHERE " +
                        "li.shipdate <= " + date +
                    " WITH " +
                        " li.returnflag AS l_returnflag, " +
                        " li.linestatus AS l_linestatus, " +
                        " SUM(li.quantity) AS sum_qty, " +
                        " SUM(li.extendedPrice) AS sum_base_price, " +
                        " SUM(li.extendedPrice*(1-li.discount)) AS sum_disc_price, " +
                        " SUM(li.extendedPrice*(1-li.discount)*(1+li.tax)) AS sum_charge, " +
                        " AVG(li.quantity) AS avg_qty, " +
                        " AVG(li.extendedPrice) AS avg_price, " +
                        " AVG(li.discount) AS avg_disc, " +
                        " COUNT(*) AS count_order " +
                    " RETURN " +
                        " l_returnflag, " +
                        " l_linestatus, " +
                        " sum_qty, " +
                        " sum_base_price, " +
                        " sum_disc_price, " +
                        " sum_charge, " +
                        " avg_qty, " +
                        " avg_price, " +
                        " avg_disc, " +
                        " count_order " +
                    " ORDER BY " +
                        " l_returnflag ASC, " +
                        " l_linestatus ASC ";
        
        
        StatementResult result = session.run(query,parameters("date",date));
     
        System.out.println("QUERY 1: " + query);
        
        while(result.hasNext()) {
            System.out.println(result.next());
        }
        
    }
    
    static public void query2(Session session, String name, int size,String substring_type) {
        String query =
                "MATCH (:Region{name: '" + name + "'})<-[:has]-(n:Nation)<-[:has]-" +
                "(s:Supplier)<-[:has]-(ps:Partsupp)<-[:has]-(p:Part{size: " + size + "}) " +
                "WHERE p.type=~ '.*" + substring_type + "*.'" +
                "WITH MIN(ps.supplycost) AS min_supplycost " +
                "MATCH (:Region{name: '" + name + "'})<-[:has]-(n:Nation)<-[:has]-" +
                "(s:Supplier)<-[:has]-(ps:Partsupp{supplycost:min_supplycost})<-[:has]-(p:Part{size: " + size + "}) " +
                "WHERE p.type=~ '.*" + substring_type + "*.'" +
                "WITH "
                + "s.accbal AS s_accbal,"
                + "s.name AS s_name,"
                + "n.name AS n_name,"
                + "p.partkey AS p_partkey,"
                + "p.mfgr AS p_mfgr,"
                + "s.adress AS s_adress," 
                + "s.phone AS s_phone,"
                + "s.comment AS s_comment " +
                "RETURN "
                + "s_accbal,"
                + "s_name,"
                + "n_name,"
                + "p_partkey,"
                + "p_mfgr,"
                + "s_adress,"
                + "s_phone,"
                + "s_comment " +
                "ORDER BY "
                + "s_accbal DESC,"
                + "s_name ASC,"
                + "n_name ASC,"
                + "p_partkey ASC";
        
        StatementResult result = session.run(query);
     
        System.out.println("QUERY 2: " + query);
        
        while(result.hasNext()) {
            System.out.println(result.next());
        }
    }

    static public void query3(Session session, long date1, long date2, String segment) {
        String query =       		
        		
        		"MATCH (l1:LineItem)-[:has]->(o1:Order)-[:has]->(c1:Customer) " +
                        " WHERE " +
                        "     l1.shipdate > " + date2 + " AND " +
                        "     o1.orderdate < " + date1 + "  AND " +
                        "     c1.mktsegment = '" + segment + "' AND " +
                        "     o1.orderkey = l1.orderkey  AND " +
                        " 	  c1.custkey = o1.custkey " +
                        " WITH " +
                        "      l1.orderkey AS l_orderkey, " +
                        "      o1.orderdate AS o_orderdate, " +
                        "      o1.shippriority AS o_shippriority, " +
                        "      SUM(l1.extendedPrice*(1-l1.discount)) AS revenue " +
                        " RETURN " +
                        "      l_orderkey, " +
                        "      revenue, " +
                        "      o_orderdate, " +
                        "      o_shippriority " +
                        " ORDER BY " +
                        "      revenue DESC, " +
                        "      o_orderdate ASC ";
        	
                       
        
        
        StatementResult result = session.run(query);
     
        System.out.println("QUERY 3: " + query);
        
        while(result.hasNext()) {
            System.out.println(result.next());
        }
        
    }
    
    public static void main(String[] args) {

        Driver driver =  GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j","cbde"));
        Session session = driver.session();
        
        delete_graph(session); 
        create_graph(session);
        
        query1(session,20180526);
        System.out.println("FIN1");
        
        query2(session,"region1",10,"typ");
        System.out.println("FIN2");

        query3(session,20190526,20170526,"aa");
        System.out.println("FIN3");

    }
    
}