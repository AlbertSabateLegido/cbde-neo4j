/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.cbde.neo4j;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;

public class Neo4j {
    
    static void delete_graph(Session session) {
        session.run("MATCH (a) DETACH DELETE a");
    }
    
    
    //create a line item node l_returnflag l_linestatus l_quantity l_extendedprice l_discount L_tax l_shipdate l_orderkey
    static void create_lineitem(Session session, String identifier, String l_returnflag, String l_linestatus, int l_quantity, int l_extendedprice, int l_discount, int l_tax, Date l_shipdate, int l_orderkeyint, int l_suppkey) {
        String s =  "CREATE (" + identifier + ":LineItem {orderkey: " + l_orderkeyint +
        		", suppkey: " + l_suppkey + ", returnflag: '" + l_returnflag + "', quantity: " + l_quantity +
  	            ", extendedPrice: " + l_extendedprice + ", discount: " + l_discount + 
                    ", tax: " + l_tax + ", shipdate: '" + l_shipdate + "', linestatus: '" + l_linestatus + "'})";
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
    static void create_orders(Session session, String identifier, int o_orderkey, Date o_orderdate, String o_shippriority, int o_custkey) {
  	String s =  "CREATE (" + identifier + ":Order {orderkey: " + o_orderkey + 
  			", custkey: " + o_custkey +  ", orderdate: '" + o_orderdate + "', shippriority: '" + o_shippriority + "'})";
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
    static void create_nation(Session session, String identifier, int n_regionkey, int n_nationkey, String n_name) {
        String s =  "CREATE (" + identifier + ":Nation {regionkey: " + n_regionkey + 
                    ", nationkey: " + n_nationkey + ", name: '" + n_name + "'})";
        System.out.println(s);
        session.run(s);
    }
  	
    // create partsupp ps_supplycost ps_partkey ps_suppkey
    static void create_partsupp(Session session, String identifier, int ps_partkey, int ps_suppkey, int ps_supplycost) {
        String s =  "CREATE (" + identifier + ":Partsupp {partkey: '" + ps_partkey + 
                "', suppkey: '" + ps_suppkey + "', supplycost: '" + ps_supplycost + "'})";
        System.out.println(s);
        session.run(s);
    }
  	
    //create customer C_mktsegment c_custkey
    static void create_customer(Session session, String identifier, int c_custkey, int c_nationkey, String c_mktsegment) {
        String s =  "CREATE (" + identifier + ":Customer {custkey: " + 
                    c_custkey + ", c_nationkey:" + c_nationkey +", mktsegment: '" + c_mktsegment + "'})";
        System.out.println(s);
        session.run(s);
    }
    
    static void create_join_nation_region(Session session, String region, String nation, int n_regionkey, int r_regionkey) {
    	String s =  "MATCH (" + region + ":Region {regionkey: " + r_regionkey + "}), (" + nation +
                 ":Nation {regionkey: " + n_regionkey + "}) CREATE (" + nation + ")-[:has]->(" + region + ")";
        System.out.println(s);
        session.run(s);
    }
    
    static void create_join_supplier_nation(Session session, String supplier, String nation, int s_nationkey, int n_nationkey) {
    	String s =  "MATCH (" + supplier + ":Supplier {nationkey: " + s_nationkey + "}), (" + nation +
                ":Nation {nationkey: " + n_nationkey + "}) CREATE (" + supplier + ")-[:has]->(" + nation + ")";
        System.out.println(s);
        session.run(s);
    }
    
    static void create_join_nation_customer(Session session, String nation, String customer, int n_nationkey, int c_nationkey) {
    	String s =  "MATCH (" + customer + ":Customer {nationkey: " + c_nationkey + "}), (" + nation +
                ":Nation {nationkey: " + n_nationkey + "}) CREATE (" + customer + ")-[:has]->(" + nation + ")";
        System.out.println(s);
        session.run(s);
    }
    
    static void create_join_supplier_partsupplier(Session session, String supplier, String partsupp, int s_suppkey, int ps_suppkey) {
    	String s =  "MATCH (" + partsupp + ":Partsupp {suppkey: " + ps_suppkey + "}), (" + supplier +
                ":Supplier {suppkey: " + s_suppkey + "}) CREATE (" + partsupp + ")-[:has]->(" + supplier + ")";
        System.out.println(s);
        session.run(s);
    }
    
    static void create_join_partsupp_part(Session session, String partsupp, String part, int ps_partkey, int p_partkey) {
    	String s =  "MATCH (" + partsupp + ":Partsupp {partkey: " + ps_partkey + "}), (" + part +
                ":Part {partkey: " + p_partkey + "}) CREATE (" + partsupp + ")-[:has]->(" + part + ")";
        System.out.println(s);
        session.run(s);
    }
    
    static void create_join_supplier_lineitem(Session session, String supplier, String lineitem, int l_suppkey, int s_suppkey) {
    	String s =  "MATCH (" + lineitem + ":Lineitem {suppkey: " + l_suppkey + "}), (" + supplier +
                ":Supplier {suppkey: " + s_suppkey + "}) CREATE (" + lineitem + ")-[:has]->(" + supplier + ")";
        System.out.println(s);
        session.run(s);
    }
    
    static void create_join_lineitem_order(Session session, String lineitem, String order, int l_orderkey, int o_orderkey) {
    	String s =  "MATCH (" + lineitem + ":Lineitem {orderkey: " + l_orderkey + "}), (" + order +
                ":Order {orderkey: " + o_orderkey + "}) CREATE (" + lineitem + ")-[:has]->(" + order + ")";
        System.out.println(s);
        session.run(s);
    }
    
    static void create_join_order_customer(Session session, String order, String customer, int o_custkey, int c_custkey) {
        String s =  "MATCH (" + order + ":Order {custkey: " + o_custkey + "}), (" + customer +
                ":Customer {custkey: " + c_custkey + "}) CREATE (" + order + ")-[:has]->(" + customer + ")";
        System.out.println(s);
        session.run(s);
    }
    static void create_graph(Session session) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Date d1 = new Date();
        Date d2 = new Date();
        try {
            d1 = sdf.parse("2018-05-26");
            d2 = sdf.parse("2018-05-10");
        } catch (ParseException ex) {
            Logger.getLogger(Neo4j.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        create_lineitem(session, "l1", "T", "R", 10, 105, 30,  5, d1, 1,1);
        create_lineitem(session, "l2", "T", "S", 40,  50, 15, 10, d1, 1,1);
        create_lineitem(session, "l3", "F", "R", 20,  75, 10, 10, d1, 2,1);
        create_lineitem(session, "l4", "T", "S", 30,  80, 15,  5, d2, 2,2);
        create_lineitem(session, "l5", "F", "R", 10,  65, 20, 10, d2, 3,2);
        create_lineitem(session, "l6", "T", "R", 20,  90, 30,  5, d2, 3,2);
        
        create_supplier(session,"s1", 1, 1, 10, "a", "B", 123, "AB");
        create_supplier(session,"s2", 2, 2, 10, "a", "B", 123, "AB");
        create_supplier(session,"s3", 3, 2, 10, "a", "B", 123, "AB");
        
        create_part(session, "p1", 1, "ab", 2, "ab");
        create_part(session, "p2", 2, "ab", 2, "ab");
        create_part(session, "p3", 3, "ab", 2, "ab");
        
        create_orders(session, "o1", 1, d1, "ab0", 1);
        create_orders(session, "o2", 2, d1, "ab0", 1);
        create_orders(session, "o3", 3, d2, "ab0", 2);
        
        create_region(session, "r1", 1, "aba");
        create_region(session, "r2", 2, "aba");
        create_region(session, "r3", 3, "aba");
        create_region(session, "r4", 4, "aba");
        
        create_nation(session, "n1", 1, 1, "bba");
        create_nation(session, "n2", 1, 2, "bba");
        create_nation(session, "n3", 3, 3, "bba");
        create_nation(session, "n4", 4, 4, "bba");
        
        create_partsupp(session, "ps1", 1, 1, 1);
        create_partsupp(session, "ps2", 2, 2, 1);
        create_partsupp(session, "ps3", 3, 3, 1);
        
        create_customer(session, "c1", 1, 2, "aa");
        create_customer(session, "c2", 2, 1, "aa");
        create_customer(session, "c3", 3, 3, "aa");
        create_customer(session, "c4", 4, 4, "aa");

        create_join_nation_region(session, "r1", "n1", 1, 1);
        create_join_nation_region(session, "r1", "n2", 1, 1);
        create_join_nation_region(session, "r3", "n3", 3, 3);
        create_join_nation_region(session, "r4", "n4", 4, 4);
        
        create_join_supplier_nation(session, "s1", "n1", 1, 1);
        create_join_supplier_nation(session, "s2", "n2", 2, 2);
        create_join_supplier_nation(session, "s3", "n2", 2, 2);
        
        create_join_nation_customer(session, "n1", "c2", 1, 1);
        create_join_nation_customer(session, "n2", "c1", 2, 2);
        create_join_nation_customer(session, "n3", "c3", 3, 3);
        create_join_nation_customer(session, "n4", "c4", 4, 4);
        
       create_join_supplier_partsupplier(session, "s1", "ps1", 1, 1);
       create_join_supplier_partsupplier(session, "s2", "ps2", 2, 2);
       create_join_supplier_partsupplier(session, "s3", "ps3", 3, 3);
       
       create_join_partsupp_part(session, "ps1", "p1", 1, 1);
       create_join_partsupp_part(session, "ps2", "p2", 2, 2);
       create_join_partsupp_part(session, "ps3", "p3", 3, 3);
       
       create_join_supplier_lineitem(session, "s1", "l1", 1, 1);
       create_join_supplier_lineitem(session, "s1", "l2", 1, 1);
       create_join_supplier_lineitem(session, "s2", "l3", 2, 2);
       create_join_supplier_lineitem(session, "s2", "l4", 2, 2);
       create_join_supplier_lineitem(session, "s3", "l5", 3, 3);
       create_join_supplier_lineitem(session, "s3", "l6", 3, 3);
       
       create_join_lineitem_order(session, "l1", "o1", 1, 1);
       create_join_lineitem_order(session, "l2", "o1", 1, 1);
       create_join_lineitem_order(session, "l3", "o2", 2, 2);
       create_join_lineitem_order(session, "l4", "o2", 2, 2);
       create_join_lineitem_order(session, "l5", "o3", 3, 3);
       create_join_lineitem_order(session, "l6", "o3", 3, 3);
       
       create_join_order_customer(session, "o1", "c1", 1, 1);
       create_join_order_customer(session, "o2", "c1", 1, 1);
       create_join_order_customer(session, "o3", "c2", 2, 2);

      
    }
    
    static public void query1(Session session, Date date) {
        String query =
                "MATCH (li:LineItem) " +
                    " WHERE " +
                        "li.shipdate <= '" + date +
                    "' WITH " +
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
        
        
        StatementResult result = session.run(query);
     
        System.out.println("QUERY 1: " + query);
        
        while(result.hasNext()) {
            System.out.println(result.next());
        }
        
    }
    
    public static void main(String[] args) {

        Driver driver =  GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j","Mocolandia96."));
        Session session = driver.session();
        
        delete_graph(session); 
        create_graph(session);
         
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Date d = new Date();
        try { 
            d = sdf.parse("2018-05-26"); 
        }
        catch (ParseException ex) {
            Logger.getLogger(Neo4j.class.getName()).log(Level.SEVERE, null, ex); 
        }

        query1(session,d);
        System.out.println("Fin");


    }
    
}