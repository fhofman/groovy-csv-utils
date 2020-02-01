@Grab(group = 'net.sf.opencsv', module = 'opencsv', version = '2.3')
import au.com.bytecode.opencsv.CSVReader
import au.com.bytecode.opencsv.CSVParser
import au.com.bytecode.opencsv.CSVWriter

import groovy.sql.Sql

def salida = [];
//def TEST_OUTPUT_FILE_NAME = 'testOut.csv'
def sql = Sql.newInstance("jdbc:mysql://localhost:3306/pmobi", "root", "root", "com.mysql.jdbc.Driver")
/*def n1 = sql.dataSet("nivel1")
def n2 = sql.dataSet("nivel2")
def n3 = sql.dataSet("nivel3")
def rep = sql.dataSet("reparticion")*/
//csvs.each{ file ->
List<String[]> rows = new CSVReader(new FileReader(new File("estructura.csv"))).readAll()
//def rowsOver100 = rows.findAll {it[1].toInteger() > 100}

def getId(t, sql){
  sql.firstRow("select id from ${t} order by id desc limit 1",[]).id 
}

def n1c = null, n2c = null, n3c = null, repc = null, q;
def next_n1_id, next_n2_id,next_n3_id,next_rep_id;
next_n1_id = getId("nivel1", sql); 
next_n2_id = getId("nivel2", sql); 
next_n3_id = getId("nivel3", sql); 
next_rep_id = getId("reparticion", sql);
rows.each{ fields ->
  //println "n1c ${n1c}"
  
  if(n1c == null || n1c != fields[0] ){
    next_n1_id++;
    n1c = fields[0]
    q = "insert into nivel1(id, version, nombre) values(${next_n1_id} , 0 , '${n1c}');"
    salida << q
  }
  
  if (n2c == null || (n2c != fields[1] && n1c == fields[0])){
    next_n2_id++;
    n2c = fields[1]
    q = "insert into nivel2(id, version, nombre, nivel1_id) values(${next_n2_id} , 0 , '${n2c}', ${next_n1_id});"
    salida << q
  }
  
  
  if(n3c == null || n3c != fields[2] && n2c == fields[1] && n1c == fields[0]){
    next_n3_id++
    n3c = fields[2]
    q = "insert into nivel3(id, version, nombre, nivel2_id) values(${next_n3_id} , 0 , '${n3c}', ${next_n2_id});"
    salida << q
  }
   
  if(repc == null || repc != fields[3] && n3c == fields[2] && n2c == fields[1] && n1c == fields[0]){
    next_rep_id++;
    repc = fields[3]
    q = "insert into reparticion(id, version, nombre, nivel3_id) values(${next_rep_id} , 0 , '${repc}', ${next_n3_id});"
    salida << q
  }
}

println salida.join("\n")

