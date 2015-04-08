/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package jackcesstest;

import java.io.File;
import java.util.*;
import com.healthmarketscience.jackcess.*;
import com.healthmarketscience.jackcess.util.ExportFilter;
import com.healthmarketscience.jackcess.util.ExportUtil;
import com.healthmarketscience.jackcess.util.SimpleExportFilter;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;

/**
 *
 * @author Diego
 */
public class JackcessTest {    
    
    public static DecimalFormat df = new DecimalFormat("000000000000000000"); 
    static String ctlFile;
    static String infile="";
    static String table="";
    static String[] columns;
    
    public static void main(String[] args) {
        
      Locale.setDefault(new Locale("es_CL"));      
            
        if(args.length==0 || args.length==1 || args.length>2){            
            System.out.println("JackessTest.jar es un paquete para exportar una tabla access (.mdb) a un archivo separado por comas (.csv)");
            System.out.println("Uso: java -jar JackcessTest.jar -control <control_file>");                        
            return;
        }
        // Capturar parametros de PARAMILS
        for (int i = 0; i < args.length; ++i) {                        
            if (args[0].equals("-control")){
                if(args[1]==null){
                    System.out.println("Excepción: Argumento nulo");   
                    System.out.println("Uso: java -jar JackcessTest.jar -control <control_file>");
                }
                else{
                    ctlFile=args[1];
                }
            }
            else{
                    System.out.println("Excepción: Opción no válida");   
                    System.out.println("Uso: java -jar JackcessTest.jar -control <control_file>");
                }            
        }
        
      if(!readCtlFile(ctlFile))
        return;
        
      try (Database db = DatabaseBuilder.open(              
              
        new File(infile))) {                                            
          
            ExportFilter eFilter;
            eFilter = new SimpleExportFilter() {
              private List<Column> _cols = new ArrayList<Column>();
              private int _colIdx = 0;
              
              @Override
              public Object[] filterRow(Object[] row) throws IOException
              {
                  SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss");   
                  
                  for (int i = 0; i < row.length; ++i) {                                        
                    if (row[i].getClass().getCanonicalName().equals("com.healthmarketscience.jackcess.impl.ColumnImpl.DateExt"))
                        row[i] = sdf.format(row[i]);
                    if (row[i].getClass().getCanonicalName().equals("java.lang.Double"))
                        row[i] = Integer.parseInt(String.format(df.format(Double.valueOf(row[i].toString()).longValue())));   
                  }                                                                         
                  return row;
              }
              
              @Override
              public List<Column> filterColumns(List<Column> _columns) {
                  for (Column c : _columns) {
                      if(Arrays.asList(columns).contains(c.getName())){
                        _cols.add(c);   
                      }                        
                  }
                  return _cols;
              }              
          };

            ExportUtil.exportFile(
                db, 
                table, 
                new File("jackcess_output.csv"), 
                true, 
                ";", 
                '"', 
                eFilter);
            
        } catch (Exception e) {
            e.printStackTrace(System.out);
        }      
    }
    
    public static boolean readCtlFile(String filename)
    {		        
        BufferedReader br = null;
        StringTokenizer st;   
        String cadena="";

        try
        {            
            br = new BufferedReader(new FileReader(filename)); 
            st = new StringTokenizer(br.readLine());  
            String _infile=st.nextToken().toString();
            
            if(_infile.compareToIgnoreCase("infile")==0){
                while(st.hasMoreTokens()){
                    infile=infile+st.nextToken().toString()+" ";                                    
                }        
                infile = infile.substring(0, infile.length()-1);
                System.out.println("infile="+infile); 
            }
            else{
                System.out.println("JackCess: Error de sintaxis");
                System.out.println("Se espera palabra clave INFILE, se ha encontrado '"+_infile+"'");
                return false;
            }
            
            st = new StringTokenizer(br.readLine());
            String _table=st.nextToken().toString();
            
            if(_table.compareToIgnoreCase("table")==0){    
                while(st.hasMoreTokens()){
                    table=table+st.nextToken().toString()+" ";                                    
                }            
                table = table.substring(0, table.length()-1);
                System.out.println("table="+table);                
            }
            else{
                System.out.println("JackCess: Error de sintaxis");
                System.out.println("Se espera palabra clave TABLE, se ha encontrado '"+_table+"'");
                return false;
            }
            
            int cont=0;
            String linea="";
            while((linea=br.readLine())!=null){
                st = new StringTokenizer(linea);  
                cadena=cadena+st.nextToken().toString();
                cont++;
            }            
            if(cont==0){
                System.out.println("JackCess: Error de sintaxis");
                System.out.println("Se espera al menos una columna a leer");
                return false;
            }
            if(cadena.charAt(0)!='(' || cadena.charAt(cadena.length()-1)!=')'){
                System.out.println("JackCess: Error de sintaxis");
                System.out.println("Las columnas deben estar cerradas entre paréntesis");
                return false;
            }
            
            cadena=cadena.replace("(","");
            cadena=cadena.replace(")", "");
            
            columns=cadena.split(",");             
        }
        catch (Exception e)
        {            
            System.err.println("ERROR: "+e);
            return false;
        }     
        return true;
    }
}
