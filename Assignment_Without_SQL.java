import java.sql.*;
import java.util.*;


public class Assignment_Without_SQL {

	public static String convertDate(String day){
		if(day.length() == 1) day = "0"+day;
		return day;
	}
	
	public static void main(String[] args) {
		String usr ="postgres";
		String pwd ="12345";
		String url ="jdbc:postgresql://localhost:5432/postgres";

		Map<String, Transaction> CustMax1 = new HashMap<String, Transaction>();
		Map<String, Transaction> CustMin1 = new HashMap<String, Transaction>();
		Map<String, ProductAmount> TotalAmount1 = new HashMap<String, ProductAmount>();
		Map<String, ProductAmount> TotalAmount2 = new HashMap<String, ProductAmount>();
		
		//Set<String> TotalAmount2 = new HashSet<String>();
		
		Map<String, Transaction> NY_Max = new HashMap<String, Transaction>();
		Map<String, Transaction> NJ_Min = new HashMap<String, Transaction>();
		Map<String, Transaction> CT_Min = new HashMap<String, Transaction>();
		Map<String, ProductAmount> Query2 = new HashMap<String, ProductAmount>();
		
		String YearString;
		Integer YearInt;
		
		try
		{
			Class.forName("org.postgresql.Driver");
			Connection conn = DriverManager.getConnection(url, usr, pwd);
			System.out.println("Success connecting server!");
		
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery("SELECT * FROM sales");
			while (rs.next()){
					Transaction tran = new Transaction();
					tran.cust = rs.getString("cust");
					tran.prod = rs.getString("prod");
					tran.state = rs.getString("state");
					tran.date = convertDate(rs.getString("month"))+"/"+convertDate(rs.getString("day"))+"/"+rs.getString("year");
					tran.quant = rs.getInt("quant");
					String key = tran.cust + tran.prod;
					YearString = tran.date.substring(tran.date.length() - 4);
					YearInt = Integer.parseInt(YearString);
					
//query 1
					if(CustMax1.containsKey(tran.cust)){
						if(CustMax1.get(tran.cust).quant < tran.quant){
							CustMax1.put(tran.cust, tran);
						}
					}
					else{
						CustMax1.put(tran.cust, tran);
					}
					if(CustMin1.containsKey(tran.cust)){
						if(CustMin1.get(tran.cust).quant > tran.quant){
							CustMin1.put(tran.cust, tran);
						}
					}
					else{
						CustMin1.put(tran.cust, tran);
					}
					
					if(TotalAmount1.containsKey(tran.cust)){
						TotalAmount1.get(tran.cust).counter++;
						TotalAmount1.get(tran.cust).quant += tran.quant;
					}
					else{
						ProductAmount temp = new ProductAmount();
						temp.counter = 1;
						temp.quant = tran.quant;
						temp.cust = tran.cust;
						temp.produt = tran.prod;
						TotalAmount1.put(tran.cust, temp);
					}
//query 2
//Conduct Query 2 (Max NY)				
					if(NY_Max.containsKey(key) && tran.state.equals("NY") && YearInt > 2000){
						if((NY_Max.get(key).quant < tran.quant) && tran.state.equals("NY"))
							NY_Max.put(key, tran);
					}
					else if (tran.state.equals("NY") && YearInt > 2000){
						NY_Max.put(key, tran);
					}
//Conduct Query 2 (Min NJ)				
					if(NJ_Min.containsKey(key) && tran.state.equals("NJ") && YearInt > 2000){
						if((NJ_Min.get(key).quant > tran.quant) && tran.state.equals("NJ"))
							NJ_Min.put(key, tran);
					}
					else if(tran.state.equals("NJ") && YearInt > 2000){
						NJ_Min.put(key, tran);
					}
//Conduct Query 2 (Min CT)	
					
					if(CT_Min.containsKey(key) && tran.state.equals("CT")){
						if((CT_Min.get(key).quant > tran.quant) && tran.state.equals("CT"))
							CT_Min.put(key, tran);
					}
					else if( tran.state.equals("CT")){
						CT_Min.put(key, tran);
					}
//acumulate total quant for each custumer (Query 2)		
					if(TotalAmount2.containsKey(key)){
						TotalAmount2.get(key).counter++;
						TotalAmount2.get(key).quant += tran.quant;
					}
					else{
						ProductAmount temp = new ProductAmount();
						temp.counter = 1;
						temp.quant = tran.quant;
						temp.cust = tran.cust;
						temp.produt = tran.prod;
						TotalAmount2.put(key, temp);
					}	
					
					
					
					
					
					
				}
		 	}
			catch(SQLException | ClassNotFoundException e){
					System.out.println("Connection URL or username or password errors!");
					e.printStackTrace();
			}
			
		
		
// query 1 display following the specified format
				double avg;
				System.out.printf("CUSTOMER  MAX_Q  MAX_PROD  MAX_DATE    ST  MIN_Q  MIN_PROD  MIN_DATE    ST  AVG_Q\n");
				System.out.printf("========  =====  ========  ==========  ==  =====  ========  ==========  ==  =====\n");
				for(String Samplekey: CustMax1.keySet()){
					avg = TotalAmount1.get(CustMax1.get(Samplekey).cust).quant/TotalAmount1.get(CustMax1.get(Samplekey).cust).counter;
					System.out.printf("%-8s  %5d  %-8s  %-10s  %2s  %5d  %-8s  %-10s  %2s  %5s\n", 
						CustMax1.get(Samplekey).getCust(),
						CustMax1.get(Samplekey).getQuant(),
						CustMax1.get(Samplekey).getProd(),
						CustMax1.get(Samplekey).getDate(),
						CustMax1.get(Samplekey).getState(),
						
						CustMin1.get(Samplekey).getQuant(),
						CustMin1.get(Samplekey).getProd(),
						CustMin1.get(Samplekey).getDate(),
						CustMin1.get(Samplekey).getState(),
						
						String.format("%.0f",avg));
					}
				System.out.printf("\n\n");
// query 1 display following the specified format
				System.out.printf("CUSTOMER  PRODUCT  NY_MAX  DATE        NJ_MIN  DATE        CT_MIN  DATE     \n");
				System.out.printf("========  =======  ======  ==========  ======  ==========  ======  ==========\n");
				for(String Samplekey: TotalAmount2.keySet()){
					//String Samplekey = Data.get(key).getCust()+Data.get(key).getProd();
					String NY_maximum= null;
					String NY_date= null;
					String NJ_minimum= null;
					String NJ_date= null;
					String CT_minimum= null;
					String CT_date= null;
					
					if(NY_Max.containsKey(Samplekey)){
						NY_maximum=Integer.toString(NY_Max.get(Samplekey).getQuant());
						NY_date=NY_Max.get(Samplekey).getDate();
						}
					if(NJ_Min.containsKey(Samplekey)){
						NJ_minimum=Integer.toString(NJ_Min.get(Samplekey).getQuant());
						NJ_date=NJ_Min.get(Samplekey).getDate();
						}
					if(CT_Min.containsKey(Samplekey)){
						CT_minimum=Integer.toString(CT_Min.get(Samplekey).getQuant());
						CT_date=CT_Min.get(Samplekey).getDate();
						}
					System.out.printf("%-8s  %-7s  %6s  %-10s  %6s  %-10s  %6s  %-10s\n", 
						TotalAmount2.get(Samplekey).getCust(),
						TotalAmount2.get(Samplekey).getProdut(),
						NY_maximum,
						NY_date,
						
						NJ_minimum,
						NJ_date,
						
						CT_minimum,
						CT_date);
					}
					System.out.printf("\n\n");
	}
}
