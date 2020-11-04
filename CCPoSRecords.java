package cardtransaction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class CCPoSRecords {
	
	private String card_id = null;
	private String member_id = null;
	private double amount = 0.0;
	private String postcode = null;
	private String pos_id = null;
	private Date transaction_dt = null;
	private String status = null;
	
	private static Admin hBaseAdmin = null;
	
	private static String lookupTable = "look_up_table";
	private static String transactionTable = "card_transactions";
	
	public String getCard_id() {
		return card_id;
	}
	public String getMember_id() {
		return member_id;
	}
	public double getAmount() {
		return amount;
	}
	public String getPostcode() {
		return postcode;
	}
	public String getPos_id() {
		return pos_id;
	}
	public Date getTransaction_dt() {
		return transaction_dt;
	}
	public String getStatus() {
		return status;
	}
	
	//create java object from json string 
	public CCPoSRecords(String jsonObj) {

		JSONParser parser = new JSONParser();
		try {

			JSONObject obj = (JSONObject) parser.parse(jsonObj);

			this.card_id = String.valueOf(obj.get("card_id"));
			this.member_id = String.valueOf(obj.get("member_id"));
			this.amount = Double.parseDouble(String.valueOf(obj.get("amount")));
			this.postcode = String.valueOf(obj.get("postcode"));
			this.pos_id = String.valueOf(obj.get("pos_id"));
			this.transaction_dt = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss")
					.parse(String.valueOf(obj.get("transaction_dt")));

		} catch (ParseException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	// method to connect to hbase
	public static Admin getHbaseAdmin() throws IOException {
		Configuration conf = HBaseConfiguration.create();
		conf.setInt("timeout", 100000);
		conf.set("hbase.master", RecordConsumer.HOST_IP + ":60000");
		conf.set("hbase.zookeeper.quorum", RecordConsumer.HOST_IP);
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		conf.set("zookeeper.znode.parent", "/hbase");
		try {
			Connection con = ConnectionFactory.createConnection(conf);
			if (hBaseAdmin == null) {
				hBaseAdmin = con.getAdmin();
			}

		} catch (IOException e) {
			e.printStackTrace();
		}

		return hBaseAdmin;
	}
	
	//call logic to check for genuine/fraud records and update hbase table
	public void ProcessRecords(CCPoSRecords obj) {
		try {
			checkFraudulent();
			updateRecordsInHbase(obj);

		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	//compare rule params UCL/SCORE/SPEED
	private void checkFraudulent() throws IOException {
		double ucl = 0;
		int score = 0;
		// double speed = 0;

		ucl = getUCL(this.getCard_id());
		score = getScore(this.getCard_id());
		//speed = getSpeed();
		
		if (score < 200) {
			this.status = "FRAUD";
		}else if (this.getAmount() > ucl) {
			this.status = "FRAUD";
		} else if (getSpeed() > 0.25) { //Only call speed method if it passes above two checks
			this.status = "FRAUD";
		} else {
			this.status = "GENUINE";
		}
	}
	
	//get the latest UCL score for card ID from Rule_params column family
	public static Double getUCL(String cardID) throws IOException {
		if (hBaseAdmin == null) {
			hBaseAdmin = getHbaseAdmin();
		}
		double val = 0;
		try {
			Get cardId = new Get(Bytes.toBytes(cardID));
			Table htable = hBaseAdmin.getConnection().getTable(TableName.valueOf(lookupTable));
			Result result = htable.get(cardId);
			byte[] value = result.getValue(Bytes.toBytes("Rule_params"), Bytes.toBytes("UCL"));
			if (value != null) {
				val = Double.parseDouble(Bytes.toString(value));
			} else {
				val = 0d;
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

		return val;
	}

	//Get the score from look_up_table and 
	public static Integer getScore(String cardID) throws IOException {
		if (hBaseAdmin == null) {
			hBaseAdmin = getHbaseAdmin();
		}
		int val = 0;
		try {
			Get cardId = new Get(Bytes.toBytes(cardID));
			Table htable = hBaseAdmin.getConnection().getTable(TableName.valueOf(lookupTable));
			Result result = htable.get(cardId);
			byte[] value = result.getValue(Bytes.toBytes("Rule_params"), Bytes.toBytes("score"));
			if (value != null) {
				val = Integer.parseInt(Bytes.toString(value));
			} else
				val = 0;

		} catch (Exception e) {
			e.printStackTrace();
		}

		return val;
	}

	private double getSpeed() {
		double distance = 0;
		long timeDifference = 0L;
		double speed = 0;

		try {
			DistanceUtility distUtil = new DistanceUtility();
			String lastPostCode = getPostCode(this.getCard_id());

			Date lastTransactionDt = getTransactionDate(this.getCard_id());
			distance = distUtil.getDistanceViaZipCode(lastPostCode, this.getPostcode());
			timeDifference = (java.lang.Math.abs(this.getTransaction_dt().getTime() - lastTransactionDt.getTime()))
					/ 1000;
			if (lastPostCode.equals(this.getPostcode())) {
				speed = 0;
			} else {
				speed = distance / timeDifference;
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

		return speed;
	}
	//get the latest postcode from look_up_table
	public static String getPostCode(String cardID) throws IOException {

		if (hBaseAdmin == null) {
			hBaseAdmin = getHbaseAdmin();
		}
		String val = null;
		try {
			Get cardId = new Get(Bytes.toBytes(cardID));
			Table htable = hBaseAdmin.getConnection().getTable(TableName.valueOf(lookupTable));
			Result result = htable.get(cardId);
			byte[] value = result.getValue(Bytes.toBytes("Rule_params"), Bytes.toBytes("postcode"));
			if (value != null) {
				val = Bytes.toString(value);
			} else
				val = null;

		} catch (Exception e) {
			e.printStackTrace();
		}

		return val;
	}

	//get latest transaction date from look_up_table column family: card_details
	public static Date getTransactionDate(String cardID) throws IOException {
		
		if (hBaseAdmin == null) {
			hBaseAdmin = getHbaseAdmin();
		}
		Date val = null;
		try {
			Get cardId = new Get(Bytes.toBytes(cardID));
			Table htable = hBaseAdmin.getConnection().getTable(TableName.valueOf(lookupTable));
			Result result = htable.get(cardId);
			byte[] value = result.getValue(Bytes.toBytes("card_details"), Bytes.toBytes("transaction_dt"));
			if (value != null) {
				val = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(Bytes.toString(value));
			} else
				val = null;
			} catch (Exception e) {
				e.printStackTrace();
			}
			return val;
	}

	//insert all record in transaction and if its genuine then insert in look_up_tables
	public static void updateRecordsInHbase(CCPoSRecords posRecord) throws IOException {

		try {
			if (hBaseAdmin == null) {
				hBaseAdmin = getHbaseAdmin();
			}

			Double Amount = posRecord.getAmount();
			DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			String transDate = dateFormat.format(posRecord.getTransaction_dt());
			
			Table htable = hBaseAdmin.getConnection().getTable(TableName.valueOf(transactionTable));
			
			//row key that was defined in my table is card_id~pos_id~transaction_dt
			String rowkey = posRecord.getCard_id().concat("~").concat(posRecord.getPos_id()).concat("~").concat(transDate);

			Put p = new Put(Bytes.toBytes(rowkey));
			
			//TD:card_id, TD:pos_id, TD:transaction_dt,TD:member_id,TD:amount, TD:postcode, TD:status
			p.addColumn(Bytes.toBytes("TD"), Bytes.toBytes("card_id"), Bytes.toBytes(posRecord.getCard_id()));
			p.addColumn(Bytes.toBytes("TD"), Bytes.toBytes("member_id"), Bytes.toBytes(posRecord.getMember_id()));
			p.addColumn(Bytes.toBytes("TD"), Bytes.toBytes("amount"), Bytes.toBytes(Amount.toString()));
			p.addColumn(Bytes.toBytes("TD"), Bytes.toBytes("postcode"), Bytes.toBytes(posRecord.getPostcode()));
			p.addColumn(Bytes.toBytes("TD"), Bytes.toBytes("pos_id"), Bytes.toBytes(posRecord.getPos_id()));
			p.addColumn(Bytes.toBytes("TD"), Bytes.toBytes("transaction_dt"), Bytes.toBytes(transDate));
			p.addColumn(Bytes.toBytes("TD"), Bytes.toBytes("status"), Bytes.toBytes(posRecord.getStatus()));

			htable.put(p);

		} catch (Exception e) {
			e.printStackTrace();
		}

		//if the record is genuine add details in look_up_table
		if (posRecord.getStatus().equals("GENUINE")) {
			try {
				if (hBaseAdmin == null) {
					hBaseAdmin = getHbaseAdmin();
				}

				DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				String val = dateFormat.format(posRecord.getTransaction_dt());

				Table htable = hBaseAdmin.getConnection().getTable(TableName.valueOf(lookupTable));

				Put p = new Put(Bytes.toBytes(posRecord.getCard_id()));

				p.addColumn(Bytes.toBytes("Rule_params"), Bytes.toBytes("postcode"), Bytes.toBytes(posRecord.getPostcode()));
				p.addColumn(Bytes.toBytes("card_details"), Bytes.toBytes("transaction_dt"), Bytes.toBytes(val));

				htable.put(p);
			} catch (Exception e) {
				e.printStackTrace();
			}
		} 
	}
	public static void closeConnection() throws IOException {
		hBaseAdmin.getConnection().close();
	}
}
