package com.zettamine.casestudy.avgrainfall;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;

public class AverageRainFall {

	private static PreparedStatement psat = null;

	public static void main(String[] args) {
		calcAvgRainFall();
	}

	public static void calcAvgRainFall() {

		Stream<String> lines;
		try {
			lines = Files.lines(Paths.get("src/main/resources/AllCityMonthlyRainfall.txt"));
			List<String> list = new ArrayList<>();
			lines.forEach(data -> list.add(data.toString()));
			String[] monthlyData = null;
			for (int i = 0; i < list.size(); i++) {
				monthlyData = list.get(i).split(",");
				double avg = Arrays.stream(monthlyData).filter(data -> data.matches("\\d{1,3}"))
						.mapToDouble(Double::parseDouble).summaryStatistics().getAverage();
				insertData(monthlyData[0], monthlyData[1], avg);
			}
			displayMaxAvgRainfall();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static void insertData(String pincode, String cityName, double avg) {

		Connection conn = getConnection();
		try {
			psat = conn.prepareStatement("Insert INTO annual_rainfall values(?,?,?)");
			psat.setString(1, pincode);
			psat.setString(2, cityName);
			psat.setDouble(3, avg);
			psat.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public static void displayMaxAvgRainfall() {
		
		Connection conn = getConnection();
		try {
			psat = conn.prepareStatement("SELECT * FROM annual_rainfall WHERE average_annual_rainfall IN "
										+ "(select max(average_annual_rainfall) from  annual_rainfall)");
			ResultSet rs = psat.executeQuery();
			while(rs.next()) {
				String pincode = rs.getString(1);
				String city = rs.getString(2);
				Double rainFall = rs.getDouble(3);
				System.out.println("The city with the highest average rainfall is: " + pincode + " - " + city + " - " + rainFall);
			}
		}
		catch(SQLException e) {
			e.printStackTrace();
		}
		
	}
	private static Connection getConnection() {

		try {
			Class.forName("org.postgresql.Driver");
			Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost:5432/JDBC", "postgres",
					"Home23");
			return conn;
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}
}
