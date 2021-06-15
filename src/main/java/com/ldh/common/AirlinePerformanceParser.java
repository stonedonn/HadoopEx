package com.ldh.common;

import org.apache.hadoop.io.Text;

public class AirlinePerformanceParser {
	// 날짜에 관한 필드 정의
	private int year;
	private int month;
	private int day;
	
	// 출발,도착 지연 시간에 대한 필드 정의
	private int arrivalDelayTime = 0;
	private int departureDelayTime = 0;
	private int distance = 0;
	
	private boolean arrivalDelayAvailable = true;
	private boolean departureDelayAvailable = true;
	private boolean distanceAvailable = true;
	
	// 항공사에 대한 고유번호 필드 정의
	private String uniqueCarrier;
	
	public AirlinePerformanceParser(Text text) {
		try {
			String[] columns = text.toString().split(",");
			
			year = Integer.parseInt(columns[0]);
			month = Integer.parseInt(columns[1]);
			day = Integer.parseInt(columns[2]);
			uniqueCarrier = columns[5];
			
			if(!columns[16].equals("")) {
				departureDelayTime = (int)Float.parseFloat(columns[16]); 
			} else {
				departureDelayAvailable = false;
			}
			
			if(!columns[28].equals("")) {
				arrivalDelayTime = (int)Float.parseFloat(columns[16]); 
			} else {
				arrivalDelayAvailable = false;
			}
			if(!columns[37].equals("")) {
				distance = (int)Float.parseFloat(columns[16]); 
			} else {
				distanceAvailable = false;
			}
			
			
			
		} catch (Exception e) {
			// TODO: handle exception
		}
	}

	public int getYear() {
		return year;
	}

	public int getMonth() {
		return month;
	}

	public int getDay() {
		return day;
	}

	public int getArrivalDelayTime() {
		return arrivalDelayTime;
	}

	public int getDepartureDelayTime() {
		return departureDelayTime;
	}

	public int getDistance() {
		return distance;
	}

	public boolean isArrivalDelayAvailable() {
		return arrivalDelayAvailable;
	}

	public boolean isDepartureDelayAvailable() {
		return departureDelayAvailable;
	}

	public boolean isDistanceAvailable() {
		return distanceAvailable;
	}

	public String getUniqueCarrier() {
		return uniqueCarrier;
	}

	
	
}
