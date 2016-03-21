package com.michaels.integration;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FilenameFilter;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;

import org.apache.log4j.*;

public class RetekIntegration {

	private static Logger logger = Logger.getLogger(RetekIntegration.class.getSimpleName());
	
	// constant for accessing the input column index
	private static final int SKU = 0;
	private static final int UPC = 1;
	private static final int PRODUCT = 2;
	private static final int ITEM_DESC = 3;
	private static final int DEPARTMENT = 4;
	private static final int CLASS = 5;
	private static final int SUBCLASS = 6;
	private static final int US_PRICE = 14;   
	private static final int CND_PRICE = 16;  

	private String sessionToken = "";
	private String notificationPolicyStr;
	
	private String customerRoleRef;
	
	// hashmaps used to store service fields
	// references and data types. Assumed
	// application is not a multi-threaded app
	private HashMap<String, String> fieldRefs = new HashMap<String, String>();
	private HashMap<String, String> fieldRefTypes = new HashMap<String, String>();
	
	// hashmap to store existing services by SKU field
	private HashMap<String, String> serviceBySKUMap = new HashMap<String, String>();
	
	// hashmap to store existing servicesCategory field
	private HashMap<String, String> serviceCategoryMap = new HashMap<String, String>();
		
	// hashmap to store class code to process
	private HashMap<String, String> classToProcessMap = new HashMap<String, String>();
		
	// hardcoded json strings
	private final String GROUP_GENERAL_TIME = "\"time\":null";
	private final String INDIVIDUAL_GENERAL_TIME = "\"time\":{\"length\":null,\"startTimes\":[[0,0,0],[15,0,0],[30,0,0],[45,0,0]]}";
	
	private final String ADDONS = "{\"addOnRules\":{\"maxAddOns\":{},\"maxTime\":0,\"minAddOns\":{},\"minTime\":0},\"addOns\":[],";
	private final String EVENTSOURCE_GENERAL = "\"eventSource\":[\"new\",{\"alternatives\":[],\"general\":{\"cost\":{{$$$$}},\"fields\":{},\"locations\":[\"any\",[]]," + 
	"\"roles\":{\"%s\":{\"autoAttendee\":null,\"roleDetails\":{\"attendeeFields\":{},\"matchingThingFields\":{},\"maxAttendees\":1,\"paddingAfter\":0,\"paddingBefore\":0,\"thingFields\":{}}}},%s},";
	private final String GROUP_SERVICE_TYPE = "\"serviceCategory\":\"%s\",\"window\":[[3600000,true],\"Infinity\"]}]";
	private final String INDIVIDUAL_SERVICE_TYPE = "\"serviceCategory\":\"individualService\",\"window\":[[3600000,true],\"Infinity\"]}]";
	
	private int nbrOfInputRead = 0;
	private int nbrOfInputSkipped = 0;
	private int nbrOfSvcCreated = 0;
	private int nbrOfSvcUpdated = 0;
	private int nbrOfErrors = 0;
	
	private String serverURI;
	
	public RetekIntegration() {
		serverURI = AppProperties.getInstance().getServerURI();
	}
	
	private String sendGetRequest(String urlString) throws Exception {
		URL url = new URL(urlString);
		HttpURLConnection conn = (HttpURLConnection) url.openConnection();
		conn.setRequestMethod("GET");
		conn.setRequestProperty("Accept", "application/json");

		if (conn.getResponseCode() != HttpURLConnection.HTTP_OK
				&& conn.getResponseCode() != HttpURLConnection.HTTP_CREATED) {
			throw new RuntimeException("Failed : HTTP error code : " + conn.getResponseCode());
		}

		BufferedReader br = new BufferedReader(new InputStreamReader((conn.getInputStream())));

		StringBuffer sb = new StringBuffer("");
		String line;
		while ((line = br.readLine()) != null) {
			sb.append(line);
		}

		conn.disconnect();
		return sb.toString();
	}
	
	private String sendPostRequest(String urlString, String param) throws Exception {
		URL url = new URL(urlString);
		HttpURLConnection conn = (HttpURLConnection) url.openConnection();
		conn.setDoOutput(true);
		conn.setDoInput(true);
		conn.setRequestMethod("POST");
		conn.setRequestProperty("Content-Type", "application/json");

		OutputStream os = conn.getOutputStream();
		os.write(param.getBytes());
		os.flush();

		if (conn.getResponseCode() != HttpURLConnection.HTTP_OK
				&& conn.getResponseCode() != HttpURLConnection.HTTP_CREATED) {
			throw new RuntimeException("Failed : HTTP error code : " + conn.getResponseCode());
		}

		BufferedReader br = new BufferedReader(new InputStreamReader((conn.getInputStream())));

		StringBuffer sb = new StringBuffer("");
		String line;
		while ((line = br.readLine()) != null) {
			sb.append(line);
		}
		
		conn.disconnect();
		return sb.toString();
	}

	private boolean initData() {
		// save class codes to indicate
		// input line to process
		String[] classes = AppProperties.getInstance().getSelectClassCodes().split("\\|");
		for(String strClass : classes) {
			classToProcessMap.put(strClass, strClass);
		}
		
		logger.trace("classToProcessMap:" + classToProcessMap);
		
		// prepare skedge specific strings for
		// inserting/updating service. 
		getRefsFromFrontend();
		getServiceFieldRefs();
		if (verifyServiceFieldRefs()) {
			getNotificationPolicy();
			readExistingServiceCategory();
			readExistingServiceSKU();
			return true;
		}
		
		return false;
	}
	
	private boolean authenticate() {
		// System.out.println("Connecting to " + serverURI + " ...");
		logger.debug("Authenticating...");
		logger.debug("Connecting to " + serverURI + " ...");

		String urlString = serverURI + "/auth/email/authenticate";
		String param = String.format("{\"request\": {\"email\":\"%s\",\"password\":\"%s\"}, \"token\": \"%s\"}",
		AppProperties.getInstance().getUsername(), AppProperties.getInstance().getPwd(),
		AppProperties.getInstance().getToken());
		
		try {
			String response = sendPostRequest(urlString, param);
			try {
				JSONObject jsonObj = new JSONObject(response);
				sessionToken = ((JSONObject) jsonObj.get("envelopeContents")).get("token").toString();
				logger.debug("Logged in. Session token: " + sessionToken);

			} catch (JSONException e) {
				logger.error("Exception in authenticate()" + e.getMessage());
				e.printStackTrace();
			}

		} catch (MalformedURLException e) {
			logger.error("MalformedURLException:" + e.getMessage());
		} catch (IOException e) {
			logger.error("IOException:" + e.getMessage());
		} catch (Exception e) {
			logger.error("Could not authenticate successfully, HTTP error code : " + e.getMessage());
		}
		
		return sessionToken.length() > 0;

	}

	// store all service SKU's in a hashmap
	// so we can check if the service by
	// the SKU already exist in the system.
	// mapping is SKU value/Service ref
	private void readExistingServiceSKU() {
		String params = "{\"request\":[\"any\",[]],\"token\":\"" + sessionToken + "\"}";

		String urlString = serverURI + "/service/findAndRead?q=" + params;

		logger.debug("In readExistingServiceSKU(). Requesting: " + urlString);

		String skuRef = fieldRefs.get("SKU").toString();

		String response;
		try {
			response = sendGetRequest(urlString);
			
			try {
				JSONObject jsonObj = new JSONObject(response);
				if (jsonObj.has("envelopeContents")) {
					JSONObject services = (JSONObject) jsonObj.get("envelopeContents");

					logger.trace("Found: " + services.length() + " existing services");
					
					for (int i = 0; i < services.length(); i++) {
						String serviceRef = (String) services.names().get(i);
						JSONObject jsonFields = ((JSONObject) services.get(serviceRef)).getJSONObject("fields");

						if (jsonFields.has(skuRef)) {
							JSONArray strs = (JSONArray) jsonFields.get(skuRef);
							logger.trace("Inserting " + strs.get(1).toString() + " - " + serviceRef + " to serviceBySKUMap");
							serviceBySKUMap.put(strs.get(1).toString(), serviceRef);
						}
					}
				}

			} catch (JSONException e) {
				logger.error("JSONException in readExistingServiceSKU():" + e.getMessage());
				e.printStackTrace();
			}

		} catch (MalformedURLException e) {
			logger.error("MalformedURLException in readExistingServiceSKU():" + e.getMessage());
			e.printStackTrace();
		} catch (IOException e) {
			logger.error("IOException in readExistingServiceSKU():" + e.getMessage());
			e.printStackTrace();
		} catch(Exception e) {
			logger.error("Exception in readExistingServiceSKU():" + e.getMessage());
		}


	}

	// store all service categories in a hashmap
	// so we can check if a category service
	// already exist in the system.
	// mapping is categoryService name/categoryService ref
	private void readExistingServiceCategory() {
		String params = "{\"request\":[\"any\",[]],\"token\":\"" + sessionToken + "\"}";

		String urlString = serverURI + "/serviceCategory/findAndRead?q=" + params;
		
		logger.debug("In readExistingServiceCategory(). Requesting: " + urlString);
		
		try {
			
			String response = sendGetRequest(urlString);
			
			try {
				JSONObject jsonObj = new JSONObject(response);
				if (jsonObj.has("envelopeContents")) {
					JSONObject serviceCategories = (JSONObject) jsonObj.get("envelopeContents");
					
					for (int i = 0; i < serviceCategories.length(); i++) {
						String svcCategoryRef = (String) serviceCategories.names().get(i);
						String svcCategoryName = ((JSONObject) serviceCategories.get(svcCategoryRef)).getString("name");
						
						logger.info("Inserting: " + svcCategoryName + " - " + svcCategoryRef);
						
						serviceCategoryMap.put(svcCategoryName, svcCategoryRef);
					}
				}
			} catch (JSONException e) {
				logger.error("JSONException in readExistingServiceCategory():" + e.getMessage());
				e.printStackTrace();
			}
			
		} catch (MalformedURLException e) {
			logger.error("MalformedURLException in readExistingServiceCategory():" + e.getMessage());
			e.printStackTrace();
		} catch (IOException e) {
			logger.error("IOException in readExistingServiceCategory():" + e.getMessage());
			e.printStackTrace();
		} catch (Exception e) {
			logger.error("Exception in readExistingServiceCategory():" + e.getMessage());
		}
	}

	// get field references from frontend
	// we need to get the customerRoleRef
	private void getRefsFromFrontend() {
		String params = "{\"request\":[],\"token\":\"" + sessionToken + "\"}";

		String urlString = serverURI + "/frontend/frontend?q=" + params;

		logger.debug("In getRefsFromFrontend(). Requesting: " + urlString);
		try {

			String response = sendGetRequest(urlString);

			try {
				JSONObject jsonObj = new JSONObject(response);
				if (jsonObj.has("envelopeContents")) {
					JSONObject content = jsonObj.getJSONObject("envelopeContents");

					// System.out.println("content: " + content);
					if (content.has("customerRoleRef")) {
						customerRoleRef = content.getString("customerRoleRef").toString();
					}
				}
			} catch (JSONException e) {
				logger.error("JSONException in getRefsFromFrontend():" + e.getMessage());
				e.printStackTrace();
			}

		} catch (MalformedURLException e) {
			logger.error("MalformedURLException in getRefsFromFrontend():" + e.getMessage());
			e.printStackTrace();
		} catch (IOException e) {
			logger.error("IOException in getRefsFromFrontend():" + e.getMessage());
			e.printStackTrace();
		} catch (Exception e) {
			logger.error("Exception in getRefsFromFrontend():" + e.getMessage());
			e.printStackTrace();
		}
	}
	
	private void getNotificationPolicy() {
			String params = "{\"request\":{\"eventType\":[\"event\",[]]},\"token\":\"" + sessionToken + "\"}";

			String urlString = serverURI + "/thing/genericEventNotificationPolicy?q=" + params;

			logger.debug("In getNotificationPolicy(). Requesting: " + urlString);
			try {
				String response = sendGetRequest(urlString);
				
				try {
					JSONObject jsonObj = new JSONObject(response);
					if (jsonObj.has("envelopeContents")) {
						JSONObject content = jsonObj.getJSONObject("envelopeContents");
						notificationPolicyStr = content.toString();
					}
				} catch (JSONException e) {
					logger.error("JSONException in getNotificationPolicy():" + e.getMessage());
					e.printStackTrace();
				}
				
		} catch (MalformedURLException e) {
			logger.error("MalformedURLException in getNotificationPolicy():" + e.getMessage());
		} catch (IOException e) {
			logger.error("IOException in getNotificationPolicy():" + e.getMessage());
		} catch (Exception e) {
			logger.error("Exception in getNotificationPolicy():" + e.getMessage());
		}

	}
	
	// get all references for the service fields and store
	// them in the hashmap
	private void getServiceFieldRefs() {
		String params = "{\"request\":[\"any\",[]],\"token\":\"" + sessionToken + "\"}";

		String urlString = serverURI + "/serviceField/findAndRead?q=" + params;
		logger.debug("In getServiceFieldRefs(). Requesting: " + urlString);
		
		try {
			String response = sendGetRequest(urlString);
			
			try {
				JSONObject jsonObj = new JSONObject(response);
				if (jsonObj.has("envelopeContents")) {
					JSONObject content = jsonObj.getJSONObject("envelopeContents");
					
					for (int i = 0; i < content.length(); i++) {
						String fieldRef = (String) content.names().get(i);
						String fieldName = ((JSONObject) content.get(fieldRef)).get("name").toString();
						
						JSONArray dataTypeArray = (JSONArray) ((JSONObject) content.get(fieldRef)).get("type");
						String dataType = dataTypeArray.getString(0);
						
						// save the field reference and data type
						// for later use
						fieldRefs.put(fieldName, fieldRef);
						fieldRefTypes.put(fieldName, dataType);
					}
				}
			} catch (JSONException e) {
				logger.error("JSONException in getServiceFieldRefs():" + e.getMessage());
				e.printStackTrace();
			}
			
		} catch (MalformedURLException e) {
			logger.error("MalformedURLException in getServiceFieldRefs():" + e.getMessage());
		} catch (IOException e) {
			logger.error("IOException in getServiceFieldRefs():" + e.getMessage());
		} catch (Exception e) {
			logger.error("Exception in getServiceFieldRefs():" + e.getMessage());
		}
	}


	private void createServiceCategory(String svcCategory) {
		
		String param = "{\"request\":{\"value\":" + 
				String.format(
				"{\"color\":\"%s\",\"name\":\"%s\",\"notificationPolicy\":%s}},\"token\":\"%s\"}",
				AppProperties.getInstance().getDefaultCalendarColor(), svcCategory, notificationPolicyStr, sessionToken);
		
		String urlString = serverURI + "/serviceCategory/create";
				
		logger.debug("In createServiceCategory(). Requesting: " + urlString);
		logger.trace("param: " + param);
		
		try {
			String response = sendPostRequest(urlString, param);
			
			try {
				// need to collect the new service category
				// reference and store it in hashmap
				JSONObject jsonObj = new JSONObject(response);
				if (jsonObj.has("envelopeContents")) {
					String serviceCategoryRef = jsonObj.get("envelopeContents").toString();
					
					serviceCategoryMap.put(svcCategory, serviceCategoryRef);
				}
			} catch (JSONException e) {
				logger.error("JSONException in createServiceCategory():" + e.getMessage());
				e.printStackTrace();
			}
		
		} catch (MalformedURLException e) {
			logger.error("MalformedURLException in createServiceCategory():" + e.getMessage());
		} catch (IOException e) {
			logger.error("IOException in createServiceCategory():" + e.getMessage());
		} catch (Exception e) {
			logger.error("Exception in createServiceCategory():" + e.getMessage());
		} 
	}

	private String fixDoubleQuotes(String name) {
		// change input from ie:
		// "COLORED STICKER 5X7""" ==> COLORED STICKER 5X7\"
		// "PS-20234 ""I LOVE YOU"" JEWELRY" ==> PS-20234 \"I LOVE YOU\" JEWELRY"
		// we need to have '\"' instead of just '"' because we are sending to
		// the web service as an input parameter
		if (name.startsWith("\"") && name.endsWith("\"")) {
			String noWrappingQuote = name.substring(1, name.length()-1);
			return noWrappingQuote.replace("\"\"", "\\\"");
		}
		return name;
	}
	
	// returns the price in cents
	// for example 12.5 => 1250, 9 => 900, 12.99 => 1299
	private int getPrice(String s) {
		if (s.length() > 0) {
			float f = Float.parseFloat(s);
			return ((int) (100 * f));
		}
		return 0;
	}

	private String getGroupServiceParamStr(String[] cols) {
		
		String itemDesc = fixDoubleQuotes(cols[ITEM_DESC]);
		String[] departments = cols[DEPARTMENT].split("-");
		String[] classes = cols[CLASS].split("-");
		String[] subClasses = cols[SUBCLASS].split("-");
		
		String skuField = String.format("\"%s\":[\"%s\",\"%s\"]", fieldRefs.get("SKU"), fieldRefTypes.get("SKU"), cols[SKU]);
		String upcField = String.format("\"%s\":[\"%s\",\"%s\"]", fieldRefs.get("UPC"), fieldRefTypes.get("UPC"), cols[UPC]);
		String productField = String.format("\"%s\":[\"%s\",\"%s\"]", fieldRefs.get("Product"), fieldRefTypes.get("Product"), cols[PRODUCT]);
		String deptField = String.format("\"%s\":[\"%s\",\"%s\"]", fieldRefs.get("Department"), fieldRefTypes.get("Department"), departments[1]);
		String deptNbrField = String.format("\"%s\":[\"%s\",\"%s\"]", fieldRefs.get("Department number"), fieldRefTypes.get("Department number"), departments[0]);
		String classField = String.format("\"%s\":[\"%s\",\"%s\"]", fieldRefs.get("Class"), fieldRefTypes.get("Class"), classes[1]);
		String classNbrField = String.format("\"%s\":[\"%s\",\"%s\"]", fieldRefs.get("Class number"), fieldRefTypes.get("Class number"), classes[1]);
		String subClassField = String.format("\"%s\":[\"%s\",\"%s\"]", fieldRefs.get("Subclass"), fieldRefTypes.get("Subclass"), subClasses[1]);
		String subClassNbrField = String.format("\"%s\":[\"%s\",\"%s\"]", fieldRefs.get("Subclass number"), fieldRefTypes.get("Subclass number"), subClasses[0]);
		String svcPublishedField = String.format("\"%s\":[\"%s\",%s]", fieldRefs.get("Published"), fieldRefTypes.get("Published"), "false");
		
		String svcCategoryRef = serviceCategoryMap.get(subClasses[1]);
		
		String s2 = String.format(EVENTSOURCE_GENERAL, customerRoleRef, GROUP_GENERAL_TIME );
		String s3 = String.format("\"notificationPolicy\":%s,", notificationPolicyStr) + 
				String.format(GROUP_SERVICE_TYPE, svcCategoryRef);
		String s4 = String.format(",\"fields\":{%s,%s,%s,%s,%s,%s,%s,%s,%s,%s},", 
				skuField, upcField, productField, deptField, deptNbrField, classField, classNbrField, subClassField, subClassNbrField, svcPublishedField);
		String s5 = String.format("\"name\":\"%s\"}", itemDesc);

		return ADDONS + s2 + s3 + s4 + s5;
	}
	
	private String getIndividualSvcParamStr(String[] cols) {
		String itemDesc = fixDoubleQuotes(cols[ITEM_DESC]);
		
		String skuField = String.format("\"%s\":[\"%s\",\"%s\"]", fieldRefs.get("SKU"), fieldRefTypes.get("SKU"), cols[SKU]);
		String upcField = String.format("\"%s\":[\"%s\",\"%s\"]", fieldRefs.get("UPC"), fieldRefTypes.get("UPC"), cols[UPC]);
		String deptField = String.format("\"%s\":[\"%s\",\"%s\"]", fieldRefs.get("Department"), fieldRefTypes.get("Department"), cols[DEPARTMENT]);
		String classField = String.format("\"%s\":[\"%s\",\"%s\"]", fieldRefs.get("Class"), fieldRefTypes.get("Class"), cols[CLASS]);
		String categoryField = String.format("\"%s\":[\"%s\",\"%s\"]", fieldRefs.get("Category"), fieldRefTypes.get("Category"), cols[SUBCLASS]);
		String productField = String.format("\"%s\":[\"%s\",\"%s\"]", fieldRefs.get("Product"), fieldRefTypes.get("Product"), cols[PRODUCT]);
		String svcPublishedField = String.format("\"%s\":[\"%s\",%s]", fieldRefs.get("Published"), fieldRefTypes.get("Published"), "false");
		
		
		String s1 = String.format("{\"request\":%s", ADDONS);
		
		String s2 = String.format(EVENTSOURCE_GENERAL, customerRoleRef, INDIVIDUAL_GENERAL_TIME );
		String s3 = String.format("\"notificationPolicy\":%s,%s", notificationPolicyStr, INDIVIDUAL_SERVICE_TYPE);
		String s4 = String.format(",\"fields\":{%s,%s,%s,%s,%s,%s,%s},", skuField, upcField, productField, deptField, classField, categoryField, svcPublishedField);
		String s5 = String.format("\"name\":\"%s\"}", itemDesc);

		return ADDONS + s2 + s3 + s4 + s5;
	}

	

	private String invokeWsAPI(String param, boolean isCanada, boolean createNew) {
		String serviceRef = "";

		boolean hasError = true;

		String urlString = isCanada ? (AppProperties.getInstance().getCndServerURI() + "/service/update")
				: serverURI + "/service";

		if (!isCanada && !createNew) {
			// updating existing service for US
			urlString += "/update";
		}

		logger.debug("In invokeWsAPI(). Requesting: " + urlString);

		logger.debug("param:" + param);

		try {
			String response = sendPostRequest(urlString, param);
			try {
				// need to collect the new service reference
				// if we just created a new service for US
				// as we need the ref when updating the CND
				// price
				if (createNew) {
					JSONObject jsonObj = new JSONObject(response);
					if (jsonObj.has("envelopeContents")) {
						serviceRef = jsonObj.get("envelopeContents").toString();
					}
				}
			} catch (JSONException e) {
				logger.error("JSONException in invokeWsAPI():" + e.getMessage());
				e.printStackTrace();
			}
			
			// if we come here, everything is OK
			hasError = false;
			
		} catch (MalformedURLException e) {
			logger.error("MalformedURLException in invokeWsAPI():" + e.getMessage());
			e.printStackTrace();
		} catch (IOException e) {
			logger.error("IOException in invokeWsAPI():" + e.getMessage());
			e.printStackTrace();
		} catch (Exception e) {
			logger.error("Exception in invokeWsAPI():" + e.getMessage());
			e.printStackTrace();
		} finally {
			if (hasError)
				nbrOfErrors++;
		}

		return serviceRef;
	}

	private void checkServiceCategory(String svcCategoryName) {
		// check if the service category exists.
		// create one if it does not
		if (svcCategoryName.length() > 0) {
			if (!serviceCategoryMap.containsKey(svcCategoryName)) {
				// create a new serviceCategory
				createServiceCategory(svcCategoryName);
			}
		}
	}
	
	private boolean serviceExists(String sku) {
		return serviceBySKUMap.containsKey(sku);
	}
	
	private boolean shouldProcessLine(String className) {
		String[] classes = className.split("-");
		return classToProcessMap.containsKey(classes[0]);
	}
	
	// actual work for creating or updating
	// a service
	private void processInputLine(String line) {
		String delimiter = "\\|";

		String[] cols = line.split(delimiter);

		if (cols.length <= 0) 
			return;
		
		String usPrice = "" + getPrice(cols[US_PRICE]);
		String cndPrice = "" + getPrice(cols[CND_PRICE]);
		// determine if this is a group or individual service
		// boolean isIndividualService = (cols[CLASS].trim().equals("4-PARTIES"));
		
		// determine if line should be skipped
		if (!shouldProcessLine(cols[CLASS])) {
			logger.trace("Skipping: " + line);
			nbrOfInputSkipped++;
			return;
		}
		
		// this should not be applied
		// String paramStr = isIndividualService ? getIndividualSvcParamStr(cols) : getGroupServiceParamStr(cols);

		String[] subClass = cols[SUBCLASS].split("-");
		if (subClass.length > 0) {
			checkServiceCategory(subClass[1]);
		}
		
		String paramStr = getGroupServiceParamStr(cols);
		
		String usParam;
		String existingSvcRef = "";
		boolean createNewService = true;
		
		if (serviceExists(cols[SKU])) { 
			logger.debug("*** Service with SKU:" + cols[SKU] + " already exists ***");
			
			// setup parameters for updating an existing service
			existingSvcRef = serviceBySKUMap.get(cols[SKU]).toString();
			usParam = "{\"request\":{\"ref\":\"" + existingSvcRef + "\",\"value\":" + paramStr.replace("{{$$$$}}", usPrice) 
			+ ",\"visibility\":null},\"token\":\"" + sessionToken + "\"}";
			createNewService = false;
		
		} else {
			// setup parameters for creating a new service
			usParam = "{\"request\":" + paramStr.replace("{{$$$$}}", usPrice) 
			+ ",\"token\":\"" + sessionToken + "\"}";
		}
		
		String serviceRef = invokeWsAPI(usParam, false, createNewService);
		if (serviceRef.length() == 0 && existingSvcRef.length() > 0) {
			serviceRef = existingSvcRef;
		}
		
		String cndParam = "{\"request\":{\"ref\":\"" + serviceRef + "\",\"value\":" + paramStr.replace("{{$$$$}}", cndPrice) +
				",\"visibility\":null},\"token\":\"" + sessionToken + "\"}";
		
		// call for Canada price is always an update
		invokeWsAPI(cndParam, true, false);
		
		nbrOfInputRead++;
		if (createNewService) {
			nbrOfSvcCreated++;
		} else {
			nbrOfSvcUpdated++;
		}
		
		// we also need to update the hashmap
		// for existing service when adding new service
		if (createNewService) {
			serviceBySKUMap.put(cols[SKU], serviceRef);
		}
	}

	// return true if all required service fields
	// are defined in the skedge system, false
	// otherwise
	private boolean verifyServiceFieldRefs() {
		if (!fieldRefs.containsKey("SKU")) {
			logger.error("SKU field is not defined in Skedge");
			return false;
		}
		if (!fieldRefs.containsKey("UPC")) {
			logger.error("UPC field is not defined in Skedge");
			return false;
		}
		if (!fieldRefs.containsKey("Product")) {
			logger.error("Product field is not defined in Skedge");
			return false;
		}
		if (!fieldRefs.containsKey("Class")) {
			logger.error("Class field is not defined in Skedge");
			return false;
		}
		if (!fieldRefs.containsKey("Class number")) {
			logger.error("Class number field is not defined in Skedge");
			return false;
		}
		if (!fieldRefs.containsKey("Subclass")) {
			logger.error("Subclass field is not defined in Skedge");
			return false;
		}
		if (!fieldRefs.containsKey("Subclass number")) {
			logger.error("Subclass number field is not defined in Skedge");
			return false;
		}
		if (!fieldRefs.containsKey("Department")) {
			logger.error("Department field is not defined in Skedge");
			return false;
		}
		if (!fieldRefs.containsKey("Department number")) {
			logger.error("Department number field is not defined in Skedge");
			return false;
		}

		return true;
	}

	private void archiveFile(File file) {
		String filename = file.getName();
		String filePath = file.getAbsolutePath();
		File fileToMove = new File(file.getAbsolutePath());
		String newPath = AppProperties.getInstance().getArchivePath() + filename;
		boolean isMoved = fileToMove.renameTo(
				new File(newPath));
		if (isMoved) {
			logger.info("Moved file: " + filePath + " to: " + AppProperties.getInstance().getArchivePath() + "/" + filename);
		} else {
			logger.error("Failed to move file: " + filePath + " to: " + AppProperties.getInstance().getArchivePath() + "/" + filename);
		}
		
	}
	
	private void parseAndInjest(File file) {
		BufferedReader br = null;
		String line = "";
		
		try {

			br = new BufferedReader(new FileReader(file.getAbsolutePath()));
			while ((line = br.readLine()) != null) {
				if (line.startsWith("SKU|UPC|PRODUCT_CLASS"))
					continue;
				if (line.isEmpty()) {
					continue;
				}
				
				// do the actual work
				processInputLine(line);
			}
			
			// File successfully processed
			archiveFile(file);

		} catch (FileNotFoundException e) {
			logger.error("FileNotFoundException in parseAndInjest():" + e.getMessage());
		} catch (IOException e) {
			logger.error("IOException in parseAndInjest():" + e.getMessage());
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					logger.error("IOException in parseAndInjest():" + e.getMessage());
				}
			}
		}

	}

	private File[] getCSVFileList(String dirPath) {
        File dir = new File(dirPath);   
        File[] fileList = dir.listFiles( new FilenameFilter() {
			
			@Override
			public boolean accept(File dir, String name) {
				
				return (name.startsWith("item-") && name.endsWith(".csv"));
			}
		});
        
        return fileList;
    }

	public void processCSVFile() {

		File[] fileList = getCSVFileList(AppProperties.getInstance().getInjestPath());

		logger.debug("Processing files in " + AppProperties.getInstance().getInjestPath());

		if (fileList != null && fileList.length > 0) {

			// if we come here, that means
			// there are files to process and
			// need to prepare skedge stuff
			if (initData()) {
	
				for (File file : fileList) {
					logger.info("===========================================");
					logger.info("   Processing: " + file.getName());
					logger.info("===========================================");
	
					parseAndInjest(file);
				}
			} else {
				logger.error("All required Skedge service fields are not set up. Exiting application");
			}
		} else {
			logger.info("No item-xxxxxxxx.csv files to process");
		}
		
		logger.info("===========================================");
		logger.info("===    Retek Integration Completed      ===");
		logger.info("   Number of input lines read: " + nbrOfInputRead);
		logger.info("   Number of input lines skipped: " + nbrOfInputSkipped);
		logger.info("   Number of services created: " + nbrOfSvcCreated);
		logger.info("   Number of services updated: " + nbrOfSvcUpdated);
		logger.info("   Number of errors encountered: " + nbrOfErrors);
		logger.info("===========================================");

		// System.out.println("*** At end serviceBySKUMap entry:" +
		// serviceBySKUMap.size());

	}
	
  public static void main(String[] args) {
	  logger.info("===========================================");
	  logger.info("===      Retek Integration Started      ===");
	  logger.info("===========================================");
	  
	  RetekIntegration client = new RetekIntegration();
	  System.out.println("Authenticating...");
	  
	  if (client.authenticate()) {
		  System.out.println("Setting up integration...");
		  client.processCSVFile();
	  }
  }
}