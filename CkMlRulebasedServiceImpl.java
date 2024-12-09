package com.lti.knowledge.service.impl;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import com.lti.common.utils.RestTemplateUtil;
import com.lti.knowledge.constants.CkMlRulebasedConstants;
import com.lti.knowledge.service.CkMlRulebasedService;
import com.ltimindtree.exception.CustomException;

import ch.qos.logback.classic.Logger;

@Service
public class CkMlRulebasedServiceImpl implements CkMlRulebasedService {

	
	@Value("${ML_SERVER_IP}")
	private String ML_IP;
	
	@Value("${ML_RULE_BASED}")
	private String ML_PORT;
	
	@Autowired
	private RestTemplate externalRestTemplate;
	
	private static final Logger logger = (Logger) LoggerFactory.getLogger(CkMlRulebasedServiceImpl.class);




	@Override
	public Map<String, Object> RuleBasedforDefects(Map<String, Object> requestBody,String token) throws CustomException {
		// TODO Auto-generated method stub
		Map<String, String> headers = new HashMap<>();
		headers.put("Authorization", token);

		Map<String, Object> responseData= new HashMap<>();
		try {
			responseData = RestTemplateUtil.executePostMethod(CkMlRulebasedConstants.HTTP+ ML_IP + CkMlRulebasedConstants.COLON + ML_PORT +
					CkMlRulebasedConstants.DEFECTS_URI, requestBody, externalRestTemplate, headers);

		}  catch (CustomException e) {
			if(e.getHttpStatus().equals(HttpStatus.SERVICE_UNAVAILABLE)) {
				throw new CustomException(HttpStatus.INTERNAL_SERVER_ERROR,CkMlRulebasedConstants.ERROR_MSG_011);
			}
			else {
				throw e;
			}
		}

		return responseData;
	}




	@Override
	public Map<String, Object> RuleBasedforUserstories(Map<String, Object> requestBody, String token)throws CustomException {
		Map<String, String> headers = new HashMap<>();
		headers.put("Authorization", token);

		Map<String, Object> responseData= new HashMap<>();
		try {
			responseData = RestTemplateUtil.executePostMethod(CkMlRulebasedConstants.HTTP+ ML_IP + CkMlRulebasedConstants.COLON + ML_PORT +CkMlRulebasedConstants.USERSTORIES_URI, requestBody, externalRestTemplate, headers);

		}catch (CustomException e) {
			if(e.getHttpStatus().equals(HttpStatus.SERVICE_UNAVAILABLE)) {
				throw new CustomException(HttpStatus.INTERNAL_SERVER_ERROR,CkMlRulebasedConstants.ERROR_MSG_011);
			}
			else {
				throw e;
			}
		}


		return responseData;
		
	}




	@Override
	public Map<String, Object> RuleBasedforTestcases(Map<String, Object> requestBody, String token)throws CustomException {
		Map<String, String> headers = new HashMap<>();
		headers.put("Authorization", token);

		Map<String, Object> responseData= new HashMap<>();
		try {
			responseData = RestTemplateUtil.executePostMethod(CkMlRulebasedConstants.HTTP+ ML_IP + CkMlRulebasedConstants.COLON + ML_PORT +CkMlRulebasedConstants.TESTCASES_URI, requestBody, externalRestTemplate, headers);

		}catch (CustomException e) {
			if(e.getHttpStatus().equals(HttpStatus.SERVICE_UNAVAILABLE)) {
				throw new CustomException(HttpStatus.INTERNAL_SERVER_ERROR,CkMlRulebasedConstants.ERROR_MSG_011);
			}
			else {
				throw e;
			}
		}

		return responseData;
	}




	@Override
	public Map<String, Object> RuleBasedforSourcecode(Map<String, Object> requestBody, String token)throws CustomException {
		Map<String, String> headers = new HashMap<>();
		headers.put("Authorization", token);

		Map<String, Object> responseData= new HashMap<>();
		try {
			responseData = RestTemplateUtil.executePostMethod(CkMlRulebasedConstants.HTTP+ ML_IP + CkMlRulebasedConstants.COLON + ML_PORT +CkMlRulebasedConstants.SOURCECODE_URI, requestBody, externalRestTemplate, headers);

		} catch (CustomException e) {
			if(e.getHttpStatus().equals(HttpStatus.SERVICE_UNAVAILABLE)) {
				throw new CustomException(HttpStatus.INTERNAL_SERVER_ERROR,CkMlRulebasedConstants.ERROR_MSG_011);
			}
			else {
				throw e;
			}
		}

		return responseData;
	}
}