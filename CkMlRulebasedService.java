package com.lti.knowledge.service;

import java.util.Map;

import com.ltimindtree.exception.CustomException;

public interface CkMlRulebasedService {

	Map<String, Object> RuleBasedforDefects(Map<String, Object> requestBody, String authToken) throws CustomException;

	Map<String, Object> RuleBasedforUserstories(Map<String, Object> requestBody, String authToken)throws CustomException;

	Map<String, Object> RuleBasedforTestcases(Map<String, Object> requestBody, String authToken)throws CustomException;

	Map<String, Object> RuleBasedforSourcecode(Map<String, Object> requestBody, String authToken)throws CustomException;

}
