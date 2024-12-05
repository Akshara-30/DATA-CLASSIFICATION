package com.lti.knowledge.service;

import com.lti.common.exception.CustomException;
import com.lti.knowledge.to.SecretsTO;

public interface SecretsService {
	
	public void saveSecret(SecretsTO secretsInput,String secretId) throws CustomException;
	
	public void updateSecret(SecretsTO secretsInput,String secretId) throws CustomException;

	public void deleteSecret(int pcmId, String secretId)throws CustomException;
	
	public String fetchAccessToken(int pcmId, String secretId)throws CustomException;
	

}
