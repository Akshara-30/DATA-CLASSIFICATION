package com.lti.knowledge.service.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lti.common.config.WSO2Config;
import com.lti.common.constants.CommonConstants;
import com.lti.common.constants.PathConstants;
import com.lti.common.exception.CustomException;
import com.lti.common.exception.Exceptions;
import com.lti.common.to.ElasticSyncDetailsTO;
import com.lti.knowledge.dao.ElasticConnectorAuditPropertiesDao;
import com.lti.knowledge.entities.ElasticConnectorAuditPropertiesEntity;
import com.lti.knowledge.service.CKMuxElasticConnectorAuditPropertiesService;
import com.lti.knowledge.to.ElasticConnectorAuditPropertiesTO;
import com.lti.knowledge.util.StoryUtils;

import io.micrometer.core.lang.NonNull;

@Service
public class CKMuxElasticConnectorAuditPropertiesServiceImpl implements CKMuxElasticConnectorAuditPropertiesService {

	private static final String ERROR01 = "Project Id %d not found";

	public static final Logger logger = LoggerFactory.getLogger(CKMuxElasticConnectorAuditPropertiesServiceImpl.class);

	@Autowired
	private ElasticConnectorAuditPropertiesDao elasticDao;

	@Autowired
	private WSO2Config wso2Config;

	@Autowired
	private RestTemplate wso2RestTemplate;

	@Autowired
	private ObjectMapper objectMapper;

	@Override
	public ElasticConnectorAuditPropertiesTO fetchAuditPropertiesByProjectId(Integer projectId) {
		ElasticConnectorAuditPropertiesTO auditPropertiesTO = null;
		Optional<ElasticConnectorAuditPropertiesEntity> auditPropertiesEntity = elasticDao.findById(projectId);
		if (auditPropertiesEntity.isPresent()) {
			auditPropertiesTO = new ElasticConnectorAuditPropertiesTO();
			BeanUtils.copyProperties(auditPropertiesEntity.get(), auditPropertiesTO);

			String clientIp = auditPropertiesEntity.get().getClientIp();
			if (clientIp != null)
				auditPropertiesTO.setClientIp(List.of(clientIp.split(",", -1)));

			String traceIds = auditPropertiesEntity.get().getTraceIds();
			if (traceIds != null)
				auditPropertiesTO.setTraceIds(List.of(traceIds.split(",", -1)));
		}
		return auditPropertiesTO;
	}

	@NonNull
	@Override
	public List<Integer> fetchProjectIdsWithRunningStatus() {
		return elasticDao.findAllProjectIdsBySyncStatus(CommonConstants.SYNC_RUNNING);
	}

	@Override
	public ElasticConnectorAuditPropertiesTO saveOrUpdateAuditProperties(
			ElasticConnectorAuditPropertiesTO auditPropertiesToSave) throws CustomException {
		ElasticConnectorAuditPropertiesEntity auditPropertiesEntity = new ElasticConnectorAuditPropertiesEntity();
		BeanUtils.copyProperties(auditPropertiesToSave, auditPropertiesEntity);

		boolean parallelTracing = true;

		try {
			List<String> clientIp = auditPropertiesToSave.getClientIp();
			if (clientIp != null) {
				auditPropertiesEntity.setClientIp(String.join(",", clientIp));
			}

			List<String> traceIds = auditPropertiesToSave.getTraceIds();
			if (traceIds != null) {
				auditPropertiesEntity.setTraceIds(String.join(",", traceIds));
			}

			ElasticConnectorAuditPropertiesTO fetchAuditPropertiesByProjectId = fetchAuditPropertiesByProjectId(
					auditPropertiesToSave.getProjectId());
			if (fetchAuditPropertiesByProjectId != null) {
				parallelTracing = fetchAuditPropertiesByProjectId.isParallelTracing();
			}

			auditPropertiesEntity.setParallelTracing(parallelTracing);
			auditPropertiesEntity = elasticDao.save(auditPropertiesEntity);

			String authUrl = wso2Config.url() + PathConstants.AUTHENTICATION;
			String token = StoryUtils.generateToken(authUrl, wso2RestTemplate, wso2Config.basicAuth(),
					wso2Config.apiKey());
			String uri = wso2Config.url() + PathConstants.SAVE_UPDATE_ELASTIC_AUDIT_PROPERTIES;
			HttpHeaders headers = new HttpHeaders();
			headers.add(CommonConstants.WSO2_ACCESS_TOKEN, token);
			headers.add(CommonConstants.API_KEY, wso2Config.apiKey());
			headers.setContentType(MediaType.APPLICATION_JSON);
			HttpEntity<Object> entity = new HttpEntity<>(objectMapper.writeValueAsString(auditPropertiesToSave),
					headers);
			StoryUtils.executePostRequest(wso2RestTemplate, uri, entity);
		} catch (Exception e) {
			throw new CustomException(Exceptions.ERROR050, e);
		}
		return auditPropertiesToSave;
	}

	@NonNull
	@Override
	public List<ElasticSyncDetailsTO> getElasticSyncDetails() {
		List<ElasticSyncDetailsTO> result = new ArrayList<>();

		List<ElasticConnectorAuditPropertiesEntity> auditPropertiesEntityList = elasticDao.findAll();

		for (ElasticConnectorAuditPropertiesEntity auditPropertiesEntity : auditPropertiesEntityList) {
			ElasticSyncDetailsTO syncDetails = new ElasticSyncDetailsTO();
			syncDetails.setProjectId(auditPropertiesEntity.getProjectId());
			syncDetails.setSyncStatus(auditPropertiesEntity.getSyncStatus());
			syncDetails.setSyncMsg(auditPropertiesEntity.getSyncMsg());
			syncDetails.setLastSyncDate(auditPropertiesEntity.getLastSyncDate());

			result.add(syncDetails);
		}

		return result;
	}

	@NonNull
	@Override
	public List<ElasticSyncDetailsTO> checkElasticSyncStatus(List<Integer> projectIds) {
		List<ElasticSyncDetailsTO> result = new ArrayList<>();

		if (CollectionUtils.isEmpty(projectIds)) {
			return result;
		}

		// Received projectIds will be in RUNNING Sync status
		// Check if the Sync status is not RUNNING
		// And return the lastSyncDate
		List<ElasticConnectorAuditPropertiesEntity> auditPropertiesEntityList = elasticDao.findAllById(projectIds);
		for (ElasticConnectorAuditPropertiesEntity auditPropertiesEntity : auditPropertiesEntityList) {
			// Sync status is not RUNNING -> Sync completed / failed
			if (!CommonConstants.SYNC_RUNNING.equals(auditPropertiesEntity.getSyncStatus())) {
				ElasticSyncDetailsTO syncDetails = new ElasticSyncDetailsTO();
				syncDetails.setProjectId(auditPropertiesEntity.getProjectId());
				syncDetails.setSyncStatus(auditPropertiesEntity.getSyncStatus());
				syncDetails.setSyncMsg(auditPropertiesEntity.getSyncMsg());
				syncDetails.setLastSyncDate(auditPropertiesEntity.getLastSyncDate());
				result.add(syncDetails);
			}
		}

		return result;
	}

	@Override
	public boolean deleteAuditProperties(Integer projectId) throws CustomException {
		try {
			elasticDao.deleteById(projectId);
			String authUrl = wso2Config.url() + PathConstants.AUTHENTICATION;
			String token = StoryUtils.generateToken(authUrl, wso2RestTemplate, wso2Config.basicAuth(),
					wso2Config.apiKey());
			String uri = wso2Config.url()
					+ PathConstants.DELETE_AUDIT_PROPERTIES.replace("{projectId}", String.valueOf(projectId));
			HttpHeaders headers = new HttpHeaders();
			headers.add(CommonConstants.WSO2_ACCESS_TOKEN, token);
			headers.add(CommonConstants.API_KEY, wso2Config.apiKey());
			headers.setContentType(MediaType.APPLICATION_JSON);
			HttpEntity<Object> entity = new HttpEntity<>(headers);
			StoryUtils.executePostRequest(wso2RestTemplate, uri, entity);
			return true;
		} catch (EmptyResultDataAccessException e) {
			logger.error("Audit properties does not exists for project id: {}", projectId, e);
			throw new CustomException(HttpStatus.BAD_REQUEST,
					"Audit properties does not exists for project id: %d".formatted(projectId));
		} catch (Exception e) {
			logger.error("Error in deleting audit properties for project id: {}", projectId, e);
			throw new CustomException(Exceptions.ERROR058);
		}
	}

	@NonNull
	@Override
	public void updateSyncDetails(Integer projectId, String lastSyncDate, String syncStatus, String syncMsg)
			throws CustomException {
		try {
			elasticDao.updateSyncDetails(projectId, lastSyncDate, syncStatus, syncMsg);
			ElasticConnectorAuditPropertiesTO auditPropertiesToSave = new ElasticConnectorAuditPropertiesTO();
			auditPropertiesToSave.setProjectId(projectId);
			auditPropertiesToSave.setLastSyncDate(lastSyncDate);
			auditPropertiesToSave.setSyncStatus(syncStatus);
			auditPropertiesToSave.setSyncMsg(syncMsg);
			String authUrl = wso2Config.url() + PathConstants.AUTHENTICATION;
			String token = StoryUtils.generateToken(authUrl, wso2RestTemplate, wso2Config.basicAuth(),
					wso2Config.apiKey());
			String uri = wso2Config.url() + PathConstants.UPDATE_ELASTIC_SYNC_DETAILS;
			HttpHeaders headers = new HttpHeaders();
			headers.add(CommonConstants.WSO2_ACCESS_TOKEN, token);
			headers.add(CommonConstants.API_KEY, wso2Config.apiKey());
			headers.setContentType(MediaType.APPLICATION_JSON);
			HttpEntity<Object> entity = new HttpEntity<>(objectMapper.writeValueAsString(auditPropertiesToSave),
					headers);
			StoryUtils.executePostRequest(wso2RestTemplate, uri, entity);
		} catch (CustomException e) {
			throw e;
		} catch (Exception e) {
			throw new CustomException(Exceptions.ERROR050, e);
		}
	}

	@NonNull
	@Override
	public void updateUiDetails(Integer projectId, boolean uiTraceFlag, String uiTraceId, String username,
			String serviceName, String startTime, String stopTime, String clientIp) {
		elasticDao.updateUiDetails(projectId, uiTraceFlag, uiTraceId, username, serviceName, startTime, stopTime,
				clientIp);
	}

	@NonNull
	@Override
	public void updateFlag(String traceType, Integer projectId, boolean flag) throws CustomException {
		switch (traceType) {
		case "metrics":
			elasticDao.updateMetricsFlag(projectId, flag);
			break;

		case "transactionErrorTraces":
			elasticDao.updateTransactionErrorTracesFlag(projectId, flag);
			break;

		case "transactionTraces":
			elasticDao.updateTransactionTracesFlag(projectId, flag);
			break;

		case "dbTraces":
			elasticDao.updateDbTracesFlag(projectId, flag);
			break;

		default:
			throw new CustomException(HttpStatus.BAD_REQUEST, "Invalid trace type");
		}
	}

	@NonNull
	@Override
	public void updateSearchAfterTime(String traceType, Integer projectId, String searchAfterTime, String traceIds)
			throws CustomException {
		switch (traceType) {
		case "metrics":
			elasticDao.updateMetricsSearchAfterTime(projectId, searchAfterTime);
			break;

		case "transactionErrorTraces":
			elasticDao.updateTransactionErrorTracesSearchAfterTime(projectId, searchAfterTime);
			break;

		case "transactionTraces":
			elasticDao.updateTransactionTracesSearchAfterTime(projectId, searchAfterTime, traceIds);
			break;

		case "dbTraces":
			elasticDao.updateDbTracesSearchAfterTime(projectId, searchAfterTime);
			break;

		default:
			throw new CustomException(HttpStatus.BAD_REQUEST, "Invalid trace type");
		}
	}

	@Override
	public void updateParallelTracingFlag(Integer projectId, boolean tracingFlag) throws CustomException{
		
		if(fetchAuditPropertiesByProjectId(projectId)==null)
			throw new CustomException(HttpStatus.BAD_REQUEST, String.format(ERROR01,projectId));
		elasticDao.updateParallelTracingFlag(projectId, tracingFlag);
	}

}