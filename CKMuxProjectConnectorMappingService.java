package com.lti.knowledge.service;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.springframework.lang.NonNull;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.lti.common.exception.CustomException;
import com.lti.common.to.ProjectConnectorMappingTO;
import com.lti.common.to.ResponseTO;
import com.lti.common.to.SyncDetailsTO;
import com.lti.knowledge.entities.ConnectorHubEntity;
import com.lti.knowledge.entities.ProjectConnectorMappingEntity;

public interface CKMuxProjectConnectorMappingService {

	boolean verifyConfigDetailsWithToolServices(@NonNull ProjectConnectorMappingTO pcmTO) throws CustomException;

	ConnectorHubEntity validateAndVerifyPCMConfigDetails(ProjectConnectorMappingTO pcmTO) throws CustomException;

	ProjectConnectorMappingTO saveProjectConnector(ProjectConnectorMappingTO pcmTo) throws CustomException;

	ProjectConnectorMappingTO getProjectConnector(int projectConnectorId) throws CustomException;

	List<ProjectConnectorMappingTO> getProjectConnectors(Boolean returnSensitive) throws CustomException;

	ProjectConnectorMappingTO updateProjectConnector(ProjectConnectorMappingTO pcmTo, int projectConnectorId)
			throws CustomException,JsonProcessingException;

	boolean deleteProjectConnector(Integer projectConnectorId) throws CustomException;

	List<ProjectConnectorMappingEntity> getPCMDetailsByConnectorId(int connectorId) throws CustomException;

	ProjectConnectorMappingEntity findByPcmId(int projectConnectorId) throws CustomException;

	ProjectConnectorMappingEntity findByPCMIdWithAssociations(int projectConnectorId) throws CustomException;

	List<ProjectConnectorMappingEntity> getPCMListToSync(int projectId) throws CustomException;

	List<ProjectConnectorMappingEntity> setSyncStatusAsRunning(List<ProjectConnectorMappingEntity> pcmListToSync,int projectId) throws CustomException;

	@NonNull
	List<ProjectConnectorMappingEntity> getPCMListWithRunningStatus();

	CompletableFuture<ResponseTO<SyncDetailsTO>> syncSdlcData(ProjectConnectorMappingEntity pcmEntity,
		boolean resumeInterruptedSync);

	CompletableFuture<SyncDetailsTO> syncSdlcHistoryData(ProjectConnectorMappingEntity pcmEntity,
		boolean resumeInterruptedSync);

	@NonNull
	List<SyncDetailsTO> checkPCMSyncStatus(List<SyncDetailsTO> pcmIds);

	String updateLastSyncDateById(Integer projectConnectorId, String sycnStatus, String syncMsg) throws CustomException;

	String updateResyncFieldById(Integer projectConnectorId, String reSycnField);

	String updateResyncHistoryFieldById(Integer projectConnectorId, String reSycnHistoryField);

	List<ProjectConnectorMappingTO> fetchPCMForProject(int projectId) throws CustomException;

	Integer fetchPCMRowCount(String toolApiUrl, String testPlanFolderName, Integer projectId, Integer connectorId)
			throws CustomException;
	
	Integer fetchADORowCount(String toolApiUrl, String testPlanFolderName, Integer projectId, Integer connectorId, String teams)
			throws CustomException;

	Integer fetchPCMGitLabRowCount(String toolApiUrl, String testPlanFolderName, int projectId, String branchName,
			int connectorId) throws CustomException;

	List<ProjectConnectorMappingTO> fetchPCMForSessionProject(int projectId) throws CustomException;

}
