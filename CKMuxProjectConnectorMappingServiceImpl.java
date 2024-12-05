package com.lti.knowledge.service.impl;

import static com.lti.common.constants.CommonConstants.SyncStatus.RUNNING;
import static com.lti.common.constants.CommonConstants.Tool.*;
import static com.lti.common.constants.CommonConstants.ToolCategory.VCS;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.data.util.Pair;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.lang.NonNull;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.reactive.function.client.WebClient;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lti.common.config.WSO2Config;
import com.lti.common.constants.CommonConstants;
import com.lti.common.constants.CommonConstants.SyncStatus;
import com.lti.common.constants.PathConstants;
import com.lti.common.exception.CustomException;
import com.lti.common.exception.ExceptionMessages;
import com.lti.common.exception.Exceptions;
import com.lti.common.to.ExcelFileDetailTO;
import com.lti.common.to.ProjectConnectorMappingTO;
import com.lti.common.to.ProjectTO;
import com.lti.common.to.ResponseTO;
import com.lti.common.to.SyncDetailsTO;
import com.lti.common.to.TeamTO;
import com.lti.common.to.ToolConfigDetailsTO;
import com.lti.common.utils.CustomValidators;
import com.lti.common.utils.RestTemplateUtil;
import com.lti.common.utils.WebClientUtil;
import com.lti.knowledge.dao.ConnectorHubDAO;
import com.lti.knowledge.dao.ProjectConnectorMappingDAO;
import com.lti.knowledge.entities.ConnectorHubEntity;
import com.lti.knowledge.entities.ExcelFileDetailEntity;
import com.lti.knowledge.entities.ProjectConnectorMappingEntity;
import com.lti.knowledge.service.CKMuxProjectConnectorMappingService;
import com.lti.knowledge.service.SecretsService;
import com.lti.knowledge.to.SecretsTO;
import com.lti.knowledge.util.StoryUtils;

import ch.qos.logback.classic.Logger;

@Service
public class CKMuxProjectConnectorMappingServiceImpl implements CKMuxProjectConnectorMappingService {

	private static final Logger logger = (Logger) LoggerFactory
		.getLogger(CKMuxProjectConnectorMappingServiceImpl.class);

	@Autowired
	private ProjectConnectorMappingDAO pcmDao;

	@Autowired
	private SecretsService secretService;

	@Autowired
	private ConnectorHubDAO connectorHubDao;

	@Autowired
	private ExcelFileUploadServiceImpl excelFileUploadService;

	@Autowired
	private FieldMappingServiceImpl fieldMappingService;

	@Autowired
	private WSO2Config wso2Config;

	@Autowired
	private RestTemplate wso2RestTemplate;

	@Autowired
	private WebClient.Builder internalWebClient;

	@Autowired
	private ObjectMapper objectMapper;

	private static final DateTimeFormatter dateTimeStandardFormat =
		DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS");

	@Value("${secret-id}")
	private String secretId;

	@Value("${secret.store.flag}")
	private String secretFlag;

	public static String MSG_001 = "Failed to fetch Projects";
	public static String PROJECT = "{projectId}";

	ConnectorHubEntity validatePCMConfigDetails(@NonNull ProjectConnectorMappingTO pcmTO) throws CustomException {
		// TODO: Verify - update validation code for remaining tool

		CustomValidators.validateNull(pcmTO, Map.of(
			"projectId", "Project ID",
			"connectorId", "Connector ID",
			"connectorName", "Connector Name",
			"toolCategoryId", "Tool Category ID",
			"toolCategory", "Tool Category Name",
			"toolId", "Tool ID",
			"toolName", "Tool Name"
		));
		if (!pcmTO.getToolName().equals(EXCEL.label)) {
			CustomValidators.validateNull(pcmTO, Map.of(
				"toolApiUrl", "Tool URL",
				"testPlanFolderName", "Tool Project",
				"accessToken", "Access Token"
			));
		}
		CustomValidators.validateNegative(pcmTO.getProjectConnectorId(), "Project Connector ID");

		CommonConstants.ToolCategory toolCategoryName = CommonConstants.ToolCategory.resolve(pcmTO.getToolCategory());
		CommonConstants.Tool toolName = CommonConstants.Tool.resolve(pcmTO.getToolName());

		if (toolName.equals(CommonConstants.Tool.EXCEL)) {
			CustomValidators.validateNull(pcmTO.getExcelFiles(), "Excel files");

			for (ExcelFileDetailTO fileDetail : pcmTO.getExcelFiles()) {
				CustomValidators.validateNull(fileDetail.getFileId(), "Excel file id");
				CustomValidators.length(fileDetail.getFileId(), "Excel file id", 36); // uuid length

				CustomValidators.fileFolderName(fileDetail.getFileName(), "Excel file name");

				// Sheet names are comma separated
				for (String sheetName : fileDetail.getSheets().split(",\\s*")) {
					CustomValidators.minmaxLength(sheetName, "Excel sheet: " + sheetName, 1, 31);
					CustomValidators.excelSheetName(sheetName);
				}
			}

			// cleanup
			pcmTO.setToolApiUrl(null);
			pcmTO.setTestPlanFolderName(null);
			pcmTO.setAccessToken(null);
			pcmTO.setUserName(null);
			pcmTO.setBranchName(null);
			pcmTO.setTeams(null);

		} else {
			CustomValidators.minmaxLength(pcmTO.getToolApiUrl(), "Tool API URL", 12, 128);
			CustomValidators.url(pcmTO.getToolApiUrl(), "Tool API URL");

			CustomValidators.minmaxLength(pcmTO.getAccessToken(), "Access Token", 8, 512);

			CustomValidators.username(pcmTO.getUserName(), "Username / Email ID");
			CustomValidators.minmaxLength(pcmTO.getUserName(), "Username / Email ID", 3, 50);

			if (toolCategoryName.equals(VCS)) {

				if (toolName.equals(CommonConstants.Tool.GITLAB)) {

					String projectPath = pcmTO.getTestPlanFolderName().trim().replaceAll("\\s+", "");
					if (projectPath.isEmpty()) {
						throw new CustomException(HttpStatus.BAD_REQUEST, "Project Path should not be empty");
					}

					if (projectPath.length() > 255) {
						throw new CustomException(HttpStatus.BAD_REQUEST,
							"Project Path should not contain more than 255 charcters");
					}
					CustomValidators.notStartWithForwardSlash(projectPath, "Project Path");
					CustomValidators.notEndWithforwardSlash(projectPath, "Project Path");
					if (projectPath.contains("/")) {
						String[] spiltted = projectPath.split("/");
						for (String part : spiltted) {
							if (part.isEmpty()) {
								throw new CustomException(HttpStatus.BAD_REQUEST, "Project Path should not be empty");
							}
						}
					}
					pcmTO.setTestPlanFolderName(projectPath);
				}
				if (toolName.equals(CommonConstants.Tool.RTC)) {
					CustomValidators.validateNull(pcmTO.getBranchName(), "Stream/Component Name");
					int index = pcmTO.getBranchName().indexOf("/");
					if (index == -1) {
						logger.error("Stream Name and Component Name should be separated by '/'");
						throw new CustomException(Exceptions.ERROR050);
					}
					CustomValidators.validateNull(pcmTO.getBranchName().substring(0, index), "Stream Name");
					CustomValidators.validateNull(pcmTO.getBranchName().substring(index + 1), "Component Name");
				}
				else
					CustomValidators.validateNull(pcmTO.getBranchName(), "Branch Name");
			} else {
				// cleanup
				pcmTO.setBranchName(null);
			}

			if (!toolName.equals(ADO) && !toolName.equals(AZURE_GIT)) {
				CustomValidators.validateNull(pcmTO.getUserName(), "Username / Email ID");
			}

			if (toolName.equals(CommonConstants.Tool.ADO) && !ObjectUtils.isEmpty(pcmTO.getTeams())) {

				List<TeamTO> teams = pcmTO.getTeams();

				for (int i = 0; i < teams.size(); i++) {
					String teamName = teams.get(i).getTeamName();
					List<String> areaPath = teams.get(i).getAreaPaths();

					checkIfTeamValid(teamName);
					checkIfAreaPathValid(areaPath);
				}
			} else {
				// cleanup
				pcmTO.setTeams(null);
			}
		}

		Optional<ConnectorHubEntity> connectorHubOptional = connectorHubDao
			.findByConnectorIdAndConnectorName(pcmTO.getConnectorId(), pcmTO.getConnectorName());

		if (connectorHubOptional.isEmpty()) {
			throw new CustomException(HttpStatus.BAD_REQUEST,
				"Provided Connector Id/Name (%s, %s) is invalid.".formatted(pcmTO.getConnectorId(),
					pcmTO.getConnectorName()));
		}

		ConnectorHubEntity connectorHub = connectorHubOptional.get();

		// Tool Id/Name
		if (!Objects.equals(connectorHub.getToolId(), pcmTO.getToolId()) ||
			!Objects.equals(connectorHub.getToolMasterEntity().getToolName(), pcmTO.getToolName())) {
			throw new CustomException(HttpStatus.NOT_FOUND,
				"Provided Tool Id/Name (%s, %s) doesn't belong to the provided Connector: %s"
					.formatted(pcmTO.getToolId(), pcmTO.getToolName(), pcmTO.getConnectorName()));
		}

		// Tool Category Id/Name
		if (!Objects.equals(connectorHub.getToolMasterEntity().getCategoryId(), pcmTO.getToolCategoryId()) ||
			!Objects.equals(connectorHub.getToolMasterEntity().getToolCategoryEntity().getCategoryName(), pcmTO.getToolCategory())) {
			throw new CustomException(HttpStatus.NOT_FOUND,
				"Provided Tool: %s doesn't belong to the Tool Category Id/Name (%s, %s)"
					.formatted(pcmTO.getToolName(), pcmTO.getToolCategoryId(), pcmTO.getToolCategory()));
		}

		return connectorHub;
	}

	public void checkIfTeamValid(String teamName) throws CustomException {

		CustomValidators.validateNull(teamName, "Team name");
		CustomValidators.minmaxLength(teamName, "Team name", 3, 64);
		CustomValidators.notStartWithUnderscore(teamName, "Team name");
		CustomValidators.notStartWithPeriod(teamName, "Team name");
		CustomValidators.notEndWithDoublePeriod(teamName, "Team name");
		CustomValidators.notContainCharacter(teamName, "Team name");
		CustomValidators.doubleSymbol(teamName, "Team name", ".");

	}

	public void checkIfAreaPathValid(List<String> areaPath) throws CustomException {
		if (ObjectUtils.isEmpty(areaPath)) return;

		for (int i = 0; i < areaPath.size(); i++) {
			String area_path = areaPath.get(i);

			CustomValidators.minmaxLength(area_path, "Area path", 3, 4000);

			String[] areaNodes = area_path.split("\\\\");
			if (areaNodes.length > 15) {
				throw new CustomException(HttpStatus.BAD_REQUEST,
					area_path + "Area path should not contain more than 14 backslash (\\) (hierarchy).");

			}
			for (String areaNode : areaNodes) {

				CustomValidators.AreaPathNotContainPeriod(areaNode, "Area node");
				CustomValidators.notContainCharAreaPath(areaNode, "Area node");

				if (areaNode.length() > 255) {
					throw new CustomException(HttpStatus.BAD_REQUEST,
						areaNode + " Area node should not contain more than 255 characters.");

				}

			}

		}

	}

	/**
	 * Verifies the Configuration details with individual Tool Microservices
	 *
	 * @return true
	 */
	@Override
	public boolean verifyConfigDetailsWithToolServices(@NonNull ProjectConnectorMappingTO pcmTO)
		throws CustomException {
		// TODO: Verify - update for remaining tool

		String url = switch (CommonConstants.Tool.resolve(pcmTO.getToolName())) {
			case ADO -> PathConstants.ADO_VERIFY_CONFIG_DETAILS;
			case JIRA -> PathConstants.JIRA_VERIFY_CONFIG_DETAILS;
			case RALLY -> PathConstants.RALLY_VERIFY_CONFIG_DETAILS;
			case GITLAB -> PathConstants.GITLAB_VERIFY_CONFIG_DETAILS;
			case AZURE_GIT -> PathConstants.AZUREGIT_VERIFY_CONFIG_DETAILS;
			case RTC -> PathConstants.RTC_VERIFY_CONFIG_DETAILS;
			default -> "";
		};

		if (!StringUtils.hasText(url))
			return true;

		ToolConfigDetailsTO toolConfigDetail = new ToolConfigDetailsTO();
		toolConfigDetail.setProjectConnectorId(pcmTO.getProjectConnectorId());
		toolConfigDetail.setToolName(pcmTO.getToolName());
		toolConfigDetail.setToolApiUrl(pcmTO.getToolApiUrl());
		toolConfigDetail.setToolProject(pcmTO.getTestPlanFolderName());
		toolConfigDetail.setRepository(pcmTO.getTestPlanFolderName());
		toolConfigDetail.setUserName(pcmTO.getUserName());
		toolConfigDetail.setAccessToken(pcmTO.getAccessToken());
		toolConfigDetail.setBranchName(pcmTO.getBranchName());
		toolConfigDetail.setTeams(pcmTO.getTeams());

		RestTemplateUtil.executeInternalPOST(url, new HttpEntity<>(toolConfigDetail),
			"Failed to verify Configuration Details",
			"[%s Service is Unavailable]".formatted(toolConfigDetail.getToolName())
				+ "::Please Contact Administrator",
			Boolean.class);
		return true;
	}

	/**
	 * Verifies the PCM Configuration Details
	 *
	 * <li>Validates the details with Custom Validations
	 * {@link #validatePCMConfigDetails}
	 * <li>Checks if the details are changed after Sync
	 * {@link #checkIfPCMDetailsChangedAfterSync}
	 * <li>Verifies the details with individual Tool Services
	 * {@link #verifyConfigDetailsWithToolServices}
	 *
	 * @return true
	 */
	@Override
	public ConnectorHubEntity validateAndVerifyPCMConfigDetails(@NonNull ProjectConnectorMappingTO pcmTO) throws CustomException {
		ConnectorHubEntity connectorHubEntity = validatePCMConfigDetails(pcmTO);
		checkIfPCMDetailsChangedAfterSync(pcmTO);
		verifyConfigDetailsWithToolServices(pcmTO);
		return connectorHubEntity;
	}

	void checkIfPCMDetailsChangedAfterSync(ProjectConnectorMappingTO pcmTO) throws CustomException {
		if (pcmTO.getProjectConnectorId() == 0)
			return;
		if (pcmTO.getToolName().equals(EXCEL.label))
			return;
		if (!StringUtils.hasText(pcmTO.getLastSyncDate()))
			return;

		ProjectConnectorMappingEntity oldPCMDetails = findByPcmId(pcmTO.getProjectConnectorId());

		if (oldPCMDetails.getProjectId() != pcmTO.getProjectId()) {
			throw new CustomException(HttpStatus.BAD_REQUEST,
				"Not allowed to change 'Project' after Connector has been synced");
		}
		if (!oldPCMDetails.getToolApiUrl().equals(pcmTO.getToolApiUrl())
			|| !oldPCMDetails.getTestPlanFolderName().equals(pcmTO.getTestPlanFolderName())) {
			throw new CustomException(HttpStatus.BAD_REQUEST,
				"Not allowed to change 'Tool API URL & Tool Project' after Connector has been synced");

		}

		// TODO: Verify - handle for remaining tools

		// RTC
		if (pcmTO.getToolName().equals(CommonConstants.Tool.RTC.label)) {
			int index = oldPCMDetails.getBranchName().indexOf("/");
			String oldComponent = oldPCMDetails.getBranchName().substring(index + 1);
			int newIndex = pcmTO.getBranchName().indexOf("/");
			if (newIndex == -1) {
				logger.error("Stream Name and Component Name should be separated by '/'");
				throw new CustomException(Exceptions.ERROR050);
			}
			String newComponent = pcmTO.getBranchName().substring(newIndex + 1);
			if (!oldComponent.equals(newComponent)) {
				throw new CustomException(HttpStatus.BAD_REQUEST,
					"Not allowed to change 'Component' after Connector has been synced.");
			}
		}

		// ADO
		if (pcmTO.getToolName().equals(CommonConstants.Tool.ADO.label)) {
			if ((!ObjectUtils.isEmpty(pcmTO.getTeams()) || !ObjectUtils.isEmpty(oldPCMDetails.getTeams()))
				&& !Objects.equals(oldPCMDetails.getTeams(), pcmTO.getTeams())) {
				throw new CustomException(HttpStatus.BAD_REQUEST,
					"Not allowed to change 'Teams' after Connector has been synced");
			}
		}
	}



	@Override
	@Transactional(rollbackFor = com.lti.common.exception.CustomException.class)
	public ProjectConnectorMappingTO saveProjectConnector(ProjectConnectorMappingTO pcmTo) throws CustomException {

		ConnectorHubEntity connectorHubEntity = validateAndVerifyPCMConfigDetails(pcmTo);
		ProjectConnectorMappingEntity pcmEntityToSave = new ProjectConnectorMappingEntity();

		if (pcmTo.getToolName().equals(CommonConstants.Tool.EXCEL.label)) {
			excelFileUploadService.verifyExcelFileDetailsForSave(pcmTo, pcmEntityToSave);
		}

		int nextPcmId = fetchNextPcmId();
		try {
			logger.info("saving or updating PCM's Data");

			pcmTo.setProjectConnectorId(nextPcmId);
			pcmEntityToSave.setProjectConnectorId(nextPcmId);
			pcmEntityToSave.setProjectId(pcmTo.getProjectId());
			pcmEntityToSave.setConnectorId(pcmTo.getConnectorId());
			pcmEntityToSave.setConnectorHubEntity(connectorHubEntity);

			if (!pcmTo.getToolName().equals(CommonConstants.Tool.EXCEL.label)) {
				pcmEntityToSave.setToolApiUrl(pcmTo.getToolApiUrl());
				pcmEntityToSave.setTestPlanFolderName(pcmTo.getTestPlanFolderName());
				pcmEntityToSave.setUserName(pcmTo.getUserName());
				pcmEntityToSave.setAccessToken(pcmTo.getAccessToken());
				if (!secretFlag.equals(CommonConstants.POSTGRES)) {
					pcmEntityToSave.setAccessToken(null);
				}
				pcmEntityToSave.setBranchName(pcmTo.getBranchName());

				if (pcmTo.getToolCategory().equals(VCS.label)) {
					pcmEntityToSave.setBranchName(pcmTo.getBranchName());
				}
				if (pcmTo.getToolName().equals(CommonConstants.Tool.ADO.label)) {
					pcmEntityToSave.setTeams(pcmTo.getTeams());
				}
			} else {
				pcmEntityToSave.getExcelFileDetailEntityList().forEach(excelFileDetailEntity ->
					excelFileDetailEntity.setPcmId(pcmTo.getProjectConnectorId())
				);
			}

			pcmEntityToSave = pcmDao.save(pcmEntityToSave);
			// set the secret values for aws/vault
			SecretsTO secretsInput = new SecretsTO(pcmTo.getClientId(),pcmTo.getAccessToken(),pcmTo.getClientSecret(),pcmTo.getProjectConnectorId());
			// wil call aws/vault saveSecret method, if secret.store.flag=aws/vault
			secretService.saveSecret(secretsInput, secretId);

			ProjectConnectorMappingTO wso2Input = new ProjectConnectorMappingTO();
			wso2Input.setProjectConnectorId(nextPcmId);
			wso2Input.setConnectorId(pcmEntityToSave.getConnectorId());
			wso2Input.setProjectId(pcmEntityToSave.getProjectId());

			String authUrl = wso2Config.url() + PathConstants.AUTHENTICATION;
			String token = StoryUtils.generateToken(authUrl, wso2RestTemplate, wso2Config.basicAuth(),
				wso2Config.apiKey());
			String uri = wso2Config.url() + PathConstants.SAVE_PROJECT_CONNECTOR;
			HttpHeaders headers = new HttpHeaders();
			headers.add(CommonConstants.WSO2_ACCESS_TOKEN, token);
			headers.add(CommonConstants.API_KEY, wso2Config.apiKey());
			headers.setContentType(MediaType.APPLICATION_JSON);
			HttpEntity<Object> entity = new HttpEntity<>(objectMapper.writeValueAsString(wso2Input), headers);
			StoryUtils.executePostRequest(wso2RestTemplate, uri, entity);

			if (pcmTo.getToolName().equals(CommonConstants.Tool.EXCEL.label)) {
				// move file from temp to final storage
				// less likely to get fail ??
				excelFileUploadService.commitUploadedExcelFileToFinalStorage(pcmTo.getExcelFiles());
			}

			pcmTo.setToolIcon(connectorHubEntity.getToolMasterEntity().getToolIcon());
			pcmTo.setLastSyncDate(pcmEntityToSave.getLastSyncDate());
			pcmTo.setSyncStatus(pcmEntityToSave.getSyncStatus());
			pcmTo.setSyncMsg(pcmEntityToSave.getSyncMsg());
			pcmTo.setLastHistorySyncDate(pcmEntityToSave.getLastHistorySyncDate());
			pcmTo.setHistorySyncStatus(pcmEntityToSave.getHistorySyncStatus());
			pcmTo.setHistorySyncMsg(pcmEntityToSave.getHistorySyncMsg());

			return pcmTo;
		} catch (CustomException e) {
			throw e;
		} catch (Exception e) {
			throw new CustomException(Exceptions.ERROR050, e);
		}
	}

	public void setAccessToken(ProjectConnectorMappingTO pcmTo,String toolName) throws CustomException {

		// if accessToken is not stored in postgres, set the accesstoken 
		if (!secretFlag.equalsIgnoreCase(CommonConstants.POSTGRES) && ObjectUtils.isEmpty(pcmTo.getAccessToken()) 
				&& (!toolName.equals(EXCEL.label))) {
			// fetch the access token from aws/vault
			String accessToken = secretService.fetchAccessToken(pcmTo.getProjectConnectorId(), secretId);
			pcmTo.setAccessToken(accessToken);
		}
	}
	@Override
	public ProjectConnectorMappingTO getProjectConnector(int projectConnectorId) throws CustomException {

		ProjectConnectorMappingEntity pcmEntity = findByPCMIdWithAssociations(projectConnectorId);

		ProjectConnectorMappingTO pcmTo = new ProjectConnectorMappingTO();
		BeanUtils.copyProperties(pcmEntity, pcmTo);

		// if accessToken is not stored in postgres, set the accesstoken 
		setAccessToken(pcmTo,pcmEntity.getConnectorHubEntity().getToolMasterEntity().getToolName());

		if (pcmEntity.getConnectorHubEntity().getToolMasterEntity().getToolName().equals(EXCEL.label)) {
			List<ExcelFileDetailTO> excelFiles = new ArrayList<>(pcmEntity.getExcelFileDetailEntityList().size());
			pcmEntity.getExcelFileDetailEntityList().forEach(excelFileDetailEntity -> {
				ExcelFileDetailTO excelFileDetailTO = new ExcelFileDetailTO();
				BeanUtils.copyProperties(excelFileDetailEntity, excelFileDetailTO);
				excelFiles.add(excelFileDetailTO);
			});
			pcmTo.setExcelFiles(excelFiles);

			// FieldMappingDetails is only required for Excel currently
			pcmTo.setFieldDetailsList(fieldMappingService.getFieldMappingDetails(pcmEntity, true));
		}

		return pcmTo;
	}

	@Override
	public List<ProjectConnectorMappingTO> getProjectConnectors(Boolean returnSensitive) throws CustomException {
		List<ProjectConnectorMappingTO> pcmTOList = new ArrayList<>();

		List<ProjectConnectorMappingEntity> pcmEntityIterable = pcmDao.findAllPCMWithAssociations();

		for (ProjectConnectorMappingEntity pcmEntity : pcmEntityIterable) {
			ProjectConnectorMappingTO pcmTO = new ProjectConnectorMappingTO();
			BeanUtils.copyProperties(pcmEntity, pcmTO);

			// if accessToken is not stored in postgres, set the accessToken 
			setAccessToken(pcmTO,pcmEntity.getConnectorHubEntity().getToolMasterEntity().getToolName());

			ConnectorHubEntity connectorHubentity = pcmEntity.getConnectorHubEntity();
			pcmTO.setConnectorName(connectorHubentity.getConnectorName());
			pcmTO.setConnectorId(connectorHubentity.getConnectorId());
			pcmTO.setToolId(connectorHubentity.getToolId());
			pcmTO.setToolCategoryId(connectorHubentity.getToolMasterEntity().getToolCategoryEntity().getCategoryId());
			pcmTO.setToolCategory(connectorHubentity.getToolMasterEntity().getToolCategoryEntity().getCategoryName());
			pcmTO.setToolIcon(connectorHubentity.getToolMasterEntity().getToolIcon());
			pcmTO.setToolName(connectorHubentity.getToolMasterEntity().getToolName());

			if (pcmTO.getToolName().equals(EXCEL.label)) {
				List<ExcelFileDetailEntity> excelFileDetailEntityList = pcmEntity.getExcelFileDetailEntityList();
				List<ExcelFileDetailTO> excelFiles = new ArrayList<>(excelFileDetailEntityList.size());

				excelFileDetailEntityList.forEach(excelFileDetailEntity -> {
					ExcelFileDetailTO excelFileDetailTO = new ExcelFileDetailTO();
					BeanUtils.copyProperties(excelFileDetailEntity, excelFileDetailTO);
					excelFiles.add(excelFileDetailTO);
				});
				pcmTO.setExcelFiles(excelFiles);
			}

			pcmTOList.add(pcmTO);
		}

		return pcmTOList;

	}

	@Override
	@Transactional(rollbackFor = com.lti.common.exception.CustomException.class)
	public ProjectConnectorMappingTO updateProjectConnector(ProjectConnectorMappingTO pcmTo, int projectConnectorId)
		throws CustomException, JsonProcessingException {

		// if accessToken is empty and the flag is not set as postgres
		setAccessToken(pcmTo,pcmTo.getToolName());

		ConnectorHubEntity connectorHubEntity = validateAndVerifyPCMConfigDetails(pcmTo);
		ProjectConnectorMappingEntity pcmEntity = findByPcmId(projectConnectorId);

		Pair<Set<String>, Set<String>> filesToSaveAndRemoveTuple =
			excelFileUploadService.verifyExcelFileDetailsForUpdate(pcmTo, pcmEntity);

		List<ProjectConnectorMappingEntity> pcmValidationList = null;
		try {
			if (pcmTo.getToolCategory().equals(VCS.label)) {

				pcmValidationList = pcmDao.fetchPCMUpdateGitLabData(pcmTo.getToolApiUrl(),
					pcmTo.getTestPlanFolderName(), pcmTo.getProjectId(), pcmTo.getBranchName(),
					pcmTo.getConnectorId());

				if (pcmValidationList != null && pcmValidationList.size() == 1
					&& pcmValidationList.get(0).getProjectConnectorId() != pcmTo.getProjectConnectorId()) {
					throw new CustomException(HttpStatus.BAD_REQUEST,
						"Connector for toolApiUrl, testPlanFolderName, projectId, branchName, connectorId combination "
							+ "already exists");
				}

				pcmEntity.setBranchName(pcmTo.getBranchName());
			} else if (!pcmTo.getToolName().equals(EXCEL.label)) {

				String teamString = ObjectUtils.isEmpty(pcmTo.getTeams()) ? null : objectMapper.writeValueAsString(pcmTo.getTeams());

				pcmValidationList = pcmDao.fetchPCMDataValidate(pcmTo.getToolApiUrl(), pcmTo.getTestPlanFolderName(),
					pcmTo.getProjectId(), pcmTo.getConnectorId(), teamString);

				if (pcmValidationList != null && pcmValidationList.size() == 1
					&& pcmValidationList.get(0).getProjectConnectorId() != pcmTo.getProjectConnectorId()) {
					throw new CustomException(HttpStatus.BAD_REQUEST,
						"Connector for toolApiUrl, testPlanFolderName, projectId, connectorId " + (pcmTo.getToolName().equals(ADO.label) ? ", teams" : "") + " combination already "
							+ "exists");
				}
			}

			pcmEntity.setProjectId(pcmTo.getProjectId());

			if (!pcmTo.getToolName().equals(CommonConstants.Tool.EXCEL.label)) {
				pcmEntity.setToolApiUrl(pcmTo.getToolApiUrl());
				pcmEntity.setTestPlanFolderName(pcmTo.getTestPlanFolderName());
				pcmEntity.setUserName(pcmTo.getUserName());
				pcmEntity.setAccessToken(pcmTo.getAccessToken());

				if (!secretFlag.equals(CommonConstants.POSTGRES)) {
					pcmEntity.setAccessToken(null);
				}
				if (pcmTo.getToolName().equals(ADO.label)) {
					pcmEntity.setTeams(pcmTo.getTeams());
				}
			}

			pcmEntity = pcmDao.save(pcmEntity);

			// set values for aws/vault
			SecretsTO secretsInput = new SecretsTO(pcmTo.getClientId(),pcmTo.getAccessToken(),pcmTo.getClientSecret(),pcmTo.getProjectConnectorId());
			// wil call aws/vault saveSecret method, if secret.store.flag=aws/vault
			secretService.updateSecret(secretsInput, secretId);

			updateProjectConnectorInCK(pcmTo);

			if (pcmTo.getToolName().equals(CommonConstants.Tool.EXCEL.label)) {
				// move file from temp to final storage
				// less likely to get fail ??
				if (!filesToSaveAndRemoveTuple.getFirst().isEmpty())
					excelFileUploadService.commitUploadedExcelFileToFinalStorage(filesToSaveAndRemoveTuple.getFirst());

				if (!filesToSaveAndRemoveTuple.getSecond().isEmpty())
					excelFileUploadService.deleteExcelFileFromFinalStorage(filesToSaveAndRemoveTuple.getSecond());
			}

			pcmTo.setConnectorName(connectorHubEntity.getConnectorName());
			pcmTo.setToolId(connectorHubEntity.getToolId());
			pcmTo.setToolName(connectorHubEntity.getToolMasterEntity().getToolName());
			pcmTo.setToolIcon(connectorHubEntity.getToolMasterEntity().getToolIcon());

			pcmTo.setLastSyncDate(pcmEntity.getLastSyncDate());
			pcmTo.setSyncStatus(pcmEntity.getSyncStatus());
			pcmTo.setSyncMsg(pcmEntity.getSyncMsg());
			pcmTo.setLastHistorySyncDate(pcmEntity.getLastHistorySyncDate());
			pcmTo.setHistorySyncStatus(pcmEntity.getHistorySyncStatus());
			pcmTo.setHistorySyncMsg(pcmEntity.getHistorySyncMsg());

		} catch (CustomException e) {
			throw e;
		}
		return pcmTo;

	}

	void updateProjectConnectorInCK(ProjectConnectorMappingTO pcmTo) throws CustomException {
		logger.info("WSO2 - Updating ProjectConnectorMapping:{} In CK", pcmTo.getProjectConnectorId());

		ProjectConnectorMappingTO wso2Input = new ProjectConnectorMappingTO();
		wso2Input.setProjectConnectorId(pcmTo.getProjectConnectorId());
		wso2Input.setConnectorId(pcmTo.getConnectorId());
		wso2Input.setProjectId(pcmTo.getProjectId());
		wso2Input.setLastSyncDate(pcmTo.getLastSyncDate());
		wso2Input.setLastHistorySyncDate(pcmTo.getLastHistorySyncDate());
		wso2Input.setResyncField(pcmTo.getResyncField());
		wso2Input.setResyncHistoryField(pcmTo.getResyncHistoryField());
		wso2Input.setSyncMsg(pcmTo.getSyncMsg());
		wso2Input.setSyncStatus(pcmTo.getSyncStatus());
		wso2Input.setHistorySyncMsg(pcmTo.getHistorySyncMsg());
		wso2Input.setHistorySyncStatus(pcmTo.getHistorySyncStatus());

		// NOTE: Only for Excel, FieldMappingDetails needs to be synced with the CK
		// temporarily it is sent with the RUNNING sync status update
		if (pcmTo.getToolName().equals(EXCEL.label)
			&& StringUtils.hasText(pcmTo.getSyncStatus())
			&& pcmTo.getSyncStatus().equals(CommonConstants.SYNC_RUNNING)) {
			wso2Input.setFieldDetailsList(pcmTo.getFieldDetailsList());
		}

		String authUrl = wso2Config.url() + PathConstants.AUTHENTICATION;
		String token = StoryUtils.generateToken(authUrl, wso2RestTemplate, wso2Config.basicAuth(), wso2Config.apiKey());

		HttpHeaders headers = new HttpHeaders();
		headers.add("AccessToken", token);
		headers.add("apikey", wso2Config.apiKey());
		headers.setContentType(MediaType.APPLICATION_JSON);
		HttpEntity<Object> entity = new HttpEntity<>(wso2Input, headers);

		String uri = wso2Config.url() + PathConstants.UPDATE_PROJECT_CONNECTOR.replace("{projectConnectorId}",
				String.valueOf(wso2Input.getProjectConnectorId()));
		StoryUtils.executePutMethod(wso2RestTemplate, uri, entity);
	}

	@Override
	@Transactional(rollbackFor = com.lti.common.exception.CustomException.class)
	public boolean deleteProjectConnector(Integer projectConnectorId) throws CustomException {
		try {
			ProjectConnectorMappingEntity pcmEntity = findByPCMIdWithAssociations(projectConnectorId);
			pcmDao.deleteById(projectConnectorId);

			String authUrl = wso2Config.url() + PathConstants.AUTHENTICATION;
			String token = StoryUtils.generateToken(authUrl, wso2RestTemplate, wso2Config.basicAuth(),
				wso2Config.apiKey());
			HttpHeaders headers = new HttpHeaders();
			headers.add("AccessToken", token);
			headers.add("apikey", wso2Config.apiKey());
			headers.setContentType(MediaType.APPLICATION_JSON);
			HttpEntity<Object> entity = new HttpEntity<>(headers);
			String uri = wso2Config.url() + PathConstants.DELETE_PROJECT_CONNECTOR.replace("{projectConnectorId}",
				String.valueOf(projectConnectorId));
			StoryUtils.executeDeleteMethod(wso2RestTemplate, uri, entity);

			// will delete the secret present in aws/vault
			secretService.deleteSecret(projectConnectorId, secretId);

			if (CommonConstants.Tool.EXCEL.label.equals(pcmEntity.getConnectorHubEntity().getToolMasterEntity().getToolName())) {
				excelFileUploadService.deleteExcelFileFromFinalStorage(
					pcmEntity.getExcelFileDetailEntityList().stream().map(ExcelFileDetailEntity::getFileId).collect(Collectors.toSet())
				);
			}

			return true;
		} catch (Exception e) {
			logger.error("Failed to Delete ConnectorDetail: {}", projectConnectorId);
			throw new CustomException(Exceptions.ERROR073, e);
		}
	}

	@Override
	public List<ProjectConnectorMappingEntity> getPCMDetailsByConnectorId(int connectorId) throws CustomException {
		List<ProjectConnectorMappingEntity> pcmEntityList = new ArrayList<ProjectConnectorMappingEntity>();
		pcmEntityList = pcmDao.getPCMDetailsByConnectorId(connectorId);
		return pcmEntityList;
	}

	@Override
	public ProjectConnectorMappingEntity findByPcmId(int projectConnectorId) throws CustomException {
		CustomValidators.validateNull(projectConnectorId, "ProjectConnectorMapping ID");

		// Check if PCM Exists
		Optional<ProjectConnectorMappingEntity> pcmOptional = pcmDao.findById(projectConnectorId);
		if (pcmOptional.isEmpty()) {
			throw new CustomException(HttpStatus.BAD_REQUEST,
				"ProjectConnectorMapping doesn't exist with ID: " + projectConnectorId);
		}
		return pcmOptional.get();
	}

	@Override
	public ProjectConnectorMappingEntity findByPCMIdWithAssociations(int projectConnectorId) throws CustomException {
		CustomValidators.validateNull(projectConnectorId, "ProjectConnectorMapping ID");

		// Check if PCM Exists
		Optional<ProjectConnectorMappingEntity> pcmOptional = pcmDao.findByPCMIdWithAssociations(projectConnectorId);
		if (pcmOptional.isEmpty()) {
			throw new CustomException(HttpStatus.BAD_REQUEST,
				"ProjectConnectorMapping doesn't exist with ID: " + projectConnectorId);
		}
		return pcmOptional.get();
	}

	@Override
	@NonNull
	public List<ProjectConnectorMappingEntity> getPCMListToSync(int projectId) throws CustomException {
		CustomValidators.validateNull(projectId, "Project ID");

		List<ProjectConnectorMappingEntity> pcmList = pcmDao.findByProjectIdWithAssociations(projectId);
		if (pcmList.isEmpty()) {
			throw new CustomException(HttpStatus.BAD_REQUEST,
				"Project %d doesn't have any Connectors mapped to it".formatted(projectId));
		}

		List<ProjectConnectorMappingEntity> pcmListToSync = pcmList.stream()
			.filter(pcm -> !CommonConstants.SYNC_RUNNING.equals(pcm.getSyncStatus())).toList();
		if (pcmListToSync.isEmpty()) {
			throw new CustomException(HttpStatus.BAD_REQUEST,
				"All Connectors' sync under this Project is already in RUNNING state");
		}

		return pcmListToSync;
	}

	public List<ProjectConnectorMappingEntity> setSyncStatusAsRunning(List<ProjectConnectorMappingEntity> pcmListToSync,int projectId) throws CustomException{

        for(int i=0;i<pcmListToSync.size();i++) {

			ProjectConnectorMappingEntity pcmEntity = pcmListToSync.get(i);
			//setting Sync Status as RUNNING
			pcmEntity.setSyncStatus(CommonConstants.SYNC_RUNNING);

			//setting History Sync Status as RUNNING
//			String toolName = pcmEntity.getConnectorHubEntity().getToolMasterEntity().getToolName();
//			String toolCategoryName = pcmEntity.getConnectorHubEntity().getToolMasterEntity().getToolCategoryEntity().getCategoryName();
//			if (!VCS.label.equals(toolCategoryName) && !EXCEL.label.equals(toolName)) {
//				pcmEntity.setHistorySyncStatus(CommonConstants.SYNC_RUNNING);
//			}

        }
        pcmListToSync = pcmDao.saveAll(pcmListToSync);
        logger.info("Sync status set as RUNNING for all Connectors under ProjectId: {}",projectId);

        return pcmListToSync;
	}

	@Override
	@NonNull
	public List<ProjectConnectorMappingEntity> getPCMListWithRunningStatus() {
		return pcmDao.findAllPCMBySyncStatusWithAssociations(CommonConstants.SYNC_RUNNING);
	}

	@Override
	@Async("connectorSyncTaskExecutor")
	public CompletableFuture<ResponseTO<SyncDetailsTO>> syncSdlcData(ProjectConnectorMappingEntity pcmEntity,
		boolean resumeInterruptedSync) {

		if (!resumeInterruptedSync && CommonConstants.SYNC_RUNNING.equals(pcmEntity.getSyncStatus())) {
			return CompletableFuture.failedFuture(new CustomException(Exceptions.SYNC_ALREADY_RUNNING));
		}

		int projectConnectorId = pcmEntity.getProjectConnectorId();
		String toolName = pcmEntity.getConnectorHubEntity().getToolMasterEntity().getToolName();

		ProjectConnectorMappingTO pcmTO = new ProjectConnectorMappingTO();
		BeanUtils.copyProperties(pcmEntity, pcmTO);
		pcmTO.setToolName(toolName);

		// will fetch the accessToken present in aws/vault
		try {
			setAccessToken(pcmTO,pcmTO.getToolName());
		} catch (CustomException customException) {
			return CompletableFuture.failedFuture(customException);
		}
		
		try {
			verifyConfigDetailsWithToolServices(pcmTO);
		} catch (CustomException customException) {
			try {
				updateLastSyncDateById(projectConnectorId, CommonConstants.SYNC_FAILED, customException.getMessage());
			} catch (CustomException ce) {
				// do nothing, return the original exception
				logger.error("Failed to Update Last Sync Date : ", ce.getMessage());

			}
			return CompletableFuture.failedFuture(customException);
		}

		try {
			logger.info("Connector {} Latest Sync Started", projectConnectorId);

			ResponseEntity<ResponseTO<SyncDetailsTO>> responseEntity = null;

			switch (CommonConstants.Tool.resolve(toolName)) {
				case ADO -> {
					logger.info("Performing Connector Latest Sync REST call - ADO");

					responseEntity = WebClientUtil.executeInternalPostMethod(
						internalWebClient,
						PathConstants.ADO_DUMP.formatted(projectConnectorId),
						new ParameterizedTypeReference<SyncDetailsTO>() {},
						Exceptions.ERROR507,
						"ADO Sync Failed",
						ExceptionMessages.ERROR_MSG003,
						ExceptionMessages.ERROR_MSG004);
				}
				case JIRA -> {
					logger.info("Performing Connector Latest Sync REST call - Jira");

					responseEntity = WebClientUtil.executeInternalPostMethod(
						internalWebClient,
						PathConstants.JIRA_DUMP.formatted(projectConnectorId),
						new ParameterizedTypeReference<SyncDetailsTO>() {},
						Exceptions.ERROR508,
						"Jira Sync Failed",
						ExceptionMessages.ERROR_MSG005,
						ExceptionMessages.ERROR_MSG006);
				}
				case GITLAB -> {
					logger.info("Performing Connector Latest Sync REST call - Gitlab");

					responseEntity = WebClientUtil.executeInternalPostMethod(
						internalWebClient,
						PathConstants.GITLAB_DUMP.formatted(projectConnectorId),
						new ParameterizedTypeReference<SyncDetailsTO>() {},
						Exceptions.ERROR509,
						"Gitlab Sync Failed",
						ExceptionMessages.ERROR_MSG007,
						ExceptionMessages.ERROR_MSG008);
				}
				case EXCEL -> {
					logger.info("Performing Connector Latest Sync REST call - Excel");

					responseEntity = WebClientUtil.executeInternalPostMethod(
						internalWebClient,
						PathConstants.EXCEL_DUMP.formatted(projectConnectorId),
						new ParameterizedTypeReference<SyncDetailsTO>() {},
						Exceptions.ERROR523,
						"Excel Sync Failed",
						ExceptionMessages.ERROR_MSG018,
						ExceptionMessages.ERROR_MSG019);
				}
				case AZURE_GIT -> {
					logger.info("Performing Connector Latest Sync REST call - AzureGit");

					responseEntity = WebClientUtil.executeInternalPostMethod(
						internalWebClient,
						PathConstants.AZUREGIT_DUMP.formatted(projectConnectorId),
						new ParameterizedTypeReference<SyncDetailsTO>() {},
						Exceptions.ERROR524,
						"AzureGit Sync Failed",
						ExceptionMessages.ERROR_MSG020,
						ExceptionMessages.ERROR_MSG021);
				}
				case RALLY -> {
					logger.info("Performing Connector Latest Sync REST call - Rally");

					responseEntity = WebClientUtil.executeInternalPostMethod(
						internalWebClient,
						PathConstants.RALLY_DUMP.formatted(projectConnectorId),
						new ParameterizedTypeReference<SyncDetailsTO>(){},
						Exceptions.ERROR528,
						"Rally Sync Failed",
						ExceptionMessages.ERROR_MSG022,
						ExceptionMessages.ERROR_MSG023
					);
				}
				case RTC -> {
					logger.info("Performing Connector Latest Sync REST call - RTC");

					responseEntity = WebClientUtil.executeInternalPostMethod(
						internalWebClient,
						PathConstants.RTC_DUMP.formatted(projectConnectorId),
						new ParameterizedTypeReference<SyncDetailsTO>(){},
						Exceptions.ERROR530,
						"RTC Sync Failed",
						ExceptionMessages.ERROR_MSG024,
						ExceptionMessages.ERROR_MSG025
					);
				}
				default -> {
					return CompletableFuture.failedFuture(
						new CustomException(HttpStatus.BAD_REQUEST, "Provided Tool is not supported for Sync"));
				}
			}
			logger.info("Connector {} Latest Sync Completed", projectConnectorId);

			ResponseTO<SyncDetailsTO> response = responseEntity.getBody();
			if (response == null || response.getData() == null) {
				return CompletableFuture.failedFuture(new CustomException(Exceptions.ERROR122));
			}
			response.setData(objectMapper.convertValue(response.getData(), SyncDetailsTO.class));
			return CompletableFuture.completedFuture(response);
		} catch (Exception e) {
			// Handles following scenarios
			// 1. Exceptions uncaught by CustomException
			// 2. Exceptions caught by CustomException of status SERVICE_UNAVAILABLE
			// In above cases it didn't connect to the tool service (down/unavailable)
			// and the sync details needs to be updated manually from here
			if (!(e instanceof CustomException customException)
				|| (HttpStatus.SERVICE_UNAVAILABLE.equals(customException.getHttpStatus())
				&& !customException.getMessage().toLowerCase().contains("wso"))) {
				try {
					updateLastSyncDateById(projectConnectorId, CommonConstants.SYNC_FAILED,
						e instanceof CustomException customException
							? customException.getMessage()
							: Exceptions.ERROR050.getMessage()
					);
				} catch (CustomException ce) {
					// do nothing, return the original exception
					logger.error("Failed to update Latest sync details", ce);
				}
			}

			if (e instanceof CustomException customException) {
				return CompletableFuture.failedFuture(customException);
			} else {
				return CompletableFuture.failedFuture(new CustomException(Exceptions.ERROR050, e));
			}
		}
	}

	@Override
	@Async("connectorSyncTaskExecutor")
	public CompletableFuture<SyncDetailsTO> syncSdlcHistoryData(ProjectConnectorMappingEntity pcmEntity,
		boolean resumeInterruptedSync) {

		if (!resumeInterruptedSync && CommonConstants.SYNC_RUNNING.equals(pcmEntity.getHistorySyncStatus())) {
			return CompletableFuture.failedFuture(new CustomException(Exceptions.SYNC_ALREADY_RUNNING));
		}

		int projectConnectorId = pcmEntity.getProjectConnectorId();
		String toolName = pcmEntity.getConnectorHubEntity().getToolMasterEntity().getToolName();

		ProjectConnectorMappingTO pcmTO = new ProjectConnectorMappingTO();
		BeanUtils.copyProperties(pcmEntity, pcmTO);
		pcmTO.setToolName(toolName);

		try {
			// will fetch the accessToken present in aws/vault
			setAccessToken(pcmTO,pcmTO.getToolName());
		} catch (CustomException customException) {
			// do nothing, return the original exception
			return CompletableFuture.failedFuture(customException);
		}
		
		try {
			verifyConfigDetailsWithToolServices(pcmTO);
		} catch (CustomException customException) {
			try {
				updateHistorySyncDetails(pcmEntity, SyncStatus.FAILED, customException.getMessage());
			} catch (CustomException ce) {
				// do nothing, return the original exception
				logger.error("Failed to Update History Sync details : ",ce.getMessage());

			}
			return CompletableFuture.failedFuture(customException);
		}

		try {
			logger.info("Connector {} History Sync Started", projectConnectorId);
			updateHistorySyncDetails(pcmEntity, RUNNING, null);

			ResponseEntity<ResponseTO<Void>> responseEntity = null;

			switch (CommonConstants.Tool.resolve(toolName)) {
				case ADO -> {
					logger.info("Performing Connector History Sync REST call - ADO");

					responseEntity = WebClientUtil.executeInternalPostMethod(
						internalWebClient,
						PathConstants.ADO_HISTORY_DUMP.formatted(projectConnectorId),
						new ParameterizedTypeReference<Void>() {},
						Exceptions.ERROR507,
						"ADO History Sync Failed",
						ExceptionMessages.ERROR_MSG003,
						ExceptionMessages.ERROR_MSG004
					);
				}
				case JIRA -> {
					logger.info("Performing Connector History Sync REST call - Jira");

					responseEntity = WebClientUtil.executeInternalPostMethod(
						internalWebClient,
						PathConstants.JIRA_HISTORY_DUMP.formatted(projectConnectorId),
						new ParameterizedTypeReference<Void>() {},
						Exceptions.ERROR508,
						"Jira History Sync Failed",
						ExceptionMessages.ERROR_MSG005,
						ExceptionMessages.ERROR_MSG006
					);
				}
				case RALLY -> {
					logger.info("Performing Connector History Sync REST call - Rally");

					responseEntity = WebClientUtil.executeInternalPostMethod(
						internalWebClient,
						PathConstants.RALLY_HISTORY_DUMP.formatted(projectConnectorId),
						new ParameterizedTypeReference<Void>() {},
						Exceptions.ERROR528,
						"Rally History Sync Failed",
						ExceptionMessages.ERROR_MSG022,
						ExceptionMessages.ERROR_MSG023
					);
				}
				default -> {
					return CompletableFuture.failedFuture(
						new CustomException(HttpStatus.BAD_REQUEST, "Provided Tool is not supported for History Sync")
					);
				}
			}
			logger.info("Connector {} History Sync Completed", projectConnectorId);

			ResponseTO<Void> response = responseEntity.getBody();
			if (response == null) {
				return CompletableFuture.failedFuture(new CustomException(Exceptions.ERROR122));
			}

			String lastHistorySyncDate = updateHistorySyncDetails(pcmEntity, SyncStatus.COMPLETED, null);

			SyncDetailsTO result = new SyncDetailsTO();
			result.setProjectConnectorId(projectConnectorId);
			result.setLastHistorySyncDate(lastHistorySyncDate);
			result.setHistorySyncStatus(SyncStatus.COMPLETED.name());

			return CompletableFuture.completedFuture(result);

		} catch (Exception e) {
			try {
				updateHistorySyncDetails(pcmEntity, SyncStatus.FAILED,
					e instanceof CustomException ce
						? ce.getMessage()
						: Exceptions.ERROR050.getMessage()
				);
			} catch (CustomException ce) {
				// do nothing, return the original exception
				logger.error("Failed to update History sync details", ce);
			}

			if (e instanceof CustomException customException) {
				return CompletableFuture.failedFuture(customException);
			} else {
				return CompletableFuture.failedFuture(
					new CustomException(Exceptions.ERROR050, e)
				);
			}
		}
	}

	@Override
	@NonNull
	public List<SyncDetailsTO> checkPCMSyncStatus(List<SyncDetailsTO> pcmSyncRunningList) {
		List<SyncDetailsTO> result = new ArrayList<>();

		if (CollectionUtils.isEmpty(pcmSyncRunningList)) {
			return result;
		}

		Map<Integer, SyncDetailsTO> inputSyncDetailsMapByPCMId = pcmSyncRunningList.stream()
			.collect(Collectors.toMap(SyncDetailsTO::getProjectConnectorId, Function.identity()));

		// Received pcmIds will be in RUNNING Sync status
		// Check if the Sync status is not RUNNING
		// And return the lastSyncDate
		List<SyncDetailsTO> syncDetailsFromDB = pcmDao.fetchSyncDetailsForRunningPCMList(
			inputSyncDetailsMapByPCMId.keySet()
		);

		for (SyncDetailsTO pcmSyncDetails : syncDetailsFromDB) {
			SyncDetailsTO inputSyncDetails = inputSyncDetailsMapByPCMId.get(pcmSyncDetails.getProjectConnectorId());
			SyncDetailsTO outputSyncDetails = new SyncDetailsTO();

			// Sync status is not RUNNING -> Sync COMPLETED / FAILED
			if (RUNNING.name().equals(inputSyncDetails.getSyncStatus())
				&& !RUNNING.name().equals(pcmSyncDetails.getSyncStatus())) {
				outputSyncDetails.setLastSyncDate(pcmSyncDetails.getLastSyncDate());
				outputSyncDetails.setSyncStatus(pcmSyncDetails.getSyncStatus());
				outputSyncDetails.setSyncMsg(pcmSyncDetails.getSyncMsg());
			}
			if (RUNNING.name().equals(inputSyncDetails.getHistorySyncStatus())
				&& !RUNNING.name().equals(pcmSyncDetails.getHistorySyncStatus())) {
				outputSyncDetails.setLastHistorySyncDate(pcmSyncDetails.getLastHistorySyncDate());
				outputSyncDetails.setHistorySyncStatus(pcmSyncDetails.getHistorySyncStatus());
				outputSyncDetails.setHistorySyncMsg(pcmSyncDetails.getHistorySyncMsg());
			}

			if (StringUtils.hasText(outputSyncDetails.getSyncStatus())
				|| StringUtils.hasText(outputSyncDetails.getHistorySyncStatus())) {

				outputSyncDetails.setProjectConnectorId(pcmSyncDetails.getProjectConnectorId());
				result.add(outputSyncDetails);
			}
		}
		return result;
	}

	@Override
	public String updateLastSyncDateById(Integer projectConnectorId, String syncStatus, String syncMsg)
		throws CustomException {
		String returnedData = null;

		ProjectConnectorMappingEntity pcmEntity = findByPCMIdWithAssociations(projectConnectorId);
		ProjectConnectorMappingTO pcmTO = new ProjectConnectorMappingTO();

		String toolName = pcmEntity.getConnectorHubEntity().getToolMasterEntity().getToolName();
		pcmTO.setToolName(toolName);

		if (syncStatus.equalsIgnoreCase(CommonConstants.SYNC_COMPLETED)) {
			pcmEntity.setLastSyncDate(OffsetDateTime.now(ZoneOffset.UTC).format(dateTimeStandardFormat) + "Z");
			pcmEntity.setSyncStatus(syncStatus);
			pcmEntity.setSyncMsg(syncMsg);

			pcmEntity = pcmDao.save(pcmEntity);
			returnedData = pcmEntity.getLastSyncDate();

			// PUSH updated lastSyncDate to CK
			logger.info("WSO2 - Updating ProjectConnectorMapping:{} lastSyncDate In CK",
				pcmEntity.getProjectConnectorId());
			pcmTO.setProjectConnectorId(projectConnectorId);
			pcmTO.setSyncStatus(syncStatus);
			pcmTO.setSyncMsg(syncMsg);
			pcmTO.setLastSyncDate(returnedData);
			updateProjectConnectorInCK(pcmTO);

		} else if (syncStatus.equalsIgnoreCase(CommonConstants.SYNC_FAILED)) {
			logger.info("Sync Status is failed");
			pcmEntity.setSyncStatus(syncStatus);
			pcmEntity.setSyncMsg(syncMsg);

			pcmEntity = pcmDao.save(pcmEntity);
			returnedData = pcmEntity.getSyncMsg();

			// PUSH updated lastSyncDate to CK
			logger.info("WSO2 - Updating ProjectConnectorMapping:{} lastSyncDate In CK",
				pcmEntity.getProjectConnectorId());
			pcmTO.setProjectConnectorId(projectConnectorId);
			pcmTO.setSyncStatus(syncStatus);
			pcmTO.setSyncMsg(syncMsg);
			updateProjectConnectorInCK(pcmTO);
		} else if (syncStatus.equalsIgnoreCase(CommonConstants.SYNC_RUNNING)) {
			logger.info("Sync Status is running");
			pcmEntity.setSyncStatus(syncStatus);
			pcmEntity.setSyncMsg(null);

			pcmEntity = pcmDao.save(pcmEntity);
			returnedData = pcmEntity.getSyncStatus();

			// PUSH updated lastSyncDate to CK
			logger.info("WSO2 - Updating ProjectConnectorMapping:{} lastSyncDate In CK",
				pcmEntity.getProjectConnectorId());
			pcmTO.setProjectConnectorId(projectConnectorId);
			pcmTO.setSyncStatus(syncStatus);
			pcmTO.setSyncMsg(null);

			// NOTE: Only for Excel, FieldMappingDetails needs to be synced with the CK
			// temporarily it is sent with the RUNNING sync status update
			if (toolName.equals(EXCEL.label)) {
				pcmTO.setFieldDetailsList(fieldMappingService.getFieldMappingDetails(pcmEntity, true));
			}

			updateProjectConnectorInCK(pcmTO);
		}

		return returnedData;
	}

	public String updateHistorySyncDetails(ProjectConnectorMappingEntity pcmEntity, SyncStatus syncStatus, String syncMsg) throws CustomException {

		String lastHistorySyncDate = null;
		int pcmId = pcmEntity.getProjectConnectorId();
		String toolName = pcmEntity.getConnectorHubEntity().getToolMasterEntity().getToolName();

		ProjectConnectorMappingTO pcmTO = new ProjectConnectorMappingTO();
		pcmTO.setProjectConnectorId(pcmId);
		pcmTO.setToolName(toolName);

		switch (syncStatus) {
			case COMPLETED -> {
				lastHistorySyncDate = OffsetDateTime.now(ZoneOffset.UTC).format(dateTimeStandardFormat) + "Z";
				pcmDao.updateHistorySyncAsCompleted(pcmId, lastHistorySyncDate);
				pcmTO.setLastHistorySyncDate(lastHistorySyncDate);
			}
			case FAILED -> pcmDao.updateHistorySyncAsFailed(pcmId, syncMsg);
			case RUNNING -> {
				int count = pcmDao.updateHistorySyncAsRunning(pcmId);
				if (count == 0) {
					throw new CustomException(Exceptions.SYNC_ALREADY_RUNNING);
				}
			}
		}

		// PUSH updated lastHistorySyncDate to CK
		logger.info("WSO2 - Updating ProjectConnectorMapping:{} lastHistorySyncDate In CK", pcmId);
		pcmTO.setHistorySyncStatus(syncStatus.name());
		pcmTO.setHistorySyncMsg(syncMsg);

		updateProjectConnectorInCK(pcmTO);

		return lastHistorySyncDate;
	}

	@Override
	public String updateResyncFieldById(Integer projectConnectorId, String reSycnField) {

		Optional<ProjectConnectorMappingEntity> ptmDataOptional = pcmDao.findById(projectConnectorId);
		if (ptmDataOptional.isPresent()) {
			ProjectConnectorMappingEntity pcmEntity = ptmDataOptional.get();
			pcmEntity.setResyncField(reSycnField);
			pcmDao.save(pcmEntity);

			return "SUCCESS";
		}
		return CommonConstants.SYNC_FAILED;
	}

	@Override
	public String updateResyncHistoryFieldById(Integer projectConnectorId, String reSycnHistoryField) {

		Optional<ProjectConnectorMappingEntity> ptmDataOptional = pcmDao.findById(projectConnectorId);
		if (ptmDataOptional.isPresent()) {
			ProjectConnectorMappingEntity pcmEntity = ptmDataOptional.get();
			pcmEntity.setResyncHistoryField(reSycnHistoryField);
			pcmDao.save(pcmEntity);

			return "SUCCESS";
		}
		return CommonConstants.SYNC_FAILED;
	}

	@Override
	public List<ProjectConnectorMappingTO> fetchPCMForProject(int projectId) throws CustomException {

		List<ProjectConnectorMappingTO> pcmList = new ArrayList<>();
		Iterable<ProjectConnectorMappingEntity> pcmData = pcmDao.findPCMByProjectId(projectId);

		for (ProjectConnectorMappingEntity pcmObj : pcmData) {

			//			3 ---> Category Id of Test Management Tool
			// Removed check to only fetch test management ids
			ProjectConnectorMappingTO pcm = new ProjectConnectorMappingTO();
			BeanUtils.copyProperties(pcmObj, pcm);
			pcm.setAccessToken("");
			if (pcmObj.getProjectId() != 0) {
				/*
				 * Removing auth type as paramater for keycloak changes User story - 20110
				 */
				//				ProjectTO projectTo = configService.fetchProject(pcmObj.getProjectId(), token);
				ProjectTO projectTo = fetchProject(pcmObj.getProjectId());
				pcm.setProjectId(projectTo.getProjectId());
				// pcm.setProjectName(projectTo.getProjectName());
			} else {
				throw new CustomException(HttpStatus.BAD_REQUEST,
					"Test Managemnt Tool is missing in Project Connector Mapping with ID: "
						+ pcmObj.getProjectConnectorId());
			}

			if (pcmObj.getConnectorHubEntity() != null
				&& pcmObj.getConnectorHubEntity().getToolMasterEntity() != null) {
				pcm.setConnectorId(pcmObj.getConnectorHubEntity().getConnectorId());
				pcm.setConnectorName(pcmObj.getConnectorHubEntity().getConnectorName());
				pcm.setToolId(pcmObj.getConnectorHubEntity().getToolMasterEntity().getToolId());
				pcm.setToolName(pcmObj.getConnectorHubEntity().getToolMasterEntity().getToolName());
				pcm.setToolIcon(pcmObj.getConnectorHubEntity().getToolMasterEntity().getToolIcon());
				pcm.setToolCategoryId(pcmObj.getConnectorHubEntity().getToolMasterEntity().getCategoryId());
				pcm.setToolCategory(
					pcmObj.getConnectorHubEntity().getToolMasterEntity().getToolCategoryEntity().getCategoryName());

			} else {
				throw new CustomException(HttpStatus.BAD_REQUEST,
					"Connector Master is missing in Project Connector Mapping with ID: "
						+ pcmObj.getProjectConnectorId());
			}

			pcmList.add(pcm);
		}
		return pcmList;

	}

	@Override
	public List<ProjectConnectorMappingTO> fetchPCMForSessionProject(int projectId) throws CustomException {

		List<ProjectConnectorMappingTO> pcmList = new ArrayList<>();
		Iterable<ProjectConnectorMappingEntity> pcmData = pcmDao.findPCMByProjectId(projectId);

		for (ProjectConnectorMappingEntity pcmObj : pcmData) {

			ProjectConnectorMappingTO pcm = new ProjectConnectorMappingTO();
			BeanUtils.copyProperties(pcmObj, pcm);
			pcm.setAccessToken("");
			if (pcmObj.getProjectId() != 0) {
				pcm.setProjectId(projectId);
			} else {
				throw new CustomException(HttpStatus.BAD_REQUEST,
					"Test Managemnt Tool is missing in Project Connector Mapping with ID: "
						+ pcmObj.getProjectConnectorId());
			}

			if (pcmObj.getConnectorHubEntity() != null
				&& pcmObj.getConnectorHubEntity().getToolMasterEntity() != null) {
				pcm.setConnectorId(pcmObj.getConnectorHubEntity().getConnectorId());
				pcm.setConnectorName(pcmObj.getConnectorHubEntity().getConnectorName());
				pcm.setToolId(pcmObj.getConnectorHubEntity().getToolMasterEntity().getToolId());
				pcm.setToolName(pcmObj.getConnectorHubEntity().getToolMasterEntity().getToolName());
				pcm.setToolIcon(pcmObj.getConnectorHubEntity().getToolMasterEntity().getToolIcon());
				pcm.setToolCategoryId(pcmObj.getConnectorHubEntity().getToolMasterEntity().getCategoryId());
				pcm.setToolCategory(
					pcmObj.getConnectorHubEntity().getToolMasterEntity().getToolCategoryEntity().getCategoryName());

			} else {
				throw new CustomException(HttpStatus.BAD_REQUEST,
					"Connector Master is missing in Project Connector Mapping with ID: "
						+ pcmObj.getProjectConnectorId());
			}

			pcmList.add(pcm);
		}
		return pcmList;

	}

	@SuppressWarnings("unchecked")
	public ProjectTO fetchProject(int projectId) throws CustomException {

		String authUrl = wso2Config.url() + PathConstants.AUTHENTICATION;
		String token = StoryUtils.generateToken(authUrl, wso2RestTemplate, wso2Config.basicAuth(), wso2Config.apiKey());

		HttpHeaders headers = new HttpHeaders();
		headers.add(CommonConstants.WSO2_ACCESS_TOKEN, token);
		headers.add(CommonConstants.API_KEY, wso2Config.apiKey());
		headers.setAccept(List.of(MediaType.APPLICATION_JSON));

		ResponseEntity<Object> response = null;
		ProjectTO projectTo = null;

		String uri = wso2Config.url() + PathConstants.FETCH_PROJECT_BY_ID.replace(PROJECT, String.valueOf(projectId));

		try {
			response = wso2RestTemplate.exchange(uri, HttpMethod.GET, new HttpEntity<Object>(headers), Object.class);
			if (response.getStatusCode().is2xxSuccessful()) {
				Object res = response.getBody();
				if (res != null) {
					Map<String, Object> responseData = (Map<String, Object>) res;
					Object resultTemp = responseData.get(CommonConstants.DATA);
					projectTo = objectMapper.convertValue(resultTemp, ProjectTO.class);
				}
			}
		} catch (IllegalStateException | ResourceAccessException e) {
			logger.error(ExceptionMessages.ERROR_MSG015, (Object[]) e.getStackTrace());
			throw new CustomException(Exceptions.ERROR516);
		} catch (IllegalArgumentException e) {
			logger.error(ExceptionMessages.ERROR_MSG016, (Object[]) e.getStackTrace());
			throw new CustomException(Exceptions.ERROR104);
		} catch (HttpStatusCodeException e) {
			throw RestTemplateUtil.handleInternalRestResponse(e, MSG_001, ExceptionMessages.ERROR_MSG001,
				ExceptionMessages.ERROR_MSG002);
		} catch (Exception e) {
			logger.error(ExceptionMessages.ERROR_MSG017, (Object[]) e.getStackTrace());
			throw new CustomException(Exceptions.ERROR050);
		}
		return projectTo;
	}

	@SuppressWarnings({ "unchecked" })
	public int fetchNextPcmId() throws CustomException {
		int pcmId = 0;
		String authUrl = wso2Config.url() + PathConstants.AUTHENTICATION;
		String token = StoryUtils.generateToken(authUrl, wso2RestTemplate, wso2Config.basicAuth(), wso2Config.apiKey());
		// String uri = PathConstants.HTTPS + wso2Ip + PathConstants.COLON + wso2Port +
		// PathConstants.NEXTPCMID;

		String uri = wso2Config.url() + PathConstants.NEXTPCMID;

		HttpHeaders headers = new HttpHeaders();
		headers.add(CommonConstants.WSO2_ACCESS_TOKEN, token);
		headers.add(CommonConstants.API_KEY, wso2Config.apiKey());
		headers.setAccept(List.of(MediaType.APPLICATION_JSON));
		HttpEntity<String> entity = new HttpEntity<>(headers);
		try {
			ResponseEntity<Object> response = wso2RestTemplate.exchange(uri, HttpMethod.GET, entity, Object.class);
			if (response.getStatusCode().is2xxSuccessful()) {
				Object reponseBody = response.getBody();
				if (reponseBody != null) {
					Map<String, Object> responseDataMap = (Map<String, Object>) reponseBody;
					pcmId = (Integer) responseDataMap.get("pcmId");
					logger.info("pcmId Fetched successfully  : {} ");
				}
			} else {
				logger.error(ExceptionMessages.ERROR_MSG051);
				throw new CustomException(Exceptions.ERROR050);
			}
		} catch (IllegalStateException | ResourceAccessException e) {
			logger.error(ExceptionMessages.ERROR_MSG015, (Object[]) e.getStackTrace());
			throw new CustomException(Exceptions.ERROR516);
		} catch (IllegalArgumentException e) {
			logger.error(ExceptionMessages.ERROR_MSG016, (Object[]) e.getStackTrace());
			throw new CustomException(Exceptions.ERROR104);
		} catch (Exception e) {
			logger.error(ExceptionMessages.ERROR_MSG017, (Object[]) e.getStackTrace());
			throw new CustomException(Exceptions.ERROR050);
		}

		return pcmId;
	}

	@Override
	public Integer fetchPCMRowCount(String toolApiUrl, String testPlanFolderName, Integer projectId,
		Integer connectorId) {
		return pcmDao.fetchPCMRowCount(toolApiUrl, testPlanFolderName, projectId, connectorId);
	}

	public Integer fetchADORowCount(String toolApiUrl, String testPlanFolderName, Integer projectId,
		Integer connectorId, String teams) {
		return pcmDao.fetchADORowCount(toolApiUrl, testPlanFolderName, projectId, connectorId, teams);
	}

	@Override
	public Integer fetchPCMGitLabRowCount(String toolApiUrl, String testPlanFolderName, int projectId,
		String branchName, int connectorId) {
		return pcmDao.fetchPCMGitLabRowCount(toolApiUrl, testPlanFolderName, projectId, branchName, connectorId);
	}
}
