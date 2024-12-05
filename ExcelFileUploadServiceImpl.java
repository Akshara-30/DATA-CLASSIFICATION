package com.lti.knowledge.service.impl;

import static org.bouncycastle.util.encoders.Hex.toHexString;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.PostConstruct;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.data.util.Pair;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.util.MultiValueMap;
import org.springframework.util.MultiValueMapAdapter;
import org.springframework.util.ObjectUtils;
import org.springframework.web.multipart.MultipartFile;

import com.lti.common.constants.CommonConstants;
import com.lti.common.exception.CustomException;
import com.lti.common.exception.CustomRuntimeException;
import com.lti.common.to.ExcelFileDetailTO;
import com.lti.common.to.ProjectConnectorMappingTO;
import com.lti.knowledge.dao.ExcelFileDetailDAO;
import com.lti.knowledge.dao.ProjectConnectorMappingDAO;
import com.lti.knowledge.entities.ExcelFileDetailEntity;
import com.lti.knowledge.entities.ProjectConnectorMappingEntity;

@Service
public class ExcelFileUploadServiceImpl {

	@Value("${assets.path}")
	private String assetsPath;

	@Autowired
	private ExcelFileDetailDAO excelFileDetailDAO;

	@Autowired
	private ProjectConnectorMappingDAO pcmDao;

	private String excelDirectoryUrl;
	private String excelTempDirectoryUrl;

	private static final int EXCEL_TEMP_FILE_PURGE_THREAD_ALIVE_TIME_MS = 30 * 60 * 1000;
	private static final int EXCEL_TEMP_FILE_PURGE_SCHEDULE_MS = 20 * 60 * 1000;

	// uuid -> [filename, scheduledTask]
	// holds file uploaded in temp directory
	private Map<String, ExcelFilePurgeTask> excelTempFileStagedMap = null;
	private ScheduledThreadPoolExecutor purgingScheduledExecutorService = null;

	private static final Logger logger = LoggerFactory.getLogger(ExcelFileUploadServiceImpl.class);

	@PostConstruct
	private void setup() throws IOException {
		excelDirectoryUrl = assetsPath + "/excel/";
		excelTempDirectoryUrl = excelDirectoryUrl + "/.tmp/";

		purgingScheduledExecutorService = (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(1);
		purgingScheduledExecutorService.setKeepAliveTime(EXCEL_TEMP_FILE_PURGE_THREAD_ALIVE_TIME_MS,
			TimeUnit.MILLISECONDS);
		purgingScheduledExecutorService.allowCoreThreadTimeOut(true);
		purgingScheduledExecutorService.setRemoveOnCancelPolicy(true);

		purgingScheduledExecutorService.execute(this::purgeAllTempExcelFilesOnStartup);

		migrateFromPcmToExcelFileDetailsTable();
	}

	// TODO: Remove the code in future release
	private void migrateFromPcmToExcelFileDetailsTable() throws IOException {
		if (!excelDirectoryExists()) {
			return;
		}

		long count = excelFileDetailDAO.count();
		if (count > 0) return;

		List<ProjectConnectorMappingEntity> pcmEntityList = pcmDao.findByConnectorId(4);
		if (CollectionUtils.isEmpty(pcmEntityList)) return;

		List<ExcelFileDetailEntity> fileDetailEntities = new ArrayList<>(pcmEntityList.size());
		HashMap<String, String> fileNameMap = new HashMap<>(pcmEntityList.size());

		pcmEntityList.forEach(pcmEntity -> {
			ExcelFileDetailEntity fileDetailEntity = new ExcelFileDetailEntity();
			fileDetailEntity.setPcmId(pcmEntity.getProjectConnectorId());
			fileDetailEntity.setFileId(UUID.randomUUID().toString());
			fileDetailEntity.setFileName(pcmEntity.getTestPlanFolderName());
			fileDetailEntity.setSheets(pcmEntity.getToolApiUrl());
			fileDetailEntities.add(fileDetailEntity);

			fileNameMap.put(
				"PCM%s_%s".formatted(pcmEntity.getProjectConnectorId(), pcmEntity.getTestPlanFolderName()),
				fileDetailEntity.getFileId() + FilenameUtils.EXTENSION_SEPARATOR + FilenameUtils.getExtension(pcmEntity.getTestPlanFolderName())
			);
		});

		Stream<Path> fileList = Files.list(excelDirectoryInstance().toPath());
		fileList
			.filter(path -> fileNameMap.containsKey(path.toFile().getName()))
			.forEach(path -> {
				try {
					Files.move(path, path.resolveSibling(fileNameMap.get(path.toFile().getName())));
				} catch (IOException e) {
					fileList.close();
					throw new RuntimeException(e);
				}
			}
		);
		fileList.close();

		excelFileDetailDAO.saveAll(fileDetailEntities);
	}

	// --------------------------------------------------------------------------------------------------------------
	// Verify

	void verifyExcelFileDetailsForSave(
		ProjectConnectorMappingTO pcmTo, ProjectConnectorMappingEntity pcmEntityToSave) throws CustomException {
		if (!pcmTo.getToolName().equals(CommonConstants.Tool.EXCEL.label))
			return;

		Set<String> idSet = new HashSet<>(pcmTo.getExcelFiles().size());
		pcmTo.setExcelFiles(
			pcmTo.getExcelFiles().stream().filter(f -> idSet.add(f.getFileId())).collect(Collectors.toList())
		);

		// file upload followed by PCM save/update
		// file is bound to be present in the map
		checkExcelFileIsStagedInMap(pcmTo.getExcelFiles());

		checkDuplicateFileUpload(pcmTo.getExcelFiles(), excelTempDirectoryInstance());

		List<ExcelFileDetailEntity> fileDetailEntities = new ArrayList<>(pcmTo.getExcelFiles().size());
		for (ExcelFileDetailTO fileDetail : pcmTo.getExcelFiles()) {
			ExcelFileDetailEntity fileDetailEntity = new ExcelFileDetailEntity();
			BeanUtils.copyProperties(fileDetail, fileDetailEntity);
			fileDetailEntities.add(fileDetailEntity);
		}

		pcmEntityToSave.setExcelFileDetailEntityList(fileDetailEntities);
	}

	Pair<Set<String>, Set<String>> verifyExcelFileDetailsForUpdate(ProjectConnectorMappingTO pcmTo,
		ProjectConnectorMappingEntity pcmEntityToSave) throws CustomException {

		if (!pcmTo.getToolName().equals(CommonConstants.Tool.EXCEL.label))
			return null;

		Set<String> idSet = new HashSet<>(pcmTo.getExcelFiles().size());
		pcmTo.setExcelFiles(
			pcmTo.getExcelFiles().stream().filter(f -> idSet.add(f.getFileId())).collect(Collectors.toList())
		);

		// map will be used for saving the files
		HashMap<String, ExcelFileDetailTO> inputExcelFileToSaveMap = new HashMap<>(pcmTo.getExcelFiles().size());
		for (ExcelFileDetailTO fileDetail : pcmTo.getExcelFiles()) {
			inputExcelFileToSaveMap.put(fileDetail.getFileId(), fileDetail);
		}

		// map will be used for deleting the files
		HashMap<String, ExcelFileDetailEntity> dbExcelFileToDeleteMap
			= new HashMap<>(pcmEntityToSave.getExcelFileDetailEntityList().size());
		for (ExcelFileDetailEntity fileDetailEntity : pcmEntityToSave.getExcelFileDetailEntityList()) {
			dbExcelFileToDeleteMap.put(fileDetailEntity.getFileId(), fileDetailEntity);
		}

		List<ExcelFileDetailEntity> fileDetailEntities = new ArrayList<>(pcmTo.getExcelFiles().size());

		for (ExcelFileDetailTO fileDetail : pcmTo.getExcelFiles()) {
			ExcelFileDetailEntity fileDetailEntity = new ExcelFileDetailEntity();

			if (dbExcelFileToDeleteMap.containsKey(fileDetail.getFileId())) {
				ExcelFileDetailEntity dbFileDetailEntity = dbExcelFileToDeleteMap.get(fileDetail.getFileId());

				// set the filename to handle any arbitrary input filename
				fileDetail.setFileName(dbFileDetailEntity.getFileName());

				if (fileDetail.getSheets().equals(dbFileDetailEntity.getSheets())) {
					fileDetailEntity.setChangedDate(dbFileDetailEntity.getChangedDate());
				} else {
					fileDetailEntity.setChangedDate(null);
				}

				inputExcelFileToSaveMap.remove(fileDetail.getFileId());
				dbExcelFileToDeleteMap.remove(fileDetail.getFileId());
			} else {
				// file upload followed by PCM save/update
				// file is bound to be present in the map
				checkExcelFileIsStagedInMap(fileDetail);
			}

			fileDetailEntity.setPcmId(pcmTo.getProjectConnectorId());
			fileDetailEntity.setFileId(fileDetail.getFileId());
			fileDetailEntity.setFileName(fileDetail.getFileName());
			fileDetailEntity.setSheets(fileDetail.getSheets());

			fileDetailEntities.add(fileDetailEntity);
		}

		checkDuplicateFileUpload(pcmTo.getExcelFiles(), excelDirectoryInstance());

		pcmEntityToSave.setExcelFileDetailEntityList(fileDetailEntities);

		return Pair.of(inputExcelFileToSaveMap.keySet(), dbExcelFileToDeleteMap.keySet());
	}

	void checkDuplicateFileUpload(List<ExcelFileDetailTO> fileDetailList, File directory) throws CustomException {
		Map<String, String> fileIdToNameMap = fileDetailList.stream()
			.collect(Collectors.toMap(ExcelFileDetailTO::getFileId, ExcelFileDetailTO::getFileName));

		try (Stream<Path> pathStream = Files.find(directory.toPath(), 2,
			(path, attr) -> fileIdToNameMap.containsKey(filename(path)), FileVisitOption.FOLLOW_LINKS)) {

			MultiValueMap<Long, Object> sizeToHashMap = new MultiValueMapAdapter<>(new HashMap<>(fileDetailList.size()));

			pathStream.sorted(Comparator.comparingLong(p -> p.toFile().lastModified())).forEach(path -> {
				try {
					long size = Files.size(path);
					List<Object> list = sizeToHashMap.get(size);
					if (ObjectUtils.isEmpty(list)) {
						sizeToHashMap.add(size, path);
					} else {
						if (list.size() == 1) {
							list.set(0, hashFileContent((Path) list.get(0)));
						}
						String hash = hashFileContent(path);
						if (list.contains(hash)) {
							throw new CustomRuntimeException(HttpStatus.BAD_REQUEST,
								"Duplicate file upload detected: " + fileIdToNameMap.get(filename(path)));
						}
						list.add(hash);
					}
				} catch (IOException | NoSuchAlgorithmException e) {
					throw new CustomRuntimeException(HttpStatus.INTERNAL_SERVER_ERROR, "Failed to verify uploaded files", e);
				}
			});
		} catch (IOException e) {
			throw new CustomException(HttpStatus.INTERNAL_SERVER_ERROR, "Failed to verify uploaded files", e);
		}
	}

	String hashFileContent(Path path) throws NoSuchAlgorithmException, IOException {
		MessageDigest md = MessageDigest.getInstance("SHA-256");
		try (InputStream in = new BufferedInputStream(Files.newInputStream(path))) {
			byte[] buffer = new byte[8192];
			int read;
			while ((read = in.read(buffer)) > 0) {
				md.update(buffer, 0, read);
			}
		}
		return toHexString(md.digest());
	}

	// --------------------------------------------------------------------------------------------------------------
	// Directory Instance

	private File excelDirectoryInstance() {
		return new File(excelDirectoryUrl);
	}

	private boolean excelDirectoryExists() {
		return excelDirectoryInstance().exists();
	}

	private File excelTempDirectoryInstance() {
		return new File(excelTempDirectoryUrl);
	}

	File getExcelDirectory() throws CustomException {
		File directory = excelDirectoryInstance();
		boolean directoryExists;

		try {
			directoryExists = directory.exists();
		} catch (SecurityException e) {
			throw new CustomException(HttpStatus.INTERNAL_SERVER_ERROR,
				"Failed to read Excel storage directory, missing read access", e);
		}

		try {
			if (!directoryExists) {
				directoryExists = directory.mkdirs();
			}
			if (!directoryExists || !directory.canWrite()) {
				throw new SecurityException("Failed to write into Excel storage directory, missing write access");
			}
		} catch (SecurityException e) {
			throw new CustomException(HttpStatus.INTERNAL_SERVER_ERROR,
				"Failed to write into Excel storage directory, missing write access", e);
		}

		return directory;
	}

	File getExcelTempDirectory() throws CustomException {
		File directory = excelTempDirectoryInstance();
		boolean directoryExists;

		try {
			directoryExists = directory.exists();
		} catch (SecurityException e) {
			throw new CustomException(HttpStatus.INTERNAL_SERVER_ERROR,
				"Failed to read Excel storage directory, missing read access", e);
		}

		try {
			if (!directoryExists) {
				directoryExists = directory.mkdirs();
			}
			if (!directoryExists || !directory.canWrite()) {
				throw new SecurityException("Failed to write into Excel storage directory, missing write access");
			}
		} catch (SecurityException e) {
			throw new CustomException(HttpStatus.INTERNAL_SERVER_ERROR,
				"Failed to write into Excel storage directory, missing write access", e);
		}

		return directory;
	}


	// --------------------------------------------------------------------------------------------------------------
	// Excel Sheets

	String readExcelSheetsName(InputStream fileInputStream) throws CustomException {
		StringBuilder sheetNames = new StringBuilder();

		try (Workbook workbook = WorkbookFactory.create(fileInputStream)) {
			Iterator<Sheet> sheetIterator = workbook.sheetIterator();
			while (sheetIterator.hasNext()) {
				Sheet sheet = sheetIterator.next();
				if (sheet.getSheetName() != null) {
					sheetNames.append(sheet.getSheetName());
					if (sheetIterator.hasNext())
						sheetNames.append(",");
				}
			}
		} catch (Exception e) {
			throw new CustomException(HttpStatus.BAD_REQUEST, "Failed to retrieve Excel sheets", e);
		}

		return sheetNames.toString();
	}

	public String getExcelSheetName(MultipartFile file) throws CustomException {
		try {
			return readExcelSheetsName(file.getInputStream());
		} catch (IOException e) {
			throw new CustomException(HttpStatus.BAD_REQUEST, "Failed to read the Excel file", e);
		}
	}

	public HashMap<String, String> getPCMExcelSheetName(ProjectConnectorMappingEntity pcmEntity) throws CustomException {

		if (!CommonConstants.Tool.EXCEL.label.equals(pcmEntity.getConnectorHubEntity().getToolMasterEntity().getToolName())) {
			throw new CustomException(
				HttpStatus.BAD_REQUEST,
				"Project Connector ID: " + pcmEntity.getProjectConnectorId() + " is not associated with Excel tool"
			);
		}

		List<ExcelFileDetailEntity> fileDetailEntityList = pcmEntity.getExcelFileDetailEntityList();
		if (ObjectUtils.isEmpty(fileDetailEntityList)) {
			throw new CustomException(HttpStatus.BAD_REQUEST, "Excel file doesn't exists for PCM ID: " + pcmEntity.getProjectConnectorId());
		}

		if (!excelDirectoryExists()) {
			throw new CustomException(HttpStatus.INTERNAL_SERVER_ERROR, "Excel storage directory doesn't exists");
		}

		File excelDirectory = getExcelDirectory();

		Set<String> fileIds = fileDetailEntityList.stream().map(ExcelFileDetailEntity::getFileId)
			.collect(Collectors.toSet());

		File[] pcmExcelFiles = excelDirectory.listFiles((file, name) -> fileIds.contains(filename(name)));

		if (ObjectUtils.isEmpty(pcmExcelFiles)) {
			throw new CustomException(
				HttpStatus.BAD_REQUEST,
				"Excel file doesn't exists for PCM ID: " + pcmEntity.getProjectConnectorId()
				+ " Try to upload the Excel file and save Project Connector");
		}

		HashMap<String, String> resultMap = new HashMap<>(fileIds.size());
		try {
			for (File excelFile : pcmExcelFiles) {
				resultMap.put(filename(excelFile.getName()), readExcelSheetsName(new FileInputStream(excelFile)));
			}
		} catch (FileNotFoundException | SecurityException e) {
			throw new CustomException(
				HttpStatus.INTERNAL_SERVER_ERROR,
				"Failed to read the Excel file for PCM ID: " + pcmEntity.getProjectConnectorId(),
				e
			);
		}

		return resultMap;
	}


	// --------------------------------------------------------------------------------------------------------------
	// Save/Delete
	
	/* void saveExcel(MultipartFile file, int pcmId) throws CustomException {
		try {
			File directory = getExcelDirectory();

			String prefix = "PCM" + pcmId + "_";

			File[] previousFiles = directory.listFiles((f, name) -> name.startsWith(prefix));
			if (!ObjectUtils.isEmpty(previousFiles)) {
				for (File prevFile : previousFiles) {
					Files.delete(prevFile.toPath());
				}
			}

			String filename = file.getOriginalFilename();
			String fileUrl = directory + "/" + prefix + filename;

			try (FileOutputStream f = new FileOutputStream(fileUrl)) {
				InputStream in = file.getInputStream();
				int ch = 0;
				while ((ch = in.read()) != -1) {
					f.write(ch);
				}
				f.flush();
			}
		} catch (CustomException e) {
			throw e;
		} catch (Exception e) {
			throw new CustomException(HttpStatus.INTERNAL_SERVER_ERROR, "Failed to save the uploaded excel", e);
		}
	}

	void deleteExcel(int pcmId) throws CustomException {
		try {
			if (!excelDirectoryExists()) {
				return;
			}

			File excelDirectory = getExcelDirectory();

			String prefix = "PCM" + pcmId + "_";
			File[] pcmExcelFiles = excelDirectory.listFiles((file, name) -> name.startsWith(prefix));
			if (ObjectUtils.isEmpty(pcmExcelFiles)) {
				return;
			}
			for (File prevFile : pcmExcelFiles) {
				Files.delete(prevFile.toPath());
			}
		} catch (Exception e) {
			throw new CustomException(HttpStatus.INTERNAL_SERVER_ERROR, "Failed to delete the excel", e);
		}
	} */


	// --------------------------------------------------------------------------------------------------------------
	// Temp storage

	public List<String> stageUploadedExcelFileToTempStorage(List<MultipartFile> files) throws CustomException {
		File excelTempDirectory = getExcelTempDirectory();
		List<String> idList = new ArrayList<>(files.size());

		for (MultipartFile file : files) {
			UUID uuid = UUID.randomUUID();
			String path = excelTempDirectory + "/" + uuid + "." + FilenameUtils.getExtension(file.getOriginalFilename());

			try {
				FileUtils.copyInputStreamToFile(file.getInputStream(), new File(path));

				// auto remove file from the temp directory
				// if the corresponding PCM save/update operation is not completed
				scheduleTempExcelFilePurging(uuid.toString(), file.getOriginalFilename());

				idList.add(uuid.toString());
			} catch (Exception e) {
				throw new CustomException(
					HttpStatus.INTERNAL_SERVER_ERROR,
					"Failed to save the uploaded Excel file: " + file.getOriginalFilename(),
					e
				);
			}
		}

		return idList;
	}

	void commitUploadedExcelFileToFinalStorage(List<ExcelFileDetailTO> fileDetailList) throws CustomException {
		Set<String> fileIds = fileDetailList.stream().map(ExcelFileDetailTO::getFileId).collect(Collectors.toSet());
		commitUploadedExcelFileToFinalStorage(fileIds);
	}

	void commitUploadedExcelFileToFinalStorage(Set<String> fileIds) throws CustomException {
		File excelTempDirectory = getExcelTempDirectory();

		File[] stagedFileList = excelTempDirectory.listFiles((f, name) -> fileIds.contains(filename(name)));
		if (ObjectUtils.isEmpty(stagedFileList) || stagedFileList.length != fileIds.size()) {
			throw new CustomException(HttpStatus.BAD_REQUEST,
				"Excel file is missing/invalid. Try again by uploading the Excel file before saving Project Connector");
		}

		// move file from temp to final storage
		for (File stagedFile : stagedFileList) {
			try {
				FileUtils.moveFileToDirectory(stagedFile, excelDirectoryInstance(), false);
			} catch (Exception e) {
				throw new CustomException(
					HttpStatus.INTERNAL_SERVER_ERROR, "Failed to save the uploaded Excel file: " + stagedFile.getName(),
					e
				);
			}
		}

		excludeTempExcelFileFromPurging(fileIds);
	}

	void deleteExcelFileFromFinalStorage(Set<String> fileIds) throws CustomException {
		File excelDirectory = getExcelDirectory();

		// ignore missing files
		File[] fileList = excelDirectory.listFiles((f, name) -> fileIds.contains(filename(name)));
		if (ObjectUtils.isEmpty(fileList)) {
			return;
		}

		for (File file : fileList) {
			try {
				FileUtils.delete(file);
			} catch (Exception e) {
				throw new CustomException(
					HttpStatus.INTERNAL_SERVER_ERROR, "Failed to delete the Excel file: " + file.getName(),
					e
				);
			}
		}
	}

	/* private void deleteOldPCMExcelFile(int pcmId) throws CustomException {
		try {
			File directory = getExcelDirectory();

			String prefix = "PCM" + pcmId + "_";

			File[] previousFiles = directory.listFiles((f, name) -> name.startsWith(prefix));
			if (!ObjectUtils.isEmpty(previousFiles)) {
				for (File prevFile : previousFiles) {
					Files.delete(prevFile.toPath());
				}
			}
		} catch (Exception e) {
			throw new CustomException(HttpStatus.INTERNAL_SERVER_ERROR,
				"Failed to delete the old Excel file associated with the PCM: " + pcmId, e);
		}
	} */

	void checkExcelFileIsStagedInMap(ExcelFileDetailTO fileDetail) throws CustomException {
		if (ObjectUtils.isEmpty(excelTempFileStagedMap)
			|| !excelTempFileStagedMap.containsKey(fileDetail.getFileId())) {
			throw new CustomException(HttpStatus.BAD_REQUEST,
				"Excel file is missing/invalid. Try again by uploading the Excel file before saving Project Connector");
		}

		// set the filename to handle any arbitrary input filename
		fileDetail.setFileName(excelTempFileStagedMap.get(fileDetail.getFileId()).filename());
	}

	void checkExcelFileIsStagedInMap(List<ExcelFileDetailTO> fileDetailList) throws CustomException {
		if (ObjectUtils.isEmpty(excelTempFileStagedMap)
			|| !fileDetailList.stream().allMatch(fileDetail -> excelTempFileStagedMap.containsKey(fileDetail.getFileId()))) {
			throw new CustomException(HttpStatus.BAD_REQUEST,
				"Excel file is missing/invalid. Try again by uploading the Excel file before saving Project Connector");
		}

		// set the filename to handle any arbitrary input filename
		for (ExcelFileDetailTO fileDetail : fileDetailList) {
			fileDetail.setFileName(excelTempFileStagedMap.get(fileDetail.getFileId()).filename());
		}
	}

	private void scheduleTempExcelFilePurging(String uuid, String filename) {
		if (excelTempFileStagedMap == null)
			excelTempFileStagedMap = new ConcurrentHashMap<>(5);

		ScheduledFuture<?> task = purgingScheduledExecutorService.schedule(this::purgeTempExcelFilesOnTimeout,
			EXCEL_TEMP_FILE_PURGE_SCHEDULE_MS, TimeUnit.MILLISECONDS);

		excelTempFileStagedMap.put(uuid, new ExcelFilePurgeTask(filename, task));

		logger.debug("staged: {} tasks: {} (scheduled)", excelTempFileStagedMap.size(),
			purgingScheduledExecutorService.getQueue().size());
	}

	private void purgeAllTempExcelFilesOnStartup() {
		purgeTempExcelFiles(true, null);
	}

	private void purgeTempExcelFilesOnTimeout() {
		purgeTempExcelFiles(
			false,
			path -> ((System.currentTimeMillis() - path.toFile().lastModified()) >= (EXCEL_TEMP_FILE_PURGE_SCHEDULE_MS / 2))
		);
	}

	private void purgeTempExcelFiles(boolean cleanOnStartup, Predicate<Path> filterPredicate) {
		if (!excelTempDirectoryInstance().exists())
			return;

		logger.debug("Starting purging process of temp excel file");

		AtomicInteger deletedCount = new AtomicInteger();
		try (Stream<Path> tempDirStream = Files.list(Path.of(excelTempDirectoryUrl))) {
			tempDirStream
				.filter(path -> cleanOnStartup || filterPredicate.test(path))
				.forEach(path -> {
						try {
							Files.delete(path);
							if (!cleanOnStartup) {
								String fileId = path.toFile().getName().substring(0, 36); // uuid length
								excelTempFileStagedMap.get(fileId).task().cancel(false);
								excelTempFileStagedMap.remove(fileId);
							}
							deletedCount.getAndIncrement();
							logger.debug("Removing Excel file from .tmp directory: {}", path.toFile().getName());
						} catch (Exception e) {
							logger.error("Failed to purge the temp excel files", e);
						}
					}
				);

			if (!cleanOnStartup) {
				logger.debug("staged: {} tasks: {} (purged)", excelTempFileStagedMap.size(),
					purgingScheduledExecutorService.getQueue().size());

				if (excelTempFileStagedMap.isEmpty()) {
					clearScheduledQueuedTasksForPurging();
				}
			}

			logger.debug("Completed purging process of temp excel file: {}", deletedCount.get());
		} catch (Exception e) {
			logger.error("Failed to purge the temp excel files", e);
		}
	}

	private void excludeTempExcelFileFromPurging(Set<String> fileIds) {
		fileIds.forEach(fileId -> {
			excelTempFileStagedMap.get(fileId).task().cancel(true);
			excelTempFileStagedMap.remove(fileId);
		});

		logger.debug("staged: {} tasks: {} (excluded)", excelTempFileStagedMap.size(),
			purgingScheduledExecutorService.getQueue().size());

		if (excelTempFileStagedMap.isEmpty()) {
			clearScheduledQueuedTasksForPurging();
		}
	}

	void clearScheduledQueuedTasksForPurging() {
		logger.debug("staged: {} tasks: {} (clear before)", excelTempFileStagedMap.size(),
			purgingScheduledExecutorService.getQueue().size());

		if (!purgingScheduledExecutorService.getQueue().isEmpty()) {
			purgingScheduledExecutorService.getQueue()
				.forEach(runnable -> purgingScheduledExecutorService.remove(runnable));
			purgingScheduledExecutorService.purge();

			logger.debug("staged: {} tasks: {} (clear after)", excelTempFileStagedMap.size(),
				purgingScheduledExecutorService.getQueue().size());
		}
	}


	// --------------------------------------------------------------------------------------------------------------

	public Resource getSampleExcel() throws CustomException {
		FileSystemResource sampleExcelResource = new FileSystemResource(
			getExcelDirectory().getPath() + "/Excel_Sample_File.xlsx");
		if (!sampleExcelResource.exists() || !sampleExcelResource.isReadable()) {
			throw new CustomException(HttpStatus.INTERNAL_SERVER_ERROR,
				"Sample Excel file doesn't exists or is missing read access");
		}
		return sampleExcelResource;
	}

	String filename(Path path) {
		return FilenameUtils.getBaseName(path.toFile().getName());
	}
	String filename(String fileName) {
		return FilenameUtils.getBaseName(fileName);
	}
}

record ExcelFilePurgeTask(String filename, ScheduledFuture<?> task) {}
