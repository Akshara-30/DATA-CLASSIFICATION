package com.lti.knowledge.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import javax.persistence.EntityManager;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;

import org.apache.commons.lang.StringUtils;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.util.ObjectUtils;

import com.lti.common.constants.CommonConstants;
import com.lti.common.exception.CustomException;
import com.lti.common.to.FieldMappingTO;
import com.lti.common.utils.CustomValidators;
import com.lti.knowledge.dao.FieldMappingDao;
import com.lti.knowledge.dao.FieldMappingMstDao;
import com.lti.knowledge.dao.ProjectConnectorMappingDAO;
import com.lti.knowledge.dao.ToolDAO;
import com.lti.knowledge.entities.FieldMappingEntity;
import com.lti.knowledge.entities.FieldMappingMstEntity;
import com.lti.knowledge.entities.ProjectConnectorMappingEntity;
import com.lti.knowledge.entities.ToolMasterEntity;
import com.lti.knowledge.to.FieldMappingMstTO;

@Service
public class FieldMappingServiceImpl {

	@Autowired
	private ProjectConnectorMappingDAO pcmDao;

	@Autowired
	private ToolDAO toolDao;

	@Autowired
	private FieldMappingDao fieldMappingDao;

	@Autowired
	private FieldMappingMstDao fieldMappingMstDao;

	@Autowired
	private EntityManager entityManager;

	public List<FieldMappingMstTO> getFieldMappingMstDetails(Integer toolId) {
		List<FieldMappingMstTO> result = new ArrayList<>();
		Optional<ToolMasterEntity> toolEntityOpt = toolDao.findById(toolId);
		if (toolEntityOpt.isPresent()) {
			ToolMasterEntity toolEntity = toolEntityOpt.get();
			if (toolEntity != null) {
				List<FieldMappingMstEntity> fieldMstEntityList = toolEntity.getFieldMappingMstEntityList();
				List<FieldMappingMstTO> fieldMasterTOList = new ArrayList<>();
				fieldMstEntityList.forEach(fieldMstEntity -> {
					FieldMappingMstTO fieldMstTO = new FieldMappingMstTO();
					BeanUtils.copyProperties(fieldMstEntity, fieldMstTO);
					fieldMstTO.setDataCategoryName(fieldMstEntity.getDataCategoryMasterEntity().getDataCategoryName());
					fieldMasterTOList.add(fieldMstTO);
				});
				result.addAll(fieldMasterTOList);
			}
		}
		return result;
	}

	public Map<String, Object> getFieldMappingDetails(Integer projectConnectorId) {
		Map<String, Object> result = new HashMap<>();
		Optional<ProjectConnectorMappingEntity> projectConnectorMappingObj = pcmDao.findById(projectConnectorId);
		if (projectConnectorMappingObj.isPresent()) {
			ProjectConnectorMappingEntity projConnectorEntity = projectConnectorMappingObj.get();
			result.put("fieldMappingDetails", getFieldMappingDetails(projConnectorEntity, false));
			result.put("default", projConnectorEntity.getFieldMappingEntityList().isEmpty());
			result.put("projectConnectorId", projectConnectorId);
		}
		return result;
	}

	public List<FieldMappingTO> getFieldMappingDetails(ProjectConnectorMappingEntity pcmEntity, boolean excludeEmpty) {

		List<FieldMappingMstTO> fieldMappingMstList =
			getFieldMappingMstDetails(pcmEntity.getConnectorHubEntity().getToolId());
		List<FieldMappingEntity> fieldMappingOfPCMList = pcmEntity.getFieldMappingEntityList();

		List<FieldMappingTO> fieldMappingDetails = new ArrayList<>(fieldMappingMstList.size());
		if (fieldMappingOfPCMList.isEmpty()) {
			for (FieldMappingMstTO fieldMappingMstTO : fieldMappingMstList) {
				if (excludeEmpty && StringUtils.isEmpty(fieldMappingMstTO.getValue())) {
					continue;
				}
				FieldMappingTO fieldMappingTO = new FieldMappingTO();
				BeanUtils.copyProperties(fieldMappingMstTO, fieldMappingTO);
				fieldMappingTO.setDataCategoryName(fieldMappingMstTO.getDataCategoryName());
				fieldMappingDetails.add(fieldMappingTO);
			}
		} else {
			HashMap<Integer, HashMap<String, String>> fieldMappingOfPCMGroupByEntityAndKey = new HashMap<>();
			fieldMappingOfPCMList.forEach(f -> {
				if (!fieldMappingOfPCMGroupByEntityAndKey.containsKey(f.getDataCategoryId())) {
					fieldMappingOfPCMGroupByEntityAndKey.put(f.getDataCategoryId(), new HashMap<>());
				}
				fieldMappingOfPCMGroupByEntityAndKey.get(f.getDataCategoryId()).put(f.getKey(), f.getValue());
			});
			for (FieldMappingMstTO fieldMappingMstTO : fieldMappingMstList) {
				String value = fieldMappingMstTO.getValue();

				if (fieldMappingOfPCMGroupByEntityAndKey.containsKey(fieldMappingMstTO.getDataCategoryId())
					&& fieldMappingOfPCMGroupByEntityAndKey.get(fieldMappingMstTO.getDataCategoryId())
					.containsKey(fieldMappingMstTO.getKey())) {
					value = fieldMappingOfPCMGroupByEntityAndKey.get(fieldMappingMstTO.getDataCategoryId())
						.get(fieldMappingMstTO.getKey());
				}

				if (excludeEmpty && StringUtils.isEmpty(value)) {
					continue;
				}

				FieldMappingTO fieldMappingTO = new FieldMappingTO();
				BeanUtils.copyProperties(fieldMappingMstTO, fieldMappingTO);
				fieldMappingTO.setValue(value);
				fieldMappingTO.setDataCategoryName(fieldMappingMstTO.getDataCategoryName());
				fieldMappingDetails.add(fieldMappingTO);
			}
		}

		return fieldMappingDetails;
	}

	public boolean saveFieldMappingDetails(int projectConnectorId, boolean isDefault,
		List<FieldMappingTO> changedMappings) throws CustomException {

		CustomValidators.validateNegative(projectConnectorId, "Project Connector Id");
		if (ObjectUtils.isEmpty(changedMappings)) {
			throw new CustomException(HttpStatus.BAD_REQUEST, "Field Mapping Changed Details cannot be empty");
		}

		Optional<ProjectConnectorMappingEntity> projectConnectorMappingOpt = pcmDao.findById(projectConnectorId);
		if (projectConnectorMappingOpt.isEmpty()) {
			throw new CustomException(HttpStatus.BAD_REQUEST,
				"Project Connector Mapping does not exists for ID: " + projectConnectorId);
		}

		ProjectConnectorMappingEntity pcmEntity = projectConnectorMappingOpt.get();
		int toolId = pcmEntity.getConnectorHubEntity().getToolId();
		String toolName = pcmEntity.getConnectorHubEntity().getToolMasterEntity().getToolName();

		for (FieldMappingTO mapping : changedMappings) {
			CustomValidators.validateNegative(mapping.getDataCategoryId(), "Data Category Id");
			CustomValidators.validateNull(mapping.getKey(), "Field Mapping Key");
			if (CommonConstants.Tool.EXCEL.label.equals(toolName) && (mapping.getKey().equals("id") || mapping.getKey()
				.equals("type"))) {
				throw new CustomException(HttpStatus.BAD_REQUEST,
					"Field Mapping for Id/Type is not allowed to " + "change");
			}
		}

		List<FieldMappingMstEntity> fieldMappingMstOfChangedIdList =
			findAllByDataCategoryAndKeyPairs(changedMappings, toolId);

		if (fieldMappingMstOfChangedIdList.size() != changedMappings.size()) {
			throw new CustomException(HttpStatus.BAD_REQUEST,
				"Field Mapping Changed Details are not valid for Tool ID: " + toolId);
		}

		HashMap<Integer, HashMap<String, String>> changedMappingsGroupByEntityAndKey = new HashMap<>();
		changedMappings.forEach(changes -> {
			int dataCategoryId = changes.getDataCategoryId();

			if (!changedMappingsGroupByEntityAndKey.containsKey(dataCategoryId)) {
				changedMappingsGroupByEntityAndKey.put(dataCategoryId, new HashMap<>());
			}
			changedMappingsGroupByEntityAndKey.get(dataCategoryId)
				.put(changes.getKey(), StringUtils.trim(changes.getValue()));
		});

		List<FieldMappingEntity> fieldMappingUnchangedListToDelete = new ArrayList<>();
		List<FieldMappingEntity> fieldMappingChangedListToSave =
			fieldMappingMstOfChangedIdList.stream().map(mstEntity -> {
				String changedValue =
					changedMappingsGroupByEntityAndKey.get(mstEntity.getDataCategoryId()).get(mstEntity.getKey());

				FieldMappingEntity entity = new FieldMappingEntity();
				BeanUtils.copyProperties(mstEntity, entity);
				entity.setProjectConnectorId(projectConnectorId);
				entity.setValue(changedValue);

				if (StringUtils.equals(mstEntity.getValue(), changedValue)) {
					fieldMappingUnchangedListToDelete.add(entity);
					return null;
				}
				return entity;
			}).filter(Objects::nonNull).toList();

		if (!fieldMappingUnchangedListToDelete.isEmpty())
			fieldMappingDao.deleteAllInBatch(fieldMappingUnchangedListToDelete);
		if (!fieldMappingChangedListToSave.isEmpty()) fieldMappingDao.saveAll(fieldMappingChangedListToSave);

		return true;
	}

	List<FieldMappingMstEntity> findAllByDataCategoryAndKeyPairs(List<FieldMappingTO> pairs, int toolId) {
		CriteriaBuilder cb = entityManager.getCriteriaBuilder();
		CriteriaQuery<FieldMappingMstEntity> query = cb.createQuery(FieldMappingMstEntity.class);
		Root<FieldMappingMstEntity> root = query.from(FieldMappingMstEntity.class);

		List<Predicate> predicates = pairs.stream()
			.map(pair -> cb.and(cb.equal(root.get("dataCategoryId"), pair.getDataCategoryId()),
				cb.equal(root.get("key"), pair.getKey())))
			.toList();

		query.where(cb.and(cb.equal(root.get("toolId"), toolId), cb.or(predicates.toArray(new Predicate[0]))));

		return entityManager.createQuery(query).getResultList();
	}
}
