package com.lti.knowledge.service.impl;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.lti.knowledge.dao.ConnectorHubDAO;
import com.lti.knowledge.entities.ConnectorHubEntity;
import com.lti.knowledge.entities.ToolMasterEntity;
import com.lti.knowledge.service.CKMuxConnectorHubService;
import com.lti.knowledge.to.ConnectorHubTO;

import ch.qos.logback.classic.Logger;

@Service
public class CKMuxConnectorHubServiceImpl implements CKMuxConnectorHubService {

	private static final Logger logger = (Logger) LoggerFactory.getLogger(CKMuxConnectorHubServiceImpl.class);

	@Autowired
	private ConnectorHubDAO connectorHubDao;

	@Override
	public List<ConnectorHubTO> getConnectors() {
		List<ConnectorHubTO> connectorHubTOList = new ArrayList<>();

		Iterable<ConnectorHubEntity> connectorHubEntityIterable = connectorHubDao.findAll();
		connectorHubEntityIterable.forEach(connectorHubEntity -> {
			ConnectorHubTO connectorHubTO = new ConnectorHubTO();
			BeanUtils.copyProperties(connectorHubEntity, connectorHubTO);

			ToolMasterEntity toolMasterEntity = connectorHubEntity.getToolMasterEntity();
			connectorHubTO.setToolName(toolMasterEntity.getToolName());
			connectorHubTO.setToolIcon(toolMasterEntity.getToolIcon());
			connectorHubTO.setCategoryId(toolMasterEntity.getCategoryId());
			connectorHubTO.setCategoryName(toolMasterEntity.getToolCategoryEntity().getCategoryName());

			connectorHubTOList.add(connectorHubTO);
		});
		return connectorHubTOList;
	}
}