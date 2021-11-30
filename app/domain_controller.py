import requests
import json
from governance_controller import GovernanceController


class DomainController:

    __instance = None

    @staticmethod
    def getInstance():
        if DomainController.__instance is None:
            DomainController()
        return DomainController.__instance

    def __init__(self):
        if DomainController.__instance is not None:
            pass
        else:
            DomainController.__instance = self

    # Create domain
    def createDomain(self, domainName, domainDescription=""):

        gover_control = GovernanceController.getInstance()
        cookies = gover_control.getCookie()
        headers = gover_control.getHeaders()

        data = '{"id":"-1","name":"' + domainName + '","description":"' + domainDescription + '","type":"XD","subtype":"DS","properties":{"dataStore":{"version":"","url":"","security":""}},"enrichedProperties":{},"metadataPath":"' + domainName + ':"' + ',"active":true,"vendor":"crossdata"}'
        data = json.loads(data)

        response = requests.post(gover_control.getApiUrl() + '/dictionary/user/catalog/v1/dataAsset/', headers=headers,
                                 cookies=cookies, json=data, verify=False)

        return response

    # Delete domain
    def deleteDomain(self, domainName):

        gover_control = GovernanceController.getInstance()
        cookies = gover_control.getCookie()
        headers = gover_control.getHeaders()

        params = (
            ('metadataPathLike', domainName + ':%'),
            ('featuresToUpdate', '-1'),
        )

        response = requests.delete(
            gover_control.getApiUrl() + '/dictionary/user/catalog/v1/dataAsset/federatedResource', headers=headers,
            params=params, cookies=cookies, verify=False)

        return response

    # Add physical tables
    def addPhysicalTable(self, domainName, dataSource, database, tableName, schema=""):

        physicalTableName = schema + "." + tableName if schema != "" else tableName
        logicalTableName = schema + "_" + tableName if schema != "" else tableName

        physicalTableMetadataPath = dataSource + "://" + database
        logicalTableMetadataPath = domainName + '://' + domainName

        return self.addPhysicalTableByMetadatapath(physicalTableMetadataPath, logicalTableMetadataPath,
                                                   physicalTableName, logicalTableName)

    def addPhysicalTableByMetadatapath(self, physicalTableMetadataPath, logicalTableMetadataPath, physicalTableName,
                                       logicalTableName):

        gover_control = GovernanceController.getInstance()
        cookies = gover_control.getCookie()
        headers = gover_control.getHeaders()

        physicalTableMetadataPath = physicalTableMetadataPath + ">/:" + physicalTableName + ":"
        logicalTableMetadataPath = logicalTableMetadataPath + ">/:" + logicalTableName + ":"

        data = '{"dataAssets":[{"id":"-1","name":"' + logicalTableName + '","type":"XD","subtype":"RESOURCE","metadataPath":"' + logicalTableMetadataPath + '","active":true,"vendor":"crossdata","properties":{"crossDataResource":{"type":"TABLE","resourceType":"SQL","schema":"SQL","partition":"na","link":{"link":["' + physicalTableMetadataPath + '"]}}}}]}'

        data = json.loads(data)

        response = requests.post(
            gover_control.getApiUrl() + '/dictionary/user/catalog/v1/dataAsset/federatedResourceBulk', headers=headers,
            cookies=cookies, json=data, verify=False)

        return response

    # Delete physical tables
    def deletePhysicalTable(self, domainName, tableName, schema=""):

        logicalTableName = schema + "_" + tableName if schema != "" else tableName
        logicalTableMetadataPath = domainName + '://' + domainName

        return self.deletePhysicalTableByMetadataPath(logicalTableMetadataPath, logicalTableName)

    def deletePhysicalTableByMetadataPath(self, logicalMetadataPath, logicalTableName):

        gover_control = GovernanceController.getInstance()
        cookies = gover_control.getCookie()
        headers = gover_control.getHeaders()

        logicalMetadataPath = logicalMetadataPath + ">/:" + logicalTableName + ":"

        params = (
            ('metadataPathLikeIn', logicalMetadataPath + "%"),
            ('featuresToUpdate', '-1'),
        )

        response = requests.delete(
            gover_control.getApiUrl() + '/dictionary/user/catalog/v1/dataAsset/federatedResourceBulk', headers=headers,
            params=params, cookies=cookies, verify=False)

        return response

    # Add concepts
    def addConcept(self, domain, ontology, taxonomy, concept):
        gover_control = GovernanceController.getInstance()
        cookies = gover_control.getCookie()
        headers = gover_control.getHeaders()

        ontologyPath = "ontologies://" + ontology + "/" + taxonomy + ">/:" + concept + ":"

        data = '[{"id":-1,"ontologyPath":"' + ontologyPath + '","name":"' + concept + '","metadataPath":"' + domain + ':","informational":false,"master":false,"operational":false,"properties":{},"readOnly":false,"version":"1.0","schemaPropertiesId":1}]'

        data = json.loads(data)

        response = requests.post(
            gover_control.getApiUrl() + '/dictionary/user/businessLayer/v1/businessLayerConcept/bulk', headers=headers,
            cookies=cookies, json=data, verify=False)

        return response

    # Delete concept
    def deleteConcept(self, domain, concept):
        gover_control = GovernanceController.getInstance()
        cookies = gover_control.getCookie()
        headers = gover_control.getHeaders()

        domainConcept = self.getConceptDetails(domain, concept)

        if domainConcept is not None:
            domainConceptId = domainConcept["id"]

            response = requests.delete(
                gover_control.getApiUrl() + '/dictionary/user/businessLayer/v1/businessLayerConcept/' + str(
                    domainConceptId), headers=headers, cookies=cookies, verify=False)

            return response

    def getDomainConcepts(self, domain, conceptFilter="%"):
        gover_control = GovernanceController.getInstance()
        cookies = gover_control.getCookie()
        headers = gover_control.getHeaders()

        semanticDomainName = "semantic_" + domain
        conceptMetadataPath = semanticDomainName + "://" + semanticDomainName + ">/:" + conceptFilter + ":"

        params = (
            ('size', '10'),
            ('page', '0'),
            ('metadataPathLike', conceptMetadataPath),
            ('sort', 'name,asc'),
            ('subtypeIn', 'PATH,RESOURCE'),
        )

        response = requests.get(
            gover_control.getApiUrl() + '/dictionary/user/catalog/v1/dataAsset/searchByMetadataPathLikeAndSubtypeIn',
            headers=headers, params=params, cookies=cookies, verify=False)

        if response.status_code in [200, 201]:
            return response.json()["dataAssets"]["dataAssets"]

        return []

    def getDomainConcept(self, domain, concept):
        concepts = self.getDomainConcepts(domain, concept)

        if len(concepts) == 0:
            return None
        else:
            return concepts[0]

    def getConceptDetails(self, domain, concept):
        gover_control = GovernanceController.getInstance()
        cookies = gover_control.getCookie()
        headers = gover_control.getHeaders()

        conceptObject = self.getDomainConcept(domain, concept)

        if conceptObject is None:
            return None
        else:
            conceptLink = conceptObject["properties"]["semanticResource"]["cLink"]
            params = (
                ('metadataPathLike', domain + ':'),
                ('ontologyPathLike', conceptLink),
            )

            response = requests.get(
                gover_control.getApiUrl() + '/dictionary/user/businessLayer/v1/businessLayerConcept/searchByMetadataPathLikeAndOntologyPathLike',
                headers=headers, params=params, cookies=cookies, verify=False)

            return response.json()["content"][0]

    # Get Federated Details of physical tables, CONNECTIONS!
    def getFederatedDetails(self, domainName, metadataPath):
        gover_control = GovernanceController.getInstance()
        cookies = gover_control.getCookie()
        headers = gover_control.getHeaders()

        params = (
            ('metadataPathIn', metadataPath),
            ('subtypeIn', 'RESOURCE'),
            ('collection', domainName + ':'),
            ('incomingFks', 'true'),
        )

        response = requests.get(
            gover_control.getApiUrl() + '/dictionary/user/catalog/v1/dataAsset/searchFederatedByMetadataPathInAndSubtypeIn',
            headers=headers, params=params, cookies=cookies, verify=False)

        return response.json()

    # Get Domain Tables
    def getDomainResourcesAssets(self, domainName):
        gover_control = GovernanceController.getInstance()
        cookies = gover_control.getCookie()
        headers = gover_control.getHeaders()

        params = (
            ('size', '10'),
            ('page', '0'),
            ('metadataPathLike', domainName + ':%'),
            ('sort', 'name,asc'),
            ('subtypeIn', 'RESOURCE'),
        )

        response = requests.get(
            gover_control.getApiUrl() + '/dictionary/user/catalog/v1/dataAsset/searchByMetadataPathLikeAndSubtypeIn',
            headers=headers, params=params, cookies=cookies, verify=False)

        return response.json()["dataAssets"]["dataAssets"]

    # Get Table details by Table Source
    def getDomainResourceAsset(self, domainName, dataStore, database, tableName, schema=""):
        gover_control = GovernanceController.getInstance()
        cookies = gover_control.getCookie()
        headers = gover_control.getHeaders()

        metadataPath = gover_control.getPhysicalTableMetadataPath(dataStore, database, tableName, schema)

        dataAssets = self.getDomainResourcesAssets(domainName)

        for asset in dataAssets:
            if asset["properties"]["crossDataResource"]["link"]["links"][0]["metadataPath"] == metadataPath:
                return asset

        return None
