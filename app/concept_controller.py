import requests
import json
from domain_controller import DomainController
from governance_controller import GovernanceController


class ConceptController:

    __instance = None

    @staticmethod
    def getInstance():
        if ConceptController.__instance is None:
            ConceptController()
        return ConceptController.__instance

    def __init__(self):
        if ConceptController.__instance is not None:
            pass
        else:
            ConceptController.__instance = self

    def setMode(self, domainName, concept, use="Master", mode="Informational"):
        domainController = DomainController.getInstance()
        gover_control = GovernanceController.getInstance()
        cookies = gover_control.getCookie()
        headers = gover_control.getHeaders()

        concept = domainController.getConceptDetails(domainName, concept)
        concept = concept["businessLayerViewCollection"][0]
        concept.pop("businessLayerTableCollection", None)

        if use == "Master":
            concept["master"] = True
            concept["readOnly"] = False
        else:
            concept["master"] = False
            concept["readOnly"] = True

        if mode == "Informational":
            concept["informational"] = True
            concept["operational"] = False
        else:
            concept["informational"] = False
            concept["operational"] = True

        response = requests.put(
            gover_control.getApiUrl() + '/dictionary/user/businessLayer/v1/businessLayerView/' + str(concept["id"]),
            headers=headers, cookies=cookies, json=concept, verify=False)

        return response

    def setStatus(self, domainName, concept, status):
        # Draft, In Progress, Need Modifications, Under Review, Pending Publish, Pending Unpublish
        domainController = DomainController.getInstance()
        gover_control = GovernanceController.getInstance()
        cookies = gover_control.getCookie()
        headers = gover_control.getHeaders()

        concept = domainController.getConceptDetails(domainName, concept)

        data = '{"commit":"ok","entityId":494,"entityTypeId":6,"id":-1,"status":"Under Review","type":"CHANGE_STATUS"}'

        data = json.loads(data)

        data["entityId"] = concept["id"]
        data["entityTypeId"] = concept["statusId"]["flow"]["entityTypeId"]
        data["status"] = status

        response = requests.post(gover_control.getApiUrl() + '/dictionary/user/bpm/v1/statusHistoric', headers=headers,
                                 cookies=cookies, json=data, verify=False)

        return response

    def addTable(self, domainName, concept, dataSource, database, tableName, schema=""):
        domainController = DomainController.getInstance()
        gover_control = GovernanceController.getInstance()
        cookies = gover_control.getCookie()
        headers = gover_control.getHeaders()

        conceptId = domainController.getConceptDetails(domainName, concept)["businessLayerViewCollection"][0]["id"]
        physicalTableDataAsset = domainController.getDomainResourceAsset(domainName, dataSource, database, tableName,
                                                                         schema)

        data = '[{"id":-1,"metadataPath":"' + physicalTableDataAsset[
            "metadataPath"] + '","schemaPropertiesId":3,"businessLayerViewId":' + str(
            conceptId) + ',"dataAssetId":' + str(physicalTableDataAsset[
                                                     "id"]) + ',"properties":{"attributes":{"informational":false,"operational":false,"master":false,"readOnly":false,"replica":false,"journal":false,"refusalsJournal":false}}}]'

        data = json.loads(data)

        response = requests.post(
            gover_control.getApiUrl() + '/dictionary/user/businessLayer/v1/businessLayerTable/bulk', headers=headers,
            cookies=cookies, json=data, verify=False)

        return response

    def deleteTable(self):
        # Delete table from a concept
        pass

    def connectTables(self, domainName, concept, srcDataSource, srcDatabase, srcSchema, srcTableName, srcPkColumn,
                     dstDataSource, dstDatabase, dstSchema, dstTableName, dstFkColumn, fkName):
        # print(domainName, concept, srcDataSource, srcDatabase, srcSchema, srcTableName, srcPkColumn,
        #              dstDataSource, dstDatabase, dstSchema, dstTableName, dstFkColumn, fkName)

        domainController = DomainController.getInstance()
        gover_control = GovernanceController.getInstance()
        cookies = gover_control.getCookie()
        headers = gover_control.getHeaders()

        srcTableDataAssetMetadataPath = gover_control.getPhysicalTableDataAssetMetadataPath(domainName, srcDataSource,
                                                                                            srcDatabase, srcTableName,
                                                                                            srcSchema)
        dstTableDataAssetMetadataPath = gover_control.getPhysicalTableDataAssetMetadataPath(domainName, dstDataSource,
                                                                                            dstDatabase, dstTableName,
                                                                                            dstSchema)

        conceptDetails = domainController.getConceptDetails(domainName, concept)
        conceptFederatedDetails = domainController.getFederatedDetails(domainName, srcTableDataAssetMetadataPath)

        data = '{"incomming":[],"outcomming":[{"fkIdentifier":"testfk","father":"1326","destinationTable":{"id":1327}}]}'
        data = json.loads(data)

        tableList = conceptDetails["businessLayerViewCollection"][0]["businessLayerTableCollection"]

        for table in tableList:
            if table["metadataPath"] == srcTableDataAssetMetadataPath:
                data["outcomming"][0]["father"] = table["id"]

            if table["metadataPath"] == dstTableDataAssetMetadataPath:
                data["outcomming"][0]["destinationTable"]["id"] = table["id"]

        # print(conceptFederatedDetails)
        try:
            fkList = conceptFederatedDetails["dataAssets"]["dataAssets"][0]["enrichedProperties"]["federated"]["fk"]
            for fk in fkList:
                if fk["pkTable"] == dstTableDataAssetMetadataPath and fk["fkName"] == fkName:
                    data["outcomming"][0]["fkIdentifier"] = fk["fkName"]

            response = requests.put(
                gover_control.getApiUrl() + '/dictionary/user/businessLayer/v1/businessLayerTable/' + str(
                    data["outcomming"][0]["father"]) + '/foreignKeys', headers=headers, cookies=cookies, json=data,
                verify=False)
            return response
        except Exception as e:
            return None

    def setTableAsRoot(self, domainName, concept, dataSource, database, tableName, schema=""):
        domainController = DomainController.getInstance()
        gover_control = GovernanceController.getInstance()
        cookies = gover_control.getCookie()
        headers = gover_control.getHeaders()

        concept = domainController.getConceptDetails(domainName, concept)
        tableMetadataPath = gover_control.getPhysicalTableDataAssetMetadataPath(domainName, dataSource, database,
                                                                                tableName, schema)

        tableId = -1
        tableObject = None
        for tableCollection in concept["businessLayerViewCollection"][0]["businessLayerTableCollection"]:
            if tableCollection["metadataPath"] == tableMetadataPath:
                tableId = tableCollection["id"]
                tableObject = tableCollection
                break

        if tableObject is None:
            return None

        data = tableObject
        data.pop("properties", None)
        data["type"] = "ROOT"

        response = requests.put(
            gover_control.getApiUrl() + '/dictionary/user/businessLayer/v1/businessLayerTable/' + str(tableId),
            headers=headers, cookies=cookies, json=data, verify=False)

        return response

    def getTableAssetFields(self, domainName, dataSource, database, tableName, schema=""):
        gover_control = GovernanceController.getInstance()
        cookies = gover_control.getCookie()
        headers = gover_control.getHeaders()

        tableMetadataPath = gover_control.getPhysicalTableDataAssetMetadataPath(domainName, dataSource, database,
                                                                                tableName, schema)

        params = (
            ('size', '999'),
            ('page', '0'),
            ('metadataPathLike', tableMetadataPath + '%'),
            ('sort', 'name,asc'),
            ('subtypeIn', 'FIELD'),
        )

        response = requests.get(
            gover_control.getApiUrl() + '/dictionary/user/catalog/v1/dataAsset/searchByMetadataPathLikeAndSubtypeIn',
            headers=headers, params=params, cookies=cookies, verify=False)

        return response.json().get("dataAssets", {}).get("dataAssets", [])

    def getConceptAssetFields(self, domainName, concept):
        gover_control = GovernanceController.getInstance()
        domainController = DomainController.getInstance()

        cookies = gover_control.getCookie()
        headers = gover_control.getHeaders()

        conceptMetadataPath = gover_control.getSemanticConceptMetadataPath(domainName, concept)
        conceptDetails = domainController.getConceptDetails(domainName, concept)
        # params = (
        #    ('size', '999'),
        #    ('page', '0'),
        #    ('metadataPathLike', conceptMetadataPath + '%'),
        #    ('sort', 'name,asc'),
        #    ('subtypeIn', 'FIELD'),
        # )

        # response = requests.get(gover_control.getApiUrl() + '/dictionary/user/catalog/v1/dataAsset/searchByMetadataPathLikeAndSubtypeIn', headers=headers, params=params, cookies=cookies, verify=False)

        params = (
            ('ontologyPathLike', conceptDetails["ontologyPath"] + '%'),
            ('typeIn', 'PROPERTY'),
            ('size', '999'),
            ('sort', 'name'),
        )

        response = requests.get(
            gover_control.getApiUrl() + '/dictionary/user/businessLayer/v1/ontology/searchByOntologyPathLikeAndTypeIn',
            headers=headers, params=params, cookies=cookies, verify=False)

        return response.json()["content"]

    def publishDomainConcepts(self, domain):
        conceptController = ConceptController.getInstance()
        concepts = self.getDomainConcepts(domain)
        for concept in concepts:
            conceptController.setStatus(domain, concept["name"], "Pending Publish")
            pass
        pass

    def unpublishDomainConcepts(self, domain):
        conceptController = ConceptController.getInstance()
        concepts = self.getDomainConcepts(domain)
        for concept in concepts:
            conceptController.setStatus(domain, concept["name"], "Pending Unpublish")
            pass
        pass