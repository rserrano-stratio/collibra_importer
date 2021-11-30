import os
import copy
import calendar
import time
import json
import pandas as pd
from governance_controller import GovernanceController
from db_controller import DBController
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


class CollibraController:
    __instance = None
    __dbc = None
    tables = ["dataconcepts", "datadomains", "dataelements", "dataqualityrules", "dqrequirements",
"dqrequirementsfacts", "elementdictionary", "rulerequirementmapping", "conceptdictionary","mappingqrs"]

    @staticmethod
    def getInstance():
        if CollibraController.__instance is None:
            CollibraController()
        return CollibraController.__instance

    def __init__(self):
        if CollibraController.__instance is not None:
            pass
        else:
            CollibraController.__dbc = DBController.getInstance()
            CollibraController.__instance = self

    def getCompletenessQr(self, attributeName, metadataPath, status, threshold):

        data = {
            "id": -1,
            "parameters": {
                "filter": {
                    "order": 1,
                    "type": "and",
                    "cond": [
                        {
                            "order": 1,
                            "attribute": "identifier_holding",
                            "operation": "is not null",
                            "param": [],
                            "formatter": "",
                            "timeZoneId": ""
                        },
                        {
                            "order": 2,
                            "attribute": "identifier_holding",
                            "operation": "is not empty",
                            "param": [],
                            "formatter": "",
                            "timeZoneId": ""
                        }
                    ]
                },
                "table": {
                    "type": "ONTOLOGY"
                },
                "catalogAttributeType": "RESOURCE"
            },
            "metadataPath": "ontologies://hsbcbm_testManual/hsbc_business_model/account_data/financial_position_data>/:identifier_holding:",
            "catalogAttributeType": "RESOURCE",
            "name": "test_qr_1",
            "description": "",
            "active": True,
            "resultOperationType": "%",
            "resultOperation": ">=",
            "resultUnit": {
                "name": "",
                "value": "0.95"
            },
            "link": {
                "dashboards": []
            },
            "type": "SPARK",
            "resultExecute": {
                "type": "EXE_REA"
            },
            "audit": False,
            "resultAction": {
                "type": "ACT_PASS"
            }
        }

        data["parameters"]["filter"]["cond"][0]["attribute"] = attributeName
        data["parameters"]["filter"]["cond"][1]["attribute"] = attributeName
        data["metadataPath"] = metadataPath
        data["active"] = status
        data["resultUnit"]["value"] = threshold
        data["name"] = attributeName + "_" + "Completeness_Automated"

        return data

    def getValidity1Qr(self, attributeName, metadataPath, status, threshold):

        data = {
            "id": -1,
            "parameters": {
                "filter": {
                    "order": 1,
                    "type": "and",
                    "cond": [
                        {
                            "order": 1,
                            "param": [
                                {
                                    "name": "",
                                    "type": "ValueOperand",
                                    "value": "USD"
                                },
                                {
                                    "name": "",
                                    "type": "ValueOperand",
                                    "value": "EUR"
                                }
                            ],
                            "attribute": "currency",
                            "formatter": "",
                            "operation": "in",
                            "timeZoneId": ""
                        }
                    ],
                },
                "table": {
                    "type": "ONTOLOGY"
                },
                "catalogAttributeType": "RESOURCE"
            },
            "metadataPath": "ontologies://hsbcbm_testManual/hsbc_business_model/account_data/financial_position_data>/:identifier_holding:",
            "catalogAttributeType": "RESOURCE",
            "name": "test_qr_1",
            "description": "",
            "active": True,
            "resultOperationType": "%",
            "resultOperation": ">=",
            "resultUnit": {
                "name": "",
                "value": "0.95"
            },
            "link": {
                "dashboards": []
            },
            "type": "SPARK",
            "resultExecute": {
                "type": "EXE_REA"
            },
            "audit": False,
            "resultAction": {
                "type": "ACT_PASS"
            }
        }

        data["parameters"]["filter"]["cond"][0]["attribute"] = attributeName
        data["metadataPath"] = metadataPath
        data["active"] = status
        data["resultUnit"]["value"] = threshold
        data["name"] = attributeName + "_" + "Validity001_Automated"

        return data

    def getValidity2Qr(self, attributeName, metadataPath, status, threshold):

        data = {
            "id": -1,
            "parameters": {
                "filter": {
                    "order": 1,
                    "type": "and",
                    "cond": [
                        {
                            "order": 1,
                            "param": [
                                {
                                    "name": "",
                                    "type": "ValueOperand",
                                    "value": "^.*{8,10}$"
                                }
                            ],
                            "attribute": "",
                            "formatter": "",
                            "operation": "regex",
                            "timeZoneId": ""
                        }
                    ],
                },
                "table": {
                    "type": "ONTOLOGY"
                },
                "catalogAttributeType": "RESOURCE"
            },
            "metadataPath": "ontologies://hsbcbm_testManual/hsbc_business_model/account_data/financial_position_data>/:identifier_holding:",
            "catalogAttributeType": "RESOURCE",
            "name": "test_qr_1",
            "description": "",
            "active": True,
            "resultOperationType": "%",
            "resultOperation": ">=",
            "resultUnit": {
                "name": "",
                "value": "0.95"
            },
            "link": {
                "dashboards": []
            },
            "type": "SPARK",
            "resultExecute": {
                "type": "EXE_REA"
            },
            "audit": False,
            "resultAction": {
                "type": "ACT_PASS"
            }
        }

        data["parameters"]["filter"]["cond"][0]["attribute"] = attributeName
        data["metadataPath"] = metadataPath
        data["active"] = status
        data["resultUnit"]["value"] = threshold
        data["name"] = attributeName + "_" + "Validity002_Automated"

        return data

    def getValidity3Qr(self, attributeName, metadataPath, status, threshold):

        data = {
            "id": -1,
            "parameters": {
                "filter": {
                    "order": 1,
                    "type": "and",
                    "cond": [
                        {
                            "order": 1,
                            "param": [
                                {
                                    "name": "",
                                    "type": "ValueOperand",
                                    "value": "^[^\\\\!|\\\\?|\\\\@|\\\\[|\\\\]|\\\\Ç|\\\\%|\\\\#|\\\\“|\\\\±|\\\\+|\\\\*|\\\\ç|\\\\&|\\\\/|\\\\||\\\\(|\\\\)|\\\\{|\\\\}|\\\\≠|\\\\=|\\\\¿|\\\\,|\\\\-|\\\\_|\\\\$|\\\\£|\\\\¨|\\\\è|\\\\é|\\\\à|\\\\.|\\\\:|\\\\;|\\\\<|\\\\>]*$"
                                }
                            ],
                            "attribute": "",
                            "formatter": "",
                            "operation": "regex",
                            "timeZoneId": ""
                        }
                    ],
                },
                "table": {
                    "type": "ONTOLOGY"
                },
                "catalogAttributeType": "RESOURCE"
            },
            "metadataPath": "ontologies://hsbcbm_testManual/hsbc_business_model/account_data/financial_position_data>/:identifier_holding:",
            "catalogAttributeType": "RESOURCE",
            "name": "test_qr_1",
            "description": "",
            "active": True,
            "resultOperationType": "%",
            "resultOperation": ">=",
            "resultUnit": {
                "name": "",
                "value": "0.95"
            },
            "link": {
                "dashboards": []
            },
            "type": "SPARK",
            "resultExecute": {
                "type": "EXE_REA"
            },
            "audit": False,
            "resultAction": {
                "type": "ACT_PASS"
            }
        }

        data["parameters"]["filter"]["cond"][0]["attribute"] = attributeName
        data["metadataPath"] = metadataPath
        data["active"] = status
        data["resultUnit"]["value"] = threshold
        data["name"] = attributeName + "_" + "Validity003_Automated"

        return data

    def getConformity1Qr(self, attributeName, metadataPath, status, threshold):

        data = {
            "id": -1,
            "parameters": {
                "filter": {
                    "order": 1,
                    "type": "and",
                    "cond": [
                        {
                            "order": 1,
                            "param": [],
                            "attribute": "",
                            "formatter": "",
                            "operation": "is date",
                            "timeZoneId": ""
                        }
                    ],
                },
                "table": {
                    "type": "ONTOLOGY"
                },
                "catalogAttributeType": "RESOURCE"
            },
            "metadataPath": "ontologies://hsbcbm_testManual/hsbc_business_model/account_data/financial_position_data>/:identifier_holding:",
            "catalogAttributeType": "RESOURCE",
            "name": "test_qr_1",
            "description": "",
            "active": True,
            "resultOperationType": "%",
            "resultOperation": ">=",
            "resultUnit": {
                "name": "",
                "value": "0.95"
            },
            "link": {
                "dashboards": []
            },
            "type": "SPARK",
            "resultExecute": {
                "type": "EXE_REA"
            },
            "audit": False,
            "resultAction": {
                "type": "ACT_PASS"
            }
        }

        data["parameters"]["filter"]["cond"][0]["attribute"] = attributeName
        data["metadataPath"] = metadataPath
        data["active"] = status
        data["resultUnit"]["value"] = threshold
        data["name"] = attributeName + "_" + "Conformity001_Automated"

        return data

    def getConformity2Qr(self, attributeName, metadataPath, status, threshold):

        data = {
            "id": -1,
            "parameters": {
                "filter": {
                    "order": 1,
                    "type": "and",
                    "cond": [
                        {
                            "order": 1,
                            "param": [
                                {
                                    "name": "",
                                    "type": "ValueOperand",
                                    "value": "^[0-9]+$"
                                }
                            ],
                            "attribute": "",
                            "formatter": "",
                            "operation": "regex",
                            "timeZoneId": ""
                        }
                    ],
                },
                "table": {
                    "type": "ONTOLOGY"
                },
                "catalogAttributeType": "RESOURCE"
            },
            "metadataPath": "ontologies://hsbcbm_testManual/hsbc_business_model/account_data/financial_position_data>/:identifier_holding:",
            "catalogAttributeType": "RESOURCE",
            "name": "test_qr_1",
            "description": "",
            "active": True,
            "resultOperationType": "%",
            "resultOperation": ">=",
            "resultUnit": {
                "name": "",
                "value": "0.95"
            },
            "link": {
                "dashboards": []
            },
            "type": "SPARK",
            "resultExecute": {
                "type": "EXE_REA"
            },
            "audit": False,
            "resultAction": {
                "type": "ACT_PASS"
            }
        }

        data["parameters"]["filter"]["cond"][0]["attribute"] = attributeName
        data["metadataPath"] = metadataPath
        data["active"] = status
        data["resultUnit"]["value"] = threshold
        data["name"] = attributeName + "_" + "Conformity002_Automated"

        return data

    def getConformity3Qr(self, attributeName, metadataPath, status, threshold):

        data = {
            "id": -1,
            "parameters": {
                "filter": {
                    "order": 1,
                    "type": "and",
                    "cond": [
                        {
                            "order": 1,
                            "param": [
                                {
                                    "name": "",
                                    "type": "ValueOperand",
                                    "value": "^.*{8,10}$"
                                }
                            ],
                            "attribute": "",
                            "formatter": "",
                            "operation": "regex",
                            "timeZoneId": ""
                        }
                    ],
                },
                "table": {
                    "type": "ONTOLOGY"
                },
                "catalogAttributeType": "RESOURCE"
            },
            "metadataPath": "ontologies://hsbcbm_testManual/hsbc_business_model/account_data/financial_position_data>/:identifier_holding:",
            "catalogAttributeType": "RESOURCE",
            "name": "test_qr_1",
            "description": "",
            "active": True,
            "resultOperationType": "%",
            "resultOperation": ">=",
            "resultUnit": {
                "name": "",
                "value": "0.95"
            },
            "link": {
                "dashboards": []
            },
            "type": "SPARK",
            "resultExecute": {
                "type": "EXE_REA"
            },
            "audit": False,
            "resultAction": {
                "type": "ACT_PASS"
            }
        }

        data["parameters"]["filter"]["cond"][0]["attribute"] = attributeName
        data["metadataPath"] = metadataPath
        data["active"] = status
        data["resultUnit"]["value"] = threshold
        data["name"] = attributeName + "_" + "Conformity003_Automated"

        return data

    @staticmethod
    def generateTaxonomy_old(row):
        path = []

        l1 = CollibraController.normalizeName(row["CDM L1"])
        l2 = CollibraController.normalizeName(row["CDM L2"])
        l3 = CollibraController.normalizeName(row["CDM L3"])

        if l1 != "" and l2 != "":
            path.append(l1)

        if l3 != "":
            path.append(l2)

        path = ["t_" + str(x) for x in path]

        return "/".join(path)

    @staticmethod
    def generateTaxonomy(row):
        path = []

        l0 = CollibraController.normalizeName(row["dd_l0"])
        l1 = CollibraController.normalizeName(row["dd_l1"])
        l2 = CollibraController.normalizeName(row["dd_l2"])
        l3 = CollibraController.normalizeName(row["dd_l3"])

        if l0 != "" and l1 != "":
            path.append(l0)

        if l1 != "" and l2 != "":
            path.append(l1)

        if l3 != "":
            path.append(l2)

        path = ["t_" + str(x) for x in path]

        return "/".join(path)

    @staticmethod
    def generateEntity_old(row):
        l1 = CollibraController.normalizeName(row["CDM L1"])
        l2 = CollibraController.normalizeName(row["CDM L2"])
        l3 = CollibraController.normalizeName(row["CDM L3"])

        entity = l1
        if l2 != "":
            entity = l2
        if l3 != "":
            entity = l3
        return entity

    @staticmethod
    def generateEntity(row):
        l0 = CollibraController.normalizeName(row["dd_l0"])
        l1 = CollibraController.normalizeName(row["dd_l1"])
        l2 = CollibraController.normalizeName(row["dd_l2"])
        l3 = CollibraController.normalizeName(row["dd_l3"])

        entity = l0
        if l1 != "":
            entity = l1
        if l2 != "":
            entity = l2
        if l3 != "":
            entity = l3
        return entity

    @staticmethod
    def generateProperty(row):
        return CollibraController.normalizeName(row["de_name"])  # HSBC Data Model Data Element

    @staticmethod
    def normalizeName(name):
        if name is None:
            return ""

        if str(name).lower() in ["nan", "none"]:
            return ""

        if name[0] == "_":
            name = name[1:]

        name = name.strip()

        return str(name).replace(" ", "_").replace("\n", "").replace("[", "").replace("]", "").replace("\r",
                                                                                                       "").lower().replace(
            "peoduct", "product").replace("&", "and").replace("(", "").replace(")", "").replace("{", "").replace("}",
                                                                                                                 "").replace(
            "/", "or").replace(",", "").replace("-", "").replace(".", "")

    @staticmethod
    def getClassFullPath_old(row):
        classPath = row["entity"]

        if row["taxonomy_path"] != "":
            classPath = row["taxonomy_path"] + "/" + classPath

        return classPath

    @staticmethod
    def getClassFullPath(row, ontologyName="collibra_hsbc_bm", ontologyBaseTaxonomy="new_hsbc_business_model"):
        if row["taxonomy_path"] != "":
            classPath = 'ontologies://{}/{}'.format(ontologyName, ontologyBaseTaxonomy) + "/" + row[
                "taxonomy_path"] + ">/:" + row["entity"] + ":"
        else:
            classPath = 'ontologies://{}/{}'.format(ontologyName, ontologyBaseTaxonomy) + ">/:" + row["entity"] + ":"

        return classPath

    @staticmethod
    def getElementFullPath(row, ontologyName="collibra_hsbc_bm", ontologyBaseTaxonomy="new_hsbc_business_model"):
        if row["taxonomy_path"] != "":
            classPath = 'ontologies://{}/{}'.format(ontologyName, ontologyBaseTaxonomy) + "/" + row[
                "taxonomy_path"] + "/" + row["entity"] + ">/:" + row["property"] + ":"
        else:
            classPath = 'ontologies://{}/{}'.format(ontologyName, ontologyBaseTaxonomy) + "/" + row["entity"] + ">/:" + \
                        row["property"] + ":"
        return classPath

    def processCollibraData(self, ontologyName, ontologyBaseTaxonomy, filter='%'):
        governanceController = GovernanceController.getInstance()

        qrQuery = '''select dd.l0_domain_name as dd_l0, dd.l1_domain_name as dd_l1, dd.l2_domain_name as dd_l2, dd.l3_domain_name as dd_l3, 
                    dec.name as de_name, dc.requirement_description as dc_requirement_description, 
                    req.dq_dimension_id as req_dimension_id, dc.data_quality_requirement_target as dqr_target, mqc.stratio_qr_dimension as qr_generic_name
                    from dataelements dec
                    join dqrequirementsfacts req
                    on dec.collibra_resource_id = req.hbim_data_element_id
                    join dataconcepts dcc 
                    on req.applies_to_concept_resource_id = dcc.collibra_resource_id
                    join conceptdictionary cdc 
                    on cdc.concept_id = dcc.collibra_resource_id 
                    join datadomains dd
                    on dd.collibra_resource_id = cdc.domain_id 
                    join dqrequirements dc 
                    on req.data_quality_requirement_id = dc.collibra_resource_id
                    join mappingqrs mqc
                    on dc.requirement_description = mqc.requirement_description
                    where dc.published_indicator = 1
                '''
        engine = self.__dbc.engine
        qr_df = pd.read_sql_query(qrQuery, engine)

        # qr_df = pd.read_csv("data/collibraQR.csv")
        #qr_df = pd.read_csv("data/collibra_data.csv")
        # qr_df = qr_df[qr_df["dq_name"].str.contains("CMP001")]

        #qr_df["de_name"] = qr_df["de_name"].map(lambda x: governanceController.normalizeName(x))
        #qr_df["dd_name"] = qr_df["dd_name"].map(lambda x: governanceController.normalizeName(x))
        #qr_df["dd_l0"] = qr_df["dd_l0"].map(lambda x: governanceController.normalizeName(x))
        #qr_df["dd_l1"] = qr_df["dd_l1"].map(lambda x: governanceController.normalizeName(x))
        #qr_df["dd_l2"] = qr_df["dd_l2"].map(lambda x: governanceController.normalizeName(x))
        #qr_df["dd_l3"] = qr_df["dd_l3"].map(lambda x: governanceController.normalizeName(x))
        #qr_df["dd_name"] = qr_df["dd_name"].map(lambda x: str(governanceController.normalizeName(x))).map(
            #lambda x: x if x != "nan" else "hsbc_entity")
        #qr_df["dd_l0"] = qr_df["dd_l0"].map(lambda x: str(x)).map(lambda x: x if x != "nan" else "hsbc_entity")

        # qr_created = 0
        # succesfulQr = []
        # failedQr = []

        comp_qr_created = 0
        val_qr_created = 0
        conf_qr_created = 0
        succesfulQr = []
        failedQr = []
        
        for element in qr_df.iterrows():
            # build metadataPath
            # get the actualDataAsset (confirme the actual metadatapath exist)
            # assign the Qr
            nameLike = '%' + element[1]["qr_generic_name"] + '%'
            print(nameLike)
            res = governanceController.getQRByName(name=nameLike, size=1)
            break

            
            '''
            if "CMP001" in str(element[1]["dq_name"]):

                l0 = str(element[1]["dd_l0"]).lower().replace(" ", "_")
                l1 = str(element[1]["dd_l1"]).lower().replace(" ", "_")
                l2 = str(element[1]["dd_l2"]).lower().replace(" ", "_")
                l3 = str(element[1]["dd_l3"]).lower().replace(" ", "_")
                columnName = str(element[1]["de_name"]).lower().replace(" ", "_").replace("[", "").replace("]", "")

                metadataPath = 'ontologies://{}/{}'.format(ontologyName, ontologyBaseTaxonomy) + "/" + l0

                if l1 is not None and l1 not in ["", "nan"]:
                    metadataPath = metadataPath + "/" + l1

                if l2 is not None and l2 not in ["", "nan"]:
                    metadataPath = metadataPath + "/" + l2

                if l3 is not None and l3 not in ["", "nan"]:
                    metadataPath = metadataPath + "/" + l3

                metadataPath = metadataPath + ">/:{}:".format(columnName)
                res = governanceController.searchDataAssetByMetadataPath(metadataPath)

                if res.get("totalElements", 0) != 0:

                    column_qr = self.getCompletenessQr(columnName, metadataPath,
                                                       str(element[1]["dqr_published_indicator"]).lower() == "true",
                                                       str(element[1]["dq_target"]))
                    qr_result = governanceController.addQualityRule(column_qr)

                    if qr_result is not None and qr_result.ok:
                        succesfulQr.append(metadataPath)
                        comp_qr_created += 1
                    else:
                        failedQr.append(metadataPath)

            if "VLD001" in str(element[1]["dq_name"]):

                l0 = str(element[1]["dd_l0"]).lower().replace(" ", "_")
                l1 = str(element[1]["dd_l1"]).lower().replace(" ", "_")
                l2 = str(element[1]["dd_l2"]).lower().replace(" ", "_")
                l3 = str(element[1]["dd_l3"]).lower().replace(" ", "_")
                columnName = str(element[1]["de_name"]).lower().replace(" ", "_").replace("[", "").replace("]", "")

                metadataPath = 'ontologies://{}/{}'.format(ontologyName, ontologyBaseTaxonomy) + "/" + l0

                if l1 is not None and l1 not in ["", "nan"]:
                    metadataPath = metadataPath + "/" + l1

                if l2 is not None and l2 not in ["", "nan"]:
                    metadataPath = metadataPath + "/" + l2

                if l3 is not None and l3 not in ["", "nan"]:
                    metadataPath = metadataPath + "/" + l3

                metadataPath = metadataPath + ">/:{}:".format(columnName)
                res = governanceController.searchDataAssetByMetadataPath(metadataPath)

                if res.get("totalElements", 0) != 0:

                    column_qr = self.getValidity1Qr(columnName, metadataPath,
                                                    str(element[1]["dqr_published_indicator"]).lower() == "true",
                                                    str(element[1]["dq_target"]))
                    qr_result = governanceController.addQualityRule(column_qr)

                    if qr_result is not None and qr_result.ok:
                        succesfulQr.append(metadataPath)
                        val_qr_created += 1
                    else:
                        failedQr.append(metadataPath)

            if "VLD002" in str(element[1]["dq_name"]):

                l0 = str(element[1]["dd_l0"]).lower().replace(" ", "_")
                l1 = str(element[1]["dd_l1"]).lower().replace(" ", "_")
                l2 = str(element[1]["dd_l2"]).lower().replace(" ", "_")
                l3 = str(element[1]["dd_l3"]).lower().replace(" ", "_")
                columnName = str(element[1]["de_name"]).lower().replace(" ", "_").replace("[", "").replace("]", "")

                metadataPath = 'ontologies://{}/{}'.format(ontologyName, ontologyBaseTaxonomy) + "/" + l0

                if l1 is not None and l1 not in ["", "nan"]:
                    metadataPath = metadataPath + "/" + l1

                if l2 is not None and l2 not in ["", "nan"]:
                    metadataPath = metadataPath + "/" + l2

                if l3 is not None and l3 not in ["", "nan"]:
                    metadataPath = metadataPath + "/" + l3

                metadataPath = metadataPath + ">/:{}:".format(columnName)
                res = governanceController.searchDataAssetByMetadataPath(metadataPath)

                if res.get("totalElements", 0) != 0:

                    column_qr = self.getValidity2Qr(columnName, metadataPath,
                                                    str(element[1]["dqr_published_indicator"]).lower() == "true",
                                                    str(element[1]["dq_target"]))
                    qr_result = governanceController.addQualityRule(column_qr)

                    if qr_result is not None and qr_result.ok:
                        succesfulQr.append(metadataPath)
                        val_qr_created += 1
                    else:
                        failedQr.append(metadataPath)

            if "VLD003" in str(element[1]["dq_name"]) or "VLD004" in str(element[1]["dq_name"]):

                l0 = str(element[1]["dd_l0"]).lower().replace(" ", "_")
                l1 = str(element[1]["dd_l1"]).lower().replace(" ", "_")
                l2 = str(element[1]["dd_l2"]).lower().replace(" ", "_")
                l3 = str(element[1]["dd_l3"]).lower().replace(" ", "_")
                columnName = str(element[1]["de_name"]).lower().replace(" ", "_").replace("[", "").replace("]", "")

                metadataPath = 'ontologies://{}/{}'.format(ontologyName, ontologyBaseTaxonomy) + "/" + l0

                if l1 is not None and l1 not in ["", "nan"]:
                    metadataPath = metadataPath + "/" + l1

                if l2 is not None and l2 not in ["", "nan"]:
                    metadataPath = metadataPath + "/" + l2

                if l3 is not None and l3 not in ["", "nan"]:
                    metadataPath = metadataPath + "/" + l3

                metadataPath = metadataPath + ">/:{}:".format(columnName)
                res = governanceController.searchDataAssetByMetadataPath(metadataPath)

                if res.get("totalElements", 0) != 0:

                    column_qr = self.getValidity3Qr(columnName, metadataPath,
                                                    str(element[1]["dqr_published_indicator"]).lower() == "true",
                                                    str(element[1]["dq_target"]))
                    qr_result = governanceController.addQualityRule(column_qr)

                    if qr_result is not None and qr_result.ok:
                        succesfulQr.append(metadataPath)
                        val_qr_created += 1
                    else:
                        failedQr.append(metadataPath)

            if "CFM001" in str(element[1]["dq_name"]):

                l0 = str(element[1]["dd_l0"]).lower().replace(" ", "_")
                l1 = str(element[1]["dd_l1"]).lower().replace(" ", "_")
                l2 = str(element[1]["dd_l2"]).lower().replace(" ", "_")
                l3 = str(element[1]["dd_l3"]).lower().replace(" ", "_")
                columnName = str(element[1]["de_name"]).lower().replace(" ", "_").replace("[", "").replace("]", "")

                metadataPath = 'ontologies://{}/{}'.format(ontologyName, ontologyBaseTaxonomy) + "/" + l0

                if l1 is not None and l1 not in ["", "nan"]:
                    metadataPath = metadataPath + "/" + l1

                if l2 is not None and l2 not in ["", "nan"]:
                    metadataPath = metadataPath + "/" + l2

                if l3 is not None and l3 not in ["", "nan"]:
                    metadataPath = metadataPath + "/" + l3

                metadataPath = metadataPath + ">/:{}:".format(columnName)
                res = governanceController.searchDataAssetByMetadataPath(metadataPath)

                if res.get("totalElements", 0) != 0:

                    column_qr = self.getConformity1Qr(columnName, metadataPath,
                                                      str(element[1]["dqr_published_indicator"]).lower() == "true",
                                                      str(element[1]["dq_target"]))
                    qr_result = governanceController.addQualityRule(column_qr)

                    if qr_result is not None and qr_result.ok:
                        succesfulQr.append(metadataPath)
                        conf_qr_created += 1
                    else:
                        failedQr.append(metadataPath)

            if "CFM002" in str(element[1]["dq_name"]):

                l0 = str(element[1]["dd_l0"]).lower().replace(" ", "_")
                l1 = str(element[1]["dd_l1"]).lower().replace(" ", "_")
                l2 = str(element[1]["dd_l2"]).lower().replace(" ", "_")
                l3 = str(element[1]["dd_l3"]).lower().replace(" ", "_")
                columnName = str(element[1]["de_name"]).lower().replace(" ", "_").replace("[", "").replace("]", "")

                metadataPath = 'ontologies://{}/{}'.format(ontologyName, ontologyBaseTaxonomy) + "/" + l0

                if l1 is not None and l1 not in ["", "nan"]:
                    metadataPath = metadataPath + "/" + l1

                if l2 is not None and l2 not in ["", "nan"]:
                    metadataPath = metadataPath + "/" + l2

                if l3 is not None and l3 not in ["", "nan"]:
                    metadataPath = metadataPath + "/" + l3

                metadataPath = metadataPath + ">/:{}:".format(columnName)
                res = governanceController.searchDataAssetByMetadataPath(metadataPath)

                if res.get("totalElements", 0) != 0:

                    column_qr = self.getConformity2Qr(columnName, metadataPath,
                                                      str(element[1]["dqr_published_indicator"]).lower() == "true",
                                                      str(element[1]["dq_target"]))
                    qr_result = governanceController.addQualityRule(column_qr)

                    if qr_result is not None and qr_result.ok:
                        succesfulQr.append(metadataPath)
                        conf_qr_created += 1
                    else:
                        failedQr.append(metadataPath)

            if "CFM003" in str(element[1]["dq_name"]):

                l0 = str(element[1]["dd_l0"]).lower().replace(" ", "_")
                l1 = str(element[1]["dd_l1"]).lower().replace(" ", "_")
                l2 = str(element[1]["dd_l2"]).lower().replace(" ", "_")
                l3 = str(element[1]["dd_l3"]).lower().replace(" ", "_")
                columnName = str(element[1]["de_name"]).lower().replace(" ", "_").replace("[", "").replace("]", "")

                metadataPath = 'ontologies://{}/{}'.format(ontologyName, ontologyBaseTaxonomy) + "/" + l0

                if l1 is not None and l1 not in ["", "nan"]:
                    metadataPath = metadataPath + "/" + l1

                if l2 is not None and l2 not in ["", "nan"]:
                    metadataPath = metadataPath + "/" + l2

                if l3 is not None and l3 not in ["", "nan"]:
                    metadataPath = metadataPath + "/" + l3

                metadataPath = metadataPath + ">/:{}:".format(columnName)
                res = governanceController.searchDataAssetByMetadataPath(metadataPath)

                if res.get("totalElements", 0) != 0:

                    column_qr = self.getConformity3Qr(columnName, metadataPath,
                                                      str(element[1]["dqr_published_indicator"]).lower() == "true",
                                                      str(element[1]["dq_target"]))
                    qr_result = governanceController.addQualityRule(column_qr)

                    if qr_result is not None and qr_result.ok:
                        succesfulQr.append(metadataPath)
                        conf_qr_created += 1
                    else:
                        failedQr.append(metadataPath)
            '''
        print(res)
        qr_created = comp_qr_created + conf_qr_created + val_qr_created
        # print("Existing Data Elements: {}".format(qr_created))
        print("Sucessfully created: {} quality rules".format(len(succesfulQr)))
        print("Failed to create: {} quality rules".format(len(failedQr)))
        #qrs = governanceController.getQRByName(name="%_Automated%", size=10000)
        #print("Total Collibra QRs Created: {}".format(len(qrs)))
        #return succesfulQr, failedQr, len(qrs)
        return succesfulQr, failedQr, 0

    """
            for element in qr_df.iterrows():
            # build metadataPath
            # get the actualDataAsset (confirm the actual metadatapath exist)
            # assign the Qr

            l0 = str(element[1]["dd_l0"]).lower().replace(" ", "_")
            l1 = str(element[1]["dd_l1"]).lower().replace(" ", "_")
            l2 = str(element[1]["dd_l2"]).lower().replace(" ", "_")
            l3 = str(element[1]["dd_l3"]).lower().replace(" ", "_")
            # columnName = str(element[1]["de_name"]).lower().replace(" ", "_").replace("[", "").replace("]", "")

            row = {
                "CDM L1": l0,
                "CDM L2": l1,
                "CDM L3": l2,
                "HSBC Data Model Data Element": element[1]["de_name"]
            }
            taxonomy_path = CollibraController.generateTaxonomy(row)
            entity = CollibraController.generateEntity(row)
            property = CollibraController.generateProperty(row)
            class_full_path = CollibraController.getClassFullPath({"entity": entity, "taxonomy_path": taxonomy_path})
            dp_full_path = class_full_path + "/" + property

            metadataPath = 'ontologies://{}/{}'.format(ontologyName, ontologyBaseTaxonomy) + "/" + class_full_path

            metadataPath = metadataPath + ">/:{}:".format(property)
            # print(metadataPath)
            res = governanceController.searchDataAssetByMetadataPath(metadataPath)
            # print("Found" if res.get("totalElements", 0) != 0 else "Not found")
            # print("===========================================================")
            if res.get("totalElements", 0) != 0:
                print("Found: " + str(metadataPath))
                column_qr = self.getCompletenessQr(property, metadataPath,
                                                   str(element[1]["dqr_published_indicator"]).lower() == "true",
                                                   str(element[1]["dq_target"]))

                qr_result = None
                try:
                    qr_result = governanceController.addQualityRule(column_qr)
                except Exception as e:
                    print("Exception happened while creating the Quality Rule: " + str(e))

                if qr_result is not None and qr_result.ok:
                    succesfulQr.append(metadataPath)
                else:
                    failedQr.append(metadataPath)

                qr_created += 1
    """

    def truncateTables(self):
        for table in CollibraController.tables:
            try:
                self.__dbc.truncate_table(table)
            except Exception as e:
                print("Failed to truncate table: {}".format(table))
                pass

    def uploadCollibraFiles(self, directory="."):

        engine = self.__dbc.engine
        files = os.listdir(directory)
        csv_files = [file for file in files if '.csv' in file]
        csv_files_with_names = []
        for csv_file in csv_files:
            csv_files_with_names.append({"filename": csv_file,
                                         "new_name": csv_file.lower().replace(".csv", "").replace(" ", "_")})
        accepted_files = [file for file in csv_files_with_names if
                          any(table in file["new_name"] for table in CollibraController.tables)]
        for accepted_file in accepted_files:
            for table in CollibraController.tables:
                if table == accepted_file["new_name"]:
                    accepted_file["table"] = table
                    break

        postgreSQLConnection = engine.connect()

        for file in accepted_files:
            filepath = os.path.join(directory, file["filename"])
            postgreSQLTable = file["table"]
            df = pd.read_csv(filepath)
            org_columns = df.columns.to_list()
            new_columns = [column.lower().replace(" ", "_") for column in org_columns]
            n = len(org_columns)
            new_columns_dict = {}

            for i in range(n):
                new_columns_dict[org_columns[i]] = new_columns[i]
            df.rename(columns=new_columns_dict, inplace=True)

            try:
                frame = df.to_sql(postgreSQLTable, postgreSQLConnection, if_exists='append')
            except ValueError as vx:
                print(vx)
            except Exception as ex:
                print(ex)
            else:
                print("PostgreSQL Table %s has been created successfully." % postgreSQLTable)

        postgreSQLConnection.close()
        pass

    def add_collibra_resource_id(self, data):
        data_elements = data[["de_name", "de_id", "dd_name", "dd_id", "dd_l0", "dd_l1", "dd_l2", "dd_l3"]]
        data_elements.drop_duplicates()
        data_elements["taxonomy_path"] = ""
        data_elements["entity"] = ""
        data_elements["property"] = ""

        data_elements["taxonomy_path"] = data_elements.apply(CollibraController.generateTaxonomy, axis=1)
        data_elements["entity"] = data_elements.apply(CollibraController.generateEntity, axis=1)
        data_elements["property"] = data_elements.apply(CollibraController.generateProperty, axis=1)

        data_elements["class_full_path"] = data_elements.apply(CollibraController.getClassFullPath, axis=1)
        data_elements["dp_full_path"] = data_elements.apply(CollibraController.getElementFullPath, axis=1)

        de_final = data_elements[["dp_full_path", "de_id"]].drop_duplicates()
        dd_final = data_elements[["class_full_path", "dd_id"]].drop_duplicates()

        # Step X Data Property
        gov = GovernanceController.getInstance()
        headers = gov.getHeaders()
        cookies = gov.getCookie()

        try:
            attribute = gov.key_searchByKeyLike("%collibra_resource_id%")[0]
            collibra_resource_id_attr_id = attribute["id"]
        except Exception as e:
            collibra_resource_id_attr_id = gov.key_createKey("collibra_resource_id", "collibra_resource_id")
            pass
        for row in de_final.iterrows():
            data = '[{"id":-1,"key":{"id":40,"key":"collibra_resource_id"},"value":{"name":"","value":"' + row[1][
                "de_id"] + '"},"metadataPath":"' + row[1]["dp_full_path"] + '"}]'
            data = json.loads(data)

            data[0]["key"]["id"] = collibra_resource_id_attr_id

            response = requests.post(
                'https://admin.hsbc.stratio.com/service/dg-businessglossary-api/dictionary/user/catalog/v1/keyDataAsset',
                headers=headers, cookies=cookies, json=data, verify=False)
            print(response)

        # Step Y Class
        for row in dd_final.iterrows():
            data = '[{"id":-1,"key":{"id":40,"key":"collibra_resource_id"},"value":{"name":"","value":"' + row[1][
                "dd_id"] + '"},"metadataPath":"' + row[1]["class_full_path"] + '"}]'
            data = json.loads(data)

            response = requests.post(
                'https://admin.hsbc.stratio.com/service/dg-businessglossary-api/dictionary/user/catalog/v1/keyDataAsset',
                headers=headers, cookies=cookies, json=data, verify=False)
            print(response)


        pass

    def export_new_qrs_to_collibra(self, filter='ontologies:%'):
        def getDomainMetadataPath(de_metadataPath):
            s = de_metadataPath.split(">/")[0]
            first = s.rsplit("/")[0:-1]
            last = s.rsplit("/")[-1]
            dm_metadataPath = "/".join(first) + ">/:" + last + ":"

            return dm_metadataPath

        rule = pd.read_csv("data/rule.csv", delimiter=",")
        dq_requirements = pd.read_csv("data/dq_requirements.csv", delimiter=",")

        rule_dict = {}
        dq_requirements_dict = {}
        for column in rule.columns.to_list():
            rule_dict[column] = ""

        for column in dq_requirements.columns.to_list():
            dq_requirements_dict[column] = ""

        gov = GovernanceController.getInstance()
        qrs = gov.getQR(10000, metadataPath=filter)
        qrs = qrs['content']

        # initialize the two dataframe empty

        rule_dicts = []
        dq_requirements_dicts = []

        for qr in qrs:
            if qr['name'].split("_")[-1] == "Manual":
                # add condition name don't exist in the database (we don't count the updates)
                rule_dict = {}
                dq_requirements_dict = {}

                de_metadataPath = qr['metadataPath']
                dm_metadataPath = getDomainMetadataPath(de_metadataPath)

                de_attributes = gov.getAttributes(de_metadataPath)
                dm_attributes = gov.getAttributes(dm_metadataPath)

                de_collibra_id = ""
                dm_collibra_id = ""

                for de_attribute in de_attributes:
                    if de_attribute['key']['key'] == "collibra_resource_id":
                        de_collibra_id = de_attribute['value']['value']

                    if de_collibra_id != "":
                        for dm_attribute in dm_attributes:
                            if dm_attribute['key']['key'] == "collibra_resource_id":
                                dm_collibra_id = dm_attribute['value']['value']

                # print("Domain_collibra_id: {}, Data_element_collibra_id: {}".format(dm_collibra_id, de_collibra_id))

                dimension = qr["name"].split("_")[-2]

                s = de_metadataPath.split(">/")
                de_name = s[1].strip(":")
                dd_name = s[0].rsplit("/")[-1]

                # Direct Rule excel
                rule_dict["Full Name"] = qr["name"]
                rule_dict["Name"] = qr["name"]
                rule_dict["Asset Id"] = ""
                rule_dict["Community"] = "Inventories and Reference Models [Unpublished]"
                rule_dict["Domain Type"] = "Rulebook"
                rule_dict["Domain"] = "Group DQ Rules [Unpublished]"
                rule_dict["Status"] = "Draft"
                rule_dict["Asset Type"] = "Data Quality Rule"
                rule_dict["DQ Dimension"] = dimension
                rule_dict["Description (No Formatting)"] = qr['description']

                # Direct dq_requirements excel
                dq_requirements_dict["Name"] = ""  # We need a definition
                dq_requirements_dict["Full Name"] = ""  # We need a definition

                # white in the excel
                dq_requirements_dict["Asset Id"] = ""

                dq_requirements_dict["Community"] = "Data Governance and Standards [Published]"
                dq_requirements_dict["Domain Type"] = "Rulebook"
                dq_requirements_dict["Asset Type"] = "Data Quality Requirement"
                dq_requirements_dict["Domain"] = "Group DQ Requirements [Published]"
                dq_requirements_dict[
                    "Requirement Measures Element [HSBC Canonical Data Element] > Domain Id"] = dm_collibra_id
                dq_requirements_dict[
                    "Requirement Measures Element [HSBC Canonical Data Element] > Asset Id"] = de_collibra_id

                # Rule elements linked to dq_requirements elements
                rule_dict["Implements Requirement [Data Quality Requirement] > Name"] = dq_requirements_dict["Name"]
                rule_dict["Implements Requirement [Data Quality Requirement] > Full Name"] = dq_requirements_dict[
                    "Full Name"]
                rule_dict["Implements Requirement [Data Quality Requirement] > Asset Type"] = dq_requirements_dict[
                    "Asset Type"]
                rule_dict["Implements Requirement [Data Quality Requirement] > Community"] = dq_requirements_dict[
                    "Community"]
                rule_dict["Implements Requirement [Data Quality Requirement] > Domain Type"] = dq_requirements_dict[
                    "Domain Type"]
                rule_dict["Implements Requirement [Data Quality Requirement] > Domain"] = dq_requirements_dict[
                    "Domain"] = ""
                rule_dict["Implements Requirement [Data Quality Requirement] > Domain Id"] = dq_requirements_dict[
                    "Requirement Measures Element [HSBC Canonical Data Element] > Domain Id"]
                rule_dict["Implements Requirement [Data Quality Requirement] > Asset Id"] = dq_requirements_dict[
                    "Requirement Measures Element [HSBC Canonical Data Element] > Asset Id"]

                # Dq requirements excel
                dq_requirements_dict["Description (No Formatting)"] = ""  # description for the dq requirement
                dq_requirements_dict["DQ Threshold"] = qr["resultUnit"]["value"]
                dq_requirements_dict["DQ Dimension"] = dimension

                # white in the dq_requirements excel
                dq_requirements_dict["Status"] = ""
                dq_requirements_dict["Requirement For Term [Preferred Business Term] > Name"] = ""
                dq_requirements_dict["Requirement For Term [Preferred Business Term] > Full Name"] = ""
                dq_requirements_dict["Requirement For Term [Preferred Business Term] > Asset Type"] = ""
                dq_requirements_dict["Requirement For Term [Preferred Business Term] > Community"] = ""
                dq_requirements_dict["Requirement For Term [Preferred Business Term] > Domain Type"] = ""
                dq_requirements_dict["Requirement For Term [Preferred Business Term] > Domain"] = ""
                dq_requirements_dict["Requirement For Term [Preferred Business Term] > Domain Id"] = ""
                dq_requirements_dict["Requirement For Term [Preferred Business Term] > Asset Id"] = ""

                dq_requirements_dict["Requirement Measures Element [HSBC Canonical Data Element] > Name"] = de_name
                dq_requirements_dict["Requirement Measures Element [HSBC Canonical Data Element] > Full Name"] = dd_name
                dq_requirements_dict[
                    "Requirement Measures Element [HSBC Canonical Data Element] > Asset Type"] = "HSBC Canonical Data Element"
                dq_requirements_dict[
                    "Requirement Measures Element [HSBC Canonical Data Element] > Community"] = "HSBC Business Information Model [Published]"
                dq_requirements_dict[
                    "Requirement Measures Element [HSBC Canonical Data Element] > Domain Type"] = "HSBC Business Glossary"
                dq_requirements_dict[
                    "Requirement Measures Element [HSBC Canonical Data Element] > Domain"] = "HBIM - Group Data Dictionary [Published]"

                # white in the dq_requirements excel
                dq_requirements_dict["Measures Compliance To [Standard] > Name"] = ""
                dq_requirements_dict["Measures Compliance To [Standard] > Full Name"] = ""
                dq_requirements_dict["Measures Compliance To [Standard] > Asset Type"] = ""
                dq_requirements_dict["Measures Compliance To [Standard] > Community"] = ""
                dq_requirements_dict["Measures Compliance To [Standard] > Domain Type"] = ""
                dq_requirements_dict["Measures Compliance To [Standard] > Domain"] = ""
                dq_requirements_dict["Measures Compliance To [Standard] > Domain Id"] = ""
                dq_requirements_dict["Measures Compliance To [Standard] > Asset Id"] = ""

                rule_dicts.append(rule_dict)
                dq_requirements_dicts.append(dq_requirements_dict)
        if len(rule_dicts) > 0 and len(dq_requirements_dicts) > 0:
            df_rule_export = pd.DataFrame(rule_dicts)
            df_dqRequirements_export = pd.DataFrame(dq_requirements_dicts, columns=list(dq_requirements_dicts[0].keys()))
            ts = str(calendar.timegm(time.gmtime()))
            export_path = os.path.join("export", ts)
            os.makedirs(export_path, exist_ok=True)
            rule_export_fn = os.path.join(export_path, "export_rule_{}.csv".format(ts))
            dqr_export_fn = os.path.join(export_path, "export_dqRequirements_{}.csv".format(ts))
            df_rule_export.to_csv(rule_export_fn, header=True, index=False)
            df_dqRequirements_export.to_csv(dqr_export_fn, header=True, index=False)
            return ts, export_path
        return "", ""
