import os
import requests
import re
from bs4 import BeautifulSoup
from requests.packages.urllib3.exceptions import InsecureRequestWarning
import json
import os
import copy
import datetime
from cct_controller import CCTController

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


class GovernanceController:
    __instance = None
    __cct = None

    __tenant = None
    __user = None
    __password = None
    __root_url = ""
    __user_role = "SuperAdmin"

    __login_mode = os.getenv('GOV_LOGIN_MODE', "pass") # pass, cert
    ssl_cert = os.getenv('POSTGRES_CERT')
    ssl_key = os.getenv('POSTGRES_KEY')
    ssl_root_cert = os.getenv('CA_BUNDLE_PEM')

    login = '/login'
    governance_login = '/profile'
    governance_path = '/service/dg-businessglossary-api/dictionary/user'
    ontology_graph_path = '/service/dg-ontology-graph-api/ontology'
    bdl_concept_uri = '/businessLayer/v1/businessLayerConcept'
    bdl_view_uri = '/businessLayer/v1/businessLayerView'
    bdl_table_uri = '/businessLayer/v1/businessLayerTable'
    bdl_feature_uri = '/businessLayer/v1/businessLayerPropertyFeature'

    @staticmethod
    def getInstance(root_url=None, tenant=None, user=None, password=None):
        if GovernanceController.__instance is None:
            GovernanceController(root_url, tenant, user, password)
        return GovernanceController.__instance

    def __init__(self, root_url=None, tenant=None, user=None, password=None):
        if GovernanceController.__instance is not None:
            pass
        else:
            # try:
            #     GovernanceController.__cct = CCTController()
            #     res = GovernanceController.__cct.main()
            #     pass
            # except:
            #     pass
            self.updateAccess(root_url, tenant, user, password)
            self.sso_url = self.__root_url + GovernanceController.login
            self.gov_url = self.__root_url + GovernanceController.governance_path
            self.graph_url = self.__root_url + GovernanceController.ontology_graph_path
            self.gov_cookie_url = self.gov_url + GovernanceController.governance_login

            self.cookies = self.get_cookies()
            self.cookies_time = datetime.datetime.now()
            GovernanceController.__instance = self

    def get_login_mode(self):
        return self.__login_mode

    def set_login_mode(self, login_mode):
        self.__login_mode = login_mode

    def get_root_url(self):
        return self.__root_url

    def updateAccess(self, root_url=None, tenant=None, user=None, password=None):
        if root_url is not None:
            self.__root_url = root_url
            self.sso_url = root_url + GovernanceController.login
            self.gov_url = root_url + GovernanceController.governance_path
            self.graph_url = root_url + GovernanceController.ontology_graph_path
            self.gov_cookie_url = self.gov_url + GovernanceController.governance_login
        if tenant is not None:
            self.__tenant = tenant
        if user is not None:
            self.__user = user
        if password is not None:
            self.__password = password

    def refresh_login(self, renew_cookies=False):
        d2 = datetime.datetime.now()
        time_delta = d2 - self.cookies_time
        total_seconds = time_delta.total_seconds()
        minutes = total_seconds / 60
        if renew_cookies or minutes >= 120:
            self.cookies = self.get_cookies()
            self.cookies_time = d2

    def getBaseUrl(self):
        # return self.__root_url
        return re.sub("^|/$", "", self.__root_url)

    def getApiUrl(self):
        if self.get_login_mode() == "pass":
            return self.getBaseUrl() + "/service/dg-businessglossary-api"
        return self.getBaseUrl()

    def getUiUrl(self):
        return self.getBaseUrl() + "/service/governance-ui"

    def getOntologyAPIUrl(self):
        return self.getBaseUrl() + "/service/dg-ontology-graph-api"

    # Get cookies function
    def login_sso(self, url, username, password, tenant):
        """
        Function that simulates the login in to sparta endpoint flow with SSO to obtain a valid
        cookie that will be used to make requests to Marathon
        """
        # First request to mesos master to be redirected to gosec sso login
        # page and be given a session cookie
        r = requests.Session()
        first_response = r.get(url, verify=False)
        callback_url = first_response.url

        # Parse response body for hidden tags needed in the data of our login post request
        body = first_response.text
        all_tags = BeautifulSoup(body, 'lxml').find_all('input', type='hidden')
        tags_to_find = ['lt', 'execution']
        hidden_tags = [tag.attrs for tag in all_tags if tag['name'] in tags_to_find]
        data = {tag['name']: tag['value'] for tag in hidden_tags}

        # Add the rest of needed fields and login credentials in the data of
        # our login post request and send it
        data.update({
            '_eventId': 'submit',
            'submit': 'LOGIN',
            'username': username,
            'password': password,
            'tenant': tenant
        })
        login_response = r.post(callback_url, data=data, verify=False)
        return login_response

    def get_cookies(self):
        if self.get_login_mode() == "cert":
            return {}
        # Login and get cookies
        cookies = self.login_sso(self.sso_url, self.__user, self.__password, self.__tenant).request._cookies
        gov_cookie = requests.get(
            self.gov_cookie_url, verify=False, allow_redirects=True, cookies=cookies
        ).headers.get('set-cookie').split(' ')

        governance_cookie = [v for k, v in
                             [k.strip(';').split('=') for k in gov_cookie if 'stratio-governance-auth' in k]]
        cookies.set('stratio-governance-auth', governance_cookie[0])
        return cookies

    def getHeaders(self):
        headers = {
            'Connection': 'keep-alive',
            'Accept': 'application/json, text/plain, */*',
            'X-UserID': self.__user,
            'X-RolesID': self.__user_role,
            'X-TenantID': self.__tenant,
            # 'Content-Type': 'application/json',
        }

        return headers

    def get_headers(self):

        headers = {
            'X-RolesID': self.__user_role,
            'X-TenantID': self.__tenant,
            'X-UserID': self.__user,
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }

        return headers

    def getCookie(self):
        self.refresh_login()
        return self.cookies

    def getPhysicalTableMetadataPath(self, dataSource, database, tableName, schema=""):
        tableName = schema + "." + tableName if schema != "" else tableName

        metadataPath = "{}://{}>/:{}:".format(dataSource, database, tableName)

        return metadataPath

    def getPhysicalTableDataAssetMetadataPath(self, domain, dataSource, database, tableName, schema=""):
        tableName = schema + "_" + tableName if schema != "" else tableName

        metadataPath = "{0}://{0}>/:{1}:".format(domain, tableName)

        return metadataPath

    def getConceptMetadataPath(self, domainName, conceptName):
        metadataPath = "{0}://{0}>/:{1}:".format(domainName, conceptName)

        return metadataPath

    def requestsGet(self, url, params=None, new_headers=None):
        cookies = self.getCookie()
        headers = self.getHeaders()
        if new_headers is not None:
            headers = new_headers
        if self.get_login_mode() == "pass":
            response = requests.get(url, headers=headers, params=params, cookies=cookies, verify=False)
        else:
            response = requests.get(url, headers=headers, params=params, cert=(self.ssl_cert, self.ssl_key), verify=self.ssl_root_cert)
        return response
    
    def requestsPostJSON(self, url, data_json=None):
        cookies = self.getCookie()
        headers = self.getHeaders()
        if self.get_login_mode() == "pass":
            response = requests.post(url, headers=headers, cookies=cookies, json=data_json, verify=False)
        else:
            response = requests.post(url, headers=headers, cookies=cookies, json=data_json, cert=(self.ssl_cert, self.ssl_key), verify=self.ssl_root_cert)
        return response

    def requestsPostFiles(self, url, headers=None, params=None, files=None):
        cookies = self.getCookie()
        #headers = self.getHeaders()
        if self.get_login_mode() == "pass":
            response = requests.post(url, headers=new_headers, params=params, files=files, cookies=self.getCookie(), verify=False)
        else:
            response = requests.post(url, headers=new_headers, params=params, files=files, cert=(self.ssl_cert, self.ssl_key), verify=self.ssl_root_cert)
        return response

    def requestsDelete(self, url):
        cookies = self.getCookie()
        headers = self.getHeaders()
        if self.get_login_mode() == "pass":
            response = requests.delete(url, headers=headers, cookies=cookies, verify=False)
        else:
            response = requests.delete(url, headers=headers, cert=(self.ssl_cert, self.ssl_key), verify=self.ssl_root_cert)
        return response

    def requestsPut(self, url, params=None):
        cookies = self.getCookie()
        headers = self.getHeaders()
        if self.get_login_mode() == "pass":
            response = requests.put(url, headers=headers, params=params, cookies=cookies, verify=False)
        else:
            response = requests.put(url, headers=headers, params=params, cert=(self.ssl_cert, self.ssl_key), verify=self.ssl_root_cert)
        return response

    def getConceptsDetails(self, metadataPath, conceptFilter="%"):

        params = (
            ('size', '10'),
            ('page', '0'),
            ('metadataPathLike', metadataPath + conceptFilter),
            ('sort', 'name,asc'),
            ('typeNotIn', 'SEMANTIC,HDFS,XD,SQL'),
            ('subtypeIn', 'RESOURCE,PATH'),
        )

        response = requestGet(self.getApiUrl() + '/dictionary/user/catalog/v1/dataAsset/searchByMetadataPathLikeAndTypeNotInAndSubtypeIn', params=params)

        if response.status_code in [200, 201] and response.json().get("totalElements", None) is not None:
            return response.json()["dataAssets"]["dataAssets"]

        return []

    def getSemanticConceptMetadataPath(self, domainName, conceptName):
        semanticDomainName = "semantic_" + domainName
        metadataPath = "{0}://{0}>/:{1}:".format(semanticDomainName, conceptName)

        return metadataPath

    def addQualityRule(self, qr_json):
        headers = self.getHeaders()
        cookies = self.getCookie()

        response = self.requestsPostJSON(self.getApiUrl() + '/dictionary/user/quality/v1/quality', data_json=qr_json)

        # print(response.status_code)
        # print(response.content)

        return response

    def deleteQualityRule(self, qr_id):
        headers = self.getHeaders()
        cookies = self.getCookie()

        response = self.requestsDelete(self.getApiUrl() + '/dictionary/user/quality/v1/quality/' + str(qr_id))

        return response

    def searchDataAssetByMetadataPath(self, metadataPath):

        headers = self.getHeaders()
        cookies = self.getCookie()

        params = (
            ('metadataPathLike', metadataPath),
        )

        response = self.requestsGet(self.getApiUrl() + '/dictionary/user/catalog/v1/dataAsset/searchByMetadataPathLike', params=params)

        return response.json()

    def uploadOntology(self, ontologyName, filepath):
        new_headers = copy.deepcopy(self.getHeaders())
        new_headers.pop("Content-Type")
        new_headers["Accept"] = "*/*"

        params = (
            ('ontologyName', ontologyName),
        )

        path_head, path_tail = os.path.split(filepath)
        files = {
            'file': (path_tail, open(filepath, 'rb'), "application/rdf+xml")
        }

        response = self.requestsPostFiles(self.getOntologyAPIUrl() + '/ontology/importOntology', headers=new_headers, params=params, files=files)
        return response

    def downloadOntology(self, ontologyName, ontologyBase="https://www.stratio.com"):
        new_headers = copy.deepcopy(self.getHeaders())
        new_headers.pop("Content-Type")
        new_headers["Accept"] = "*/*"

        output_fn = ontologyName + ".owl"

        params = (
            ('ontologyBase', ontologyBase),
            ('ontologyName', ontologyName),
        )

        response = self.requestsGet(self.getOntologyAPIUrl() + '/ontology/exportOntology', params=params, new_headers=new_headers)

        if response is not None and response.ok:
            with open(output_fn, mode='wb') as localfile:
                localfile.write(response.content)

        return output_fn

    def getQR(self, size=10, metadataPath='_%'):

        params = (
            ('size', size),
            ('page', '0'),
            ('sort', 'modifiedAt,desc'),
            ('metadataPathLike', metadataPath),
        )

        response = self.requestsGet(self.getApiUrl() + '/dictionary/user/quality/v1/quality', params=params)

        return response.json()

    def getQRByName(self, metadataPath='_%', name='%', size=10):

        params = (
            ('size', size),
            ('page', '0'),
            ('sort', 'modifiedAt,desc'),
            ('metadataPathLike', metadataPath),
            ('nameLike', name),
        )

        response = self.requestsGet(self.getApiUrl() + '/dictionary/user/quality/v1/quality', params=params)
        try:
            return response.json().get("content", [])
        except Exception as e:
            return []

    def getQRById(self, id):

        params = (
            ('id', id),
        )

        response = self.requestsGet(self.getApiUrl() + '/dictionary/user/quality/v1/quality/' + str(id), params=params)
        try:
            return response.json()
        except Exception as e:
            return []

    def getAttributes(self, metadataPath):

        params = (
            ('relation', 'false'),
            ('metadataPath', metadataPath),
        )

        response = self.requestsGet(self.getApiUrl() + '/dictionary/user/catalog/v1/keyDataAsset/searchByMetadataPath', params=params)

        return response.json()

    def getQrInMetadataPath(self, metadataPath, size=500):

        params = (
            ('size', size),
            ('metadataPathLike', metadataPath),
        )

        response = self.requestsGet(self.getApiUrl() + '/dictionary/user/quality/v1/quality/searchByMetadataPathLike', params=params)

        return response.json()

    def getConceptsRelated(self, metadataPath):

        params = (
            ('relation', 'true'),
            ('metadataPath', metadataPath),
        )

        response = self.requestsGet(self.getApiUrl() + '/dictionary/user/catalog/v1/keyDataAsset/searchByMetadataPath', params=params)

        return response.json()

    def key_searchByKeyLike(self, key_filter="%"):

        params = (
            ('keyLike', key_filter),
        )

        response = self.requestsGet(self.getApiUrl() + '/dictionary/user/catalog/v1/key/searchByKeyLike', params=params)

        try:
            return response.json().get("content", [])
        except Exception as e:
            return []

    def key_createKey(self, key_name, key_description, security=False, relation=False, key_type="string", key_subType="open",
                      key_value=""):
        data = '{"id":-1,"key":"test_key","description":"test_key","active":true,"tenant":"NONE","security":false,"valueDesign":{"type":"string","subType":"open","value":""},"relation":false}'
        data = json.loads(data)
        data["key"] = key_name
        data["description"] = key_description
        data["tenant"] = self.__tenant
        data["security"] = security
        data["relation"] = relation
        data["valueDesign"]["type"] = key_type
        data["valueDesign"]["subType"] = key_subType
        data["valueDesign"]["value"] = key_value

        response = self.requestsPostJSON(self.getApiUrl() + '/dictionary/user/catalog/v1/key', data_json=data)
        try:
            response.json()["id"]
        except Exception as e:
            return -1

    def getGenericQRs(self):
        params = (
            ('size', '999'),
            ('page', '0'),
            ('sort', 'modifiedAt,desc'),
            ('metadataPathLike', ''),
        )

        response = self.requestsGet(self.getApiUrl() + '/dictionary/user/quality/v1/quality', params=params)
        try:
            data = response.json()
            totalElements = data.get("totalElements", 0)
            if totalElements > 0:
                return data.get("content", [])
            return []
        except Exception as e:
            return []

    def getGenericQR(self, qrName):
        genericQRs = self.getGenericQRs()
        foundQR = None
        for qr in genericQRs:
            if qr["name"] == qrName:
                foundQR = qr
                break
        if foundQR is not None:
            response = self.requestsGet(self.getApiUrl() + '/dictionary/user/quality/v1/quality/' + str(foundQR["id"]), params=params)
            try:
                return response.json()
            except Exception as e:
                return None
        return None

    def getQRByID(self, id):
        params = (
            ('id', id)
        )
        response = self.requestsGet(self.getApiUrl() + '/dictionary/user/quality/v1/quality/' + str(id), params=params)
        try:
            return response.json()
        except Exception as e:
            return None

    def getCustomQRFromGeneric(self, genericQRName, attributeName, metadataPath, namePrefix="", dqDimension="",
                               extraParams=[]):
        genericQR = self.getGenericQR(genericQRName)
        if genericQR is None:
            return {}
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
            "metadataPath": "ontologies://ontology",
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

        data["parameters"]["filter"] = genericQR["parameters"]["filter"]
        data["resultAction"] = genericQR["resultAction"]
        data["resultOperation"] = genericQR["resultOperation"]
        data["resultOperationType"] = genericQR["resultOperationType"]
        data["resultUnit"] = genericQR["resultUnit"]
        data["active"] = genericQR["active"]
        data["description"] = genericQR["description"]
        data["audit"] = genericQR["audit"]

        if len(extraParams) > 0:
            for i in range(len(extraParams)):
                if len(data["parameters"]["filter"]["cond"][i]["param"]) > 0:
                    if data["parameters"]["filter"]["cond"][i]["param"][0].get("type", "") == "ValueOperand":
                        data["parameters"]["filter"]["cond"][i]["param"][0]["value"] = extraParams[i]

        # data["parameters"]["filter"]["cond"][0]["attribute"] = attributeName
        for cond in data["parameters"]["filter"]["cond"]:
            cond["attribute"] = attributeName
        data["metadataPath"] = metadataPath
        # data["active"] = status
        # data["resultUnit"]["value"] = threshold
        if dqDimension != "":
            dqDimension = dqDimension + "_"
        data["name"] = namePrefix + attributeName + "_" + genericQRName + "_" + dqDimension + "Inferred"

        return data

    def replicate_qrs(self, ontology_name):
        output_count = 0
        gov = GovernanceController.getInstance()
        ontologyMetadataPathLike = "%" + ontology_name + "%"
        list_Qr = gov.getQrInMetadataPath(ontologyMetadataPathLike)["content"]
        # print("Found {} QRs for the ontology: {}".format(len(list_Qr), ontology_name))
        for element in list_Qr:

            element_metadataPath = element["metadataPath"]
            elements_related = gov.getConceptsRelated(element_metadataPath)
            qr = copy.deepcopy(element)

            # if len(elements_related) > 0:
            #     print("{} has {} related concepts".format(element_metadataPath, len(elements_related)))
            for element_related in elements_related:

                element_related_metadataPath = element_related["value"]["value"]["metadataPath"]
                # print(element_related_metadataPath)
                element_related_name = element_related_metadataPath.split(":")[-2]
                # print(element_related_name)
                qr["id"] = -1
                qr["metadataPath"] = element_related_metadataPath
                qr["name"] = "replicatedQR_" + element["name"]

                for cond in qr["parameters"]["filter"]["cond"]:
                    cond["attribute"] = element_related_name
                    if cond["operation"].lower() == "regex":
                        if cond["param"][0]["type"].lower() == "ValueOperand".lower():
                            cond["param"][0]["value"] = cond["param"][0]["value"].replace("\\", "\\\\")

                qr.pop("query")
                qr.pop("qualityGenericId")
                qr.pop("tenant")
                qr.pop("createdAt")
                qr.pop("modifiedAt")
                qr.pop("userId")
                print(qr)
                try:
                    res = gov.addQualityRule(qr)
                    # print(res)
                    output_count = output_count + 1
                except Exception as e:
                    print("Error occurred replicating the QR")
                    pass
        return output_count

    def connect_ontologies(self, data):
        def getClassFullPath_Src(row):
            classPath = row["src_entity"]
            if row["src_taxonomy"] != "":
                classPath = row["src_taxonomy"] + "/" + classPath
            classPath = row["src_ontology"] + "/" + classPath
            return classPath

        def getClassFullPath_Dst(row):
            classPath = row["dst_entity"]
            if row["dst_taxonomy"] != "":
                classPath = row["dst_taxonomy"] + "/" + classPath
            classPath = row["dst_ontology"] + "/" + classPath
            return classPath

        connected_properties_count = 0
        df_mapping = data
        # src_ontology, src_taxonomy, src_entity, src_property
        df_mapping["src_class_full_path"] = df_mapping.apply(getClassFullPath_Src, axis=1)
        df_mapping["dst_class_full_path"] = df_mapping.apply(getClassFullPath_Dst, axis=1)

        df_mapping["src_ontologyMDP"] = "ontologies://" + df_mapping[
            "src_class_full_path"] + ">/:" + df_mapping["src_property"] + ":"
        df_mapping["dst_ontologyMDP"] = "ontologies://" + df_mapping[
            "dst_class_full_path"] + ">/:" + df_mapping["dst_property"] + ":"


        attribute = self.key_searchByKeyLike("%related to%")[0]
        attribute_id = attribute["id"]

        for row in df_mapping.iterrows():
            res = self.getConceptsDetails(row[1]["dst_ontologyMDP"])

            if len(res) > 0:
                res = res[0]
                data = '''[{
                "id": -1,
                "key": {
                    "id": 34,
                    "key": "related to",
                    "description": "generic relation from an entity to other one",
                    "active": true,
                    "security": false,
                    "relation": true,
                    "valueDesign": {
                        "type": "relation"
                    },
                    "userId": "admin",
                    "createdAt": "2021-01-20T08:56:36.175Z",
                    "modifiedAt": "2021-01-20T08:56:36.175Z"
                },
                "value": {
                    "name": "",
                    "value": {
                        "id": 35648,
                        "name": "action",
                        "resource": "DATA_ASSET_ONTOLOGY",
                        "metadataPath": "ontologies://mdm/mdm>/:action:"
                    }
                },
                "metadataPath": "ontologies://hsbc_gpb/hsbc_business_model/Data_Elements>/:account_balance_amount:"}]'''

                data = json.loads(data)

                data[0]["key"]["id"] = attribute_id
                data[0]["value"]["value"]["id"] = res["id"]
                data[0]["value"]["value"]["name"] = res["name"]
                data[0]["value"]["value"]["metadataPath"] = row[1]["dst_ontologyMDP"]
                data[0]["metadataPath"] = row[1]["src_ontologyMDP"]

                response = self.requestsPostJSON(self.getApiUrl() + '/dictionary/user/catalog/v1/keyDataAsset', data_json=data)
                if response.ok:
                    connected_properties_count += 1
        return connected_properties_count
