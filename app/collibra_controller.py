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
from db_models import CollibraStatus, CollibraImportProcess, CollibraDBRecord
import re
import copy

regex = "[a-zA-Z0-9]+"

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


class CollibraRecord:
    dd_l0 = None
    dd_l1 = None
    dd_l2 = None
    dd_l3 = None
    de_name = None
    dc_requirement_description = None
    req_dimension_id = None
    dqr_target = None
    qr_generic_name = None

    def __init__(self):
        pass
    
    def __init__(self, dd_l0, dd_l1, dd_l2, dd_l3, de_name, dc_requirement_description, req_dimension_id, dqr_target, qr_generic_name):
        self.dd_l0 = dd_l0
        self.dd_l1 = dd_l1
        self.dd_l2 = dd_l2
        self.dd_l3 = dd_l3
        self.de_name = de_name
        self.dc_requirement_description = dc_requirement_description
        self.req_dimension_id = req_dimension_id
        self.dqr_target = dqr_target
        self.qr_generic_name = qr_generic_name
        pass

class CollibraController:
    __instance = None
    __dbc = None
    tables = ["dataconcepts", "datadomains", "dataelements", "dataqualityrules", "dqrequirements",
"dqrequirementsfacts", "elementdictionary", "rulerequirementmapping", "conceptdictionary"]
    mapping_table = "mappingqrs"
    control_tables = [CollibraImportProcess.__tablename__, CollibraStatus.__tablename__, CollibraDBRecord.__tablename__]

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
    def get_all_values(d):
        if isinstance(d, dict):
            for v in d.values():
                yield from CollibraController.get_all_values(v)
        elif isinstance(d, list):
            for v in d:
                yield from CollibraController.get_all_values(v)
        else:
            yield d 


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

    def register_collibra_process(self, records_count):
        collibra_process = CollibraImportProcess()
        collibra_process.records_count = records_count

        try:
            result = self.__dbc.add_record_with_return(collibra_process)
        except Exception as e:
            result = None
            print(e)

        return result

    def register_collibra_status(self, metadatapath, qr_dimension, qr_name, qr_id, qr_status, qr_active, qr_threshold, import_process_id):
        status_record = CollibraStatus()
        status_record.metadatapath = metadatapath
        status_record.stratio_qr_dimension = qr_dimension
        status_record.stratio_qr_name = qr_name
        status_record.stratio_qr_id = qr_id
        status_record.stratio_qr_status = qr_status
        status_record.stratio_qr_active = qr_active
        status_record.qr_threshold = qr_threshold
        status_record.import_process_id = import_process_id
        #status_record.creation_date = None
        #status_record.update_date = None

        try:
            id = self.__dbc.add_record_with_return(status_record)
        except Exception as e:
            print(e)

        return id

    def getLastCollibraProcessID(self):
        pass

    @staticmethod
    def normalize_name(name):
        result = re.findall(regex, name)
        result = " ".join(result)
        return result.replace(" ", "_").replace("/", "-")

    def deleteCollibraCreatedQRs(self):
        governanceController = GovernanceController.getInstance()
        all_created_qrs = self.__dbc.get_all(CollibraStatus)
        all_deleted = True
        deleted_qrs_count = 0
        for created_qr in all_created_qrs:
            try:
                print(created_qr.stratio_qr_id)
                governanceController.deleteQualityRule(created_qr.stratio_qr_id)
                deleted_qrs_count += 1
            except Exception as e:
                all_deleted = False
                print("Error deleting QR with id: " + str(created_qr.stratio_qr_id))
                pass
        return all_deleted, deleted_qrs_count

    def processCollibraData(self, ontologyName, ontologyBaseTaxonomy, filter='%'):
        run_mode = os.getenv("APP_RUN_MODE", "PRO")
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
                    on dc.requirement_description = mqc.requirement_description and mqc.stratio_qr_dimension is not null
                    where dc.published_indicator = 1
                '''
        #TODO check null in stratio_qr_dimension and change mappings qrs csv with all rows, check if condition have VAR, if have VAR make inactive qr, delete counter
        engine = self.__dbc.engine
        qr_df = pd.read_sql_query(qrQuery, engine)

        df_index = qr_df.index
        number_of_rows = len(df_index)

        collibra_process_id = self.register_collibra_process(number_of_rows)

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
        #print(qr_df.count())
        current_count = 0

        generic_qrs_dict = {}

        genericQRs = governanceController.getGenericQRs()
        for qr in genericQRs:
            generic_template = governanceController.getQRById(qr["id"])
            generic_template["id"] = -1
            generic_template["catalogAttributeType"] = "RESOURCE"
            generic_template["parameters"]["catalogAttributeType"] = "RESOURCE"
            generic_template["parameters"]["catalogAttributeType"] = "RESOURCE"
            generic_template["parameters"]["table"]["type"] = "ONTOLOGY"
            del generic_template["tenant"]
            del generic_template["createdAt"]
            del generic_template["modifiedAt"]
            del generic_template["userId"]
            del generic_template["qualityGenericId"]
            del generic_template["query"]
            generic_qrs_dict[qr["name"]] = generic_template
        
        print("Found: {} Generic QRs".format(len(generic_qrs_dict.keys())))

        with open('failed_qrs.txt', 'w') as f:
        
            for element in qr_df.iterrows():
                # build metadataPath
                # get the actualDataAsset (confirme the actual metadatapath exist)
                # assign the Qr
                #print(element[1]["dc_requirement_description"])
                nameLike = element[1]["qr_generic_name"]
                #print(nameLike)
                qr_template = copy.deepcopy(generic_qrs_dict[nameLike])

                class_name = CollibraController.normalize_name(str(element[1]["dd_l0"]))

                l0 = CollibraController.normalize_name(str(element[1]["dd_l0"]))
                l1 = CollibraController.normalize_name(str(element[1]["dd_l1"]))
                l2 = CollibraController.normalize_name(str(element[1]["dd_l2"]))
                l3 = CollibraController.normalize_name(str(element[1]["dd_l3"]))
                
                columnName = CollibraController.normalize_name(CollibraController.normalize_name(str(element[1]["de_name"]).split("[")[0].rstrip()))

                metadataPath = 'ontologies://{}/{}'.format(ontologyName, ontologyBaseTaxonomy) + "/" + l0 + "_t"
                

                if l1 is not None and l1 not in ["", "nan", "None"]:
                    metadataPath = metadataPath + "/" + l1 + "_t"
                    class_name = CollibraController.normalize_name(str(element[1]["dd_l1"]))
                if l2 is not None and l2 not in ["", "nan", "None"]:
                    metadataPath = metadataPath + "/" + l2 + "_t"
                    class_name = CollibraController.normalize_name(str(element[1]["dd_l2"]))

                if l3 is not None and l3 not in ["", "nan", "None"]:
                    metadataPath = metadataPath + "/" + l3 + "_t"
                    class_name = CollibraController.normalize_name(str(element[1]["dd_l3"]))

                metadataPath = metadataPath + ">/:{}:{}:".format(class_name,columnName)
                
                #print(qr_template)

                ### Collibra Record
                collibra_record = CollibraRecord(l0, l1, l2, l3, columnName, element[1]["dc_requirement_description"], element[1]["req_dimension_id"], element[1]["dqr_target"], nameLike)
                collibra_db_record = CollibraDBRecord()
                collibra_db_record.dd_l0 = collibra_record.dd_l0
                collibra_db_record.dd_l1 = collibra_record.dd_l1
                collibra_db_record.dd_l2 = collibra_record.dd_l2
                collibra_db_record.dd_l3 = collibra_record.dd_l3
                collibra_db_record.de_name = collibra_record.de_name
                collibra_db_record.dc_requirement_description = collibra_record.dc_requirement_description
                collibra_db_record.req_dimension_id = collibra_record.req_dimension_id
                collibra_db_record.dqr_target = collibra_record.dqr_target
                collibra_db_record.qr_generic_name = collibra_record.qr_generic_name
                collibra_db_record.import_process_id = collibra_process_id
                collibra_db_record.stratio_qr_dimension = nameLike
                collibra_db_record.metadatapath = metadataPath

                self.__dbc.add_record(collibra_db_record)
                ### End of Collibra Record

                
                qr_template["metadataPath"] = metadataPath
                for cond in qr_template["parameters"]["filter"]["cond"]:
                    cond["attribute"] = columnName
                    if any(x in ['VAR', 'VAR1', 'VAR2'] for x in CollibraController.get_all_values(cond)):
                        qr_template["active"] = False
                qr_template["name"] = "{}-{}-{}".format(class_name, columnName, nameLike)
                threshold_float = element[1]["dqr_target"] * 100
                qr_template["resultUnit"]["value"] = str(threshold_float).replace(".",",")
                #print(type(qr_template["resultUnit"]["value"]))
                #print(qr_template["resultUnit"]["value"])

                #print(qr_template)

                res = governanceController.searchDataAssetByMetadataPath(metadataPath)
                '''
                if res.get("totalElements", 0) != 0:
                    succesfulQr.append(metadataPath)
                else:
                    failedQr.append(metadataPath)
                    f.write(metadataPath + '\n')
                current_count += 1
                #if current_count == 30:
                #    break
                '''
                if res.get("totalElements", 0) != 0:
                    add_qr = True
                    # Check If Status already exists
                    # If exists, check if it is the same
                    if self.__dbc.find_collibra_status_record(nameLike, metadataPath) is not None:
                        #TBI, Check if it is not the same then, delete and create
                        add_qr = False
                        pass

                    if add_qr:
                        qr_result = governanceController.addQualityRule(qr_template)

                        #print(qr_result.text)
                        #print(qr_result.ok)

                        if qr_result is not None and qr_result.ok:
                            succesfulQr.append(metadataPath)
                            comp_qr_created += 1
                            qr_id = qr_result.json().get("id")
                            qr_active = True
                            qr_status = "Active"
                            if not qr_template["active"]:
                                qr_active = False
                                qr_status = "Inactive"
                                
                            #register_collibra_status(self, metadatapath, qr_dimension, qr_name, qr_id, qr_status, qr_active, qr_threshold, import_process_id)
                            self.register_collibra_status(metadataPath, nameLike, qr_template["name"], qr_id, qr_status, qr_active, threshold_float, collibra_process_id)

                        else:
                            failedQr.append(metadataPath)
                

                        #qr_result = governanceController.addQualityRule(qr_template)
                else:
                    failedQr.append(metadataPath)
                    f.write(metadataPath + '\n')
                        
                current_count += 1
                if run_mode.lower()=="dev" and current_count == 1:
                    break

                
        
        # At the end of the process, we need to check if there is a need to delete a created QR because it does not exist in Collibra any more
        # To do so, we compare status table for this import ID, with the table input of the previous Collibra Import Process
        # We need to query CollibraStatus table, get unique Metadtapath & Dimension QRs
        # Look for them in the CollibraDBRecord for this processID, if does not exist, delete!
        
        delete_counter = 0

        all_created_qrs = self.__dbc.get_all(CollibraStatus)
        for created_qr in all_created_qrs:
            if not self.__dbc.exists_collibra_db_record(collibra_process_id, created_qr.stratio_qr_dimension, created_qr.metadatapath):
                # Delete the QR!!
                governanceController.deleteQualityRule(created_qr.stratio_qr_id)
                delete_counter += 1
                pass
            pass
        
        qr_created = comp_qr_created + conf_qr_created + val_qr_created
        # print("Existing Data Elements: {}".format(qr_created))
        print("Sucessfully created: {} quality rules".format(len(succesfulQr)))
        print("Failed to create: {} quality rules".format(len(failedQr)))
        print("Deleted: {} quality rules".format(delete_counter))
        #qrs = governanceController.getQRByName(name="%_Automated%", size=10000)
        #print("Total Collibra QRs Created: {}".format(len(qrs)))
        #return succesfulQr, failedQr, len(qrs)
        print(failedQr)
        return succesfulQr, failedQr, 0


    def truncateTables(self):
        for table in CollibraController.tables:
            try:
                self.__dbc.truncate_table(table)
            except Exception as e:
                print("Failed to truncate table: {}".format(table))
                pass

    def truncateMappingQRsTable(self):
        try:
            self.__dbc.truncate_table(CollibraController.mapping_table)
        except Exception as e:
            print("Failed to truncate table: {}".format(table))
            pass

    def truncateControlTables(self):
        for table in CollibraController.control_tables:
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


    def uploadCollibraMappingQRsFile(self, filepath):

        engine = self.__dbc.engine

        postgreSQLConnection = engine.connect()

        postgreSQLTable = CollibraController.mapping_table
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