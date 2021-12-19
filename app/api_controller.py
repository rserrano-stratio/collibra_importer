import os
import shutil
import zipfile
import uvicorn
from asyncio import sleep

from typing import List
from fastapi import FastAPI, File, UploadFile
from fastapi.responses import FileResponse
from fastapi.openapi.utils import get_openapi
# from pydantic import BaseModel

import pandas as pd

from governance_controller import GovernanceController
from collibra_controller import CollibraController
#from infer_controller import InferController


app = FastAPI()

uploads_path = "data/uploads"
csvs_path = "data/csvs"
controller = CollibraController.getInstance()
#icontroller = InferController.getInstance()


def zipdir(path, ziph):
    # ziph is zipfile handle
    for root, dirs, files in os.walk(path):
        for file in files:
            ziph.write(os.path.join(root, file),
                       os.path.relpath(os.path.join(root, file),
                                       os.path.join(path, '.')))


@app.get("/")
async def root():
    return {"message": "Collibra Integration Manager"}

@app.post("/delete_mappingqrs_table")
def delete_mappingqrs_table():
    controller.truncateMappingQRsTable()
    return {"status": "Done"}

@app.post("/update_mapping_qrs_table/")
async def update_mapping_qrs_table(file: UploadFile = File(...)):
    filepath = os.path.join(uploads_path, file.filename)
    with open(filepath, 'wb') as writer:
        writer.write(file.file.read())

    return await upload_mapping_qrs(filepath)

@app.post("/update_collibra_qrs_files/")
async def update_collibra_qrs_files(ontologyName: str = "collibra", ontologyBaseTaxonomy: str="stratio.com/collibra", files: List[UploadFile] = File(...),
                                    filter: str = '%', truncate: bool=True, upload: bool=True):
    zip_file_path = os.path.join(uploads_path, files[0].filename)
    base = os.path.basename(files[0].filename)
    filename_wo_ext = os.path.splitext(base)[0]
    directory_to_extract_to = os.path.join(csvs_path, filename_wo_ext)
    for file in files:
        filepath = os.path.join(uploads_path, file.filename)
        with open(filepath, 'wb') as writer:
            writer.write(file.file.read())

    shutil.rmtree(directory_to_extract_to, ignore_errors=True)
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        zip_ref.extractall(directory_to_extract_to)
    return await process_collibra(directory_to_extract_to, ontologyName, ontologyBaseTaxonomy, filter,
                                  truncate=truncate, upload=upload)

@app.get("/export_new_quality_rules/")
def export_new_qrs_to_collibra(metadataPath: str = "_%"):
    def export_qrs(metadataPath):
        ts, export_path = controller.export_new_qrs_to_collibra(metadataPath)
        if ts == "":
            return "", ""
        zip_fn = os.path.join("export", str(ts) + ".zip")
        zipf = zipfile.ZipFile(zip_fn, 'w', zipfile.ZIP_DEFLATED)
        zipdir(export_path, zipf)
        zipf.close()
        return zip_fn, str(ts) + ".zip"

    zip_fp, zip_fn = export_qrs(metadataPath)
    if zip_fp == "":
        return {"status": "No New QRs found to be exported"}
    return FileResponse(zip_fp, media_type="application/zip", filename=zip_fn)


# @app.post("/infer_quality_rules/")
# def infer_quality_rules(domain: str = "hsbc_bm_v2", entity: str = "test"):
#     output = icontroller.infer_qrs(domain, entity)
#     return {"Inferred_QRs": output}  # , "Mapping_QRs": mapping_qr_output


@app.post("/replicate_quality_rules/")
def replicate_quality_rules(ontology: str = "collibra"):
    output = GovernanceController.getInstance().replicate_qrs(ontology)
    return {"Replicated_QRs": output}

@app.post("/delete_collibra_quality_rules")
def delete_collibra_quality_rules():
    all_deleted, deleted_qrs_count = controller.deleteCollibraCreatedQRs()
    return {"Deleted_QRs": deleted_qrs_count}

@app.post("/purge_collibra_data")
def purge_collibra_data():
    all_deleted, deleted_qrs_count = controller.deleteCollibraCreatedQRs()
    if all_deleted:
        controller.truncateTables()
        controller.truncateControlTables()
    return {"success": all_deleted, "Deleted_QRs": deleted_qrs_count}

@app.post("/connect_ontologies/")
def connect_ontologies(file: UploadFile = File(...)):
    filepath = os.path.join(uploads_path, file.filename)
    with open(filepath, 'wb') as writer:
        writer.write(file.file.read())
    df = pd.read_csv(filepath)
    output = GovernanceController.getInstance().connect_ontologies(df)
    return {"Connected_Properties": output}


async def process_collibra(directory, ontologyName, ontologyBaseTaxonomy, filter, truncate=False, upload=False):
    # if not truncate:
    #     return {"t": "t"}
    if truncate:
        controller.truncateTables()
    if upload:
        controller.uploadCollibraFiles(directory)

    succesfulQr, failedQr, allCollibra = controller.processCollibraData(ontologyName, ontologyBaseTaxonomy, filter)
    return {"status": "Updating QRs completed", "succesfulQRs": len(succesfulQr), "failedQRs": len(failedQr),
            "Total Imported QRs": allCollibra}
    #return 0

async def upload_mapping_qrs(filepath):
    controller.truncateMappingQRsTable()
    controller.uploadCollibraMappingQRsFile(filepath)

    return {"status": "Updating Mapping QRs completed"}


def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    openapi_schema = get_openapi(
        title="Collibra QRs Controller",
        version="1.0.0",
        description="Collibra QRs Controller API",
        routes=app.routes,
    )
    openapi_schema["info"]["x-logo"] = {
        "url": "https://fastapi.tiangolo.com/img/logo-margin/logo-teal.png"
    }
    app.openapi_schema = openapi_schema
    return app.openapi_schema


app.openapi = custom_openapi

if __name__ == "__main__":
    gov = GovernanceController.getInstance(os.getenv('GOV_ROOT_URL', "https://admin.saassgt.stratio.com/"), os.getenv('GOV_TENANT', "caceis"), os.getenv('GOV_USER', "sdabbour"), os.getenv('GOV_PASSWORD', "aeWo4tha"))
    uvicorn.run(app, host="0.0.0.0", port=8000)
