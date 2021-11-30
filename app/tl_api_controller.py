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
import re

from governance_controller import GovernanceController
from collibra_controller import CollibraController
from infer_controller import InferController


app = FastAPI()

uploads_path = "/tmp"
csvs_path = "data/csvs"
controller = CollibraController.getInstance()
icontroller = InferController.getInstance()


def zipdir(path, ziph):
    # ziph is zipfile handle
    for root, dirs, files in os.walk(path):
        for file in files:
            ziph.write(os.path.join(root, file),
                       os.path.relpath(os.path.join(root, file),
                                       os.path.join(path, '.')))


@app.post("/analyze_plsql_lineage/")
def analyze_plsql_lineage(process_name: str = "EDW_DOCUMENT", file: UploadFile = File(...)):
    filepath = os.path.join(uploads_path, file.filename)
    with open(filepath, 'wb') as writer:
        writer.write(file.file.read())
    # df = pd.read_csv(filepath)
    # output = GovernanceController.getInstance().connect_ontologies(df)
    pattern_input = re.compile(":DWH_SCHEMA\.[a-zA-Z_0-9]+")
    pattern_output = re.compile(":DWH_SCHEMA_IDS\.[a-zA-Z_0-9]+")

    input_tables = []
    output_table = ""
    for i, line in enumerate(open(filepath)):
        for match in re.finditer(pattern_input, line):
            input_tables.append(match.group().replace(":DWH_SCHEMA.", ""))
            print('Found Input table on line %s: %s' % (i + 1, match.group()))
        for match in re.finditer(pattern_output, line):
            output_table = match.group().replace(":DWH_SCHEMA_IDS.", "")
            print('Found Output table on line %s: %s' % (i + 1, match.group()))
    output = {
        "process": process_name,
        "input_tables": input_tables,
        "output_table": output_table
    }
    return output


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
    gov = GovernanceController.getInstance("https://bootstrap.demo.labs.stratio.com/", "financial", "admin", "1234")
    uvicorn.run(app, host="0.0.0.0", port=8000)
