from collibra_controller import CollibraController
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

controller = CollibraController.getInstance()

# controller.truncateTables()
# controller.uploadCollibraFiles("data")

ontologyName = "hsbc_business_model"
ontologyBaseTaxonomy = "hsbc_business_model"
controller.processCollibraData(ontologyName, ontologyBaseTaxonomy, 'GPB_%')
