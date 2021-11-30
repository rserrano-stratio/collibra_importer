select 
/dataelements.name as de_name, /dataelements.published_indicator as published_indicator,
/datadomains.name as dd_name, datadomains.l0_domain_name as dd_l0, datadomains.l1_domain_name as dd_l1, datadomains.l2_domain_name as dd_l2, datadomains.l3_domain_name as dd_l3, concat(datadomains.l0_domain_name , '', datadomains.l1_domain_name, '', datadomains.l2_domain_name, '', datadomains.l3_domain_name, '', dataelements.name) as domain_path,
dqrequirements.published_indicator as dqr_published_indicator, dqrequirements.data_quality_requirement_target as dqr_target,
dataqualityrules.name as dq_name, dataqualityrules.data_quality_rule as dq_qr_rule, dataqualityrules.data_quality_dimension_id as dq_dimension, dataqualityrules.data_quality_target as dq_target 



select dd.l0_domain_name as dd_l0, dd.l1_domain_name as dd_l1, dd.l2_domain_name as dd_l2, dd.l3_domain_name as dd_l3, 
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


curl 'https://admin.saassgt.stratio.com/service/dg-businessglossary-api/dictionary/user/quality/v1/quality?size=10&page=0&sort=modifiedAt,desc&metadataPathLike=&nameLike=%25Validity_1%25' \
  -H 'Connection: keep-alive' \
  -H 'sec-ch-ua: "Chromium";v="92", " Not A;Brand";v="99", "Google Chrome";v="92"' \
  -H 'Accept: application/json, text/plain, */*' \
  -H 'X-UserID: sdabbour' \
  -H 'X-RolesID: SuperAdmin' \
  -H 'sec-ch-ua-mobile: ?0' \
  -H 'X-TenantID: caceis' \
  -H 'User-Agent: Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36' \
  -H 'Sec-Fetch-Site: same-origin' \
  -H 'Sec-Fetch-Mode: cors' \
  -H 'Sec-Fetch-Dest: empty' \
  -H 'Referer: https://admin.saassgt.stratio.com/service/governance-ui/quality-rules' \
  -H 'Accept-Language: es-ES,es;q=0.9' \
  -H 'Cookie: _ga=GA1.2.847504608.1615795988; intercom-id-wn4z9z0y=307d263f-7d8d-4789-87cb-230a42f29876; SSOID=s1; _gid=GA1.2.1571481711.1638272631; ia-login-token=Bearer%20eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJvc2NhcmdvbWV6IiwiQ0xBSU1fVE9LRU4iOiJST0xFX0dVRVNULFJPTEVfREVTS0JPT0tJTkdfVVNFUiIsImlhdCI6MTYzODI3MjYzNSwiaXNzIjoiTVMtTE9HSU4iLCJqdGkiOiIxZTFhMzI3Yy05MmZiLTRlNWQtYjE5NC00MWNiNzYzODVlZDgiLCJuYW1lIjoiT3NjYXIgR29tZXogUmFtaXJleiIsIm1haWwiOiJvc2NhcmdvbWV6QHN0cmF0aW8uY29tIiwiZXhwIjoxNjM4MzAxNDM1fQ.IZaDV5EGEXiZGrJuXl5iBFQFRNoq6n0hWGzKX8fEbvA; ia-login-role=GUEST; _clck=1cciiuy|1|ewv|0; _clsk=1c1k8nn|1638272685760|5|1|d.clarity.ms/collect; sso_redirection=/service/dg-businessglossary-api/dictionary/swagger-ui.html; dcos-acs-auth-cookie=eyJhbGciOiJIUzI1NiIsImtpZCI6InNlY3JldCIsInR5cCI6IkpXVCJ9.eyJjbiI6InNkYWJib3VyIiwiZXhwIjoxNjM4MzA2NTg1LCJncm91cHMiOlsiY2FjZWlzLWRpc2NvdmVyeS1hZG1pbiIsIm1hbmFnZXJfYWRtaW4iLCJyb2NrZXQtY2FjZWlzIiwiY2FjZWlzLVN1cGVyQWRtaW4tZ292ZXJuYW5jZSIsImNhY2VpcyJdLCJtYWlsIjoic2RhYmJvdXJAc2Fhc3NndC5pbnQiLCJ0ZW5hbnQiOiJjYWNlaXMiLCJ1aWQiOiJzZGFiYm91ciJ9.X2mj19w45sDPWtJVJxj6IGyCK_6yLh63KHYn8lVdaOk; dcos-acs-info-cookie=eyJ1aWQiOiJzZGFiYm91ciIsImRlc2NyaXB0aW9uIjoic2RhYmJvdXIifQ==; stratio-governance-auth=eyJhbGciOiJIUzI1NiJ9.eyJnb3Zlcm5hbmNlX3JvbGVzIjoiU3VwZXJBZG1pbiIsInVpZCI6InNkYWJib3VyIn0.36gAg1mYqUse8br_p982EIDcBW5KvichRG168_BNvwg' \
  --compressed