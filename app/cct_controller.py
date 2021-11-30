#!/usr/bin/python
import json
import os
import time
import datetime
import traceback
import requests


class CCTController:

    settings = {
        "url": os.getenv("CCT_URL", "admin.hsbc.stratio.com"),
        "username": os.getenv("CCT_USERNAME", "mberrojalbiz"),
        "pwd": os.getenv("CCT_PASSWORD", "stratio"),
        "tenant": os.getenv("CCT_TENANT", "hsbc"),
        "gosecauth": os.getenv("CCT_GOSEC_AUTH", False),
        "sso_port": os.getenv("CCT_SSO_PORT", 9005)
    }

    def __init__(self, url=None, username=None, password=None, tenant_name=None, gosecauth=None, sso_port=None):
        requests.packages.urllib3.disable_warnings()
        if url is not None:
            self.settings.update({"url": url})
        if username is not None:
            self.settings.update({"username": username})
        if password is not None:
            self.settings.update({"pwd": password})
        if tenant_name is not None:
            self.settings.update({"tenant": tenant_name})
        if gosecauth is not None:
            self.settings.update({"gosecauth": gosecauth})
        if sso_port is not None:
            self.settings.update({"sso_port": sso_port})
        self.token = ""
        self.result = ""
        self.result_time = datetime.datetime.now()

    def enable_gosecauth(self):
        self.settings["gosecauth"] = True

    def disable_gosecauth(self):
        self.settings["gosecauth"] = False

    def set_tenant(self, tenant_name):
        self.settings["tenant"] = tenant_name

    @staticmethod
    def log(t):
        with open("/app/login_eos.log", "a+") as file:
            file.write(t + "\n")

    @staticmethod
    def error(msg, url, status):
        CCTController.log("ERROR: " + msg + " - URL: " + url + " - Status: " + str(status))
        error = dict(msg='', url='', status='')
        error['msg'] = msg
        error['url'] = url
        error['status'] = str(status)
        print("CCT_error: " + str(error))
        return error

    def run_module(self, settings):
        result = dict()
        sso_port = settings['sso_port']
        url = settings['url']
        proxy_access_url = "https://" + url
        username = settings['username']
        password = settings['pwd']
        tenant = settings['tenant']
        gosecauth = settings['gosecauth']
        status_code = [200, 201]

        CCTController.log("Received login command: " + str(settings))

        if str(sso_port) == str(0):
            gosec_sso_url = "https://" + url + "/sso"
        else:
            gosec_sso_url = "https://" + url + ":" + str(sso_port) + "/sso"

        # Setup session
        session = requests.Session()

        CCTController.log("Trying to get login page")
        try:
            r = session.get(proxy_access_url + "/login", verify=False, allow_redirects=True)
            # extract info from body
            execution, lt = CCTController._get_login_info(r)
            status = r.status_code
            CCTController.log("Received login page with status code " + str(status))
            if status not in status_code:
                CCTController.error(
                    "Server responded with status code " + str(status) + ". Success status codes are: " + str(
                        status_code),
                    proxy_access_url + "/login",
                    status)

        except Exception as e:
            CCTController.error(
                "Error getting SSO logging. Make sure your cluster is properly installed and the 'gosec-sso' service is running. " + str(
                    e), proxy_access_url + "/login", "-1")
            raise e

        # Get the dcos-auth cookie
        CCTController.log("Trying to post to '/login' to get dcos-auth token")
        try:
            r = session.post(gosec_sso_url + "/login", {
                "service": gosec_sso_url + "/oauth2.0/callbackAuthorize",
                "lt": lt,
                "_eventId": "submit",
                "execution": execution,
                "submit": "LOGIN",
                "username": username,
                "password": password,
                "tenant": tenant
            }, verify=False, allow_redirects=True)
            status = r.status_code
            CCTController.log("Received status code " + str(status) + " while trying to post to '/login'")

            # If user or pass are wrong
            if not 'dcos-acs-auth-cookie' in session.cookies:
                CCTController.error("Invalid Credentials. Please use the correct username and password and try again.",
                                    proxy_access_url + "/login", status)

            result['token'] = session.cookies['dcos-acs-auth-cookie']
            result['tgc'] = session.cookies['TGC']
            ticket_response = str(r.history[0].headers['location']).split('ticket=')[1]
            result['ticket'] = str(ticket_response)
            result['status'] = r.status_code
            # print("=====")
            # print(session.cookies.keys())
            # print(r.cookies())
            # print(result)
            # print("=*=*=*=*")
            rrs = session.get("https://admin.hsbc.stratio.com/service/dg-businessglossary-api/dictionary/user/profile",
                              headers={"X-RolesID": "SuperAdmin", "X-TenantID": "hsbc", "X-UserID": "admin"})
            # print(rrs.status_code)
            # print(rrs.text)
            # print(session.cookies.keys())
            # print(rrs.cookies)
            result['stratio-governance-auth'] = session.cookies['stratio-governance-auth']
            # print("=====")

            if status not in status_code:
                CCTController.error(
                    "Server responded with status code " + str(status) + ". Success status codes are: " + str(
                        status_code),
                    proxy_access_url + "/login",
                    status)

        except Exception as e:
            CCTController.error(
                "Error authenticating in SSO. Make sure your cluster is porperly installed and the 'gosec-sso' service is running. " + str(
                    e),
                proxy_access_url + "/login", -1)

        CCTController.log("Login in dcos successfull")

        if gosecauth:
            CCTController.log("Trying to log into gosecmanagement")
            try:
                # When getting the user cookie, gosecmanagement responds with status code 500
                status_code.append(500)
                cookies = {'dcos-acs-auth-cookie': session.cookies['dcos-acs-auth-cookie'],
                           'TGC': session.cookies['TGC']}
                if str(sso_port) == str(0):
                    r = session.post(
                        gosec_sso_url + "/login?service=https%3A%2F%2F" + url + "%2Fsso%2Foauth2.0%2FcallbackAuthorize",
                        cookies=cookies, verify=False, allow_redirects=True)
                else:
                    r = session.post(gosec_sso_url + "/login?service=https%3A%2F%2F" + url + "%3A" + str(
                        sso_port) + "%2Fsso%2Foauth2.0%2FcallbackAuthorize", cookies=cookies, verify=False,
                                     allow_redirects=True)

                ticket_response = str(r.history[0].headers['location']).split('ticket=')[1]

                r = session.post(
                    gosec_sso_url + "/oauth2.0/authorize?redirect_uri=https://" + url + "/service/gosec-management-ui/login&client_id=gosec-management-oauth-id",
                    cookies=cookies, verify=False, allow_redirects=True)
                status = r.status_code
                CCTController.log("Received status code " + str(status) + " while trying to log in /oauth2")

                test_base_url = url.split(".", maxsplit=1)[1]
                test_base_url_internal = test_base_url.replace(".stratio.com", ".int")
                if str(sso_port) == str(0):
                    url_with_sso = url
                else:
                    url_with_sso = url + ":" + str(sso_port)
                test_url = "https://" + url_with_sso + "/sso/login?service=https%3A%2F%2F" + url_with_sso + "%2Fsso%2Foauth2.0%2FcallbackAuthorize%3Fclient_id%3Dadminrouter_paas.node.eos." + test_base_url_internal + "%26redirect_uri%3Dhttps%253A%252F%252F" + url + "%252Facs%252Fapi%252Fv1%252Fauth%252Flogin%26response_type%3Dcode%26client_name%3DCasOAuthClient"
                r = session.post(test_url,
                                 cookies=cookies, verify=False, allow_redirects=True)
                # r = session.post("https://" + url + "/service/gosecmanagement/login?code=" + str(ticket_response),
                #                  cookies=cookies, verify=False, allow_redirects=True)

                status = r.status_code
                CCTController.log("Received status code " + str(status) + " while trying to log in gosecmanagement")

                if 'user' in session.cookies.keys() and str(session.cookies['user']) == "":
                    CCTController.error("User cookie could not be retrieved. Gosec Management is not responding.",
                                        "https://" + url + "/service/gosec-management-ui/", status)
                else:
                    CCTController.log("Received cookie " + str(session.cookies.get('user', '')))
                    result['user'] = str(session.cookies.get('user', ''))
                    result['token'] = session.cookies['dcos-acs-auth-cookie']
                    result['tgc'] = session.cookies['TGC']
                    result['ticket'] = str(ticket_response)
                    result['status'] = r.status_code

                if status not in status_code:
                    CCTController.error(
                        "Server responded with status code " + str(status) + ". Success status codes are: " + str(
                            status_code),
                        proxy_access_url + "/login",
                        status)

            except Exception as e:
                CCTController.error(
                    "Gosec Management is not responding. Please deploy an instance of 'gosecmanagement' or change 'gosecauth' argument to False. " + str(
                        e),
                    proxy_access_url + "/login",
                    -1)
                traceback.print_exc()

        CCTController.log("Login in goscemanagement successfull")
        # in the event of a successful module execution, you will want to
        # simple AnsibleModule.exit_json(), passing the key/value results
        return result

    def main(self, settings=None):
        if settings is None:
            settings = self.settings
        CCTController.log("------------------------")
        CCTController.log("Starting login process")
        result = self.run_module(settings)
        # print(result)
        # print("Token:")
        # print(result['token'])
        CCTController.log("End of login process")
        self.token = result['token']
        self.result = result
        self.result_time = datetime.datetime.now()
        return result['token'], result

    @staticmethod
    def _get_login_info(r):
        body = r.content.decode("UTF-8")
        lt_left_match = "name=\"lt\" value=\""
        lt1 = body.index(lt_left_match)
        prelt = body[lt1 + len(lt_left_match):]
        lt = prelt[:prelt.index("\" />")].strip()
        execution_left_match = "name=\"execution\" value=\""
        execution1 = body.index(execution_left_match)
        execution = body[execution1 + len("name=\"execution\" value=\""):].split("\"")[0]

        return execution, lt

    def refresh_login(self, settings=None, renew_cookies=True):
        d2 = datetime.datetime.now()
        time_delta = d2 - self.result_time
        total_seconds = time_delta.total_seconds()
        minutes = total_seconds / 60
        settings_to_use = self.settings.copy()
        if settings is not None:
            settings_to_use = settings
        if renew_cookies or minutes >= 120:
            self.main(settings_to_use)

    def get_cookies(self, renew_cookies=True):
        self.refresh_login(self.settings, renew_cookies)
        token = self.token
        result = self.result
        return {
            "dcos-acs-auth-cookie": token,
            "stratio-governance-auth": result.get('stratio-governance-auth', '')}

    def send_get(self, url, params=None, tenant_name=None, gosecauth=None, username=None, password=None,
                 fill_url=False, headers=None, ocookies=None, renew_cookies=True):
        settings = self.settings.copy()
        if tenant_name is not None:
            settings["tenant"] = tenant_name
        if gosecauth is not None:
            settings["gosecauth"] = gosecauth
        if username is not None:
            settings["username"] = username
        if password is not None:
            settings["pwd"] = password
        self.refresh_login(settings, renew_cookies)
        token = self.token
        result = self.result
        cookies = {
            "dcos-acs-auth-cookie": token,
            "stratio-governance-auth": result.get('stratio-governance-auth', '')}
        if ocookies is not None:
            cookies = ocookies
        if fill_url:
            url = url.replace("${url}", self.settings["url"])
        if headers is not None:
            # print(cookies)
            # print(headers)
            r = requests.get(url, params=params, cookies=cookies, verify=False, headers=headers)
        else:
            r = requests.get(url, params=params, cookies=cookies, verify=False)
        # print(r.text)
        return r

    def send_post(self, url, data=None, djson=None, tenant_name=None, gosecauth=None, username=None, password=None,
                  fill_url=False, ocookies=None, renew_cookies=True):
        settings = self.settings.copy()
        if tenant_name is not None:
            settings["tenant"] = tenant_name
        if gosecauth is not None:
            settings["gosecauth"] = gosecauth
        if username is not None:
            settings["username"] = username
        if password is not None:
            settings["pwd"] = password
        self.refresh_login(settings, renew_cookies)
        token = self.token
        result = self.result
        cookies = {
            "dcos-acs-auth-cookie": token,
            "stratio-governance-auth": result.get('stratio-governance-auth', '')}
        if ocookies is not None:
            cookies = ocookies
        if fill_url:
            url = url.replace("${url}", self.settings["url"])
        r = requests.post(url, data=data, json=djson, cookies=cookies, verify=False)
        # print(r.text)
        return r

    def send_put(self, url, data=None, djson=None, headers=None, tenant_name=None, gosecauth=None, username=None,
                 password=None, fill_url=False, ocookies=None, renew_cookies=True):
        settings = self.settings.copy()
        if tenant_name is not None:
            settings["tenant"] = tenant_name
        if gosecauth is not None:
            settings["gosecauth"] = gosecauth
        if username is not None:
            settings["username"] = username
        if password is not None:
            settings["pwd"] = password
        self.refresh_login(settings, renew_cookies)
        token = self.token
        result = self.result
        cookies = {
            "dcos-acs-auth-cookie": token,
            "stratio-governance-auth": result.get('stratio-governance-auth', '')}
        if ocookies is not None:
            cookies = ocookies
        if fill_url:
            url = url.replace("${url}", self.settings["url"])
        r = requests.put(url, data=data, json=djson, cookies=cookies, headers=headers, verify=False)
        # print(r.text)
        return r

    def send_delete(self, url, headers=None, tenant_name=None, gosecauth=None, username=None, password=None,
                    fill_url=False, ocookies=None, renew_cookies=True):
        settings = self.settings.copy()
        if tenant_name is not None:
            settings["tenant"] = tenant_name
        if gosecauth is not None:
            settings["gosecauth"] = gosecauth
        if username is not None:
            settings["username"] = username
        if password is not None:
            settings["pwd"] = password
        self.refresh_login(settings, renew_cookies)
        token = self.token
        result = self.result
        cookies = {
            "dcos-acs-auth-cookie": token,
            "stratio-governance-auth": result.get('stratio-governance-auth', '')}
        if ocookies is not None:
            cookies = ocookies
        if fill_url:
            url = url.replace("${url}", self.settings["url"])
        r = requests.delete(url, cookies=cookies, headers=headers, verify=False)
        # print(r.text)
        return r

    def does_tenant_have_services(self, tenant_name):
        # https://admin.stratio-eyctp.com/marathon/v2/groups/bdl-demo/apps
        url = "https://{0}/marathon/v2/groups/{1}/apps"
        try:
            r = self.send_get(
                url.format(self.settings["url"], tenant_name))
            if r.status_code == 200:
                apps = json.loads(r.text)
                if type(apps) is list and len(apps) > 0:
                    return True
        except Exception as e:
            print("Exception in does_tenant_have_services Tenant: {} Exception: {}".format(tenant_name, str(e)))
            pass
        return False
