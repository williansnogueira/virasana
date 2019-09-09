import json
import time
from selenium import webdriver

GECKO_PATH = "D:\\Users\\25052288840\\Downloads\\chromedriver.exe"

SUITE_URL = "https://www.suiterfb.receita.fazenda"
POS_ACD_URL = "https://portalunico.suiterfb.receita.fazenda/cct/api/deposito-carga/consultar-estoque-pos-acd?numeroConteiner="
DUE_ITEMS_URL = "https://portalunico.suiterfb.receita.fazenda/due/api/due/obterDueComItensResumidos?due="


def auth_suite_rfb(driver, portal_url=SUITE_URL):
    driver.get(portal_url)
    time.sleep(1)
    LOGON_REDIRECT = "/camweb/grupo?sis=SUITERFB&url=www.suiterfb.receita.fazenda/api/private/redirect"
    driver.get(portal_url + LOGON_REDIRECT)
    time.sleep(1)
    form = driver.find_element_by_name("loginCertForm")
    form.submit()
    time.sleep(2)


def limpa_pre(page_source: str):
    inicio = page_source.find('">{"')
    fim = page_source.find('</pre>')
    return page_source[inicio + 2:fim]


def get_dues_json_pos_acd(page_source: str):
    json_pos_acd = json.loads(limpa_pre(page_source))
    lista_due = json_pos_acd['lista']
    result = []
    for item in lista_due:
        result.append(item['numeroDUE'])
    return result


def get_dues_pos_acd(driver, conteineres, pos_acd_url=POS_ACD_URL):
    conteineres_listadue = {}
    for conteiner in conteineres:
        driver.get(pos_acd_url + conteiner)
        pos_acd = driver.page_source
        conteineres_listadue[conteiner] = get_dues_json_pos_acd(pos_acd)
    return conteineres_listadue


def get_dues_json_due(page_source: str):
    json_due = json.loads(limpa_pre(page_source))
    result = json_due
    return result


def detalha_dues(driver, conteineres_listadue, due_items_url=DUE_ITEMS_URL):
    conteineres_due = {}
    for conteiner, dues in conteineres_listadue.items():
        for due in dues:
            if due and isinstance(due, str):
                driver.get(due_items_url + due)
            due_page = driver.page_source
            conteineres_due[conteiner] = get_dues_json_due(due_page)
    return conteineres_due


if __name__ == '__main__':
    import os

    print(GECKO_PATH)
    print(os.path.exists(GECKO_PATH))
    driver = webdriver.Chrome(GECKO_PATH)
    auth_suite_rfb(driver)
    conteineres = ['MSCU6656780']
    conteineres_listadue = get_dues_pos_acd(driver, conteineres)
    print(conteineres_listadue)
    conteineres_due = detalha_dues(driver, conteineres_listadue)
    print(conteineres_due)
    driver.close()
