import requests
import time

from common import HTTP_API_ROOT


def get_predictors_list(company_id=None):
    headers = {}
    if company_id is not None:
        headers['company-id'] = f'{company_id}'
    res = requests.get(f'{HTTP_API_ROOT}/predictors/', headers=headers)
    assert res.status_code == 200
    return res.json()


def get_predictors_names_list(company_id=None):
    predictors = get_predictors_list(company_id=company_id)
    return [x['name'] for x in predictors]


def check_predictor_exists(name):
    assert name in get_predictors_names_list()


def check_predictor_not_exists(name):
    assert name not in get_predictors_names_list()


def get_predictor_data(name):
    predictors = get_predictors_list()
    for p in predictors:
        if p['name'] == name:
            return p
    return None


def get_datasources_names(company_id=None):
    headers = {}
    if company_id is not None:
        headers['company-id'] = f'{company_id}'
    res = requests.get(f'{HTTP_API_ROOT}/datasources', headers=headers)
    assert res.status_code == 200
    return [x['name'] for x in res.json()]


def check_ds_not_exists(ds_name):
    res = requests.get(f'{HTTP_API_ROOT}/datasources')
    assert res.status_code == 200
    ds_names = [x['name'] for x in res.json()]
    assert ds_name not in ds_names


def check_ds_exists(ds_name):
    res = requests.get(f'{HTTP_API_ROOT}/datasources')
    assert res.status_code == 200
    ds_names = [x['name'] for x in res.json()]
    assert ds_name in ds_names


def check_ds_analyzable(ds_name):
    start_time = time.time()
    analyze_done = False
    while analyze_done is False and (time.time() - start_time) < 30:
        res = requests.get(f'{HTTP_API_ROOT}/datasources/{ds_name}/analyze')
        assert res.status_code == 200
        analyze_done = res.json().get('status', '') != 'analyzing'
        time.sleep(1)
    assert analyze_done


def wait_predictor_learn(predictor_name):
    start_time = time.time()
    learn_done = False
    while learn_done is False and (time.time() - start_time) < 180:
        learn_done = get_predictor_data(predictor_name)['status'] == 'complete'
        time.sleep(1)
    assert learn_done


def get_integrations_names(company_id=None):
    headers = {}
    if company_id is not None:
        headers['company-id'] = f'{company_id}'
    res = requests.get(f'{HTTP_API_ROOT}/config/integrations', headers=headers)
    assert res.status_code == 200
    return res.json()['integrations']
