import os
import pytest
import subprocess
from pathlib import Path
import pandas as pd
import time
from lemlab.db_connection import db_connection

import yaml

from lemlab.platform import lem_blockchain

generate_bids_offer = True

offers_blockchain, bids_blockchain = None, None
offers_db, bids_db = None, None

user_infos_blockchain, user_infos_db = None, None
id_meters_blockchain, id_meters_db = None, None


@pytest.fixture(scope="session", autouse=True)
def setUp():
    yaml_file = os.path.join(str(Path(__file__).parent.parent.parent), "lem_analysis", "sim_config_blockchain.yaml")
    if generate_bids_offer:
        project_dir = str(Path(__file__).parent.parent.parent)
        script_path = os.path.join(project_dir, 'lem_analysis',
                                   'lem_simulation_blockchain.py ' + yaml_file)
        batcmd = 'python ' + script_path

        p = subprocess.Popen(batcmd, stdout=subprocess.PIPE, bufsize=1)
        for line in iter(p.stdout.readline, b''):
            print(line.decode("utf-8"))  # for live output
        p.stdout.close()
        p.wait()

        time.sleep(20)

    global offers_blockchain, bids_blockchain, offers_db, bids_db, user_infos_blockchain, user_infos_db, id_meters_blockchain, id_meters_db
    # load configuration file
    with open(yaml_file) as config_file:
        sim_config = yaml.load(config_file, Loader=yaml.FullLoader)
    # Create a db connection object
    db_obj = db_connection.DatabaseConnection(db_dict=sim_config['database'])
    # Read offers and bids from db
    bids_db, offers_db = db_obj.get_bids_offers_market()
    user_infos_db, id_meters_db = [db_obj.get_info_user(user_id) for user_id in
                                   db_obj.get_list_all_users()], db_obj.get_info_meter()
    db_obj.end_connection()
    print('Market contains', str(len(offers_db)), 'valid offers and', str(len(bids_db)), 'valid bids.')
    # Jump to end of function if offers or bids are empty
    if offers_db.empty or bids_db.empty:
        raise Exception(pd.Timestamp(time.time(), unit="s", tz="Europe/Berlin"),
                        ': All offers and/or bids are empty. No clearing possible')

    lem_blockchain.setUpBlockchain(timeout=180)
    offers_blockchain, bids_blockchain = lem_blockchain.getOffers_or_Bids(
        isOffer=True), lem_blockchain.getOffers_or_Bids(isOffer=False)
    user_infos_blockchain, id_meters_blockchain = lem_blockchain.functions.get_user_infos().call(), lem_blockchain.functions.get_id_meters().call()


def test_equals_offers_bids():
    offs_db, bds_db = lem_blockchain.manipulateDataFrames(offers_db, bids_db)

    '''for x in offs_db + bds_db + offers_blockchain + bids_blockchain:
        assert x[5] == x[6]  # t_submission=ts_delivery'''

    assert len(offs_db) == len(offers_blockchain)
    assert len(bds_db) == len(bids_blockchain)

    assert set(offs_db) == set(offers_blockchain)
    assert set(bds_db) == set(bids_blockchain)


def test_equals_user_infos():
    assert len(user_infos_blockchain) == len(user_infos_db)

    cols_user_info = ['id_user', 'balance_account', 't_update_balance',
                      'price_energy_bid_max', 'price_energy_offer_min', 'ts_delivery_first', 'ts_delivery_last']
    price_multiplier = 10000
    for i in range(len(user_infos_db)):
        user_infos_db[i] = user_infos_db[i][cols_user_info]
        for col in ["price_energy_bid_max", "price_energy_offer_min", "balance_account"]:
            user_infos_db[i][col] = price_multiplier * user_infos_db[i][col]
            user_infos_db[i][col] = user_infos_db[i][col].apply(round)

    user_infos_db_list = [tuple(user_infos_db[i].values.tolist()[0]) for i in range(len(user_infos_db))]

    assert set(user_infos_blockchain) == set(user_infos_db_list)


def test_equals_id_meters():
    global id_meters_db
    assert len(id_meters_blockchain) == len(id_meters_db)

    cols_id_meter = ['id_meter', 'id_user', 'type_meter', 'id_meter_main',
                     'id_aggregator', 'quality_energy', 'ts_delivery_first', 'ts_delivery_last', 'info_additional']
    id_meters_db = id_meters_db[cols_id_meter]

    id_meters_db_list = [tuple(x) for x in id_meters_db.values.tolist()]

    assert set(id_meters_blockchain) == set(id_meters_db_list)
