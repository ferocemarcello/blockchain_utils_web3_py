import os
import time

import pytest
import yaml
from pathlib import Path
import pandas as pd
from lemlab.db_connection import db_connection, db_param
from lemlab.platform import lem_blockchain, lem_settings
from lemlab.platform.lem_blockchain_test_sort import checkEqualSortedOffersBids_blockchain_db

offers_blockchain, bids_blockchain = None, None
offers_db, bids_db = None, None


@pytest.fixture(scope="session", autouse=True)
def setUp():
    yaml_file = os.path.join(str(Path(__file__).parent.parent.parent), "lem_analysis", "sim_config_blockchain.yaml")
    global offers_blockchain, bids_blockchain, offers_db, bids_db, user_infos_blockchain, user_infos_db, id_meters_blockchain, id_meters_db
    # load configuration file
    with open(yaml_file) as config_file:
        sim_config = yaml.load(config_file, Loader=yaml.FullLoader)
    # Create a db connection object
    db_obj = db_connection.DatabaseConnection(db_dict=sim_config['database'])
    # Read offers and bids from db
    bids_db, offers_db = db_obj.get_bids_offers_market()
    db_obj.end_connection()
    print('Market contains', str(len(offers_db)), 'valid offers and', str(len(bids_db)), 'valid bids.')
    # Jump to end of function if offers or bids are empty
    if offers_db.empty or bids_db.empty:
        raise Exception(pd.Timestamp(time.time(), unit="s", tz="Europe/Berlin"),
                        ': All offers and/or bids are empty. No clearing possible')
    lem_blockchain.setUpBlockchain()
    offers_blockchain = lem_blockchain.getOffers_or_Bids(isOffer=True)
    bids_blockchain = lem_blockchain.getOffers_or_Bids(isOffer=False)


def test_filtering_on_ts_delivery():
    t_now = round(time.time())
    market_horizon = lem_settings.horizon_market
    # Calculate number of market clearings
    n_clearings = int(market_horizon / lem_settings.interval_clearing)
    print("n_clearings: " + str(n_clearings))
    t_clearing_first = t_now - (t_now % lem_settings.interval_clearing) + lem_settings.interval_clearing
    for i in range(n_clearings):
        print("i: " + str(i))

        t_clearing_current = t_clearing_first + lem_settings.interval_clearing * i
        print("t_clearing_current: " + str(t_clearing_current))

        curr_clearing_offers_db = offers_db[offers_db[db_param.TS_DELIVERY] == t_clearing_current]
        curr_clearing_bids_db = bids_db[bids_db[db_param.TS_DELIVERY] == t_clearing_current]

        curr_clearing_offers_db = curr_clearing_offers_db.sort_values(
            by=[db_param.PRICE_ENERGY, db_param.QUALITY_ENERGY],
            ascending=[True, False])
        curr_clearing_bids_db = curr_clearing_bids_db.sort_values(by=[db_param.PRICE_ENERGY, db_param.QUALITY_ENERGY],
                                                                  ascending=[False, False])

        curr_clearing_offers_db, curr_clearing_bids_db = lem_blockchain.manipulateDataFrames(
            curr_clearing_offers_db, curr_clearing_bids_db)

        print("len curr_clearing_offers_db: " + str(len(curr_clearing_offers_db)))
        print("len curr_clearing_bids_db: " + str(len(curr_clearing_bids_db)))
        curr_clearing_offers_blockchain, curr_clearing_bids_blockchain = lem_blockchain.functions.filter_sort_OffersBids_memory_two_keys(
            t_clearing_current).call()

        assert set(curr_clearing_offers_blockchain) == set(curr_clearing_offers_db)
        assert set(curr_clearing_bids_blockchain) == set(curr_clearing_bids_db)

        samesorted_offers = checkEqualSortedOffersBids_blockchain_db(curr_clearing_offers_blockchain,
                                                                     curr_clearing_offers_db, 3)
        samesorted_bids = checkEqualSortedOffersBids_blockchain_db(curr_clearing_bids_blockchain,
                                                                   curr_clearing_bids_db, 3)
        assert samesorted_offers and samesorted_bids
        # assert curr_clearing_offers_db == curr_clearing_offers_blockchain
        # assert curr_clearing_bids_db == curr_clearing_bids_blockchain
