import os
import pytest
from pathlib import Path
import pandas as pd
import time
from lemlab.db_connection import db_connection, db_param

import yaml

from lemlab.platform import lem_blockchain

offers_blockchain, bids_blockchain = None, None
offers_db, bids_db = None, None


@pytest.fixture(scope="session", autouse=True)
def setUp():
    global offers_blockchain, bids_blockchain, offers_db, bids_db
    # load configuration file
    yaml_file = os.path.join(str(Path(__file__).parent.parent.parent), "lem_analysis", "sim_config_blockchain.yaml")
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


def test_sorting():
    start = time.time()
    sorted_offers_db = offers_db.sort_values(by=[db_param.PRICE_ENERGY, db_param.QUALITY_ENERGY],
                                             ascending=[True, False])
    end = time.time()
    print("offers_python_sorted done in " + str(end - start) + "seconds")
    start = time.time()
    sorted_bids_db = bids_db.sort_values(by=[db_param.PRICE_ENERGY, db_param.QUALITY_ENERGY], ascending=[True, False])
    end = time.time()
    print("bids_python_sorted done in " + str(end - start) + "seconds")
    sorted_offers_db_list, sorted_bids_db_list = lem_blockchain.manipulateDataFrames(sorted_offers_db,
                                                                                     sorted_bids_db)

    lem_blockchain.setUpBlockchain(contract_name="Sorting")

    start = time.time()
    offers_blockchain_sorted = lem_blockchain.functions.insertionSortOffersBidsPrice_Quality(offers_blockchain,
                                                                                             True, False).call()
    end = time.time()
    print("offers_blockchain_sorted done in " + str(end - start) + "seconds")

    start = time.time()
    bids_blockchain_sorted = lem_blockchain.functions.insertionSortOffersBidsPrice_Quality(bids_blockchain, True,
                                                                                           False).call()
    end = time.time()
    print("bids_blockchain_sorted done in " + str(end - start) + "seconds")

    prices_offers_blockchain = [x[3] for x in offers_blockchain_sorted]
    prices_bids_blockchain = [x[3] for x in bids_blockchain_sorted]

    prices_db_offers = [x[3] for x in sorted_offers_db_list]  # ts_deliveries
    prices_db_bids = [x[3] for x in sorted_bids_db_list]  # ts_deliveries

    assert sorted(prices_offers_blockchain) == prices_offers_blockchain and sorted(
        prices_bids_blockchain) == prices_bids_blockchain  # assert that the offers and lists are sorted on the blockchain
    assert prices_db_offers == prices_offers_blockchain and prices_db_bids == prices_bids_blockchain  # assert that the lists of ts_deliveries are in the same order on db and blockchain

    # control if the two lists are sorted in the same way and with the same values
    # special control needed since some entries have the same values, but different order because they share the same ts_delivery
    samesorted_offers = checkEqualSortedOffersBids_blockchain_db(offers_blockchain_sorted, sorted_offers_db_list, 3)
    samesorted_bids = checkEqualSortedOffersBids_blockchain_db(bids_blockchain_sorted, sorted_bids_db_list, 3)

    # assert that the offers and bids are in the same order on db and blockchain, and they have the same values
    assert samesorted_offers and samesorted_bids
    # assert offers_blockchain_sorted == sorted_offers_db_list and bids_blockchain_sorted == sorted_bids_db_list


def checkEqualSortedOffersBids_blockchain_db(ofbids_left, ofbids_right, index_param):
    if ofbids_right == ofbids_left: return True
    if len(ofbids_right) != len(ofbids_left): return False

    i = 0
    while i < len(ofbids_right):
        if (ofbids_right[i]) != (ofbids_left[i]):  # if entries at same index are not equal
            if ofbids_right[i][index_param] != ofbids_left[i][index_param]:  # if they dont have the same ts_delivery
                return False
            else:
                # all the entries from the db with the same ts_delivery
                same_ts_delivery_db = [entry for entry in ofbids_right if
                                       entry[index_param] == ofbids_right[i][index_param]]
                # all the entries from the blockchain with the same ts_delivery
                same_ts_delivery_blockchain = [entry for entry in ofbids_left if
                                               entry[index_param] == ofbids_left[i][index_param]]
                # if the entries with the same ts_delivery are not the same, regardless of the order
                if set(same_ts_delivery_blockchain) != set(same_ts_delivery_db): return False
                # increment i to the next entry with different ts_delivery
                i += len(same_ts_delivery_db) - 1
        i += 1
    return True
