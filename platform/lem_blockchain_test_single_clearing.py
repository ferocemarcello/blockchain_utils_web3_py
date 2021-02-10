import os
import pytest
import yaml
import pandas as pd
import time
from pathlib import Path
import subprocess

from lemlab.db_connection import db_connection, db_param
from lemlab.platform import lem_blockchain, lem_settings, lem

temp_market_result_columns = ['id_user_offer', 'qty_energy_offer', 'price_energy_offer', 'quality_energy_offer',
                              'type_offer', 'number_offer', 'status_offer', 't_submission_offer', 'ts_delivery',
                              'id_user_bid', 'qty_energy_bid', 'price_energy_bid', 'quality_energy_bid', 'type_bid',
                              'number_bid', 'status_bid', 't_submission_bid',
                              'qty_energy_traded', 'price_energy_cleared_uniform',
                              'price_energy_cleared_discriminative']
db = 'MF_Test'
num_off_bids = 1000
generate_bids_offer = False
verbose = False

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


def test_clearings():
    t_now = round(time.time())
    market_horizon = lem_settings.horizon_market
    # Calculate number of market clearings
    n_clearings = int(market_horizon / lem_settings.interval_clearing)
    print("n_clearings: " + str(n_clearings))
    t_clearing_first = t_now - (t_now % lem_settings.interval_clearing) + lem_settings.interval_clearing

    supplier_bids = False
    uniform_pricing = True
    discriminative_pricing = False

    for i in range(n_clearings):
        print("i: " + str(i))
        t_clearing_current = t_clearing_first + lem_settings.interval_clearing * i
        # t_clearing_current = 1609272900
        print("t_clearing_current: " + str(t_clearing_current))

        if i == 0 and supplier_bids:
            add_supplier_bids = True
        else:
            add_supplier_bids = False

        odb, bdb = get_filtered_sorted_offers_db(t_clearing_current, add_supplier_bids)

        bids_offers_cleared_python, offers_uncleared_python, bids_uncleared_python, \
        offers_cleared_python, bids_cleared_python = \
            lem.clearing_standard(odb, bdb, pricing_uniform=uniform_pricing,
                                  pricing_discriminative=discriminative_pricing)
        tx_hash = lem_blockchain.functions.single_clearing(t_clearing_current, False, uniform_pricing,
                                                           discriminative_pricing, t_clearing_current, True, False, False).transact(
            {'from': lem_blockchain.coinbase})

        lem_blockchain.web3_instance.eth.waitForTransactionReceipt(tx_hash)
        if verbose:
            log = lem_blockchain.getLog(tx_hash=tx_hash)
            print(log)

        temp_market_results_blockchain = lem_blockchain.functions.getTempMarketResults().call()
        assert len(bids_offers_cleared_python) == len(temp_market_results_blockchain)
        if len(bids_offers_cleared_python) > 0 and len(temp_market_results_blockchain) > 0:
            temp_market_results_blockchain = reformat_single_clearing_results(temp_market_results_blockchain,
                                                                              uniform_pricing, discriminative_pricing)
            df_results_blockchain = prepare_df_single_clearing(temp_market_results_blockchain,
                                                               bids_offers_cleared_python)
            try:
                pd.testing.assert_frame_equal(df_results_blockchain, bids_offers_cleared_python, check_exact=False,
                                              check_names=False, atol=1.0e-4)
                assert True
            except AssertionError:
                assert False

def reformat_single_clearing_results(temp_market_results_blockchain, uniform_pricing, discriminative_pricing):
    temp_market_results_blockchain = [list(x) for x in temp_market_results_blockchain]

    if not uniform_pricing and not discriminative_pricing:
        temp_market_results_blockchain = [x[:-3] + [x[-1]] for x in temp_market_results_blockchain]
    elif uniform_pricing and not discriminative_pricing:
        temp_market_results_blockchain = [x[:-2] + [x[-1]] for x in temp_market_results_blockchain]
    elif discriminative_pricing and not uniform_pricing:
        temp_market_results_blockchain = [x[:-3] + x[-2:] for x in temp_market_results_blockchain]

    return temp_market_results_blockchain

def prepare_df_single_clearing(temp_market_results_blockchain, bids_offers_cleared_python):
    df_results_blockchain = lem_blockchain.convertToPdDataFrame(temp_market_results_blockchain,
                                                                bids_offers_cleared_python.columns.to_list())
    if len(df_results_blockchain) > 0:
        columns_to_float = (
            bids_offers_cleared_python.dtypes[bids_offers_cleared_python.dtypes == "float64"]).index.to_list()
        columns_to_int = (
            bids_offers_cleared_python.dtypes[bids_offers_cleared_python.dtypes == "int64"]).index.to_list()
        columns_to_divide = [x for x in bids_offers_cleared_python.columns if "price" in x]
        df_results_blockchain = lem_blockchain.df_convert_data_types(df_results_blockchain,
                                                                     columns_to_float,
                                                                     columns_to_int, columns_to_divide)
        df_results_blockchain.set_index(df_results_blockchain['qty_energy_traded'].cumsum().astype('int'),
                                        inplace=True, drop=False)
    return df_results_blockchain


def get_filtered_sorted_offers_db(t_clearing_current, add_supplier_bids):
    curr_clearing_offers_db = offers_db[offers_db[db_param.TS_DELIVERY] == t_clearing_current]
    curr_clearing_bids_db = bids_db[bids_db[db_param.TS_DELIVERY] == t_clearing_current]

    if add_supplier_bids:
        curr_clearing_bids_db, curr_clearing_offers_db = lem.add_supplier_bids(t_clearing_current,
                                                                               curr_clearing_bids_db,
                                                                               curr_clearing_offers_db)

    curr_clearing_offers_db = curr_clearing_offers_db.sort_values(by=[db_param.PRICE_ENERGY, db_param.QTY_ENERGY],
                                                                  ascending=[True, False])
    curr_clearing_bids_db = curr_clearing_bids_db.sort_values(by=[db_param.PRICE_ENERGY, db_param.QTY_ENERGY],
                                                              ascending=[False, False])

    return curr_clearing_offers_db, curr_clearing_bids_db
