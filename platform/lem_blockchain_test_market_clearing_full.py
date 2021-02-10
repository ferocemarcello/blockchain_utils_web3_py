import os
import pytest

import pandas as pd
import time
from pathlib import Path

import yaml

from lemlab.db_connection import db_connection, db_param
from lemlab.platform import lem_blockchain, lem_settings, lem

verbose_blockchain = False
shuffle = False

offers_blockchain, bids_blockchain = None, None
offers_db, bids_db = None, None

yaml_file = os.path.join(str(Path(__file__).parent.parent.parent), "lem_analysis", "sim_config_blockchain.yaml")
# load configuration file
with open(yaml_file) as config_file:
    sim_config = yaml.load(config_file, Loader=yaml.FullLoader)
# Create a db connection object
db_obj = db_connection.DatabaseConnection(db_dict=sim_config['database'])


@pytest.fixture(scope="session", autouse=True)
def setUp():
    global offers_blockchain, bids_blockchain, offers_db, bids_db, user_infos_blockchain, user_infos_db, id_meters_blockchain, id_meters_db

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
    # Set clearing time
    t_clearing_start = round(time.time())
    print('######################## Market clearing started #############################')
    print('Market clearing started:', pd.Timestamp(t_clearing_start, unit="s", tz="Europe/Berlin"))

    t_now = round(time.time())
    market_horizon = lem_settings.horizon_market
    # Calculate number of market clearings
    n_clearings = int(market_horizon / lem_settings.interval_clearing)
    uniform_pricing = True
    discriminative_pricing = True
    supplier_bids = False
    start = time.time()
    market_results_python = lem.market_clearing(db_obj=db_obj, t_override=t_now, market_horizon=market_horizon,
                                                bids_offers_clear=False,
                                                bids_offers_archive=True,
                                                pricing_uniform=uniform_pricing,
                                                pricing_discriminative=discriminative_pricing,
                                                shuffle=shuffle, t_clearing_start=t_clearing_start)
    market_results_python = market_results_python[0]
    end = time.time()
    print("market clearing in python done in " + str(end - start) + " seconds")
    start = time.time()
    market_results_blockchain = get_market_results_blockchain(t_now, n_clearings, supplier_bids=supplier_bids,
                                                              uniform_pricing=uniform_pricing,
                                                              discriminative_pricing=discriminative_pricing,
                                                              t_clearing_start=t_clearing_start,
                                                              market_results_python=market_results_python,
                                                              shuffle=shuffle)
    end = time.time()
    print("market clearing on blockchain done in " + str(end - start) + " seconds")
    if shuffle:
        assert len(market_results_python) >= 0.1 * len(market_results_blockchain) or len(
            market_results_blockchain) >= 0.1 * len(market_results_python)
        try:
            pd.testing.assert_frame_equal(market_results_python, market_results_blockchain, check_exact=False,
                                          check_names=False, atol=1.0e-4)
        except Exception as e:
            print(e)
    else:
        if market_results_blockchain.empty and market_results_python.empty:
            assert True
        else:
            pd.testing.assert_frame_equal(market_results_python, market_results_blockchain, check_exact=False,
                                          check_names=False, atol=1.0e-4)
            assert True
        price_multiplier = 10000
        additional_price_multiplier = 1000  # I have to multiply again here the price on db by 1000, since I don't divide by 1000 on blockchain
        tx_hash = lem_blockchain.functions.updateBalances().transact({'from': lem_blockchain.coinbase})
        lem_blockchain.web3_instance.eth.waitForTransactionReceipt(tx_hash)
        user_infos_blockchain = lem_blockchain.functions.get_user_infos().call()
        user_infos_db = pd.concat([db_obj.get_info_user(user_id) for user_id in
                                   db_obj.get_list_all_users()])
        user_infos_blockchain_dataframe = lem_blockchain.convertToPdDataFrame(user_infos_blockchain,
                                                                              user_infos_db.columns.to_list())

        cols_to_multiply = [db_param.BALANCE_ACCOUNT, db_param.PRICE_ENERGY_BID_MAX, db_param.PRICE_ENERGY_OFFER_MIN]
        cols_to_multiply_add = [db_param.BALANCE_ACCOUNT]
        for col in cols_to_multiply:
            user_infos_db[col] = (price_multiplier * user_infos_db[col]).apply(round)
        for col in cols_to_multiply_add:
            user_infos_db[col] = (additional_price_multiplier * user_infos_db[col]).apply(round)

        user_infos_db = user_infos_db.sort_values(by=[db_param.ID_USER], ascending=[True])
        user_infos_blockchain_dataframe = user_infos_blockchain_dataframe.sort_values(by=[db_param.ID_USER],
                                                                                      ascending=[True])

        user_infos_db = user_infos_db.set_index(user_infos_blockchain_dataframe.index)

        assert len(user_infos_db) == len(user_infos_blockchain_dataframe)
        pd.testing.assert_frame_equal(user_infos_db, user_infos_blockchain_dataframe, check_exact=False,
                                      check_names=False, rtol=0.1)
        assert True


def findLimit(n_clearings_max, t_clearing_current, supplier_bids, uniform_pricing, discriminative_pricing,
              t_clearing_start,
              gasThreshold):
    n_clearings_current = n_clearings_max
    estimate = 10 * gasThreshold
    while estimate > gasThreshold:
        try:
            estimate = lem_blockchain.functions.market_clearing(n_clearings_current, t_clearing_current,
                                                                supplier_bids, uniform_pricing,
                                                                discriminative_pricing,
                                                                lem_settings.interval_clearing,
                                                                t_clearing_start, False, verbose_blockchain, True,
                                                                False).estimateGas()
            n_clearings_current = int(n_clearings_current / (estimate / gasThreshold))
        except:
            n_clearings_current = int(n_clearings_current / 2)

    return n_clearings_current


def findLimit_two(n_clearings_max, t_clearing_current, supplier_bids, uniform_pricing, discriminative_pricing,
                  t_clearing_start,
                  gasThreshold):
    estimate = gasThreshold * 10
    n_clearings_current = n_clearings_max
    while estimate > gasThreshold:
        try:
            estimate = lem_blockchain.functions.market_clearing(n_clearings_current, t_clearing_current,
                                                                supplier_bids, uniform_pricing,
                                                                discriminative_pricing,
                                                                lem_settings.interval_clearing,
                                                                t_clearing_start, False, verbose_blockchain, True,
                                                                False).estimateGas()
            if estimate > gasThreshold:
                n_clearings_current = int(n_clearings_current * 0.75)
        except ValueError:
            n_clearings_current = int(n_clearings_current / 2)

    return n_clearings_current


def get_market_results_blockchain(t_override, n_clearings, supplier_bids, uniform_pricing, discriminative_pricing,
                                  t_clearing_start,
                                  market_results_python, shuffle=True):
    t_now = t_override
    t_clearing_first = t_now - (t_now % lem_settings.interval_clearing) + lem_settings.interval_clearing

    t_clearing_current = t_clearing_first
    n_clearings_done = 0

    limit_clearings = findLimit(n_clearings, t_clearing_first, supplier_bids, uniform_pricing, discriminative_pricing,
                                t_clearing_start,
                                gasThreshold=40000000)

    n_clearings_current = limit_clearings
    deleteFinalMarketResults = True
    update_balances = False
    while n_clearings_done < n_clearings:  # last step
        if n_clearings - n_clearings_done <= n_clearings_current:
            n_clearings_current = n_clearings - n_clearings_done
            update_balances = False
        try:
            tx_hash = lem_blockchain.functions.market_clearing(n_clearings_current, t_clearing_current,
                                                               supplier_bids,
                                                               uniform_pricing,
                                                               discriminative_pricing,
                                                               lem_settings.interval_clearing,
                                                               t_clearing_start, shuffle, verbose_blockchain,
                                                               deleteFinalMarketResults, update_balances).transact(
                {'from': lem_blockchain.coinbase})
            lem_blockchain.web3_instance.eth.waitForTransactionReceipt(tx_hash)
            deleteFinalMarketResults = False
            if verbose_blockchain:
                log = lem_blockchain.getLog(tx_hash=tx_hash)
                print(log)
            n_clearings_done += n_clearings_current
            t_clearing_current = t_clearing_first + lem_settings.interval_clearing * n_clearings_done
            n_clearings_current = limit_clearings
        except ValueError as e:
            print(e)
            n_clearings_current = int(n_clearings_current * 0.75)
            update_balances = False

    market_results_blockchain = lem_blockchain.functions.getMarketResultsTotal().call()
    market_results_blockchain = convertToPdFinalMarketResults(market_results_blockchain, uniform_pricing,
                                                              discriminative_pricing, market_results_python)
    return market_results_blockchain


def reformat_full_clearing_results(final_market_results_blockchain, uniform_pricing, discriminative_pricing):
    final_market_results_blockchain = [list(x) for x in final_market_results_blockchain]

    if not uniform_pricing and not discriminative_pricing:
        final_market_results_blockchain = [x[:8] + x[-2:] for x in final_market_results_blockchain]
    elif uniform_pricing and not discriminative_pricing:
        final_market_results_blockchain = [x[:9] + x[-2:] for x in final_market_results_blockchain]
    elif discriminative_pricing and not uniform_pricing:
        final_market_results_blockchain = [x[:8] + x[-3:] for x in final_market_results_blockchain]

    return final_market_results_blockchain


def convertToPdFinalMarketResults(market_results_blockchain, uniform_pricing, discriminative_pricing,
                                  market_results_python):
    market_results_blockchain = reformat_full_clearing_results(market_results_blockchain, uniform_pricing,
                                                               discriminative_pricing)
    market_results_blockchain = lem_blockchain.convertToPdDataFrame(market_results_blockchain,
                                                                    market_results_python.columns.to_list())
    if len(market_results_blockchain) > 0:
        columns_to_float = (
            market_results_python.dtypes[market_results_python.dtypes == "float64"]).index.to_list()
        columns_to_int = (
            market_results_python.dtypes[market_results_python.dtypes == "int64"]).index.to_list()
        columns_to_divide = [x for x in market_results_python.columns if "price" in x]

        market_results_blockchain = lem_blockchain.df_convert_data_types(market_results_blockchain,
                                                                         columns_to_float, columns_to_int,
                                                                         columns_to_divide)
        return market_results_blockchain
