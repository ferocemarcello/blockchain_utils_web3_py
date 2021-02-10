import copy
import math
import time
import os

from numpy import int64, float64
from web3 import Web3, HTTPProvider
import pandas as pd
from pathlib import Path

from lemlab.db_connection import db_param
from lemlab.platform import lem_settings

Platform_contract = None
coinbase = None
functions = None
contract_address = None
web3_instance = None

cols_offer_bid = ['id_user', 'qty_energy', 'type', 'price_energy', 'status', 't_submission', 'ts_delivery', 'number',
                  'quality_energy']
price_multiplier = 10000  # used in solidity, since solidity doesn't handle floating point values


def push_offer_or_bid_dataframe(dataframe):
    bid_offer_asTuple = manipulateTuple(manipulateDataFrames(dataframe)[0])
    tx_hash = functions.pushOfferOrBid(bid_offer_asTuple, bid_offer_asTuple[2] == 0).transact({'from': coinbase})
    #web3_instance.eth.waitForTransactionReceipt(tx_hash)


def getLog(tx_hash):
    tx_receipt = web3_instance.eth.waitForTransactionReceipt(tx_hash)
    log_to_process = tx_receipt['logs'][0]
    processed_log = Platform_contract.events.logString().processLog(log_to_process)
    log = processed_log['args']['arg']
    return log


def manipulateTuple(tup):
    tup_mod = (str(tup[0]), int(tup[1]), int(tup[2]), int(tup[3]), int(tup[4]), int(tup[5]), int(tup[6]),
               int(tup[7]), int(tup[8]))  # this cast is necessary to pass to the smart contract)
    return tup_mod


def manipulateTupleList(tl):
    tuple_list = [manipulateTuple(tup) for tup in tl]  # this cast is necessary to pass to the smart contract)
    return tuple_list


def sort_offers_bids_tsdelivery(ascendingOffers=True, ascendingBids=True):
    tx_hash = functions.sortOffersAndBidsByTsDelivery(ascendingOffers, ascendingBids).transact({'from': coinbase})
    web3_instance.eth.waitForTransactionReceipt(tx_hash)


def manipulateDataFrames(*dfs):
    lists = []
    for df in dfs:
        dfm = copy.deepcopy(df)
        dfm = dfm[cols_offer_bid]
        dfm["price_energy"] = price_multiplier * dfm["price_energy"]
        if type(dfm) == pd.core.frame.DataFrame:
            dfm['price_energy'] = dfm['price_energy'].apply(round)
            lists.append([tuple(x) for x in dfm.values.tolist()])
        elif type(dfm) == pd.core.series.Series:
            dfm['price_energy'] = round(dfm['price_energy'])
            lists.append(dfm.values.tolist())
        # dfm['price_energy'] = dfm['price_energy'].astype(int)
        # tuple_list = dfm.to_records(index=False)
        # tuple_list = manipulateTupleList(tuple_list)
    return lists


def convertToPdDataFrame(_list_of_lists, cols):
    df = pd.DataFrame(_list_of_lists, columns=cols)
    return df


def round_down(n, decimals=0):
    multiplier = 10 ** decimals
    return math.floor(n * multiplier) / multiplier


def df_convert_data_types(dataframe, columns_to_float, columns_to_int, columns_to_divide):
    for c in columns_to_float:
        dataframe[c] = dataframe[c].astype(float64)
    for c in columns_to_int:
        dataframe[c] = dataframe[c].astype(int64)
    for c in columns_to_divide:
        dataframe[c] = round((1 / price_multiplier) * dataframe[c], 4)
    return dataframe


def df_market_result_convert_data_types(dataframe, uniform_pricing, discriminative_pricing):
    dataframe['qty_energy_offer'] = dataframe['qty_energy_offer'].astype(float)

    dataframe['price_energy_offer'] = dataframe['price_energy_offer'].astype(float)
    dataframe["price_energy_offer"] = round((1 / price_multiplier) * dataframe["price_energy_offer"], 4)

    dataframe['quality_energy_offer'] = dataframe['quality_energy_offer'].astype(float)
    dataframe['type_offer'] = dataframe['type_offer'].astype(float)
    dataframe['number_offer'] = dataframe['number_offer'].astype(float)
    dataframe['status_offer'] = dataframe['status_offer'].astype(float)
    dataframe['t_submission_offer'] = dataframe['t_submission_offer'].astype(float)
    dataframe['ts_delivery'] = dataframe['ts_delivery'].astype(float)
    dataframe['qty_energy_bid'] = dataframe['qty_energy_bid'].astype(float)

    dataframe['price_energy_bid'] = dataframe['price_energy_bid'].astype(float)
    dataframe["price_energy_bid"] = round((1 / price_multiplier) * dataframe["price_energy_bid"], 4)

    dataframe['quality_energy_bid'] = dataframe['quality_energy_bid'].astype(float)
    dataframe['type_bid'] = dataframe['type_bid'].astype(float)
    dataframe['number_bid'] = dataframe['number_bid'].astype(float)
    dataframe['status_bid'] = dataframe['status_bid'].astype(float)
    dataframe['t_submission_bid'] = dataframe['t_submission_bid'].astype(float)
    if uniform_pricing:
        dataframe['price_energy_cleared_uniform'] = round(
            (1 / price_multiplier) * dataframe["price_energy_cleared_uniform"], 4)
    if discriminative_pricing:
        dataframe['price_energy_cleared_discriminative'] = round(
            (1 / price_multiplier) * dataframe["price_energy_cleared_discriminative"], 4)

    return dataframe


def setUpBlockchain(host="localhost", port="8501", contract_name="Platform", timeout=2000, network_id='8995',
                    project_dir=None):
    global contract_address, Platform_contract, coinbase, functions, web3_instance
    try:
        web3_instance = Web3(
            HTTPProvider("http://" + host + ":" + port, request_kwargs={'timeout': timeout}))  # seconds
        # getting abi, bytecode, address via json file created by truffle
        if project_dir == None:
            json_path = os.path.join(str(Path(__file__).parent.parent.parent), "Truffle", "build", "contracts",
                                     contract_name + '.json')
        else:
            json_path = os.path.join(project_dir, 'Truffle', 'build', 'contracts', contract_name + '.json')
        import json
        with open(json_path) as json_file:
            data = json.load(json_file)
        bytecode = data['bytecode']
        network_ids = list(data['networks'].keys())
        contract_address = data['networks'][network_id]['address']
        abi = json.dumps(data['abi'])
        Platform_contract = web3_instance.eth.contract(address=contract_address, abi=abi, bytecode=bytecode)
        coinbase = web3_instance.eth.coinbase
        functions = Platform_contract.functions
    except Exception as e:
        print(e)


def clearTempData():
    try:
        tx_hash = functions.clearTempData().transact({'from': coinbase})
        web3_instance.eth.waitForTransactionReceipt(tx_hash)
    except:
        limit_to_remove = 200
        while len(getOffers_or_Bids()) > 0 or len(getOffers_or_Bids(False)) > 0:
            try:
                tx_hash = functions.clearTempData_gas_limit(limit_to_remove).transact({'from': coinbase})
                web3_instance.eth.waitForTransactionReceipt(tx_hash)
            except:
                limit_to_remove -= 50


def clear_user_infos():
    try:
        tx_hash = functions.clearUserInfos().transact({'from': coinbase})
        web3_instance.eth.waitForTransactionReceipt(tx_hash)
    except:
        limit_to_remove = 500
        while len(functions.get_user_infos()) > 0:
            try:
                tx_hash = functions.clearUserInfos_gas_limit(limit_to_remove).transact({'from': coinbase})
                web3_instance.eth.waitForTransactionReceipt(tx_hash)
            except:
                limit_to_remove -= 50


def getOffers_or_Bids(isOffer=True, temp=True):
    if isOffer:
        return functions.getOffers(temp).call()
    else:
        return functions.getBids(temp).call()


def getOffersBids_sort_manipulate():
    offers_list = getOffers_or_Bids(isOffer=True)
    bids_list = getOffers_or_Bids(isOffer=False)

    offers_df = convertToPdDataFrame(offers_list, cols_offer_bid)
    bids_df = convertToPdDataFrame(bids_list, cols_offer_bid)

    offers_df.sort_values(db_param.TS_DELIVERY)
    bids_df.sort_values(db_param.TS_DELIVERY)

    offers_list, bids_list = manipulateDataFrames(offers_df, bids_df)
    return offers_list, bids_list


def test():
    pass


def market_clearing_blockchain(n_clearings, t_clearing_first, supplier_bids,
                               uniform_pricing, discriminative_pricing):
    sort_offers_bids_tsdelivery()
    start = time.time()
    tx_hash = functions.market_clearing(n_clearings, int(t_clearing_first), supplier_bids,
                                        uniform_pricing, discriminative_pricing,
                                        lem_settings.interval_clearing).transact({'from': coinbase})
    end = time.time()
    print("market clearing done in " + str(end - start) + " seconds")
    log = getLog(tx_hash=tx_hash)
    print(log)
    market_results_total = functions.getMarketResultsTotal().call()
    clearTempData()
    return market_results_total
