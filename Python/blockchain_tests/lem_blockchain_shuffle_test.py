from lemlab.platform import blockchain_utils


#I test if the shuffling produces different orders of values, but the same set of values
def test_shuffling():
    blockchain_utils.setUpBlockchain(timeout=60)
    offers_blockchain = blockchain_utils.getOffers_or_Bids(isOffer=True)
    bids_blockchain = blockchain_utils.getOffers_or_Bids(isOffer=False)

    blockchain_utils.setUpBlockchain(contract_name="Lib")
    offers_blockchain_shuffled = blockchain_utils.functions.shuffle_OfferBids(offers_blockchain).call()
    bids_blockchain_shuffled = blockchain_utils.functions.shuffle_OfferBids(bids_blockchain).call()

    assert len(offers_blockchain) == len(offers_blockchain_shuffled) and len(bids_blockchain) == len(
        bids_blockchain_shuffled)
    assert offers_blockchain != offers_blockchain_shuffled and bids_blockchain != bids_blockchain_shuffled
    assert set([tuple(x) for x in offers_blockchain]) == set([tuple(x) for x in offers_blockchain_shuffled])
    assert set([tuple(x) for x in bids_blockchain]) == set([tuple(x) for x in bids_blockchain_shuffled])
