from lemlab.platform import lem_blockchain


def test_shuffling():
    lem_blockchain.setUpBlockchain(timeout=60)
    offers_blockchain = lem_blockchain.getOffers_or_Bids(isOffer=True)
    bids_blockchain = lem_blockchain.getOffers_or_Bids(isOffer=False)

    lem_blockchain.setUpBlockchain(contract_name="Lib")
    offers_blockchain_shuffled = lem_blockchain.functions.shuffle_OfferBids(offers_blockchain).call()
    bids_blockchain_shuffled = lem_blockchain.functions.shuffle_OfferBids(bids_blockchain).call()

    assert len(offers_blockchain) == len(offers_blockchain_shuffled) and len(bids_blockchain) == len(
        bids_blockchain_shuffled)
    assert offers_blockchain != offers_blockchain_shuffled and bids_blockchain != bids_blockchain_shuffled
    assert set([tuple(x) for x in offers_blockchain]) == set([tuple(x) for x in offers_blockchain_shuffled])
    assert set([tuple(x) for x in bids_blockchain]) == set([tuple(x) for x in bids_blockchain_shuffled])
