pragma solidity >=0.5.0 <0.7.5;
pragma experimental ABIEncoderV2;

contract Param {
    
    string id_supplier = 'SUPPLIER';        //id of energy supplier
    uint qty_energy_supplier_bid = 100000000;    //quantity supplier bids on lem
    uint qty_energy_supplier_offer = 100000000;  //quantity supplier offers on lem
    uint price_supplier_bid = 1000;         //bid price in €/kWh *10000
    uint price_supplier_offer = 10;     //offer price in €/kWh *10000

	function getIdSupplier() public view returns(string memory) {
	    return id_supplier;
	}
	function getPriceOfferSupplier() public view returns(uint) {
	    return price_supplier_offer;
	}
	function getPriceBidSupplier() public view returns(uint) {
	    return price_supplier_bid;
	}
	function getQtyOfferSupplier() public view returns(uint) {
	    return qty_energy_supplier_offer;
	}
	function getQtyBidSupplier() public view returns(uint) {
	    return qty_energy_supplier_bid;
	}
}